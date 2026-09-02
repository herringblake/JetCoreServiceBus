"""Integration tests for bus_client.py (Design.md §11 Track B, Step B6).

Requires a live NATS server, bootstrapped per Steps A3-A6 (`bash
infra/nats/up.sh`, or `docker compose up -d nats` if already bootstrapped).

Unlike registry.py's tests (Step B5), these can't use arbitrary unique
subjects — real adapter identities only have permission for the exact
subjects Step A2's manifest grants them. Tests use webhook-listener-01-TEST
and file-storage-01-TEST — dedicated test-only twins of the real
webhook-listener-01/file-storage-01 pair (Defects.md Defect 1), with
identical permissions on their real subject (events.files.FileWriteRequested)
but no live production counterpart to collide with — with an autouse
fixture purging the EVENTS stream before each test for isolation, and a
unique durable consumer name per test (a stale consumer's cursor could
otherwise point at sequence numbers a purge just invalidated).
"""

import base64
import time
import uuid

import pytest
from _helpers import SUBJECT, connect, settings, wait_until_cache_nonempty
from jetcore.bus_client import REDELIVERY_BACKOFF_SECONDS, BusClient
from jetcore.crypto import encrypt_for_recipients, generate_signing_keypair, sign
from jetcore.envelope import EncryptionMetadata, Event, EventDetails, EventEnvelope

# Kept as local aliases so the rest of this file (and its history/diffs)
# doesn't need to change beyond the import — the shared implementations
# now live in _helpers.py / conftest.py (Step B7), alongside
# test_end_to_end.py which needs the same setup.
_connect = connect
_wait_until_cache_nonempty = wait_until_cache_nonempty


async def test_publish_and_fetch_round_trip(durable_name: str) -> None:
    publisher = await _connect("webhook-listener-01-test")
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)
        # subscribe() starts the heartbeat; wait for the publisher's
        # recipient cache to actually see it registered.
        await _wait_until_cache_nonempty(await publisher._cache_for(SUBJECT))

        event_id = await publisher.publish(
            SUBJECT, b"hello from the round trip", event_type="FileWriteRequested"
        )

        received = await consumer.fetch(durable_name, timeout=3)

        assert len(received) == 1
        assert received[0].payload == b"hello from the round trip"
        assert received[0].details.event_type == "FileWriteRequested"
        assert received[0].details.source_service_id == "webhook-listener-01-test"
        # Design.md §13 Step I1 — publish() now returns the generated
        # eventId, and it must be the *same* id the receiver actually sees.
        assert event_id == received[0].details.event_id
        await received[0].ack()
    finally:
        await publisher.close()
        await consumer.close()


async def test_publish_with_no_recipients_does_not_crash(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No one is registered for this subject in this test — should warn,
    not raise, and still publish (open-bus metadata is useful even
    encrypted-for-no-one).

    This test's first version failed intermittently, and initially looked
    like a `caplog`-vs-asyncio quirk — it wasn't. A separate, real bug:
    `subscribe()` in an earlier test starts a heartbeat that `close()`
    cancels but never deregisters, so its KV entry lingers past that
    test until its 60s TTL naturally expires — meaning "no recipients"
    wasn't actually true when this test ran after one that subscribed.
    Fixed by having `_clean_state` purge service-directory too, not just
    the stream (see that fixture's docstring). Verified `caplog` itself
    was never the problem with an isolated probe once the real bug was
    understood — worth recording so no one "fixes" this again by
    reaching for a manual logging handler."""
    caplog.set_level("WARNING", logger="jetcore.bus_client")
    publisher = await _connect("webhook-listener-01-test")
    try:
        await publisher.publish(
            SUBJECT, b"no one is listening yet", event_type="FileWriteRequested"
        )
        assert "no registered recipients yet" in caplog.text
    finally:
        await publisher.close()


async def test_tampered_ciphertext_is_not_returned(durable_name: str) -> None:
    """A message whose payload was corrupted in transit should fail to
    decrypt, and fetch() should not hand it back as if it were valid."""
    publisher = await _connect("webhook-listener-01-test")
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)
        await _wait_until_cache_nonempty(await publisher._cache_for(SUBJECT))

        await publisher.publish(
            SUBJECT, b"this will be tampered with", event_type="FileWriteRequested"
        )

        # Intercept the real message, corrupt its ciphertext, republish —
        # then let the real consumer try to process the corrupted version.
        sub = consumer._subscriptions[durable_name]
        [msg] = await sub.fetch(batch=1, timeout=3)
        await msg.ack()

        event = Event.from_wire(msg.data)
        tampered = Event(
            event=EventEnvelope(
                eventDetails=event.event.event_details,
                encryption=event.event.encryption,
                eventPayload=base64.b64encode(b"not the real ciphertext").decode(),
            )
        )
        await publisher._js.publish(SUBJECT, tampered.to_wire())

        received = await consumer.fetch(durable_name, timeout=3)

        assert received == []
    finally:
        await publisher.close()
        await consumer.close()


async def test_signature_not_matching_claimed_key_is_rejected(durable_name: str) -> None:
    """A message whose signature bytes don't match its own claimed
    `sourcePublicKey` should be rejected.

    History worth keeping: an *earlier* version of this test tried to
    simulate "an impostor pretends to be webhook-listener-01" by signing
    with a different keypair and setting `sourcePublicKey` to that *same*
    different keypair's public key. That's self-consistent (the embedded
    key genuinely produced the signature), so it correctly *passed*
    verification back when verification trusted the message's own
    embedded key — meaning the earlier test was wrong, not the code. That
    finding is what led to the service-identity directory below this test
    (Design.md §9 item #4): verification now looks up the *trusted* key
    for `sourceServiceId` rather than trusting the embedded claim, which
    is what actually closes the gap. See
    `test_impersonation_with_self_consistent_key_is_rejected` immediately
    below for the exact original scenario, now correctly rejected.
    """
    publisher = await _connect("webhook-listener-01-test")
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)

        claimed_signer = generate_signing_keypair()
        actual_signer = generate_signing_keypair()  # different key than claimed
        plaintext = b"signature will not match the claimed key"
        ciphertext = encrypt_for_recipients(plaintext, [consumer._encryption_keypair.public_key])
        mismatched_signature = sign(actual_signer.seed, plaintext)

        forged = Event(
            event=EventEnvelope(
                eventDetails=EventDetails(
                    eventType="FileWriteRequested",
                    eventSchemaVersion="1.0.0",
                    sourceServiceId="webhook-listener-01-test",
                    sourcePublicKey=claimed_signer.public_key,  # doesn't match the signature below
                    signature=base64.b64encode(mismatched_signature).decode(),
                ),
                encryption=EncryptionMetadata(
                    algorithm="age-v1", recipients=[consumer._encryption_keypair.public_key]
                ),
                eventPayload=base64.b64encode(ciphertext).decode(),
            )
        )
        await publisher._js.publish(SUBJECT, forged.to_wire())

        received = await consumer.fetch(durable_name, timeout=3)

        assert received == []
    finally:
        await publisher.close()
        await consumer.close()


async def test_impersonation_with_self_consistent_key_is_rejected(durable_name: str) -> None:
    """The exact scenario that exposed Design.md §9 item #4 in Step B6:
    an impostor signs with THEIR OWN key and embeds THAT SAME key as
    `sourcePublicKey` — internally self-consistent, so a verifier that
    only checked the message's own embedded claim would (wrongly) accept
    it. This must now be rejected, because verification looks up the
    trusted key registered for "webhook-listener-01-test" (by the real
    `publisher` below, at connect time) instead of trusting the embedded
    claim — and the impostor's key doesn't match it."""
    publisher = await _connect("webhook-listener-01-test")
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)

        impostor = generate_signing_keypair()
        plaintext = b"pretending to be webhook-listener-01"
        ciphertext = encrypt_for_recipients(plaintext, [consumer._encryption_keypair.public_key])
        self_consistent_signature = sign(impostor.seed, plaintext)

        forged = Event(
            event=EventEnvelope(
                eventDetails=EventDetails(
                    eventType="FileWriteRequested",
                    eventSchemaVersion="1.0.0",
                    sourceServiceId="webhook-listener-01-test",  # claims to be the real sender
                    sourcePublicKey=impostor.public_key,  # self-consistent with the signature below
                    signature=base64.b64encode(self_consistent_signature).decode(),
                ),
                encryption=EncryptionMetadata(
                    algorithm="age-v1", recipients=[consumer._encryption_keypair.public_key]
                ),
                eventPayload=base64.b64encode(ciphertext).decode(),
            )
        )
        await publisher._js.publish(SUBJECT, forged.to_wire())

        received = await consumer.fetch(durable_name, timeout=3)

        assert received == []
    finally:
        await publisher.close()
        await consumer.close()


async def test_message_from_unregistered_sender_is_rejected(durable_name: str) -> None:
    """A sourceServiceId that never registered an identity can't be
    verified at all — rejected as unverifiable, not accepted by default."""
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)

        ghost = generate_signing_keypair()
        plaintext = b"from a service that was never registered"
        ciphertext = encrypt_for_recipients(plaintext, [consumer._encryption_keypair.public_key])

        forged = Event(
            event=EventEnvelope(
                eventDetails=EventDetails(
                    eventType="FileWriteRequested",
                    eventSchemaVersion="1.0.0",
                    sourceServiceId=f"never-registered-{uuid.uuid4().hex[:8]}",
                    sourcePublicKey=ghost.public_key,
                    signature=base64.b64encode(sign(ghost.seed, plaintext)).decode(),
                ),
                encryption=EncryptionMetadata(
                    algorithm="age-v1", recipients=[consumer._encryption_keypair.public_key]
                ),
                eventPayload=base64.b64encode(ciphertext).decode(),
            )
        )
        # Publish via a real, permissioned identity — this test is about
        # sourceServiceId being unregistered, not about publish permissions.
        publisher = await _connect("webhook-listener-01-test")
        try:
            await publisher._js.publish(SUBJECT, forged.to_wire())
        finally:
            await publisher.close()

        received = await consumer.fetch(durable_name, timeout=3)

        assert received == []
    finally:
        await consumer.close()


async def test_publish_with_same_msg_id_does_not_duplicate(durable_name: str) -> None:
    """Design.md §16 Step N2 (§9 item #10). A real, consumer-visible proof
    — not just "the second publish() call didn't raise" — that a repeated
    `msg_id` within the stream's own dedup window results in exactly one
    fetchable message, the actual thing an idempotency key needs to
    guarantee for a retried HTTP request."""
    publisher = await _connect("webhook-listener-01-test")
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)
        await _wait_until_cache_nonempty(await publisher._cache_for(SUBJECT))

        shared_msg_id = f"idempotency-probe-{uuid.uuid4().hex[:8]}"
        first_event_id = await publisher.publish(
            SUBJECT, b"attempt one", event_type="FileWriteRequested", msg_id=shared_msg_id
        )
        second_event_id = await publisher.publish(
            SUBJECT, b"attempt two (a retry)", event_type="FileWriteRequested", msg_id=shared_msg_id
        )
        # publish()'s own docstring: on a duplicate, it fetches the real
        # original message back and returns *its* eventId, not the second
        # call's own locally-built-but-never-persisted one — a caller
        # correlating a sync reply against this id (REST API Service's
        # own ?wait=) needs the real, original id, not a fresh one
        # nothing will ever reply to.
        assert second_event_id == first_event_id

        received = await consumer.fetch(durable_name, timeout=3)
        assert len(received) == 1
        assert received[0].payload == b"attempt one"
        assert received[0].details.event_id == first_event_id
        await received[0].ack()

        # Confirm there's genuinely nothing else queued behind it — not
        # just "fetch happened to return 1", but "there was only ever 1".
        nothing_more = await consumer.fetch(durable_name, timeout=1)
        assert nothing_more == []
    finally:
        await publisher.close()
        await consumer.close()


async def test_publish_without_msg_id_is_unaffected(durable_name: str) -> None:
    """The additive half of Design.md §16 Step N2: two publishes with no
    `msg_id` at all (every existing caller, unchanged) must NOT be treated
    as duplicates of each other just because they share identical payload
    bytes — dedup is keyed on Nats-Msg-Id, never inferred from content."""
    publisher = await _connect("webhook-listener-01-test")
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)
        await _wait_until_cache_nonempty(await publisher._cache_for(SUBJECT))

        await publisher.publish(SUBJECT, b"identical payload", event_type="FileWriteRequested")
        await publisher.publish(SUBJECT, b"identical payload", event_type="FileWriteRequested")

        received = await consumer.fetch(durable_name, timeout=3)
        assert len(received) == 1
        await received[0].ack()
        received_second = await consumer.fetch(durable_name, timeout=3)
        assert len(received_second) == 1
        await received_second[0].ack()
    finally:
        await publisher.close()
        await consumer.close()


async def test_nak_applies_escalating_backoff_delay(durable_name: str) -> None:
    """Design.md §16 Step N1 (§9 item #9). A real, live-timed proof, not
    just "nak() didn't raise" — confirms ReceivedEvent.nak() actually
    delays redelivery per REDELIVERY_BACKOFF_SECONDS rather than the bare
    `msg.nak()` this project found (empirically, via a throwaway probe
    against this exact pinned nats-server/nats-py) redelivers near-
    instantly regardless of a consumer's configured `backoff`."""
    publisher = await _connect("webhook-listener-01-test")
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)
        await _wait_until_cache_nonempty(await publisher._cache_for(SUBJECT))
        await publisher.publish(SUBJECT, b"backoff probe", event_type="FileWriteRequested")

        first = await consumer.fetch(durable_name, timeout=3)
        assert len(first) == 1
        t0 = time.monotonic()
        await first[0].nak()

        # Immediately after nak() — nothing should be available yet; the
        # first backoff entry hasn't elapsed. A short timeout, well under
        # REDELIVERY_BACKOFF_SECONDS[0], so this can't accidentally pass
        # by just being slow enough to stumble past the real delay.
        too_soon = await consumer.fetch(durable_name, timeout=REDELIVERY_BACKOFF_SECONDS[0] / 2)
        assert too_soon == []

        second = await consumer.fetch(durable_name, timeout=REDELIVERY_BACKOFF_SECONDS[0] + 3)
        elapsed = time.monotonic() - t0
        assert len(second) == 1
        assert second[0].details.event_id == first[0].details.event_id
        # Real redelivery genuinely waited for roughly the configured
        # first backoff entry, not near-zero (the bare-nak() bug this
        # fix replaces) and not implausibly longer either.
        assert REDELIVERY_BACKOFF_SECONDS[0] * 0.5 < elapsed < REDELIVERY_BACKOFF_SECONDS[0] + 3
        await second[0].ack()
    finally:
        await publisher.close()
        await consumer.close()


async def test_fetch_without_subscribe_raises() -> None:
    consumer = await _connect("file-storage-01-test")
    try:
        with pytest.raises(ValueError, match="call subscribe"):
            await consumer.fetch("never-subscribed-durable")
    finally:
        await consumer.close()


async def test_connect_as_adapter_uses_a_stable_signing_identity(durable_name: str) -> None:
    """Design.md §12 Step C6: connect_as_adapter() must derive its signing
    key from the .creds file's own embedded nkey every time, not generate
    a fresh one per call — the actual fix for the class of problem Step
    B7 found (repeated/concurrent connections invalidating each other's
    registered identity). Two separate connections for the same
    serviceId must agree on the same signing public key."""
    wl_settings = settings("webhook-listener-01-test")
    client_a = await BusClient.connect_as_adapter(wl_settings, adapter_type="test")
    client_b = await BusClient.connect_as_adapter(wl_settings, adapter_type="test")
    try:
        assert client_a._signing_public_key == client_b._signing_public_key
    finally:
        await client_a.close()
        await client_b.close()


async def test_connect_as_adapter_signature_verifies_against_creds_derived_key(
    durable_name: str,
) -> None:
    """Not just "the two public keys match each other" — the derived
    identity must actually be the one file-storage-01 uses to enforce
    permissions/verify signatures, proven end-to-end: publish via
    connect_as_adapter, fetch via the ordinary test connect(), and
    confirm the message verifies and decrypts normally."""
    publisher = await BusClient.connect_as_adapter(
        settings("webhook-listener-01-test"), adapter_type="test"
    )
    consumer = await _connect("file-storage-01-test")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)
        await _wait_until_cache_nonempty(await publisher._cache_for(SUBJECT))

        await publisher.publish(SUBJECT, b"signed with the real creds identity", event_type="X")

        received = await consumer.fetch(durable_name, timeout=3)

        assert len(received) == 1
        assert received[0].payload == b"signed with the real creds identity"
        assert received[0].details.source_service_id == "webhook-listener-01-test"
    finally:
        await publisher.close()
        await consumer.close()

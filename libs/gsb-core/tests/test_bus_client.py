"""Integration tests for bus_client.py (Design.md §11 Track B, Step B6).

Requires a live NATS server, bootstrapped per Steps A3-A6 (`bash
infra/nats/up.sh`, or `docker compose up -d nats` if already bootstrapped).

Unlike registry.py's tests (Step B5), these can't use arbitrary unique
subjects — real adapter identities (webhook-listener-01, file-storage-01)
only have permission for the exact subjects Step A2's manifest grants them.
Tests use the real Phase 2 pair on their real subject
(events.files.FileWriteRequested), with an autouse fixture purging the
EVENTS stream before each test for isolation, and a unique durable
consumer name per test (a stale consumer's cursor could otherwise point at
sequence numbers a purge just invalidated).
"""

import asyncio
import base64
import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from gsb_core.bus_client import BusClient
from gsb_core.config import AdapterSettings
from gsb_core.crypto import (
    encrypt_for_recipients,
    generate_encryption_keypair,
    generate_signing_keypair,
    sign,
)
from gsb_core.envelope import EncryptionMetadata, Event, EventDetails, EventEnvelope
from gsb_core.registry import RecipientCache
from nats.js.errors import NoKeysError

NATS_URL = "nats://localhost:4222"
SUBJECT = "events.files.FileWriteRequested"
CREDS_DIR = "infra/nats/operator/creds"


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    """Purges the EVENTS stream AND any service-directory registrations
    for SUBJECT before each test, via gsb-admin — real adapter identities
    can't be given arbitrary unique test subjects (only their real, fixed
    ones), so tests get clean state each time instead.

    The KV half of this was added after a real, initially-confusing
    failure: `test_publish_with_no_recipients_does_not_crash` failed
    intermittently depending on what ran before it in the same session —
    `subscribe()` in an earlier test starts a real heartbeat that
    `BusClient.close()` cancels but never deregisters, so the KV entry
    lingers until its 60s TTL naturally expires. Purging only the message
    stream (registry.py's own concern is untouched by that) wasn't enough;
    a genuinely "no recipients" test needs the registry cleaned too, not
    just the stream.
    """
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/gsb-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    kv = await js.key_value("service-directory")
    try:
        for key in await kv.keys(filters=[f"{SUBJECT}."]):
            await kv.delete(key)
    except NoKeysError:
        pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"


def _settings(service_id: str) -> AdapterSettings:
    # AdapterSettings needs an env-driven load in production (Step B4), but
    # constructing it directly is simpler and equally valid for tests —
    # pydantic-settings models are just pydantic models with extra loading
    # behavior bolted on.
    return AdapterSettings(
        service_id=service_id,
        nats_url=NATS_URL,
        nats_creds_path=f"{CREDS_DIR}/{service_id}.creds",
    )


async def _connect(service_id: str) -> BusClient:
    signing = generate_signing_keypair()
    encryption = generate_encryption_keypair()
    return await BusClient.connect(
        _settings(service_id),
        adapter_type="test-adapter",
        encryption_keypair=encryption,
        signing_seed=signing.seed,
        signing_public_key=signing.public_key,
    )


async def _wait_until_cache_nonempty(cache: RecipientCache, *, timeout: float = 3.0) -> None:
    async def _poll() -> None:
        while not cache.current():
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def test_publish_and_fetch_round_trip(durable_name: str) -> None:
    publisher = await _connect("webhook-listener-01")
    consumer = await _connect("file-storage-01")
    try:
        await consumer.subscribe(SUBJECT, durable_name=durable_name)
        # subscribe() starts the heartbeat; wait for the publisher's
        # recipient cache to actually see it registered.
        await _wait_until_cache_nonempty(await publisher._cache_for(SUBJECT))

        await publisher.publish(
            SUBJECT, b"hello from the round trip", event_type="FileWriteRequested"
        )

        received = await consumer.fetch(durable_name, timeout=3)

        assert len(received) == 1
        assert received[0].payload == b"hello from the round trip"
        assert received[0].details.event_type == "FileWriteRequested"
        assert received[0].details.source_service_id == "webhook-listener-01"
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
    caplog.set_level("WARNING", logger="gsb_core.bus_client")
    publisher = await _connect("webhook-listener-01")
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
    publisher = await _connect("webhook-listener-01")
    consumer = await _connect("file-storage-01")
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
    publisher = await _connect("webhook-listener-01")
    consumer = await _connect("file-storage-01")
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
                    sourceServiceId="webhook-listener-01",
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
    trusted key registered for "webhook-listener-01" (by the real
    `publisher` below, at connect time) instead of trusting the embedded
    claim — and the impostor's key doesn't match it."""
    publisher = await _connect("webhook-listener-01")
    consumer = await _connect("file-storage-01")
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
                    sourceServiceId="webhook-listener-01",  # claims to be the real sender
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
    consumer = await _connect("file-storage-01")
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
        publisher = await _connect("webhook-listener-01")
        try:
            await publisher._js.publish(SUBJECT, forged.to_wire())
        finally:
            await publisher.close()

        received = await consumer.fetch(durable_name, timeout=3)

        assert received == []
    finally:
        await consumer.close()


async def test_fetch_without_subscribe_raises() -> None:
    consumer = await _connect("file-storage-01")
    try:
        with pytest.raises(ValueError, match="call subscribe"):
            await consumer.fetch("never-subscribed-durable")
    finally:
        await consumer.close()

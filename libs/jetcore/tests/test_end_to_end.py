"""Step B7 — End-to-end proof (Design.md §11 Track B): two in-process
"fake adapters" built on BusClient, exchanging one signed+encrypted event
through the real bus. This is the concrete "Phase 1 is done" checkpoint —
everything downstream (real adapters, Phase 2+) builds on this working.

Scope note: Step B6's own integration tests (test_bus_client.py) already
proved BusClient's correctness in detail — round-trip, tamper detection,
signature verification, impersonation rejection, unregistered senders.
This file doesn't re-prove those properties. What it proves instead, and
what B6's tests didn't: that the whole thing holds together when
structured as two independent, *concurrently running* adapter-shaped
objects — the actual shape real adapters will take (connect once at
startup, then run an ongoing loop) — rather than a single linear script
issuing calls to two BusClient instances in sequence.

Requires a live NATS server, bootstrapped per Steps A3-A6.
"""

from __future__ import annotations

import asyncio

from _helpers import SUBJECT, connect, wait_until_cache_nonempty
from jetcore.bus_client import BusClient, ReceivedEvent


class FakePublisherAdapter:
    """Mimics a real adapter's publish-side shape (e.g. the eventual
    Webhook Listener, Design.md §8): connect once, publish, shut down.
    Fire-and-forget from the caller's perspective — same as the real one."""

    def __init__(self, service_id: str) -> None:
        self._service_id = service_id

    async def run(self, *, subject: str, payload: bytes, event_type: str) -> None:
        await self.run_many(subject=subject, items=[(payload, event_type)])

    async def run_many(self, *, subject: str, items: list[tuple[bytes, str]]) -> None:
        """Connects once, publishes each (payload, event_type) pair in
        order, then shuts down — several business events happening during
        one adapter's connected session, not one connection per event.

        Deliberately NOT modeled as multiple independent
        FakePublisherAdapter instances under the same service_id — an
        earlier version of this test did exactly that and hit a real, but
        test-fixture-only, problem: `connect()` (a test helper) generates
        a *fresh random* signing key every call, and since service-identity
        registration is last-writer-wins per serviceId (Design.md §4.6),
        concurrent instances sharing one serviceId kept invalidating each
        other's registered key — 2 of 3 messages failed verification, not
        because anything was actually forged, but because the "trusted"
        key kept changing out from under already-signed messages. A real
        adapter doesn't do this: one serviceId means one persistent nkey
        loaded from one .creds file, not a fresh key per connection — and
        the design's actual answer to horizontal scaling is giving each
        replica its own distinct serviceId (Decision #18), not sharing
        one. This method avoids the artifact by keeping one connection
        (one key) for the whole sequence, which is also the more
        realistic shape anyway."""
        client = await connect(self._service_id)
        try:
            # A real publisher doesn't necessarily wait for a recipient to
            # exist (test_publish_with_no_recipients_does_not_crash, B6,
            # covers that case) — this fake one does, deliberately, so the
            # test is deterministic rather than racing the consumer's
            # startup. In production this "wait" would more naturally be
            # "the consumer adapter has already been running for a while."
            cache = await client._cache_for(subject)
            await wait_until_cache_nonempty(cache)
            for payload, event_type in items:
                await client.publish(subject, payload, event_type=event_type)
        finally:
            await client.close()


class FakeConsumerAdapter:
    """Mimics a real adapter's consume-side shape (e.g. the eventual File
    Storage Adapter, Design.md §8): connect + subscribe once at startup,
    then run an ongoing fetch loop — collecting whatever arrives until
    told to stop, the same structure a real long-running adapter process
    would have."""

    def __init__(self, service_id: str) -> None:
        self._service_id = service_id
        self.received: list[ReceivedEvent] = []
        self._client: BusClient | None = None
        self._durable_name: str | None = None

    async def start(self, *, subject: str, durable_name: str) -> None:
        """The startup half of a real adapter's lifecycle — must complete
        before anyone can usefully publish to it (this is what registers
        it as a recipient)."""
        self._client = await connect(self._service_id)
        self._durable_name = await self._client.subscribe(subject, durable_name=durable_name)

    async def run_until(self, *, count: int, timeout: float) -> None:
        """The ongoing half — a real adapter's main loop runs forever;
        this one stops once it's collected `count` events or `timeout`
        elapses, so the test can await it directly."""
        assert self._client is not None and self._durable_name is not None, "call start() first"
        deadline = asyncio.get_event_loop().time() + timeout
        while len(self.received) < count and asyncio.get_event_loop().time() < deadline:
            events = await self._client.fetch(self._durable_name, batch=count, timeout=1.0)
            for event in events:
                await event.ack()
            self.received.extend(events)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.close()


async def test_two_fake_adapters_exchange_one_event_end_to_end(durable_name: str) -> None:
    """The Phase 1 completion checkpoint: a publisher-shaped adapter and a
    consumer-shaped adapter, each with an independent lifecycle, running
    concurrently, exchange one signed+encrypted event through the real
    bus — no shared state between them except the bus itself."""
    publisher = FakePublisherAdapter("webhook-listener-01")
    consumer = FakeConsumerAdapter("file-storage-01")

    # Startup (subscribe) must complete before publishing has a recipient
    # to encrypt for — the same ordering a real deployment would have
    # ("the consumer adapter is already running before anyone publishes
    # to it"), not a test-only synchronization trick.
    await consumer.start(subject=SUBJECT, durable_name=durable_name)
    try:
        # The actual proof: both adapters' ongoing work happens
        # concurrently via the event loop, not as sequential steps in one
        # script.
        await asyncio.gather(
            consumer.run_until(count=1, timeout=5),
            publisher.run(
                subject=SUBJECT,
                payload=b"end-to-end proof payload",
                event_type="FileWriteRequested",
            ),
        )

        assert len(consumer.received) == 1
        received = consumer.received[0]
        assert received.payload == b"end-to-end proof payload"
        assert received.details.event_type == "FileWriteRequested"
        assert received.details.source_service_id == "webhook-listener-01"
        # The signature was verified against the trusted service-identity
        # directory (Design.md §4.6, Decision #20) as part of fetch() —
        # if it hadn't verified, `received` would be empty, not present
        # with a bad signature silently accepted.
    finally:
        await consumer.stop()


async def test_fake_adapters_exchange_a_sequence_of_events(durable_name: str) -> None:
    """A closer-to-production shape than a single message: the consumer's
    loop processes several events as they arrive, from one publisher's
    connected session — the shape a real adapter takes as multiple
    business events happen during its lifetime (see `run_many`'s
    docstring for why this is one connection, not three)."""
    publisher = FakePublisherAdapter("webhook-listener-01")
    consumer = FakeConsumerAdapter("file-storage-01")

    await consumer.start(subject=SUBJECT, durable_name=durable_name)
    try:
        await asyncio.gather(
            consumer.run_until(count=3, timeout=5),
            publisher.run_many(
                subject=SUBJECT,
                items=[(f"event-{i}".encode(), "FileWriteRequested") for i in range(3)],
            ),
        )

        assert len(consumer.received) == 3
        assert {r.payload for r in consumer.received} == {b"event-0", b"event-1", b"event-2"}
    finally:
        await consumer.stop()

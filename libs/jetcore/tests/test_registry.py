"""Tests for registry.py (Design.md §4.5) — Step B5.

Requires a live NATS server, bootstrapped per Steps A3-A6: `bash
infra/nats/up.sh` (or, if already up, just `docker compose up -d nats`).
Connects as `jetcore-admin` (full `$KV.service-directory.>` access — Step A3),
so these tests can exercise both the "own namespace" and "other adapter's
namespace" cases without hitting the per-adapter permission scoping tested
separately in Step A7.

Each test uses a unique subject (a fresh uuid4 suffix) so tests can't
interfere with each other or with anything left in the real bucket.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from jetcore.registry import IDENTITY_TTL_SECONDS, RecipientCache, RegistryClient
from nats.js import JetStreamContext

CREDS_PATH = "infra/nats/operator/creds/jetcore-admin.creds"
NATS_URL = "nats://localhost:4222"


@pytest.fixture
async def jetstream() -> AsyncGenerator[JetStreamContext]:
    nc = await nats.connect(NATS_URL, user_credentials=CREDS_PATH)
    try:
        yield nc.jetstream()
    finally:
        await nc.close()


@pytest.fixture
async def registry(jetstream: JetStreamContext) -> RegistryClient:
    return await RegistryClient.connect(jetstream)


@pytest.fixture
def subject() -> str:
    """A fresh, never-used-before subject per test."""
    return f"events.test.RegistryTest{uuid.uuid4().hex[:8]}"


@pytest.fixture
def service_id() -> str:
    """A fresh, never-used-before service id per test — for the
    service-identity tests, which key entries by service id alone (a long,
    90-day per-key TTL, Design.md §16 Step N3 — not the 60s bucket-wide TTL
    `subject` above relies on, so unlike that fixture, isolation matters
    for the whole test run, not just concurrent tests)."""
    return f"test-service-{uuid.uuid4().hex[:8]}"


async def test_register_then_lookup(registry: RegistryClient, subject: str) -> None:
    await registry.register(
        subject=subject,
        service_id="svc-a",
        adapter_type="test-adapter",
        encryption_public_key="pub-a",
    )

    recipients = await registry.lookup_recipients(subject)

    assert recipients == ["pub-a"]


async def test_lookup_empty_subject_returns_empty_list(
    registry: RegistryClient, subject: str
) -> None:
    """No one has ever registered for this (freshly generated) subject."""
    assert await registry.lookup_recipients(subject) == []


async def test_lookup_does_not_include_other_subjects(
    registry: RegistryClient, subject: str
) -> None:
    other_subject = f"{subject}-unrelated"
    await registry.register(
        subject=other_subject,
        service_id="svc-a",
        adapter_type="test-adapter",
        encryption_public_key="pub-a",
    )

    assert await registry.lookup_recipients(subject) == []


async def test_multiple_recipients_for_same_subject(registry: RegistryClient, subject: str) -> None:
    await registry.register(
        subject=subject, service_id="svc-a", adapter_type="t", encryption_public_key="pub-a"
    )
    await registry.register(
        subject=subject, service_id="svc-b", adapter_type="t", encryption_public_key="pub-b"
    )

    assert sorted(await registry.lookup_recipients(subject)) == ["pub-a", "pub-b"]


async def test_deregister_removes_from_lookup(registry: RegistryClient, subject: str) -> None:
    await registry.register(
        subject=subject, service_id="svc-a", adapter_type="t", encryption_public_key="pub-a"
    )
    assert await registry.lookup_recipients(subject) == ["pub-a"]

    await registry.deregister(subject=subject, service_id="svc-a")

    assert await registry.lookup_recipients(subject) == []


async def test_heartbeat_registers_repeatedly(registry: RegistryClient, subject: str) -> None:
    """Doesn't wait out the real 60s bucket TTL (that mechanism was already
    proven empirically in Step A5/A7) — just confirms the heartbeat task
    actually drives repeated register() calls on schedule, and stops
    cleanly on cancellation."""
    task = await registry.heartbeat(
        subject=subject,
        service_id="svc-a",
        adapter_type="t",
        encryption_public_key="pub-a",
        interval_seconds=0.2,
    )
    try:
        await asyncio.sleep(0.7)
        assert await registry.lookup_recipients(subject) == ["pub-a"]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_cache_sees_pre_existing_recipient_on_start(
    jetstream: JetStreamContext, registry: RegistryClient, subject: str
) -> None:
    await registry.register(
        subject=subject, service_id="svc-a", adapter_type="t", encryption_public_key="pub-a"
    )

    kv = await jetstream.key_value("service-directory")
    cache = RecipientCache(kv, subject)
    await cache.start()
    try:
        assert cache.current() == ["pub-a"]
    finally:
        await cache.stop()


async def test_cache_picks_up_new_registration_after_start(
    jetstream: JetStreamContext, registry: RegistryClient, subject: str
) -> None:
    kv = await jetstream.key_value("service-directory")
    cache = RecipientCache(kv, subject)
    await cache.start()
    try:
        assert cache.current() == []

        await registry.register(
            subject=subject, service_id="svc-a", adapter_type="t", encryption_public_key="pub-a"
        )

        async def _eventually_sees_it() -> None:
            while cache.current() != ["pub-a"]:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_eventually_sees_it(), timeout=5)
    finally:
        await cache.stop()


async def test_cache_removes_entry_on_deregister(
    jetstream: JetStreamContext, registry: RegistryClient, subject: str
) -> None:
    await registry.register(
        subject=subject, service_id="svc-a", adapter_type="t", encryption_public_key="pub-a"
    )
    kv = await jetstream.key_value("service-directory")
    cache = RecipientCache(kv, subject)
    await cache.start()
    try:
        assert cache.current() == ["pub-a"]

        await registry.deregister(subject=subject, service_id="svc-a")

        async def _eventually_empty() -> None:
            while cache.current() != []:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_eventually_empty(), timeout=5)
    finally:
        await cache.stop()


async def test_cache_ignores_unrelated_subjects(
    jetstream: JetStreamContext, registry: RegistryClient, subject: str
) -> None:
    other_subject = f"{subject}-unrelated"
    kv = await jetstream.key_value("service-directory")
    cache = RecipientCache(kv, subject)
    await cache.start()
    try:
        await registry.register(
            subject=other_subject,
            service_id="svc-a",
            adapter_type="t",
            encryption_public_key="pub-a",
        )
        # Give a moment for a (wrongly) matching update to arrive, if the
        # watch's server-side filtering were broken.
        await asyncio.sleep(0.3)

        assert cache.current() == []
    finally:
        await cache.stop()


async def test_cache_stop_cancels_cleanly(jetstream: JetStreamContext, subject: str) -> None:
    kv = await jetstream.key_value("service-directory")
    cache = RecipientCache(kv, subject)
    await cache.start()

    await cache.stop()

    assert cache._task is None


# --- service-identity (Design.md §9 item #4) ---------------------------


async def test_register_identity_then_lookup(registry: RegistryClient, service_id: str) -> None:
    await registry.register_identity(
        service_id=service_id, adapter_type="test-adapter", signing_public_key="signing-pub-a"
    )

    assert await registry.lookup_signing_key(service_id) == "signing-pub-a"


async def test_lookup_unknown_identity_returns_none(
    registry: RegistryClient, service_id: str
) -> None:
    """Never registered — should be a clean None, not an exception."""
    assert await registry.lookup_signing_key(service_id) is None


async def test_register_identity_overwrites_previous_key(
    registry: RegistryClient, service_id: str
) -> None:
    """Re-registering (e.g. on every BusClient.connect()) replaces the
    previous key as far as lookup_signing_key() (a plain KeyValue.get(),
    always the latest revision) is concerned — independent of the
    bucket's own history depth (5, Design.md §16 Step N4), which affects
    what `nats kv history` can show after the fact, not what a normal
    lookup returns."""
    await registry.register_identity(
        service_id=service_id, adapter_type="test-adapter", signing_public_key="old-key"
    )
    await registry.register_identity(
        service_id=service_id, adapter_type="test-adapter", signing_public_key="new-key"
    )

    assert await registry.lookup_signing_key(service_id) == "new-key"


async def test_register_identity_sets_the_configured_ttl(
    jetstream: JetStreamContext, registry: RegistryClient, service_id: str
) -> None:
    """Design.md §16 Step N3 (§9 item #11) — a real, direct proof the
    write actually carries a per-key TTL, not just that register/lookup
    round-trips (the test above already covers that). Reads the raw
    stream message's own Nats-TTL header rather than trusting the field
    exists in registry.py's own code."""
    await registry.register_identity(
        service_id=service_id, adapter_type="test-adapter", signing_public_key="pub-a"
    )

    msg = await jetstream.get_last_msg("KV_service-identity", f"$KV.service-identity.{service_id}")

    assert msg.headers is not None
    assert msg.headers["Nats-TTL"] == str(IDENTITY_TTL_SECONDS)

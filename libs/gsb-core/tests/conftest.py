"""Shared fixtures for gsb-core's live-NATS integration tests (Steps B5-B7).
Auto-discovered by pytest for every test module in this directory."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from _helpers import CREDS_DIR, NATS_URL, SUBJECT
from nats.js.errors import NoKeysError


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    """Purges the EVENTS stream AND any service-directory registrations
    for SUBJECT before each test, via gsb-admin — real adapter identities
    can't be given arbitrary unique test subjects (only their real, fixed
    ones), so tests get clean state each time instead.

    The KV half of this was added after a real, initially-confusing
    failure in Step B6: a "no recipients" test failed intermittently
    depending on what ran before it — `subscribe()` in an earlier test
    starts a real heartbeat that `BusClient.close()` cancels but never
    deregisters, so the KV entry lingers until its 60s TTL naturally
    expires. Purging only the message stream (registry.py's own concern
    is untouched by that) wasn't enough.

    service-identity (Step B6's later addition) isn't purged here — its
    entries have no TTL, but `BusClient.connect()` overwrites its own
    entry every call, so there's no analogous staleness risk to guard
    against.
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

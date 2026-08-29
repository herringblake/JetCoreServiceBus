"""Shared fixtures for webhook_listener's live-NATS integration tests
(Design.md §12 Step D5). Mirrors libs/jetcore/tests/conftest.py's and
file_storage_adapter/tests/conftest.py's _clean_state fixture and its
rationale exactly — re-implemented locally rather than imported, for the
same self-containment reason _webhook_listener_helpers.py explains.

Applies to every test in this directory, including Step D3's
FakeBusClient-only tests — they don't strictly need it, but paying a
trivial extra NATS round trip is simpler than splitting this directory's
fixture scope, and file_storage_adapter's own pure tests already accept
the same trade-off (Step C4)."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from nats.js.errors import NoKeysError

NATS_URL = "nats://localhost:4222"
CREDS_DIR = "infra/nats/operator/creds"
WRITE_REQUESTED_SUBJECT = "events.files.FileWriteRequested"


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/jetcore-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    kv = await js.key_value("service-directory")
    try:
        for key in await kv.keys(filters=[f"{WRITE_REQUESTED_SUBJECT}."]):
            await kv.delete(key)
    except NoKeysError:
        pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

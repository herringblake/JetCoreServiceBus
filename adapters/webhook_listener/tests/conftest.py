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


def _is_test_identity(service_id: str) -> bool:
    """Defects.md Defect 2: this fixture's own KV cleanup below used to
    blanket-delete every registered recipient for FileWriteRequested —
    including the REAL, live file-storage-01 container's own registration,
    not just this test suite's own ephemeral ones. Confirmed by direct
    reproduction (polling a real container's KV entry once a second during
    a normal pytest run) that this left real containers unregistered for
    up to 59 consecutive seconds, each occurrence producing a real,
    permanently-undecryptable message on their end. Only ever clean up
    entries this project's own test identities own — the `-test` suffix
    (Defect 1's dedicated twins) or the `test-` prefix (test-observer-01) —
    never a bare real adapter serviceId."""
    return service_id.endswith("-test") or service_id.startswith("test-")


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/jetcore-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    kv = await js.key_value("service-directory")
    try:
        for key in await kv.keys(filters=[f"{WRITE_REQUESTED_SUBJECT}."]):
            if _is_test_identity(key.removeprefix(f"{WRITE_REQUESTED_SUBJECT}.")):
                await kv.delete(key)
    except NoKeysError:
        pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

"""Shared fixtures for file_storage_adapter's live-NATS integration tests
(Design.md §12 Step C4 onward). Mirrors libs/jetcore/tests/conftest.py's
_clean_state fixture and its rationale exactly — same real bug documented
there applies here too (subscribe()'s heartbeat outlives close() until its
60s TTL expires), just re-implemented locally rather than imported, for
the same self-containment reason _helpers.py explains."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from _file_storage_helpers import (
    CREATE_COMPLETED_SUBJECT,
    CREDS_DIR,
    NATS_URL,
    WRITE_COMPLETED_SUBJECT,
    WRITE_REQUESTED_SUBJECT,
)
from nats.js.errors import NoKeysError, NotFoundError

# The one fixed (non-random) durable consumer name in this whole test
# suite — __main__.run() (Step C6) deliberately uses a stable
# "{service_id}-write-handler" name, unlike every other test here's
# randomized `durable_name` fixture, because that's what a real
# deployment needs (resume its own cursor across restarts). Step C7's
# entrypoint test reuses that same real name on purpose, which means it
# — and only it — needs this consumer actively deleted between runs, not
# just the stream purged: a leftover consumer from an earlier run
# attaches to (doesn't replace) an existing one on `pull_subscribe`.
FIXED_ENTRYPOINT_DURABLE_NAME = "file-storage-01-write-handler"


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/jetcore-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    try:
        await js.delete_consumer("EVENTS", FIXED_ENTRYPOINT_DURABLE_NAME)
    except NotFoundError:
        pass
    kv = await js.key_value("service-directory")
    for subject in (WRITE_REQUESTED_SUBJECT, CREATE_COMPLETED_SUBJECT, WRITE_COMPLETED_SUBJECT):
        try:
            for key in await kv.keys(filters=[f"{subject}."]):
                await kv.delete(key)
        except NoKeysError:
            pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

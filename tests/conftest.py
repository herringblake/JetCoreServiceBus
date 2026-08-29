"""Shared fixtures for the cross-adapter end-to-end test (Design.md §12
Step E4). Mirrors the same _clean_state pattern used throughout
libs/jetcore/tests and adapters/*/tests — re-implemented locally rather
than imported, for the self-containment reasons those already explain.

Also deletes the File Storage Adapter entrypoint's one FIXED (non-random)
durable consumer name, same reasoning as
adapters/file_storage_adapter/tests/conftest.py: a real deployment resumes
its own cursor across restarts, so this test (which drives that real
entrypoint) reuses the real name on purpose — which means a leftover
consumer from an earlier run needs explicit cleanup, not just a stream
purge.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from nats.js.errors import NoKeysError, NotFoundError

NATS_URL = "nats://localhost:4222"
CREDS_DIR = "infra/nats/operator/creds"
WRITE_REQUESTED_SUBJECT = "events.files.FileWriteRequested"
CREATE_COMPLETED_SUBJECT = "events.files.FileCreateCompleted"
WRITE_COMPLETED_SUBJECT = "events.files.FileWriteCompleted"
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

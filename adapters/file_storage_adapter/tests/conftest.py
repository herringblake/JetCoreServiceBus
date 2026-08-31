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
    DELETE_COMPLETED_SUBJECT,
    DELETE_REQUESTED_SUBJECT,
    LIST_COMPLETED_SUBJECT,
    LIST_REQUESTED_SUBJECT,
    NATS_URL,
    OPERATION_FAILED_SUBJECT,
    READ_COMPLETED_SUBJECT,
    READ_REQUESTED_SUBJECT,
    WRITE_COMPLETED_SUBJECT,
    WRITE_REQUESTED_SUBJECT,
)
from nats.js.errors import NoKeysError, NotFoundError

_ALL_SUBJECTS = (
    WRITE_REQUESTED_SUBJECT,
    CREATE_COMPLETED_SUBJECT,
    WRITE_COMPLETED_SUBJECT,
    READ_REQUESTED_SUBJECT,
    READ_COMPLETED_SUBJECT,
    LIST_REQUESTED_SUBJECT,
    LIST_COMPLETED_SUBJECT,
    DELETE_REQUESTED_SUBJECT,
    DELETE_COMPLETED_SUBJECT,
    OPERATION_FAILED_SUBJECT,
)

# The fixed (non-random) durable consumer names in this whole test suite —
# __main__.run() (Steps C6/F5) deliberately uses stable
# "{service_id}-{command}-handler" names, unlike every other test here's
# randomized `durable_name` fixture, because that's what a real deployment
# needs (resume its own cursor across restarts). Step C7's entrypoint test
# reuses these same names on purpose, which means they — and only they —
# need active deletion between runs, not just a stream purge: a leftover
# consumer from an earlier run attaches to (doesn't replace) an existing
# one on `pull_subscribe`.
#
# Named after file-storage-01-TEST, not file-storage-01 itself (Defects.md
# Defect 1) — Step C7's entrypoint test drives the real __main__.run()
# under a dedicated test-only identity (infra/nats/adapter_identities.yaml)
# so its own durable consumers can never collide with the real, separately
# deployed file-storage-01 container's identically-shaped ones.
FIXED_ENTRYPOINT_DURABLE_NAMES = (
    "file-storage-01-test-write-handler",
    "file-storage-01-test-read-handler",
    "file-storage-01-test-list-handler",
    "file-storage-01-test-delete-handler",
)


def _is_test_identity(service_id: str) -> bool:
    """Defects.md Defect 2: this fixture's own KV cleanup below used to
    blanket-delete every registered recipient for a shared subject —
    including the REAL, live file-storage-01/webhook-listener-01
    containers' own registrations, not just this test suite's own
    ephemeral ones. Confirmed by direct reproduction (polling a real
    container's KV entry once a second during a normal pytest run) that
    this left real containers unregistered for up to 59 consecutive
    seconds, each occurrence producing a real, permanently-undecryptable
    message on their end. Only ever clean up entries this project's own
    test identities own — the `-test` suffix (Defect 1's dedicated twins)
    or the `test-` prefix (test-observer-01) — never a bare real adapter
    serviceId."""
    return service_id.endswith("-test") or service_id.startswith("test-")


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/jetcore-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    for name in FIXED_ENTRYPOINT_DURABLE_NAMES:
        try:
            await js.delete_consumer("EVENTS", name)
        except NotFoundError:
            pass
    kv = await js.key_value("service-directory")
    for subject in _ALL_SUBJECTS:
        try:
            for key in await kv.keys(filters=[f"{subject}."]):
                if _is_test_identity(key.removeprefix(f"{subject}.")):
                    await kv.delete(key)
        except NoKeysError:
            pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

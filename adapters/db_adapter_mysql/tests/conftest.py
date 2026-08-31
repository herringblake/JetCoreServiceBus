"""Shared fixtures for db_adapter_mysql's live-NATS/live-MySQL
integration tests (Design.md §13 Step J4 onward). Mirrors the same
_clean_state pattern used throughout this project's other test suites —
re-implemented locally rather than imported, for the self-containment
reasons _db_adapter_mysql_helpers.py explains. Extended here with the
orders table truncate every other adapter's conftest.py hasn't needed,
since this is the first track with a second real datastore alongside
NATS."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from _db_adapter_mysql_helpers import (
    CREDS_DIR,
    NATS_URL,
    ORDER_CREATED_SUBJECT,
    ORDER_PERSISTED_SUBJECT,
    root_engine,
)
from nats.js.errors import NoKeysError, NotFoundError
from sqlalchemy import text

_ALL_SUBJECTS = (ORDER_CREATED_SUBJECT, ORDER_PERSISTED_SUBJECT)

# test_db_adapter_mysql_entrypoint.py drives the real __main__.run() under
# db-adapter-mysql-01-TEST (Defects.md Defect 1), not db-adapter-mysql-01
# itself — its write handler's fixed (non-random) durable name follows
# suit. Same "needs active deletion between runs, not just a stream
# purge" reasoning as every other adapter's own FIXED_ENTRYPOINT_DURABLE_
# NAME(S) — a leftover consumer from an earlier run attaches to (doesn't
# replace) an existing one on `pull_subscribe`.
FIXED_ENTRYPOINT_DURABLE_NAME = "db-adapter-mysql-01-test-write-handler"


def _is_test_identity(service_id: str) -> bool:
    """Defects.md Defect 2: this fixture's own KV cleanup below used to
    blanket-delete every registered recipient for a shared subject like
    events.orders.OrderCreated — including the REAL, live db-adapter-mysql-01/
    http-adapter-01/webhook-sender-01 containers' own registrations, not
    just this test suite's own ephemeral ones. Confirmed by direct
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
    try:
        await js.delete_consumer("EVENTS", FIXED_ENTRYPOINT_DURABLE_NAME)
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

    engine = root_engine()
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM orders"))
    await engine.dispose()

    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

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

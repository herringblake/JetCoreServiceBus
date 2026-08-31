"""Shared fixtures for this project's cross-adapter end-to-end tests
(Design.md §12 Step E4, §13 Step K4). Mirrors the same _clean_state
pattern used throughout libs/jetcore/tests and adapters/*/tests —
re-implemented locally rather than imported, for the self-containment
reasons those already explain.

Also deletes each real entrypoint's FIXED (non-random) durable consumer
names, same reasoning as adapters/file_storage_adapter/tests/conftest.py:
a real deployment resumes its own cursor across restarts, so these tests
(which drive real entrypoints, not fresh randomized durables) reuse the
real names on purpose — which means leftover consumers from an earlier
run need explicit cleanup, not just a stream purge. `file_storage_adapter
.__main__.run()` (Steps C6/F5) always starts all four command handlers
together, so all four of its fixed names need cleaning here even though
Step E4's own test only exercises the write path.

Step K4 extends this fixture with a second real datastore's own cleanup
(MySQL's `orders` table, the same `DELETE FROM orders` pattern
adapters/db_adapter_mysql/tests/conftest.py already established) — the
first time this root conftest.py has needed anything beyond NATS.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from nats.js.errors import NoKeysError, NotFoundError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

NATS_URL = "nats://localhost:4222"
CREDS_DIR = "infra/nats/operator/creds"
WRITE_REQUESTED_SUBJECT = "events.files.FileWriteRequested"
CREATE_COMPLETED_SUBJECT = "events.files.FileCreateCompleted"
WRITE_COMPLETED_SUBJECT = "events.files.FileWriteCompleted"
READ_REQUESTED_SUBJECT = "events.files.FileReadRequested"
READ_COMPLETED_SUBJECT = "events.files.FileReadCompleted"
LIST_REQUESTED_SUBJECT = "events.files.FileListRequested"
LIST_COMPLETED_SUBJECT = "events.files.FileListCompleted"
DELETE_REQUESTED_SUBJECT = "events.files.FileDeleteRequested"
DELETE_COMPLETED_SUBJECT = "events.files.FileDeleteCompleted"
OPERATION_FAILED_SUBJECT = "events.files.FileOperationFailed"
ORDER_CREATED_SUBJECT = "events.orders.OrderCreated"
ORDER_PERSISTED_SUBJECT = "events.orders.OrderPersisted"
FIXED_ENTRYPOINT_DURABLE_NAMES = (
    "file-storage-01-write-handler",
    "file-storage-01-read-handler",
    "file-storage-01-list-handler",
    "file-storage-01-delete-handler",
    "db-adapter-mysql-01-write-handler",
    # rest_api_service/app.py's own lifespan (Design.md §13 Step I5) builds
    # its durable name from the subject string directly, the same
    # dot-replacement convention Step G4 introduced.
    "rest-api-service-01-events_orders_OrderPersisted",
)


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
    for subject in (
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
        ORDER_CREATED_SUBJECT,
        ORDER_PERSISTED_SUBJECT,
    ):
        try:
            for key in await kv.keys(filters=[f"{subject}."]):
                await kv.delete(key)
        except NoKeysError:
            pass
    await nc.close()

    engine = create_async_engine(
        "mysql+asyncmy://root:dev-only-change-me@localhost:3306/jetcore", pool_pre_ping=True
    )
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM orders"))
    await engine.dispose()

    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

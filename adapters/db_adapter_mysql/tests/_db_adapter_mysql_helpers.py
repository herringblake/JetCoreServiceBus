"""Test-only helpers for db_adapter_mysql's live-NATS/live-MySQL
integration tests. A local copy of the same connect()/wait_until_cache_has()
pattern used throughout this project's other test suites — not a
cross-package import, for the self-containment reasons already explained
in libs/jetcore/tests/_helpers.py and its siblings across the other
adapters."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings
from jetcore.crypto import generate_encryption_keypair, generate_signing_keypair
from sqlalchemy.ext.asyncio import create_async_engine

if TYPE_CHECKING:
    from jetcore.registry import RecipientCache
    from sqlalchemy.ext.asyncio import AsyncEngine

NATS_URL = "nats://localhost:4222"
CREDS_DIR = "infra/nats/operator/creds"

ORDER_CREATED_SUBJECT = "events.orders.OrderCreated"
ORDER_PERSISTED_SUBJECT = "events.orders.OrderPersisted"
ROW_CHANGED_SUBJECT = "events.db.orders.RowChanged"

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "jetcore"


def settings(service_id: str) -> AdapterSettings:
    return AdapterSettings(
        service_id=service_id,
        nats_url=NATS_URL,
        nats_creds_path=f"{CREDS_DIR}/{service_id}.creds",
    )


async def connect(service_id: str) -> BusClient:
    signing = generate_signing_keypair()
    encryption = generate_encryption_keypair()
    return await BusClient.connect(
        settings(service_id),
        adapter_type="test-adapter",
        encryption_keypair=encryption,
        signing_seed=signing.seed,
        signing_public_key=signing.public_key,
    )


async def wait_until_cache_has(cache: RecipientCache, count: int, *, timeout: float = 3.0) -> None:
    async def _poll() -> None:
        while len(cache.current()) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)


def root_engine() -> AsyncEngine:
    """A root-privileged connection for test setup/teardown/direct-SQL
    verification — NOT what the adapter itself uses (that's the
    write-path-scoped `jetcore_write` user, db.py's `create_write_engine`,
    Step J1's least-privilege split). Tests need broader access than any
    single adapter identity has by design."""
    return create_async_engine(
        f"mysql+asyncmy://root:dev-only-change-me@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
        pool_pre_ping=True,
    )


def write_engine() -> AsyncEngine:
    """The real `jetcore_write` identity (Step J1), for tests that want to
    exercise the write handler's own SQLAlchemy engine directly rather
    than reusing write_handler.py's create_write_engine (kept as a
    separate literal build here so a bug in db.py's URL construction
    can't accidentally hide itself from these tests)."""
    return create_async_engine(
        f"mysql+asyncmy://jetcore_write:jetcore-write-dev-only@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
        pool_pre_ping=True,
    )

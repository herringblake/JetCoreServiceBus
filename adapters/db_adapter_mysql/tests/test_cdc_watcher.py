"""Integration tests for cdc_watcher.py (Design.md §13 Step J5) — against
the real Phase 1 NATS stack AND a real MySQL binlog, no mocking of either.

A fresh CdcWatcher (Step J5's own module docstring: no persisted read
position across restarts) starts from the earliest binlog MySQL still
retains, not "now" — confirmed by testing. Every test here therefore
generates a UNIQUE order_id (uuid4) and polls the observer until it finds
the RowChanged carrying THAT order_id, rather than assuming the very next
event received is its own — real backlog noise from every earlier test
run in this session is expected and tolerated, not something to
suppress.

Uses test-observer-01 (extended with events.db.orders.RowChanged
subscribe permission in this step) — db-adapter-mysql-01 is the only
identity with publish permission on this subject, and a real adapter
deliberately never subscribes to its own output (the same reasoning
documented for every other adapter's result events).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import pymysql
import pytest
from _db_adapter_mysql_helpers import (
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_PORT,
    ROW_CHANGED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from db_adapter_mysql.cdc_watcher import CdcWatcher
from db_adapter_mysql.settings import DbAdapterSettings
from jetcore.bus_client import BusClient


def _direct_sql_connection() -> pymysql.connections.Connection:
    """A raw connection bypassing the bus AND SQLAlchemy entirely — the
    one check that actually proves the CDC path tails the real binlog
    rather than just echoing the write handler's own writes (Design.md
    §13 Step J7/K3's own framing)."""
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user="root",
        password="dev-only-change-me",
        database=MYSQL_DATABASE,
        autocommit=True,
    )


async def _find_row_changed(
    observer: BusClient, durable_name: str, order_id: str, *, operation: str, timeout: float = 15.0
) -> dict[str, Any]:
    async def _poll() -> dict[str, Any]:
        while True:
            received = await observer.fetch(durable_name, batch=10, timeout=2.0)
            for event in received:
                data = json.loads(event.payload)
                if data["operation"] != operation:
                    continue
                if data.get("row", {}).get("order_id") == order_id:
                    return data  # type: ignore[no-any-return]
                if data.get("previousRow", {}).get("order_id") == order_id:
                    return data  # type: ignore[no-any-return]

    return await asyncio.wait_for(_poll(), timeout=timeout)


@pytest.fixture
def db_settings() -> DbAdapterSettings:
    return DbAdapterSettings(
        service_id="db-adapter-mysql-01",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/db-adapter-mysql-01.creds",
        mysql_host=MYSQL_HOST,  # "localhost" — tests run on the host, not
        # inside the Compose network, unlike a
        # real deployed adapter (which would use
        # the "mysql" service-name default)
        mysql_write_user="jetcore_write",
        mysql_write_password="jetcore-write-dev-only",
        mysql_cdc_user="jetcore_cdc",
        mysql_cdc_password="jetcore-cdc-dev-only",
    )


async def test_cdc_detects_a_direct_sql_insert_bypassing_the_bus(
    durable_name: str, db_settings: DbAdapterSettings
) -> None:
    observer = await connect("test-observer-01")
    adapter_client = await connect("db-adapter-mysql-01")
    watcher = CdcWatcher(adapter_client, db_settings)
    watcher_task = asyncio.create_task(watcher.run())
    try:
        observer_durable = await observer.subscribe(ROW_CHANGED_SUBJECT, durable_name=durable_name)
        await wait_until_cache_has(await adapter_client._cache_for(ROW_CHANGED_SUBJECT), 1)

        order_id = f"cdc-direct-{uuid.uuid4().hex[:8]}"
        conn = _direct_sql_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO orders (order_id, item, quantity) VALUES (%s, %s, %s)",
                (order_id, "direct-sql-item", 7),
            )
        finally:
            conn.close()

        data = await _find_row_changed(observer, observer_durable, order_id, operation="insert")
        assert data["table"] == "orders"
        assert data["row"]["item"] == "direct-sql-item"
        assert data["row"]["quantity"] == 7
        assert "previousRow" not in data
    finally:
        watcher.request_shutdown()
        await asyncio.wait_for(watcher_task, timeout=5)
        await observer.close()
        await adapter_client.close()


async def test_cdc_detects_a_bus_originated_update(
    durable_name: str, db_settings: DbAdapterSettings
) -> None:
    observer = await connect("test-observer-01")
    adapter_client = await connect("db-adapter-mysql-01")
    watcher = CdcWatcher(adapter_client, db_settings)
    watcher_task = asyncio.create_task(watcher.run())
    try:
        observer_durable = await observer.subscribe(ROW_CHANGED_SUBJECT, durable_name=durable_name)
        await wait_until_cache_has(await adapter_client._cache_for(ROW_CHANGED_SUBJECT), 1)

        order_id = f"cdc-update-{uuid.uuid4().hex[:8]}"
        conn = _direct_sql_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO orders (order_id, item, quantity) VALUES (%s, %s, %s)",
                (order_id, "before-item", 1),
            )
            await _find_row_changed(observer, observer_durable, order_id, operation="insert")

            cur.execute(
                "UPDATE orders SET item = %s, quantity = %s WHERE order_id = %s",
                ("after-item", 9, order_id),
            )
        finally:
            conn.close()

        data = await _find_row_changed(observer, observer_durable, order_id, operation="update")
        assert data["row"]["item"] == "after-item"
        assert data["row"]["quantity"] == 9
        assert data["previousRow"]["item"] == "before-item"
        assert data["previousRow"]["quantity"] == 1
    finally:
        watcher.request_shutdown()
        await asyncio.wait_for(watcher_task, timeout=5)
        await observer.close()
        await adapter_client.close()


async def test_cdc_detects_a_direct_sql_delete(
    durable_name: str, db_settings: DbAdapterSettings
) -> None:
    observer = await connect("test-observer-01")
    adapter_client = await connect("db-adapter-mysql-01")
    watcher = CdcWatcher(adapter_client, db_settings)
    watcher_task = asyncio.create_task(watcher.run())
    try:
        observer_durable = await observer.subscribe(ROW_CHANGED_SUBJECT, durable_name=durable_name)
        await wait_until_cache_has(await adapter_client._cache_for(ROW_CHANGED_SUBJECT), 1)

        order_id = f"cdc-delete-{uuid.uuid4().hex[:8]}"
        conn = _direct_sql_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO orders (order_id, item, quantity) VALUES (%s, %s, %s)",
                (order_id, "to-be-deleted", 3),
            )
            await _find_row_changed(observer, observer_durable, order_id, operation="insert")

            cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        finally:
            conn.close()

        data = await _find_row_changed(observer, observer_durable, order_id, operation="delete")
        assert data["row"]["item"] == "to-be-deleted"
        assert "previousRow" not in data
    finally:
        watcher.request_shutdown()
        await asyncio.wait_for(watcher_task, timeout=5)
        await observer.close()
        await adapter_client.close()

"""Integration test for the real entrypoint wiring (Design.md §13 Step
J6/J7) — drives db_adapter_mysql.__main__.run() directly: a real
BusClient (via connect_as_adapter), a real MySQL, a real shutdown
handshake. Proves the write path and CDC read path actually work
TOGETHER under the real asyncio.gather, not just individually (each
already covered by test_write_handler.py/test_cdc_watcher.py) — one
OrderCreated should produce BOTH a real OrderPersisted (write path) AND a
real RowChanged (CDC path noticing the very same upsert), from one
concurrently-running process.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import db_adapter_mysql.__main__ as entrypoint
from _db_adapter_mysql_helpers import (
    ORDER_CREATED_SUBJECT,
    ORDER_PERSISTED_SUBJECT,
    ROW_CHANGED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from db_adapter_mysql.settings import DbAdapterSettings
from jetcore.bus_client import BusClient


async def test_real_entrypoint_processes_write_and_cdc_together_and_shuts_down_cleanly(
    durable_name: str,
) -> None:
    settings = DbAdapterSettings(
        service_id="db-adapter-mysql-01",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/db-adapter-mysql-01.creds",
        mysql_host="localhost",
        mysql_write_user="jetcore_write",
        mysql_write_password="jetcore-write-dev-only",
        mysql_cdc_user="jetcore_cdc",
        mysql_cdc_password="jetcore-cdc-dev-only",
    )
    adapter_client = await BusClient.connect_as_adapter(
        settings, adapter_type="database-adapter-mysql"
    )
    publisher = await connect("rest-api-service-01")
    observer = await connect("test-observer-01")
    shutdown = asyncio.Event()
    run_task: asyncio.Task[None] | None = None
    try:
        persisted_durable = await publisher.subscribe(
            ORDER_PERSISTED_SUBJECT, durable_name=f"{durable_name}-persisted"
        )
        row_changed_durable = await observer.subscribe(
            ROW_CHANGED_SUBJECT, durable_name=f"{durable_name}-rowchanged"
        )

        run_task = asyncio.create_task(
            entrypoint.run(adapter_client, settings=settings, shutdown=shutdown)
        )
        # The write handler's own subscribe() (inside run(), not yet
        # called at this point) is what actually registers
        # db-adapter-mysql-01 as a recipient for OrderCreated — this wait
        # has to come AFTER run_task starts, the same ordering mistake
        # already found and fixed in Step I7's own entrypoint test.
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)
        await wait_until_cache_has(await adapter_client._cache_for(ORDER_PERSISTED_SUBJECT), 1)
        await wait_until_cache_has(await adapter_client._cache_for(ROW_CHANGED_SUBJECT), 1)

        await publisher.publish(
            ORDER_CREATED_SUBJECT,
            b'{"orderId": "j6-entrypoint-proof", "item": "gadget", "quantity": 4}',
            event_type="OrderCreated",
        )

        [persisted] = await publisher.fetch(persisted_durable, timeout=5)
        assert persisted.details.event_type == "OrderPersisted"
        persisted_data = json.loads(persisted.payload)
        assert persisted_data["orderId"] == "j6-entrypoint-proof"

        async def _find_matching_row_changed() -> dict[str, Any]:
            while True:
                received = await observer.fetch(row_changed_durable, batch=10, timeout=2.0)
                for event in received:
                    data = json.loads(event.payload)
                    if data.get("row", {}).get("order_id") == "j6-entrypoint-proof":
                        return data  # type: ignore[no-any-return]

        row_changed = await asyncio.wait_for(_find_matching_row_changed(), timeout=10)
        assert row_changed["operation"] == "insert"
        assert row_changed["row"]["item"] == "gadget"

        shutdown.set()
        await asyncio.wait_for(run_task, timeout=5)
        run_task = None
    finally:
        if run_task is not None:
            run_task.cancel()
        await adapter_client.close()
        await publisher.close()
        await observer.close()

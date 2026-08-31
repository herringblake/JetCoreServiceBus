"""End-to-end test for the REST API Service <-> Database Adapter
synchronous round trip (Design.md §13 Step K4) — the genuinely new
"two real adapters, one waiting on the other's reply" pattern Phase 3
introduces (Decision #24/#26), the automated counterpart to Step K3's
manual smoke test (already run against the real `docker compose` stack:
a real `curl`-equivalent POST, real containers, a real MySQL).

Runs both adapters' REAL entrypoint wiring concurrently in-process —
`rest_api_service.app.create_app` via `TestClient` (Step I7/D5's pattern)
and `db_adapter_mysql.__main__.run` as a background task (Step J7/C7's
pattern) — rather than orchestrating real Docker containers from pytest;
K3 already proved that layer works, so this proves the two adapters' real
application code works *together* end to end, the same separation of
concerns Step E4 already established for the Phase 2 vertical slice.
File Storage's four commands and Webhook Sender/HTTP Adapter's one-way
relays are already covered at the adapter level (Tracks F/G/H) and via
K3's manual proof — this is the one pair that actually needed its own
dedicated cross-adapter test (Design.md §13 Track K's own sequencing
note): a real synchronous reply, not just a one-way relay.

Connects as db-adapter-mysql-01-TEST and rest-api-service-01-TEST,
dedicated test-only twins of the real adapters (Defects.md Defect 1) —
not db-adapter-mysql-01/rest-api-service-01 themselves, whose own live
docker-compose containers run throughout this whole dev session.
"""

from __future__ import annotations

import asyncio
import json

import db_adapter_mysql.__main__ as db_adapter_entrypoint
from db_adapter_mysql.settings import DbAdapterSettings
from fastapi.testclient import TestClient
from jetcore.bus_client import BusClient
from rest_api_service.app import create_app
from rest_api_service.settings import RestApiServiceSettings


async def test_real_post_gets_a_real_sync_reply_from_the_real_database_adapter() -> None:
    db_settings = DbAdapterSettings(
        service_id="db-adapter-mysql-01-test",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/db-adapter-mysql-01-test.creds",
        mysql_host="localhost",
        mysql_write_user="jetcore_write",
        mysql_write_password="jetcore-write-dev-only",
        mysql_cdc_user="jetcore_cdc",
        mysql_cdc_password="jetcore-cdc-dev-only",
    )
    db_client = await BusClient.connect_as_adapter(
        db_settings, adapter_type="database-adapter-mysql"
    )
    db_shutdown = asyncio.Event()
    db_task: asyncio.Task[None] | None = None
    try:
        # The real Database Adapter, running its real entrypoint loop
        # (write path + CDC watcher, Step J6), exactly as
        # `python -m db_adapter_mysql` does.
        db_task = asyncio.create_task(
            db_adapter_entrypoint.run(db_client, settings=db_settings, shutdown=db_shutdown)
        )
        await asyncio.sleep(0.3)  # let subscribe()/the CDC watcher's thread actually start

        # The real REST API Service, driven through a real HTTP request —
        # its own lifespan connects a real, independent BusClient (Step
        # I5/I6's connect_as_adapter), exactly as
        # `python -m rest_api_service` does.
        rest_settings = RestApiServiceSettings(
            service_id="rest-api-service-01-test",
            nats_url="nats://localhost:4222",
            nats_creds_path="infra/nats/operator/creds/rest-api-service-01-test.creds",
            default_reply_timeout_seconds=10.0,
        )
        app = create_app(rest_settings)
        body = json.dumps(
            {"orderId": "k4-end-to-end-proof", "item": "cross-adapter-widget", "quantity": 7}
        ).encode()
        with TestClient(app) as client:
            response = await asyncio.to_thread(client.post, "/api/orders?wait=8", content=body)

        assert response.status_code == 200
        data = response.json()
        assert data["orderId"] == "k4-end-to-end-proof"
        assert data["status"] == "persisted"

        db_shutdown.set()
        await asyncio.wait_for(db_task, timeout=5)
        db_task = None
    finally:
        if db_task is not None:
            db_task.cancel()
        await db_client.close()

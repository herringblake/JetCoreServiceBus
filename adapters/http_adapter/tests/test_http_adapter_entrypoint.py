"""Integration test for the real entrypoint wiring (Design.md §13 Step
H5) — drives http_adapter.__main__.run() directly: a real BusClient (via
connect_as_adapter), a real local HTTP server, a real shutdown handshake.
Mirrors webhook_sender's own entrypoint test (Step G5) exactly.

Connects as http-adapter-01-TEST, a dedicated test-only identity
(Defects.md Defect 1) — not http-adapter-01 itself, whose real
docker-compose container runs throughout this whole dev session and
would otherwise race this test for the service-identity/service-directory
KV entries.
"""

from __future__ import annotations

import asyncio

import http_adapter.__main__ as entrypoint
import httpx
from _http_adapter_helpers import (
    ORDER_CREATED_SUBJECT,
    REQUEST_COMPLETED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from _http_adapter_local_server import LocalHttpServer
from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings


async def test_real_entrypoint_calls_and_shuts_down_cleanly(durable_name: str) -> None:
    server = LocalHttpServer(status_code=200)
    settings = AdapterSettings(
        service_id="http-adapter-01-test",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/http-adapter-01-test.creds",
    )
    adapter_client = await BusClient.connect_as_adapter(settings, adapter_type="http-adapter")
    publisher = await connect("rest-api-service-01-test")
    observer = await connect("test-observer-01")
    shutdown = asyncio.Event()
    run_task: asyncio.Task[None] | None = None
    try:
        completed_durable = await observer.subscribe(
            REQUEST_COMPLETED_SUBJECT, durable_name=durable_name
        )
        await wait_until_cache_has(await adapter_client._cache_for(REQUEST_COMPLETED_SUBJECT), 1)

        async with httpx.AsyncClient() as http_client:
            run_task = asyncio.create_task(
                entrypoint.run(
                    adapter_client,
                    subjects=[ORDER_CREATED_SUBJECT],
                    target_base_url=server.url,
                    auth_token="entrypoint-token",
                    http_client=http_client,
                    service_id="http-adapter-01-test",
                    shutdown=shutdown,
                )
            )
            await asyncio.sleep(0.2)  # let subscribe() actually happen
            await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

            await publisher.publish(
                ORDER_CREATED_SUBJECT, b"H5 entrypoint proof", event_type="OrderCreated"
            )

            [result] = await observer.fetch(completed_durable, timeout=5)
            assert result.details.event_type == "RequestCompleted"
            assert result.details.source_service_id == "http-adapter-01-test"

            assert len(server.received) == 1
            assert server.received[0].body == b"H5 entrypoint proof"
            assert server.received[0].headers["Authorization"] == "Bearer entrypoint-token"

            shutdown.set()
            await asyncio.wait_for(run_task, timeout=5)
            run_task = None
    finally:
        if run_task is not None:
            run_task.cancel()
        server.shutdown()
        await adapter_client.close()
        await publisher.close()
        await observer.close()

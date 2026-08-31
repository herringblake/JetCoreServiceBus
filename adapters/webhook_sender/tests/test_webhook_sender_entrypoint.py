"""Integration test for the real entrypoint wiring (Design.md §13 Step
G5) — drives webhook_sender.__main__.run() directly: a real BusClient
(via connect_as_adapter), a real local HTTP server, a real shutdown
handshake. Mirrors the same pattern Steps C7/F6 each already established
for their own adapters — test_relay_handler.py already proves
RelayHandler works in isolation; this is the one test that proves it
still works wired into the real multi-subject gather (Step G4), including
the "subjects is empty" keepalive path.

Connects as webhook-sender-01-TEST, a dedicated test-only identity
(Defects.md Defect 1) — not webhook-sender-01 itself, whose real
docker-compose container runs throughout this whole dev session and
would otherwise race this test for the service-identity/service-directory
KV entries.
"""

from __future__ import annotations

import asyncio

import httpx
import webhook_sender.__main__ as entrypoint
from _webhook_sender_helpers import ORDER_CREATED_SUBJECT, connect, wait_until_cache_has
from _webhook_sender_local_server import LocalHttpServer
from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings


async def test_real_entrypoint_relays_and_shuts_down_cleanly(durable_name: str) -> None:
    server = LocalHttpServer(status_code=200)
    settings = AdapterSettings(
        service_id="webhook-sender-01-test",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/webhook-sender-01-test.creds",
    )
    sender_client = await BusClient.connect_as_adapter(settings, adapter_type="webhook-sender")
    publisher = await connect("rest-api-service-01-test")
    shutdown = asyncio.Event()
    run_task: asyncio.Task[None] | None = None
    try:
        async with httpx.AsyncClient() as http_client:
            run_task = asyncio.create_task(
                entrypoint.run(
                    sender_client,
                    subjects=[ORDER_CREATED_SUBJECT],
                    target_url=server.url,
                    outbound_secret="entrypoint-secret",
                    http_client=http_client,
                    service_id="webhook-sender-01-test",
                    shutdown=shutdown,
                )
            )
            await asyncio.sleep(0.2)  # let subscribe() actually happen

            await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)
            await publisher.publish(
                ORDER_CREATED_SUBJECT, b"G5 entrypoint proof", event_type="OrderCreated"
            )

            for _ in range(50):
                if server.received:
                    break
                await asyncio.sleep(0.1)

            assert len(server.received) == 1
            assert server.received[0].body == b"G5 entrypoint proof"
            assert server.received[0].headers["X-Webhook-Secret"] == "entrypoint-secret"

            # Graceful shutdown: setting the event must make run() actually
            # return within a bounded time, not hang.
            shutdown.set()
            await asyncio.wait_for(run_task, timeout=5)
            run_task = None
    finally:
        if run_task is not None:
            run_task.cancel()
        server.shutdown()
        await sender_client.close()
        await publisher.close()


async def test_real_entrypoint_with_no_subjects_still_shuts_down_cleanly() -> None:
    """The keepalive task (Design.md §13 Step G4) — with zero configured
    subjects there's nothing to gather except it, so run() must still
    respond to shutdown rather than hang forever with an empty gather."""
    settings = AdapterSettings(
        service_id="webhook-sender-01-test",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/webhook-sender-01-test.creds",
    )
    sender_client = await BusClient.connect_as_adapter(settings, adapter_type="webhook-sender")
    shutdown = asyncio.Event()
    run_task: asyncio.Task[None] | None = None
    try:
        async with httpx.AsyncClient() as http_client:
            run_task = asyncio.create_task(
                entrypoint.run(
                    sender_client,
                    subjects=[],
                    target_url="http://127.0.0.1:1",  # never used — no subjects
                    outbound_secret=None,
                    http_client=http_client,
                    service_id="webhook-sender-01-test",
                    shutdown=shutdown,
                )
            )
            await asyncio.sleep(0.2)
            assert not run_task.done()  # still alive, waiting on shutdown

            shutdown.set()
            await asyncio.wait_for(run_task, timeout=5)
            run_task = None
    finally:
        if run_task is not None:
            run_task.cancel()
        await sender_client.close()

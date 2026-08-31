"""End-to-end test for the Webhook Listener -> File Storage Adapter
vertical slice (Design.md §12 Step E4) — the automated, repeatable
counterpart to Step E3's manual smoke test (already run against the real
`docker compose` stack: real curl, real containers, real bind mount).

Runs both adapters' REAL entrypoint wiring concurrently in-process —
`webhook_listener.app.create_app` via `TestClient` (Step D5's pattern) and
`file_storage_adapter.__main__.run` as a background task (Step C7's
pattern) — rather than orchestrating real Docker containers from pytest;
E3 already proved the container/Compose layer works, so this proves the
two adapters' actual application code works *together*, the same
separation of concerns Steps C7/D5 each already established for one
adapter alone. Closes the loop the way Step B7 did for the library by
itself, this time for two real adapters.

Connects as file-storage-01-TEST and webhook-listener-01-TEST, dedicated
test-only twins of the real adapters (Defects.md Defect 1) — not
file-storage-01/webhook-listener-01 themselves, whose own live
docker-compose containers run throughout this whole dev session.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import file_storage_adapter.__main__ as file_storage_entrypoint
from _e2e_helpers import connect
from fastapi.testclient import TestClient
from file_storage_adapter.write_handler import CREATE_COMPLETED_SUBJECT
from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings
from webhook_listener.app import create_app
from webhook_listener.settings import WebhookListenerSettings

WEBHOOK_SECRET = "e4-end-to-end-secret"


async def test_real_webhook_post_ends_up_as_a_real_file_and_completion_event(
    tmp_path: Path, durable_name: str
) -> None:
    fsa_settings = AdapterSettings(
        service_id="file-storage-01-test",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/file-storage-01-test.creds",
    )
    fsa_client = await BusClient.connect_as_adapter(
        fsa_settings, adapter_type="file-storage-adapter"
    )
    observer = await connect("test-observer-01")
    shutdown = asyncio.Event()
    fsa_task: asyncio.Task[None] | None = None
    try:
        completed_durable = await observer.subscribe(
            CREATE_COMPLETED_SUBJECT, durable_name=durable_name
        )

        # The real File Storage Adapter, running its real entrypoint loop
        # (write-path handler + external-creation watch, Steps C4/C5),
        # exactly as `python -m file_storage_adapter` does.
        fsa_task = asyncio.create_task(
            file_storage_entrypoint.run(
                fsa_client,
                watch_dir=tmp_path,
                service_id="file-storage-01-test",
                shutdown=shutdown,
            )
        )
        await asyncio.sleep(0.2)  # let subscribe()/awatch actually start

        # The real Webhook Listener, driven through a real HTTP request —
        # its own lifespan connects a real, independent BusClient (Step
        # C6/D4's connect_as_adapter), exactly as `python -m
        # webhook_listener` does.
        whl_settings = WebhookListenerSettings(
            service_id="webhook-listener-01-test",
            nats_url="nats://localhost:4222",
            nats_creds_path="infra/nats/operator/creds/webhook-listener-01-test.creds",
            webhook_secret=WEBHOOK_SECRET,
        )
        app = create_app(whl_settings)
        body = b"E4 end-to-end proof: real webhook POST -> real file storage adapter"
        with TestClient(app) as client:
            response = client.post(
                "/webhooks/e4/proof.txt",
                content=body,
                headers={"X-Webhook-Secret": WEBHOOK_SECRET},
            )
            assert response.status_code == 202

        received = await observer.fetch(completed_durable, timeout=5)
        assert len(received) == 1
        assert received[0].details.event_type == "FileCreateCompleted"
        assert received[0].details.source_service_id == "file-storage-01-test"

        written = tmp_path / "e4" / "proof.txt"
        assert written.read_bytes() == body
    finally:
        shutdown.set()
        if fsa_task is not None:
            await asyncio.wait_for(fsa_task, timeout=5)
        await fsa_client.close()
        await observer.close()

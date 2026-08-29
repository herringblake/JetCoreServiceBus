"""The Webhook Listener's HTTP surface (Design.md §8, §12 Step D3).

`POST /webhooks/{path:path}` (Decision #22): verifies `X-Webhook-Secret`
before touching the bus at all, maps the URL path segment + raw body into
a FileWriteRequested command, publishes it fire-and-forget, and responds
`202 Accepted` once *published* — not once the File Storage Adapter has
actually processed it (Design.md §12's parameter table; no reply subject
exists for this, per the manifest's own description of
webhook-listener-01). Path-traversal validation is deliberately NOT
duplicated here — that's the File Storage Adapter's job (`resolve_within`,
Step C4); this adapter stays a thin HTTP-to-command translator.

`GET /healthz` exists because not every image in this stack ships a
usable shell/HTTP client for a Compose-native healthcheck (A6/A7's
finding) — a Python-native ASGI app can trivially serve its own.
"""

from __future__ import annotations

import base64
import json
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from jetcore.bus_client import BusClient

from webhook_listener.settings import WebhookListenerSettings

logger = logging.getLogger(__name__)

WRITE_REQUESTED_SUBJECT = "events.files.FileWriteRequested"
ADAPTER_TYPE = "webhook-listener"


def create_app(
    settings: WebhookListenerSettings, *, bus_client: BusClient | None = None
) -> FastAPI:
    """Builds the ASGI app.

    `bus_client`, when given, is used as-is, and the lifespan below skips
    connecting/closing one of its own — Step D3's tests inject a real
    test-connected client this way, without going through the production
    `connect_as_adapter()` path. The real entrypoint (Step D4) leaves this
    unset, so the lifespan owns the connection's whole lifecycle (connect
    on startup, close on shutdown — FastAPI's `lifespan`, not a module-
    global, so nothing is connected before the app is actually serving)."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if bus_client is not None:
            app.state.bus_client = bus_client
            yield
            return
        client = await BusClient.connect_as_adapter(settings, adapter_type=ADAPTER_TYPE)
        app.state.bus_client = client
        try:
            yield
        finally:
            await client.close()

    app = FastAPI(lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/webhooks/{path:path}", status_code=202)
    async def receive_webhook(path: str, request: Request) -> Response:
        header_secret = request.headers.get("X-Webhook-Secret")
        expected_secret = settings.webhook_secret.get_secret_value()
        # secrets.compare_digest: constant-time comparison, not `==` — the
        # whole point of checking a secret at all (Decision #21).
        if header_secret is None or not secrets.compare_digest(header_secret, expected_secret):
            raise HTTPException(status_code=401, detail="missing or invalid X-Webhook-Secret")

        if not path:
            raise HTTPException(status_code=400, detail="a target file path is required")

        body = await request.body()
        payload = json.dumps({"path": path, "content": base64.b64encode(body).decode()}).encode()

        client: BusClient = request.app.state.bus_client
        await client.publish(WRITE_REQUESTED_SUBJECT, payload, event_type="FileWriteRequested")

        return Response(status_code=202)

    return app

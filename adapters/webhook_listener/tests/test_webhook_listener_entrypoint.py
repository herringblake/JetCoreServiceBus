"""Integration test for the real entrypoint wiring (Design.md §12 Step
D5) — `create_app(settings)` WITHOUT an injected `bus_client`, so its own
`lifespan` connects a REAL `BusClient` (via `connect_as_adapter`, Step
C6's identity-stability fix), exactly the way `python -m webhook_listener`
does in production (Step D4). Driven through FastAPI's `TestClient` (real
ASGI requests, no fake) rather than a real subprocess/TCP socket — the
manual verification in Step D4 already proved the real process/uvicorn
layer works; this is the one thing Step D3's `FakeBusClient`-based tests
can't prove: a real POST results in a real, signed, encrypted
`FileWriteRequested` landing on the `EVENTS` stream.

This is Track D's exit criterion (Design.md §12): the pieces built across
D1-D4 actually work together against the real bus, not just individually.

One real design mistake this test itself hit while being written, worth
recording: `TestClient` runs the ASGI app's `lifespan` (and everything it
creates, including the real `BusClient`) on its own background
thread/event loop (anyio's blocking portal). Reaching into
`app.state.bus_client` from THIS test's own event loop and `await`-ing
its methods directly — e.g. to pre-warm its recipient cache — silently
hangs (nats-py Futures bound to one loop never get resolved by I/O
handled on another). Fixed by not doing that at all: `observer.subscribe`
happens *before* the app is even created, and `RecipientCache.start()`
(registry.py) blocks until its initial catch-up snapshot is loaded before
returning — so by the time the webhook listener's own client creates its
recipient cache on its *first* `publish()` call (inside the POST,
entirely on the portal thread, no cross-loop access at all), the
already-registered observer is already visible. No explicit wait needed;
ordering alone guarantees it.
"""

from __future__ import annotations

import base64
import json

from _webhook_listener_helpers import connect
from fastapi.testclient import TestClient
from webhook_listener.app import WRITE_REQUESTED_SUBJECT, create_app
from webhook_listener.settings import WebhookListenerSettings

WEBHOOK_SECRET = "d5-live-bus-secret"


async def test_real_post_publishes_a_real_signed_encrypted_command(durable_name: str) -> None:
    settings = WebhookListenerSettings(
        service_id="webhook-listener-01",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/webhook-listener-01.creds",
        webhook_secret=WEBHOOK_SECRET,
    )
    observer = await connect("test-observer-01")
    try:
        observer_durable = await observer.subscribe(
            WRITE_REQUESTED_SUBJECT, durable_name=durable_name
        )

        app = create_app(settings)  # no injected bus_client — the real lifespan connects
        with TestClient(app) as client:
            body = b"real end-to-end proof: TestClient -> real lifespan BusClient -> real NATS"
            response = client.post(
                "/webhooks/d5/proof.txt",
                content=body,
                headers={"X-Webhook-Secret": WEBHOOK_SECRET},
            )
            assert response.status_code == 202
        # TestClient's `with` block has now exited — the real BusClient
        # is closed, same as production's shutdown path (Step D4). The
        # publish already completed before the response was sent, so
        # fetching it back afterward is fine: JetStream durability, not
        # dependent on the publisher's connection staying open.

        received = await observer.fetch(observer_durable, timeout=5)

        assert len(received) == 1
        result = received[0]
        assert result.details.event_type == "FileWriteRequested"
        assert result.details.source_service_id == "webhook-listener-01"
        data = json.loads(result.payload)
        assert data["path"] == "d5/proof.txt"
        assert base64.b64decode(data["content"]) == body
    finally:
        await observer.close()

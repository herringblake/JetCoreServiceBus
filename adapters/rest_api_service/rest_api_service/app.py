"""The REST API Service's HTTP surface (Design.md §8, §13 Step I5).

`POST /api/orders` (Decision #14's placeholder bounded context — no real
order schema exists yet, so the raw request body is passed through
verbatim as the bus payload, the same "thin HTTP-to-command translator"
posture the Webhook Listener already established for FileWriteRequested,
Step D3): publishes `events.orders.OrderCreated`, capturing the returned
`eventId` (Step I1, Decision #24) to use as `correlationId` for a
matching reply.

  - No `?wait=` (or `?wait=0`): `202 Accepted` immediately, matching the
    Webhook Listener's own fire-and-forget default — plus the `eventId`
    in the response body, since here (unlike the Webhook Listener) a
    caller might reasonably want it even without waiting, given the whole
    point of this adapter is correlatable replies.
  - `?wait=<seconds>`, capped at `settings.default_reply_timeout_seconds`:
    registers a pending reply (Step I4), awaits it, returns `200` + the
    real `OrderPersisted` payload, or `504` on timeout — unregistering
    the pending entry either way, so a late reply after timeout doesn't
    leak a resolved-but-unread future.

`GET /healthz` — same reasoning as every other FastAPI-based adapter
(Step D3): not every image in this stack ships a usable shell/HTTP client
for a Compose-native healthcheck.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Query, Request, Response
from jetcore.bus_client import BusClient

from rest_api_service.pending_replies import (
    ORDER_PERSISTED_SUBJECT,
    PendingReplies,
    run_reply_watcher,
)
from rest_api_service.settings import RestApiServiceSettings

logger = logging.getLogger(__name__)

ORDER_CREATED_SUBJECT = "events.orders.OrderCreated"
ADAPTER_TYPE = "rest-api-service"


def create_app(settings: RestApiServiceSettings, *, bus_client: BusClient | None = None) -> FastAPI:
    """Builds the ASGI app.

    `bus_client`, when given, is used as-is, and the lifespan below skips
    both connecting one of its own AND starting the reply watcher —
    matching the Webhook Listener's own `create_app()` shape (Step D3),
    extended here since this adapter also owns a background consumer
    loop the Webhook Listener never needed. A test that wants the
    sync-reply path exercised for real (Step I7) therefore leaves
    `bus_client` unset, the same as Step D5's real-bus proof."""

    pending = PendingReplies()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if bus_client is not None:
            app.state.bus_client = bus_client
            app.state.pending = pending
            yield
            return

        client = await BusClient.connect_as_adapter(settings, adapter_type=ADAPTER_TYPE)
        durable_name = f"{settings.service_id}-{ORDER_PERSISTED_SUBJECT.replace('.', '_')}"
        await client.subscribe(ORDER_PERSISTED_SUBJECT, durable_name=durable_name)

        watcher_shutdown = asyncio.Event()
        watcher_task = asyncio.create_task(
            run_reply_watcher(client, pending, durable_name=durable_name, shutdown=watcher_shutdown)
        )

        app.state.bus_client = client
        app.state.pending = pending
        try:
            yield
        finally:
            watcher_shutdown.set()
            watcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher_task
            await client.close()

    app = FastAPI(lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/orders")
    async def create_order(
        request: Request,
        wait: float = Query(default=0.0, ge=0.0),
        idempotency_key: str | None = Header(default=None),
    ) -> Response:
        # Design.md §16 Step N2 (§9 item #10) — optional, additive: a
        # caller that never sends this header gets today's plain
        # at-least-once behavior, unchanged. One that retries the exact
        # same `Idempotency-Key` after a network blip (e.g. it never saw
        # this endpoint's own response, even though the publish already
        # landed) gets the retry's publish suppressed server-side by
        # JetStream's own dedup window (BusClient.publish()'s own
        # docstring), not a second OrderCreated.
        body = await request.body()
        client: BusClient = request.app.state.bus_client
        event_id = await client.publish(
            ORDER_CREATED_SUBJECT, body, event_type="OrderCreated", msg_id=idempotency_key
        )

        if wait <= 0:
            return Response(
                status_code=202,
                content=json.dumps({"eventId": event_id}),
                media_type="application/json",
            )

        capped_wait = min(wait, settings.default_reply_timeout_seconds)
        pending_replies: PendingReplies = request.app.state.pending
        future = pending_replies.register(event_id)
        try:
            event = await asyncio.wait_for(future, timeout=capped_wait)
            return Response(status_code=200, content=event.payload, media_type="application/json")
        except TimeoutError:
            return Response(
                status_code=504,
                content=json.dumps({"eventId": event_id, "error": "timeout waiting for reply"}),
                media_type="application/json",
            )
        finally:
            pending_replies.unregister(event_id)

    return app

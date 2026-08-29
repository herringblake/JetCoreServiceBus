"""Integration tests for relay_handler.py (Design.md §13 Step G3) —
against the real Phase 1 stack and a real local HTTP server (no mocking,
same discipline as every other adapter's tests in this project).

Uses rest-api-service-01's identity as the test trigger publisher —
it already has publish permission on the placeholder
events.orders.OrderCreated subject from Phase 1's original manifest setup
(Decision #14/#18), and Track I (the real REST API Service) doesn't exist
yet — the same "use a not-yet-built adapter's real identity as a test
trigger" approach Track F used for test-observer-01.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import httpx
import pytest
from _local_http_server import LocalHttpServer
from _webhook_sender_helpers import ORDER_CREATED_SUBJECT, connect, wait_until_cache_has
from webhook_sender.relay_handler import RelayHandler


@pytest.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


async def test_received_event_is_relayed_to_target_url(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    server = LocalHttpServer(status_code=200)
    publisher = await connect("rest-api-service-01")
    sender_client = await connect("webhook-sender-01")
    handler = RelayHandler(
        sender_client,
        subject=ORDER_CREATED_SUBJECT,
        target_url=server.url,
        outbound_secret=None,
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(
            ORDER_CREATED_SUBJECT, b"order payload proof", event_type="OrderCreated"
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert len(server.received) == 1
        request = server.received[0]
        assert request.body == b"order payload proof"
        assert request.headers["X-Event-Type"] == "OrderCreated"
        assert "X-Webhook-Secret" not in request.headers
    finally:
        server.shutdown()
        await publisher.close()
        await sender_client.close()


async def test_outbound_secret_is_sent_when_configured(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    server = LocalHttpServer(status_code=200)
    publisher = await connect("rest-api-service-01")
    sender_client = await connect("webhook-sender-01")
    handler = RelayHandler(
        sender_client,
        subject=ORDER_CREATED_SUBJECT,
        target_url=server.url,
        outbound_secret="relay-secret",
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"payload", event_type="OrderCreated")

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert server.received[0].headers["X-Webhook-Secret"] == "relay-secret"
    finally:
        server.shutdown()
        await publisher.close()
        await sender_client.close()


async def test_failed_post_is_still_acked_not_redelivered(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    """Decision #12 — best-effort, single attempt. A downstream 500
    shouldn't cause redelivery, which would itself be a retry."""
    server = LocalHttpServer(status_code=500)
    publisher = await connect("rest-api-service-01")
    sender_client = await connect("webhook-sender-01")
    handler = RelayHandler(
        sender_client,
        subject=ORDER_CREATED_SUBJECT,
        target_url=server.url,
        outbound_secret=None,
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"payload", event_type="OrderCreated")

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1
        assert len(server.received) == 1  # the attempt really happened

        # Acked despite the 500 — a second fetch should find nothing left
        # (BusClient.fetch()'s Step C4 fix: [] on idle, not an exception).
        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        server.shutdown()
        await publisher.close()
        await sender_client.close()


async def test_connection_refused_is_still_acked_not_redelivered(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    """A downstream that isn't even listening — httpx.ConnectError, not
    an HTTP status — must be caught the same way a 500 is (Decision #12
    doesn't distinguish "server errored" from "server unreachable")."""
    publisher = await connect("rest-api-service-01")
    sender_client = await connect("webhook-sender-01")
    # Port 1 is always refused (privileged, nothing binds it in a test env).
    handler = RelayHandler(
        sender_client,
        subject=ORDER_CREATED_SUBJECT,
        target_url="http://127.0.0.1:1",
        outbound_secret=None,
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"payload", event_type="OrderCreated")

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await publisher.close()
        await sender_client.close()

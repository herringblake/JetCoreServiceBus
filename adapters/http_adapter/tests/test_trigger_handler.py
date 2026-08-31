"""Integration tests for trigger_handler.py (Design.md §13 Step H3) —
against the real Phase 1 stack and a real local HTTP server (no mocking,
same discipline as webhook_sender's own tests, Step G5).

Uses rest-api-service-01's identity as the test trigger publisher — the
same "use a not-yet-built adapter's real identity as a test trigger"
approach Track G used, since Track I (the real REST API Service) doesn't
exist as code yet, but its real NATS identity already does (Phase 1
placeholder provisioning). rest-api-service-01 has `subscribe: []` in the
manifest, though — a real, immediately-obvious permission error the first
version of this test hit trying to have it *also* observe the reply
(subscribing registers a service-directory recipient entry, which needs
KV-write permission this identity doesn't have). test-observer-01
(extended in Step H5, matching Step F6's precedent) is the observer
instead.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import httpx
import pytest
from _http_adapter_helpers import (
    ORDER_CREATED_SUBJECT,
    REQUEST_COMPLETED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from _http_adapter_local_server import LocalHttpServer
from http_adapter.trigger_handler import TriggerHandler


@pytest.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


async def test_success_response_publishes_request_completed(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    server = LocalHttpServer(status_code=200)
    publisher = await connect("rest-api-service-01-test")
    observer = await connect("test-observer-01")
    adapter_client = await connect("http-adapter-01-test")
    handler = TriggerHandler(
        adapter_client,
        subject=ORDER_CREATED_SUBJECT,
        target_base_url=server.url,
        auth_token=None,
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await observer.subscribe(
            REQUEST_COMPLETED_SUBJECT, durable_name=f"{durable_name}-completed"
        )

        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)
        await wait_until_cache_has(await adapter_client._cache_for(REQUEST_COMPLETED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"order payload", event_type="OrderCreated")

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert len(server.received) == 1
        assert server.received[0].body == b"order payload"

        [result] = await observer.fetch(completed_durable, timeout=3)
        assert result.details.event_type == "RequestCompleted"
        assert result.details.source_service_id == "http-adapter-01-test"
        data = json.loads(result.payload)
        assert data["status"] == "success"
        assert data["statusCode"] == 200
        assert data["occurredAt"].endswith("Z")
    finally:
        server.shutdown()
        await publisher.close()
        await observer.close()
        await adapter_client.close()


async def test_error_status_still_publishes_request_completed(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    """This adapter reports the outcome, it doesn't judge it (Design.md
    §13) — a 503 is still a real response, not a failure to reach the
    API at all."""
    server = LocalHttpServer(status_code=503)
    publisher = await connect("rest-api-service-01-test")
    observer = await connect("test-observer-01")
    adapter_client = await connect("http-adapter-01-test")
    handler = TriggerHandler(
        adapter_client,
        subject=ORDER_CREATED_SUBJECT,
        target_base_url=server.url,
        auth_token=None,
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await observer.subscribe(
            REQUEST_COMPLETED_SUBJECT, durable_name=f"{durable_name}-completed"
        )

        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)
        await wait_until_cache_has(await adapter_client._cache_for(REQUEST_COMPLETED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"order payload", event_type="OrderCreated")

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await observer.fetch(completed_durable, timeout=3)
        data = json.loads(result.payload)
        assert data["status"] == "error"
        assert data["statusCode"] == 503
    finally:
        server.shutdown()
        await publisher.close()
        await observer.close()
        await adapter_client.close()


async def test_auth_token_is_sent_as_bearer_header(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    server = LocalHttpServer(status_code=200)
    publisher = await connect("rest-api-service-01-test")
    adapter_client = await connect("http-adapter-01-test")
    handler = TriggerHandler(
        adapter_client,
        subject=ORDER_CREATED_SUBJECT,
        target_base_url=server.url,
        auth_token="test-token",
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"payload", event_type="OrderCreated")

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert server.received[0].headers["Authorization"] == "Bearer test-token"
    finally:
        server.shutdown()
        await publisher.close()
        await adapter_client.close()


async def test_connection_refused_is_nakd_for_redelivery(
    durable_name: str, http_client: httpx.AsyncClient
) -> None:
    """Unlike the Webhook Sender's deliberate best-effort choice (Decision
    #12), a genuine connection failure here naks — worth a retry, since
    there's nothing meaningful to report yet."""
    publisher = await connect("rest-api-service-01-test")
    adapter_client = await connect("http-adapter-01-test")
    handler = TriggerHandler(
        adapter_client,
        subject=ORDER_CREATED_SUBJECT,
        target_base_url="http://127.0.0.1:1",  # always refused
        auth_token=None,
        http_client=http_client,
    )
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"payload", event_type="OrderCreated")

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1  # fetched, but nak'd rather than acked

        # Still pending redelivery — a fresh fetch should see it again.
        again = await handler.run_once(handler_durable, timeout=3)
        assert again == 1
    finally:
        await publisher.close()
        await adapter_client.close()

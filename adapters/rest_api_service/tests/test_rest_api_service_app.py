"""Integration tests for app.py (Design.md §13 Step I5/I7) — against the
real Phase 1 stack, no injected bus_client and no FakeBusClient layer:
`create_app(settings)`'s own `lifespan` connects a real BusClient (via
`connect_as_adapter`) exactly the way `python -m rest_api_service` does
in production (Step I6), the same "D5's pattern" the design breakdown
calls for explicitly — this track's whole point is the real sync-reply
correlation, so there's little value in a FakeBusClient-only layer that
can't exercise it (unlike webhook_listener's D3/D5 split, which had a
pure fire-and-forget path worth unit-testing fast and separately).

Uses db-adapter-mysql-01-TEST's identity as the "fake replier" for the
sync-reply tests — a dedicated test-only twin of db-adapter-mysql-01
(Defects.md Defect 1), not that real serviceId itself: Track J's real
Database Adapter is now a live docker-compose container (Step K2), and
this whole app also connects as rest-api-service-01-TEST rather than
rest-api-service-01 for the same reason. test-observer-01 (extended in
this step) independently captures a request's real eventId where a test
needs to, mirroring Step F6's precedent — it has no live production
counterpart to collide with, so it's unaffected by Defect 1 and stays as
the real test-observer-01 identity.
"""

from __future__ import annotations

import asyncio
import json

from _rest_api_service_helpers import (
    ORDER_CREATED_SUBJECT,
    ORDER_PERSISTED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from fastapi.testclient import TestClient
from rest_api_service.app import create_app
from rest_api_service.settings import RestApiServiceSettings

SETTINGS = RestApiServiceSettings(
    service_id="rest-api-service-01-test",
    nats_url="nats://localhost:4222",
    nats_creds_path="infra/nats/operator/creds/rest-api-service-01-test.creds",
    default_reply_timeout_seconds=5.0,
)


def test_healthz_returns_ok() -> None:
    with TestClient(create_app(SETTINGS)) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_fire_and_forget_publishes_a_real_order_created(durable_name: str) -> None:
    observer = await connect("test-observer-01")
    try:
        observer_durable = await observer.subscribe(
            ORDER_CREATED_SUBJECT, durable_name=durable_name
        )

        with TestClient(create_app(SETTINGS)) as client:
            response = client.post("/api/orders", content=b'{"item": "widget"}')

        assert response.status_code == 202
        event_id = response.json()["eventId"]
        assert event_id

        received = await observer.fetch(observer_durable, timeout=5)
        assert len(received) == 1
        assert received[0].details.event_type == "OrderCreated"
        assert received[0].details.event_id == event_id
        assert received[0].payload == b'{"item": "widget"}'
    finally:
        await observer.close()


async def test_repeated_idempotency_key_publishes_only_once(durable_name: str) -> None:
    """Design.md §16 Step N2 (§9 item #10) — a real, end-to-end proof
    against this adapter's actual HTTP surface: two POSTs sharing one
    Idempotency-Key produce exactly one OrderCreated on the bus, and both
    responses carry the *same* eventId (BusClient.publish()'s own
    duplicate-fetches-the-original behavior, exercised here through the
    real app, not called directly)."""
    observer = await connect("test-observer-01")
    try:
        observer_durable = await observer.subscribe(
            ORDER_CREATED_SUBJECT, durable_name=durable_name
        )

        with TestClient(create_app(SETTINGS)) as client:
            headers = {"Idempotency-Key": f"test-idem-{durable_name}"}
            first = client.post("/api/orders", content=b'{"item": "widget"}', headers=headers)
            second = client.post(
                "/api/orders", content=b'{"item": "widget-retry"}', headers=headers
            )

        assert first.status_code == 202
        assert second.status_code == 202
        first_event_id = first.json()["eventId"]
        second_event_id = second.json()["eventId"]
        assert second_event_id == first_event_id

        received = await observer.fetch(observer_durable, timeout=5)
        assert len(received) == 1
        # The first call's body, not the retry's
        assert received[0].payload == b'{"item": "widget"}'
        assert received[0].details.event_id == first_event_id
        await received[0].ack()

        nothing_more = await observer.fetch(observer_durable, timeout=1)
        assert nothing_more == []
    finally:
        await observer.close()


async def test_sync_reply_returns_the_real_order_persisted_payload(durable_name: str) -> None:
    replier = await connect("db-adapter-mysql-01-test")
    try:
        replier_durable = await replier.subscribe(ORDER_CREATED_SUBJECT, durable_name=durable_name)

        async def _fake_database_adapter() -> None:
            # Waits for the real OrderCreated, then plays the Database
            # Adapter's own write-ack role (Decision #26) — publishes a
            # matching OrderPersisted, correlationId set to the trigger's
            # real eventId, same as the real adapter (Track J) will.
            [triggering] = await replier.fetch(replier_durable, timeout=5)
            await triggering.ack()
            await replier.publish(
                ORDER_PERSISTED_SUBJECT,
                b'{"orderId": "abc-123", "status": "persisted"}',
                event_type="OrderPersisted",
                correlation_id=triggering.details.event_id,
            )

        # client.post() is a blocking call — TestClient's own ASGI app
        # (and this endpoint's asyncio.wait_for(future, ...)) really does
        # run on a separate thread (Starlette's anyio blocking portal, see
        # webhook_listener's test_webhook_listener_entrypoint.py for the
        # fuller writeup of that mechanism), but calling it synchronously
        # from THIS coroutine would still starve _fake_database_adapter()
        # above — both live on this test's own event loop, and a
        # synchronous call never yields control back to it. Routed through
        # asyncio.to_thread so this test's loop stays free to actually run
        # the replier concurrently with the in-flight request.
        with TestClient(create_app(SETTINGS)) as client:
            # The app's own lifespan (entered above) is what actually
            # subscribes rest-api-service-01-test to OrderPersisted — only now
            # is it a registered recipient replier.publish() can encrypt
            # for, so the cache-wait has to be here, not before entering
            # this block.
            await wait_until_cache_has(await replier._cache_for(ORDER_PERSISTED_SUBJECT), 1)
            replier_task = asyncio.create_task(_fake_database_adapter())

            response = await asyncio.to_thread(
                client.post, "/api/orders?wait=4", content=b'{"item": "widget"}'
            )

        await asyncio.wait_for(replier_task, timeout=5)

        assert response.status_code == 200
        assert json.loads(response.content) == {"orderId": "abc-123", "status": "persisted"}
    finally:
        await replier.close()


async def test_wait_capped_at_settings_still_times_out_correctly() -> None:
    """A caller asking for a longer wait than the server allows is capped
    (Design.md §13 Step I3), not rejected — proven here by asking for far
    more than SETTINGS' own 5s cap and confirming the request still
    returns (504, nothing replies) well before that requested duration
    would have elapsed, i.e. the cap was actually applied server-side."""
    with TestClient(create_app(SETTINGS)) as client:
        response = client.post("/api/orders?wait=9999", content=b"{}")

    assert response.status_code == 504
    assert "eventId" in response.json()


async def test_timeout_with_nothing_replying_returns_504() -> None:
    with TestClient(create_app(SETTINGS)) as client:
        response = client.post("/api/orders?wait=1", content=b'{"item": "nobody answers"}')

    assert response.status_code == 504
    body = response.json()
    assert body["error"]
    assert "eventId" in body

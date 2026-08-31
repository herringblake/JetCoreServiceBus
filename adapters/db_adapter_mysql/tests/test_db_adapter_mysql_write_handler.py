"""Integration tests for write_handler.py (Design.md §13 Step J4) —
against the real Phase 1 NATS stack AND a real MySQL (Step J1's
docker-compose service), no mocking of either (matching this whole
project's practice for NATS, extended here to MySQL for the first time).

Uses rest-api-service-01's real identity as both the trigger publisher
(it already has publish permission on events.orders.OrderCreated,
Decision #14/#18) AND the OrderPersisted observer (extended with that
subscribe permission in Design.md §13 Step I4, ahead of this track
actually existing) — the one adapter identity in this whole manifest that
already covers both sides of a single write-path round trip without
needing test-observer-01 at all.
"""

from __future__ import annotations

from _db_adapter_mysql_helpers import (
    ORDER_CREATED_SUBJECT,
    ORDER_PERSISTED_SUBJECT,
    connect,
    root_engine,
    wait_until_cache_has,
    write_engine,
)
from db_adapter_mysql.write_handler import WriteCommandHandler
from sqlalchemy import text


async def test_order_created_is_upserted_and_persisted_published(durable_name: str) -> None:
    # ONE client for both roles, not two separate connect("rest-api-service-01-test")
    # calls — a real bug the first version of this test hit: two concurrent
    # connections under the same serviceId each register_identity() their
    # own freshly-generated signing key (BusClient.connect(), unlike
    # connect_as_adapter(), has no stable creds-derived identity), and
    # whichever registers second silently invalidates the other's, so
    # messages signed by the first fail signature verification. Documented
    # in bus_client.py's connect_as_adapter() docstring (Step B7); this is
    # the same underlying issue, just via two ad-hoc test connections
    # instead of two real adapter restarts.
    publisher = await connect("rest-api-service-01-test")
    adapter_client = await connect("db-adapter-mysql-01-test")
    engine = write_engine()
    handler = WriteCommandHandler(adapter_client, engine=engine)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await publisher.subscribe(
            ORDER_PERSISTED_SUBJECT, durable_name=f"{durable_name}-persisted"
        )

        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)
        await wait_until_cache_has(await adapter_client._cache_for(ORDER_PERSISTED_SUBJECT), 1)

        await publisher.publish(
            ORDER_CREATED_SUBJECT,
            b'{"orderId": "order-1", "item": "widget", "quantity": 2}',
            event_type="OrderCreated",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await publisher.fetch(completed_durable, timeout=3)
        assert result.details.event_type == "OrderPersisted"
        assert result.details.source_service_id == "db-adapter-mysql-01-test"

        root = root_engine()
        try:
            async with root.connect() as conn:
                row = (
                    await conn.execute(
                        text("SELECT item, quantity FROM orders WHERE order_id = :id"),
                        {"id": "order-1"},
                    )
                ).one()
        finally:
            await root.dispose()
        assert row.item == "widget"
        assert row.quantity == 2
    finally:
        await publisher.close()
        await adapter_client.close()
        await engine.dispose()


async def test_order_created_upserts_idempotently_on_repeat(durable_name: str) -> None:
    """Decision #25 — order_id is caller-supplied specifically so a
    repeat/redelivered OrderCreated for the same order updates the
    existing row rather than failing on a duplicate-key error."""
    publisher = await connect("rest-api-service-01-test")
    adapter_client = await connect("db-adapter-mysql-01-test")
    engine = write_engine()
    handler = WriteCommandHandler(adapter_client, engine=engine)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(
            ORDER_CREATED_SUBJECT,
            b'{"orderId": "order-2", "item": "first-item", "quantity": 1}',
            event_type="OrderCreated",
        )
        assert await handler.run_once(handler_durable, timeout=3) == 1

        await publisher.publish(
            ORDER_CREATED_SUBJECT,
            b'{"orderId": "order-2", "item": "updated-item", "quantity": 5}',
            event_type="OrderCreated",
        )
        assert await handler.run_once(handler_durable, timeout=3) == 1

        root = root_engine()
        try:
            async with root.connect() as conn:
                rows = (
                    await conn.execute(
                        text("SELECT item, quantity FROM orders WHERE order_id = :id"),
                        {"id": "order-2"},
                    )
                ).all()
        finally:
            await root.dispose()
        assert len(rows) == 1  # upsert, not a second row
        assert rows[0].item == "updated-item"
        assert rows[0].quantity == 5
    finally:
        await publisher.close()
        await adapter_client.close()
        await engine.dispose()


async def test_malformed_payload_is_acked_not_redelivered(durable_name: str) -> None:
    publisher = await connect("rest-api-service-01-test")
    adapter_client = await connect("db-adapter-mysql-01-test")
    engine = write_engine()
    handler = WriteCommandHandler(adapter_client, engine=engine)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(ORDER_CREATED_SUBJECT), 1)

        await publisher.publish(ORDER_CREATED_SUBJECT, b"not json", event_type="OrderCreated")

        assert await handler.run_once(handler_durable, timeout=3) == 1
        # Acked despite being malformed — a second fetch finds nothing left.
        assert await handler.run_once(handler_durable, timeout=1) == 0
    finally:
        await publisher.close()
        await adapter_client.close()
        await engine.dispose()

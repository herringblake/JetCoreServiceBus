"""Write-path command handler (Design.md §13 Step J4) — subscribes to
events.orders.OrderCreated, upserts into the `orders` table (Decision
#25: `order_id` is caller-supplied, so this needs no round trip to learn
a DB-assigned id first), and publishes events.orders.OrderPersisted
(Decision #26) with `correlationId` set to the triggering event's
`eventId`.

Retry posture, mirroring the File Storage Adapter's write handler (Step
C4): a malformed payload is deterministically bad (redelivery would never
fix it) — logged and acked, not retried. A real MySQL error (connection
dropped, deadlock, etc.) is plausibly transient — left unacked for
redelivery instead. `tenacity` (§7.2) is intentionally NOT used for a
first in-process retry loop here: JetStream redelivery already IS the
retry mechanism (Design.md §6), and stacking a second one on top would
just delay how quickly a real outage becomes visible as pending/unacked
messages. Recorded as a deliberate choice, not an oversight — Dependencies.md
keeps `tenacity` recorded for wherever a future need for *bounded,
in-process* retry (e.g. around a single flaky call, not a whole message)
actually shows up.
"""

from __future__ import annotations

import logging

from jetcore.bus_client import BusClient, ReceivedEvent
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from db_adapter_mysql.payloads import (
    MalformedPayloadError,
    decode_order_created,
    encode_order_persisted,
)

logger = logging.getLogger(__name__)

ORDER_CREATED_SUBJECT = "events.orders.OrderCreated"
ORDER_PERSISTED_SUBJECT = "events.orders.OrderPersisted"

# The row-alias form (`... AS new ON DUPLICATE KEY UPDATE col = new.col`),
# not the older `VALUES(col)` function — confirmed by testing, not just
# style preference: MySQL 8.0.20+ deprecated VALUES() in this context, and
# it turns out to actually matter here, not just draw a warning — it
# requires SELECT privilege on every referenced column (MySQL treats
# VALUES() as reading a virtual table), which `jetcore_write` deliberately
# doesn't have (Decision #25/Step J1 — INSERT/UPDATE only). The row-alias
# form needs no such grant.
_UPSERT_SQL = text(
    "INSERT INTO orders (order_id, item, quantity) VALUES (:order_id, :item, :quantity) AS new "
    "ON DUPLICATE KEY UPDATE item = new.item, quantity = new.quantity"
)


class WriteCommandHandler:
    def __init__(self, client: BusClient, *, engine: AsyncEngine) -> None:
        self._client = client
        self._engine = engine

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(ORDER_CREATED_SUBJECT, durable_name=durable_name)

    async def run_once(self, durable_name: str, *, batch: int = 10, timeout: float = 5.0) -> int:
        events = await self._client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in events:
            await self._handle(event)
        return len(events)

    async def _handle(self, event: ReceivedEvent) -> None:
        try:
            request = decode_order_created(event.payload)
        except MalformedPayloadError:
            logger.exception(
                "malformed OrderCreated payload for event %s — acking, not retrying",
                event.details.event_id,
            )
            await event.ack()
            return

        try:
            async with self._engine.begin() as conn:
                await conn.execute(
                    _UPSERT_SQL,
                    {
                        "order_id": request.order_id,
                        "item": request.item,
                        "quantity": request.quantity,
                    },
                )
        except DBAPIError:
            # Plausibly transient (connection dropped, deadlock, MySQL
            # briefly unreachable) — worth a retry via redelivery, unlike
            # the deterministic decode failure above.
            logger.exception(
                "MySQL error upserting order %s for event %s — leaving unacked for redelivery",
                request.order_id,
                event.details.event_id,
            )
            await event.nak()
            return

        await self._client.publish(
            ORDER_PERSISTED_SUBJECT,
            encode_order_persisted(order_id=request.order_id),
            event_type="OrderPersisted",
            correlation_id=event.details.event_id,
        )
        await event.ack()

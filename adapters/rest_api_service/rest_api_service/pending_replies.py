"""In-memory correlation of the REST API Service's synchronous `?wait=`
requests against `events.orders.OrderPersisted` replies (Design.md §13
Step I4, Decision #26). Reuses `BusClient` pub/sub + this dict instead of
core NATS request-reply (Decision #24): `correlationId` is the caller's
own `eventId` (Step I1's now-returned value), not a NATS inbox subject —
consistent with how every other result event in this project already
correlates back to its trigger.

The mapping is entirely in-memory and per-process — a REST API Service
restart drops every pending `?wait=` request outright (the caller just
sees its own connection close/timeout). Fine for this phase's single
demo instance; a real multi-instance deployment behind a load balancer
would need a shared store instead, out of scope here.
"""

from __future__ import annotations

import asyncio
import logging

from jetcore.bus_client import BusClient, ReceivedEvent

logger = logging.getLogger(__name__)

ORDER_PERSISTED_SUBJECT = "events.orders.OrderPersisted"


class PendingReplies:
    """Keyed by correlationId (== the triggering OrderCreated's eventId).
    One `asyncio.Future` per outstanding `?wait=` request."""

    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[ReceivedEvent]] = {}

    def register(self, correlation_id: str) -> asyncio.Future[ReceivedEvent]:
        future: asyncio.Future[ReceivedEvent] = asyncio.get_running_loop().create_future()
        self._pending[correlation_id] = future
        return future

    def unregister(self, correlation_id: str) -> None:
        """Called from the request handler's own `finally` — on a timeout
        OR a successful resolution, either way, so a reply that arrives
        late (after the handler already gave up and returned 504) finds
        no pending entry left to resolve into, rather than leaking a
        resolved-but-never-read future."""
        self._pending.pop(correlation_id, None)

    def resolve(self, event: ReceivedEvent) -> bool:
        """Resolves the matching pending future, if any. Returns whether a
        match was found — purely informational for the watcher loop's own
        logging; an orphaned reply is still acked either way (see
        run_reply_watcher below), since successfully consuming a message
        with nowhere to deliver it isn't a processing failure."""
        correlation_id = event.details.correlation_id
        if correlation_id is None:
            return False
        future = self._pending.get(correlation_id)
        if future is None or future.done():
            return False
        future.set_result(event)
        return True


async def run_reply_watcher(
    client: BusClient,
    pending: PendingReplies,
    *,
    durable_name: str,
    shutdown: asyncio.Event,
    batch: int = 10,
    timeout: float = 2.0,
) -> None:
    """Background loop feeding `pending` from real OrderPersisted arrivals
    — the same poll-loop shape every other adapter's entrypoint already
    uses (Steps C6/G4/H4), reused here instead of inventing a new one.

    A reply with no matching pending request (one that outlived its own
    `?wait=` timeout, or was never one to begin with) is logged and still
    acked — it was successfully consumed, just not delivered anywhere;
    nothing about that warrants redelivery."""
    while not shutdown.is_set():
        received = await client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in received:
            if not pending.resolve(event):
                logger.info(
                    "OrderPersisted %s has no matching pending request "
                    "(correlationId=%s) — likely outlived its own ?wait= timeout",
                    event.details.event_id,
                    event.details.correlation_id,
                )
            await event.ack()

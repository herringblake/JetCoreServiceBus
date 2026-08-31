"""Payload (de)serialization matching schemas/events.orders.OrderCreated
and OrderPersisted (Design.md §13 Step J4) — the first concrete shape
given to Decision #14's placeholder `orders` subjects, since this is the
first track that actually needs to parse `OrderCreated`'s contents (the
REST API Service, Track I, deliberately passes it through as opaque
bytes — Decision #14/§13 Step I5).

`orderId` is caller-supplied (Decision #25 — no DB-assigned autoincrement,
so the write path can upsert idempotently without a round trip back
through the bus first). `item`/`quantity` are the placeholder business
columns `infra/mysql/init.sql` (Step J1) already defines; real columns
replace these whenever `orders` stops being a placeholder bounded context.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from jetcore.envelope import utc_now


class MalformedPayloadError(ValueError):
    """`eventPayload` didn't decode to the shape its schema file
    describes — deterministically bad, not a transient failure (matching
    every other adapter's handler: acked, not redelivered)."""


@dataclass(frozen=True)
class OrderCreatedRequest:
    order_id: str
    item: str
    quantity: int


def decode_order_created(raw: bytes) -> OrderCreatedRequest:
    try:
        data = json.loads(raw)
        order_id = data["orderId"]
        item = data["item"]
        quantity = data.get("quantity", 1)
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise MalformedPayloadError(str(exc)) from exc
    if not isinstance(order_id, str) or not order_id:
        raise MalformedPayloadError(f"orderId must be a non-empty string, got {order_id!r}")
    if not isinstance(item, str) or not item:
        raise MalformedPayloadError(f"item must be a non-empty string, got {item!r}")
    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity < 1:
        raise MalformedPayloadError(f"quantity must be a positive integer, got {quantity!r}")
    return OrderCreatedRequest(order_id=order_id, item=item, quantity=quantity)


def encode_order_persisted(*, order_id: str, occurred_at: datetime | None = None) -> bytes:
    occurred_at = occurred_at or utc_now()
    return json.dumps(
        {
            "orderId": order_id,
            "status": "persisted",
            "occurredAt": occurred_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
    ).encode()


def _json_safe_row(row: dict[str, object]) -> dict[str, object]:
    """Binlog row values come back as real Python types (`datetime`,
    etc.) from python-mysql-replication, not pre-serialized — confirmed
    by testing (Step J5), not assumed. `orders`' own columns only ever
    produce `datetime` (created_at/updated_at) beyond JSON-native types,
    but this stays generic rather than hardcoding those two column names."""
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in row.items()}


def encode_row_changed(
    *,
    operation: str,
    table: str,
    row: dict[str, object],
    previous_row: dict[str, object] | None = None,
) -> bytes:
    """The CDC read path's own output shape (Design.md §13 parameter
    table) — one schema covering all three binlog row-event types rather
    than three. `previous_row` is only ever set for `operation="update"`
    (Track parameters table's own rule, enforced by the caller, not here)."""
    payload: dict[str, object] = {
        "operation": operation,
        "table": table,
        "row": _json_safe_row(row),
    }
    if previous_row is not None:
        payload["previousRow"] = _json_safe_row(previous_row)
    return json.dumps(payload).encode()

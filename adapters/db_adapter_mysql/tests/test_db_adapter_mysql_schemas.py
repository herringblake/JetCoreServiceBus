"""Tests for the Step J4/J5 JSON Schema files
(schemas/events.orders.OrderCreated.v1.json,
schemas/events.orders.OrderPersisted.v1.json,
schemas/events.db.orders.RowChanged.v1.json, Design.md §13) — proves
they're well-formed JSON Schema, and that the payload shapes settled in
§13 actually validate the way they're documented to. Mirrors
http_adapter/tests/test_http_adapter_schemas.py's approach (Step H3).

RowChanged's own coverage was added at Design.md §14 Step L4 — found
missing during Phase 4 planning, the one schema (of 14) with no
automated validation test anywhere, unlike its 13 siblings.

Named test_db_adapter_mysql_schemas.py, not test_schemas.py: same
bare-filename collision already documented repeatedly (Steps C1/D1/D2/
F1/H5)."""

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "schemas"


def _load(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((SCHEMAS_DIR / f"{name}.v1.json").read_text())
    return data


def test_order_created_schema_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(_load("events.orders.OrderCreated"))


def test_order_persisted_schema_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(_load("events.orders.OrderPersisted"))


def test_order_created_example_validates() -> None:
    payload = {"orderId": "order-1", "item": "widget", "quantity": 3}
    Draft202012Validator(_load("events.orders.OrderCreated")).validate(payload)


def test_order_created_without_quantity_validates() -> None:
    payload = {"orderId": "order-1", "item": "widget"}
    Draft202012Validator(_load("events.orders.OrderCreated")).validate(payload)


def test_order_persisted_example_validates() -> None:
    payload = {"orderId": "order-1", "status": "persisted", "occurredAt": "2026-08-30T20:00:00Z"}
    Draft202012Validator(_load("events.orders.OrderPersisted")).validate(payload)


def test_row_changed_schema_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(_load("events.db.orders.RowChanged"))


def test_row_changed_insert_example_validates() -> None:
    payload = {
        "operation": "insert",
        "table": "orders",
        "row": {"order_id": "order-1", "item": "widget", "quantity": 3},
    }
    Draft202012Validator(_load("events.db.orders.RowChanged")).validate(payload)


def test_row_changed_update_example_validates() -> None:
    payload = {
        "operation": "update",
        "table": "orders",
        "row": {"order_id": "order-1", "item": "widget-v2", "quantity": 5},
        "previousRow": {"order_id": "order-1", "item": "widget", "quantity": 3},
    }
    Draft202012Validator(_load("events.db.orders.RowChanged")).validate(payload)


def test_row_changed_delete_example_validates() -> None:
    payload = {
        "operation": "delete",
        "table": "orders",
        "row": {"order_id": "order-1", "item": "widget", "quantity": 3},
    }
    Draft202012Validator(_load("events.db.orders.RowChanged")).validate(payload)


def test_row_changed_rejects_unknown_operation() -> None:
    payload = {"operation": "truncate", "table": "orders", "row": {}}
    validator = Draft202012Validator(_load("events.db.orders.RowChanged"))
    assert not validator.is_valid(payload)

"""Tests for the Step J4 JSON Schema files
(schemas/events.orders.OrderCreated.v1.json,
schemas/events.orders.OrderPersisted.v1.json, Design.md §13) — proves
they're well-formed JSON Schema, and that the payload shapes settled in
§13 actually validate the way they're documented to. Mirrors
http_adapter/tests/test_http_adapter_schemas.py's approach (Step H3).

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

"""Tests for payloads.py (Design.md §13 Step J4) — pure (de)serialization,
no live NATS/MySQL needed.

Named test_db_adapter_mysql_payloads.py, not test_payloads.py: same
bare-filename collision already documented repeatedly (Steps C1/D1/D2/
F1/H5)."""

import json

import pytest
from db_adapter_mysql.payloads import (
    MalformedPayloadError,
    decode_order_created,
    encode_order_persisted,
)


def test_decode_order_created_with_quantity() -> None:
    raw = json.dumps({"orderId": "order-1", "item": "widget", "quantity": 3}).encode()

    request = decode_order_created(raw)

    assert request.order_id == "order-1"
    assert request.item == "widget"
    assert request.quantity == 3


def test_decode_order_created_defaults_quantity_to_one() -> None:
    raw = json.dumps({"orderId": "order-1", "item": "widget"}).encode()

    request = decode_order_created(raw)

    assert request.quantity == 1


def test_decode_order_created_rejects_non_json() -> None:
    with pytest.raises(MalformedPayloadError):
        decode_order_created(b"not json")


def test_decode_order_created_rejects_missing_order_id() -> None:
    raw = json.dumps({"item": "widget"}).encode()

    with pytest.raises(MalformedPayloadError):
        decode_order_created(raw)


def test_decode_order_created_rejects_missing_item() -> None:
    raw = json.dumps({"orderId": "order-1"}).encode()

    with pytest.raises(MalformedPayloadError):
        decode_order_created(raw)


def test_decode_order_created_rejects_zero_quantity() -> None:
    raw = json.dumps({"orderId": "order-1", "item": "widget", "quantity": 0}).encode()

    with pytest.raises(MalformedPayloadError):
        decode_order_created(raw)


def test_decode_order_created_rejects_boolean_quantity() -> None:
    """bool is a subclass of int in Python — `True`/`False` would
    otherwise silently pass an `isinstance(quantity, int)` check."""
    raw = json.dumps({"orderId": "order-1", "item": "widget", "quantity": True}).encode()

    with pytest.raises(MalformedPayloadError):
        decode_order_created(raw)


def test_encode_order_persisted_round_trips() -> None:
    encoded = encode_order_persisted(order_id="order-1")

    data = json.loads(encoded)
    assert data["orderId"] == "order-1"
    assert data["status"] == "persisted"
    assert data["occurredAt"].endswith("Z")

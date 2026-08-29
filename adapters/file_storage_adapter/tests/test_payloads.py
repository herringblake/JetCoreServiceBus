"""Tests for payloads.py (Design.md §12 Step C4) — pure encode/decode
logic, no NATS needed. Cross-checked against the schemas themselves in
test_schemas.py; this file is about the Python-level contract."""

import base64
import json
from datetime import UTC, datetime

import pytest
from file_storage_adapter.payloads import (
    MalformedPayloadError,
    WriteRequest,
    decode_write_requested,
    encode_create_completed,
    encode_write_completed,
)


def test_decode_write_requested_round_trips() -> None:
    raw = json.dumps(
        {"path": "notes/todo.txt", "content": base64.b64encode(b"hello").decode()}
    ).encode()

    request = decode_write_requested(raw)

    assert request == WriteRequest(path="notes/todo.txt", content=b"hello")


def test_decode_write_requested_rejects_invalid_json() -> None:
    with pytest.raises(MalformedPayloadError):
        decode_write_requested(b"not json at all")


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps({"content": base64.b64encode(b"hello").decode()}).encode(),  # missing path
        json.dumps({"path": "notes/todo.txt"}).encode(),  # missing content
        # empty path
        json.dumps({"path": "", "content": base64.b64encode(b"hello").decode()}).encode(),
        json.dumps({"path": "notes/todo.txt", "content": "not-base64!!"}).encode(),  # bad base64
        # wrong type
        json.dumps({"path": 5, "content": base64.b64encode(b"hello").decode()}).encode(),
    ],
)
def test_decode_write_requested_rejects_malformed_payloads(raw: bytes) -> None:
    with pytest.raises(MalformedPayloadError):
        decode_write_requested(raw)


def test_encode_create_completed_matches_schema_shape() -> None:
    occurred_at = datetime(2026, 8, 25, 18, 4, 0, tzinfo=UTC)

    raw = encode_create_completed(path="notes/todo.txt", size_bytes=5, occurred_at=occurred_at)

    assert json.loads(raw) == {
        "path": "notes/todo.txt",
        "sizeBytes": 5,
        "occurredAt": "2026-08-25T18:04:00Z",
    }


def test_encode_write_completed_matches_schema_shape() -> None:
    occurred_at = datetime(2026, 8, 25, 18, 5, 0, tzinfo=UTC)

    raw = encode_write_completed(path="notes/todo.txt", size_bytes=11, occurred_at=occurred_at)

    assert json.loads(raw) == {
        "path": "notes/todo.txt",
        "sizeBytes": 11,
        "occurredAt": "2026-08-25T18:05:00Z",
    }


def test_encode_completed_defaults_occurred_at_to_now() -> None:
    raw = encode_create_completed(path="notes/todo.txt", size_bytes=5)

    data = json.loads(raw)
    assert data["occurredAt"].endswith("Z")

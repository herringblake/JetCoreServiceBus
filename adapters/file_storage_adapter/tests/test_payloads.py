"""Tests for payloads.py (Design.md §12 Step C4) — pure encode/decode
logic, no NATS needed. Cross-checked against the schemas themselves in
test_schemas.py; this file is about the Python-level contract."""

import base64
import json
from datetime import UTC, datetime

import pytest
from file_storage_adapter.payloads import (
    FileEntry,
    MalformedPayloadError,
    PathOnlyRequest,
    WriteRequest,
    decode_path_only_request,
    decode_write_requested,
    encode_create_completed,
    encode_delete_completed,
    encode_list_completed,
    encode_operation_failed,
    encode_read_completed,
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


# --- decode_path_only_request (Design.md §13 Step F1/F2) -------------------


def test_decode_path_only_request_round_trips() -> None:
    raw = json.dumps({"path": "notes/todo.txt"}).encode()

    assert decode_path_only_request(raw) == PathOnlyRequest(path="notes/todo.txt")


def test_decode_path_only_request_rejects_empty_path_by_default() -> None:
    raw = json.dumps({"path": ""}).encode()

    with pytest.raises(MalformedPayloadError):
        decode_path_only_request(raw)


def test_decode_path_only_request_allows_empty_path_when_opted_in() -> None:
    """FileListRequested's own case (Design.md §13 Step F3) — empty path
    means watch_dir itself."""
    raw = json.dumps({"path": ""}).encode()

    assert decode_path_only_request(raw, allow_empty_path=True) == PathOnlyRequest(path="")


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps({}).encode(),  # missing path
        json.dumps({"path": 5}).encode(),  # wrong type
        b"not json at all",
    ],
)
def test_decode_path_only_request_rejects_malformed_payloads(raw: bytes) -> None:
    with pytest.raises(MalformedPayloadError):
        decode_path_only_request(raw)


# --- encode_read_completed ---------------------------------------------


def test_encode_read_completed_matches_schema_shape() -> None:
    occurred_at = datetime(2026, 8, 25, 18, 4, 0, tzinfo=UTC)

    raw = encode_read_completed(path="notes/todo.txt", content=b"hello", occurred_at=occurred_at)

    assert json.loads(raw) == {
        "path": "notes/todo.txt",
        "content": base64.b64encode(b"hello").decode(),
        "sizeBytes": 5,
        "occurredAt": "2026-08-25T18:04:00Z",
    }


# --- encode_list_completed -----------------------------------------------


def test_encode_list_completed_matches_schema_shape() -> None:
    occurred_at = datetime(2026, 8, 25, 18, 4, 0, tzinfo=UTC)
    entries = [
        FileEntry(name="todo.txt", is_directory=False, size_bytes=11),
        FileEntry(name="archive", is_directory=True, size_bytes=0),
    ]

    raw = encode_list_completed(path="notes", entries=entries, occurred_at=occurred_at)

    assert json.loads(raw) == {
        "path": "notes",
        "entries": [
            {"name": "todo.txt", "isDirectory": False, "sizeBytes": 11},
            {"name": "archive", "isDirectory": True, "sizeBytes": 0},
        ],
        "occurredAt": "2026-08-25T18:04:00Z",
    }


def test_encode_list_completed_with_no_entries() -> None:
    raw = encode_list_completed(path="empty-dir", entries=[])

    assert json.loads(raw)["entries"] == []


# --- encode_delete_completed -----------------------------------------------


def test_encode_delete_completed_matches_schema_shape() -> None:
    occurred_at = datetime(2026, 8, 25, 18, 4, 0, tzinfo=UTC)

    raw = encode_delete_completed(path="notes/todo.txt", occurred_at=occurred_at)

    assert json.loads(raw) == {"path": "notes/todo.txt", "occurredAt": "2026-08-25T18:04:00Z"}


# --- encode_operation_failed -----------------------------------------------


def test_encode_operation_failed_matches_schema_shape() -> None:
    occurred_at = datetime(2026, 8, 25, 18, 4, 0, tzinfo=UTC)

    raw = encode_operation_failed(
        path="notes/missing.txt", operation="read", reason="not_found", occurred_at=occurred_at
    )

    assert json.loads(raw) == {
        "path": "notes/missing.txt",
        "operation": "read",
        "reason": "not_found",
        "occurredAt": "2026-08-25T18:04:00Z",
    }

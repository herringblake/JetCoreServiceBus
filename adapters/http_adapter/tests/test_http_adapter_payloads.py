"""Tests for payloads.py (Design.md §13 Step H3) — pure encode logic, no
NATS needed. Cross-checked against the schema itself in
test_http_adapter_schemas.py; this file is about the Python-level
contract.

Named test_http_adapter_payloads.py, not test_payloads.py: the same
bare-filename collision Step C1 found for test_scaffold.py applies to any
identically-named file across workspace members — confirmed again here
(mypy: "Duplicate module named test_payloads", also at
file_storage_adapter's own test_payloads.py)."""

import base64
import json
from datetime import UTC, datetime

from http_adapter.payloads import encode_request_completed


def test_encode_request_completed_matches_schema_shape() -> None:
    occurred_at = datetime(2026, 8, 29, 18, 4, 0, tzinfo=UTC)

    raw = encode_request_completed(
        status="success", status_code=200, body=b'{"ok": true}', occurred_at=occurred_at
    )

    assert json.loads(raw) == {
        "status": "success",
        "statusCode": 200,
        "body": base64.b64encode(b'{"ok": true}').decode(),
        "occurredAt": "2026-08-29T18:04:00Z",
    }


def test_encode_request_completed_defaults_occurred_at_to_now() -> None:
    raw = encode_request_completed(status="error", status_code=503, body=b"")

    data = json.loads(raw)
    assert data["occurredAt"].endswith("Z")

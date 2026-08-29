"""Tests for the Step H3 JSON Schema file
(schemas/events.http.RequestCompleted.v1.json, Design.md §13) — proves
it's well-formed JSON Schema, and that the payload shape settled in §13
actually validates the way it's documented to. Mirrors
file_storage_adapter/tests/test_schemas.py's approach (Step C3).

Named test_http_adapter_schemas.py, not test_schemas.py: same
bare-filename collision as test_http_adapter_payloads.py — see that
file's docstring."""

import base64
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "schemas"


def _load(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((SCHEMAS_DIR / f"{name}.v1.json").read_text())
    return data


def test_schema_file_is_valid_draft_2020_12() -> None:
    Draft202012Validator.check_schema(_load("events.http.RequestCompleted"))


def test_success_example_validates() -> None:
    payload = {
        "status": "success",
        "statusCode": 200,
        "body": base64.b64encode(b'{"ok": true}').decode(),
        "occurredAt": "2026-08-29T18:04:00Z",
    }
    Draft202012Validator(_load("events.http.RequestCompleted")).validate(payload)


def test_error_example_validates() -> None:
    payload = {
        "status": "error",
        "statusCode": 503,
        "body": base64.b64encode(b"Service Unavailable").decode(),
        "occurredAt": "2026-08-29T18:04:00Z",
    }
    Draft202012Validator(_load("events.http.RequestCompleted")).validate(payload)

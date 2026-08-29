"""Tests for the Step C3 JSON Schema files (schemas/events.files.*.v1.json,
Design.md §12) — proves each is well-formed JSON Schema (not just valid
JSON), and that the payload shapes settled in Design.md §12's parameter
table actually validate the way they're documented to.

`jsonschema` (dev dependency group, root pyproject.toml) is a test/tooling
concern shared across adapters, not a runtime dependency of this one — the
adapter itself doesn't validate against these at runtime in v1, it just
needs to conform to them (Step C4's job)."""

import base64
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, ValidationError

SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "schemas"


def _load(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((SCHEMAS_DIR / f"{name}.v1.json").read_text())
    return data


@pytest.mark.parametrize(
    "name",
    [
        "events.files.FileWriteRequested",
        "events.files.FileCreateCompleted",
        "events.files.FileWriteCompleted",
    ],
)
def test_schema_file_is_valid_draft_2020_12(name: str) -> None:
    Draft202012Validator.check_schema(_load(name))


def test_file_write_requested_example_validates() -> None:
    payload = {"path": "notes/todo.txt", "content": base64.b64encode(b"hello").decode()}
    Draft202012Validator(_load("events.files.FileWriteRequested")).validate(payload)


def test_file_write_requested_missing_content_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Draft202012Validator(_load("events.files.FileWriteRequested")).validate(
            {"path": "notes/todo.txt"}
        )


def test_file_write_requested_unknown_field_is_rejected() -> None:
    """additionalProperties: false — catches drift between the schema and
    the actual payload shape early, rather than silently ignoring typos."""
    payload = {
        "path": "notes/todo.txt",
        "content": base64.b64encode(b"hello").decode(),
        "unexpected": "field",
    }
    with pytest.raises(ValidationError):
        Draft202012Validator(_load("events.files.FileWriteRequested")).validate(payload)


def test_file_create_completed_example_validates() -> None:
    payload = {
        "path": "notes/todo.txt",
        "sizeBytes": 5,
        "occurredAt": "2026-08-25T18:04:00Z",
    }
    Draft202012Validator(_load("events.files.FileCreateCompleted")).validate(payload)


def test_file_write_completed_example_validates() -> None:
    payload = {
        "path": "notes/todo.txt",
        "sizeBytes": 11,
        "occurredAt": "2026-08-25T18:05:00Z",
    }
    Draft202012Validator(_load("events.files.FileWriteCompleted")).validate(payload)


def test_completed_events_reject_negative_size() -> None:
    payload = {"path": "notes/todo.txt", "sizeBytes": -1, "occurredAt": "2026-08-25T18:05:00Z"}
    with pytest.raises(ValidationError):
        Draft202012Validator(_load("events.files.FileWriteCompleted")).validate(payload)

"""Payload (de)serialization matching schemas/events.files.*.v1.json
(Design.md §12 Step C3/C4, §13 Step F1) — the one place these shapes are
built/parsed, so each handler and its tests can't drift from what the
schema files actually say.

`eventPayload` (the bus envelope's encrypted field) carries these as plain
JSON bytes, pre-encryption — BusClient handles the envelope/crypto/signing
layer (Design.md §4.3); this module only knows about the logical payload
inside it.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime

from jetcore.envelope import utc_now


class MalformedPayloadError(ValueError):
    """`eventPayload` didn't decode to the shape its schemas/*.v1.json
    file describes — deterministically bad, not a transient failure (see
    each handler for how that distinction affects ack/nak)."""


@dataclass(frozen=True)
class WriteRequest:
    path: str
    content: bytes


def decode_write_requested(raw: bytes) -> WriteRequest:
    try:
        data = json.loads(raw)
        path = data["path"]
        content = base64.b64decode(data["content"], validate=True)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise MalformedPayloadError(str(exc)) from exc
    if not isinstance(path, str) or not path:
        raise MalformedPayloadError(f"path must be a non-empty string, got {path!r}")
    return WriteRequest(path=path, content=content)


@dataclass(frozen=True)
class PathOnlyRequest:
    """Shared shape for FileReadRequested/FileDeleteRequested/
    FileListRequested — all three are just `{"path": "..."}`."""

    path: str


def decode_path_only_request(raw: bytes, *, allow_empty_path: bool = False) -> PathOnlyRequest:
    """`allow_empty_path`: only `FileListRequested` permits an empty
    string (meaning watch_dir itself, Design.md §13 Step F3) — read/delete
    always need a real target, matching their schemas' `minLength: 1`."""
    try:
        data = json.loads(raw)
        path = data["path"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise MalformedPayloadError(str(exc)) from exc
    if not isinstance(path, str):
        raise MalformedPayloadError(f"path must be a string, got {path!r}")
    if not path and not allow_empty_path:
        raise MalformedPayloadError("path must be a non-empty string")
    return PathOnlyRequest(path=path)


def _iso(dt: datetime) -> str:
    # Matches the "Z"-suffixed format the envelope itself serializes with
    # (confirmed empirically in Step B2), rather than diverging into the
    # equally-valid but differently-shaped "+00:00" suffix.
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _encode_completed(*, path: str, size_bytes: int, occurred_at: datetime | None) -> bytes:
    return json.dumps(
        {
            "path": path,
            "sizeBytes": size_bytes,
            "occurredAt": _iso(occurred_at or utc_now()),
        }
    ).encode()


def encode_create_completed(
    *, path: str, size_bytes: int, occurred_at: datetime | None = None
) -> bytes:
    return _encode_completed(path=path, size_bytes=size_bytes, occurred_at=occurred_at)


def encode_write_completed(
    *, path: str, size_bytes: int, occurred_at: datetime | None = None
) -> bytes:
    return _encode_completed(path=path, size_bytes=size_bytes, occurred_at=occurred_at)


def encode_read_completed(
    *, path: str, content: bytes, occurred_at: datetime | None = None
) -> bytes:
    return json.dumps(
        {
            "path": path,
            "content": base64.b64encode(content).decode(),
            "sizeBytes": len(content),
            "occurredAt": _iso(occurred_at or utc_now()),
        }
    ).encode()


@dataclass(frozen=True)
class FileEntry:
    name: str
    is_directory: bool
    size_bytes: int


def encode_list_completed(
    *, path: str, entries: list[FileEntry], occurred_at: datetime | None = None
) -> bytes:
    return json.dumps(
        {
            "path": path,
            "entries": [
                {"name": e.name, "isDirectory": e.is_directory, "sizeBytes": e.size_bytes}
                for e in entries
            ],
            "occurredAt": _iso(occurred_at or utc_now()),
        }
    ).encode()


def encode_delete_completed(*, path: str, occurred_at: datetime | None = None) -> bytes:
    return json.dumps({"path": path, "occurredAt": _iso(occurred_at or utc_now())}).encode()


def encode_operation_failed(
    *, path: str, operation: str, reason: str, occurred_at: datetime | None = None
) -> bytes:
    return json.dumps(
        {
            "path": path,
            "operation": operation,
            "reason": reason,
            "occurredAt": _iso(occurred_at or utc_now()),
        }
    ).encode()

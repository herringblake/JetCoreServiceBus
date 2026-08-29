"""Payload (de)serialization matching schemas/events.files.*.v1.json
(Design.md §12 Step C3/C4) — the one place these three JSON shapes are
built/parsed, so the handler (commands.py) and its tests can't drift from
what the schema files actually say.

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
    """`eventPayload` didn't decode to the shape schemas/
    events.files.FileWriteRequested.v1.json describes — deterministically
    bad, not a transient failure (see commands.py for how that
    distinction affects ack/nak)."""


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

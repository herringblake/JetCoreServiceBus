"""Payload encoding matching schemas/events.http.RequestCompleted.v1.json
(Design.md §13 Step H3) — the one place this shape is built, so the
handler and its tests can't drift from what the schema file says.

No decode function here, unlike the File Storage Adapter's payloads.py —
the trigger event's own payload is passed straight through to the
external API as-is (Design.md §13), never parsed by this adapter.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime

from jetcore.envelope import utc_now


def _iso(dt: datetime) -> str:
    # Matches the "Z"-suffixed format the envelope itself serializes with
    # (confirmed empirically in Step B2) — the same convention every
    # other adapter's result-event encoding already follows.
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def encode_request_completed(
    *,
    status: str,
    status_code: int,
    body: bytes,
    occurred_at: datetime | None = None,
) -> bytes:
    return json.dumps(
        {
            "status": status,
            "statusCode": status_code,
            "body": base64.b64encode(body).decode(),
            "occurredAt": _iso(occurred_at or utc_now()),
        }
    ).encode()

"""REST API Service configuration (Design.md §13 Step I3). Extends
jetcore's AdapterSettings with the two settings this adapter needs beyond
the shared baseline — mirrors the Webhook Listener's own HTTP-front-door
shape (settings.py, Step D2) for `http_port`, plus one new setting this
track's sync-reply path needs.

Field <-> env var mapping (still JETCORE_-prefixed, per jetcore.config):
  http_port                       JETCORE_HTTP_PORT
      — the port the ASGI app binds (Design.md §12 parameter table's
        default: 8080, same as the Webhook Listener).
  default_reply_timeout_seconds   JETCORE_DEFAULT_REPLY_TIMEOUT_SECONDS
      — the cap on how long a `?wait=<seconds>` request is allowed to
        block (Design.md §13 Step I5) — a caller-supplied `wait` above
        this is silently capped, not rejected, since "wait a bit less
        than you asked" is a reasonable degrade and matches how
        `BusClient.fetch()`'s own `timeout` already works.
"""

from __future__ import annotations

from jetcore.config import AdapterSettings


class RestApiServiceSettings(AdapterSettings):
    http_port: int = 8080
    default_reply_timeout_seconds: float = 30.0

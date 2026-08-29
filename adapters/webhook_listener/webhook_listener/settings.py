"""Webhook Listener configuration (Design.md §12 Step D2). Extends
jetcore's AdapterSettings with the two settings this adapter needs beyond
the shared baseline, the same subclassing pattern
adapters/file_storage_adapter/file_storage_adapter/settings.py already
uses for real (Step C2).

Field <-> env var mapping (still JETCORE_-prefixed, per jetcore.config):
  webhook_secret   JETCORE_WEBHOOK_SECRET   — the shared secret an inbound
                                               request's X-Webhook-Secret
                                               header must match
                                               (Decision #21). A generic
                                               placeholder mechanism — no
                                               real webhook source is
                                               integrated yet (Design.md
                                               §8), so there's no
                                               source-specific scheme
                                               (HMAC signature header,
                                               etc.) to build against.
  http_port        JETCORE_HTTP_PORT        — the port the ASGI app binds
                                               (Design.md §12 parameter
                                               table: 8080).
"""

from __future__ import annotations

from jetcore.config import AdapterSettings
from pydantic import SecretStr


class WebhookListenerSettings(AdapterSettings):
    webhook_secret: SecretStr
    http_port: int = 8080

"""Webhook Sender configuration (Design.md §13 Step G2). Extends
jetcore's AdapterSettings with the two settings this adapter needs beyond
the shared baseline — the same subclassing pattern
adapters/file_storage_adapter/file_storage_adapter/settings.py and
adapters/webhook_listener/webhook_listener/settings.py already use.

This is also the first adapter to actually use AdapterSettings' own
`subjects` field for real (Design.md §7.6) — every prior adapter
hardcoded its subscribe subject(s) in code; the Webhook Sender's whole
point is relaying whatever `JETCORE_SUBJECTS` names, so it has to be
runtime-configurable.

Field <-> env var mapping (still JETCORE_-prefixed, per jetcore.config):
  target_url        JETCORE_TARGET_URL        — the external webhook URL
                                                  every relayed event gets
                                                  POSTed to (Decision #27:
                                                  one fixed target per
                                                  instance).
  outbound_secret    JETCORE_OUTBOUND_SECRET    — optional; sent as
                                                  X-Webhook-Secret on the
                                                  outbound POST if set —
                                                  the mirror image of the
                                                  Webhook Listener's own
                                                  inbound check (Decision
                                                  #21). Not required: the
                                                  receiving side may not
                                                  be another adapter of
                                                  ours.
"""

from __future__ import annotations

from jetcore.config import AdapterSettings
from pydantic import SecretStr


class WebhookSenderSettings(AdapterSettings):
    target_url: str
    outbound_secret: SecretStr | None = None

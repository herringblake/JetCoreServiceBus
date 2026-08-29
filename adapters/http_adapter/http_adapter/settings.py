"""HTTP Adapter configuration (Design.md §13 Step H2). Extends jetcore's
AdapterSettings with the two settings this adapter needs beyond the
shared baseline — mirrors webhook_sender/settings.py's shape (Step G2),
including being the second adapter to rely on AdapterSettings.subjects at
runtime (the configured trigger subject).

Field <-> env var mapping (still JETCORE_-prefixed, per jetcore.config):
  target_base_url   JETCORE_TARGET_BASE_URL   — the external REST API's
                                                 base URL every triggered
                                                 call is made against
                                                 (Decision #27: one fixed
                                                 target per instance).
  auth_token         JETCORE_AUTH_TOKEN         — optional; sent as
                                                 `Authorization: Bearer
                                                 <token>` if set. Not
                                                 required: the target API
                                                 may not need auth, or may
                                                 use a scheme this adapter
                                                 doesn't need to know
                                                 about (e.g. an API key
                                                 baked into the URL).
"""

from __future__ import annotations

from jetcore.config import AdapterSettings
from pydantic import SecretStr


class HttpAdapterSettings(AdapterSettings):
    target_base_url: str
    auth_token: SecretStr | None = None

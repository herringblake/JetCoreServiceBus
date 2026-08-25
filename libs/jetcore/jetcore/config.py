"""Adapter configuration loading (Design.md §7.6) — Step B4 (Design.md §11
Track B). Env-var/`.env`-driven, via `pydantic-settings`.

Field <-> env var mapping (all `JETCORE_`-prefixed, per §7.6):
  service_id       JETCORE_SERVICE_ID       — must match this instance's
                                          serviceId in adapter_identities.yaml
                                          (Step A2)
  nats_url         JETCORE_NATS_URL         — defaults to the docker-compose
                                          service name (Step A6)
  nats_creds_path  JETCORE_NATS_CREDS_PATH  — the .creds file Step A3's
                                          bootstrap_auth.sh generated for
                                          this service_id (JWT+nkey bundled)
  subjects         JETCORE_SUBJECTS         — subjects this instance subscribes
                                          to / registers itself as a
                                          recipient for (Design.md §4.5).
                                          NOT publish subjects — what an
                                          adapter publishes to is inherent
                                          to its own logic (e.g. "reply to
                                          FileWriteRequested with
                                          FileWriteCompleted"), not
                                          something you'd reconfigure
                                          without also changing code.
  log_level        JETCORE_LOG_LEVEL        — defaults to INFO
"""

from __future__ import annotations

import json
from typing import Annotated, Literal

from pydantic import FilePath, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class AdapterSettings(BaseSettings):
    """Base config every adapter loads. Subclass to add adapter-specific
    settings (still `JETCORE_`-prefixed) — pydantic-settings merges them."""

    model_config = SettingsConfigDict(env_prefix="JETCORE_", env_file=".env", extra="ignore")

    service_id: str
    nats_url: str = "nats://nats:4222"
    # FilePath: pydantic's built-in "must exist and be a file" type — fails
    # fast at config-load time rather than surfacing as a confusing
    # connection error later (Step B6).
    nats_creds_path: FilePath
    # NoDecode: pydantic-settings otherwise tries its own JSON-decode of the
    # raw env string *before* our field_validator below ever runs — plain
    # comma-separated text fails there with a low-level SettingsError,
    # confirmed by testing, not assumed. NoDecode defers entirely to the
    # validator instead.
    subjects: Annotated[list[str], NoDecode] = []
    log_level: LogLevel = "INFO"

    @field_validator("subjects", mode="before")
    @classmethod
    def _parse_subjects(cls, value: object) -> object:
        """Accept a JSON array string (pydantic-settings' native list
        parsing, confirmed by testing — plain comma-separated fails there)
        OR a plain comma-separated string (much more natural to hand-write
        in a docker-compose `environment:` block or a .env file — no JSON
        quoting to get right) OR an already-parsed list (constructing this
        directly in Python/tests)."""
        if not isinstance(value, str):
            return value
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            return json.loads(stripped)
        return [s.strip() for s in stripped.split(",") if s.strip()]

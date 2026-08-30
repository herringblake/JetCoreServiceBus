"""Tests for settings.py (Design.md §13 Step I3)."""

from pathlib import Path

import pytest
from rest_api_service.settings import RestApiServiceSettings


@pytest.fixture
def creds_file(tmp_path: Path) -> Path:
    path = tmp_path / "test.creds"
    path.write_text("not a real creds file, just needs to exist")
    return path


def _env(monkeypatch: pytest.MonkeyPatch, creds_file: Path, **extra: str) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "rest-api-service-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_http_port_defaults_to_8080(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file)

    settings = RestApiServiceSettings(_env_file=None)

    assert settings.http_port == 8080
    assert settings.service_id == "rest-api-service-01"  # inherited field still works


def test_http_port_is_loaded_from_env(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, JETCORE_HTTP_PORT="9090")

    settings = RestApiServiceSettings(_env_file=None)

    assert settings.http_port == 9090


def test_default_reply_timeout_seconds_defaults_to_30(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(monkeypatch, creds_file)

    settings = RestApiServiceSettings(_env_file=None)

    assert settings.default_reply_timeout_seconds == 30.0


def test_default_reply_timeout_seconds_is_loaded_from_env(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(monkeypatch, creds_file, JETCORE_DEFAULT_REPLY_TIMEOUT_SECONDS="5")

    settings = RestApiServiceSettings(_env_file=None)

    assert settings.default_reply_timeout_seconds == 5.0

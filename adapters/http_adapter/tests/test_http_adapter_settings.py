"""Tests for settings.py (Design.md §13 Step H2)."""

from pathlib import Path

import pytest
from http_adapter.settings import HttpAdapterSettings
from pydantic import ValidationError


@pytest.fixture
def creds_file(tmp_path: Path) -> Path:
    path = tmp_path / "test.creds"
    path.write_text("not a real creds file, just needs to exist")
    return path


def _env(monkeypatch: pytest.MonkeyPatch, creds_file: Path, **extra: str) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "http-adapter-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_loads_target_base_url_from_env(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, JETCORE_TARGET_BASE_URL="https://example.test/api")

    settings = HttpAdapterSettings(_env_file=None)

    assert settings.target_base_url == "https://example.test/api"
    assert settings.service_id == "http-adapter-01"  # inherited field still works


def test_auth_token_defaults_to_none(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, JETCORE_TARGET_BASE_URL="https://example.test/api")

    settings = HttpAdapterSettings(_env_file=None)

    assert settings.auth_token is None


def test_auth_token_is_loaded_and_masked(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(
        monkeypatch,
        creds_file,
        JETCORE_TARGET_BASE_URL="https://example.test/api",
        JETCORE_AUTH_TOKEN="s3cr3t-token",
    )

    settings = HttpAdapterSettings(_env_file=None)

    assert settings.auth_token is not None
    assert settings.auth_token.get_secret_value() == "s3cr3t-token"
    assert "s3cr3t-token" not in repr(settings)


def test_missing_target_base_url_raises(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "http-adapter-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))

    with pytest.raises(ValidationError):
        HttpAdapterSettings(_env_file=None)


def test_subjects_field_still_works_via_inheritance(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(
        monkeypatch,
        creds_file,
        JETCORE_TARGET_BASE_URL="https://example.test/api",
        JETCORE_SUBJECTS="events.orders.OrderCreated",
    )

    settings = HttpAdapterSettings(_env_file=None)

    assert settings.subjects == ["events.orders.OrderCreated"]

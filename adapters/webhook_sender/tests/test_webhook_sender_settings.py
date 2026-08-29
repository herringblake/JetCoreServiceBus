"""Tests for settings.py (Design.md §13 Step G2)."""

from pathlib import Path

import pytest
from pydantic import ValidationError
from webhook_sender.settings import WebhookSenderSettings


@pytest.fixture
def creds_file(tmp_path: Path) -> Path:
    path = tmp_path / "test.creds"
    path.write_text("not a real creds file, just needs to exist")
    return path


def _env(monkeypatch: pytest.MonkeyPatch, creds_file: Path, **extra: str) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "webhook-sender-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_loads_target_url_from_env(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, JETCORE_TARGET_URL="https://example.test/hooks")

    settings = WebhookSenderSettings(_env_file=None)

    assert settings.target_url == "https://example.test/hooks"
    assert settings.service_id == "webhook-sender-01"  # inherited field still works


def test_outbound_secret_defaults_to_none(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(monkeypatch, creds_file, JETCORE_TARGET_URL="https://example.test/hooks")

    settings = WebhookSenderSettings(_env_file=None)

    assert settings.outbound_secret is None


def test_outbound_secret_is_loaded_and_masked(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(
        monkeypatch,
        creds_file,
        JETCORE_TARGET_URL="https://example.test/hooks",
        JETCORE_OUTBOUND_SECRET="s3cr3t",
    )

    settings = WebhookSenderSettings(_env_file=None)

    assert settings.outbound_secret is not None
    assert settings.outbound_secret.get_secret_value() == "s3cr3t"
    assert "s3cr3t" not in repr(settings)


def test_missing_target_url_raises(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "webhook-sender-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))

    with pytest.raises(ValidationError):
        WebhookSenderSettings(_env_file=None)


def test_subjects_field_still_works_via_inheritance(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    """The first adapter to actually rely on AdapterSettings.subjects at
    runtime (Design.md §13 Step G2's own docstring) — confirms it's not
    broken by subclassing, on top of B4's own test already proving the
    subclassing pattern itself works."""
    _env(
        monkeypatch,
        creds_file,
        JETCORE_TARGET_URL="https://example.test/hooks",
        JETCORE_SUBJECTS="events.orders.OrderCreated",
    )

    settings = WebhookSenderSettings(_env_file=None)

    assert settings.subjects == ["events.orders.OrderCreated"]

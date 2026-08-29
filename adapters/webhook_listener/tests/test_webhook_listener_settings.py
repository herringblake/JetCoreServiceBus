"""Tests for settings.py (Design.md §12 Step D2).

Named test_webhook_listener_settings.py, not test_settings.py: the same
bare-filename collision Step C1 found for test_scaffold.py applies to any
identically-named file across workspace members — confirmed again here
(mypy: "Duplicate module named test_settings", also at
file_storage_adapter's own test_settings.py) before it could bite the
full suite."""

from pathlib import Path

import pytest
from pydantic import ValidationError
from webhook_listener.settings import WebhookListenerSettings


@pytest.fixture
def creds_file(tmp_path: Path) -> Path:
    path = tmp_path / "test.creds"
    path.write_text("not a real creds file, just needs to exist")
    return path


def _env(monkeypatch: pytest.MonkeyPatch, creds_file: Path, **extra: str) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "webhook-listener-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_loads_webhook_secret_from_env(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, JETCORE_WEBHOOK_SECRET="s3cr3t")

    settings = WebhookListenerSettings(_env_file=None)

    assert settings.webhook_secret.get_secret_value() == "s3cr3t"
    assert settings.service_id == "webhook-listener-01"  # inherited field still works


def test_http_port_defaults_to_8080(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, JETCORE_WEBHOOK_SECRET="s3cr3t")

    settings = WebhookListenerSettings(_env_file=None)

    assert settings.http_port == 8080


def test_http_port_is_overridable(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, JETCORE_WEBHOOK_SECRET="s3cr3t", JETCORE_HTTP_PORT="9000")

    settings = WebhookListenerSettings(_env_file=None)

    assert settings.http_port == 9000


def test_missing_webhook_secret_raises(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "webhook-listener-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))

    with pytest.raises(ValidationError):
        WebhookListenerSettings(_env_file=None)


def test_webhook_secret_is_masked_in_repr(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    """SecretStr's whole point — a secret shouldn't leak into logs via an
    incidental repr()/str() of the settings object."""
    _env(monkeypatch, creds_file, JETCORE_WEBHOOK_SECRET="s3cr3t")

    settings = WebhookListenerSettings(_env_file=None)

    assert "s3cr3t" not in repr(settings)
    assert "s3cr3t" not in str(settings.webhook_secret)

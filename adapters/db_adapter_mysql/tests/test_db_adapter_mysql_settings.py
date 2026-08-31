"""Tests for settings.py (Design.md §13 Step J3)."""

from pathlib import Path

import pytest
from db_adapter_mysql.settings import DbAdapterSettings
from pydantic import ValidationError


@pytest.fixture
def creds_file(tmp_path: Path) -> Path:
    path = tmp_path / "test.creds"
    path.write_text("not a real creds file, just needs to exist")
    return path


def _env(monkeypatch: pytest.MonkeyPatch, creds_file: Path, **extra: str) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "db-adapter-mysql-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    monkeypatch.setenv("JETCORE_MYSQL_WRITE_USER", "jetcore_write")
    monkeypatch.setenv("JETCORE_MYSQL_WRITE_PASSWORD", "write-secret")
    monkeypatch.setenv("JETCORE_MYSQL_CDC_USER", "jetcore_cdc")
    monkeypatch.setenv("JETCORE_MYSQL_CDC_PASSWORD", "cdc-secret")
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_mysql_host_port_database_default(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(monkeypatch, creds_file)

    settings = DbAdapterSettings(_env_file=None)

    assert settings.mysql_host == "mysql"
    assert settings.mysql_port == 3306
    assert settings.mysql_database == "jetcore"
    assert settings.service_id == "db-adapter-mysql-01"  # inherited field still works


def test_mysql_host_port_database_are_loaded_from_env(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(
        monkeypatch,
        creds_file,
        JETCORE_MYSQL_HOST="db.example.test",
        JETCORE_MYSQL_PORT="3307",
        JETCORE_MYSQL_DATABASE="other_db",
    )

    settings = DbAdapterSettings(_env_file=None)

    assert settings.mysql_host == "db.example.test"
    assert settings.mysql_port == 3307
    assert settings.mysql_database == "other_db"


def test_write_and_cdc_credentials_are_distinct_and_masked(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(monkeypatch, creds_file)

    settings = DbAdapterSettings(_env_file=None)

    assert settings.mysql_write_user == "jetcore_write"
    assert settings.mysql_write_password.get_secret_value() == "write-secret"
    assert settings.mysql_cdc_user == "jetcore_cdc"
    assert settings.mysql_cdc_password.get_secret_value() == "cdc-secret"
    # Two DISTINCT identities, not one set of credentials reused for both
    # roles (Design.md §13 Step J3) — a real, not just cosmetic, check.
    assert settings.mysql_write_user != settings.mysql_cdc_user
    assert "write-secret" not in repr(settings)
    assert "cdc-secret" not in repr(settings)


def test_missing_write_credentials_raises(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "db-adapter-mysql-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    monkeypatch.setenv("JETCORE_MYSQL_CDC_USER", "jetcore_cdc")
    monkeypatch.setenv("JETCORE_MYSQL_CDC_PASSWORD", "cdc-secret")

    with pytest.raises(ValidationError):
        DbAdapterSettings(_env_file=None)


def test_missing_cdc_credentials_raises(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "db-adapter-mysql-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    monkeypatch.setenv("JETCORE_MYSQL_WRITE_USER", "jetcore_write")
    monkeypatch.setenv("JETCORE_MYSQL_WRITE_PASSWORD", "write-secret")

    with pytest.raises(ValidationError):
        DbAdapterSettings(_env_file=None)

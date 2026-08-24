"""Tests for config.py (Design.md §7.6) — Step B4."""

from pathlib import Path

import pytest
from gsb_core.config import AdapterSettings
from pydantic import ValidationError


@pytest.fixture
def creds_file(tmp_path: Path) -> Path:
    """A file that merely needs to exist — AdapterSettings only validates
    that the path is real, not that it's a well-formed .creds file (that's
    for whoever actually connects with it, Step B6)."""
    path = tmp_path / "test.creds"
    path.write_text("not a real creds file, just needs to exist")
    return path


def _env(monkeypatch: pytest.MonkeyPatch, creds_file: Path, **extra: str) -> None:
    monkeypatch.setenv("GSB_SERVICE_ID", "file-storage-01")
    monkeypatch.setenv("GSB_NATS_CREDS_PATH", str(creds_file))
    for key, value in extra.items():
        monkeypatch.setenv(key, value)


def test_loads_required_fields_from_env(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file)

    settings = AdapterSettings(_env_file=None)

    assert settings.service_id == "file-storage-01"
    assert settings.nats_creds_path == creds_file


def test_defaults(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file)

    settings = AdapterSettings(_env_file=None)

    assert settings.nats_url == "nats://nats:4222"
    assert settings.subjects == []
    assert settings.log_level == "INFO"


def test_subjects_comma_separated(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(
        monkeypatch,
        creds_file,
        GSB_SUBJECTS="events.files.FileWriteRequested, events.files.FileReadRequested",
    )

    settings = AdapterSettings(_env_file=None)

    assert settings.subjects == [
        "events.files.FileWriteRequested",
        "events.files.FileReadRequested",
    ]


def test_subjects_json_array(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, GSB_SUBJECTS='["events.a", "events.b"]')

    settings = AdapterSettings(_env_file=None)

    assert settings.subjects == ["events.a", "events.b"]


def test_subjects_empty_string_means_empty_list(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(monkeypatch, creds_file, GSB_SUBJECTS="")

    settings = AdapterSettings(_env_file=None)

    assert settings.subjects == []


def test_missing_required_service_id_raises(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    monkeypatch.setenv("GSB_NATS_CREDS_PATH", str(creds_file))

    with pytest.raises(ValidationError):
        AdapterSettings(_env_file=None)


def test_nonexistent_creds_path_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GSB_SERVICE_ID", "file-storage-01")
    monkeypatch.setenv("GSB_NATS_CREDS_PATH", str(tmp_path / "does-not-exist.creds"))

    with pytest.raises(ValidationError):
        AdapterSettings(_env_file=None)


def test_invalid_log_level_raises(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    _env(monkeypatch, creds_file, GSB_LOG_LEVEL="VERBOSE")

    with pytest.raises(ValidationError):
        AdapterSettings(_env_file=None)


def test_subclassing_adds_adapter_specific_settings(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path
) -> None:
    _env(monkeypatch, creds_file, GSB_WATCH_DIR="/data/files")

    class FileStorageSettings(AdapterSettings):
        watch_dir: str

    settings = FileStorageSettings(_env_file=None)

    assert settings.watch_dir == "/data/files"
    assert settings.service_id == "file-storage-01"  # inherited field still works

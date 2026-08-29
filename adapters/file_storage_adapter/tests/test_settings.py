"""Tests for settings.py (Design.md §12 Step C2)."""

from pathlib import Path

import pytest
from file_storage_adapter.settings import FileStorageSettings
from pydantic import ValidationError


@pytest.fixture
def creds_file(tmp_path: Path) -> Path:
    path = tmp_path / "test.creds"
    path.write_text("not a real creds file, just needs to exist")
    return path


def _env(monkeypatch: pytest.MonkeyPatch, creds_file: Path, watch_dir: Path) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "file-storage-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))
    monkeypatch.setenv("JETCORE_WATCH_DIR", str(watch_dir))


def test_loads_watch_dir_from_env(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path, tmp_path: Path
) -> None:
    watch_dir = tmp_path / "files"
    watch_dir.mkdir()
    _env(monkeypatch, creds_file, watch_dir)

    settings = FileStorageSettings(_env_file=None)

    assert settings.watch_dir == watch_dir
    assert settings.service_id == "file-storage-01"  # inherited field still works


def test_nonexistent_watch_dir_raises(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path, tmp_path: Path
) -> None:
    _env(monkeypatch, creds_file, tmp_path / "does-not-exist")

    with pytest.raises(ValidationError):
        FileStorageSettings(_env_file=None)


def test_watch_dir_that_is_a_file_not_a_directory_raises(
    monkeypatch: pytest.MonkeyPatch, creds_file: Path, tmp_path: Path
) -> None:
    not_a_dir = tmp_path / "not-a-dir.txt"
    not_a_dir.write_text("a file, not a directory")
    _env(monkeypatch, creds_file, not_a_dir)

    with pytest.raises(ValidationError):
        FileStorageSettings(_env_file=None)


def test_missing_watch_dir_env_raises(monkeypatch: pytest.MonkeyPatch, creds_file: Path) -> None:
    monkeypatch.setenv("JETCORE_SERVICE_ID", "file-storage-01")
    monkeypatch.setenv("JETCORE_NATS_CREDS_PATH", str(creds_file))

    with pytest.raises(ValidationError):
        FileStorageSettings(_env_file=None)

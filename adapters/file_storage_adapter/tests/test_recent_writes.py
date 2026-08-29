"""Tests for recent_writes.py (Design.md §12 Step C5) — pure logic, no
NATS or filesystem watching needed for the class itself (this test module
still runs under the directory's live-NATS autouse fixture like every
other test here, same as test_paths.py/test_payloads.py already do)."""

import time
from pathlib import Path

from file_storage_adapter.recent_writes import RecentWrites


def test_unmarked_path_is_not_recent(tmp_path: Path) -> None:
    recent = RecentWrites()
    assert recent.was_recent(tmp_path / "never-marked.txt") is False


def test_marked_path_is_recent(tmp_path: Path) -> None:
    recent = RecentWrites()
    path = tmp_path / "marked.txt"

    recent.mark(path)

    assert recent.was_recent(path) is True


def test_mark_expires_after_ttl(tmp_path: Path) -> None:
    recent = RecentWrites(ttl_seconds=0.05)
    path = tmp_path / "marked.txt"

    recent.mark(path)
    assert recent.was_recent(path) is True

    time.sleep(0.1)

    assert recent.was_recent(path) is False


def test_marking_one_path_does_not_affect_another(tmp_path: Path) -> None:
    recent = RecentWrites()
    recent.mark(tmp_path / "a.txt")

    assert recent.was_recent(tmp_path / "b.txt") is False

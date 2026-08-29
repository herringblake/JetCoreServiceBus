"""Tests for paths.py (Design.md §12 Step C4) — path-traversal rejection is
the security-relevant behavior here, so it gets pure, fast, no-NATS-needed
unit tests of its own rather than only being covered indirectly."""

from pathlib import Path

import pytest
from file_storage_adapter.paths import PathTraversalError, resolve_within


@pytest.fixture
def watch_dir(tmp_path: Path) -> Path:
    d = tmp_path / "watch"
    d.mkdir()
    return d


def test_simple_relative_path_resolves_inside_watch_dir(watch_dir: Path) -> None:
    result = resolve_within(watch_dir, "notes/todo.txt")
    assert result == (watch_dir / "notes" / "todo.txt").resolve()


def test_dotdot_traversal_is_rejected(watch_dir: Path) -> None:
    with pytest.raises(PathTraversalError):
        resolve_within(watch_dir, "../outside.txt")


def test_deeper_dotdot_traversal_is_rejected(watch_dir: Path) -> None:
    with pytest.raises(PathTraversalError):
        resolve_within(watch_dir, "a/b/../../../outside.txt")


def test_absolute_path_is_rejected(watch_dir: Path) -> None:
    """Path silently discards `root` when the right-hand side is itself
    absolute (confirmed: Path("/a") / "/etc/passwd" == Path("/etc/passwd"))
    — this is exactly the case that would otherwise bypass watch_dir."""
    with pytest.raises(PathTraversalError):
        resolve_within(watch_dir, "/etc/passwd")


def test_symlink_escape_is_rejected(watch_dir: Path) -> None:
    outside = watch_dir.parent / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("nope")
    (watch_dir / "escape").symlink_to(outside)

    with pytest.raises(PathTraversalError):
        resolve_within(watch_dir, "escape/secret.txt")


def test_path_equal_to_watch_dir_itself_is_allowed(watch_dir: Path) -> None:
    # Degenerate case (an empty-ish "." path) — should resolve to watch_dir
    # itself without raising, since it doesn't escape it.
    assert resolve_within(watch_dir, ".") == watch_dir.resolve()

"""Path safety for the File Storage Adapter (Design.md §12 Step C4).

A `FileWriteRequested` command's `path` field comes from whatever published
it — ultimately, in Phase 2, an HTTP caller via the Webhook Listener. A
command-driven adapter that writes wherever a message tells it to is a real
path-traversal risk (Design.md §12's parameter table); `resolve_within` is
the one place that boundary is enforced, so every write goes through it
rather than trusting `path` directly.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath


class PathTraversalError(ValueError):
    """`path` would resolve to somewhere outside the adapter's watch_dir."""

    def __init__(self, path: str) -> None:
        super().__init__(f"path {path!r} escapes the adapter's watch_dir")
        self.path = path


def resolve_within(root: Path, relative: str) -> Path:
    """Resolves `relative` against `root` and confirms the result is
    actually inside `root`, raising PathTraversalError otherwise.

    Two things have to be caught, both confirmed by testing rather than
    assumed:
      - `..` traversal (e.g. "../../etc/passwd") — caught by resolving
        both paths (which normalizes `..` components, following any real
        symlinks along the way) and checking containment.
      - An absolute `path` (e.g. "/etc/passwd") — `Path` silently DISCARDS
        `root` entirely when the right-hand side of `/` is itself
        absolute (confirmed: `Path("/a") / "/etc/passwd" ==
        Path("/etc/passwd")`, not an error and not a relative join). The
        containment check below still catches this (the result ends up
        outside `root`), but `relative` is rejected explicitly first
        instead, so an absolute-path attempt fails fast on the string
        itself rather than relying on `.resolve()`'s filesystem I/O to
        (correctly, but less obviously) reject it after the fact.
    """
    if PurePosixPath(relative).is_absolute():
        raise PathTraversalError(relative)

    root_resolved = root.resolve()
    candidate = (root_resolved / relative).resolve()

    if candidate != root_resolved and root_resolved not in candidate.parents:
        raise PathTraversalError(relative)

    return candidate

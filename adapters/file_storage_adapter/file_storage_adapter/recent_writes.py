"""Coordination between write_handler.py (Step C4) and create_watch.py
(Step C5) — both watch the same watch_dir, for different reasons, and a
raw filesystem event can't tell "a file the write-path handler just
created via a command" apart from "a file that genuinely appeared from
outside" (Decision #19's actual distinction). Without this, every
command-triggered creation would ALSO trip the external-creation watch and
publish a second, spurious FileCreateCompleted with no correlationId.

A short TTL, not a permanent record: this only needs to bridge the gap
between the write finishing and watchfiles' own debounced event for it
arriving (default debounce 1600ms, confirmed via `watchfiles.awatch`'s
signature) — not track write history in general.
"""

from __future__ import annotations

import time
from pathlib import Path

DEFAULT_TTL_SECONDS = 5.0  # comfortably above watchfiles' 1600ms default
# debounce + step, with margin for scheduling jitter under load.


class RecentWrites:
    def __init__(self, *, ttl_seconds: float = DEFAULT_TTL_SECONDS) -> None:
        self._ttl_seconds = ttl_seconds
        self._marked_at: dict[Path, float] = {}

    def mark(self, path: Path) -> None:
        self._marked_at[path] = time.monotonic()

    def was_recent(self, path: Path) -> bool:
        self._prune()
        return path in self._marked_at

    def _prune(self) -> None:
        cutoff = time.monotonic() - self._ttl_seconds
        expired = [p for p, marked_at in self._marked_at.items() if marked_at < cutoff]
        for p in expired:
            del self._marked_at[p]

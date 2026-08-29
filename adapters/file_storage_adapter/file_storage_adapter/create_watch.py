"""External-creation watch (Design.md §12 Step C5, Decision #19) — watches
watch_dir for files that appear with NO preceding FileWriteRequested
command (e.g. a shared/remote directory another process writes into
directly) and publishes FileCreateCompleted for them, with no
correlationId since there's no request to correlate against. Creation-
detection only — externally-triggered *updates* to already-existing files
remain out of scope (Design.md §9), unchanged by this step.

Runs concurrently with write_handler.py's consume loop (Step C6 wires
both via asyncio.gather, the shape Step B7 already proved), not instead
of it. See recent_writes.py for why a command-triggered creation doesn't
also get reported here.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from jetcore.bus_client import BusClient
from watchfiles import Change, awatch

from file_storage_adapter.payloads import encode_create_completed
from file_storage_adapter.recent_writes import RecentWrites

logger = logging.getLogger(__name__)

CREATE_COMPLETED_SUBJECT = "events.files.FileCreateCompleted"

# A raw "added" filesystem event fires when a file is first created, not
# necessarily when whatever's writing it is done — confirmed by testing
# with a deliberately slow multi-chunk write (watchfiles' own docs make no
# "settled" guarantee either way). Polls st_size until it stops changing
# for a few consecutive reads before treating the file as done.
_SETTLE_POLL_INTERVAL_SECONDS = 0.2
_SETTLE_STABLE_READS = 3


async def wait_until_settled(
    path: Path,
    *,
    poll_interval: float = _SETTLE_POLL_INTERVAL_SECONDS,
    stable_reads: int = _SETTLE_STABLE_READS,
) -> int | None:
    """Polls `path`'s size until it's unchanged across `stable_reads`
    consecutive checks, returning the final size — or None if the file
    disappeared before settling (e.g. a genuinely transient temp file)."""
    last_size = -1
    consecutive_matches = 0
    while consecutive_matches < stable_reads:
        await asyncio.sleep(poll_interval)
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return None
        if size == last_size:
            consecutive_matches += 1
        else:
            consecutive_matches = 0
            last_size = size
    return last_size


class ExternalCreationWatch:
    def __init__(
        self,
        client: BusClient,
        *,
        watch_dir: Path,
        recent_writes: RecentWrites,
    ) -> None:
        self._client = client
        self._watch_dir = watch_dir
        self._recent_writes = recent_writes

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        """Runs until `stop_event` is set (or the enclosing task is
        cancelled). `watchfiles.awatch`'s own `stop_event` parameter is the
        documented clean-shutdown mechanism — used here rather than relying
        solely on task cancellation reaching its internal watcher thread."""
        async for changes in awatch(self._watch_dir, stop_event=stop_event):
            for change, raw_path in changes:
                if change is not Change.added:
                    continue
                path = Path(raw_path)
                if not path.is_file():
                    continue  # a new directory (e.g. from mkdir) is not a file creation
                if self._recent_writes.was_recent(path):
                    continue  # our own write_handler created this — not "external"
                await self._handle_new_file(path)

    async def _handle_new_file(self, path: Path) -> None:
        size = await wait_until_settled(path)
        if size is None:
            logger.info("file %s disappeared before settling — skipping", path)
            return

        relative = path.relative_to(self._watch_dir)
        await self._client.publish(
            CREATE_COMPLETED_SUBJECT,
            encode_create_completed(path=str(relative), size_bytes=size),
            event_type="FileCreateCompleted",
        )

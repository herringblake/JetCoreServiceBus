"""Write-path command handler (Design.md §12 Step C4) — the first real use
of BusClient outside jetcore's own test/example code. Subscribes to
events.files.FileWriteRequested, writes the file (auto-creating it if
missing, per Decision #19), and publishes the matching completion event.

Named write_handler.py, not the more generic commands.py: Phase 2 only
builds the write path (Design.md §9/§12) — list/read/delete handlers are
deferred to Phase 3 and will get their own modules when they exist, rather
than this one growing to cover work that hasn't been scoped yet.

Known v1 simplification, not addressed here: `resolve_within` (paths.py)
validates against the filesystem at call time, but the write itself
happens slightly later — a TOCTOU window exists if something replaces a
path component with a symlink in between. Not defended against (would
need O_NOFOLLOW / openat2 RESOLVE_BENEATH-style primitives); acceptable
for a local, single-writer dev/demo deployment, not a hardened multi-
tenant filesystem sandbox.
"""

from __future__ import annotations

import logging
from pathlib import Path

import aiofiles
from jetcore.bus_client import BusClient, ReceivedEvent

from file_storage_adapter.paths import PathTraversalError, resolve_within
from file_storage_adapter.payloads import (
    MalformedPayloadError,
    decode_write_requested,
    encode_create_completed,
    encode_write_completed,
)
from file_storage_adapter.recent_writes import RecentWrites

logger = logging.getLogger(__name__)

WRITE_REQUESTED_SUBJECT = "events.files.FileWriteRequested"
CREATE_COMPLETED_SUBJECT = "events.files.FileCreateCompleted"
WRITE_COMPLETED_SUBJECT = "events.files.FileWriteCompleted"


class WriteCommandHandler:
    """Owns the write-path half of the File Storage Adapter. The
    external-creation watch (Step C5) is a separate, concurrently-running
    piece that publishes FileCreateCompleted independently of this class —
    `recent_writes`, when given, is the coordination point between the two
    (see recent_writes.py): a command-triggered creation would otherwise
    also trip the raw filesystem watch and get a second, spurious
    FileCreateCompleted with no correlationId."""

    def __init__(
        self,
        client: BusClient,
        *,
        watch_dir: Path,
        recent_writes: RecentWrites | None = None,
    ) -> None:
        self._client = client
        self._watch_dir = watch_dir
        self._recent_writes = recent_writes

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(WRITE_REQUESTED_SUBJECT, durable_name=durable_name)

    async def run_once(self, durable_name: str, *, batch: int = 10, timeout: float = 5.0) -> int:
        """Fetches and handles up to `batch` pending commands, returning how
        many were fetched. Split out from an infinite loop so both tests and
        the real main loop (Step C6) can drive it directly."""
        events = await self._client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in events:
            await self._handle(event)
        return len(events)

    async def _handle(self, event: ReceivedEvent) -> None:
        try:
            request = decode_write_requested(event.payload)
        except MalformedPayloadError:
            # Deterministically bad — redelivery would never fix a payload
            # that doesn't parse. Ack (stop redelivery) rather than nak,
            # same reasoning as the path-traversal case below.
            logger.exception(
                "malformed FileWriteRequested payload for event %s — acking, not retrying",
                event.details.event_id,
            )
            await event.ack()
            return

        try:
            target = resolve_within(self._watch_dir, request.path)
        except PathTraversalError:
            logger.warning(
                "rejected FileWriteRequested for event %s: path %r escapes watch_dir "
                "— acking, not retrying",
                event.details.event_id,
                request.path,
            )
            await event.ack()
            return

        existed_before = target.exists()
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(target, "wb") as f:
                await f.write(request.content)
        except OSError:
            # Unlike the two cases above, this one might genuinely be
            # transient (disk full, permission hiccup) — worth a retry via
            # redelivery rather than giving up immediately.
            logger.exception(
                "I/O error writing %s for event %s — leaving unacked for redelivery",
                target,
                event.details.event_id,
            )
            await event.nak()
            return

        # Marked regardless of create-vs-update — cheap, and an update
        # could in principle still surface as an "added" event on some
        # backend/edge case, not just genuinely-new files.
        if self._recent_writes is not None:
            self._recent_writes.mark(target)

        size_bytes = len(request.content)
        if existed_before:
            await self._client.publish(
                WRITE_COMPLETED_SUBJECT,
                encode_write_completed(path=request.path, size_bytes=size_bytes),
                event_type="FileWriteCompleted",
                correlation_id=event.details.event_id,
            )
        else:
            await self._client.publish(
                CREATE_COMPLETED_SUBJECT,
                encode_create_completed(path=request.path, size_bytes=size_bytes),
                event_type="FileCreateCompleted",
                correlation_id=event.details.event_id,
            )

        await event.ack()

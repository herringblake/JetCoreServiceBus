"""Read-path command handler (Design.md §13 Step F2). Subscribes to
events.files.FileReadRequested, reads the file, and publishes
FileReadCompleted or FileOperationFailed (Decision #23) — mirrors
write_handler.py's shape and ack/nak reasoning exactly.
"""

from __future__ import annotations

import logging
from pathlib import Path

import aiofiles
from jetcore.bus_client import BusClient, ReceivedEvent

from file_storage_adapter.paths import PathTraversalError, resolve_within
from file_storage_adapter.payloads import (
    MalformedPayloadError,
    decode_path_only_request,
    encode_operation_failed,
    encode_read_completed,
)

logger = logging.getLogger(__name__)

READ_REQUESTED_SUBJECT = "events.files.FileReadRequested"
READ_COMPLETED_SUBJECT = "events.files.FileReadCompleted"
OPERATION_FAILED_SUBJECT = "events.files.FileOperationFailed"


class ReadCommandHandler:
    def __init__(self, client: BusClient, *, watch_dir: Path) -> None:
        self._client = client
        self._watch_dir = watch_dir

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(READ_REQUESTED_SUBJECT, durable_name=durable_name)

    async def run_once(self, durable_name: str, *, batch: int = 10, timeout: float = 5.0) -> int:
        events = await self._client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in events:
            await self._handle(event)
        return len(events)

    async def _handle(self, event: ReceivedEvent) -> None:
        try:
            request = decode_path_only_request(event.payload)
        except MalformedPayloadError:
            logger.exception(
                "malformed FileReadRequested payload for event %s — acking, not retrying",
                event.details.event_id,
            )
            await event.ack()
            return

        try:
            target = resolve_within(self._watch_dir, request.path)
        except PathTraversalError:
            logger.warning(
                "rejected FileReadRequested for event %s: path %r escapes watch_dir "
                "— acking, not retrying",
                event.details.event_id,
                request.path,
            )
            await event.ack()
            return

        # A directory at this path is treated the same as "not found" for
        # a read — the schema's reason enum doesn't need a third value
        # just for this edge case (Design.md §13's FileOperationFailed
        # scoping).
        if not target.is_file():
            await self._client.publish(
                OPERATION_FAILED_SUBJECT,
                encode_operation_failed(path=request.path, operation="read", reason="not_found"),
                event_type="FileOperationFailed",
                correlation_id=event.details.event_id,
            )
            await event.ack()
            return

        try:
            async with aiofiles.open(target, "rb") as f:
                content = await f.read()
        except OSError:
            logger.exception(
                "I/O error reading %s for event %s — leaving unacked for redelivery",
                target,
                event.details.event_id,
            )
            await event.nak()
            return

        await self._client.publish(
            READ_COMPLETED_SUBJECT,
            encode_read_completed(path=request.path, content=content),
            event_type="FileReadCompleted",
            correlation_id=event.details.event_id,
        )
        await event.ack()

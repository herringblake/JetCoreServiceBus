"""List-path command handler (Design.md §13 Step F3). Subscribes to
events.files.FileListRequested, lists a directory's immediate contents
(non-recursive — Decision #23), and publishes FileListCompleted or
FileOperationFailed.
"""

from __future__ import annotations

import logging
from pathlib import Path

from jetcore.bus_client import BusClient, ReceivedEvent

from file_storage_adapter.paths import PathTraversalError, resolve_within
from file_storage_adapter.payloads import (
    FileEntry,
    MalformedPayloadError,
    decode_path_only_request,
    encode_list_completed,
    encode_operation_failed,
)

logger = logging.getLogger(__name__)

LIST_REQUESTED_SUBJECT = "events.files.FileListRequested"
LIST_COMPLETED_SUBJECT = "events.files.FileListCompleted"
OPERATION_FAILED_SUBJECT = "events.files.FileOperationFailed"


class ListCommandHandler:
    def __init__(self, client: BusClient, *, watch_dir: Path) -> None:
        self._client = client
        self._watch_dir = watch_dir

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(LIST_REQUESTED_SUBJECT, durable_name=durable_name)

    async def run_once(self, durable_name: str, *, batch: int = 10, timeout: float = 5.0) -> int:
        events = await self._client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in events:
            await self._handle(event)
        return len(events)

    async def _handle(self, event: ReceivedEvent) -> None:
        try:
            # Empty path means watch_dir itself (Design.md §13 Step F3) —
            # the one command whose decode allows it.
            request = decode_path_only_request(event.payload, allow_empty_path=True)
        except MalformedPayloadError:
            logger.exception(
                "malformed FileListRequested payload for event %s — acking, not retrying",
                event.details.event_id,
            )
            await event.ack()
            return

        try:
            target = resolve_within(self._watch_dir, request.path)
        except PathTraversalError:
            logger.warning(
                "rejected FileListRequested for event %s: path %r escapes watch_dir "
                "— acking, not retrying",
                event.details.event_id,
                request.path,
            )
            await event.ack()
            return

        if not target.exists():
            await self._client.publish(
                OPERATION_FAILED_SUBJECT,
                encode_operation_failed(path=request.path, operation="list", reason="not_found"),
                event_type="FileOperationFailed",
                correlation_id=event.details.event_id,
            )
            await event.ack()
            return

        if not target.is_dir():
            await self._client.publish(
                OPERATION_FAILED_SUBJECT,
                encode_operation_failed(
                    path=request.path, operation="list", reason="not_a_directory"
                ),
                event_type="FileOperationFailed",
                correlation_id=event.details.event_id,
            )
            await event.ack()
            return

        try:
            entries = [
                FileEntry(
                    name=child.name,
                    is_directory=child.is_dir(),
                    size_bytes=0 if child.is_dir() else child.stat().st_size,
                )
                for child in sorted(target.iterdir())
            ]
        except OSError:
            logger.exception(
                "I/O error listing %s for event %s — leaving unacked for redelivery",
                target,
                event.details.event_id,
            )
            await event.nak()
            return

        await self._client.publish(
            LIST_COMPLETED_SUBJECT,
            encode_list_completed(path=request.path, entries=entries),
            event_type="FileListCompleted",
            correlation_id=event.details.event_id,
        )
        await event.ack()

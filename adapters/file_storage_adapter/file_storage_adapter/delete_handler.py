"""Delete-path command handler (Design.md §13 Step F4). Subscribes to
events.files.FileDeleteRequested, deletes the file, and publishes
FileDeleteCompleted or FileOperationFailed.
"""

from __future__ import annotations

import logging
from pathlib import Path

from jetcore.bus_client import BusClient, ReceivedEvent

from file_storage_adapter.paths import PathTraversalError, resolve_within
from file_storage_adapter.payloads import (
    MalformedPayloadError,
    decode_path_only_request,
    encode_delete_completed,
    encode_operation_failed,
)

logger = logging.getLogger(__name__)

DELETE_REQUESTED_SUBJECT = "events.files.FileDeleteRequested"
DELETE_COMPLETED_SUBJECT = "events.files.FileDeleteCompleted"
OPERATION_FAILED_SUBJECT = "events.files.FileOperationFailed"


class DeleteCommandHandler:
    def __init__(self, client: BusClient, *, watch_dir: Path) -> None:
        self._client = client
        self._watch_dir = watch_dir

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(DELETE_REQUESTED_SUBJECT, durable_name=durable_name)

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
                "malformed FileDeleteRequested payload for event %s — acking, not retrying",
                event.details.event_id,
            )
            await event.ack()
            return

        try:
            target = resolve_within(self._watch_dir, request.path)
        except PathTraversalError:
            logger.warning(
                "rejected FileDeleteRequested for event %s: path %r escapes watch_dir "
                "— acking, not retrying",
                event.details.event_id,
                request.path,
            )
            await event.ack()
            return

        # A directory at this path is treated the same as "not found" for
        # a delete — this command only ever deletes files (matching read's
        # own scoping, Design.md §13).
        if not target.is_file():
            await self._client.publish(
                OPERATION_FAILED_SUBJECT,
                encode_operation_failed(
                    path=request.path, operation="delete", reason="not_found"
                ),
                event_type="FileOperationFailed",
                correlation_id=event.details.event_id,
            )
            await event.ack()
            return

        try:
            target.unlink()
        except OSError:
            logger.exception(
                "I/O error deleting %s for event %s — leaving unacked for redelivery",
                target,
                event.details.event_id,
            )
            await event.nak()
            return

        await self._client.publish(
            DELETE_COMPLETED_SUBJECT,
            encode_delete_completed(path=request.path),
            event_type="FileDeleteCompleted",
            correlation_id=event.details.event_id,
        )
        await event.ack()

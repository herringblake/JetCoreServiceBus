"""Integration tests for read_handler.py (Design.md §13 Step F2) — against
the real Phase 1/2 stack, same discipline as write_handler.py's own tests
(Step C4): no mocks, real NATS, real crypto, real files on disk.

Uses test-observer-01 both to *trigger* FileReadRequested (no real Phase 3
"front door" adapter publishes it yet — Design.md §13's own note on this)
and to independently observe the reply, since file-storage-01 can't watch
its own published output.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

from _file_storage_helpers import (
    OPERATION_FAILED_SUBJECT,
    READ_COMPLETED_SUBJECT,
    READ_REQUESTED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from file_storage_adapter.read_handler import ReadCommandHandler


def _read_requested_payload(path: str) -> bytes:
    return json.dumps({"path": path}).encode()


async def test_existing_file_is_read_and_published(tmp_path: Path, durable_name: str) -> None:
    existing = tmp_path / "notes" / "todo.txt"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"hello world")

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = ReadCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await client.subscribe(
            READ_COMPLETED_SUBJECT, durable_name=f"{durable_name}-completed"
        )

        await wait_until_cache_has(await client._cache_for(READ_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(READ_COMPLETED_SUBJECT), 1)

        await client.publish(
            READ_REQUESTED_SUBJECT,
            _read_requested_payload("notes/todo.txt"),
            event_type="FileReadRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await client.fetch(completed_durable, timeout=3)
        assert result.details.event_type == "FileReadCompleted"
        assert result.details.source_service_id == "file-storage-01-test"
        data = json.loads(result.payload)
        assert data["path"] == "notes/todo.txt"
        assert base64.b64decode(data["content"]) == b"hello world"
        assert data["sizeBytes"] == 11
        assert data["occurredAt"].endswith("Z")
    finally:
        await client.close()
        await fs_client.close()


async def test_missing_file_publishes_file_operation_failed(
    tmp_path: Path, durable_name: str
) -> None:
    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = ReadCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        failed_durable = await client.subscribe(
            OPERATION_FAILED_SUBJECT, durable_name=f"{durable_name}-failed"
        )

        await wait_until_cache_has(await client._cache_for(READ_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(OPERATION_FAILED_SUBJECT), 1)

        await client.publish(
            READ_REQUESTED_SUBJECT,
            _read_requested_payload("does-not-exist.txt"),
            event_type="FileReadRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await client.fetch(failed_durable, timeout=3)
        assert result.details.event_type == "FileOperationFailed"
        data = json.loads(result.payload)
        assert data == {
            "path": "does-not-exist.txt",
            "operation": "read",
            "reason": "not_found",
            "occurredAt": data["occurredAt"],
        }
        assert data["occurredAt"].endswith("Z")
    finally:
        await client.close()
        await fs_client.close()


async def test_malformed_payload_is_acked_not_left_for_redelivery(
    tmp_path: Path, durable_name: str
) -> None:
    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = ReadCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await client._cache_for(READ_REQUESTED_SUBJECT), 1)

        await client.publish(
            READ_REQUESTED_SUBJECT, b"not valid json at all", event_type="FileReadRequested"
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await client.close()
        await fs_client.close()


async def test_path_traversal_is_rejected_without_reading_outside_watch_dir(
    tmp_path: Path, durable_name: str
) -> None:
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    outside_secret = tmp_path / "secret.txt"
    outside_secret.write_bytes(b"top secret")

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = ReadCommandHandler(fs_client, watch_dir=watch_dir)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await client._cache_for(READ_REQUESTED_SUBJECT), 1)

        await client.publish(
            READ_REQUESTED_SUBJECT,
            _read_requested_payload("../secret.txt"),
            event_type="FileReadRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        # Acked, not redelivered, and definitely no FileReadCompleted
        # carrying the secret's content anywhere.
        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await client.close()
        await fs_client.close()

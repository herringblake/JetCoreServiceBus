"""Integration tests for write_handler.py (Design.md §12 Step C4) — against
the real Phase 1 stack, same discipline as libs/jetcore's own integration
tests: no mocks, real NATS, real crypto, real files on disk.

Uses test-observer-01 (infra/nats/adapter_identities.yaml) to independently
confirm what actually lands on the bus — file-storage-01 can't watch its
own published output (no adapter in this manifest does, matching the DB
Adapter's own "don't consume your own result events" precedent), so
without a separate observer identity there'd be no permitted way to check.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

from _file_storage_helpers import (
    CREATE_COMPLETED_SUBJECT,
    WRITE_COMPLETED_SUBJECT,
    WRITE_REQUESTED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from file_storage_adapter.write_handler import WriteCommandHandler


def _write_requested_payload(path: str, content: bytes) -> bytes:
    return json.dumps({"path": path, "content": base64.b64encode(content).decode()}).encode()


async def test_new_file_creates_and_publishes_file_create_completed(
    tmp_path: Path, durable_name: str
) -> None:
    publisher = await connect("webhook-listener-01")
    fs_client = await connect("file-storage-01")
    observer = await connect("test-observer-01")
    handler = WriteCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        request_durable = await observer.subscribe(
            WRITE_REQUESTED_SUBJECT, durable_name=f"{durable_name}-req"
        )
        completed_durable = await observer.subscribe(
            CREATE_COMPLETED_SUBJECT, durable_name=f"{durable_name}-created"
        )

        # Both file-storage-01 (the real consumer) and the observer must be
        # registered before the publish, or one of them would miss it —
        # not a race the test should tolerate silently.
        await wait_until_cache_has(await publisher._cache_for(WRITE_REQUESTED_SUBJECT), 2)
        await wait_until_cache_has(await fs_client._cache_for(CREATE_COMPLETED_SUBJECT), 1)

        await publisher.publish(
            WRITE_REQUESTED_SUBJECT,
            _write_requested_payload("notes/todo.txt", b"hello world"),
            event_type="FileWriteRequested",
        )

        [request_seen] = await observer.fetch(request_durable, timeout=3)
        await request_seen.ack()

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert (tmp_path / "notes" / "todo.txt").read_bytes() == b"hello world"

        [result] = await observer.fetch(completed_durable, timeout=3)
        assert result.details.event_type == "FileCreateCompleted"
        assert result.details.source_service_id == "file-storage-01"
        assert result.details.correlation_id == request_seen.details.event_id
        data = json.loads(result.payload)
        assert data["path"] == "notes/todo.txt"
        assert data["sizeBytes"] == 11
        assert data["occurredAt"].endswith("Z")
    finally:
        await publisher.close()
        await fs_client.close()
        await observer.close()


async def test_existing_file_updates_and_publishes_file_write_completed(
    tmp_path: Path, durable_name: str
) -> None:
    existing = tmp_path / "notes" / "todo.txt"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"old content")

    publisher = await connect("webhook-listener-01")
    fs_client = await connect("file-storage-01")
    observer = await connect("test-observer-01")
    handler = WriteCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await observer.subscribe(
            WRITE_COMPLETED_SUBJECT, durable_name=f"{durable_name}-updated"
        )

        await wait_until_cache_has(await publisher._cache_for(WRITE_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(WRITE_COMPLETED_SUBJECT), 1)

        await publisher.publish(
            WRITE_REQUESTED_SUBJECT,
            _write_requested_payload("notes/todo.txt", b"new content!"),
            event_type="FileWriteRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert existing.read_bytes() == b"new content!"

        [result] = await observer.fetch(completed_durable, timeout=3)
        assert result.details.event_type == "FileWriteCompleted"
        data = json.loads(result.payload)
        assert data["path"] == "notes/todo.txt"
        assert data["sizeBytes"] == 12
    finally:
        await publisher.close()
        await fs_client.close()
        await observer.close()


async def test_malformed_payload_is_acked_not_left_for_redelivery(
    tmp_path: Path, durable_name: str
) -> None:
    publisher = await connect("webhook-listener-01")
    fs_client = await connect("file-storage-01")
    handler = WriteCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(WRITE_REQUESTED_SUBJECT), 1)

        await publisher.publish(
            WRITE_REQUESTED_SUBJECT, b"not valid json at all", event_type="FileWriteRequested"
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1  # fetched and handled, even though handling meant "reject"

        # Acked, not redelivered — a second fetch should find nothing left.
        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0

        assert list(tmp_path.iterdir()) == []
    finally:
        await publisher.close()
        await fs_client.close()


async def test_path_traversal_is_rejected_without_writing_outside_watch_dir(
    tmp_path: Path, durable_name: str
) -> None:
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()

    publisher = await connect("webhook-listener-01")
    fs_client = await connect("file-storage-01")
    handler = WriteCommandHandler(fs_client, watch_dir=watch_dir)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await publisher._cache_for(WRITE_REQUESTED_SUBJECT), 1)

        await publisher.publish(
            WRITE_REQUESTED_SUBJECT,
            _write_requested_payload("../escape.txt", b"malicious"),
            event_type="FileWriteRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert not (tmp_path / "escape.txt").exists()
        assert list(watch_dir.iterdir()) == []

        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await publisher.close()
        await fs_client.close()

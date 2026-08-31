"""Integration tests for delete_handler.py (Design.md §13 Step F4) — same
discipline as read_handler.py's/write_handler.py's own tests.
"""

from __future__ import annotations

import json
from pathlib import Path

from _file_storage_helpers import (
    DELETE_COMPLETED_SUBJECT,
    DELETE_REQUESTED_SUBJECT,
    OPERATION_FAILED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from file_storage_adapter.delete_handler import DeleteCommandHandler


def _delete_requested_payload(path: str) -> bytes:
    return json.dumps({"path": path}).encode()


async def test_existing_file_is_deleted_and_published(tmp_path: Path, durable_name: str) -> None:
    existing = tmp_path / "notes" / "todo.txt"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"delete me")

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = DeleteCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await client.subscribe(
            DELETE_COMPLETED_SUBJECT, durable_name=f"{durable_name}-completed"
        )

        await wait_until_cache_has(await client._cache_for(DELETE_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(DELETE_COMPLETED_SUBJECT), 1)

        await client.publish(
            DELETE_REQUESTED_SUBJECT,
            _delete_requested_payload("notes/todo.txt"),
            event_type="FileDeleteRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert not existing.exists()

        [result] = await client.fetch(completed_durable, timeout=3)
        assert result.details.event_type == "FileDeleteCompleted"
        data = json.loads(result.payload)
        assert data["path"] == "notes/todo.txt"
        assert data["occurredAt"].endswith("Z")
    finally:
        await client.close()
        await fs_client.close()


async def test_missing_file_publishes_file_operation_failed(
    tmp_path: Path, durable_name: str
) -> None:
    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = DeleteCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        failed_durable = await client.subscribe(
            OPERATION_FAILED_SUBJECT, durable_name=f"{durable_name}-failed"
        )

        await wait_until_cache_has(await client._cache_for(DELETE_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(OPERATION_FAILED_SUBJECT), 1)

        await client.publish(
            DELETE_REQUESTED_SUBJECT,
            _delete_requested_payload("does-not-exist.txt"),
            event_type="FileDeleteRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await client.fetch(failed_durable, timeout=3)
        data = json.loads(result.payload)
        assert data["operation"] == "delete"
        assert data["reason"] == "not_found"
    finally:
        await client.close()
        await fs_client.close()


async def test_malformed_payload_is_acked_not_left_for_redelivery(
    tmp_path: Path, durable_name: str
) -> None:
    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = DeleteCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await client._cache_for(DELETE_REQUESTED_SUBJECT), 1)

        await client.publish(
            DELETE_REQUESTED_SUBJECT, b"not valid json at all", event_type="FileDeleteRequested"
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await client.close()
        await fs_client.close()


async def test_path_traversal_is_rejected_without_deleting_outside_watch_dir(
    tmp_path: Path, durable_name: str
) -> None:
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    outside_secret = tmp_path / "secret.txt"
    outside_secret.write_bytes(b"do not delete me")

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01-test")
    handler = DeleteCommandHandler(fs_client, watch_dir=watch_dir)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await client._cache_for(DELETE_REQUESTED_SUBJECT), 1)

        await client.publish(
            DELETE_REQUESTED_SUBJECT,
            _delete_requested_payload("../secret.txt"),
            event_type="FileDeleteRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        assert outside_secret.exists()  # never touched

        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await client.close()
        await fs_client.close()

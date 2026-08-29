"""Integration tests for list_handler.py (Design.md §13 Step F3) — same
discipline as read_handler.py's/write_handler.py's own tests.
"""

from __future__ import annotations

import json
from pathlib import Path

from _file_storage_helpers import (
    LIST_COMPLETED_SUBJECT,
    LIST_REQUESTED_SUBJECT,
    OPERATION_FAILED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from file_storage_adapter.list_handler import ListCommandHandler


def _list_requested_payload(path: str) -> bytes:
    return json.dumps({"path": path}).encode()


async def test_directory_contents_are_listed_and_published(
    tmp_path: Path, durable_name: str
) -> None:
    notes = tmp_path / "notes"
    notes.mkdir()
    (notes / "todo.txt").write_bytes(b"hello world")
    (notes / "archive").mkdir()

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01")
    handler = ListCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await client.subscribe(
            LIST_COMPLETED_SUBJECT, durable_name=f"{durable_name}-completed"
        )

        await wait_until_cache_has(await client._cache_for(LIST_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(LIST_COMPLETED_SUBJECT), 1)

        await client.publish(
            LIST_REQUESTED_SUBJECT, _list_requested_payload("notes"), event_type="FileListRequested"
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await client.fetch(completed_durable, timeout=3)
        assert result.details.event_type == "FileListCompleted"
        data = json.loads(result.payload)
        assert data["path"] == "notes"
        assert sorted(data["entries"], key=lambda e: e["name"]) == [
            {"name": "archive", "isDirectory": True, "sizeBytes": 0},
            {"name": "todo.txt", "isDirectory": False, "sizeBytes": 11},
        ]
    finally:
        await client.close()
        await fs_client.close()


async def test_empty_path_lists_watch_dir_itself(tmp_path: Path, durable_name: str) -> None:
    (tmp_path / "notes").mkdir()

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01")
    handler = ListCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        completed_durable = await client.subscribe(
            LIST_COMPLETED_SUBJECT, durable_name=f"{durable_name}-completed"
        )

        await wait_until_cache_has(await client._cache_for(LIST_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(LIST_COMPLETED_SUBJECT), 1)

        await client.publish(
            LIST_REQUESTED_SUBJECT, _list_requested_payload(""), event_type="FileListRequested"
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await client.fetch(completed_durable, timeout=3)
        data = json.loads(result.payload)
        assert data["path"] == ""
        assert data["entries"] == [{"name": "notes", "isDirectory": True, "sizeBytes": 0}]
    finally:
        await client.close()
        await fs_client.close()


async def test_missing_directory_publishes_file_operation_failed(
    tmp_path: Path, durable_name: str
) -> None:
    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01")
    handler = ListCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        failed_durable = await client.subscribe(
            OPERATION_FAILED_SUBJECT, durable_name=f"{durable_name}-failed"
        )

        await wait_until_cache_has(await client._cache_for(LIST_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(OPERATION_FAILED_SUBJECT), 1)

        await client.publish(
            LIST_REQUESTED_SUBJECT,
            _list_requested_payload("does-not-exist"),
            event_type="FileListRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await client.fetch(failed_durable, timeout=3)
        data = json.loads(result.payload)
        assert data["operation"] == "list"
        assert data["reason"] == "not_found"
    finally:
        await client.close()
        await fs_client.close()


async def test_path_that_is_a_file_publishes_not_a_directory_failure(
    tmp_path: Path, durable_name: str
) -> None:
    (tmp_path / "todo.txt").write_bytes(b"not a directory")

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01")
    handler = ListCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        failed_durable = await client.subscribe(
            OPERATION_FAILED_SUBJECT, durable_name=f"{durable_name}-failed"
        )

        await wait_until_cache_has(await client._cache_for(LIST_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(OPERATION_FAILED_SUBJECT), 1)

        await client.publish(
            LIST_REQUESTED_SUBJECT,
            _list_requested_payload("todo.txt"),
            event_type="FileListRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        [result] = await client.fetch(failed_durable, timeout=3)
        data = json.loads(result.payload)
        assert data["reason"] == "not_a_directory"
    finally:
        await client.close()
        await fs_client.close()


async def test_malformed_payload_is_acked_not_left_for_redelivery(
    tmp_path: Path, durable_name: str
) -> None:
    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01")
    handler = ListCommandHandler(fs_client, watch_dir=tmp_path)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await client._cache_for(LIST_REQUESTED_SUBJECT), 1)

        await client.publish(
            LIST_REQUESTED_SUBJECT, b"not valid json at all", event_type="FileListRequested"
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await client.close()
        await fs_client.close()


async def test_path_traversal_is_rejected_without_listing_outside_watch_dir(
    tmp_path: Path, durable_name: str
) -> None:
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "secret.txt").write_bytes(b"top secret")

    client = await connect("test-observer-01")
    fs_client = await connect("file-storage-01")
    handler = ListCommandHandler(fs_client, watch_dir=watch_dir)
    try:
        handler_durable = await handler.start(durable_name=durable_name)
        await wait_until_cache_has(await client._cache_for(LIST_REQUESTED_SUBJECT), 1)

        await client.publish(
            LIST_REQUESTED_SUBJECT,
            _list_requested_payload("../outside"),
            event_type="FileListRequested",
        )

        fetched = await handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        again = await handler.run_once(handler_durable, timeout=1)
        assert again == 0
    finally:
        await client.close()
        await fs_client.close()

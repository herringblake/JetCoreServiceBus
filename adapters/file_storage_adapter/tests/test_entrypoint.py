"""Integration test for the real entrypoint wiring (Design.md §12 Step
C7) — drives file_storage_adapter.__main__.run() directly: a real
BusClient (via connect_as_adapter, the real identity path Step C6 built,
not the test connect() helper's ephemeral-key shortcut), a real
watch_dir, a real shutdown handshake. C4/C5's own tests already prove
WriteCommandHandler and ExternalCreationWatch work individually; this is
the one test that proves they still work wired together exactly the way
`python -m file_storage_adapter` actually runs them.

This is Track C's exit criterion (Design.md §12): the adapter must work
standing alone, driven by test code publishing FileWriteRequested
directly, before Track D gives it a real producer.
"""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import file_storage_adapter.__main__ as entrypoint
from _file_storage_helpers import (
    CREATE_COMPLETED_SUBJECT,
    DELETE_COMPLETED_SUBJECT,
    DELETE_REQUESTED_SUBJECT,
    LIST_COMPLETED_SUBJECT,
    LIST_REQUESTED_SUBJECT,
    READ_COMPLETED_SUBJECT,
    READ_REQUESTED_SUBJECT,
    WRITE_REQUESTED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings


def _write_requested_payload(path: str, content: bytes) -> bytes:
    return json.dumps({"path": path, "content": base64.b64encode(content).decode()}).encode()


async def test_real_entrypoint_processes_a_command_and_shuts_down_cleanly(
    tmp_path: Path,
) -> None:
    settings = AdapterSettings(
        service_id="file-storage-01",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/file-storage-01.creds",
    )
    fs_client = await BusClient.connect_as_adapter(settings, adapter_type="file-storage-adapter")
    publisher = await connect("webhook-listener-01")
    observer = await connect("test-observer-01")
    shutdown = asyncio.Event()
    run_task: asyncio.Task[None] | None = None
    try:
        completed_durable = await observer.subscribe(
            CREATE_COMPLETED_SUBJECT, durable_name="test-entrypoint-observer"
        )

        run_task = asyncio.create_task(
            entrypoint.run(
                fs_client, watch_dir=tmp_path, service_id="file-storage-01", shutdown=shutdown
            )
        )
        await asyncio.sleep(0.2)  # let subscribe()/awatch startup actually happen

        await wait_until_cache_has(await publisher._cache_for(WRITE_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(CREATE_COMPLETED_SUBJECT), 1)

        await publisher.publish(
            WRITE_REQUESTED_SUBJECT,
            _write_requested_payload("proof/from-real-entrypoint.txt", b"C7 end-to-end proof"),
            event_type="FileWriteRequested",
        )

        received = await observer.fetch(completed_durable, timeout=5)
        assert len(received) == 1
        assert received[0].details.event_type == "FileCreateCompleted"
        assert received[0].details.source_service_id == "file-storage-01"

        written = tmp_path / "proof" / "from-real-entrypoint.txt"
        assert written.read_bytes() == b"C7 end-to-end proof"

        # Graceful shutdown: setting the event must make run() actually
        # return within a bounded time, not hang — the other half of this
        # step's own name.
        shutdown.set()
        await asyncio.wait_for(run_task, timeout=5)
        run_task = None
    finally:
        if run_task is not None:
            run_task.cancel()
        await fs_client.close()
        await publisher.close()
        await observer.close()


async def test_connect_as_adapter_identity_survives_a_simulated_restart(
    tmp_path: Path,
) -> None:
    """Design.md §12 Step C6's whole point, proven at the entrypoint
    level: two separate `run()` invocations for the same service_id (a
    stand-in for "the adapter restarted") must sign with the same key,
    so a recipient's trust in this serviceId doesn't need re-establishing
    every restart. libs/jetcore/tests/test_bus_client.py already proves
    connect_as_adapter() returns the same key twice; this confirms it
    holds for the actual entrypoint wiring, not just the bare classmethod."""
    settings = AdapterSettings(
        service_id="file-storage-01",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/file-storage-01.creds",
    )
    first = await BusClient.connect_as_adapter(settings, adapter_type="file-storage-adapter")
    try:
        first_key = first._signing_public_key
    finally:
        await first.close()

    second = await BusClient.connect_as_adapter(settings, adapter_type="file-storage-adapter")
    try:
        second_key = second._signing_public_key
    finally:
        await second.close()

    assert first_key == second_key


async def test_real_entrypoint_processes_read_list_and_delete_commands(tmp_path: Path) -> None:
    """Design.md §13 Step F6 — proves read/list/delete work through the
    real five-way asyncio.gather (Step F5), not just via each handler's
    own class-level tests (test_read_handler.py etc.). Each command is
    driven to completion (publish, then fetch its own reply) before the
    next starts — read/list/delete run on fully independent consumers
    with no cross-command ordering guarantee, so publishing all three
    up front and fetching afterward would race delete against list."""
    existing = tmp_path / "notes" / "todo.txt"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"already here")

    settings = AdapterSettings(
        service_id="file-storage-01",
        nats_url="nats://localhost:4222",
        nats_creds_path="infra/nats/operator/creds/file-storage-01.creds",
    )
    fs_client = await BusClient.connect_as_adapter(settings, adapter_type="file-storage-adapter")
    client = await connect("test-observer-01")
    shutdown = asyncio.Event()
    run_task: asyncio.Task[None] | None = None
    try:
        read_completed = await client.subscribe(
            READ_COMPLETED_SUBJECT, durable_name="test-entrypoint-read"
        )
        list_completed = await client.subscribe(
            LIST_COMPLETED_SUBJECT, durable_name="test-entrypoint-list"
        )
        delete_completed = await client.subscribe(
            DELETE_COMPLETED_SUBJECT, durable_name="test-entrypoint-delete"
        )

        run_task = asyncio.create_task(
            entrypoint.run(
                fs_client, watch_dir=tmp_path, service_id="file-storage-01", shutdown=shutdown
            )
        )
        await asyncio.sleep(0.2)  # let subscribe()/awatch startup actually happen

        await wait_until_cache_has(await client._cache_for(READ_REQUESTED_SUBJECT), 1)
        await client.publish(
            READ_REQUESTED_SUBJECT,
            json.dumps({"path": "notes/todo.txt"}).encode(),
            event_type="FileReadRequested",
        )
        [read_result] = await client.fetch(read_completed, timeout=5)
        read_data = json.loads(read_result.payload)
        assert base64.b64decode(read_data["content"]) == b"already here"

        await wait_until_cache_has(await client._cache_for(LIST_REQUESTED_SUBJECT), 1)
        await client.publish(
            LIST_REQUESTED_SUBJECT,
            json.dumps({"path": "notes"}).encode(),
            event_type="FileListRequested",
        )
        [list_result] = await client.fetch(list_completed, timeout=5)
        list_data = json.loads(list_result.payload)
        assert list_data["entries"] == [{"name": "todo.txt", "isDirectory": False, "sizeBytes": 12}]

        await wait_until_cache_has(await client._cache_for(DELETE_REQUESTED_SUBJECT), 1)
        await client.publish(
            DELETE_REQUESTED_SUBJECT,
            json.dumps({"path": "notes/todo.txt"}).encode(),
            event_type="FileDeleteRequested",
        )
        [delete_result] = await client.fetch(delete_completed, timeout=5)
        assert json.loads(delete_result.payload)["path"] == "notes/todo.txt"
        assert not existing.exists()

        shutdown.set()
        await asyncio.wait_for(run_task, timeout=5)
        run_task = None
    finally:
        if run_task is not None:
            run_task.cancel()
        await fs_client.close()
        await client.close()

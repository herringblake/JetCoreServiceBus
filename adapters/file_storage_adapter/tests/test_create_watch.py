"""Tests for create_watch.py (Design.md §12 Step C5).

wait_until_settled is tested standalone first (pure asyncio + filesystem,
no bus involved). The rest are live-NATS integration tests, including the
one that matters most for this step: a command-triggered creation must
NOT also be reported by the external watch (recent_writes.py's whole
reason for existing) — proven with a second, empty fetch after the
correlated one, the same "confirm no redelivery" idiom Step C4's
malformed-payload/path-traversal tests already use, reused here to prove
"confirm no *extra* publish" instead.
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncGenerator
from pathlib import Path

import aiofiles
import pytest
from _file_storage_helpers import (
    CREATE_COMPLETED_SUBJECT,
    WRITE_REQUESTED_SUBJECT,
    connect,
    wait_until_cache_has,
)
from file_storage_adapter.create_watch import ExternalCreationWatch, wait_until_settled
from file_storage_adapter.recent_writes import RecentWrites
from file_storage_adapter.write_handler import WriteCommandHandler

WatchHandle = tuple[asyncio.Event, list["asyncio.Task[None]"]]


def _write_requested_payload(path: str, content: bytes) -> bytes:
    return json.dumps({"path": path, "content": base64.b64encode(content).decode()}).encode()


async def test_wait_until_settled_waits_for_a_slow_multi_chunk_write(tmp_path: Path) -> None:
    target = tmp_path / "slow.bin"

    async def slow_write() -> None:
        async with aiofiles.open(target, "wb") as f:
            await f.write(b"a" * 10)
            await f.flush()
            await asyncio.sleep(0.5)
            await f.write(b"b" * 10)
            await f.flush()
            await asyncio.sleep(0.5)
            await f.write(b"c" * 10)
            await f.flush()

    writer = asyncio.create_task(slow_write())
    await asyncio.sleep(0.05)  # let the file actually exist before polling starts

    loop = asyncio.get_event_loop()
    start = loop.time()
    size = await wait_until_settled(target)
    elapsed = loop.time() - start

    await writer

    assert size == 30
    assert elapsed >= 0.9, "settled on an intermediate chunk instead of the finished write"


async def test_wait_until_settled_returns_none_if_file_disappears(tmp_path: Path) -> None:
    target = tmp_path / "ephemeral.txt"
    target.write_bytes(b"here briefly")

    async def delete_it() -> None:
        await asyncio.sleep(0.1)
        target.unlink()

    asyncio.create_task(delete_it())

    result = await wait_until_settled(target, poll_interval=0.05, stable_reads=2)

    assert result is None


@pytest.fixture
async def stop_watch() -> AsyncGenerator[WatchHandle]:
    """Yields (stop_event, tasks) — tests append their watch task to
    `tasks`; this fixture signals shutdown and awaits it afterward, so a
    forgotten cleanup can't leave a background watcher running past its
    test."""
    stop_event = asyncio.Event()
    tasks: list[asyncio.Task[None]] = []
    yield stop_event, tasks
    stop_event.set()
    for task in tasks:
        await asyncio.wait_for(task, timeout=3)


async def test_external_file_creation_is_detected_and_published(
    tmp_path: Path, durable_name: str, stop_watch: WatchHandle
) -> None:
    stop_event, tasks = stop_watch
    fs_client = await connect("file-storage-01-test")
    observer = await connect("test-observer-01")
    try:
        completed_durable = await observer.subscribe(
            CREATE_COMPLETED_SUBJECT, durable_name=f"{durable_name}-ext"
        )
        await wait_until_cache_has(await fs_client._cache_for(CREATE_COMPLETED_SUBJECT), 1)

        watch = ExternalCreationWatch(fs_client, watch_dir=tmp_path, recent_writes=RecentWrites())
        tasks.append(asyncio.create_task(watch.run_forever(stop_event=stop_event)))
        await asyncio.sleep(0.1)  # let awatch actually start observing before we create anything

        (tmp_path / "appeared.txt").write_bytes(b"from outside")

        received = await observer.fetch(completed_durable, timeout=8)

        assert len(received) == 1
        result = received[0]
        assert result.details.event_type == "FileCreateCompleted"
        assert result.details.correlation_id is None
        data = json.loads(result.payload)
        assert data["path"] == "appeared.txt"
        assert data["sizeBytes"] == len(b"from outside")
    finally:
        await fs_client.close()
        await observer.close()


async def test_command_triggered_creation_is_not_also_reported_by_watch(
    tmp_path: Path, durable_name: str, stop_watch: WatchHandle
) -> None:
    stop_event, tasks = stop_watch
    publisher = await connect("webhook-listener-01-test")
    fs_client = await connect("file-storage-01-test")
    observer = await connect("test-observer-01")
    try:
        recent_writes = RecentWrites()
        write_handler = WriteCommandHandler(
            fs_client, watch_dir=tmp_path, recent_writes=recent_writes
        )
        watch = ExternalCreationWatch(fs_client, watch_dir=tmp_path, recent_writes=recent_writes)

        handler_durable = await write_handler.start(durable_name=durable_name)
        completed_durable = await observer.subscribe(
            CREATE_COMPLETED_SUBJECT, durable_name=f"{durable_name}-created"
        )
        await wait_until_cache_has(await publisher._cache_for(WRITE_REQUESTED_SUBJECT), 1)
        await wait_until_cache_has(await fs_client._cache_for(CREATE_COMPLETED_SUBJECT), 1)

        tasks.append(asyncio.create_task(watch.run_forever(stop_event=stop_event)))
        await asyncio.sleep(0.1)

        await publisher.publish(
            WRITE_REQUESTED_SUBJECT,
            _write_requested_payload("notes/todo.txt", b"hello"),
            event_type="FileWriteRequested",
        )

        fetched = await write_handler.run_once(handler_durable, timeout=3)
        assert fetched == 1

        first = await observer.fetch(completed_durable, timeout=3)
        assert len(first) == 1
        assert first[0].details.correlation_id is not None  # the real, command-triggered one
        # Ack it — found while implementing Design.md §16 Step N1: this
        # was previously left un-acked, which happened to not matter only
        # because the old flat 30s AckWait comfortably outlasted the
        # second fetch's own short window below. Once ReceivedEvent.nak()
        # started applying a fast first backoff (2s, §9 item #9) to
        # *nakked* messages, an un-acked-and-un-nakked one here (this
        # test's own gap, not that fix's fault) became eligible for its
        # own ordinary redelivery well inside that window — surfacing as
        # a spurious *second* delivery of this same message, not a real
        # extra publish from the watch. Acking it removes the ambiguity:
        # every message this project's own code fetches gets ack()'d or
        # nak()'d exactly once, tests included (ReceivedEvent's own
        # docstring: "nothing here is auto-acked").
        await first[0].ack()

        # The important assertion: if recent_writes hadn't suppressed it,
        # the external watch's own (uncorrelated) publish would show up
        # within its debounce+settle window (up to ~2.2s) — wait past
        # that and confirm nothing else arrives, using bus_client.fetch()'s
        # Step C4 fix (returns [] on idle rather than raising) directly.
        second = await observer.fetch(completed_durable, timeout=4)
        assert second == []
    finally:
        await publisher.close()
        await fs_client.close()
        await observer.close()

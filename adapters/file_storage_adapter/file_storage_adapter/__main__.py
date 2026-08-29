"""Entrypoint for the File Storage Adapter (Design.md §12 Step C6, §13
Step F5). Wires settings -> a real BusClient -> all five command handlers
(write, read, list, delete, plus the external-creation watch) running
concurrently — the same asyncio.gather-of-independent-pieces shape Step B7
proved with two fake adapters, grown from Step C6's original two-way
gather to a five-way one as Track F added the remaining commands.

Run via: python -m file_storage_adapter
"""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from typing import Protocol

from jetcore.bus_client import BusClient

from file_storage_adapter.create_watch import ExternalCreationWatch
from file_storage_adapter.delete_handler import DeleteCommandHandler
from file_storage_adapter.list_handler import ListCommandHandler
from file_storage_adapter.read_handler import ReadCommandHandler
from file_storage_adapter.recent_writes import RecentWrites
from file_storage_adapter.settings import FileStorageSettings
from file_storage_adapter.write_handler import WriteCommandHandler

logger = logging.getLogger(__name__)

ADAPTER_TYPE = "file-storage-adapter"

# How long a single fetch() poll blocks waiting for a command before a
# command loop re-checks the shutdown event — the real bound on shutdown
# latency (a signal only takes effect between fetch() calls, not during
# one). Short enough to stay well inside Docker's default 10s stop grace
# period; long enough not to hammer NATS with pointless round trips.
FETCH_TIMEOUT_SECONDS = 2.0
FETCH_BATCH = 10


class _CommandHandler(Protocol):
    """Structural type covering all four command handlers (write/read/
    list/delete) — each has an identical run_once() shape (Design.md §13
    Step F5), so one _run_command_loop works for all of them rather than
    duplicating it four times."""

    async def run_once(
        self, durable_name: str, *, batch: int = 10, timeout: float = 5.0
    ) -> int: ...


async def _run_command_loop(
    handler: _CommandHandler, durable_name: str, shutdown: asyncio.Event
) -> None:
    while not shutdown.is_set():
        await handler.run_once(durable_name, batch=FETCH_BATCH, timeout=FETCH_TIMEOUT_SECONDS)


async def run(
    client: BusClient, *, watch_dir: Path, service_id: str, shutdown: asyncio.Event
) -> None:
    """The adapter's actual running logic — all five pieces (Step C4's
    write handler, Step F2-F4's read/list/delete handlers, and Step C5's
    external-creation watch) concurrently, until `shutdown` is set. Split
    out from main() so Step C7's integration test can drive this real
    wiring directly (a real BusClient, a real watch_dir, a real shutdown
    handshake) instead of only exercising each handler individually,
    which each one's own tests already do — without needing OS signals or
    a subprocess to do it."""
    recent_writes = RecentWrites()
    write_handler = WriteCommandHandler(client, watch_dir=watch_dir, recent_writes=recent_writes)
    read_handler = ReadCommandHandler(client, watch_dir=watch_dir)
    list_handler = ListCommandHandler(client, watch_dir=watch_dir)
    delete_handler = DeleteCommandHandler(client, watch_dir=watch_dir)
    watch = ExternalCreationWatch(client, watch_dir=watch_dir, recent_writes=recent_writes)

    # One durable name per service_id + command, not a random one — a real
    # deployment restarts these same consumers across process restarts and
    # expects to resume their own cursors, unlike tests, which want fresh
    # ones every run (Design.md §11 Track B test rationale).
    write_durable = f"{service_id}-write-handler"
    read_durable = f"{service_id}-read-handler"
    list_durable = f"{service_id}-list-handler"
    delete_durable = f"{service_id}-delete-handler"
    await write_handler.start(durable_name=write_durable)
    await read_handler.start(durable_name=read_durable)
    await list_handler.start(durable_name=list_durable)
    await delete_handler.start(durable_name=delete_durable)

    logger.info("file storage adapter started (service_id=%s, watch_dir=%s)", service_id, watch_dir)
    await asyncio.gather(
        _run_command_loop(write_handler, write_durable, shutdown),
        _run_command_loop(read_handler, read_durable, shutdown),
        _run_command_loop(list_handler, list_durable, shutdown),
        _run_command_loop(delete_handler, delete_durable, shutdown),
        watch.run_forever(stop_event=shutdown),
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    settings = FileStorageSettings()  # pydantic-settings loads required fields from env vars
    client = await BusClient.connect_as_adapter(settings, adapter_type=ADAPTER_TYPE)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        logger.info("received %s, shutting down", sig.name)
        shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    try:
        await run(
            client,
            watch_dir=settings.watch_dir,
            service_id=settings.service_id,
            shutdown=shutdown,
        )
    finally:
        await client.close()
        logger.info("file storage adapter stopped")


if __name__ == "__main__":
    asyncio.run(main())

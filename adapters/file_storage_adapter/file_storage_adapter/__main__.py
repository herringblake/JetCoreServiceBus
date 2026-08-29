"""Entrypoint for the File Storage Adapter (Design.md §12 Step C6). Wires
settings -> a real BusClient -> the write-path handler (C4) and the
external-creation watch (C5) running concurrently — the same
asyncio.gather-of-two-independent-pieces shape Step B7 proved with two
separate fake adapters, this time both halves of one real adapter process.

Run via: python -m file_storage_adapter
"""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from jetcore.bus_client import BusClient

from file_storage_adapter.create_watch import ExternalCreationWatch
from file_storage_adapter.recent_writes import RecentWrites
from file_storage_adapter.settings import FileStorageSettings
from file_storage_adapter.write_handler import WriteCommandHandler

logger = logging.getLogger(__name__)

ADAPTER_TYPE = "file-storage-adapter"

# How long a single fetch() poll blocks waiting for a command before the
# write loop re-checks the shutdown event — the real bound on shutdown
# latency (a signal only takes effect between fetch() calls, not during
# one). Short enough to stay well inside Docker's default 10s stop grace
# period; long enough not to hammer NATS with pointless round trips.
FETCH_TIMEOUT_SECONDS = 2.0
FETCH_BATCH = 10


async def _run_write_loop(
    handler: WriteCommandHandler, durable_name: str, shutdown: asyncio.Event
) -> None:
    while not shutdown.is_set():
        await handler.run_once(durable_name, batch=FETCH_BATCH, timeout=FETCH_TIMEOUT_SECONDS)


async def run(
    client: BusClient, *, watch_dir: Path, service_id: str, shutdown: asyncio.Event
) -> None:
    """The adapter's actual running logic — the write-path handler (C4)
    and the external-creation watch (C5) concurrently, until `shutdown` is
    set. Split out from main() so Step C7's integration test can drive
    this real wiring directly (a real BusClient, a real watch_dir, a real
    shutdown handshake) instead of only exercising WriteCommandHandler and
    ExternalCreationWatch individually, which C4/C5's own tests already
    do — without needing OS signals or a subprocess to do it."""
    recent_writes = RecentWrites()
    write_handler = WriteCommandHandler(client, watch_dir=watch_dir, recent_writes=recent_writes)
    watch = ExternalCreationWatch(client, watch_dir=watch_dir, recent_writes=recent_writes)

    # One durable name per service_id, not a random one — a real deployment
    # restarts this same consumer across process restarts and expects to
    # resume its own cursor, unlike tests, which want a fresh one every run
    # (Design.md §11 Track B test rationale).
    durable_name = f"{service_id}-write-handler"
    await write_handler.start(durable_name=durable_name)

    logger.info("file storage adapter started (service_id=%s, watch_dir=%s)", service_id, watch_dir)
    await asyncio.gather(
        _run_write_loop(write_handler, durable_name, shutdown),
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

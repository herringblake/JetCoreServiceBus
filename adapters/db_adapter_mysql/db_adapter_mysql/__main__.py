"""Entrypoint for the Database Adapter (Design.md §13 Step J6). Wires
settings -> a real BusClient -> the write handler (Step J4) + CDC watcher
(Step J5) running concurrently — the same asyncio.gather-of-independent-
pieces shape as the File Storage Adapter's write handler + external-
creation watch (Step C6), extended here with a small bridge task since
the CDC watcher's own shutdown signal is a `threading.Event`, not an
`asyncio.Event` (cdc_watcher.py's module docstring explains why).

Run via: python -m db_adapter_mysql
"""

from __future__ import annotations

import asyncio
import logging
import signal

from jetcore.bus_client import BusClient

from db_adapter_mysql.cdc_watcher import CdcWatcher
from db_adapter_mysql.db import create_write_engine
from db_adapter_mysql.settings import DbAdapterSettings
from db_adapter_mysql.write_handler import WriteCommandHandler

logger = logging.getLogger(__name__)

ADAPTER_TYPE = "database-adapter-mysql"

# Same reasoning as every other adapter's poll loop (Steps C6/F5/G4/H4/I4).
FETCH_TIMEOUT_SECONDS = 2.0
FETCH_BATCH = 10


async def _run_write_loop(
    handler: WriteCommandHandler, durable_name: str, shutdown: asyncio.Event
) -> None:
    while not shutdown.is_set():
        await handler.run_once(durable_name, batch=FETCH_BATCH, timeout=FETCH_TIMEOUT_SECONDS)


async def _bridge_shutdown(shutdown: asyncio.Event, watcher: CdcWatcher) -> None:
    """Translates the entrypoint's own asyncio.Event into the CDC
    watcher's threading.Event once shutdown is actually requested —
    the CDC watcher's own worker thread never touches asyncio primitives
    directly (cdc_watcher.py's module docstring)."""
    await shutdown.wait()
    watcher.request_shutdown()


async def run(
    client: BusClient,
    *,
    settings: DbAdapterSettings,
    shutdown: asyncio.Event,
) -> None:
    """The adapter's actual running logic — the write path and the CDC
    read path concurrently, until `shutdown` is set. Split out from
    main() the same way every other adapter's entrypoint is, so a test
    can drive this real wiring directly."""
    engine = create_write_engine(settings)
    write_handler = WriteCommandHandler(client, engine=engine)
    write_durable = f"{settings.service_id}-write-handler"
    await write_handler.start(durable_name=write_durable)

    watcher = CdcWatcher(client, settings)

    logger.info("database adapter started (service_id=%s)", settings.service_id)
    try:
        await asyncio.gather(
            _run_write_loop(write_handler, write_durable, shutdown),
            watcher.run(),
            _bridge_shutdown(shutdown, watcher),
        )
    finally:
        await engine.dispose()


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    settings = DbAdapterSettings()  # pydantic-settings loads required fields from env vars
    client = await BusClient.connect_as_adapter(settings, adapter_type=ADAPTER_TYPE)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        logger.info("received %s, shutting down", sig.name)
        shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    try:
        await run(client, settings=settings, shutdown=shutdown)
    finally:
        await client.close()
        logger.info("database adapter stopped")


if __name__ == "__main__":
    asyncio.run(main())

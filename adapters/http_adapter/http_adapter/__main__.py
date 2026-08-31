"""Entrypoint for the HTTP Adapter (Design.md §13 Step H4). Wires
settings -> a real BusClient -> one TriggerHandler per subject in
JETCORE_SUBJECTS, running concurrently — the same shape as the Webhook
Sender's entrypoint (Step G4), including its keepalive task for the
"subjects is empty" case.

Run via: python -m http_adapter
"""

from __future__ import annotations

import asyncio
import logging
import signal

import httpx
from jetcore.bus_client import BusClient

from http_adapter.settings import HttpAdapterSettings
from http_adapter.trigger_handler import TriggerHandler

logger = logging.getLogger(__name__)

ADAPTER_TYPE = "http-adapter"

# Same reasoning as every other entrypoint's poll loop (Steps C6/G4).
FETCH_TIMEOUT_SECONDS = 2.0
FETCH_BATCH = 10


async def _run_trigger_loop(
    handler: TriggerHandler, durable_name: str, shutdown: asyncio.Event
) -> None:
    while not shutdown.is_set():
        await handler.run_once(durable_name, batch=FETCH_BATCH, timeout=FETCH_TIMEOUT_SECONDS)


async def _wait_for_shutdown(shutdown: asyncio.Event) -> None:
    # See webhook_sender/__main__.py's identical helper for why this
    # trivial wrapper exists (keeps every gathered task uniformly typed
    # as Task[None]).
    await shutdown.wait()


async def run(
    client: BusClient,
    *,
    subjects: list[str],
    target_base_url: str,
    auth_token: str | None,
    http_client: httpx.AsyncClient,
    service_id: str,
    shutdown: asyncio.Event,
) -> None:
    """The adapter's actual running logic — one TriggerHandler per
    configured subject, until `shutdown` is set. Split out from main()
    the same way every other adapter's entrypoint is, so a test can drive
    this real wiring directly."""
    tasks = [asyncio.create_task(_wait_for_shutdown(shutdown))]

    for subject in subjects:
        handler = TriggerHandler(
            client,
            subject=subject,
            target_base_url=target_base_url,
            auth_token=auth_token,
            http_client=http_client,
        )
        durable_name = f"{service_id}-trigger-{subject.replace('.', '_')}"
        await handler.start(durable_name=durable_name)
        tasks.append(asyncio.create_task(_run_trigger_loop(handler, durable_name, shutdown)))

    logger.info(
        "http adapter started (service_id=%s, subjects=%s, target_base_url=%s)",
        service_id,
        subjects,
        target_base_url,
    )
    await asyncio.gather(*tasks)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    settings = HttpAdapterSettings()  # pydantic-settings loads required fields from env vars
    client = await BusClient.connect_as_adapter(settings, adapter_type=ADAPTER_TYPE)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        logger.info("received %s, shutting down", sig.name)
        shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    auth_token = settings.auth_token.get_secret_value() if settings.auth_token is not None else None

    async with httpx.AsyncClient() as http_client:
        try:
            await run(
                client,
                subjects=settings.subjects,
                target_base_url=settings.target_base_url,
                auth_token=auth_token,
                http_client=http_client,
                service_id=settings.service_id,
                shutdown=shutdown,
            )
        finally:
            await client.close()
            logger.info("http adapter stopped")


if __name__ == "__main__":
    asyncio.run(main())

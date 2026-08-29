"""Entrypoint for the Webhook Sender (Design.md §13 Step G4). Wires
settings -> a real BusClient -> one RelayHandler per subject in
JETCORE_SUBJECTS, running concurrently — the same
asyncio.gather-of-independent-pieces shape used throughout this project
(Steps C6/F5), here with a *variable* number of pieces (one per configured
subject) rather than a fixed set.

Run via: python -m webhook_sender
"""

from __future__ import annotations

import asyncio
import logging
import signal

import httpx
from jetcore.bus_client import BusClient

from webhook_sender.relay_handler import RelayHandler
from webhook_sender.settings import WebhookSenderSettings

logger = logging.getLogger(__name__)

ADAPTER_TYPE = "webhook-sender"

# Same reasoning as the File Storage Adapter's entrypoint (Step C6): short
# enough to stay well inside Docker's default 10s stop grace period, long
# enough not to hammer NATS with pointless round trips.
FETCH_TIMEOUT_SECONDS = 2.0
FETCH_BATCH = 10


async def _run_relay_loop(
    handler: RelayHandler, durable_name: str, shutdown: asyncio.Event
) -> None:
    while not shutdown.is_set():
        await handler.run_once(durable_name, batch=FETCH_BATCH, timeout=FETCH_TIMEOUT_SECONDS)


async def _wait_for_shutdown(shutdown: asyncio.Event) -> None:
    # A trivial None-returning wrapper around Event.wait() (which itself
    # returns Literal[True]) — keeps every task in run()'s gather list
    # uniformly typed as Task[None], not a mypy-only concern: a mixed-
    # return-type task list is exactly the kind of thing that's easy to
    # misuse at runtime too.
    await shutdown.wait()


async def run(
    client: BusClient,
    *,
    subjects: list[str],
    target_url: str,
    outbound_secret: str | None,
    http_client: httpx.AsyncClient,
    service_id: str,
    shutdown: asyncio.Event,
) -> None:
    """The adapter's actual running logic — one RelayHandler per
    configured subject, until `shutdown` is set. Split out from main()
    the same way every other adapter's entrypoint is (Steps C6/D4/F5), so
    a test can drive this real wiring directly without OS signals or a
    subprocess."""
    # A standalone keepalive task, always present: if `subjects` is empty
    # (a legitimate, if useless, configuration — nothing to relay),
    # nothing else here would ever complete the gather below, and the
    # process should still wait for a real shutdown signal rather than
    # exiting immediately.
    tasks = [asyncio.create_task(_wait_for_shutdown(shutdown))]

    for subject in subjects:
        handler = RelayHandler(
            client,
            subject=subject,
            target_url=target_url,
            outbound_secret=outbound_secret,
            http_client=http_client,
        )
        # Dots replaced: NATS durable consumer names conventionally avoid
        # them (subject-space's own token separator) — every other
        # durable name in this project happens to be dot-free already,
        # this is the first one built from a subject string directly.
        durable_name = f"{service_id}-relay-{subject.replace('.', '_')}"
        await handler.start(durable_name=durable_name)
        tasks.append(asyncio.create_task(_run_relay_loop(handler, durable_name, shutdown)))

    logger.info(
        "webhook sender started (service_id=%s, subjects=%s, target_url=%s)",
        service_id,
        subjects,
        target_url,
    )
    await asyncio.gather(*tasks)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    settings = WebhookSenderSettings()  # pydantic-settings loads required fields from env vars
    client = await BusClient.connect_as_adapter(settings, adapter_type=ADAPTER_TYPE)

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown(sig: signal.Signals) -> None:
        logger.info("received %s, shutting down", sig.name)
        shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    outbound_secret = (
        settings.outbound_secret.get_secret_value()
        if settings.outbound_secret is not None
        else None
    )

    async with httpx.AsyncClient() as http_client:
        try:
            await run(
                client,
                subjects=settings.subjects,
                target_url=settings.target_url,
                outbound_secret=outbound_secret,
                http_client=http_client,
                service_id=settings.service_id,
                shutdown=shutdown,
            )
        finally:
            await client.close()
            logger.info("webhook sender stopped")


if __name__ == "__main__":
    asyncio.run(main())

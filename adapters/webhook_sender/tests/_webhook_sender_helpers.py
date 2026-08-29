"""Test-only helpers for webhook_sender's live-NATS integration tests. A
local copy of the same connect()/wait_until_cache_has() pattern used
throughout this project's other test suites — not a cross-package
import, for the self-containment reasons already explained in
libs/jetcore/tests/_helpers.py and its siblings across the other
adapters."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings
from jetcore.crypto import generate_encryption_keypair, generate_signing_keypair

if TYPE_CHECKING:
    from jetcore.registry import RecipientCache

NATS_URL = "nats://localhost:4222"
CREDS_DIR = "infra/nats/operator/creds"

ORDER_CREATED_SUBJECT = "events.orders.OrderCreated"


def settings(service_id: str) -> AdapterSettings:
    return AdapterSettings(
        service_id=service_id,
        nats_url=NATS_URL,
        nats_creds_path=f"{CREDS_DIR}/{service_id}.creds",
    )


async def connect(service_id: str) -> BusClient:
    signing = generate_signing_keypair()
    encryption = generate_encryption_keypair()
    return await BusClient.connect(
        settings(service_id),
        adapter_type="test-adapter",
        encryption_keypair=encryption,
        signing_seed=signing.seed,
        signing_public_key=signing.public_key,
    )


async def wait_until_cache_has(cache: RecipientCache, count: int, *, timeout: float = 3.0) -> None:
    async def _poll() -> None:
        while len(cache.current()) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)

"""Shared test support for integration tests that need a live NATS
connection (test_bus_client.py, Step B6; test_end_to_end.py, Step B7).

Not a test module itself (doesn't match test_*.py, so pytest won't collect
it) — a plain helper module, imported explicitly. Fixtures live in
conftest.py instead, where pytest auto-discovers them.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings
from jetcore.crypto import generate_encryption_keypair, generate_signing_keypair

if TYPE_CHECKING:
    from jetcore.registry import RecipientCache

NATS_URL = "nats://localhost:4222"
SUBJECT = "events.files.FileWriteRequested"
CREDS_DIR = "infra/nats/operator/creds"


def settings(service_id: str) -> AdapterSettings:
    # AdapterSettings needs an env-driven load in production (Step B4), but
    # constructing it directly is simpler and equally valid for tests —
    # pydantic-settings models are just pydantic models with extra loading
    # behavior bolted on.
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


async def wait_until_cache_nonempty(cache: RecipientCache, *, timeout: float = 3.0) -> None:
    async def _poll() -> None:
        while not cache.current():
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)

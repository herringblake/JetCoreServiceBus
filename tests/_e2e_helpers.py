"""Test-only helpers for the cross-adapter end-to-end test (Design.md §12
Step E4). A local copy of the connect() pattern used throughout this
project's other test suites — not a cross-package import, for the
self-containment reasons already explained in
libs/jetcore/tests/_helpers.py, adapters/file_storage_adapter/tests/
_file_storage_helpers.py, and adapters/webhook_listener/tests/
_webhook_listener_helpers.py."""

from __future__ import annotations

from jetcore.bus_client import BusClient
from jetcore.config import AdapterSettings
from jetcore.crypto import generate_encryption_keypair, generate_signing_keypair

NATS_URL = "nats://localhost:4222"
CREDS_DIR = "infra/nats/operator/creds"


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

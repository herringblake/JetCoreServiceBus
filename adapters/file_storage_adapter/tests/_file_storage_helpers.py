"""Test-only helpers for file_storage_adapter's live-NATS integration
tests. A local copy of libs/jetcore/tests/_helpers.py's connect() pattern,
not a cross-package import: pytest's default (no __init__.py) test
collection makes bare cross-directory imports depend on what else has been
collected in the same run (the same underlying issue Step C1's
test_scaffold.py collision came from) — each package's test suite stays
self-contained and runnable in isolation instead.

Named _file_storage_helpers.py, not _helpers.py: the module name collision
that C1 found for test_*.py files turned out to apply just as much to a
plain support module — confirmed the hard way, running the full suite
after this file was first written as _helpers.py raised `ImportError:
cannot import name 'CREATE_COMPLETED_SUBJECT' from '_helpers'`, because
libs/jetcore/tests/_helpers.py (collected first) had already claimed
sys.modules['_helpers'] for the whole session. Every bare-imported
non-test module needs an equally unique name for the same reason
test_scaffold.py's fix does — not just test_*.py files."""

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

WRITE_REQUESTED_SUBJECT = "events.files.FileWriteRequested"
CREATE_COMPLETED_SUBJECT = "events.files.FileCreateCompleted"
WRITE_COMPLETED_SUBJECT = "events.files.FileWriteCompleted"


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
    """Waits until `cache` has registered at least `count` recipients —
    generalizes jetcore/tests/_helpers.py's wait_until_cache_nonempty
    (count=1) for tests that need more than one recipient registered
    (e.g. both the real consumer and a test observer) before publishing,
    so the publish is deterministic rather than racing either one's
    subscribe()."""

    async def _poll() -> None:
        while len(cache.current()) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)

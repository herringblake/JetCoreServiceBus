"""Shared fixtures for http_adapter's live-NATS integration tests
(Design.md §13 Step H3 onward). Mirrors the same _clean_state pattern
used throughout this project's other test suites — re-implemented
locally rather than imported, for the self-containment reasons
_http_adapter_helpers.py explains."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from _http_adapter_helpers import CREDS_DIR, NATS_URL, ORDER_CREATED_SUBJECT
from nats.js.errors import NoKeysError


def _is_test_identity(service_id: str) -> bool:
    """Defects.md Defect 2: this fixture's own KV cleanup below used to
    blanket-delete every registered recipient for events.orders.OrderCreated
    — including the REAL, live http-adapter-01/webhook-sender-01/
    db-adapter-mysql-01 containers' own registrations, not just this test
    suite's own ephemeral ones. Confirmed by direct reproduction (polling
    a real container's KV entry once a second during a normal pytest run)
    that this left real containers unregistered for up to 59 consecutive
    seconds, each occurrence producing a real, permanently-undecryptable
    message on their end. Only ever clean up entries this project's own
    test identities own — the `-test` suffix (Defect 1's dedicated twins)
    or the `test-` prefix (test-observer-01) — never a bare real adapter
    serviceId."""
    return service_id.endswith("-test") or service_id.startswith("test-")


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/jetcore-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    kv = await js.key_value("service-directory")
    try:
        for key in await kv.keys(filters=[f"{ORDER_CREATED_SUBJECT}."]):
            if _is_test_identity(key.removeprefix(f"{ORDER_CREATED_SUBJECT}.")):
                await kv.delete(key)
    except NoKeysError:
        pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

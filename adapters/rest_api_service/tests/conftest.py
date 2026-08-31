"""Shared fixtures for rest_api_service's live-NATS integration tests
(Design.md §13 Step I5 onward). Mirrors the same _clean_state pattern
used throughout this project's other test suites — re-implemented
locally rather than imported, for the self-containment reasons
_rest_api_service_helpers.py explains."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from _rest_api_service_helpers import (
    CREDS_DIR,
    NATS_URL,
    ORDER_CREATED_SUBJECT,
    ORDER_PERSISTED_SUBJECT,
)
from nats.js.errors import NoKeysError, NotFoundError

_ALL_SUBJECTS = (ORDER_CREATED_SUBJECT, ORDER_PERSISTED_SUBJECT)

# app.py's own lifespan (Step I5) — unlike every other adapter's test
# helper, which subscribes with a fresh randomized durable name per test —
# subscribes to OrderPersisted with a FIXED name derived from
# settings.service_id, the same as File Storage Adapter's entrypoint
# (Step C6/F5's FIXED_ENTRYPOINT_DURABLE_NAMES) does for the same reason:
# that's what a real deployment needs (resume its own cursor across
# restarts). A leftover consumer from an earlier real-`create_app()` test
# run attaches to (doesn't replace) an existing one on `pull_subscribe`,
# so it needs active deletion between runs, not just a stream purge.
#
# Named after rest-api-service-01-TEST, not rest-api-service-01 itself
# (Defects.md Defect 1) — test_rest_api_service_app.py's SETTINGS connects
# as the dedicated test-only identity, so its durable name (derived from
# settings.service_id) follows suit; this also keeps it from ever
# colliding with the real deployed container's own identically-shaped
# consumer.
FIXED_ENTRYPOINT_DURABLE_NAME = "rest-api-service-01-test-events_orders_OrderPersisted"


def _is_test_identity(service_id: str) -> bool:
    """Defects.md Defect 2: this fixture's own KV cleanup below used to
    blanket-delete every registered recipient for a shared subject like
    events.orders.OrderCreated — including the REAL, live
    rest-api-service-01/db-adapter-mysql-01 containers' own registrations,
    not just this test suite's own ephemeral ones. Confirmed by direct
    reproduction (polling a real container's KV entry once a second during
    a normal pytest run) that this left real containers unregistered for
    up to 59 consecutive seconds, each occurrence producing a real,
    permanently-undecryptable message on their end. Only ever clean up
    entries this project's own test identities own — the `-test` suffix
    (Defect 1's dedicated twins) or the `test-` prefix (test-observer-01) —
    never a bare real adapter serviceId."""
    return service_id.endswith("-test") or service_id.startswith("test-")


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/jetcore-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    try:
        await js.delete_consumer("EVENTS", FIXED_ENTRYPOINT_DURABLE_NAME)
    except NotFoundError:
        pass
    kv = await js.key_value("service-directory")
    for subject in _ALL_SUBJECTS:
        try:
            for key in await kv.keys(filters=[f"{subject}."]):
                if _is_test_identity(key.removeprefix(f"{subject}.")):
                    await kv.delete(key)
        except NoKeysError:
            pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

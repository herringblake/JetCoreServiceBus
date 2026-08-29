"""Shared fixtures for webhook_sender's live-NATS integration tests
(Design.md §13 Step G3 onward). Mirrors the same _clean_state pattern
used throughout this project's other test suites — re-implemented
locally rather than imported, for the self-containment reasons
_webhook_sender_helpers.py explains."""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import nats
import pytest
from _webhook_sender_helpers import CREDS_DIR, NATS_URL, ORDER_CREATED_SUBJECT
from nats.js.errors import NoKeysError


@pytest.fixture(autouse=True)
async def _clean_state() -> AsyncGenerator[None]:
    nc = await nats.connect(NATS_URL, user_credentials=f"{CREDS_DIR}/jetcore-admin.creds")
    js = nc.jetstream()
    await js.purge_stream("EVENTS")
    kv = await js.key_value("service-directory")
    try:
        for key in await kv.keys(filters=[f"{ORDER_CREATED_SUBJECT}."]):
            await kv.delete(key)
    except NoKeysError:
        pass
    await nc.close()
    yield


@pytest.fixture
def durable_name() -> str:
    return f"test-{uuid.uuid4().hex[:8]}"

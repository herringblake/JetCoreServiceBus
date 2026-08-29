"""Tests for app.py (Design.md §12 Step D3) — HTTP-facing behavior only,
against a FakeBusClient, no live NATS needed. The one test that needs a
real bus (confirming a real POST results in a real FileWriteRequested on
the stream) is Step D5's job, not duplicated here.

Named test_webhook_listener_app.py, not test_app.py: proactively unique,
per the naming-collision lesson learned repeatedly in Track C (Design.md
§12 Step C1) — a future FastAPI-based adapter (e.g. the REST API Service,
Phase 3) would very plausibly also want a module named app.py/test_app.py.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from jetcore.bus_client import BusClient
from webhook_listener.app import create_app
from webhook_listener.settings import WebhookListenerSettings

WEBHOOK_SECRET = "test-secret-value"


class FakeBusClient(BusClient):
    """A publish-recording stand-in — subclasses BusClient (not just
    duck-typed) so mypy --strict accepts it wherever a real BusClient is
    expected, without calling the real __init__ (which needs a live
    connection this fake never makes)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, str]] = []

    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        event_type: str,
        event_schema_version: str = "1.0.0",
        correlation_id: str | None = None,
    ) -> None:
        self.published.append((subject, payload, event_type))


@pytest.fixture
def fake_bus() -> FakeBusClient:
    return FakeBusClient()


@pytest.fixture
def client(fake_bus: FakeBusClient) -> Iterator[TestClient]:
    settings = WebhookListenerSettings(
        service_id="webhook-listener-01",
        nats_creds_path="infra/nats/operator/creds/webhook-listener-01.creds",
        webhook_secret=WEBHOOK_SECRET,
    )
    app = create_app(settings, bus_client=fake_bus)
    # Context-managed, not a bare TestClient(app): FastAPI/Starlette only
    # runs the app's `lifespan` (which sets app.state.bus_client) on
    # __enter__/__exit__ — confirmed by testing, a bare TestClient(app)
    # used outside a `with` block never triggers it, silently leaving
    # app.state.bus_client unset until a request actually needs it.
    with TestClient(app) as client:
        yield client


def test_healthz_returns_ok(client: TestClient) -> None:
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_valid_secret_publishes_and_returns_202(
    client: TestClient, fake_bus: FakeBusClient
) -> None:
    response = client.post(
        "/webhooks/notes/todo.txt",
        content=b"hello from a webhook",
        headers={"X-Webhook-Secret": WEBHOOK_SECRET},
    )

    assert response.status_code == 202
    assert len(fake_bus.published) == 1
    subject, payload, event_type = fake_bus.published[0]
    assert subject == "events.files.FileWriteRequested"
    assert event_type == "FileWriteRequested"
    data = json.loads(payload)
    assert data["path"] == "notes/todo.txt"
    assert base64.b64decode(data["content"]) == b"hello from a webhook"


def test_missing_secret_header_is_rejected(client: TestClient, fake_bus: FakeBusClient) -> None:
    response = client.post("/webhooks/notes/todo.txt", content=b"hello")

    assert response.status_code == 401
    assert fake_bus.published == []


def test_wrong_secret_is_rejected(client: TestClient, fake_bus: FakeBusClient) -> None:
    response = client.post(
        "/webhooks/notes/todo.txt",
        content=b"hello",
        headers={"X-Webhook-Secret": "not-the-right-secret"},
    )

    assert response.status_code == 401
    assert fake_bus.published == []


def test_empty_path_is_rejected(client: TestClient, fake_bus: FakeBusClient) -> None:
    response = client.post(
        "/webhooks/", content=b"hello", headers={"X-Webhook-Secret": WEBHOOK_SECRET}
    )

    assert response.status_code in (400, 404)  # routing itself may 404 on an empty {path:path}
    assert fake_bus.published == []


def test_nested_path_is_preserved_verbatim(client: TestClient, fake_bus: FakeBusClient) -> None:
    response = client.post(
        "/webhooks/a/b/c/deep.txt",
        content=b"nested",
        headers={"X-Webhook-Secret": WEBHOOK_SECRET},
    )

    assert response.status_code == 202
    data = json.loads(fake_bus.published[0][1])
    assert data["path"] == "a/b/c/deep.txt"


def test_binary_body_round_trips_through_base64(
    client: TestClient, fake_bus: FakeBusClient
) -> None:
    binary_body = bytes(range(256))

    response = client.post(
        "/webhooks/binary.dat",
        content=binary_body,
        headers={"X-Webhook-Secret": WEBHOOK_SECRET},
    )

    assert response.status_code == 202
    data = json.loads(fake_bus.published[0][1])
    assert base64.b64decode(data["content"]) == binary_body

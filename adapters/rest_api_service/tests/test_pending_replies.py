"""Unit tests for pending_replies.py's PendingReplies (Design.md §13 Step
I4) — pure in-memory logic, no live NATS needed. `run_reply_watcher`'s
own real-bus behavior (an orphaned reply still gets acked) is proven
instead by test_rest_api_service_app.py's real sync-reply/timeout tests,
the same "unit-test the pure logic here, integration-test the wiring
there" split D3/D5 already established.
"""

from __future__ import annotations

from jetcore.envelope import EncryptionMetadata, Event, EventDetails, EventEnvelope
from rest_api_service.pending_replies import PendingReplies


def _received_event(*, correlation_id: str | None) -> object:
    """A real ReceivedEvent built from a real Event — resolve() only ever
    reads `.details`, never `._msg` (that's ack()/nak()'s job), so a bare
    object() stands in for the underlying nats.aio.msg.Msg with no need
    for a real subscription."""
    from jetcore.bus_client import ReceivedEvent

    details = EventDetails(
        eventType="OrderPersisted",
        eventSchemaVersion="1.0.0",
        sourceServiceId="db-adapter-mysql-01",
        correlationId=correlation_id,
    )
    envelope = EventEnvelope(
        eventDetails=details,
        encryption=EncryptionMetadata(
            algorithm="age-v1 (X25519 + XChaCha20-Poly1305)", recipients=[]
        ),
        eventPayload="",
    )
    return ReceivedEvent(event=Event(event=envelope), payload=b'{"orderId": "abc"}', msg=object())  # type: ignore[arg-type]


async def test_register_then_resolve_completes_the_future() -> None:
    pending = PendingReplies()
    future = pending.register("corr-1")

    event = _received_event(correlation_id="corr-1")
    matched = pending.resolve(event)  # type: ignore[arg-type]

    assert matched is True
    assert future.done()
    assert future.result() is event


async def test_resolve_with_no_matching_registration_returns_false() -> None:
    pending = PendingReplies()

    matched = pending.resolve(_received_event(correlation_id="never-registered"))  # type: ignore[arg-type]

    assert matched is False


async def test_resolve_with_no_correlation_id_returns_false() -> None:
    pending = PendingReplies()
    pending.register("corr-1")

    matched = pending.resolve(_received_event(correlation_id=None))  # type: ignore[arg-type]

    assert matched is False


async def test_unregister_makes_a_later_reply_unmatched() -> None:
    pending = PendingReplies()
    pending.register("corr-1")
    pending.unregister("corr-1")

    matched = pending.resolve(_received_event(correlation_id="corr-1"))  # type: ignore[arg-type]

    assert matched is False


async def test_resolve_is_idempotent_a_second_reply_does_not_match_again() -> None:
    """A duplicate/late second OrderPersisted for the same correlationId
    (shouldn't happen in practice, but nothing prevents it) must not
    re-resolve an already-done future — that would raise
    InvalidStateError."""
    pending = PendingReplies()
    pending.register("corr-1")
    first = pending.resolve(_received_event(correlation_id="corr-1"))  # type: ignore[arg-type]

    second = pending.resolve(_received_event(correlation_id="corr-1"))  # type: ignore[arg-type]

    assert first is True
    assert second is False

"""Round-trip and shape tests for the event envelope (Design.md §5).

No crypto here (that's B3) — encryption/signature fields use dummy sample
values, just enough to exercise the model faithfully.
"""

import json
from datetime import UTC, datetime

import pytest
from gsb_core.envelope import (
    EncryptionMetadata,
    Event,
    EventDetails,
    EventEnvelope,
    new_event_id,
)
from pydantic import ValidationError


def _sample_event(**details_overrides: object) -> Event:
    details = EventDetails(
        eventType="OrderCreated",
        eventSchemaVersion="1.0.0",
        sourceServiceId="rest-api-service-01",
        **details_overrides,
    )
    envelope = EventEnvelope(
        eventDetails=details,
        encryption=EncryptionMetadata(
            algorithm="age-v1 (X25519 + XChaCha20-Poly1305)",
            recipients=["age1fjl83jrsqx8hznnpxrlh5g4vu9wjfg4gmkvhr6mwkyeufnzcaghqv0msgq"],
        ),
        eventPayload="Y2lwaGVydGV4dA==",
    )
    return Event(event=envelope)


def test_round_trip_preserves_all_fields() -> None:
    original = _sample_event(correlationId="corr-123", signature="c2ln")

    wire_bytes = original.to_wire()
    restored = Event.from_wire(wire_bytes)

    assert restored == original


def test_wire_shape_matches_design_doc_section_5() -> None:
    """The exact key names/nesting from Design.md §5 — camelCase, under a
    top-level "event" key. If this drifts, the design doc and the code have
    diverged."""
    event = _sample_event(correlationId="corr-123", signature="c2ln")
    parsed = json.loads(event.to_wire())

    assert set(parsed.keys()) == {"event"}
    details = parsed["event"]["eventDetails"]
    assert set(details.keys()) == {
        "eventId",
        "eventCreated",
        "eventType",
        "eventSchemaVersion",
        "sourceServiceId",
        "correlationId",
        "signature",
    }
    encryption = parsed["event"]["encryption"]
    assert set(encryption.keys()) == {"algorithm", "recipients"}
    assert isinstance(encryption["recipients"][0], str)
    assert "eventPayload" in parsed["event"]


def test_event_id_defaults_to_a_fresh_uuid7_when_not_supplied() -> None:
    d1 = EventDetails(eventType="X", eventSchemaVersion="1.0.0", sourceServiceId="svc-1")
    d2 = EventDetails(eventType="X", eventSchemaVersion="1.0.0", sourceServiceId="svc-1")

    assert d1.event_id != d2.event_id
    # UUIDv7 is time-ordered: string comparison of the standard hex form
    # reflects creation order for ids minted moments apart.
    assert d1.event_id < d2.event_id


def test_new_event_id_helper_produces_a_valid_uuid7_string() -> None:
    from uuid6 import UUID

    parsed = UUID(new_event_id())
    assert parsed.version == 7


def test_event_created_defaults_to_now_utc_when_not_supplied() -> None:
    before = datetime.now(UTC)
    details = EventDetails(eventType="X", eventSchemaVersion="1.0.0", sourceServiceId="svc-1")
    after = datetime.now(UTC)

    assert before <= details.event_created <= after
    assert details.event_created.tzinfo is not None


def test_correlation_id_and_signature_default_to_none() -> None:
    details = EventDetails(eventType="X", eventSchemaVersion="1.0.0", sourceServiceId="svc-1")

    assert details.correlation_id is None
    assert details.signature is None

    parsed = json.loads(details.model_dump_json(by_alias=True))
    assert parsed["correlationId"] is None
    assert parsed["signature"] is None


def test_missing_required_field_raises_validation_error() -> None:
    with pytest.raises(ValidationError):
        EventDetails(eventSchemaVersion="1.0.0", sourceServiceId="svc-1")  # type: ignore[call-arg]


def test_envelope_is_frozen() -> None:
    event = _sample_event()
    with pytest.raises(ValidationError):
        event.event.event_payload = "different"


def test_can_construct_with_python_attribute_names_too() -> None:
    """populate_by_name=True — internal code (crypto.py, bus_client.py)
    shouldn't be forced to use camelCase kwargs. Runtime-verified here;
    mypy's pydantic plugin doesn't know about populate_by_name and always
    expects the aliases (a known plugin limitation, not a real type error —
    confirmed by running it, not assumed), hence the ignore below."""
    details = EventDetails(  # type: ignore[call-arg]
        event_type="OrderCreated",
        event_schema_version="1.0.0",
        source_service_id="rest-api-service-01",
    )
    assert details.event_type == "OrderCreated"

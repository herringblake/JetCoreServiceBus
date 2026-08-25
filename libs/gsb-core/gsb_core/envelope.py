"""The event envelope (Design.md §5).

Pure data modeling + (de)serialization — Step B2 (Design.md §11 Track B).
Does not touch NATS or crypto: `signature` and the `encryption`/
`eventPayload` fields are populated by whoever actually signs and encrypts
(crypto.py, Step B3), not by this module. This module's job is just to be
an accurate, round-trippable model of the wire format everyone else agrees
on, plus the one piece of "pure logic" that doesn't need crypto — generating
fresh metadata (eventId/eventCreated) for a new event.
"""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field
from uuid6 import uuid7

_MODEL_CONFIG = ConfigDict(frozen=True, populate_by_name=True)


def new_event_id() -> str:
    """A fresh UUIDv7 event id, as a string."""
    return str(uuid7())


def utc_now() -> datetime:
    """The current time, UTC, timezone-aware — used as eventCreated's
    default so every envelope gets a real timestamp without the caller
    having to remember to supply one."""
    return datetime.now(UTC)


class EventDetails(BaseModel):
    """Baseline event metadata (Design.md §5)."""

    model_config = _MODEL_CONFIG

    event_id: str = Field(default_factory=new_event_id, alias="eventId")
    event_created: datetime = Field(default_factory=utc_now, alias="eventCreated")
    event_type: str = Field(alias="eventType")
    event_schema_version: str = Field(alias="eventSchemaVersion")
    source_service_id: str = Field(alias="sourceServiceId")
    correlation_id: str | None = Field(default=None, alias="correlationId")

    # Optional here on purpose: Step B2 doesn't sign anything (that's
    # crypto.py, B3). BusClient (B6) is responsible for refusing to publish
    # an envelope whose signature is still None — see Design.md Decision #5.
    signature: str | None = Field(default=None, alias="signature")

    # Added during Step B6: a recipient verifying `signature` needs the
    # sender's Ed25519 *public* nkey, and there was no way to get one —
    # `sourceServiceId` is just an opaque string, and no registry maps
    # "serviceId -> signing public key" (service-directory is keyed by
    # (subject, serviceId) for *recipient* registration, a different
    # lookup entirely). Public keys aren't secret, so carrying it directly
    # in the envelope is the simplest fix — no new directory needed. Same
    # optionality reasoning as `signature` above.
    source_public_key: str | None = Field(default=None, alias="sourcePublicKey")


class EncryptionMetadata(BaseModel):
    """Design.md §4.3 / §5 — the hybrid-encryption envelope metadata.

    `recipients` is a flat list of recipient public-key strings (age
    "recipient" strings, e.g. "age1..."), not `{keyId, wrappedKey}` pairs
    as an earlier draft of this schema had it. Corrected during Step B3
    once real encryption was implemented: age (via `pyrage`) bundles all
    per-recipient key-wrapping *inside* its single ciphertext blob — there
    is no separate "wrapped key" to expose per recipient at the application
    level (confirmed by testing `pyrage.encrypt()`'s actual output, not
    assumed from the format's docs). This list is therefore informational —
    who this event was encrypted for, useful for matching against the
    service-directory registry (§4.5) — not itself decryption material.
    """

    model_config = _MODEL_CONFIG

    algorithm: str
    recipients: list[str]


class EventEnvelope(BaseModel):
    """The `event` object itself (Design.md §5) — everything except the
    outer wrapper key."""

    model_config = _MODEL_CONFIG

    event_details: EventDetails = Field(alias="eventDetails")
    encryption: EncryptionMetadata
    event_payload: str = Field(alias="eventPayload")


class Event(BaseModel):
    """The full wire-format message: `{"event": {...}}` (Design.md §5).

    This is what actually gets published to / received from NATS.
    """

    model_config = _MODEL_CONFIG

    event: EventEnvelope

    def to_wire(self) -> bytes:
        """Serialize to the exact JSON-over-the-wire shape (camelCase
        keys, per Design.md §5) as UTF-8 bytes, ready to publish."""
        return self.model_dump_json(by_alias=True).encode("utf-8")

    @classmethod
    def from_wire(cls, data: bytes) -> Event:
        """Parse a message received off the bus back into a validated
        Event. Raises pydantic.ValidationError on a malformed envelope."""
        return cls.model_validate_json(data)

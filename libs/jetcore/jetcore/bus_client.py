"""BusClient — ties envelope (B2) + crypto (B3) + registry (B5) + JetStream
publish/subscribe into one adapter-facing façade (Design.md §11 Track B,
Step B6). Integration-tested against the real Track A stack.

Publish flow (Design.md §4.3 sequence diagram):
  1. Look up current recipients for the subject (via a cached, watch-backed
     `RecipientCache` from registry.py — no GET per publish).
  2. Encrypt the plaintext payload for those recipients (crypto.py).
  3. Sign the plaintext's digest with this adapter's own nkey (crypto.py).
  4. Build the envelope (envelope.py) and publish the wire bytes.

Consume flow:
  1. Register this adapter as a recipient for the subject + start a
     heartbeat (registry.py) — so publishers can find it.
  2. Pull from a durable, filtered JetStream consumer (Design.md §6).
  3. Decrypt with this adapter's own private key, then verify the
     signature against the envelope's `sourcePublicKey` (added to
     envelope.py during this step — see its docstring for why).
  4. Hand back a `ReceivedEvent`; the caller acks or naks explicitly.

A message that fails to parse, decrypt, or verify is logged and left
unacked (→ redelivery, and eventually the consumer's own max-deliver
handling) rather than silently dropped or, worse, treated as trustworthy.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
from typing import TYPE_CHECKING

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from jetcore.config import AdapterSettings
from jetcore.creds import load_signing_keypair
from jetcore.crypto import (
    EncryptionKeyPair,
    decrypt,
    encrypt_for_recipients,
    generate_encryption_keypair,
    sign,
    verify,
)
from jetcore.envelope import EncryptionMetadata, Event, EventDetails, EventEnvelope
from jetcore.registry import RecipientCache, RegistryClient

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient
    from nats.aio.msg import Msg
    from nats.js import JetStreamContext

logger = logging.getLogger(__name__)

ENCRYPTION_ALGORITHM = "age-v1 (X25519 + XChaCha20-Poly1305)"


class SignatureVerificationError(Exception):
    """A received event's signature didn't verify against its claimed
    sourcePublicKey. Decision #5's entire point is that this is loud, not
    silently swallowed."""


class ReceivedEvent:
    """A successfully decrypted + verified message, handed to the caller
    from `BusClient.fetch()`. Ack/nak explicitly — nothing here is
    auto-acked."""

    def __init__(self, event: Event, payload: bytes, msg: Msg) -> None:
        self.event = event
        self.payload = payload
        self._msg = msg

    @property
    def details(self) -> EventDetails:
        return self.event.event.event_details

    async def ack(self) -> None:
        await self._msg.ack()

    async def nak(self) -> None:
        await self._msg.nak()


def _consumer_config(subject: str) -> ConsumerConfig:
    return ConsumerConfig(
        filter_subject=subject,
        ack_policy=AckPolicy.EXPLICIT,
        deliver_policy=DeliverPolicy.ALL,
    )


class BusClient:
    def __init__(
        self,
        *,
        nc: NATSClient,
        js: JetStreamContext,
        registry: RegistryClient,
        settings: AdapterSettings,
        adapter_type: str,
        encryption_keypair: EncryptionKeyPair,
        signing_seed: str,
        signing_public_key: str,
    ) -> None:
        self._nc = nc
        self._js = js
        self._registry = registry
        self._settings = settings
        self._adapter_type = adapter_type
        self._encryption_keypair = encryption_keypair
        self._signing_seed = signing_seed
        self._signing_public_key = signing_public_key
        self._recipient_caches: dict[str, RecipientCache] = {}
        self._subscriptions: dict[str, JetStreamContext.PullSubscription] = {}
        self._background_tasks: list[asyncio.Task[None]] = []

    @classmethod
    async def connect(
        cls,
        settings: AdapterSettings,
        *,
        adapter_type: str,
        encryption_keypair: EncryptionKeyPair,
        signing_seed: str,
        signing_public_key: str,
    ) -> BusClient:
        nc = await nats.connect(settings.nats_url, user_credentials=str(settings.nats_creds_path))
        js = nc.jetstream()
        registry = await RegistryClient.connect(js)
        # Registers this adapter's own signing key in the service-identity
        # directory (Design.md §9 item #4) — once, here, not on a
        # heartbeat (see registry.py's module docstring for why). Every
        # adapter does this, publisher or subscriber, so recipients can
        # look up *any* claimed sender's trusted key.
        await registry.register_identity(
            service_id=settings.service_id,
            adapter_type=adapter_type,
            signing_public_key=signing_public_key,
        )
        return cls(
            nc=nc,
            js=js,
            registry=registry,
            settings=settings,
            adapter_type=adapter_type,
            encryption_keypair=encryption_keypair,
            signing_seed=signing_seed,
            signing_public_key=signing_public_key,
        )

    @classmethod
    async def connect_as_adapter(cls, settings: AdapterSettings, *, adapter_type: str) -> BusClient:
        """The connection path a real adapter entrypoint should use
        (Design.md §12 Step C6), not the lower-level `connect()` above
        (kept as-is for tests/tooling that want full control over both
        keypairs — every existing test still does).

        Signing identity: derived from `settings.nats_creds_path`'s own
        embedded nkey (jetcore.creds), the SAME key the connection itself
        authenticates with — stable across restarts, unlike a freshly
        generated one. B7 documented exactly what goes wrong when this
        isn't stable (concurrent connections invalidating each other's
        registered identity); a restarting adapter is the same problem
        across time instead of across connections.

        Encryption keypair: still generated fresh every call — Design.md
        has no persistent-storage answer for this yet (§9, a new open
        item this step added, not silently skipped). Consequence: any
        message encrypted for this adapter and still unconsumed across a
        restart becomes undecryptable. Low-stakes for Phase 2's
        command/response flow (a lost FileWriteRequested is retriable by
        its sender); worth resolving before real production use."""
        signing = load_signing_keypair(settings.nats_creds_path)
        encryption = generate_encryption_keypair()
        return await cls.connect(
            settings,
            adapter_type=adapter_type,
            encryption_keypair=encryption,
            signing_seed=signing.seed,
            signing_public_key=signing.public_key,
        )

    async def close(self) -> None:
        for task in self._background_tasks:
            task.cancel()
        for task in self._background_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for cache in self._recipient_caches.values():
            await cache.stop()
        await self._nc.close()

    async def _cache_for(self, subject: str) -> RecipientCache:
        if subject not in self._recipient_caches:
            self._recipient_caches[subject] = await self._registry.watch(subject)
        return self._recipient_caches[subject]

    # --- Publish -------------------------------------------------------

    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        event_type: str,
        event_schema_version: str = "1.0.0",
        correlation_id: str | None = None,
    ) -> None:
        cache = await self._cache_for(subject)
        recipients = cache.current()
        if not recipients:
            logger.warning("publishing to %s with no registered recipients yet", subject)

        ciphertext = encrypt_for_recipients(payload, recipients) if recipients else b""
        signature = sign(self._signing_seed, payload)

        details = EventDetails(
            eventType=event_type,
            eventSchemaVersion=event_schema_version,
            sourceServiceId=self._settings.service_id,
            sourcePublicKey=self._signing_public_key,
            correlationId=correlation_id,
            signature=base64.b64encode(signature).decode(),
        )
        envelope = EventEnvelope(
            eventDetails=details,
            encryption=EncryptionMetadata(algorithm=ENCRYPTION_ALGORITHM, recipients=recipients),
            eventPayload=base64.b64encode(ciphertext).decode(),
        )
        await self._js.publish(subject, Event(event=envelope).to_wire())

    # --- Consume ---------------------------------------------------------

    async def subscribe(self, subject: str, *, durable_name: str | None = None) -> str:
        """Registers this adapter as a recipient for `subject` (+ starts
        its heartbeat) and sets up a durable, filtered pull consumer.
        Idempotent — safe to call again (e.g. on reconnect)."""
        durable_name = durable_name or f"{self._settings.service_id}-{subject}"

        task = await self._registry.heartbeat(
            subject=subject,
            service_id=self._settings.service_id,
            adapter_type=self._adapter_type,
            encryption_public_key=self._encryption_keypair.public_key,
        )
        self._background_tasks.append(task)

        self._subscriptions[durable_name] = await self._js.pull_subscribe(
            subject, durable=durable_name, config=_consumer_config(subject)
        )
        return durable_name

    async def fetch(
        self, durable_name: str, *, batch: int = 1, timeout: float = 5.0
    ) -> list[ReceivedEvent]:
        """Pulls up to `batch` messages from a consumer already set up via
        `subscribe()`. A message that fails to parse/decrypt/verify is
        logged and left unacked (redelivery), not silently dropped.

        Returns an empty list if nothing is available within `timeout` —
        not an exception. nats-py's underlying `PullSubscription.fetch()`
        actually *raises* on an idle timeout (confirmed by testing while
        building Design.md §12 Step C4 — no Phase 1 test had ever called
        `fetch()` with truly nothing pending to notice this before), the
        same "raises on idle rather than returning an empty/None result"
        shape already documented for `KeyWatcher.updates()` in registry.py.
        A caller polling in a loop (any real adapter's main loop, or a
        test asserting "nothing left to redeliver") shouldn't have to
        wrap every call in its own try/except for this.

        Catches the builtin `TimeoutError`, not the narrower
        `nats.errors.TimeoutError` the first version of this fix used —
        confirmed by testing (a real, intermittent Step C7 test failure,
        not found by reading docs) that nats-py's `fetch()` has a *second*
        internal timeout path (its "lingering request" retry logic, for
        when an initial no-wait probe returns nothing) that raises the
        *bare* `asyncio.TimeoutError` directly rather than wrapping it in
        `nats.errors.TimeoutError`. Since `nats.errors.TimeoutError` is
        itself a `TimeoutError` subclass, catching the builtin covers both
        paths instead of just the one that happened to get exercised
        first."""
        sub = self._subscriptions.get(durable_name)
        if sub is None:
            raise ValueError(f"no consumer named {durable_name!r} — call subscribe() first")

        try:
            msgs = await sub.fetch(batch=batch, timeout=timeout)
        except TimeoutError:
            return []

        received = []
        for msg in msgs:
            try:
                received.append(await self._process(msg))
            except Exception:
                logger.exception(
                    "failed to process message on %s, leaving unacked for redelivery",
                    msg.subject,
                )
        return received

    async def _process(self, msg: Msg) -> ReceivedEvent:
        event = Event.from_wire(msg.data)
        details = event.event.event_details

        ciphertext = base64.b64decode(event.event.event_payload)
        plaintext = decrypt(ciphertext, self._encryption_keypair.private_key)

        if details.signature is None:
            raise SignatureVerificationError(f"event {details.event_id} is missing a signature")

        # Verify against the TRUSTED key looked up from service-identity,
        # not the message's own embedded sourcePublicKey — that's the
        # actual fix for Design.md §9 item #4. A self-consistent (key,
        # signature) pair embedded in the message proves nothing about who
        # really sent it; only a key independently known to belong to
        # sourceServiceId does. Confirmed by testing during Step B6: a
        # message that only had to be internally self-consistent passed
        # verification while claiming a false identity.
        trusted_key = await self._registry.lookup_signing_key(details.source_service_id)
        if trusted_key is None:
            raise SignatureVerificationError(
                f"no registered identity for sourceServiceId {details.source_service_id!r} "
                f"— cannot verify event {details.event_id}"
            )
        if details.source_public_key is not None and details.source_public_key != trusted_key:
            # Not fatal on its own — the trusted-key check below is what
            # actually matters — but a message whose own claim disagrees
            # with the trusted directory is a distinct, worth-logging
            # signal (e.g. a stale/rotated key, or something more
            # suspicious) separate from "who really signed this."
            logger.warning(
                "event %s claims a sourcePublicKey that doesn't match the "
                "registered identity for %s",
                details.event_id,
                details.source_service_id,
            )

        sig = base64.b64decode(details.signature)
        if not verify(trusted_key, plaintext, sig):
            raise SignatureVerificationError(
                f"signature verification failed for {details.event_id}"
            )

        return ReceivedEvent(event=event, payload=plaintext, msg=msg)

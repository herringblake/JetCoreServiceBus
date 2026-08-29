"""The service-directory KV registry client (Design.md §4.5) — Step B5
(Design.md §11 Track B). The first jetcore module that needs a live NATS
connection; built and tested against the real Track A stack (Step A6), not
a mock.

Three things live here:
  - `RegistryClient` — register/heartbeat this adapter's own recipient
    entries (one per subject it subscribes to), plus a one-shot lookup.
  - `RecipientCache` — a live, watch-updated view of who's currently
    registered for one subject, for a publisher to consult before
    encrypting (Design.md §4.5: "a JetStream KV watch, not a GET per
    publish").
  - Identity registration/lookup (`register_identity`/`lookup_signing_key`,
    the `service-identity` bucket) — added to resolve Design.md §9 item #4,
    a real gap found while building Step B6: `EventDetails.sourcePublicKey`
    alone only proves a signature is *self-consistent*, not that the
    embedded key actually belongs to the claimed sender. Every adapter
    registers its own signing key here once at connect time (unlike
    service-directory, this isn't scoped to "subjects subscribed to" — a
    pure publisher that never subscribes to anything still needs an
    identity entry so others can verify messages it sends), and a
    recipient looks up the *trusted* key for `sourceServiceId` here rather
    than trusting the message's own embedded claim.

Both were built against real, confirmed nats-py behavior, not its docs —
several things here don't work the way a first read of the API would
suggest:

  - `KeyValue.keys(filters=...)` is NOT a NATS wildcard filter, despite the
    name and despite accepting strings that look like subjects. Reading its
    source shows it's a plain Python substring check (`any(f in key.key for
    f in filters)`) applied client-side, after fetching *everything* via an
    unfiltered watch. A literal `*`/`>` there can never match anything,
    since keys never contain those characters. `lookup_recipients()` below
    passes a literal prefix string instead (e.g. `"events.files.Foo."`),
    which works precisely because it IS a real substring of the matching
    keys — confirmed by testing both forms against a live bucket.
  - `KeyValue.watch(keys=...)`, by contrast, DOES use its argument as a
    real NATS subject (`f"{self._pre}{keys}"`, feeding an actual consumer
    filter) — confirmed correct by watching `"events.files.Foo.*"` and
    verifying a concurrent put to an unrelated key never arrives.
  - A watch's first delivered item, and every "you're caught up to live
    data" point after that, is `None` — not a real entry. This is nats-py's
    own convention (`keys()` relies on the same signal internally).
  - TTL-based expiry (Design.md §4.5/§11's 60s bucket TTL) arrives at a
    watcher as an entry with `operation == KV_PURGE` (nats-server 2.11+'s
    `Nats-Marker-Reason: MaxAge` marker, translated by nats-py), distinct
    from an explicit `KV_DEL` — both need to be treated as "remove this
    key from the cache," which is what makes `RecipientCache` self-heal
    when an adapter stops heartbeating, with no special-casing needed.
  - `KeyWatcher.updates(timeout=...)` *raises* `nats.errors.TimeoutError`
    when idle past the timeout — it does not return `None` for "nothing
    right now" (that `None` is reserved for the caught-up signal above).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import nats.errors
from nats.js.errors import KeyNotFoundError, NoKeysError
from nats.js.kv import KV_DEL, KV_PURGE

if TYPE_CHECKING:
    from nats.js import JetStreamContext
    from nats.js.kv import KeyValue

logger = logging.getLogger(__name__)

BUCKET_NAME = "service-directory"
IDENTITY_BUCKET_NAME = "service-identity"

# Design.md §11 parameter table: TTL 60s, heartbeat well under that (3x
# safety margin) so a couple of missed beats don't drop registration.
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 20.0

_WATCH_POLL_TIMEOUT_SECONDS = 5.0


def _registration_value(*, service_id: str, adapter_type: str, encryption_public_key: str) -> bytes:
    """The value shape from Design.md §4.5: {serviceId, adapterType,
    encryptionPublicKey, registeredAt}."""
    return json.dumps(
        {
            "serviceId": service_id,
            "adapterType": adapter_type,
            "encryptionPublicKey": encryption_public_key,
            "registeredAt": datetime.now(UTC).isoformat(),
        }
    ).encode()


def _kv_key(subject: str, service_id: str) -> str:
    """Design.md §4.5's key shape: <subject>.<serviceId>."""
    return f"{subject}.{service_id}"


def _identity_value(*, service_id: str, adapter_type: str, signing_public_key: str) -> bytes:
    """The service-identity value shape — parallel to
    `_registration_value`, but for the identity/trust directory rather
    than recipient liveness."""
    return json.dumps(
        {
            "serviceId": service_id,
            "adapterType": adapter_type,
            "signingPublicKey": signing_public_key,
            "registeredAt": datetime.now(UTC).isoformat(),
        }
    ).encode()


class RegistryClient:
    """Register/heartbeat/deregister this adapter's own entries, and do a
    one-shot recipient lookup. For a publisher that needs live-updated
    recipients (the normal case before encrypting), use `RecipientCache`
    instead — this class's `lookup_recipients` does a fresh KV scan every
    call, which is exactly what Design.md §4.5 says NOT to do per publish.
    """

    def __init__(self, kv: KeyValue, identity_kv: KeyValue) -> None:
        self._kv = kv
        self._identity_kv = identity_kv

    @classmethod
    async def connect(cls, js: JetStreamContext) -> RegistryClient:
        kv = await js.key_value(BUCKET_NAME)
        identity_kv = await js.key_value(IDENTITY_BUCKET_NAME)
        return cls(kv, identity_kv)

    async def register(
        self, *, subject: str, service_id: str, adapter_type: str, encryption_public_key: str
    ) -> None:
        """Register (or refresh) this adapter as a recipient for
        `subject`. The bucket's own TTL handles expiry automatically —
        call this periodically (see `heartbeat`) to stay registered."""
        await self._kv.put(
            _kv_key(subject, service_id),
            _registration_value(
                service_id=service_id,
                adapter_type=adapter_type,
                encryption_public_key=encryption_public_key,
            ),
        )

    async def deregister(self, *, subject: str, service_id: str) -> None:
        """Explicit deregistration (Design.md §4.5's `tools/` CLI use
        case). The common "adapter just died" case doesn't need this —
        the TTL handles it."""
        await self._kv.delete(_kv_key(subject, service_id))

    async def register_identity(
        self, *, service_id: str, adapter_type: str, signing_public_key: str
    ) -> None:
        """Registers this adapter's own signing public key in the
        service-identity directory — call once, at connect time (not on a
        heartbeat; see the module docstring for why no TTL is involved
        here). Every adapter does this, publisher or subscriber, since a
        recipient needs to be able to look up *any* claimed sender's key."""
        await self._identity_kv.put(
            service_id,
            _identity_value(
                service_id=service_id,
                adapter_type=adapter_type,
                signing_public_key=signing_public_key,
            ),
        )

    async def lookup_signing_key(self, service_id: str) -> str | None:
        """The *trusted* signing public key registered for `service_id`,
        or None if that serviceId has never registered an identity. A
        recipient should verify a message's signature against this, not
        against the message's own embedded `sourcePublicKey` claim —
        that's the whole point (Design.md §9 item #4)."""
        try:
            entry = await self._identity_kv.get(service_id)
        except KeyNotFoundError:
            return None
        if entry.value is None:
            return None
        try:
            value = json.loads(entry.value)
            return str(value["signingPublicKey"])
        except (json.JSONDecodeError, KeyError):
            logger.warning("skipping malformed service-identity entry %s", service_id)
            return None

    async def heartbeat(
        self,
        *,
        subject: str,
        service_id: str,
        adapter_type: str,
        encryption_public_key: str,
        interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> asyncio.Task[None]:
        """Registers once, synchronously (awaited — see below for why),
        then starts a background task that re-registers every
        `interval_seconds` after that. Returns the Task so the caller can
        cancel it on shutdown; a failed periodic registration attempt is
        logged and retried on the next interval rather than killing the
        loop (the initial one, awaited directly here, is NOT swallowed —
        a caller relying on "subscribe() returned, so I'm registered"
        should find out immediately if that first write failed).

        The first registration is deliberately awaited here rather than
        left to fire inside the background loop's first iteration —
        confirmed by testing (an intermittent, hard-to-reproduce cross-
        test failure during Design.md §12 Step E4) that a plain
        `task.cancel()` arriving while that very first `register()` call
        was still in flight doesn't — can't — retract the write bytes
        already sent to the server: cancellation only stops the client
        from waiting on the response, not the server from applying it.
        The write would then land at some indeterminate later moment,
        occasionally *after* a completely different, later caller had
        already purged and re-registered under the same serviceId,
        silently overwriting it with stale data. Awaiting the first call
        here closes that window for the case that actually matters (a
        caller that `subscribe()`s and shortly after `close()`s, exactly
        every test in this project's suite) — the periodic re-heartbeat
        loop below still uses plain cancellation, which is fine: a
        production adapter's shutdown isn't immediately followed by a
        *different* connection reusing its exact serviceId within
        milliseconds, the specific scenario that makes the race land."""
        await self.register(
            subject=subject,
            service_id=service_id,
            adapter_type=adapter_type,
            encryption_public_key=encryption_public_key,
        )

        async def _loop() -> None:
            while True:
                await asyncio.sleep(interval_seconds)
                try:
                    await self.register(
                        subject=subject,
                        service_id=service_id,
                        adapter_type=adapter_type,
                        encryption_public_key=encryption_public_key,
                    )
                except Exception:
                    logger.exception(
                        "heartbeat registration failed for %s", _kv_key(subject, service_id)
                    )

        return asyncio.create_task(_loop())

    async def watch(self, subject: str) -> RecipientCache:
        """A live, watch-updated `RecipientCache` for `subject`, started
        and ready to use — added during Step B6 so `BusClient` doesn't
        need access to this client's private `_kv` to build one itself."""
        cache = RecipientCache(self._kv, subject)
        await cache.start()
        return cache

    async def lookup_recipients(self, subject: str) -> list[str]:
        """One-shot lookup of current recipient public keys for
        `subject`. Prefer `RecipientCache` for a publisher that looks this
        up repeatedly."""
        prefix = f"{subject}."
        try:
            keys = await self._kv.keys(filters=[prefix])
        except NoKeysError:
            return []

        recipients = []
        for key in keys:
            entry = await self._kv.get(key)
            if entry.value is None:
                logger.warning("skipping empty service-directory entry %s", key)
                continue
            try:
                value = json.loads(entry.value)
                recipients.append(value["encryptionPublicKey"])
            except (json.JSONDecodeError, KeyError):
                logger.warning("skipping malformed service-directory entry %s", key)
        return recipients


class RecipientCache:
    """A live, watch-updated view of who's registered for one subject.
    Create one per subject a publisher actually publishes to; call
    `start()` once, then read `current()` before each encrypt — no GET per
    publish (Design.md §4.5)."""

    def __init__(self, kv: KeyValue, subject: str) -> None:
        self._kv = kv
        self._subject = subject
        self._entries: dict[str, str] = {}  # full KV key -> encryptionPublicKey
        self._task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()

    async def start(self) -> None:
        """Establishes the watch and blocks until the initial snapshot of
        existing recipients has been loaded (the first "caught up to live
        data" marker) — so `current()` is accurate as soon as `start()`
        returns, not just eventually."""
        watcher = await self._kv.watch(f"{self._subject}.*")
        self._task = asyncio.create_task(self._consume(watcher))
        await self._ready.wait()

    async def _consume(self, watcher: KeyValue.KeyWatcher) -> None:
        while True:
            try:
                # nats-py ships a py.typed marker, but KeyWatcher.updates
                # itself has no type annotations — confirmed via mypy
                # (a genuinely partial-typing gap, distinct from
                # nkeys/pyrage's "no types at all", which is why this
                # needs its own narrow ignore rather than a module-wide
                # ignore_missing_imports override).
                entry = await watcher.updates(  # type: ignore[no-untyped-call]
                    timeout=_WATCH_POLL_TIMEOUT_SECONDS
                )
            except nats.errors.TimeoutError:
                # No news within the poll window — normal, not an error.
                # Distinct from the `None` "caught up" marker below.
                continue

            if entry is None:
                self._ready.set()
                continue

            if entry.operation in (KV_DEL, KV_PURGE):
                self._entries.pop(entry.key, None)
                continue

            try:
                value = json.loads(entry.value)
                self._entries[entry.key] = value["encryptionPublicKey"]
            except (json.JSONDecodeError, KeyError):
                logger.warning("skipping malformed service-directory entry %s", entry.key)

    def current(self) -> list[str]:
        """The current recipient public keys, from the in-memory cache —
        no network call."""
        return list(self._entries.values())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

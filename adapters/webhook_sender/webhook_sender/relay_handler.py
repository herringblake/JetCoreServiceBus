"""Relay handler (Design.md §13 Step G3) — subscribes to one subject and
POSTs each decrypted payload to the configured target URL, best-effort,
single attempt, no retry (Decision #12). One instance per subject in
JETCORE_SUBJECTS (Step G4's entrypoint creates one per configured subject,
mirroring the File Storage Adapter's per-command-handler pattern, Design.md
§13 Step F5) — each subject gets its own durable consumer, per §6's "one
filtered consumer per adapter" design.

Publishes nothing back to the bus — the manifest's own description of
webhook-sender-01 already settles this ("No result event published").
Acks regardless of the HTTP outcome: nak/redelivery would itself be a
retry, contradicting "best-effort, single attempt."
"""

from __future__ import annotations

import logging

import httpx
from jetcore.bus_client import BusClient, ReceivedEvent

logger = logging.getLogger(__name__)


class RelayHandler:
    def __init__(
        self,
        client: BusClient,
        *,
        subject: str,
        target_url: str,
        outbound_secret: str | None,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._client = client
        self._subject = subject
        self._target_url = target_url
        self._outbound_secret = outbound_secret
        self._http_client = http_client

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(self._subject, durable_name=durable_name)

    async def run_once(self, durable_name: str, *, batch: int = 10, timeout: float = 5.0) -> int:
        events = await self._client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in events:
            await self._handle(event)
        return len(events)

    async def _handle(self, event: ReceivedEvent) -> None:
        headers = {
            "X-Event-Type": event.details.event_type,
            "X-Event-Id": event.details.event_id,
        }
        if self._outbound_secret is not None:
            headers["X-Webhook-Secret"] = self._outbound_secret

        try:
            response = await self._http_client.post(
                self._target_url, content=event.payload, headers=headers
            )
            response.raise_for_status()
        except httpx.HTTPError:
            # Best-effort per Decision #12 — logged, not retried. The
            # downstream outage this represents was an accepted risk from
            # the moment that decision was made (Design.md §8).
            logger.exception(
                "best-effort relay of event %s on %s to %s failed — not retrying (Decision #12)",
                event.details.event_id,
                self._subject,
                self._target_url,
            )

        # Ack either way — see module docstring.
        await event.ack()

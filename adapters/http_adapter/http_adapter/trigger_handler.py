"""Trigger handler (Design.md §13 Step H3) — subscribes to one subject
and calls the external REST API when triggered, publishing the response
back as a correlated events.http.RequestCompleted. One instance per
subject in JETCORE_SUBJECTS (Step H4's entrypoint creates one per
configured subject, mirroring the Webhook Sender's per-subject shape,
Steps G3/G4) — each subject gets its own durable consumer, per §6's
design.

Unlike the Webhook Sender's deliberate best-effort/no-retry choice
(Decision #12), a genuine connection failure here (the external API
unreachable, not just returning an error status) naks for redelivery —
this adapter's whole job is reporting what the external API actually
said, and a transient network blip shouldn't silently produce nothing to
report. A received response, whether 2xx or not, always gets a
RequestCompleted — reporting the outcome, not judging it (Design.md §13).
"""

from __future__ import annotations

import logging

import httpx
from jetcore.bus_client import BusClient, ReceivedEvent

from http_adapter.payloads import encode_request_completed

logger = logging.getLogger(__name__)

REQUEST_COMPLETED_SUBJECT = "events.http.RequestCompleted"


class TriggerHandler:
    def __init__(
        self,
        client: BusClient,
        *,
        subject: str,
        target_base_url: str,
        auth_token: str | None,
        http_client: httpx.AsyncClient,
    ) -> None:
        self._client = client
        self._subject = subject
        self._target_base_url = target_base_url
        self._auth_token = auth_token
        self._http_client = http_client

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(self._subject, durable_name=durable_name)

    async def run_once(self, durable_name: str, *, batch: int = 10, timeout: float = 5.0) -> int:
        events = await self._client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in events:
            await self._handle(event)
        return len(events)

    async def _handle(self, event: ReceivedEvent) -> None:
        headers = {}
        if self._auth_token is not None:
            headers["Authorization"] = f"Bearer {self._auth_token}"

        try:
            response = await self._http_client.post(
                self._target_base_url, content=event.payload, headers=headers
            )
        except httpx.RequestError:
            # Genuinely didn't get a response at all — distinct from
            # receiving one that happens to be an error status, handled
            # below. Worth a retry via redelivery; nothing meaningful to
            # report yet.
            logger.exception(
                "could not reach %s for event %s on %s — leaving unacked for redelivery",
                self._target_base_url,
                event.details.event_id,
                self._subject,
            )
            await event.nak()
            return

        status = "success" if response.is_success else "error"
        await self._client.publish(
            REQUEST_COMPLETED_SUBJECT,
            encode_request_completed(
                status=status, status_code=response.status_code, body=response.content
            ),
            event_type="RequestCompleted",
            correlation_id=event.details.event_id,
        )
        await event.ack()

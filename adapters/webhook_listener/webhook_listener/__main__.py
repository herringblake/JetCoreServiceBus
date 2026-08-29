"""Entrypoint for the Webhook Listener (Design.md §12 Step D4). Wires
settings -> the FastAPI app (app.py's own `lifespan` owns the real
BusClient's connect/close, Step D3) -> `uvicorn`.

Run via: python -m webhook_listener
"""

from __future__ import annotations

import logging

import uvicorn

from webhook_listener.app import create_app
from webhook_listener.settings import WebhookListenerSettings


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    settings = WebhookListenerSettings()  # pydantic-settings loads required fields from env vars
    app = create_app(settings)

    # uvicorn's own Server installs SIGTERM/SIGINT handlers and drains
    # in-flight requests before shutting down — unlike the File Storage
    # Adapter's raw asyncio loop (Step C6), no hand-rolled signal handling
    # needed here. Binding 0.0.0.0, not 127.0.0.1: this runs in a
    # container (Design.md §12 Track E) and needs to accept connections
    # from outside it — not a stray default.
    uvicorn.run(app, host="0.0.0.0", port=settings.http_port)  # nosec B104


if __name__ == "__main__":
    main()

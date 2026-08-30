"""Entrypoint for the REST API Service (Design.md §13 Step I6). Wires
settings -> the FastAPI app (app.py's own `lifespan` owns the real
BusClient's connect/subscribe/watcher-task/close, Step I5) -> `uvicorn`.
Mirrors webhook_listener/__main__.py's shape (Step D4) exactly — no
hand-rolled signal handling needed, `uvicorn.run()`'s own Server already
installs SIGTERM/SIGINT handlers and drains in-flight requests.

Run via: python -m rest_api_service
"""

from __future__ import annotations

import logging

import uvicorn

from rest_api_service.app import create_app
from rest_api_service.settings import RestApiServiceSettings


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    settings = RestApiServiceSettings()  # pydantic-settings loads required fields from env vars
    app = create_app(settings)

    # Binding 0.0.0.0, not 127.0.0.1: this runs in a container (Design.md
    # §13 Track K) and needs to accept connections from outside it — not
    # a stray default (same reasoning as webhook_listener/__main__.py).
    uvicorn.run(app, host="0.0.0.0", port=settings.http_port)  # nosec B104


if __name__ == "__main__":
    main()

# REST API Service — Design.md §13 Step K1. Mirrors
# adapters/webhook_sender/Dockerfile's structure and reasoning exactly —
# see that file's header comment for the minimal-copy-set finding. The
# one difference from the outbound-only adapters: this one serves real
# inbound HTTP (Track I), so it needs the port exposed, the same as
# webhook_listener/Dockerfile (Step E2).

FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./

COPY libs/jetcore/pyproject.toml libs/jetcore/pyproject.toml
COPY adapters/rest_api_service/pyproject.toml adapters/rest_api_service/pyproject.toml

COPY libs/jetcore/jetcore libs/jetcore/jetcore
COPY adapters/rest_api_service/rest_api_service adapters/rest_api_service/rest_api_service

RUN uv sync --frozen --no-dev --package jetcore-rest-api-service

RUN useradd --uid 1000 --create-home appuser
USER appuser

ENV PATH="/app/.venv/bin:${PATH}"

EXPOSE 8080

CMD ["python", "-m", "rest_api_service"]

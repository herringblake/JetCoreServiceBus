# HTTP Adapter — Design.md §13 Step K1. Mirrors
# adapters/webhook_sender/Dockerfile's structure and reasoning exactly —
# see that file's header comment for the minimal-copy-set finding.

FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./

COPY libs/jetcore/pyproject.toml libs/jetcore/pyproject.toml
COPY adapters/http_adapter/pyproject.toml adapters/http_adapter/pyproject.toml

COPY libs/jetcore/jetcore libs/jetcore/jetcore
COPY adapters/http_adapter/http_adapter adapters/http_adapter/http_adapter

RUN uv sync --frozen --no-dev --package jetcore-http-adapter

RUN useradd --uid 1000 --create-home appuser
USER appuser

ENV PATH="/app/.venv/bin:${PATH}"

CMD ["python", "-m", "http_adapter"]

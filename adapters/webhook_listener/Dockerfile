# Webhook Listener — Design.md §12 Step E2. Mirrors
# adapters/file_storage_adapter/Dockerfile's structure and reasoning
# exactly (build context = repo root; every workspace member's
# pyproject.toml needed for uv.lock validation, but only this adapter's
# and jetcore's real source).

FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./

COPY libs/jetcore/pyproject.toml libs/jetcore/pyproject.toml
COPY adapters/file_storage_adapter/pyproject.toml adapters/file_storage_adapter/pyproject.toml
COPY adapters/webhook_listener/pyproject.toml adapters/webhook_listener/pyproject.toml

COPY libs/jetcore/jetcore libs/jetcore/jetcore
COPY adapters/webhook_listener/webhook_listener adapters/webhook_listener/webhook_listener

RUN uv sync --frozen --no-dev --package jetcore-webhook-listener

# Not root — same reasoning as file_storage_adapter/Dockerfile, applied
# here too for defense-in-depth even though this adapter has no host
# bind mount of its own to accidentally root-own.
RUN useradd --uid 1000 --create-home appuser
USER appuser

ENV PATH="/app/.venv/bin:${PATH}"

EXPOSE 8080

CMD ["python", "-m", "webhook_listener"]

# Webhook Sender — Design.md §13 Step K1. Same build-context-is-repo-root
# reasoning as adapters/file_storage_adapter/Dockerfile (Step E2) and
# adapters/webhook_listener/Dockerfile — a uv workspace path-dependency
# (`jetcore`) needs the real libs/jetcore/ tree present to resolve.
#
# Copies only jetcore's + THIS adapter's own pyproject.toml/source, not
# every workspace member's — a real, empirically-confirmed correction to
# Step E2's original Dockerfiles' broader copy set (their own comments
# say "copying only jetcore + this adapter was NOT enough," back when the
# workspace had 3 members total). Tested directly at this step: with 8
# members now in the workspace, `uv sync --frozen --package X` only
# needs X's own dependency subgraph present, not every unrelated sibling
# adapter's pyproject.toml — confirmed by building this exact minimal set
# and watching it succeed, not assumed from the older comment.

FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./

COPY libs/jetcore/pyproject.toml libs/jetcore/pyproject.toml
COPY adapters/webhook_sender/pyproject.toml adapters/webhook_sender/pyproject.toml

COPY libs/jetcore/jetcore libs/jetcore/jetcore
COPY adapters/webhook_sender/webhook_sender adapters/webhook_sender/webhook_sender

RUN uv sync --frozen --no-dev --package jetcore-webhook-sender

# Not root — same defense-in-depth reasoning as every other adapter's
# Dockerfile; this adapter has no host bind mount of its own either.
RUN useradd --uid 1000 --create-home appuser
USER appuser

ENV PATH="/app/.venv/bin:${PATH}"

CMD ["python", "-m", "webhook_sender"]

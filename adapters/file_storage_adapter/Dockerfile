# File Storage Adapter — Design.md §12 Step E2. First real Dockerfile in
# the repo; build context is the REPO ROOT (see docker-compose.yml), not
# this directory — a uv workspace path-dependency (`jetcore`) needs the
# real libs/jetcore/ tree present to resolve, confirmed by testing, not
# assumed (an adapter-scoped build context fails resolution entirely).
#
# The lockfile (uv.lock) records every workspace member, not just the
# ones this image needs — `uv sync --frozen` validates the *whole*
# workspace's pyproject.toml set against it, so every member's
# pyproject.toml (but not its source) has to exist in the build context
# too, or the sync fails with a member-not-found error. Confirmed by
# testing: copying only jetcore + this adapter was NOT enough.

FROM python:3.12-slim AS base

# The official distroless uv image, not `pip install uv` — no extra
# package manager bootstrap needed, and it's a static binary copy.
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

WORKDIR /app

# Workspace-wide files uv.lock validation needs.
COPY pyproject.toml uv.lock .python-version ./

# Every workspace member's pyproject.toml (§12 note above) ...
COPY libs/jetcore/pyproject.toml libs/jetcore/pyproject.toml
COPY adapters/file_storage_adapter/pyproject.toml adapters/file_storage_adapter/pyproject.toml
COPY adapters/webhook_listener/pyproject.toml adapters/webhook_listener/pyproject.toml

# ... but only the real source this image actually runs.
COPY libs/jetcore/jetcore libs/jetcore/jetcore
COPY adapters/file_storage_adapter/file_storage_adapter adapters/file_storage_adapter/file_storage_adapter

# --frozen: fail if uv.lock is out of date, don't silently re-resolve in
# the image build. --no-dev: pytest/ruff/mypy/bandit have no business in
# a runtime image. --package: installs only this adapter + its transitive
# deps (jetcore included), not every workspace member.
RUN uv sync --frozen --no-dev --package file-storage-adapter

# Not root: this adapter writes to a host bind mount (watch_dir, Step
# E1) — confirmed by testing that files it creates land root-owned on
# the host otherwise, which the host user then can't even delete without
# sudo. UID 1000 matches the conventional first non-root Linux user
# (this host's own included) — not guaranteed on every host, but a
# reasonable default; override at `docker run`/Compose `user:` time if
# a deployment's bind-mount ownership needs differ.
RUN useradd --uid 1000 --create-home appuser
USER appuser

ENV PATH="/app/.venv/bin:${PATH}"

CMD ["python", "-m", "file_storage_adapter"]

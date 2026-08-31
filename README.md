# Jet Core Service Bus

A NATS JetStream-based service bus with public-key-encrypted payloads. See [Design.md](Design.md) for the full architecture and decision log, [Design_Notes.md](Design_Notes.md) for the original design brief, and [Dependencies.md](Dependencies.md) for the dependency ledger. This README covers day-to-day dev environment setup.

## Prerequisites

- **Docker** (with your user in the `docker` group — see [infra/nats/README.md](infra/nats/README.md) if `docker ps` gives a permission error). Everything NATS-related (server, `nsc`, `nats` CLI, `yq`) runs containerized — no native install of any of that.
- **[uv](https://docs.astral.sh/uv/)** — the project's Python packaging/dependency manager (Design.md Decision #16). Install (user-space, no sudo needed):
  ```bash
  curl -LsSf https://astral.sh/uv/0.12.5/install.sh | sh
  ```
  This adds `~/.local/bin` to `PATH` via your shell profile — restart your shell (or `source ~/.local/bin/env`) afterward. Pinned version tracked in [Dependencies.md](Dependencies.md).
- Python itself is provisioned by `uv` automatically (pinned to 3.12 via [.python-version](.python-version)) — no separate install needed.

## Setting up the Python workspace

```bash
uv sync --all-packages
```

**Use `--all-packages`, not plain `uv sync`.** This project's root `pyproject.toml` is a "virtual" workspace root (Design.md §7.5) — nothing in it depends on `jetcore` or the future adapter packages, so a plain `uv sync` silently resolves only the shared dev-tool group and reports success *without installing any workspace member at all*. This was confirmed by actually testing it, not assumed: `uv sync` alone left `jetcore` unimportable with no error or warning. `--all-packages` is the documented uv flag for "sync every workspace member," and is the only form that actually sets this project up correctly.

**If you rename or move the checkout directory, recreate `.venv`.** `.venv/bin/`'s console scripts (`pytest`, `ruff`, `mypy`, ...) hardcode the venv's *absolute* path in their shebang line at creation time — renaming the parent directory leaves them pointing at a path that no longer exists (`error: Failed to spawn: \`pytest\` — No such file or directory`), confirmed by actually hitting this after renaming this project's own directory. A plain `uv sync --all-packages` re-run does **not** fix already-installed scripts' shebangs (it only reinstalls what changed); `rm -rf .venv && uv sync --all-packages` does. Nothing else in this repo is affected by a rename — infra (`docker-compose.yml`, the bootstrap scripts) already pins its own names/paths independent of the checkout directory, by design.

## Running the checks

```bash
./test.sh                               # tests — see below, not a bare `uv run pytest`
uv run ruff check .                     # lint
uv run ruff format .                    # format
uv run --all-packages mypy libs adapters tests   # type-check
uv run bandit -r libs/jetcore/jetcore adapters/file_storage_adapter/file_storage_adapter adapters/webhook_listener/webhook_listener adapters/webhook_sender/webhook_sender adapters/http_adapter/http_adapter adapters/rest_api_service/rest_api_service adapters/db_adapter_mysql/db_adapter_mysql # security static analysis (source only — add each new adapter's package dir here as it's scaffolded)
uv run pip-audit                        # dependency vulnerability scan
```

**Use `./test.sh`, not a bare `uv run --all-packages pytest`, whenever the six adapter containers (`docker compose up -d`) might be running.** A test's own trigger/result subject can otherwise race a real, permanently-deployed adapter that's also listening on the exact same "placeholder" subject (Decision #14) — a real, confirmed defect, not a hypothetical; see [Defects.md#defect-3-real-adapters-react-to-shared-subject-test-traffic](Defects.md#defect-3-real-adapters-react-to-shared-subject-test-traffic). `test.sh` stops the six adapter application containers (`nats`/`mysql` stay up), runs pytest, and restarts them afterward regardless of outcome — the stack is left running normally either way. Arguments pass through to pytest, e.g. `./test.sh -k some_test`.

Or all at once via `pre-commit` (config already checked in, hooks call `uv run` directly rather than letting `pre-commit` provision its own duplicate environments):

```bash
uv run --all-packages pre-commit run --all-files
```

Note: `pre-commit run --all-files` only considers files git already tracks (staged or committed) — a brand-new untracked file won't be checked until it's at least `git add`ed.

To run these automatically on every commit: `uv run pre-commit install`. Not done by default — that's a per-developer choice.

## Repository layout

See [Design.md §7.1](Design.md#71-repository-layout) for the full intended layout. In short: `libs/jetcore/` is the shared library every adapter depends on; `adapters/` (populated starting Phase 2, Design.md §12) holds one package per adapter instance; `infra/` holds infrastructure config and bootstrap scripts (see [infra/nats/README.md](infra/nats/README.md) for the NATS setup, which is further along than the Python side as of this writing).

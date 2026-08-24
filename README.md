# Gregor's Service Bus

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

**Use `--all-packages`, not plain `uv sync`.** This project's root `pyproject.toml` is a "virtual" workspace root (Design.md §7.5) — nothing in it depends on `gsb-core` or the future adapter packages, so a plain `uv sync` silently resolves only the shared dev-tool group and reports success *without installing any workspace member at all*. This was confirmed by actually testing it, not assumed: `uv sync` alone left `gsb_core` unimportable with no error or warning. `--all-packages` is the documented uv flag for "sync every workspace member," and is the only form that actually sets this project up correctly.

## Running the checks

```bash
uv run --all-packages pytest            # tests
uv run ruff check .                     # lint
uv run ruff format .                    # format
uv run --all-packages mypy libs         # type-check (adapters/ added here once Phase 3 creates it)
uv run bandit -r libs/gsb-core/gsb_core # security static analysis
uv run pip-audit                        # dependency vulnerability scan
```

Or all at once via `pre-commit` (config already checked in, hooks call `uv run` directly rather than letting `pre-commit` provision its own duplicate environments):

```bash
uv run --all-packages pre-commit run --all-files
```

Note: `pre-commit run --all-files` only considers files git already tracks (staged or committed) — a brand-new untracked file won't be checked until it's at least `git add`ed.

To run these automatically on every commit: `uv run pre-commit install`. Not done by default — that's a per-developer choice.

## Repository layout

See [Design.md §7.1](Design.md#71-repository-layout) for the full intended layout. In short: `libs/gsb-core/` is the shared library every adapter depends on; `adapters/` (populated starting Phase 3) holds one package per adapter instance; `infra/` holds infrastructure config and bootstrap scripts (see [infra/nats/README.md](infra/nats/README.md) for the NATS setup, which is further along than the Python side as of this writing).

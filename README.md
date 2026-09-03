# Jet Core Service Bus

A NATS JetStream-based service bus with public-key-encrypted payloads. See [Design.md](Design.md) for the full architecture and decision log, [Design_Notes.md](Design_Notes.md) for the original design brief, and [Dependencies.md](Dependencies.md) for the dependency ledger. This README covers day-to-day dev environment setup.

## Whats all this about?

[Overview of how Claude is being utilized for this project.](./WhatsThisAllAboutAnyway.md)

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

## Running the demo

Design.md §15 (Phase 5). Everything below was actually run against a real, freshly-rebuilt stack (`docker compose down -v` first) to capture this output — not written from memory of what should happen.

This is a local demo environment, not a deployment guide: every outbound target URL below is an in-network placeholder standing in for a real integration that doesn't exist yet (Decision #14), and every credential in [docker-compose.yml](docker-compose.yml) is a checked-in, dev-only value flagged as such inline. Nothing here should be reused as-is for a real deployment.

### Bring the stack up

```bash
bash infra/nats/up.sh          # auth bootstrap + JetStream objects (Step A3/A5)
docker compose up -d --build   # nats + mysql + all 6 adapters
```

Give MySQL a few seconds to finish `init.sql` (creates the `orders` table) before triggering anything — `docker compose logs mysql` shows `ready for connections` twice; the second one is the one that matters (the first is MySQL's own internal bootstrap restart).

### The one request that touches every adapter

REST API Service's `POST /api/orders` publishes `events.orders.OrderCreated` — and every other adapter in the stack is already wired to react to it (see [docker-compose.yml](docker-compose.yml)'s own per-service comments, and [Design.md §8](Design.md#8-initial-adapters)):

```bash
curl -X POST 'http://localhost:8081/api/orders?wait=5' \
  -H 'Content-Type: application/json' \
  -d '{"orderId": "order-42", "item": "widget", "quantity": 3}'
```

`?wait=5` makes the call block up to 5s for a correlated `OrderPersisted` reply — real captured output:

```json
{"orderId": "order-42", "status": "persisted", "occurredAt": "2026-09-01T03:50:02.979583Z"}
```

That one request fans out to all five of these, independently and concurrently — no coordination between them beyond "same subject":

1. **Database Adapter's write path** upserts the row and is what produced the `OrderPersisted` reply above.
   ```
   $ docker compose exec mysql mysql -uroot -pdev-only-change-me jetcore \
       -e "SELECT order_id, item, quantity FROM orders WHERE order_id='order-42'"
   order_id   item     quantity
   order-42   widget   3
   ```
2. **Database Adapter's CDC path**, independently, tails the binlog for that same write and publishes `events.db.orders.RowChanged` — a *different* event, with no `correlationId`, since it isn't replying to anything (Design.md Decision #26). Real captured message (`eventPayload` is the encrypted ciphertext — see the note below):
   ```json
   {"event":{"eventDetails":{"eventId":"01a05b16-...","eventType":"RowChanged","sourceServiceId":"db-adapter-mysql-01","correlationId":null,...},"encryption":{"algorithm":"age-v1 (X25519 + XChaCha20-Poly1305)","recipients":["age1ef39p..."]},"eventPayload":"YWdlLWVuY3J5cHRpb24ub3JnL3Yx..."}}
   ```
3. **Webhook Sender** relays the same `OrderCreated` as an outbound webhook to Webhook Listener, which converts it into a `FileWriteRequested` command for **File Storage Adapter** to write:
   ```
   $ cat infra/files/orders/relayed.json
   {"orderId": "order-42", "item": "widget", "quantity": 3}
   ```
   (Every order relays to this same path — a second request overwrites it. Fine for a demo; not the pattern a real integration would use.)
4. **HTTP Adapter** also reacts to `OrderCreated` with its own outbound call — pointed at `/healthz` (a real, always-answering endpoint that only accepts `GET`), so every trigger produces a real, observable `405`, not a silent no-op:
   ```
   $ docker compose logs --tail=1 http-adapter
   INFO:httpx:HTTP Request: POST http://webhook-listener:8080/healthz "HTTP/1.1 405 Method Not Allowed"
   ```

### On the encrypted payload

Every event on the bus is encrypted for its intended recipients and signed by its sender (Design.md §4) — `RowChanged`'s `eventPayload` above genuinely is unreadable without the Database Adapter's own private key, by design, not an artifact of this demo. Inspect the *decrypted* effects instead — the MySQL row, the written file, the adapters' own logs — the way steps 1-4 above do.

### If you see a `DecryptError` in an adapter's logs

Restarting an adapter with a message still in flight for it is a known, deliberately-deferred gap, not a new bug: the *signing* identity is stable across restarts (derived from the adapter's `.creds` file), but the *encryption* keypair is regenerated fresh every process start — Design.md [§9 item #4](Design.md#9-open-questions-summary). A message encrypted for the pre-restart key becomes permanently undecryptable, and redelivers up to `MAX_DELIVER_ATTEMPTS` (5) times before giving up — bounded, self-resolving log noise, confirmed by watching it happen and stop on its own during this walkthrough's own verification.

## Repository layout

See [Design.md §7.1](Design.md#71-repository-layout) for the full intended layout. In short: `libs/jetcore/` is the shared library every adapter depends on; `adapters/` (populated starting Phase 2, Design.md §12) holds one package per adapter instance — each has its own developer-facing `README.md` ([webhook_listener](adapters/webhook_listener/README.md), [file_storage_adapter](adapters/file_storage_adapter/README.md), [webhook_sender](adapters/webhook_sender/README.md), [http_adapter](adapters/http_adapter/README.md), [rest_api_service](adapters/rest_api_service/README.md), [db_adapter_mysql](adapters/db_adapter_mysql/README.md)), and [adapters/BUILDING_AN_ADAPTER.md](adapters/BUILDING_AN_ADAPTER.md) walks through building and wiring in a new one; `infra/` holds infrastructure config and bootstrap scripts (see [infra/nats/README.md](infra/nats/README.md) for the NATS setup, which is further along than the Python side as of this writing).

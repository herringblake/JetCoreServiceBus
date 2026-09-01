# Building a New Adapter

A step-by-step recipe for adding a new adapter to Jet Core Service Bus, distilled from how the six adapters already in this directory were actually built (Design.md [§12](../Design.md#12-phase-2--detailed-breakdown)/[§13](../Design.md#13-phase-3--detailed-breakdown)). Every claim below is backed by real code in this repo — cross-references point at it rather than restating it, so it can't drift out of sync.

**Read first, if you haven't**: [Design.md §4](../Design.md#4-security-model) (envelope/crypto/identity model) and [§7](../Design.md#7-python-project) (project layout, `uv` workspace). This tutorial assumes you already know *why* the bus works the way it does; it's only about *how to build against it*.

## What "an adapter" is

A small, standalone Python package under `adapters/`, one per deployed instance (not one per adapter *type* — Design.md Decision #18 allows two independently-configured instances of the same code, each with its own identity), that:

1. Connects to NATS via `jetcore`'s `BusClient` under its own stable identity.
2. Does some combination of: subscribing to bus events and reacting to them, and/or publishing new events (from an HTTP call, a database change, a timer — whatever its integration point is).
3. Ships as its own Docker image and runs as its own `docker-compose` service.

Every existing adapter is one of two shapes — pick whichever fits:

- **Bus reactor** — no HTTP surface, subscribes to subject(s), does something, optionally publishes a reply. [http_adapter](http_adapter/) and [webhook_sender](webhook_sender/) are the cleanest examples; [file_storage_adapter](file_storage_adapter/) is the same shape with four handlers running concurrently instead of one; [db_adapter_mysql](db_adapter_mysql/) adds a second, independent reactor (its CDC path) alongside the first.
- **HTTP front door** — a FastAPI app that publishes on each inbound request, optionally waiting on a correlated reply. [webhook_listener](webhook_listener/) is the fire-and-forget version; [rest_api_service](rest_api_service/) adds the synchronous-reply pattern on top.

## The recipe

Each numbered step names the real file(s) that step produces, using `http_adapter` as the running example — the simplest complete bus-reactor adapter with a published reply, so every piece below has a real, short file to point at.

### 1. Scaffold the package

```
adapters/<name>/
  pyproject.toml
  Dockerfile
  <name>/
    __init__.py
    settings.py
    __main__.py
    ... your handler module(s), payloads.py
  tests/
    conftest.py
    ...
```

`pyproject.toml` — copy [http_adapter/pyproject.toml](http_adapter/pyproject.toml) as a starting point. Two things matter:

- `[project] name = "jetcore-<name>"`, **not** bare `<name>` — Decision #28: `http-adapter`, `webhook-listener`, and `webhook-sender` are all real, unrelated packages already published on PyPI, found the hard way (Design.md §13 Step G5). Every adapter's distribution name gets the `jetcore-` prefix now, even ones whose bare name happens to be free — consistency beats checking each one. The Python import name (`<name>`, e.g. `http_adapter`) is unaffected.
- `[tool.uv.sources] <name> = { workspace = true }` for `jetcore` itself, and any other in-repo dependency.

**Nothing to edit at the workspace root** — [pyproject.toml](../pyproject.toml)'s `[tool.uv.workspace] members = ["libs/*", "adapters/*"]` is a glob; a new `adapters/<name>/pyproject.toml` is picked up automatically by `uv sync --all-packages`.

### 2. `settings.py` — configuration

Subclass `jetcore.config.AdapterSettings` and add only what's new. Every field is `JETCORE_`-prefixed automatically (`pydantic-settings`, `env_prefix="JETCORE_"`) — see [libs/jetcore/jetcore/config.py](../libs/jetcore/jetcore/config.py) for the full inherited baseline (`service_id`, `nats_url`, `nats_creds_path`, `subjects`, `log_level`) before adding a field that might already exist there.

```python
# adapters/http_adapter/http_adapter/settings.py — real, unedited
from jetcore.config import AdapterSettings
from pydantic import SecretStr

class HttpAdapterSettings(AdapterSettings):
    target_base_url: str
    auth_token: SecretStr | None = None
```

- A field with no default is **required** — missing it fails fast at startup with a clear `pydantic` error, not a confusing failure three calls later.
- Use `pydantic`'s own validating types where they fit for free: `FilePath`/`DirectoryPath` (must exist on disk), `SecretStr` (never accidentally logged/repr'd).
- Only use the inherited `subjects` field if your adapter's subscribe list is genuinely meant to be runtime-configurable (like `http_adapter`/`webhook_sender`, which relay whatever `JETCORE_SUBJECTS` names). If your subject is fixed by what the adapter *does* (like File Storage Adapter's four command subjects), hardcode it as a module constant instead — don't make configurable what isn't meant to change without also changing code.

### 3. `payloads.py` — the wire format

One module, encode/decode functions only — no bus logic, no I/O. This is the single place your event payloads' shapes live, so schemas, handlers, and tests can't independently drift from each other. See [http_adapter/payloads.py](http_adapter/http_adapter/payloads.py) for the minimal version (one `encode_request_completed`) or [file_storage_adapter/payloads.py](file_storage_adapter/file_storage_adapter/payloads.py) for a fuller one (multiple request/response shapes, a shared `MalformedPayloadError`).

`BusClient` handles the envelope (encryption, signing, `eventId`/`correlationId`) — this module only knows about the plain JSON bytes *inside* that envelope.

### 4. The handler class

One class per thing-you-react-to, each with the same three-method shape every existing handler uses — copy [http_adapter/trigger_handler.py](http_adapter/http_adapter/trigger_handler.py):

```python
class SomeHandler:
    def __init__(self, client: BusClient, *, subject: str, ...) -> None: ...

    async def start(self, *, durable_name: str | None = None) -> str:
        return await self._client.subscribe(self._subject, durable_name=durable_name)

    async def run_once(self, durable_name: str, *, batch: int = 10, timeout: float = 5.0) -> int:
        events = await self._client.fetch(durable_name, batch=batch, timeout=timeout)
        for event in events:
            await self._handle(event)
        return len(events)

    async def _handle(self, event: ReceivedEvent) -> None:
        ...  # your logic; ack() or nak() at the end, always exactly one
```

`start`/`run_once` split out from an infinite loop on purpose — it lets both a test and the real entrypoint drive one fetch cycle directly, without needing OS signals or a subprocess to prove the logic works.

**Ack/nak — pick deliberately, not by default.** Two failure classes, two different responses, matching every existing handler:

- **Deterministically bad** (malformed payload, validation failure, target not found) — log it, **ack** (stop redelivery; retrying would never fix it). Optionally publish a `*Failed`/error reply if your adapter has a result subject a caller can act on.
- **Plausibly transient** (a real I/O error, a database hiccup, target genuinely unreachable) — log it, **nak** (leave unacked for redelivery). JetStream already caps redelivery at 5 attempts (`MAX_DELIVER_ATTEMPTS` in [bus_client.py](../libs/jetcore/jetcore/bus_client.py)) — you don't need your own retry loop on top; see [db_adapter_mysql/write_handler.py](db_adapter_mysql/db_adapter_mysql/write_handler.py)'s docstring for why stacking `tenacity` here was deliberately rejected.

If your adapter should retry on failure vs. drop-and-report is itself a design decision — [webhook_sender](webhook_sender/README.md) acks even on total failure (best-effort, single-attempt, Decision #12); [http_adapter](http_adapter/README.md) naks on the equivalent failure (its whole job is reporting what really happened). Decide and document it, the way both of those did.

### 5. `__main__.py` — the entrypoint

Copy [http_adapter/__main__.py](http_adapter/http_adapter/__main__.py). Every entrypoint in this project has the same shape:

```python
async def run(client: BusClient, *, ..., shutdown: asyncio.Event) -> None:
    """The actual running logic, split out from main() so a test can
    drive this real wiring directly."""
    tasks = [asyncio.create_task(_wait_for_shutdown(shutdown))]
    for subject in subjects:
        handler = SomeHandler(client, subject=subject, ...)
        durable_name = f"{service_id}-<handler-kind>-{subject.replace('.', '_')}"
        await handler.start(durable_name=durable_name)
        tasks.append(asyncio.create_task(_run_loop(handler, durable_name, shutdown)))
    await asyncio.gather(*tasks)

async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = YourSettings()
    client = await BusClient.connect_as_adapter(settings, adapter_type=ADAPTER_TYPE)
    shutdown = asyncio.Event()
    # SIGTERM/SIGINT -> shutdown.set(), same in every adapter
    try:
        await run(client, ..., shutdown=shutdown)
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

`run()` split from `main()` matters: it's what lets `test_*_entrypoint.py` drive the *real* wiring (real `BusClient`, real handlers) against live NATS, injecting only a test-controlled `shutdown` event — proving the actual production code path works, not a stand-in. `ADAPTER_TYPE` is a free-text label (used for observability/registration only, distinct from `service_id`) — match your `adapterType` in the manifest (step 7).

Durable consumer names are deterministic (`f"{service_id}-..."`), not random — a real deployment restarts the same consumer across process restarts and expects to resume its own cursor; only tests want a fresh one every run.

**HTTP front door instead?** Skip the `run()`/`asyncio.gather` shape — see [webhook_listener/app.py](webhook_listener/webhook_listener/app.py) (`create_app()`, connect inside FastAPI's `lifespan`) and, for the sync-reply pattern on top of it, [rest_api_service/app.py](rest_api_service/rest_api_service/app.py) + [rest_api_service/pending_replies.py](rest_api_service/rest_api_service/pending_replies.py). `create_app(settings, *, bus_client=None)` accepting an optional pre-connected client is the pattern that lets tests inject a real test-identity client without going through the production `connect_as_adapter()` path — keep it if you build one of these.

### 6. Add the event schema(s)

For every **new** event type your adapter publishes (not ones that already exist), add `schemas/events.<context>.<EventType>.v1.json` — JSON Schema, Draft 2020-12, `additionalProperties: false`. Copy the shape of an existing one, e.g. [schemas/events.http.RequestCompleted.v1.json](../schemas/events.http.RequestCompleted.v1.json). Add a matching test in your `tests/` directory asserting `Draft202012Validator.check_schema(...)` passes and a real example payload validates — see [adapters/db_adapter_mysql/tests/test_db_adapter_mysql_schemas.py](db_adapter_mysql/tests/test_db_adapter_mysql_schemas.py) for the pattern, including a rejects-bad-input case.

### 7. Register the identity

Add an entry to [infra/nats/adapter_identities.yaml](../infra/nats/adapter_identities.yaml) — this is the actual source of truth for what your adapter is allowed to publish/subscribe on the bus:

```yaml
  - serviceId: <name>-01
    adapterType: <name>
    description: >
      One or two sentences: what this adapter does, in the same voice
      every other entry already uses.
    permissions:
      publish:
        - events.<context>.<YourResultEvent>
      subscribe:
        - events.<context>.<TriggerEvent>
```

Don't hand-derive the KV/JetStream-API permissions this implies — `bootstrap_auth.sh` computes those from `publish`/`subscribe` automatically (see the manifest's own header comment for exactly what's derived).

**If you'll have entrypoint/live tests** (you should) **, also add a `<name>-01-test` twin** — identical permissions, different `serviceId` — under the manifest's existing "Test/CI tooling identities" section. This isn't optional polish: a test driving your real entrypoint under the *real* `serviceId` while a real deployed instance of the same identity might also be running collides — [Defects.md Defect 1](../Defects.md#defect-1-testproduction-identity-collision) is the full story of what goes wrong and why every other adapter's tests connect as `<name>-01-test`, never bare `<name>-01`.

Apply the manifest with `bash infra/nats/up.sh` (or just `infra/nats/bootstrap_auth.sh` if NATS is already up) — no server restart needed; this generates the new `.creds` file(s) under `infra/nats/operator/creds/`.

### 8. Dockerfile

Copy [http_adapter/Dockerfile](http_adapter/Dockerfile) and change the package name/path throughout — it's a minimal, deliberately narrow copy set (only `pyproject.toml`+`uv.lock`+`.python-version` at repo root, `libs/jetcore/`, and this one adapter's own directory — not the whole repo) so an image rebuild only invalidates Docker's layer cache when something it actually depends on changed.

### 9. Wire it into `docker-compose.yml`

One service block in [docker-compose.yml](../docker-compose.yml), following an existing adapter's shape:

```yaml
  <name>:
    build:
      context: .
      dockerfile: adapters/<name>/Dockerfile
    depends_on:
      - nats          # + mysql, if you need it
    environment:
      JETCORE_SERVICE_ID: <name>-01
      JETCORE_NATS_URL: nats://nats:4222
      JETCORE_NATS_CREDS_PATH: /creds/<name>-01.creds
      # ... your adapter-specific env vars
    volumes:
      - ./infra/nats/operator/creds/<name>-01.creds:/creds/<name>-01.creds:ro
    restart: unless-stopped
```

Add `ports:` only if it's an HTTP front door — pick a host port not already claimed (check the existing services first: `8080` and `8081` are taken).

If your target is an external integration that doesn't exist yet, point it at something *inside* this stack that gives a real, observable response (the way `http_adapter` points at `webhook-listener`'s `/healthz`) — Decision #14's reasoning: a target this stack can't actually reach either needs real internet access it shouldn't depend on, or fails forever with nothing to observe.

### 10. Tests

`adapters/<name>/tests/conftest.py` — copy an existing one (e.g. [http_adapter/tests/conftest.py](http_adapter/tests/conftest.py)) for the `_is_test_identity()`-scoped `_clean_state` fixture. **Don't write a fixture that blanket-deletes every `service-directory` entry for a shared subject** — [Defects.md Defect 2](../Defects.md#defect-2-ambient-test-traffic-undecryptable-by-real-containers) is what happens when real, permanently-deployed adapters share that subject too.

Test against real NATS, never mocked — every existing test suite in this project does, and it's what actually caught Defects 1-4. Cover your handler(s) directly (construct one, call `start()`/`run_once()`, assert on real published output) and your entrypoint end to end (drive the real `run()` from step 5 against a real, test-identity `BusClient`).

Run just your new suite while developing: `uv run --all-packages pytest adapters/<name>`. Before considering it done, run the *whole* suite via [test.sh](../test.sh) (`./test.sh`, not a bare `pytest` — see the [repo root README](../README.md#running-the-checks) for why) with the full adapter fleet up, to catch anything that only shows up at full-stack scale.

### 11. Wire it into project-wide tooling

Three places list every adapter's source directory explicitly — a new one won't be covered until you add it:

- [README.md](../README.md#running-the-checks)'s `bandit` command
- [.github/workflows/ci.yml](../.github/workflows/ci.yml)'s `security` job's `bandit` step
- [test.sh](../test.sh)'s `ADAPTER_SERVICES` array — **only if** your adapter runs as a normal always-up `docker-compose` service (it should, for anything built following this tutorial); this is what lets `test.sh` stop it before running tests and restart it after, the same as every other adapter (Defects.md Defect 3).

**Nothing to do** for `.pre-commit-config.yaml`'s `mypy` hook — its `entry: uv run --all-packages mypy libs adapters tests` already covers every directory under `adapters/`, present and future, with no per-adapter edit needed.

### 12. Write this adapter's own README

One `adapters/<name>/README.md`, developer-facing: what it does, its full configuration table, what it publishes/subscribes (with payload shapes), any notable behavior/error-handling posture, how to run it standalone, how to test it. The six existing ones (linked at the top of each) are the template — match their structure and level of detail rather than inventing a new shape.

### 13. Verify, for real

```bash
uv sync --all-packages                                    # picks up the new package automatically
uv run ruff check . && uv run ruff format --check .
uv run --all-packages mypy libs adapters tests
uv run bandit -r <every adapter dir, including your new one>
uv run --all-packages pytest adapters/<name>
docker compose up -d --build <name>                        # + its dependencies
# trigger it for real, confirm the real output — curl, a bus publish via
# nats CLI, whatever fits — the same standard every step above was built to
./test.sh                                                  # full suite, full fleet
```

Nothing in this project gets marked done on the strength of "the code looks right" — every adapter here was built by actually running it against real NATS (and MySQL, where relevant) and confirming real output, not by inspection alone. Hold your new one to the same bar.

## Reference: `BusClient`'s API surface

Everything a handler needs, from [libs/jetcore/jetcore/bus_client.py](../libs/jetcore/jetcore/bus_client.py) — full details in each method's own docstring there.

| Method | Use |
|---|---|
| `BusClient.connect_as_adapter(settings, *, adapter_type)` | The entrypoint's own connection — stable signing identity derived from the `.creds` file, fresh encryption keypair per process start (Design.md [§9](../Design.md#9-open-questions-summary) item #4 — a known, deliberately-deferred gap: don't design around persistence this doesn't have). |
| `client.subscribe(subject, *, durable_name=None) -> str` | Registers as a recipient for `subject` (starts its heartbeat) and creates a durable, filtered pull consumer. Idempotent. Returns the durable name actually used. |
| `client.fetch(durable_name, *, batch=1, timeout=5.0) -> list[ReceivedEvent]` | Pulls up to `batch` already-decrypted, signature-verified messages; empty list on timeout, never raises for "nothing available." |
| `client.publish(subject, payload, *, event_type, correlation_id=None) -> str` | Encrypts for every currently-registered recipient of `subject`, signs, publishes, returns the generated `eventId`. |
| `event.ack()` / `event.nak()` | Exactly one, always, per `ReceivedEvent` — see step 4's ack/nak guidance. |
| `client.close()` | Cancels background heartbeat tasks, stops recipient-cache watches, closes the NATS connection. Always in a `finally`. |

## Checklist

- [ ] `adapters/<name>/pyproject.toml` (`jetcore-<name>` distribution name)
- [ ] `settings.py` subclassing `AdapterSettings`
- [ ] `payloads.py`
- [ ] handler class(es) — `start`/`run_once`/`_handle`, deliberate ack/nak
- [ ] `__main__.py` — `run()` split from `main()`
- [ ] `schemas/events.<context>.<Type>.v1.json` for every new event type, plus a schema test
- [ ] `infra/nats/adapter_identities.yaml` entry (+ a `-test` twin) — applied via `infra/nats/up.sh`
- [ ] `Dockerfile`
- [ ] `docker-compose.yml` service block
- [ ] `adapters/<name>/tests/` — `conftest.py` with `_is_test_identity()`-scoped cleanup, handler tests, entrypoint test
- [ ] `README.md`'s `bandit` command, `.github/workflows/ci.yml`'s `bandit` step, `test.sh`'s `ADAPTER_SERVICES`
- [ ] `adapters/<name>/README.md`
- [ ] `ruff`/`mypy`/`bandit`/`pytest` all clean; a real end-to-end trigger observed, not assumed

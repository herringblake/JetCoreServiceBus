# File Storage Adapter

A command-driven file operations service (Design.md [§8](../../Design.md#8-initial-adapters), [§12](../../Design.md#12-phase-2--detailed-breakdown) Track C, [§13](../../Design.md#13-phase-3--detailed-breakdown) Track F). Not a passive archival sink — other services ask it to `write`/`read`/`list`/`delete` files under one root directory by publishing a command event; it does the I/O and publishes a result event back.

It also does one thing nobody has to ask for: if a file shows up in its watched directory with no preceding command (something else wrote directly into a shared/mounted directory), it notices and reports that too.

## Configuration

All `JETCORE_`-prefixed env vars, loaded via `pydantic-settings` (fails fast at startup on anything missing/invalid — see [jetcore/config.py](../../libs/jetcore/jetcore/config.py) for the full shared baseline every adapter has).

| Var | Required | Default | Meaning |
|---|---|---|---|
| `JETCORE_SERVICE_ID` | yes | — | Must match this instance's `serviceId` in [infra/nats/adapter_identities.yaml](../../infra/nats/adapter_identities.yaml) (`file-storage-01` in the real deployment). |
| `JETCORE_NATS_URL` | no | `nats://nats:4222` | |
| `JETCORE_NATS_CREDS_PATH` | yes | — | The `.creds` file `bootstrap_auth.sh` generated for this `service_id`. Must exist at startup — `pydantic`'s `FilePath` validation, not a runtime surprise. |
| `JETCORE_WATCH_DIR` | yes | — | The root directory this adapter reads/writes/watches. Must exist and be a directory at startup (`pydantic`'s `DirectoryPath`). Every command's `path` is resolved *relative to this root* and validated to stay inside it — see "Path safety" below. |
| `JETCORE_LOG_LEVEL` | no | `INFO` | |

`JETCORE_SUBJECTS` (the shared baseline's own field) is **not used** by this adapter — its subscribe subjects are fixed in code (the four command subjects below), not configurable per instance.

## What it subscribes to / publishes

Four independent command handlers run concurrently, each with its own durable consumer — a message on one subject never blocks the others.

| Command (subscribe) | Success (publish) | Failure (publish) |
|---|---|---|
| `events.files.FileWriteRequested` | `events.files.FileCreateCompleted` (file was new) **or** `events.files.FileWriteCompleted` (file already existed) | — (see "Error handling" below; a bad request is acked and dropped, not reported) |
| `events.files.FileReadRequested` | `events.files.FileReadCompleted` | `events.files.FileOperationFailed` (`reason: "not_found"`) |
| `events.files.FileListRequested` | `events.files.FileListCompleted` | `events.files.FileOperationFailed` (`reason: "not_found"` or `"not_a_directory"`) |
| `events.files.FileDeleteRequested` | `events.files.FileDeleteCompleted` | `events.files.FileOperationFailed` (`reason: "not_found"`) |

Every success/failure reply carries `correlationId` set to the triggering command's `eventId`.

**One more publish, independent of any command**: `events.files.FileCreateCompleted` also fires when a file appears in `JETCORE_WATCH_DIR` with **no** preceding `FileWriteRequested` — e.g. another process writing directly into a shared/mounted directory. That publish has no `correlationId`, since there was no request to correlate against. This is creation-detection only; an externally-triggered *update* to an already-existing file is **not** detected (open question, Design.md [§9](../../Design.md#9-open-questions-summary) item #3).

## Payload shapes

Matches [schemas/events.files.*.v1.json](../../schemas/) exactly — see [payloads.py](file_storage_adapter/payloads.py) for the one place these are built/parsed.

- `FileWriteRequested`: `{"path": "<string>", "content": "<base64>"}`
- `FileReadRequested` / `FileDeleteRequested`: `{"path": "<string>"}` (must be non-empty)
- `FileListRequested`: `{"path": "<string>"}` (empty string means "list `JETCORE_WATCH_DIR` itself")
- `*Completed` replies carry `path`, plus operation-specific fields (`sizeBytes`, `content`, `entries: [{name, isDirectory, sizeBytes}]`), plus `occurredAt`.

## Path safety

Every `path` is resolved against `JETCORE_WATCH_DIR` and rejected (silently, from the caller's perspective — see below) if it would resolve outside that root — no `../` escapes, no absolute-path overrides. See [paths.py](file_storage_adapter/paths.py)'s `resolve_within`.

**Known v1 gap, not fixed**: this check happens at request time, not atomically with the write itself — a TOCTOU window exists if a path component is replaced with a symlink in between (would need `O_NOFOLLOW`/`openat2 RESOLVE_BENEATH`-style primitives to close). Acceptable for a local, single-writer dev/demo deployment; not hardened for a multi-tenant filesystem sandbox.

## Error handling

Two different failure modes get two different responses, on purpose:

- **Deterministically bad** (malformed payload, path escapes the watch dir, target not found) — logged, **acked** (stop redelivery; retrying wouldn't help), and for read/list/delete a `FileOperationFailed` is published so the caller learns why. A malformed `FileWriteRequested` is the one exception: acked and silently dropped, no failure event — there's no well-formed request to report against.
- **Plausibly transient** (a real I/O error — disk full, permission hiccup) — logged, left **unacked** for JetStream redelivery, capped at 5 attempts (`MAX_DELIVER_ATTEMPTS`, [Defects.md Defect 2](../../Defects.md#defect-2-ambient-test-traffic-undecryptable-by-real-containers)).

## Running it standalone

```bash
JETCORE_SERVICE_ID=file-storage-01 \
JETCORE_NATS_CREDS_PATH=infra/nats/operator/creds/file-storage-01.creds \
JETCORE_WATCH_DIR=infra/files \
uv run python -m file_storage_adapter
```

Requires NATS already up (`infra/nats/up.sh`) and a `.creds` file for this identity already generated (`bootstrap_auth.sh`, run as part of `up.sh`). In the normal dev stack this runs as the `file-storage-adapter` service in [docker-compose.yml](../../docker-compose.yml) instead.

## Testing

`uv run --all-packages pytest adapters/file_storage_adapter` (or `./test.sh -k file_storage_adapter` if the adapter fleet is up — see the [repo root README](../../README.md#running-the-checks)). Each handler has its own test module (`test_write_handler.py`, `test_read_handler.py`, `test_list_handler.py`, `test_delete_handler.py`, `test_create_watch.py`) plus `test_entrypoint.py` driving the real wiring end to end — all against a real NATS instance, never mocked.

## See also

- [Design.md §8](../../Design.md#8-initial-adapters) — this adapter's role among all six.
- [Design.md §12 Track C](../../Design.md#12-phase-2--detailed-breakdown) / [§13 Track F](../../Design.md#13-phase-3--detailed-breakdown) — the step-by-step build history.
- [../BUILDING_AN_ADAPTER.md](../BUILDING_AN_ADAPTER.md) — how this adapter (and any new one) is put together.

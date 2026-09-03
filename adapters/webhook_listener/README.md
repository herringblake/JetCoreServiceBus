# Webhook Listener

An inbound HTTP webhook receiver (Design.md [§8](../../Design.md#8-initial-adapters), [§12](../../Design.md#12-phase-2--detailed-breakdown) Track D). External → Bus: exposes one HTTP endpoint, converts whatever it receives into a `FileWriteRequested` command, and publishes it. Fire-and-forget from the caller's perspective — no reply subject, `202 Accepted` means "published," not "processed."

In the demo stack it's also the receiving end of Webhook Sender's own outbound relay (see [../webhook_sender/README.md](../webhook_sender/README.md)) — the only two adapters here that talk to each other directly over HTTP rather than only through the bus.

## Configuration

| Var | Required | Default | Meaning |
|---|---|---|---|
| `JETCORE_SERVICE_ID` | yes | — | `webhook-listener-01` in the real deployment. |
| `JETCORE_NATS_URL` | no | `nats://nats:4222` | |
| `JETCORE_NATS_CREDS_PATH` | yes | — | |
| `JETCORE_WEBHOOK_SECRET` | yes | — | Shared secret an inbound request's `X-Webhook-Secret` header must match (constant-time comparison). A generic placeholder mechanism — no real webhook source is integrated yet, so there's no source-specific scheme (HMAC signature header, etc.) to build against. |
| `JETCORE_HTTP_PORT` | no | `8080` | Port the ASGI app binds. |
| `JETCORE_LOG_LEVEL` | no | `INFO` | |

## HTTP surface

### `POST /webhooks/{path}`

Body: whatever bytes the caller sends (not assumed to be JSON). Header: `X-Webhook-Secret` must match `JETCORE_WEBHOOK_SECRET`.

```bash
curl -X POST http://localhost:8080/webhooks/some/nested/path.json \
  -H 'X-Webhook-Secret: dev-only-change-me' \
  -d '{"hello": "world"}'
# 202 Accepted, empty body
```

Publishes `events.files.FileWriteRequested` with `{"path": "some/nested/path.json", "content": "<base64 of the raw body>"}` — the URL path segment becomes the target file path (resolved relative to the File Storage Adapter's own `JETCORE_WATCH_DIR`; path-traversal validation happens over there, not here — see [../file_storage_adapter/README.md](../file_storage_adapter/README.md)).

- Missing/wrong secret → `401`.
- Empty path (`POST /webhooks/`) → `400`.
- Otherwise → `202`, published fire-and-forget. This adapter never learns whether the write actually succeeded.

### `GET /healthz`

`{"status": "ok"}`, `200`. Exists because not every image in this stack ships a shell/HTTP client a Compose-native healthcheck could use — a Python-native ASGI app can trivially serve its own instead.

## What it publishes

| Subject | When |
|---|---|
| `events.files.FileWriteRequested` | Every accepted `POST /webhooks/{path}` |

Subscribes to nothing.

## Running it standalone

```bash
JETCORE_SERVICE_ID=webhook-listener-01 \
JETCORE_NATS_CREDS_PATH=infra/nats/operator/creds/webhook-listener-01.creds \
JETCORE_WEBHOOK_SECRET=dev-only-change-me \
uv run python -m webhook_listener
```

Requires NATS already up (`infra/nats/up.sh`). In the normal dev stack this runs as the `webhook-listener` service in [docker-compose.yml](../../docker-compose.yml), on host port `8080`.

## Testing

`uv run --all-packages pytest adapters/webhook_listener` (or via `./test.sh` — see [repo root README](../../README.md#running-the-checks)). `test_webhook_listener_entrypoint.py` drives the real FastAPI app + a real `BusClient` end to end against live NATS.

## See also

- [Design.md §8](../../Design.md#8-initial-adapters) — this adapter's role among all six.
- [Design.md §12 Track D](../../Design.md#12-phase-2--detailed-breakdown) — the step-by-step build history (the first vertical slice, alongside File Storage Adapter).
- [README.md § Running the demo](../../README.md#running-the-demo) — this adapter as the relay target in the full-mesh walkthrough.
- [../BUILDING_AN_ADAPTER.md](../BUILDING_AN_ADAPTER.md) — how this adapter (and any new one) is put together.

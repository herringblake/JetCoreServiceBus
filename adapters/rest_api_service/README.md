# REST API Service

External → Bus, with an optional synchronous reply (Design.md [§8](../../Design.md#8-initial-adapters), [§13](../../Design.md#13-phase-3--detailed-breakdown) Track I). The "front door" for external HTTP clients — exposes a REST API, translates each call into a bus event, and can optionally block the HTTP response until a correlated reply event arrives.

## Configuration

| Var | Required | Default | Meaning |
|---|---|---|---|
| `JETCORE_SERVICE_ID` | yes | — | `rest-api-service-01` in the real deployment. |
| `JETCORE_NATS_URL` | no | `nats://nats:4222` | |
| `JETCORE_NATS_CREDS_PATH` | yes | — | |
| `JETCORE_HTTP_PORT` | no | `8080` | Port the ASGI app binds. |
| `JETCORE_DEFAULT_REPLY_TIMEOUT_SECONDS` | no | `30.0` | Hard cap on a `?wait=<seconds>` request — a caller-supplied `wait` above this is silently capped, not rejected. |
| `JETCORE_LOG_LEVEL` | no | `INFO` | |

## HTTP surface

### `POST /api/orders?wait=<seconds>`

Body: passed through verbatim as the published payload (Decision #14's placeholder bounded context — no real order schema exists yet, so this is a thin HTTP-to-command translator, the same posture Webhook Listener already established). Publishes `events.orders.OrderCreated`.

- **No `?wait=` (or `?wait=0`)** — fire-and-forget, matching Webhook Listener's own default:
  ```bash
  curl -X POST http://localhost:8081/api/orders -d '{"orderId": "order-1", "item": "widget", "quantity": 3}'
  # 202 Accepted
  # {"eventId": "01a0..."}
  ```
- **`?wait=<seconds>`, capped at `JETCORE_DEFAULT_REPLY_TIMEOUT_SECONDS`** — registers a pending reply keyed on the just-published event's own `eventId`, and blocks for a correlated `events.orders.OrderPersisted` (published by [Database Adapter](../db_adapter_mysql/README.md)'s write path):
  ```bash
  curl -X POST 'http://localhost:8081/api/orders?wait=5' -d '{"orderId": "order-1", "item": "widget", "quantity": 3}'
  # 200 OK
  # {"orderId": "order-1", "status": "persisted", "occurredAt": "2026-09-01T03:46:58.229060Z"}
  ```
  A timeout (nothing correlated arrived in time) returns `504` with `{"eventId": "...", "error": "timeout waiting for reply"}` — the pending registration is unregistered either way, so a late reply after timeout doesn't leak a resolved-but-unread future.

Real captured output for both forms — see [README.md § Running the demo](../../README.md#running-the-demo).

### `GET /healthz`

`{"status": "ok"}`, `200`.

## What it publishes / subscribes

| Subject | Direction |
|---|---|
| `events.orders.OrderCreated` | publish — every `POST /api/orders` |
| `events.orders.OrderPersisted` | subscribe — background watcher, resolves any pending `?wait=` future whose `correlationId` matches |

The `OrderPersisted` subscription runs as its own background task for the whole life of the process (started in the app's `lifespan`, not per-request) — a reply can arrive and get matched to its pending future even between requests.

## Running it standalone

```bash
JETCORE_SERVICE_ID=rest-api-service-01 \
JETCORE_NATS_CREDS_PATH=infra/nats/operator/creds/rest-api-service-01.creds \
uv run python -m rest_api_service
```

Requires NATS already up, and — for `?wait=` to ever actually resolve — something publishing a correlated `OrderPersisted` (in the real stack, [Database Adapter](../db_adapter_mysql/README.md)'s write path). In the normal dev stack this runs as the `rest-api-service` service in [docker-compose.yml](../../docker-compose.yml), on host port `8081` (`8080` was already taken by Webhook Listener).

## Testing

`uv run --all-packages pytest adapters/rest_api_service` (or via `./test.sh`). `test_rest_api_service_app.py` covers both the fire-and-forget and sync-reply paths against live NATS, including the timeout case.

## See also

- [Design.md §8](../../Design.md#8-initial-adapters) — this adapter's role among all six.
- [Design.md §13 Track I](../../Design.md#13-phase-3--detailed-breakdown) — the step-by-step build history, including Decision #24 (why `eventId` doubles as the correlation key).
- [README.md § Running the demo](../../README.md#running-the-demo) — this adapter as the trigger for the full-mesh walkthrough.
- [../BUILDING_AN_ADAPTER.md](../BUILDING_AN_ADAPTER.md) — how this adapter (and any new one) is put together.

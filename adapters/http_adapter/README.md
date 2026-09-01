# HTTP Adapter

Bus → external → Bus (Design.md [§8](../../Design.md#8-initial-adapters), [§13](../../Design.md#13-phase-3--detailed-breakdown) Track H). Subscribes to whatever subjects it's configured for; for each triggering event, calls one fixed external REST API and publishes what came back as a correlated `events.http.RequestCompleted` — success or error status alike. Reports the outcome, doesn't judge it.

The closest sibling to [Webhook Sender](../webhook_sender/README.md) (same per-subject relay shape, same `JETCORE_SUBJECTS`/`JETCORE_TARGET_*` pattern), but with the opposite retry posture and a reply published back to the bus — see "What it does" below for exactly how the two diverge.

## Configuration

| Var | Required | Default | Meaning |
|---|---|---|---|
| `JETCORE_SERVICE_ID` | yes | — | `http-adapter-01` in the real deployment. |
| `JETCORE_NATS_URL` | no | `nats://nats:4222` | |
| `JETCORE_NATS_CREDS_PATH` | yes | — | |
| `JETCORE_SUBJECTS` | yes (for this adapter to do anything) | `[]` | Comma-separated or JSON array. One trigger handler + durable consumer per subject. |
| `JETCORE_TARGET_BASE_URL` | yes | — | The external REST API every triggered call is made against — one fixed target per instance (Decision #27). |
| `JETCORE_AUTH_TOKEN` | no | — | Sent as `Authorization: Bearer <token>` if set. Not required — the target API may need no auth, or a scheme this adapter doesn't need to know about (e.g. an API key baked into the URL). |
| `JETCORE_LOG_LEVEL` | no | `INFO` | |

## What it publishes / subscribes

| Subject | Direction |
|---|---|
| *(configured via `JETCORE_SUBJECTS`)* | subscribe — trigger |
| `events.http.RequestCompleted` | publish — result, `correlationId` = the triggering event's `eventId` |

`RequestCompleted` payload: `{"status": "success"|"error", "statusCode": <int>, "body": "<base64>", "occurredAt": "<ISO 8601>"}` — `status` is `"success"` for any 2xx, `"error"` for anything else. `body` is the raw response, base64-encoded, not assumed to be JSON or even text.

## Behavior, and where it differs from Webhook Sender

- **A response was received, any status** → always publish `RequestCompleted` and **ack**. Whether the target returned 200 or 500, that's a real answer worth reporting.
- **No response at all** (connection failure, timeout) → log and **nak** (leave unacked for redelivery, capped at 5 attempts). Unlike Webhook Sender's deliberate best-effort/no-retry choice, this adapter's whole job is reporting what the external API actually said — a transient network blip shouldn't silently produce nothing to report, so it's worth a retry.

## Running it standalone

```bash
JETCORE_SERVICE_ID=http-adapter-01 \
JETCORE_NATS_CREDS_PATH=infra/nats/operator/creds/http-adapter-01.creds \
JETCORE_SUBJECTS=events.orders.OrderCreated \
JETCORE_TARGET_BASE_URL=http://localhost:8080/healthz \
uv run python -m http_adapter
```

Requires NATS already up. In the normal dev stack this runs as the `http-adapter` service in [docker-compose.yml](../../docker-compose.yml), pointed at `webhook-listener`'s `/healthz` (a real, always-answering, GET-only endpoint — every trigger produces a real, observable `405`, not a silent no-op against a target that would otherwise need real internet access this dev stack shouldn't depend on).

## Testing

`uv run --all-packages pytest adapters/http_adapter` (or via `./test.sh`). `test_http_adapter_entrypoint.py` drives the real entrypoint end to end against live NATS.

## See also

- [Design.md §8](../../Design.md#8-initial-adapters) — this adapter's role among all six.
- [Design.md §13 Track H](../../Design.md#13-phase-3--detailed-breakdown) — the step-by-step build history.
- [README.md § Running the demo](../../README.md#running-the-demo) — this adapter's own reaction as one of the demo's five effects.
- [../BUILDING_AN_ADAPTER.md](../BUILDING_AN_ADAPTER.md) — how this adapter (and any new one) is put together.

# Webhook Sender

Bus → external (Design.md [§8](../../Design.md#8-initial-adapters), [§13](../../Design.md#13-phase-3--detailed-breakdown) Track G). Subscribes to whatever subjects it's configured for, and POSTs each event's decrypted payload to one fixed external webhook URL — **single attempt, best-effort, no retry**. If the target is down, the delivery is silently dropped (logged, not redelivered) — an accepted risk from the moment this shape was decided (Decision #12), not an oversight.

Publishes nothing back to the bus. In the demo stack it relays to [Webhook Listener](../webhook_listener/README.md)'s own HTTP surface — a real, self-contained round trip, not a target that only ever errors.

## Configuration

| Var | Required | Default | Meaning |
|---|---|---|---|
| `JETCORE_SERVICE_ID` | yes | — | `webhook-sender-01` in the real deployment. |
| `JETCORE_NATS_URL` | no | `nats://nats:4222` | |
| `JETCORE_NATS_CREDS_PATH` | yes | — | |
| `JETCORE_SUBJECTS` | yes (for this adapter to do anything) | `[]` | Comma-separated (`events.orders.OrderCreated,events.other.Thing`) or a JSON array string. One relay handler + durable consumer is started per subject. This is the first adapter where `JETCORE_SUBJECTS` is actually used at runtime — every earlier adapter hardcoded its subscribe subject(s) in code. |
| `JETCORE_TARGET_URL` | yes | — | The one external webhook URL every relayed event is POSTed to — one fixed target per instance (Decision #27). Run a second instance, with its own identity, for a second target. |
| `JETCORE_OUTBOUND_SECRET` | no | — | Sent as `X-Webhook-Secret` on the outbound POST if set — the mirror image of Webhook Listener's own inbound check. Not required: the receiving side may not be one of our own adapters. |
| `JETCORE_LOG_LEVEL` | no | `INFO` | |

## What it does

For each subject in `JETCORE_SUBJECTS`: fetches, decrypts/verifies (`BusClient` handles that), and POSTs the raw decrypted payload as the request body to `JETCORE_TARGET_URL`, with headers `X-Event-Type`, `X-Event-Id`, and (if configured) `X-Webhook-Secret`.

- **Success or HTTP error status** (2xx through 5xx, a real response received) — logged if non-2xx, **acked** either way. Reporting the outcome isn't this adapter's job; it isn't Decision #12's concern either.
- **No response at all** (connection refused, timeout) — logged, **acked anyway** (not left for redelivery) — genuinely different from every other adapter here, and deliberate: "single attempt, best-effort" means retrying via redelivery would itself be a retry, contradicting the whole point of the design (Decision #12).

## Running it standalone

```bash
JETCORE_SERVICE_ID=webhook-sender-01 \
JETCORE_NATS_CREDS_PATH=infra/nats/operator/creds/webhook-sender-01.creds \
JETCORE_SUBJECTS=events.orders.OrderCreated \
JETCORE_TARGET_URL=http://localhost:8080/webhooks/orders/relayed.json \
uv run python -m webhook_sender
```

Requires NATS already up. In the normal dev stack this runs as the `webhook-sender` service in [docker-compose.yml](../../docker-compose.yml), relaying to `webhook-listener`.

## Testing

`uv run --all-packages pytest adapters/webhook_sender` (or via `./test.sh`). `test_webhook_sender_entrypoint.py` drives the real entrypoint end to end against live NATS.

## See also

- [Design.md §8](../../Design.md#8-initial-adapters) — this adapter's role among all six.
- [Design.md §13 Track G](../../Design.md#13-phase-3--detailed-breakdown) — the step-by-step build history.
- [README.md § Running the demo](../../README.md#running-the-demo) — this adapter as the first hop in the full-mesh walkthrough.
- [../BUILDING_AN_ADAPTER.md](../BUILDING_AN_ADAPTER.md) — how this adapter (and any new one) is put together.

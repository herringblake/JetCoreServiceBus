# Database Adapter (MySQL)

Bidirectional (Design.md [§8](../../Design.md#8-initial-adapters), [§13](../../Design.md#13-phase-3--detailed-breakdown) Track J). The only adapter here with two genuinely independent halves, running concurrently:

- **Write path** — subscribes to a bus event, persists it into MySQL, replies with a correlated result event.
- **CDC (change data capture) path** — tails the MySQL binlog directly and publishes *any* row change (bus-originated or not) as its own event, independent of the write path.

Both halves target one demo table, `orders` (Decision #25 — real bounded-context schemas replace this whenever real ones exist, per Decision #14).

## Configuration

| Var | Required | Default | Meaning |
|---|---|---|---|
| `JETCORE_SERVICE_ID` | yes | — | `db-adapter-mysql-01` in the real deployment. |
| `JETCORE_NATS_URL` | no | `nats://nats:4222` | |
| `JETCORE_NATS_CREDS_PATH` | yes | — | |
| `JETCORE_MYSQL_HOST` | no | `mysql` | Shared by both MySQL connections below — same server, different users. |
| `JETCORE_MYSQL_PORT` | no | `3306` | |
| `JETCORE_MYSQL_DATABASE` | no | `jetcore` | |
| `JETCORE_MYSQL_WRITE_USER` / `JETCORE_MYSQL_WRITE_PASSWORD` | yes | — | The write path's DML identity (`jetcore_write` in [infra/mysql/init.sql](../../infra/mysql/init.sql)) — `INSERT`/`UPDATE` on `orders` only, deliberately no `SELECT` (see "Why the upsert SQL looks unusual" below). |
| `JETCORE_MYSQL_CDC_USER` / `JETCORE_MYSQL_CDC_PASSWORD` | yes | — | The CDC path's replication-only identity (`jetcore_cdc`) — `REPLICATION SLAVE`/`REPLICATION CLIENT` only, no DML access at all. |
| `JETCORE_LOG_LEVEL` | no | `INFO` | |

**Two distinct MySQL users, not one set of credentials reused for both roles** — least privilege per role, not just per adapter (Decision #25).

## Write path

| Subject | Direction |
|---|---|
| `events.orders.OrderCreated` | subscribe — trigger |
| `events.orders.OrderPersisted` | publish — result, `correlationId` = the triggering event's `eventId` |

Upserts into `orders` keyed on the caller-supplied `order_id` (no round trip needed to learn a DB-assigned id first, since it's caller-supplied — Decision #25). Payload: `{"orderId": "...", "item": "...", "quantity": <int>}` in, `{"orderId": "...", "status": "persisted", "occurredAt": "..."}` out.

Retry posture: a malformed payload is deterministically bad — logged, **acked**, not retried. A real MySQL error (connection dropped, deadlock) is plausibly transient — logged, left **unacked** for redelivery (capped at 5 attempts). `tenacity` is deliberately **not** used here for an in-process retry loop — JetStream redelivery already is the retry mechanism; stacking a second one on top would just delay how quickly a real outage becomes visible.

### Why the upsert SQL looks unusual

```sql
INSERT INTO orders (order_id, item, quantity) VALUES (:order_id, :item, :quantity) AS new
ON DUPLICATE KEY UPDATE item = new.item, quantity = new.quantity
```

The row-alias form (`AS new` / `new.item`), not MySQL's older `VALUES(item)` function — confirmed by testing, not just style: MySQL 8.0.20+ deprecated `VALUES()` in this context, and it turns out to actually matter here, not just draw a warning — MySQL treats `VALUES()` as reading a virtual table, requiring `SELECT` privilege on every referenced column, which `jetcore_write` deliberately doesn't have. The row-alias form needs no such grant.

## CDC path

| Subject | Direction |
|---|---|
| `events.db.orders.RowChanged` | publish — every insert/update/delete on `orders`, from *any* source (bus-originated or a direct SQL write) |

Tails the MySQL binlog via `python-mysql-replication`'s `BinLogStreamReader`, filtered to the `orders` table. Payload: `{"operation": "insert"|"update"|"delete", "table": "orders", "row": {...}, "previousRow": {...} | null}` — `previousRow` is only present for `update`. No `correlationId` — this isn't replying to a request, it's reporting that the table changed, full stop (distinct from `OrderPersisted` above, which is a direct reply to one specific write-path request).

**Known v1 gap, not fixed**: no binlog read-position (`log_file`/`log_pos`) is persisted across restarts. A fresh `BinLogStreamReader` starts from the earliest binlog MySQL still retains, not "now" — confirmed by testing. Every restart replays the adapter's entire available binlog history as a fresh burst of `RowChanged` publishes (bounded by MySQL's own binlog retention, not unbounded). Acceptable for this project's single-demo-instance scope; a real deployment would need to persist and resume from the last-seen position — Design.md [§9](../../Design.md#9-open-questions-summary) item #6.

## Running it standalone

```bash
JETCORE_SERVICE_ID=db-adapter-mysql-01 \
JETCORE_NATS_CREDS_PATH=infra/nats/operator/creds/db-adapter-mysql-01.creds \
JETCORE_MYSQL_WRITE_USER=jetcore_write JETCORE_MYSQL_WRITE_PASSWORD=jetcore-write-dev-only \
JETCORE_MYSQL_CDC_USER=jetcore_cdc JETCORE_MYSQL_CDC_PASSWORD=jetcore-cdc-dev-only \
uv run python -m db_adapter_mysql
```

Requires NATS *and* MySQL already up, with binlog enabled (`log_bin`, `binlog_format=ROW` — see [infra/mysql/my.cnf](../../infra/mysql/my.cnf); a plain `docker run mysql` without that mount won't have CDC working). In the normal dev stack this runs as the `db-adapter-mysql` service in [docker-compose.yml](../../docker-compose.yml).

## Testing

`uv run --all-packages pytest adapters/db_adapter_mysql` (or via `./test.sh`). `test_db_adapter_mysql_write_handler.py`, `test_cdc_watcher.py`, and `test_db_adapter_mysql_entrypoint.py` all run against a real MySQL instance, not a mock — `test_cdc_watcher.py` in particular writes real rows via direct SQL and asserts on the real `RowChanged` events that come out the other side.

## See also

- [Design.md §8](../../Design.md#8-initial-adapters) — this adapter's role among all six.
- [Design.md §13 Track J](../../Design.md#13-phase-3--detailed-breakdown) — the step-by-step build history, including Decisions #25/#26.
- [README.md § Running the demo](../../README.md#running-the-demo) — both this adapter's paths as two of the demo's five effects.
- [../BUILDING_AN_ADAPTER.md](../BUILDING_AN_ADAPTER.md) — how this adapter (and any new one) is put together.

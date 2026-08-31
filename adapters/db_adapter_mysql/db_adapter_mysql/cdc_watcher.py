"""CDC read path (Design.md §13 Step J5, Decision #15) — tails the MySQL
binlog for the `orders` table via `python-mysql-replication` and
publishes every row change (bus-originated or a direct SQL write
bypassing the bus entirely) to `events.db.orders.RowChanged`.

`BinLogStreamReader` is synchronous/blocking, not async-native — two real
findings (confirmed by testing, not assumed from its docs) shaped this
module:

  1. Its documented `blocking=True` mode (wait indefinitely for the next
     binlog event) cannot be interrupted by calling `.close()` from
     another thread — tried first, and it hangs forever rather than
     unblocking the iterator. `blocking=False` instead: one `for event in
     stream` pass returns once caught up (confirmed: ~60ms even against a
     real backlog), so this module polls in a loop and sleeps between
     passes — the same "poll + check shutdown between cycles" idiom every
     other adapter's entrypoint already uses (Steps C6/G4/H4/I4), just
     running on its own OS thread (`asyncio.to_thread`) since the
     underlying library has no async API at all. The shutdown check
     itself uses a `threading.Event`, not `asyncio.Event` — asyncio
     primitives aren't safe to read from a thread other than the loop
     that owns them.
  2. Every row-change publish has to cross back from that worker thread
     into the asyncio event loop that owns the real `BusClient`/NATS
     connection — `asyncio.run_coroutine_threadsafe(...).result()` is the
     standard bridge for exactly this.

No binlog read-position is persisted across restarts (an open item,
Design.md §9 — the same spirit as the already-documented encryption-
keypair-persistence gap): with no explicit `log_file`/`log_pos`, a fresh
`BinLogStreamReader` starts from the earliest binlog MySQL still retains,
confirmed by testing — not from "now." Acceptable for this phase's demo
scope (equivalent to a full initial resync on every restart, bounded by
MySQL's own binlog retention); a real deployment would need to persist
and resume from the last-seen position.
"""

from __future__ import annotations

import asyncio
import logging
import threading

from jetcore.bus_client import BusClient
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from db_adapter_mysql.payloads import encode_row_changed
from db_adapter_mysql.settings import DbAdapterSettings

logger = logging.getLogger(__name__)

ROW_CHANGED_SUBJECT = "events.db.orders.RowChanged"

# Must differ from the real server's own server-id (1, infra/mysql/my.cnf)
# — the replication protocol's requirement, same as any MySQL replica.
# Fixed rather than derived from settings.service_id: v1 scope is a
# single CDC reader instance (Decision #18's per-instance-identity
# pattern would need this to become configurable too, if a second
# instance were ever added — out of scope here).
_CDC_SERVER_ID = 100

_POLL_INTERVAL_SECONDS = 0.5


def _publish_row_changed(
    client: BusClient,
    loop: asyncio.AbstractEventLoop,
    *,
    operation: str,
    table: str,
    row: dict[str, object],
    previous_row: dict[str, object] | None,
) -> None:
    payload = encode_row_changed(
        operation=operation, table=table, row=row, previous_row=previous_row
    )
    future = asyncio.run_coroutine_threadsafe(
        client.publish(ROW_CHANGED_SUBJECT, payload, event_type="RowChanged"), loop
    )
    future.result()  # propagate a publish failure back into this thread, not swallowed


def _run_stream(
    client: BusClient,
    settings: DbAdapterSettings,
    loop: asyncio.AbstractEventLoop,
    shutdown: threading.Event,
) -> None:
    stream = BinLogStreamReader(
        connection_settings={
            "host": settings.mysql_host,
            "port": settings.mysql_port,
            "user": settings.mysql_cdc_user,
            "passwd": settings.mysql_cdc_password.get_secret_value(),
        },
        server_id=_CDC_SERVER_ID,
        blocking=False,
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        only_tables=["orders"],
        only_schemas=[settings.mysql_database],
    )
    try:
        while not shutdown.is_set():
            for event in stream:
                if isinstance(event, WriteRowsEvent):
                    for row in event.rows:
                        _publish_row_changed(
                            client,
                            loop,
                            operation="insert",
                            table=event.table,
                            row=row["values"],
                            previous_row=None,
                        )
                elif isinstance(event, UpdateRowsEvent):
                    for row in event.rows:
                        _publish_row_changed(
                            client,
                            loop,
                            operation="update",
                            table=event.table,
                            row=row["after_values"],
                            previous_row=row["before_values"],
                        )
                elif isinstance(event, DeleteRowsEvent):
                    for row in event.rows:
                        _publish_row_changed(
                            client,
                            loop,
                            operation="delete",
                            table=event.table,
                            row=row["values"],
                            previous_row=None,
                        )
                if shutdown.is_set():
                    break
            if not shutdown.is_set():
                # Event.wait() (unlike stream.close(), see module
                # docstring) genuinely unblocks the instant shutdown is
                # set from the main thread — this isn't just a sleep.
                shutdown.wait(_POLL_INTERVAL_SECONDS)
    finally:
        stream.close()


class CdcWatcher:
    def __init__(self, client: BusClient, settings: DbAdapterSettings) -> None:
        self._client = client
        self._settings = settings
        self._shutdown = threading.Event()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        await asyncio.to_thread(_run_stream, self._client, self._settings, loop, self._shutdown)

    def request_shutdown(self) -> None:
        self._shutdown.set()

"""MySQL connection helper for the write path (Design.md §13 Step J4) —
one place building the `asyncmy`-backed SQLAlchemy async engine, so the
write handler and its tests don't each construct their own connection URL.

The CDC read path (Step J5) deliberately does NOT use this — it talks to
MySQL over the binlog replication protocol via `python-mysql-replication`
directly, a different connection mechanism entirely (and a different
MySQL user, Decision #25/Step J3 — least privilege, not shared
credentials).
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from db_adapter_mysql.settings import DbAdapterSettings


def create_write_engine(settings: DbAdapterSettings) -> AsyncEngine:
    url = (
        f"mysql+asyncmy://{settings.mysql_write_user}:"
        f"{settings.mysql_write_password.get_secret_value()}@"
        f"{settings.mysql_host}:{settings.mysql_port}/{settings.mysql_database}"
    )
    return create_async_engine(url, pool_pre_ping=True)

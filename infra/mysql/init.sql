-- Database Adapter demo schema + users (Design.md §13 Step J1, Decision
-- #25/#26 and the Track J parameter table's "two distinct MySQL users"
-- entry). Runs automatically on first container start against a fresh
-- data volume (the official mysql image's own /docker-entrypoint-initdb.d
-- convention) — `MYSQL_DATABASE` (docker-compose.yml, Track K) creates
-- the `jetcore` database itself before this file runs.

USE jetcore;

-- Decision #25: order_id is CALLER-supplied (not a DB-assigned
-- autoincrement) so the write path can upsert idempotently without a
-- round trip back through the bus first. A few placeholder business
-- columns since `orders` is itself still a placeholder bounded context
-- (Decision #14) — real columns replace these whenever a real domain
-- does.
CREATE TABLE IF NOT EXISTS orders (
    order_id   VARCHAR(64)  NOT NULL PRIMARY KEY,
    item       VARCHAR(255) NOT NULL,
    quantity   INT          NOT NULL DEFAULT 1,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Two distinct identities (least privilege), not one set of credentials
-- reused for both roles:
--
-- 1. Write path: normal DML only, scoped to this one table. No DELETE
--    (the write path only ever upserts, Decision #25) and no access to
--    any other table/database.
--
--    SELECT is included here too, confirmed necessary by testing (Step
--    J4) — not the mistake it looks like: MySQL's `INSERT ... ON
--    DUPLICATE KEY UPDATE` requires SELECT on any column read back from
--    the new row during the update, whether written as the older
--    `VALUES(col)` function OR the newer (8.0.19+) row-alias form
--    (`... AS new ... col = new.col`) — both were tried against this
--    exact grant set with only INSERT/UPDATE, and both failed with the
--    same `SELECT command denied` error before this line was added.
CREATE USER IF NOT EXISTS 'jetcore_write'@'%' IDENTIFIED BY 'jetcore-write-dev-only';
GRANT INSERT, UPDATE, SELECT ON jetcore.orders TO 'jetcore_write'@'%';

-- 2. CDC read path: REPLICATION SLAVE/REPLICATION CLIENT are GLOBAL
--    grants (the binlog protocol has no per-table scope) but they carry
--    NO access to actual table data — python-mysql-replication only ever
--    needs to open a replication connection and stream binlog events
--    with this user, never to run a SELECT against `orders` itself.
CREATE USER IF NOT EXISTS 'jetcore_cdc'@'%' IDENTIFIED BY 'jetcore-cdc-dev-only';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'jetcore_cdc'@'%';

FLUSH PRIVILEGES;

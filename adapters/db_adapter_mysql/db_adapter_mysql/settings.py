"""Database Adapter configuration (Design.md §13 Step J3). Extends
jetcore's AdapterSettings with the settings this adapter needs beyond the
shared baseline — the first adapter with **two distinct MySQL identities**
(Decision #25/Track J parameter table's "two distinct MySQL users," least
privilege): a DML user for the write path, and a separate
`REPLICATION SLAVE`/`REPLICATION CLIENT`-only user for the CDC read path.
Not one set of credentials reused for both roles.

Field <-> env var mapping (still JETCORE_-prefixed, per jetcore.config):
  mysql_host              JETCORE_MYSQL_HOST               — shared by both
                                                              connections;
                                                              same server,
                                                              different
                                                              users.
  mysql_port               JETCORE_MYSQL_PORT               — default 3306.
  mysql_database           JETCORE_MYSQL_DATABASE           — default
                                                              "jetcore"
                                                              (infra/mysql/
                                                              init.sql,
                                                              Step J1).

  mysql_write_user         JETCORE_MYSQL_WRITE_USER         — the write
                                                              path's DML
                                                              identity
                                                              (`jetcore_write`
                                                              in Step J1's
                                                              init.sql).
  mysql_write_password     JETCORE_MYSQL_WRITE_PASSWORD

  mysql_cdc_user           JETCORE_MYSQL_CDC_USER           — the CDC read
                                                              path's
                                                              replication-only
                                                              identity
                                                              (`jetcore_cdc`).
  mysql_cdc_password       JETCORE_MYSQL_CDC_PASSWORD
"""

from __future__ import annotations

from jetcore.config import AdapterSettings
from pydantic import SecretStr


class DbAdapterSettings(AdapterSettings):
    mysql_host: str = "mysql"
    mysql_port: int = 3306
    mysql_database: str = "jetcore"

    mysql_write_user: str
    mysql_write_password: SecretStr

    mysql_cdc_user: str
    mysql_cdc_password: SecretStr

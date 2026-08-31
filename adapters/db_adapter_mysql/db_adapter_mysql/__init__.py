"""db_adapter_mysql — bidirectional Database Adapter (Design.md §8, §13
Track J). Write path persists bus events to MySQL; CDC read path tails
the binlog and publishes row changes.

Step J2 scaffold. Real modules (settings, write path, CDC read path,
entrypoint) land in Steps J3-J6 — this package is intentionally empty of
application logic until then.
"""

__version__ = "0.1.0"

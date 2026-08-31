"""Step J2 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists.
Uniquely named from the start (Step C1's lesson, applied proactively by
every adapter since Track G).
"""

import db_adapter_mysql


def test_package_imports_and_has_version() -> None:
    assert db_adapter_mysql.__version__ == "0.1.0"

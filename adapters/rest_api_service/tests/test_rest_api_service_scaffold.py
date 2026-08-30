"""Step I2 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists.
Uniquely named from the start (Step C1's lesson, applied proactively by
every adapter since Track G).
"""

import rest_api_service


def test_package_imports_and_has_version() -> None:
    assert rest_api_service.__version__ == "0.1.0"

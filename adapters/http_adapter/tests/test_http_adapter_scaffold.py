"""Step H1 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists.
Uniquely named from the start (Step C1's lesson).
"""

import http_adapter


def test_package_imports_and_has_version() -> None:
    assert http_adapter.__version__ == "0.1.0"

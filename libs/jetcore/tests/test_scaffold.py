"""Step B1 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists."""

import jetcore


def test_package_imports_and_has_version() -> None:
    assert jetcore.__version__ == "0.1.0"

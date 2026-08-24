"""Step B1 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists."""

import gsb_core


def test_package_imports_and_has_version() -> None:
    assert gsb_core.__version__ == "0.1.0"

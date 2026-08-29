"""Step D1 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists.
Uniquely named (not test_scaffold.py): a bare-filename collision across
workspace members is a real, confirmed pytest/mypy failure — see Step
C1's finding in Design.md §12 and
adapters/file_storage_adapter/tests/test_file_storage_adapter_scaffold.py.
"""

import webhook_listener


def test_package_imports_and_has_version() -> None:
    assert webhook_listener.__version__ == "0.1.0"

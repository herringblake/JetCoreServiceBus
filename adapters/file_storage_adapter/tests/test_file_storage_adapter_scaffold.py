"""Step C1 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists.
Mirrors libs/jetcore/tests/test_scaffold.py (Step B1) in spirit.

Named test_file_storage_adapter_scaffold.py, not test_scaffold.py: pytest's
default (no __init__.py) test collection imports each test file as a bare
top-level module by filename alone, so a second file also named
test_scaffold.py anywhere else in the workspace (e.g. libs/jetcore/tests/)
collides — confirmed by actually hitting `import file mismatch` when this
was first named test_scaffold.py. Every future adapter's scaffold test
needs an equally unique name for the same reason; --import-mode=importlib
would also fix this but was rejected here because it breaks
libs/jetcore/tests/conftest.py's existing bare `from _helpers import ...`
(confirmed by testing, not assumed)."""

import file_storage_adapter


def test_package_imports_and_has_version() -> None:
    assert file_storage_adapter.__version__ == "0.1.0"

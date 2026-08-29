"""Step G1 scaffold smoke test — proves the package installs and the
pytest/ruff/mypy wiring actually works, before any real logic exists.
Uniquely named from the start (Step C1's lesson): a bare-filename
collision across workspace members is a real, confirmed pytest/mypy
failure — see Design.md §12 Step C1 and the sibling adapters' own
test_*_scaffold.py files.
"""

import webhook_sender


def test_package_imports_and_has_version() -> None:
    assert webhook_sender.__version__ == "0.1.0"

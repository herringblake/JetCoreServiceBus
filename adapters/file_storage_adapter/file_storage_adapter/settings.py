"""File Storage Adapter configuration (Design.md §12 Step C2). Extends
jetcore's AdapterSettings with the one setting this adapter needs beyond
the shared baseline — proves out the subclassing pattern Step B4's
test_subclassing_adds_adapter_specific_settings already exercised, this
time for a real adapter instead of a test-only example.

Field <-> env var mapping (still JETCORE_-prefixed, per jetcore.config):
  watch_dir   JETCORE_WATCH_DIR   — the directory this adapter reads/writes
                                     files in (Design.md §12 parameter
                                     table). Every FileWriteRequested `path`
                                     is resolved *relative to this root* and
                                     validated to stay inside it (Step C4) —
                                     this setting alone doesn't enforce
                                     that, it just names the root.
"""

from __future__ import annotations

from jetcore.config import AdapterSettings
from pydantic import DirectoryPath


class FileStorageSettings(AdapterSettings):
    # DirectoryPath: pydantic's built-in "must exist and be a directory"
    # type — fails fast at config-load time, same reasoning as
    # AdapterSettings.nats_creds_path's FilePath (Step B4).
    watch_dir: DirectoryPath

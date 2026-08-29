"""A real local HTTP server for webhook_sender's tests (Design.md §13
Step G5) — stdlib only (`http.server`), no new dependency, and no mocking:
this project's whole testing ethos has been real components throughout
(real NATS, real crypto, real files, real Docker); a relay adapter's own
tests shouldn't be the first to reach for a fake HTTP layer instead of a
real socket.

Named _webhook_sender_local_server.py, not _local_http_server.py (its
original name): renamed once Step H5 built an identical helper for
http_adapter's own tests and hit the exact same bare-module-name
collision this project has now found repeatedly (test_*.py files,
_*_helpers.py, and now this) — a generically-named local module isn't
safe to reuse verbatim across workspace members, ever.

Runs in a background thread (`http.server` is synchronous) alongside the
async test code, which just POSTs to a real `127.0.0.1` port.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class ReceivedRequest:
    def __init__(self, path: str, headers: dict[str, str], body: bytes) -> None:
        self.path = path
        self.headers = headers
        self.body = body


class LocalHttpServer:
    def __init__(self, *, status_code: int = 200) -> None:
        self.received: list[ReceivedRequest] = []
        self.status_code = status_code
        handler = _make_handler(self)
        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        _, port = self._httpd.server_address[:2]
        return f"http://127.0.0.1:{port}"

    def shutdown(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2)


def _make_handler(server: LocalHttpServer) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 (http.server's own naming convention)
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            server.received.append(ReceivedRequest(self.path, dict(self.headers), body))
            self.send_response(server.status_code)
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            pass  # silence http.server's default stderr access logging

    return _Handler

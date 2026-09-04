"""Small internal webhook sink used to prove Alertmanager delivery in the lab."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from typing import Any

EVENTS: list[dict[str, Any]] = []
EVENTS_LOCK = Lock()


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, body: object) -> None:
        encoded = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        if self.path == "/alerts":
            with EVENTS_LOCK:
                self._send_json(200, {"events": EVENTS})
            return
        self._send_json(404, {"detail": "not found"})

    def do_POST(self) -> None:
        if self.path != "/alerts":
            self._send_json(404, {"detail": "not found"})
            return
        size = int(self.headers.get("Content-Length", "0"))
        event = json.loads(self.rfile.read(size))
        with EVENTS_LOCK:
            EVENTS.append(event)
            del EVENTS[:-20]
        print(json.dumps(event), flush=True)
        self._send_json(200, {"status": "accepted"})

    def log_message(self, _format: str, *_args: object) -> None:
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()

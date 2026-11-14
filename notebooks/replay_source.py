"""Strict hash-verified replay of two complete public Kalshi response bodies."""

from __future__ import annotations

import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit


def request_key(path):
    parsed = urlsplit(path)
    return parsed.path, tuple(sorted(parse_qsl(parsed.query, keep_blank_values=True)))


class ReplaySource:
    def __init__(self, directory: Path):
        self.requests = []
        manifest = json.loads((directory / "manifest.json").read_text())
        bodies = {}
        for entry in manifest["entries"]:
            body = (directory / entry["filename"]).read_bytes()
            if hashlib.sha256(body).hexdigest() != entry["sha256"]:
                raise ValueError(f"Recorded body hash mismatch: {entry['filename']}")
            bodies[request_key("/trade-api/v2" + entry["request_path"])] = body
        requests = self.requests

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                pass

            def do_GET(self):
                key = request_key(self.path)
                requests.append(self.path)
                if key not in bodies:
                    self.send_error(400, "Request outside recorded coverage; no fallback")
                    return
                body = bodies[key]
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/trade-api/v2"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

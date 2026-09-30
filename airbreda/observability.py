"""
observability.py — shared by ingest_air.py and ingest_traffic.py (Day 2, Lab 2).

- setup_logging(): every log line on stdout is one JSON object, so a
  monitoring dashboard (Day 4/5) can query fields instead of parsing text.
- SourceMonitor: in-memory bad_data_count per source, plus the rolling
  one-hour window behind the BAD_DATA_THRESHOLD_EXCEEDED error event.
- start_health_server(): serves GET /health from a background thread.
"""

import json
import logging
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BAD_DATA_THRESHOLD = 10  # more than this many bad readings ...
BAD_DATA_WINDOW_SECONDS = 3600  # ... within this window logs one ERROR event


class JsonFormatter(logging.Formatter):
    """Callers log json.dumps({...}); this merges in level and time so every
    line has the same envelope. Non-JSON messages (e.g. from libraries) are
    wrapped as {"message": "..."} rather than breaking the format."""

    def format(self, record: logging.LogRecord) -> str:
        try:
            payload = json.loads(record.getMessage())
            if not isinstance(payload, dict):
                payload = {"message": payload}
        except ValueError:
            payload = {"message": record.getMessage()}
        return json.dumps({
            "level": record.levelname,
            "logged_at": utc_now(),
            **payload,
        })


def setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SourceMonitor:
    """Per-source health state: when we last fetched successfully and how
    many bad readings we've seen since the service started."""

    def __init__(self, source: str, clock=time.time):
        self.source = source
        self.bad_data_count = 0
        self.last_successful_fetch = None
        self._clock = clock
        self._recent = deque()  # clock() times of bad readings in the window
        self._alerted = False  # one ERROR per breach, not one per extra reading

    def record_success(self) -> None:
        self.last_successful_fetch = utc_now()

    def record_bad_data(self) -> None:
        now = self._clock()
        self.bad_data_count += 1
        self._recent.append(now)
        while self._recent and now - self._recent[0] > BAD_DATA_WINDOW_SECONDS:
            self._recent.popleft()

        if len(self._recent) > BAD_DATA_THRESHOLD:
            if not self._alerted:
                logging.error(json.dumps({
                    "event": "BAD_DATA_THRESHOLD_EXCEEDED",
                    "source": self.source,
                    "count": len(self._recent),
                }))
                self._alerted = True
        else:
            self._alerted = False  # back under the threshold: re-arm

    def health(self) -> dict:
        return {
            "last_successful_fetch": self.last_successful_fetch,
            "bad_data_count": self.bad_data_count,
            "source": self.source,
        }


def start_health_server(monitor: SourceMonitor, port: int) -> ThreadingHTTPServer:
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/health":
                self.send_error(404)
                return
            body = json.dumps(monitor.health()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass  # keep stdout JSON-only; health checks would be noise anyway

    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server

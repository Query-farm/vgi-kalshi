#!/usr/bin/env python3
"""A local fake of Kalshi's trade-api/v2, served for test/sql/*.test.

ci/run-integration.sh starts this, reads ``PORT:<n>`` from stdout, and points the
worker at it with ``KALSHI_BASE_URL=http://127.0.0.1:<n>/trade-api/v2``, so the
SQL suite runs offline and deterministically.

Data, all already committed under notebooks/:

* ``surface_fixtures.json`` -- the fictional DEMOWX/DEMOPOWER series, events,
  markets, trades, candles and archive the notebooks' synthetic API serves.
* ``fixtures/market.json`` and ``fixtures/orderbook.json`` -- two complete,
  unmodified public responses recorded from Kalshi for one KXBTCD market.
* the ``/margin/*`` account payloads mirror tests/test_account.py (values
  changed from real responses). They answer only a request that carries the
  three ``KALSHI-ACCESS-*`` headers for key id ``test-key-id``; the signature
  itself is checked by tests/test_account.py, not here (stdlib has no Ed25519).

Unlike the notebooks' strict replay, routing here is lenient about query
parameters: it applies the filters Kalshi documents (series, event, status,
tickers, time windows) and pages list endpoints at :data:`PAGE_CAP` rows, so
cursors are actually followed. Stdlib only: it runs under ``uv run --no-project``.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import contextlib
import json
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2] / "notebooks"
DATA: dict[str, Any] = json.loads((ROOT / "surface_fixtures.json").read_text())
RECORDED_MARKET: dict[str, Any] = json.loads((ROOT / "fixtures" / "market.json").read_text())["market"]
RECORDED_BOOKS: list[dict[str, Any]] = json.loads((ROOT / "fixtures" / "orderbook.json").read_text())[
    "orderbooks"
]

API_ROOT = "/trade-api/v2"
#: List endpoints return at most this many rows per page, so cursors are followed.
PAGE_CAP = 4
TEST_KEY_ID = "test-key-id"

#: The status *filter* vocabulary against the status *column* it selects.
STATUS_FILTER = {
    "unopened": {"initialized"},
    "open": {"active"},
    "closed": {"closed"},
    "settled": {"settled", "finalized", "determined"},
}

#: The live candle tape exists for one synthetic market only.
LIVE_CANDLES = {"DEMOWX-26NOV15-T20": DATA["candles"]}

ACCOUNT = {
    "/margin/positions": {
        "positions": [
            {
                "entry_price": "3.8643",
                "fees": "0.2773",
                "is_portfolio": False,
                "margin_used": "184.5318",
                "market_ticker": "KXGOLDPERP",
                "position": "-996.00",
                "roe": -5.07565633673979,
                "subaccount": 85,
                "subaccount_type": "isolated",
                "unrealized_pnl": "-9.3662",
            }
        ]
    },
    "/margin/balance": {
        "settled_funds": "1100.000000",
        "subaccount_balances": [
            {
                "account_equity": "0.000000",
                "subaccount": 0,
                "subaccount_type": "cross",
                "maintenance_margin": "0.0000",
            },
            {
                "account_equity": "1090.356500",
                "available_balance": "0.000000",
                "initial_margin": "233.3708",
                "maintenance_margin": "184.5318",
                "position_value": "-3858.304800",
                "resting_orders_margin": "0.0000",
                "subaccount": 85,
                "subaccount_type": "isolated",
            },
        ],
    },
    "/margin/risk": {
        "account_leverage": 20.9086,
        "positions": [
            {
                "estimated_liquidation_price": "4.9065",
                "is_portfolio": False,
                "maintenance_margin_required": "184.5318",
                "mark_price": "3.8738",
                "market_ticker": "KXGOLDPERP",
                "position": "-996.00",
                "position_leverage": 20.9086,
                "position_notional": "3858.3048",
                "subaccount": 85,
                "subaccount_type": "isolated",
            }
        ],
        "total_maintenance_margin": "184.5318",
        "total_position_notional": "3858.3048",
    },
    "/margin/funding_history": {
        "funding_history": [
            {
                "market_ticker": "KXGOLDPERP",
                "funding_time": "2026-11-16T15:00:00Z",
                "funding_rate": 0.000132,
                "mark_price": "3.8711",
                "funding_amount": "0.508962",
                "quantity": "-996.00",
                "subaccount_number": 85,
                "subaccount_type": "isolated",
            }
        ]
    },
}


class NotFound(Exception):
    """Answered as Kalshi's 404."""


class BadRequest(Exception):
    """Answered as Kalshi's 400."""


def _epoch(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())


def _series_of(ticker: str) -> str:
    return ticker.split("-", 1)[0]


def _page(rows: list[dict[str, Any]], q: dict[str, list[str]], key: str) -> dict[str, Any]:
    start = int(q.get("cursor", ["0"])[0] or 0)
    size = min(int(q.get("limit", ["1000"])[0]), PAGE_CAP)
    chunk = rows[start : start + size]
    more = start + size < len(rows)
    return {key: chunk, "cursor": str(start + size) if more else ""}


def _in_window(rows: list[dict[str, Any]], q: dict[str, list[str]], field: str) -> list[dict[str, Any]]:
    lo = int(q["min_ts"][0]) if "min_ts" in q else None
    hi = int(q["max_ts"][0]) if "max_ts" in q else None
    return [
        r for r in rows if (lo is None or _epoch(r[field]) >= lo) and (hi is None or _epoch(r[field]) <= hi)
    ]


def _candles(rows: list[dict[str, Any]], q: dict[str, list[str]]) -> list[dict[str, Any]]:
    lo, hi = int(q["start_ts"][0]), int(q["end_ts"][0])
    return [c for c in rows if lo <= c["end_period_ts"] <= hi]


def _markets(q: dict[str, list[str]], source: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = list(source)
    if "series_ticker" in q:
        rows = [m for m in rows if _series_of(m["ticker"]) == q["series_ticker"][0]]
    if "event_ticker" in q:
        rows = [m for m in rows if m["event_ticker"] == q["event_ticker"][0]]
    if "tickers" in q:
        wanted = set(",".join(q["tickers"]).split(","))
        rows = [m for m in rows if m["ticker"] in wanted]
    if "status" in q:
        status = q["status"][0]
        if status not in STATUS_FILTER:
            raise BadRequest("invalid status filter")
        rows = [m for m in rows if m["status"] in STATUS_FILTER[status]]
    return rows


def route(path: str, q: dict[str, list[str]]) -> dict[str, Any]:
    """The response body for one public GET, or raise NotFound / BadRequest."""
    parts = [unquote(p) for p in path.strip("/").split("/")]
    live_markets = [*DATA["markets"], RECORDED_MARKET]
    if path == "/series":
        rows = DATA["series"]
        if "category" in q:
            rows = [s for s in rows if s.get("category") == q["category"][0]]
        return {"series": rows}
    if path == "/exchange/status":
        return DATA["exchange_status"]
    if path == "/historical/cutoff":
        return DATA["historical_cutoff"]
    if path == "/markets":
        return _page(_markets(q, live_markets), q, "markets")
    if path == "/markets/trades":
        rows = DATA["trades"]
        if "ticker" in q:
            rows = [t for t in rows if t["ticker"] == q["ticker"][0]]
        return _page(_in_window(rows, q, "created_time"), q, "trades")
    if path == "/markets/orderbooks":
        wanted = q.get("tickers", [])
        return {"orderbooks": [b for b in RECORDED_BOOKS if b["ticker"] in wanted]}
    if path == "/markets/candlesticks":
        tickers = ",".join(q.get("market_tickers", [])).split(",")
        return {
            "markets": [
                {"market_ticker": t, "candlesticks": _candles(LIVE_CANDLES.get(t, []), q)}
                for t in tickers
                if t
            ]
        }
    if len(parts) == 2 and parts[0] == "markets":
        for market in live_markets:
            if market["ticker"] == parts[1]:
                return {"market": market}
        raise NotFound
    if len(parts) == 3 and parts[0] == "markets" and parts[2] == "orderbook":
        for book in RECORDED_BOOKS:
            if book["ticker"] == parts[1]:
                return {"orderbook_fp": book["orderbook_fp"]}
        raise NotFound
    if path == "/events":
        rows = [e for e in DATA["events"] if e["series_ticker"] == q.get("series_ticker", [""])[0]]
        return _page(rows, q, "events")
    if len(parts) in (2, 3) and parts[0] == "events":
        event = next((e for e in DATA["events"] if e["event_ticker"] == parts[1]), None)
        if event is None:
            raise NotFound
        if len(parts) == 3:
            if parts[2] != "metadata":
                raise NotFound
            return DATA["metadata"][parts[1]]
        return {"event": event, "markets": [m for m in DATA["markets"] if m["event_ticker"] == parts[1]]}
    if path == "/historical/markets":
        return _page(_markets(q, DATA["historical_markets"]), q, "markets")
    if path == "/historical/trades":
        rows = DATA["historical_trades"].get(q.get("ticker", [""])[0], [])
        return _page(_in_window(rows, q, "created_time"), q, "trades")
    if len(parts) == 4 and parts[:2] == ["historical", "markets"] and parts[3] == "candlesticks":
        return {"candlesticks": _candles(DATA["historical_candles"].get(parts[2], []), q)}
    raise NotFound


class Handler(BaseHTTPRequestHandler):
    """GET only: the worker is read-only, so any write is a 405."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("mock_api: " + (format % args) + "\n")

    def _send(self, status: int, body: object) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        """Route one request below /trade-api/v2."""
        url = urlsplit(self.path)
        if not url.path.startswith(API_ROOT + "/"):
            self._send(404, {"error": {"code": "not_found", "message": "not found"}})
            return
        path = url.path.removeprefix(API_ROOT)
        query = parse_qs(url.query, keep_blank_values=True)
        if path in ACCOUNT:
            signed = (
                all(self.headers.get(h) for h in ("KALSHI-ACCESS-SIGNATURE", "KALSHI-ACCESS-TIMESTAMP"))
                and self.headers.get("KALSHI-ACCESS-KEY") == TEST_KEY_ID
            )
            if not signed:
                self._send(401, {"error": {"code": "authentication_error", "message": "unauthorized"}})
                return
            self._send(200, ACCOUNT[path])
            return
        try:
            self._send(200, route(path, query))
        except NotFound:
            self._send(404, {"error": {"code": "not_found", "message": "not found"}})
        except BadRequest as exc:
            self._send(400, {"error": {"code": "bad_request", "message": str(exc)}})

    def do_POST(self) -> None:
        """Refuse every write."""
        self._send(405, {"error": {"message": "the worker must never write"}})

    do_PUT = do_PATCH = do_DELETE = do_POST  # noqa: N815


def main() -> None:
    """Start the server and print ``PORT:<n>`` once it is listening."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()

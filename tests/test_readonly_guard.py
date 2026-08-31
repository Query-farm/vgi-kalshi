"""Structural guard: this worker must never issue a write to Kalshi.

Kalshi's order-entry endpoints sit on the same REST API as the market data. The
worker is read-only by construction — a single ``_get`` chokepoint — and this
test fails the build if that ever stops being true.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parent.parent / "vgi_kalshi"

#: httpx verbs that would mutate state on the exchange.
WRITE_VERBS = {"post", "put", "patch", "delete", "request", "stream", "send"}


def _calls(tree: ast.AST) -> list[ast.Call]:
    return [node for node in ast.walk(tree) if isinstance(node, ast.Call)]


class TestNoWriteVerbs:
    def test_no_http_write_calls_anywhere(self) -> None:
        offenders: list[str] = []
        for path in PACKAGE.rglob("*.py"):
            tree = ast.parse(path.read_text())
            for call in _calls(tree):
                if isinstance(call.func, ast.Attribute) and call.func.attr in WRITE_VERBS:
                    offenders.append(f"{path.name}: .{call.func.attr}()")
        assert offenders == [], f"write-shaped HTTP calls found: {offenders}"

    def test_single_http_chokepoint(self) -> None:
        """Only kalshi_api.py may reference httpx at all.

        Every other module reaches the network through `api.open_client()` and
        the `_get` chokepoint below it, so there is exactly one file to audit.
        """
        users = sorted(path.name for path in PACKAGE.rglob("*.py") if "httpx" in path.read_text())
        assert users == ["kalshi_api.py"], users

    def test_no_portfolio_or_order_paths(self) -> None:
        """No credentialed endpoint families are reachable from this worker."""
        forbidden = ("/portfolio", "/order_groups", "/api_keys", "/margin/orders")
        for path in PACKAGE.rglob("*.py"):
            text = path.read_text()
            for fragment in forbidden:
                assert f'"{fragment}' not in text, f"{path.name} references {fragment}"


class TestPathTraversal:
    """A ticker must not be able to leave its path segment.

    Tickers arrive from user SQL. Interpolated raw, `market('../../portfolio/
    balance')` resolved to `/trade-api/portfolio/balance` — outside the
    market-data prefix and into the credentialed account surface this worker
    exists not to touch. With authentication configured the request would have
    been signed, too.

    `test_no_portfolio_or_order_paths` cannot catch this: it looks for forbidden
    paths as source literals, and this builds one at runtime from a value the
    source never contains.
    """

    #: Every function that interpolates a caller-supplied value into a path.
    HOSTILE = "../../portfolio/balance"

    @staticmethod
    def _urls(call: object) -> list[str]:
        import httpx

        from vgi_kalshi import kalshi_api

        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(
                200, json={"market": {}, "orderbook_fp": {}, "event": {}, "candlesticks": []}
            )

        client = httpx.Client(transport=httpx.MockTransport(handler))
        call(kalshi_api, client)  # type: ignore[operator]
        return seen

    CALLS = [
        ("market", lambda api, c: api.market(TestPathTraversal.HOSTILE, client=c)),
        ("orderbook", lambda api, c: api.orderbook(TestPathTraversal.HOSTILE, client=c)),
        (
            "candlesticks",
            lambda api, c: api.candlesticks(
                TestPathTraversal.HOSTILE,
                TestPathTraversal.HOSTILE,
                period_interval=60,
                start_ts=0,
                end_ts=1,
                client=c,
            ),
        ),
        ("event", lambda api, c: api.event(TestPathTraversal.HOSTILE, client=c)),
        ("event_metadata", lambda api, c: api.event_metadata(TestPathTraversal.HOSTILE, client=c)),
        (
            "historical_candlesticks",
            lambda api, c: api.historical_candlesticks(
                TestPathTraversal.HOSTILE, period_interval=60, start_ts=0, end_ts=1, client=c
            ),
        ),
    ]

    @pytest.mark.parametrize(("name", "call"), CALLS, ids=[c[0] for c in CALLS])
    def test_a_traversing_ticker_cannot_reach_a_credentialed_path(self, name: str, call: object) -> None:
        for url in self._urls(call):
            assert "/portfolio" not in url, f"{name} escaped its path segment: {url}"
            assert "/trade-api/v2/" in url, f"{name} left the market-data prefix: {url}"

    @pytest.mark.parametrize(("name", "call"), CALLS, ids=[c[0] for c in CALLS])
    def test_the_segment_is_encoded_not_dropped(self, name: str, call: object) -> None:
        """Encoded, not stripped — the request still describes what was asked for."""
        for url in self._urls(call):
            assert "%2F" in url, f"{name} did not encode the separator: {url}"

    def test_a_query_string_cannot_be_injected(self) -> None:
        """A `?` in a ticker would otherwise truncate the path and add parameters."""
        import httpx

        from vgi_kalshi import kalshi_api

        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(200, json={"market": {}})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        kalshi_api.market("TICKER?limit=9999", client=client)
        assert "limit=9999" not in seen[0]
        assert "%3F" in seen[0]

    def test_an_ordinary_ticker_is_unchanged(self) -> None:
        """Encoding must not mangle the tickers Kalshi actually uses."""
        from vgi_kalshi.kalshi_api import segment

        assert segment("KXBTCD-26AUG3117-T87749.99") == "KXBTCD-26AUG3117-T87749.99"

"""Structural guard: this worker must never issue a write to Kalshi.

Kalshi's order-entry endpoints sit on the same REST API as the market data. The
worker is read-only by construction — a single ``_get`` chokepoint — and this
test fails the build if that ever stops being true.
"""

from __future__ import annotations

import ast
from pathlib import Path

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

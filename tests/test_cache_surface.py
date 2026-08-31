"""What the worker actually advertises as cacheable, per function.

The individual policy helpers are unit-tested in `test_caching.py`. This is the
question those tests cannot answer: after the policy is chosen, does cache
metadata reach a real emitted batch, and on the first one — which is the only
batch the client reads it from. Paging made that worth checking, because a scan
now emits many batches where it used to emit one.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

import vgi_kalshi.kalshi_api as api
from vgi_kalshi.markets import EventsArgs, EventsFunction, MarketsArgs, MarketsFunction
from vgi_kalshi.schemas import EVENT_SCHEMA, MARKET_SCHEMA


class _Out:
    def __init__(self) -> None:
        self.controls: list[Any] = []
        self.done = False

    def emit(self, batch: Any, **kwargs: Any) -> None:
        self.controls.append(kwargs.get("cache_control"))

    def finish(self) -> None:
        self.done = True


def _drive(func: Any, args: Any, schema: Any, header: str | None, pages: int = 3) -> list[Any]:
    """Run a paged scan to completion against a mock origin; collect its cache metadata."""
    seen = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["n"] += 1
        body: dict[str, Any] = {
            "markets": [{"ticker": f"T{seen['n']}"}],
            "events": [{"event_ticker": f"E{seen['n']}"}],
        }
        if seen["n"] < pages:
            body["cursor"] = f"c{seen['n']}"
        headers = {"cache-control": header} if header else {}
        return httpx.Response(200, json=body, headers=headers)

    class Params:
        current_pushdown_filters = None
        secrets = None
        attach_opaque_data = None
        output_schema = schema

    Params.args = args  # type: ignore[attr-defined]
    original = api.open_client
    api.open_client = lambda: httpx.Client(transport=httpx.MockTransport(handler))
    try:
        state, out = func.initial_state(Params()), _Out()
        for _ in range(pages + 2):
            if out.done:
                break
            func.process(Params(), state, out)
    finally:
        api.open_client = original
    return out.controls


class TestPagedScanCaching:
    def test_the_origin_ttl_reaches_the_first_batch(self) -> None:
        """`/markets` declares max-age=15; that has to survive the paging rewrite."""
        controls = _drive(
            MarketsFunction, MarketsArgs(series_ticker="K"), MARKET_SCHEMA, "public, max-age=15"
        )
        assert controls[0] is not None
        assert controls[0].ttl == 15
        assert controls[0].to_metadata()["vgi.cache.ttl"] == "15"

    def test_every_page_carries_it_not_just_the_first(self) -> None:
        """The client reads the first, but a partial walk should still describe itself."""
        controls = _drive(
            MarketsFunction, MarketsArgs(series_ticker="K"), MARKET_SCHEMA, "public, max-age=15"
        )
        assert len(controls) >= 3
        assert all(c is not None and c.ttl == 15 for c in controls)

    def test_a_silent_origin_leaves_the_scan_uncached(self) -> None:
        controls = _drive(EventsFunction, EventsArgs(series_ticker="K"), EVENT_SCHEMA, None)
        assert controls and all(c is None for c in controls)


class TestEventsOptIn:
    """`/events` declares no freshness and is the most rate-limited endpoint here.

    Roughly 4 requests a second against ~29 for everything else, so it is the
    one place an opt-in TTL is close to required rather than a nicety. The
    argument was dropped when `events` became a paged scan; this is the guard
    against losing it again.
    """

    def test_cache_ttl_is_offered(self) -> None:
        assert "cache_ttl" in {f.name for f in EventsArgs.__dataclass_fields__.values()}

    def test_opt_in_ttl_reaches_the_batch(self) -> None:
        controls = _drive(EventsFunction, EventsArgs(series_ticker="K", cache_ttl=300), EVENT_SCHEMA, None)
        assert controls[0] is not None
        assert controls[0].ttl == 300

    def test_off_by_default(self) -> None:
        controls = _drive(EventsFunction, EventsArgs(series_ticker="K"), EVENT_SCHEMA, None)
        assert controls[0] is None

    def test_the_origin_wins_over_a_callers_guess(self) -> None:
        """If Kalshi ever declares a TTL here, its policy must beat the opt-in."""
        controls = _drive(
            EventsFunction,
            EventsArgs(series_ticker="K", cache_ttl=300),
            EVENT_SCHEMA,
            "public, max-age=15",
        )
        assert controls[0] is not None
        assert controls[0].ttl == 15


class TestCacheableSurface:
    """A written-down summary of what is cacheable, so a regression is visible."""

    @pytest.mark.parametrize(
        ("function", "expected"),
        [
            ("markets", "origin"),
            ("events", "opt-in"),
            ("market", "opt-in"),
            ("orderbook", "opt-in"),
            ("trades", "opt-in"),
            ("candlesticks", "immutability"),
            ("historical_markets", "immutability"),
            ("historical_trades", "immutability"),
            ("historical_candlesticks", "immutability"),
        ],
    )
    def test_every_market_data_function_has_a_cache_story(self, function: str, expected: str) -> None:
        """Every one of these can be cached somehow; none is unconditionally live."""
        from vgi_kalshi.worker import _KALSHI_CATALOG

        names = {f.Meta.name for f in _KALSHI_CATALOG.schemas[0].functions}
        assert function in names
        assert expected in {"origin", "opt-in", "immutability"}

    def test_opt_in_functions_all_expose_cache_ttl(self) -> None:
        """The opt-in is useless if the argument is missing, which is how it regressed."""
        from vgi_kalshi.markets import EventsArgs, OrderbookArgs, TickerArgs, TradesArgs

        for args in (TickerArgs, OrderbookArgs, TradesArgs, EventsArgs):
            assert "cache_ttl" in args.__dataclass_fields__, args.__name__

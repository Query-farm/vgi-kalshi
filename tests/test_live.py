"""Live tests against the public Kalshi API.

Opt-in: these hit the network, so they are deselected by default. Run with::

    pytest -m live

They assert shapes and invariants rather than values — Kalshi's markets move,
so anything checking a specific price would be flaky by construction.
"""

from __future__ import annotations

import time

import httpx
import pyarrow as pa
import pytest

from vgi_kalshi import kalshi_api as api
from vgi_kalshi.schemas import (
    CANDLESTICK_SCHEMA,
    EVENT_SCHEMA,
    MARKET_SCHEMA,
    ORDERBOOK_SCHEMA,
    SERIES_SCHEMA,
    TRADE_SCHEMA,
    batch_from_rows,
    flatten_candlesticks,
    flatten_orderbook,
)

pytestmark = pytest.mark.live

#: A perpetually-recurring series, so the test does not rot when contracts expire.
SERIES = "KXBTCD"

#: Unauthenticated traffic draws a 429 after only a handful of back-to-back
#: requests, so a suite that runs flat out spends its retry budget on itself and
#: fails tests that have nothing wrong with them. A small pause between tests
#: costs less than the backoff it avoids.
_PACE_SECONDS = 2.0


@pytest.fixture(autouse=True)
def _pace() -> None:
    time.sleep(_PACE_SECONDS)


def _probe(path: str, params: dict[str, object]) -> httpx.Response:
    """A raw request that survives the rate limiter without hiding a 4xx.

    The page-limit tests below deliberately bypass `api._get`: they exist to see
    an endpoint's own 400, which `_get` would be right to surface but which a
    retry loop must not be allowed to mask. They still have to get past a 429 to
    read it, hence the narrow retry here.
    """
    for attempt in range(6):
        response = httpx.get(f"{api.base_url()}{path}", params=params, timeout=30)
        if response.status_code != 429:
            return response
        time.sleep(2**attempt)
    return response


@pytest.fixture(scope="module")
def open_market() -> dict:
    markets = api.markets(SERIES, status="open", limit=1)
    if not markets:
        pytest.skip(f"no open markets in {SERIES} right now")
    return markets[0]


@pytest.fixture(scope="module")
def liquid_book() -> tuple[str, dict]:
    """An open market that actually has resting orders, and its book.

    The orderbook assertions below are all subset-shaped, so they would pass
    vacuously on an empty book — and an empty book is common (a one-sided or
    untraded market returns two empty arrays, as does a ticker that does not
    exist at all). Finding a real book first is what gives them teeth.
    """
    with api.open_client() as client:
        for market in api.markets(SERIES, status="open", limit=10, client=client):
            book = api.orderbook(market["ticker"], client=client)
            if book.get("yes_dollars") or book.get("no_dollars"):
                return market["ticker"], book
    pytest.skip(f"no open market in {SERIES} has resting orders right now")


class TestPublicAccess:
    def test_market_data_needs_no_credentials(self) -> None:
        """The whole market-data surface is public; this is the premise of the worker."""
        assert api.series_list("Crypto")


class TestMarkets:
    def test_batch_matches_declared_schema(self, open_market: dict) -> None:
        batch = batch_from_rows([open_market], MARKET_SCHEMA)
        assert batch.schema == MARKET_SCHEMA
        assert batch.column("ticker").to_pylist() == [open_market["ticker"]]

    def test_prices_are_decimals_not_floats(self, open_market: dict) -> None:
        batch = batch_from_rows([open_market], MARKET_SCHEMA)
        assert pa.types.is_decimal(batch.schema.field("yes_ask_dollars").type)


class TestMarketStatus:
    """Kalshi filters and reports status in two different vocabularies.

    `status => 'open'` returns markets whose `status` column reads `active`, and
    neither word is accepted where the other belongs. A wrong filter is a loud
    400; a wrong predicate is a silently empty result, which is worse — so the
    mapping is pinned here and the shipped examples are checked against it
    offline in `test_catalog.py::TestShippedExamples`.
    """

    #: filter argument -> the status value the returned markets carry.
    MAPPING = [("unopened", "initialized"), ("open", "active"), ("settled", "finalized")]

    @pytest.mark.parametrize(("filter_value", "column_value"), MAPPING)
    def test_filter_maps_to_its_column_value(self, filter_value: str, column_value: str) -> None:
        rows = api.markets(SERIES, status=filter_value, limit=5)
        if not rows:
            pytest.skip(f"no {filter_value} markets in {SERIES} right now")
        assert {row["status"] for row in rows} == {column_value}

    @pytest.mark.parametrize("status", ["active", "finalized", "nonsense"])
    def test_a_column_value_is_not_a_valid_filter(self, status: str) -> None:
        """`finalized` is a real market status but not a filter Kalshi accepts."""
        with pytest.raises(api.KalshiError) as excinfo:
            api.markets(SERIES, status=status, limit=1)
        assert excinfo.value.status == 400


class TestOrderbook:
    def test_levels_flatten_to_rows(self, liquid_book: tuple[str, dict]) -> None:
        ticker, book = liquid_book
        batch = batch_from_rows(flatten_orderbook(ticker, book), ORDERBOOK_SCHEMA)
        assert batch.schema == ORDERBOOK_SCHEMA
        assert batch.num_rows > 0, "a subset assertion on zero rows proves nothing"
        assert set(batch.column("side").to_pylist()) <= {"yes", "no"}
        assert all(price is not None for price in batch.column("price_dollars").to_pylist())

    def test_an_unknown_ticker_is_an_empty_book_not_a_404(self) -> None:
        """Kalshi answers 200 with two empty sides, so emptiness is not existence."""
        assert flatten_orderbook("NOPE-XYZ", api.orderbook("NOPE-XYZ")) == []


class TestBatchedFetches:
    """The batched endpoints must agree with the per-market ones they replace.

    That equivalence is the whole basis for using them: the same query returns
    the same rows, for 1/100th of the request budget.
    """

    def test_batched_books_match_per_market_books(self) -> None:
        markets = api.markets(SERIES, status="open", limit=6)
        if not markets:
            pytest.skip(f"no open markets in {SERIES} right now")
        tickers = [m["ticker"] for m in markets]
        with api.open_client() as client:
            before = {t: api.orderbook(t, client=client) for t in tickers}
            batched = api.orderbooks(tickers, client=client)
            after = {t: api.orderbook(t, client=client) for t in tickers}
        assert set(batched) == set(tickers)
        # A live book can move between reads, so only a value that matches
        # neither surrounding per-market read is a real disagreement.
        mismatched = [t for t in tickers if batched[t] != before[t] and batched[t] != after[t]]
        assert mismatched == [], f"batched books disagree with per-market books: {mismatched}"

    def test_batched_candles_match_per_market_candles(self) -> None:
        """Settled markets, so the candles are closed and cannot move underneath us."""
        markets = api.markets(SERIES, status="settled", limit=3)
        if not markets:
            pytest.skip(f"no settled markets in {SERIES} right now")
        tickers = [m["ticker"] for m in markets]
        now = int(time.time())
        window = {"period_interval": 60, "start_ts": now - 3 * 86_400, "end_ts": now}
        with api.open_client() as client:
            batched = api.batch_candlesticks(tickers, client=client, **window)
            single = {t: api.candlesticks(SERIES, t, client=client, **window) for t in tickers}
        assert batched == single

    def test_the_hundred_ticker_cap_is_still_a_hard_400(self) -> None:
        """If Kalshi raises it this can relax; if it lowers it, this catches it."""
        markets = api.markets(SERIES, limit=api.BATCH_TICKER_LIMIT + 1)
        if len(markets) <= api.BATCH_TICKER_LIMIT:
            pytest.skip("not enough markets to exceed the cap")
        tickers = [m["ticker"] for m in markets][: api.BATCH_TICKER_LIMIT + 1]
        response = _probe("/markets/orderbooks", [("tickers", t) for t in tickers])
        assert response.status_code == 400, "cap moved; update BATCH_TICKER_LIMIT"


class TestCandlesticks:
    def test_series_scoped_path_works(self, open_market: dict) -> None:
        """Regression guard: the documented /markets/{ticker}/candlesticks path 404s."""
        now = int(time.time())
        candles = api.candlesticks(
            SERIES, open_market["ticker"], period_interval=60, start_ts=now - 86_400, end_ts=now
        )
        batch = batch_from_rows(flatten_candlesticks(open_market["ticker"], candles), CANDLESTICK_SCHEMA)
        assert batch.schema == CANDLESTICK_SCHEMA


class TestTrades:
    def test_tape_flattens_to_rows(self, open_market: dict) -> None:
        rows = api.trades(open_market["ticker"], limit=10)
        if not rows:
            pytest.skip("no trades on this market yet")
        batch = batch_from_rows(rows, TRADE_SCHEMA)
        assert batch.schema == TRADE_SCHEMA
        assert batch.num_rows > 0
        assert set(batch.column("taker_side").to_pylist()) <= {"yes", "no"}


class TestEvents:
    def test_events_flatten_to_rows(self) -> None:
        batch = batch_from_rows(api.events(SERIES, limit=5), EVENT_SCHEMA)
        assert batch.schema == EVENT_SCHEMA
        assert batch.num_rows > 0
        assert set(batch.column("series_ticker").to_pylist()) == {SERIES}


class TestStatusPushdown:
    """The status mapping that filter pushdown relies on must stay exact.

    `WHERE status = 'active'` is rewritten to `status => 'open'` before the
    request. If that stops being an exact correspondence the rewrite starts
    dropping rows, and DuckDB cannot recover what was never fetched — so the
    mapping is re-checked against a complete event rather than trusted.
    """

    def test_each_mapped_pair_is_exact(self) -> None:
        from vgi_kalshi.markets import _STATUS_COLUMN_TO_FILTER

        events = api.events(SERIES, limit=1)
        if not events:
            pytest.skip(f"no events in {SERIES} right now")
        event = events[0]["event_ticker"]
        with api.open_client() as client:
            # One event is small enough to fetch whole, so neither side is capped.
            every = api.markets(SERIES, event_ticker=event, client=client)
            for column_value, filter_value in _STATUS_COLUMN_TO_FILTER.items():
                expected = {m["ticker"] for m in every if m["status"] == column_value}
                if not expected:
                    continue
                got = {
                    m["ticker"]
                    for m in api.markets(SERIES, event_ticker=event, status=filter_value, client=client)
                }
                assert expected <= got, (
                    f"status => {filter_value!r} drops markets reading {column_value!r}: "
                    f"{sorted(expected - got)}"
                )


class TestExchangeStatus:
    def test_reports_a_status_per_venue(self) -> None:
        from vgi_kalshi.schemas import EXCHANGE_STATUS_SCHEMA, flatten_exchange_status

        rows = flatten_exchange_status(api.exchange_status())
        batch = batch_from_rows(rows, EXCHANGE_STATUS_SCHEMA)
        assert batch.num_rows > 0
        assert all(isinstance(v, bool) for v in batch.column("exchange_active").to_pylist())

    def test_declares_the_shortest_ttl_in_the_api(self) -> None:
        """The cache policy is forwarded, so it is worth knowing it still exists."""
        hint = api.CacheHint()
        api.exchange_status(hint=hint)
        assert hint.cacheable
        assert hint.max_age == 1


class TestSeries:
    def test_whole_catalog_is_one_response(self) -> None:
        batch = batch_from_rows(api.series_list(), SERIES_SCHEMA)
        assert batch.num_rows > 1000
        assert batch.schema == SERIES_SCHEMA


class TestPageLimits:
    """Kalshi's max page size differs per endpoint, and exceeding it is a 400.

    Regression guard for a real break: `_paged` sent the usual 1000 to
    `/events`, which rejects anything over 200 outright rather than clamping.
    """

    def test_events_pages_successfully(self) -> None:
        assert api.events(SERIES), "events must page at its own 200 cap, not the global 1000"

    def test_events_cap_is_still_200(self) -> None:
        """If Kalshi raises the cap this can relax; if it lowers it, this catches it."""
        ok = _probe("/events", {"series_ticker": SERIES, "limit": 200})
        too_big = _probe("/events", {"series_ticker": SERIES, "limit": 201})
        assert ok.status_code == 200
        assert too_big.status_code == 400, "cap moved; update EVENTS_PAGE_LIMIT"

    def test_markets_still_accepts_1000(self) -> None:
        response = _probe("/markets", {"series_ticker": SERIES, "limit": api.PAGE_LIMIT})
        assert response.status_code == 200

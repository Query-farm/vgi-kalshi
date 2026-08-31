"""Batched fetches: one request per 100 markets instead of one per market.

This is the difference between a LATERAL over a series being affordable and it
being rate-limited into the ground — measured at ~44x on 100 markets. The two
endpoints disagree about how to spell a list of tickers, and one of them fails
by returning a wrong answer rather than an error, so the wire format is pinned
here rather than trusted.
"""

from __future__ import annotations

import httpx
import pytest

from vgi_kalshi import kalshi_api as api
from vgi_kalshi.markets import _cap_depth


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


class TestOrderbooksWireFormat:
    """`tickers` must repeat. Comma-joining is a wrong answer, not an error."""

    def test_tickers_are_repeated_not_joined(self) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.extend(request.url.params.get_list("tickers"))
            return httpx.Response(200, json={"orderbooks": []})

        api.orderbooks(["A", "B", "C"], client=_client(handler))
        assert seen == ["A", "B", "C"], "comma-joined tickers return one empty book for 'A,B,C'"

    def test_books_are_keyed_by_ticker(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "orderbooks": [
                        {"ticker": "A", "orderbook_fp": {"yes_dollars": [["0.4000", "10.00"]]}},
                        {"ticker": "B", "orderbook_fp": {}},
                    ]
                },
            )

        books = api.orderbooks(["A", "B"], client=_client(handler))
        assert set(books) == {"A", "B"}
        assert books["A"]["yes_dollars"] == [["0.4000", "10.00"]]

    def test_chunked_at_the_hundred_ticker_cap(self) -> None:
        """101 tickers in one call is a hard 400, so the client must chunk."""
        sizes: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            sizes.append(len(request.url.params.get_list("tickers")))
            return httpx.Response(200, json={"orderbooks": []})

        api.orderbooks([f"T{i}" for i in range(250)], client=_client(handler))
        assert sizes == [100, 100, 50]
        assert max(sizes) <= api.BATCH_TICKER_LIMIT

    def test_duplicates_collapse_into_one_fetch(self) -> None:
        """A LATERAL can repeat a ticker; the result is a mapping, so fetch once."""
        sizes: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            sizes.append(len(request.url.params.get_list("tickers")))
            return httpx.Response(200, json={"orderbooks": []})

        api.orderbooks(["A", "B", "A", "B", "A"], client=_client(handler))
        assert sizes == [2]

    def test_no_tickers_makes_no_request(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("should not have been called")

        assert api.orderbooks([], client=_client(handler)) == {}


class TestCandlestickBatching:
    def test_tickers_are_comma_joined(self) -> None:
        """The other batch endpoint wants the opposite spelling. Both are Kalshi's."""
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.params["market_tickers"])
            return httpx.Response(200, json={"markets": []})

        api.batch_candlesticks(
            ["A", "B"], period_interval=60, start_ts=0, end_ts=3600, client=_client(handler)
        )
        assert seen == ["A,B"]

    def test_results_are_keyed_by_market_ticker(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"markets": [{"market_ticker": "A", "candlesticks": [{"end_period_ts": 1}]}]},
            )

        found = api.batch_candlesticks(
            ["A"], period_interval=60, start_ts=0, end_ts=3600, client=_client(handler)
        )
        assert found == {"A": [{"end_period_ts": 1}]}


class TestCandlestickBatchSize:
    """The endpoint truncates past 10,000 candles instead of erroring.

    A 100-market batch of one-minute candles over a day would ask for 144,000
    and quietly get a fraction of them, so the batch has to be sized by the
    window rather than by the ticker cap alone.
    """

    DAY = 86_400

    def test_hourly_over_a_day_fits_the_ticker_cap(self) -> None:
        assert api.candlestick_batch_size(period_interval=60, start_ts=0, end_ts=self.DAY) == 100

    def test_minute_candles_shrink_the_batch(self) -> None:
        size = api.candlestick_batch_size(period_interval=1, start_ts=0, end_ts=self.DAY)
        assert size < 100
        assert size * (self.DAY // 60 + 1) <= api.BATCH_CANDLE_LIMIT

    def test_never_drops_below_one_market(self) -> None:
        """A window so wide one market alone exceeds the cap still has to progress."""
        assert api.candlestick_batch_size(period_interval=1, start_ts=0, end_ts=self.DAY * 365) == 1

    def test_batches_respect_the_computed_size(self) -> None:
        sizes: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            sizes.append(len(request.url.params["market_tickers"].split(",")))
            return httpx.Response(200, json={"markets": []})

        api.batch_candlesticks(
            [f"T{i}" for i in range(20)],
            period_interval=1,
            start_ts=0,
            end_ts=self.DAY,
            client=_client(handler),
        )
        expected = api.candlestick_batch_size(period_interval=1, start_ts=0, end_ts=self.DAY)
        assert sizes and max(sizes) == expected
        assert sum(sizes) == 20


class TestDepthCap:
    """`/markets/orderbooks` has no depth parameter, so the cap moved client-side."""

    LEVELS = [
        {"side": "yes", "price_dollars": "0.1000"},
        {"side": "yes", "price_dollars": "0.3000"},
        {"side": "yes", "price_dollars": "0.2000"},
        {"side": "no", "price_dollars": "0.9000"},
        {"side": "no", "price_dollars": "0.8000"},
    ]

    def test_keeps_the_best_levels_per_side(self) -> None:
        kept = _cap_depth(self.LEVELS, 2)
        assert [level["price_dollars"] for level in kept if level["side"] == "yes"] == ["0.3000", "0.2000"]
        assert [level["price_dollars"] for level in kept if level["side"] == "no"] == ["0.9000", "0.8000"]

    def test_depth_is_per_side_not_total(self) -> None:
        assert len(_cap_depth(self.LEVELS, 2)) == 4

    @pytest.mark.parametrize("depth", [10, 100])
    def test_a_depth_beyond_the_book_keeps_everything(self, depth: int) -> None:
        assert len(_cap_depth(self.LEVELS, depth)) == len(self.LEVELS)

"""Conversion tests for Kalshi's fixed-point strings and nested structs."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from vgi_kalshi.schemas import (
    CANDLESTICK_SCHEMA,
    EVENT_SCHEMA,
    MARKET_SCHEMA,
    ORDERBOOK_SCHEMA,
    TRADE_SCHEMA,
    batch_from_rows,
    flatten_candlesticks,
    flatten_orderbook,
    series_of,
    to_decimal,
    to_timestamp,
)


class TestFixedPoint:
    """Money must survive as Decimal, never via float."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("0.7000", Decimal("0.7000")), ("136798.00", Decimal("136798.00")), ("0.0000", Decimal("0"))],
    )
    def test_parses_kalshi_strings(self, raw: str, expected: Decimal) -> None:
        assert to_decimal(raw) == expected

    @pytest.mark.parametrize("raw", [None, "", "not-a-number"])
    def test_unparseable_is_null(self, raw: object) -> None:
        assert to_decimal(raw) is None

    def test_no_float_rounding(self) -> None:
        """0.07 is not representable in binary floating point; the Decimal path is exact."""
        value = to_decimal("0.0700")
        assert value == Decimal("0.0700")
        assert str(value) == "0.0700"


class TestTimestamps:
    def test_rfc3339_z_suffix(self) -> None:
        assert to_timestamp("2026-08-31T03:11:48.484035Z") == datetime(
            2026, 8, 31, 3, 11, 48, 484035, tzinfo=UTC
        )

    def test_epoch_seconds(self) -> None:
        assert to_timestamp(1788141600) == datetime.fromtimestamp(1788141600, tz=UTC)

    def test_null(self) -> None:
        assert to_timestamp(None) is None


class TestOrderbookFlattening:
    def test_both_sides_become_rows(self) -> None:
        book = {"yes_dollars": [["0.4000", "10.00"]], "no_dollars": [["0.5000", "7.00"]]}
        rows = flatten_orderbook("T", book)
        assert [(r["side"], r["price_dollars"]) for r in rows] == [("yes", "0.4000"), ("no", "0.5000")]

    def test_missing_side_contributes_nothing(self) -> None:
        assert len(flatten_orderbook("T", {"yes_dollars": [["0.4000", "1.00"]]})) == 1

    def test_empty_book_is_zero_rows_with_schema(self) -> None:
        batch = batch_from_rows(flatten_orderbook("T", {}), ORDERBOOK_SCHEMA)
        assert batch.num_rows == 0
        assert batch.schema == ORDERBOOK_SCHEMA


class TestCandlestickFlattening:
    def test_empty_price_struct_is_null_not_error(self) -> None:
        """Kalshi sends `"price": {}` for a period with no trades."""
        rows = flatten_candlesticks(
            "T", [{"end_period_ts": 1, "price": {}, "yes_bid": {"close_dollars": "0.3000"}}]
        )
        batch = batch_from_rows(rows, CANDLESTICK_SCHEMA)
        assert batch.column("price_close_dollars").to_pylist() == [None]
        assert batch.column("yes_bid_close_dollars").to_pylist() == [Decimal("0.3000")]


class TestBatchBuilding:
    def test_missing_keys_become_nulls(self) -> None:
        batch = batch_from_rows([{"ticker": "T"}], MARKET_SCHEMA)
        assert batch.num_rows == 1
        assert batch.column("ticker").to_pylist() == ["T"]
        assert batch.column("yes_bid_dollars").to_pylist() == [None]

    def test_schema_is_exactly_declared(self) -> None:
        batch = batch_from_rows([], MARKET_SCHEMA)
        assert batch.schema == MARKET_SCHEMA
        assert pa.types.is_decimal(batch.schema.field("yes_bid_dollars").type)


class TestUnrepresentableDecimals:
    """A value Arrow cannot hold must become NULL, never an exception.

    `to_decimal` was already defensive about garbage, but `Decimal` accepts
    "NaN"/"Infinity" and arbitrary scale quite happily — and pyarrow then raises
    on the whole array, so one odd value from one market would fail every row in
    the batch it arrived in.
    """

    @pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity"])
    def test_non_finite_is_null(self, raw: str) -> None:
        assert to_decimal(raw) is None

    @pytest.mark.parametrize(
        "raw",
        [
            "0.00005",  # finer than the column's 4dp
            "1e40",  # wider than decimal128(18, 4)
            "NaN",
        ],
    )
    def test_unrepresentable_values_become_null(self, raw: str) -> None:
        batch = batch_from_rows([{"yes_bid_dollars": raw}], MARKET_SCHEMA)
        assert batch.column("yes_bid_dollars").to_pylist() == [None]

    @pytest.mark.parametrize("raw", ["0.7000", "0.0001", "12345678901234.5678"])
    def test_everything_that_fits_survives_exactly(self, raw: str) -> None:
        """Rounding to fit would be the silent precision loss we refuse."""
        batch = batch_from_rows([{"yes_bid_dollars": raw}], MARKET_SCHEMA)
        assert batch.column("yes_bid_dollars").to_pylist() == [Decimal(raw)]


class TestSeriesTicker:
    """Kalshi's market payload has no series_ticker, but candlesticks needs one."""

    def test_derived_from_the_event_ticker_prefix(self) -> None:
        assert series_of("KXBTCD-26SEP0417") == "KXBTCD"
        assert series_of("KXMVECROSSCATEGORY-SHARD1-S2026CA8") == "KXMVECROSSCATEGORY"

    @pytest.mark.parametrize("raw", [None, "", "-LEADING"])
    def test_nothing_to_derive_is_null(self, raw: object) -> None:
        assert series_of(raw) is None

    def test_market_schema_carries_the_column(self) -> None:
        batch = batch_from_rows([{"ticker": "T", "series_ticker": "KXBTCD"}], MARKET_SCHEMA)
        assert batch.column("series_ticker").to_pylist() == ["KXBTCD"]


class TestTradesAndEvents:
    def test_trade_row_builds(self) -> None:
        batch = batch_from_rows(
            [
                {
                    "trade_id": "abc",
                    "ticker": "T",
                    "created_time": "2026-08-31T13:01:13.540945Z",
                    "taker_side": "yes",
                    "is_block_trade": False,
                    "yes_price_dollars": "0.0010",
                    "count_fp": "909.09",
                }
            ],
            TRADE_SCHEMA,
        )
        assert batch.column("yes_price_dollars").to_pylist() == [Decimal("0.0010")]
        assert batch.column("count_fp").to_pylist() == [Decimal("909.09")]
        assert batch.column("is_block_trade").to_pylist() == [False]

    def test_event_keeps_settlement_sources_nested(self) -> None:
        """Flattening the list would fan one event out into one row per source."""
        rows = [
            {
                "event_ticker": "KXBTCD-26SEP0417",
                "series_ticker": "KXBTCD",
                "settlement_sources": [{"name": "CF Benchmarks", "url": "https://example.com"}],
            },
            {"event_ticker": "KXBTCD-26SEP0517"},
        ]
        batch = batch_from_rows(rows, EVENT_SCHEMA)
        assert batch.schema == EVENT_SCHEMA
        assert batch.column("settlement_sources").to_pylist() == [
            [{"name": "CF Benchmarks", "url": "https://example.com"}],
            None,
        ]


class TestUnrepresentableTimestamps:
    """A date Python cannot hold must become NULL, never an exception.

    Found by vgi-lint's execute tier, not by any offline test: a far-future
    sentinel arrived as an epoch integer and killed an entire `event` scan with
    "OverflowError: date value out of range". `to_decimal` was made total for
    exactly this reason; `to_timestamp` was not, and the omission was invisible
    until a real payload hit it.
    """

    @pytest.mark.parametrize(
        "raw",
        [
            253402300800,  # year 10000, one second past datetime's ceiling
            99999999999999,  # far beyond it
            -99999999999999,  # and below the floor
        ],
    )
    def test_out_of_range_epoch_is_null(self, raw: int) -> None:
        assert to_timestamp(raw) is None

    def test_a_bad_date_does_not_kill_its_batch(self) -> None:
        """One unrepresentable row must not take the other rows with it."""
        rows = [
            {"event_ticker": "GOOD", "strike_date": "2026-09-04T21:00:00Z"},
            {"event_ticker": "BAD", "strike_date": 253402300800},
        ]
        batch = batch_from_rows(rows, EVENT_SCHEMA)
        assert batch.num_rows == 2
        assert batch.column("strike_date").to_pylist()[1] is None
        assert batch.column("strike_date").to_pylist()[0] is not None

    def test_booleans_are_not_epochs(self) -> None:
        """`bool` is an `int` in Python, so True would otherwise be 1970-01-01."""
        assert to_timestamp(True) is None

    @pytest.mark.parametrize("raw", ["2026-08-31T03:11:48.484035Z", 1788141600])
    def test_representable_values_still_parse(self, raw: object) -> None:
        assert to_timestamp(raw) is not None

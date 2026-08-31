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

    def test_gos_zero_time_is_null_not_year_one(self) -> None:
        """Kalshi's "unset" sentinel is Go's zero time, and it is not a date.

        Found only by running a real payload: an event that has never been
        amended reports `last_updated_ts = '0001-01-01T00:00:00Z'`. Arrow stores
        that happily at microsecond resolution, so nothing here failed — and
        then every nanosecond-resolution consumer (pandas, numpy
        `datetime64[ns]`, any cast to `timestamp[ns]`) raised `OverflowError:
        date value out of range` on materializing the row. Emitting a value the
        caller cannot read is worse than emitting NULL.
        """
        assert to_timestamp("0001-01-01T00:00:00Z") is None

    @pytest.mark.parametrize("raw", ["1500-01-01T00:00:00Z", "2300-01-01T00:00:00Z"])
    def test_values_outside_nanosecond_range_are_null(self, raw: str) -> None:
        assert to_timestamp(raw) is None

    def test_the_result_always_casts_to_nanoseconds(self) -> None:
        """The property that matters: whatever we emit, a client can hold."""
        import pyarrow as pa

        rows = [
            {"event_ticker": "A", "last_updated_ts": "0001-01-01T00:00:00Z"},
            {"event_ticker": "B", "last_updated_ts": "2026-09-04T21:00:00Z"},
            {"event_ticker": "C", "last_updated_ts": 253402300800},
        ]
        column = batch_from_rows(rows, EVENT_SCHEMA).column("last_updated_ts")
        # Raises ArrowInvalid if any value is out of the ns window.
        assert column.cast(pa.timestamp("ns", tz="UTC")).to_pylist()[1] is not None


class TestEveryConversionIsTotal:
    """No single bad value may fail the batch it arrived in.

    This has caused three separate defects — an unrepresentable decimal, Go's
    zero timestamp, a non-integer integer — each found only when a real payload
    hit it, and each fatal to a whole scan rather than to one cell. The property
    is worth stating once for every branch rather than rediscovering per type:
    a value we cannot represent becomes NULL, and its row and every row beside
    it survives.
    """

    JUNK = ["not-a-number", "", None, True, {"nested": 1}, [1, 2], float("nan"), "1.5"]

    @pytest.mark.parametrize("bad", JUNK)
    def test_no_column_type_raises_on_junk(self, bad: object) -> None:
        from vgi_kalshi.schemas import (
            EVENT_SCHEMA,
            HISTORICAL_MARKET_SCHEMA,
            MARKET_SCHEMA,
            SERIES_SCHEMA,
            TRADE_SCHEMA,
        )

        for schema in (
            MARKET_SCHEMA,
            SERIES_SCHEMA,
            EVENT_SCHEMA,
            TRADE_SCHEMA,
            HISTORICAL_MARKET_SCHEMA,
        ):
            row = dict.fromkeys(schema.names, bad)
            batch = batch_from_rows([row], schema)  # must not raise
            assert batch.num_rows == 1

    def test_a_bad_value_does_not_take_its_neighbours(self) -> None:
        from vgi_kalshi.schemas import SERIES_SCHEMA

        rows = [
            {"ticker": "GOOD", "fee_multiplier": 2},
            {"ticker": "BAD", "fee_multiplier": "n/a"},
            {"ticker": "ALSO_GOOD", "fee_multiplier": 3},
        ]
        batch = batch_from_rows(rows, SERIES_SCHEMA)
        assert batch.column("ticker").to_pylist() == ["GOOD", "BAD", "ALSO_GOOD"]
        assert batch.column("fee_multiplier").to_pylist() == [2, None, 3]

    def test_a_malformed_nested_value_is_isolated(self) -> None:
        """A list-of-struct column has no per-value hook, so it is guarded whole."""
        from vgi_kalshi.schemas import EVENT_SCHEMA

        rows = [
            {"event_ticker": "GOOD", "settlement_sources": [{"name": "CF", "url": "u"}]},
            {"event_ticker": "BAD", "settlement_sources": "not a list of structs"},
        ]
        batch = batch_from_rows(rows, EVENT_SCHEMA)
        assert batch.column("event_ticker").to_pylist() == ["GOOD", "BAD"]
        assert batch.column("settlement_sources").to_pylist()[0] == [{"name": "CF", "url": "u"}]
        assert batch.column("settlement_sources").to_pylist()[1] is None

    def test_integers_reject_booleans(self) -> None:
        """`bool` is an `int`; True must not silently become 1."""
        from vgi_kalshi.schemas import to_integer

        assert to_integer(True) is None
        assert to_integer(3) == 3


class TestStrikeColumns:
    """The structured form of a contract's threshold.

    `subtitle` says "101.0 or above" in prose; these say it in numbers. Without
    them a strike ladder cannot be analysed without regexing the ticker, which
    is what prompted adding them — an implied-probability curve is the main
    quantitative use of this data and it needs the strike as a value.
    """

    #: Kalshi's three shapes, and which bound each one sets.
    SHAPES = [
        ("greater", 90.99, None),
        ("less", None, 71),
        ("between", 70.0, 71.0),
    ]

    @pytest.mark.parametrize(("strike_type", "floor", "cap"), SHAPES)
    def test_each_strike_shape_round_trips(
        self, strike_type: str, floor: float | None, cap: float | None
    ) -> None:
        row = {"ticker": "T", "strike_type": strike_type, "floor_strike": floor, "cap_strike": cap}
        batch = batch_from_rows([row], MARKET_SCHEMA)
        assert batch.column("strike_type").to_pylist() == [strike_type]
        assert batch.column("floor_strike").to_pylist() == [None if floor is None else Decimal(str(floor))]
        assert batch.column("cap_strike").to_pylist() == [None if cap is None else Decimal(str(cap))]

    def test_a_strike_is_exact_not_a_float(self) -> None:
        """Kalshi sends these as JSON numbers; they must not stay floats.

        A ladder is grouped and joined on its strike, and float equality on
        87299.99 does not hold. Decimal is what makes `GROUP BY floor_strike`
        and a cross-venue join on strike behave.
        """
        batch = batch_from_rows([{"floor_strike": 87299.99}], MARKET_SCHEMA)
        value = batch.column("floor_strike").to_pylist()[0]
        assert value == Decimal("87299.99")
        assert isinstance(value, Decimal)

    def test_an_integer_strike_is_kept_exactly(self) -> None:
        """Not every series prices in dollars — EIA inventories strike on 435."""
        batch = batch_from_rows([{"floor_strike": 435}], MARKET_SCHEMA)
        assert batch.column("floor_strike").to_pylist() == [Decimal("435")]

    def test_the_archive_carries_them_too(self) -> None:
        """Settled ladders are the ones worth backtesting against."""
        from vgi_kalshi.schemas import HISTORICAL_MARKET_SCHEMA

        for column in ("strike_type", "floor_strike", "cap_strike"):
            assert column in HISTORICAL_MARKET_SCHEMA.names

    def test_a_junk_strike_is_null_not_fatal(self) -> None:
        batch = batch_from_rows([{"floor_strike": "not-a-number"}], MARKET_SCHEMA)
        assert batch.column("floor_strike").to_pylist() == [None]

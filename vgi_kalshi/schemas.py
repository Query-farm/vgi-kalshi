"""Arrow schemas and Kalshi's fixed-point conversions.

Kalshi sends money and contract counts as decimal *strings*, in two flavours:

* ``*_dollars`` — prices, 4 decimal places (``"0.7000"``)
* ``*_fp`` — contract counts and volumes, 2 decimal places (``"136798.00"``)

Both map to Arrow ``decimal128``. Parsing them into ``float`` would be a silent
precision loss on values that arrive exact, so every conversion here goes through
:class:`decimal.Decimal`.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

import pyarrow as pa

from vgi_kalshi.meta import field

#: Prices: 4dp, as Kalshi sends them.
DOLLARS = pa.decimal128(18, 4)

#: Contract counts and volumes: 2dp.
COUNT = pa.decimal128(18, 2)

#: Kalshi timestamps are RFC 3339 with a trailing ``Z``.
TIMESTAMP = pa.timestamp("us", tz="UTC")

#: The window a nanosecond-resolution consumer can hold. Arrow stores these
#: columns at microsecond resolution, which spans most of recorded time, but
#: pandas, numpy ``datetime64[ns]`` and anything casting to ``timestamp[ns]``
#: are limited to 1677-2262 — and a value outside it does not degrade, it
#: raises ``OverflowError: date value out of range`` when the client
#: materializes the result. Emitting one is therefore a query the caller cannot
#: read, which is worse than emitting NULL.
_NS_FLOOR = datetime(1678, 1, 1, tzinfo=UTC)
_NS_CEILING = datetime(2262, 1, 1, tzinfo=UTC)


def to_decimal(value: Any) -> Decimal | None:
    """Parse a Kalshi fixed-point string into a Decimal, or None if absent/unparseable.

    ``Decimal`` happily constructs ``NaN`` and ``Infinity`` from strings, which
    Arrow then rejects; both are filtered out here so a single odd value cannot
    fail the batch it arrived in.
    """
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def fits_decimal(value: Decimal, kind: pa.Decimal128Type) -> bool:
    """Whether ``value`` is exactly representable in ``kind``.

    Arrow raises on a value that needs more scale ("Rescaling Decimal value
    would cause data loss") or more precision than the column declares, and one
    such value would take down the whole scan. Rounding it to fit would be the
    silent precision loss this module exists to avoid, so a value that does not
    fit becomes NULL instead — visible in the result, and not fatal.
    """
    shifted = value.scaleb(kind.scale)
    if shifted != shifted.to_integral_value():
        return False
    return abs(shifted) < Decimal(10) ** kind.precision


def series_of(event_ticker: Any) -> str | None:
    """The series ticker an event belongs to, read off the event ticker's prefix.

    Kalshi's market payload carries ``event_ticker`` but no ``series_ticker``,
    and event tickers are ``{series}-{suffix}`` (``KXBTCD-26SEP0417``). Used
    only where the series is not already known from the call itself — see
    :class:`~vgi_kalshi.markets.MarketFunction`.
    """
    if not event_ticker:
        return None
    return str(event_ticker).split("-")[0] or None


def to_timestamp(value: Any) -> datetime | None:
    """Parse an RFC 3339 string, or an epoch-seconds number, into an aware UTC datetime.

    Total by construction, like :func:`to_decimal`: anything unrepresentable
    becomes NULL rather than raising, and "representable" is judged by what a
    *consumer* can hold, not by what Python can parse.

    Two distinct failures live here, both found by running real payloads rather
    than by any offline test. Parsing can raise — ``ValueError`` or
    ``OverflowError`` depending on platform, and ``OSError`` from
    ``fromtimestamp`` on extreme input — so all three are caught. And a value
    that parses fine can still be unusable: Kalshi sends Go's zero time
    (``0001-01-01T00:00:00Z``) to mean "unset", which Arrow stores happily at
    microsecond resolution and which then raises ``OverflowError`` in any
    nanosecond-resolution client that materializes it. See :data:`_NS_FLOOR`.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(float(value), tz=UTC)
        except (ValueError, OverflowError, OSError):
            return None
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
    # Kalshi sends Go's zero time, 0001-01-01T00:00:00Z, to mean "unset" — most
    # visibly as `last_updated_ts` on an event that has never been amended. That
    # is not a date, and carrying it through hands the caller a row they cannot
    # materialize. NULL is what it means and what every client can hold.
    if not _NS_FLOOR <= parsed <= _NS_CEILING:
        return None
    return parsed


def to_integer(value: Any) -> int | None:
    """Parse an integer, or None when the value is not one.

    Total for the same reason :func:`to_decimal` and :func:`to_timestamp` are:
    a single odd value in a single row must not fail the batch it arrived in.
    ``int("1.5")`` raises, and Kalshi's schema is not a contract we control.
    """
    if value is None or isinstance(value, bool) or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def column(rows: Sequence[dict[str, Any]], key: str, field: pa.Field) -> pa.Array:
    """Extract ``key`` from every row and build the Arrow array ``field`` declares.

    The conversion is chosen from the field's declared type, so a schema edit is
    the only thing needed to change how a column is parsed.

    Every branch is total. One malformed value from one market must never fail
    the whole batch — every row beside it would be lost, and the caller would
    see an exception rather than a mostly-good result with a NULL in it. This
    has now been the cause of three separate production defects (an
    unrepresentable decimal, Go's zero timestamp, a non-integer integer), so the
    nested fallback is guarded too rather than waiting to become the fourth.
    """
    values = [row.get(key) for row in rows]
    if pa.types.is_decimal(field.type):
        kind = cast("pa.Decimal128Type", field.type)
        parsed = (to_decimal(v) for v in values)
        return pa.array(
            [d if d is not None and fits_decimal(d, kind) else None for d in parsed], type=field.type
        )
    if pa.types.is_timestamp(field.type):
        return pa.array([to_timestamp(v) for v in values], type=field.type)
    if pa.types.is_boolean(field.type):
        return pa.array([None if v is None else bool(v) for v in values], type=field.type)
    if pa.types.is_integer(field.type):
        return pa.array([to_integer(v) for v in values], type=field.type)
    if pa.types.is_string(field.type):
        return pa.array([None if v is None else str(v) for v in values], type=field.type)
    # Lists and structs: Arrow validates the shape, and there is no per-value
    # conversion to interpose. Build the whole column, and if the payload does
    # not match the declared type, fall back to isolating the rows that do —
    # a nested column is worth losing a value over, not a scan.
    try:
        return pa.array(values, type=field.type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        return pa.array([_nullable_nested(v, field.type) for v in values], type=field.type)


def _nullable_nested(value: Any, kind: pa.DataType) -> Any:
    """``value`` if Arrow accepts it alone, else None."""
    if value is None:
        return None
    try:
        pa.array([value], type=kind)
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        return None
    return value


def batch_from_rows(rows: Sequence[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build one RecordBatch by pulling each schema field out of ``rows`` by name."""
    return pa.RecordBatch.from_arrays([column(rows, field.name, field) for field in schema], schema=schema)


MARKET_SCHEMA = pa.schema(
    [
        field("ticker", pa.string(), "Unique market ticker; the key every other market-data function takes."),
        field(
            "series_ticker",
            pa.string(),
            "Series this market belongs to. Not in Kalshi's payload — stamped on by the "
            "function that produced the row, so this output can drive candlesticks().",
        ),
        field(
            "event_ticker",
            pa.string(),
            "Event grouping this market with the other strikes resolving on the same date.",
        ),
        field("market_type", pa.string(), "Contract mechanism, e.g. 'binary' or 'scalar'."),
        field(
            "title",
            pa.string(),
            "Full question the contract settles, e.g. 'BTC price on Sep 4, 2026 at 5pm EDT?'.",
        ),
        field("subtitle", pa.string(), "Secondary heading distinguishing this strike within its event."),
        field("yes_sub_title", pa.string(), "Label describing what a YES holder is betting on."),
        field("no_sub_title", pa.string(), "Label describing what a NO holder is betting on."),
        field(
            "status",
            pa.string(),
            "Lifecycle state: initialized, active, closed, determined, settled or finalized. "
            "Note this is NOT the vocabulary the status filter argument accepts — filter with "
            "'open' to select markets reading 'active'.",
        ),
        field("open_time", TIMESTAMP, "When the market opened, or will open, for trading."),
        field("close_time", TIMESTAMP, "When trading closes and the outcome is locked in."),
        field("expiration_time", TIMESTAMP, "When the contract expires and settlement is final."),
        field("yes_bid_dollars", DOLLARS, "Best resting bid for YES, in dollars per contract (0 to 1)."),
        field("yes_ask_dollars", DOLLARS, "Best resting ask for YES, in dollars per contract (0 to 1)."),
        field("no_bid_dollars", DOLLARS, "Best resting bid for NO, in dollars per contract (0 to 1)."),
        field("no_ask_dollars", DOLLARS, "Best resting ask for NO, in dollars per contract (0 to 1)."),
        field("last_price_dollars", DOLLARS, "Price of the most recent trade, in dollars per contract."),
        field("previous_price_dollars", DOLLARS, "Trade price 24 hours ago, for a day-over-day comparison."),
        field("yes_bid_size_fp", COUNT, "Contracts resting at the best YES bid."),
        field("yes_ask_size_fp", COUNT, "Contracts resting at the best YES ask."),
        field("volume_fp", COUNT, "Contracts traded over this market's whole lifetime."),
        field(
            "volume_24h_fp",
            COUNT,
            "Contracts traded in the last 24 hours; the liquidity signal worth filtering on.",
        ),
        field(
            "open_interest_fp",
            COUNT,
            "Contracts currently outstanding (open positions, not cumulative volume).",
        ),
        field("liquidity_dollars", DOLLARS, "Total dollar value resting in the order book on both sides."),
        field("notional_value_dollars", DOLLARS, "Dollar value one contract pays out when it settles YES."),
        field("result", pa.string(), "Settled outcome ('yes' or 'no'); empty until the market settles."),
        field(
            "can_close_early",
            pa.bool_(),
            "Whether Kalshi may close this market before its scheduled close time.",
        ),
    ]
)

#: One row per price level per side, flattened out of the nested ``orderbook_fp``
#: object so it joins and aggregates like an ordinary table.
ORDERBOOK_SCHEMA = pa.schema(
    [
        field("ticker", pa.string(), "Market this price level belongs to."),
        field("side", pa.string(), "Which side of the contract rests here: 'yes' or 'no'."),
        field("price_dollars", DOLLARS, "Price of this level, in dollars per contract (0 to 1)."),
        field("count_fp", COUNT, "Contracts resting at this price level."),
    ]
)

#: Nested OHLC structs (``yes_bid``, ``yes_ask``, ``price``) flattened to columns.
CANDLESTICK_SCHEMA = pa.schema(
    [
        field("ticker", pa.string(), "Market these candles cover."),
        field(
            "end_period_ts",
            TIMESTAMP,
            "Closing instant of this candle's period; the column to order and join on.",
        ),
        field("volume_fp", COUNT, "Contracts traded during this period."),
        field("open_interest_fp", COUNT, "Contracts outstanding at the end of this period."),
        field(
            "price_open_dollars",
            DOLLARS,
            "First trade price in the period; NULL when the period had no trades.",
        ),
        field(
            "price_high_dollars",
            DOLLARS,
            "Highest trade price in the period; NULL when the period had no trades.",
        ),
        field(
            "price_low_dollars",
            DOLLARS,
            "Lowest trade price in the period; NULL when the period had no trades.",
        ),
        field(
            "price_close_dollars",
            DOLLARS,
            "Last trade price in the period; NULL when the period had no trades.",
        ),
        field("yes_bid_open_dollars", DOLLARS, "Best YES bid at the start of the period."),
        field("yes_bid_high_dollars", DOLLARS, "Highest best-YES-bid seen during the period."),
        field("yes_bid_low_dollars", DOLLARS, "Lowest best-YES-bid seen during the period."),
        field("yes_bid_close_dollars", DOLLARS, "Best YES bid at the end of the period."),
        field("yes_ask_open_dollars", DOLLARS, "Best YES ask at the start of the period."),
        field("yes_ask_high_dollars", DOLLARS, "Highest best-YES-ask seen during the period."),
        field("yes_ask_low_dollars", DOLLARS, "Lowest best-YES-ask seen during the period."),
        field("yes_ask_close_dollars", DOLLARS, "Best YES ask at the end of the period."),
    ]
)

#: The public trade tape. Each row is one executed trade; ``taker_side`` is the
#: side the aggressor took, and the two price columns are the same trade quoted
#: from each side of the contract.
TRADE_SCHEMA = pa.schema(
    [
        field("trade_id", pa.string(), "Kalshi's unique identifier for this execution."),
        field("ticker", pa.string(), "Market the trade executed on."),
        field("created_time", TIMESTAMP, "When the trade executed; the cursor to poll on with min_ts."),
        field("taker_side", pa.string(), "Contract side the aggressor bought: 'yes' or 'no'."),
        field("taker_outcome_side", pa.string(), "Outcome the aggressor took on, as Kalshi labels it."),
        field("taker_book_side", pa.string(), "Book side the aggressor hit: 'bid' or 'ask'."),
        field(
            "is_block_trade",
            pa.bool_(),
            "Whether this was a negotiated block trade rather than a book execution.",
        ),
        field(
            "yes_price_dollars", DOLLARS, "Execution price quoted from the YES side, in dollars per contract."
        ),
        field("no_price_dollars", DOLLARS, "The same execution quoted from the NO side; the two sum to 1."),
        field("count_fp", COUNT, "Contracts exchanged in this trade."),
    ]
)

#: Events group the markets under a series (one strike date, many strikes).
#: ``settlement_sources`` stays nested — it is a genuine list per event, and
#: flattening it would fan every event out into one row per source.
EVENT_SCHEMA = pa.schema(
    [
        field(
            "event_ticker",
            pa.string(),
            "Unique event ticker; the value markets() accepts as its event_ticker filter.",
        ),
        field("series_ticker", pa.string(), "Series this event belongs to."),
        field("title", pa.string(), "Question the event asks, e.g. 'BTC price on Sep 4, 2026 at 5pm EDT?'."),
        field("sub_title", pa.string(), "Short form of the event's resolution time."),
        field("category", pa.string(), "Kalshi's top-level category, e.g. 'Crypto' or 'Politics'."),
        field("collateral_return_type", pa.string(), "How collateral is returned when the event resolves."),
        field(
            "mutually_exclusive", pa.bool_(), "Whether exactly one market under this event can settle YES."
        ),
        field("strike_date", TIMESTAMP, "When the event's outcome is determined."),
        field(
            "strike_period", pa.string(), "Period label used instead of a strike date for recurring events."
        ),
        field(
            "available_on_brokers",
            pa.bool_(),
            "Whether the event is offered through Kalshi's broker partners.",
        ),
        field("last_updated_ts", TIMESTAMP, "When Kalshi last modified this event's metadata."),
        field(
            "settlement_sources",
            pa.list_(pa.struct([("name", pa.string()), ("url", pa.string())])),
            "Sources Kalshi settles the event against, each a {name, url} struct.",
        ),
    ]
)

#: One row per exchange index. Kalshi runs several trading venues under one
#: exchange (Default, Combos, Crypto, sports), and they open and close
#: independently, so "is the exchange open" is a per-index question.
EXCHANGE_STATUS_SCHEMA = pa.schema(
    [
        field(
            "exchange_index",
            pa.int64(),
            "Kalshi's numeric id for this trading venue (0 = Default, 1 = Combos, "
            "2 = Crypto, ...); a stable identifier, not a count or an ordering.",
        ),
        field("description", pa.string(), "Human label for the venue, e.g. 'Default', 'Crypto'."),
        field("exchange_active", pa.bool_(), "Whether this venue is up at all."),
        field("trading_active", pa.bool_(), "Whether orders can be placed on this venue right now."),
        field(
            "intra_exchange_transfers_active",
            pa.bool_(),
            "Whether positions can be transferred within this venue.",
        ),
        field(
            "exchange_active_overall",
            pa.bool_(),
            "The exchange-wide flag, repeated on every row for a filter that ignores venues.",
        ),
        field(
            "trading_active_overall",
            pa.bool_(),
            "The exchange-wide trading flag, repeated on every row.",
        ),
    ]
)

#: Descriptive metadata for one event: the sources it settles against and the
#: images Kalshi shows for it. Separate from `events()` because it is a
#: different endpoint and a different shape — one row per settlement source.
EVENT_METADATA_SCHEMA = pa.schema(
    [
        field("event_ticker", pa.string(), "Event this metadata describes."),
        field("settlement_source_name", pa.string(), "Name of a source the event settles against."),
        field("settlement_source_url", pa.string(), "Link to that source."),
        field("image_url", pa.string(), "Event image Kalshi displays."),
        field("featured_image_url", pa.string(), "Larger featured image, when Kalshi has one."),
    ]
)

SERIES_SCHEMA = pa.schema(
    [
        field(
            "ticker",
            pa.string(),
            "Unique series ticker; the key markets(), events() and candlesticks() take.",
        ),
        field("title", pa.string(), "Human-readable name of the series, e.g. 'Bitcoin price'."),
        field(
            "category", pa.string(), "Kalshi's top-level category, e.g. 'Crypto', 'Politics' or 'Economics'."
        ),
        field(
            "frequency", pa.string(), "How often new events open under this series, e.g. 'daily' or 'weekly'."
        ),
        field("fee_type", pa.string(), "Fee schedule this series trades under."),
        field(
            "fee_multiplier",
            pa.int64(),
            "Dimensionless multiplier applied to the base trading fee for this series "
            "(1 = the standard fee; higher values scale it up).",
        ),
        field("contract_url", pa.string(), "Link to the contract specification on kalshi.com."),
        field("contract_terms_url", pa.string(), "Link to the full legal terms for the contract."),
        field("tags", pa.list_(pa.string()), "Kalshi's own free-form topic labels for this series."),
        field("last_updated_ts", TIMESTAMP, "When Kalshi last modified this series definition."),
    ]
)


def flatten_candlesticks(ticker: str, candles: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten Kalshi's nested candlestick OHLC structs into flat rows.

    ``price`` arrives as ``{}`` for a period with no trades, so every nested
    lookup tolerates a missing struct and yields NULL rather than raising.
    """

    def ohlc(candle: dict[str, Any], group: str) -> dict[str, Any]:
        block = candle.get(group) or {}
        return {
            f"{group}_open_dollars": block.get("open_dollars"),
            f"{group}_high_dollars": block.get("high_dollars"),
            f"{group}_low_dollars": block.get("low_dollars"),
            f"{group}_close_dollars": block.get("close_dollars"),
        }

    rows: list[dict[str, Any]] = []
    for candle in candles:
        row: dict[str, Any] = {
            "ticker": ticker,
            "end_period_ts": candle.get("end_period_ts"),
            "volume_fp": candle.get("volume_fp"),
            "open_interest_fp": candle.get("open_interest_fp"),
        }
        for group in ("price", "yes_bid", "yes_ask"):
            row.update(ohlc(candle, group))
        rows.append(row)
    return rows


def flatten_orderbook(ticker: str, book: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten ``orderbook_fp`` into one row per (side, price level).

    Kalshi nests levels as ``{"yes_dollars": [[price, count], ...], "no_dollars": ...}``;
    an absent side simply contributes no rows.
    """
    rows: list[dict[str, Any]] = []
    for key, side in (("yes_dollars", "yes"), ("no_dollars", "no")):
        for level in book.get(key) or []:
            if not level:
                continue
            price = level[0]
            count = level[1] if len(level) > 1 else None
            rows.append({"ticker": ticker, "side": side, "price_dollars": price, "count_fp": count})
    return rows


def flatten_exchange_status(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the exchange status into one row per trading venue.

    The payload carries exchange-wide flags alongside a list of per-index
    statuses. The wide flags are repeated on every row so a query can ask
    "is trading open" without knowing the venue layout.
    """
    overall = {
        "exchange_active_overall": payload.get("exchange_active"),
        "trading_active_overall": payload.get("trading_active"),
    }
    indexes = payload.get("exchange_index_statuses") or []
    if not indexes:
        # An exchange with no per-index breakdown still has an answer.
        return [
            {
                **overall,
                "exchange_active": payload.get("exchange_active"),
                "trading_active": payload.get("trading_active"),
                "intra_exchange_transfers_active": payload.get("intra_exchange_transfers_active"),
            }
        ]
    return [{**index, **overall} for index in indexes]


def flatten_event_metadata(event_ticker: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """One row per settlement source, carrying the event's images alongside.

    An event with no declared sources still yields one row, so a lookup by
    ticker always returns its images rather than nothing.
    """
    images = {
        "event_ticker": event_ticker,
        "image_url": payload.get("image_url"),
        "featured_image_url": payload.get("featured_image_url"),
    }
    sources = payload.get("settlement_sources") or []
    if not sources:
        return [images]
    return [
        {**images, "settlement_source_name": s.get("name"), "settlement_source_url": s.get("url")}
        for s in sources
    ]


#: Archived markets carry everything a live market does, plus what the contract
#: actually paid out. Derived from MARKET_SCHEMA rather than restated, so a
#: column documented once stays documented in both.
HISTORICAL_MARKET_SCHEMA = pa.schema(
    [
        *MARKET_SCHEMA,
        field(
            "settlement_value_dollars",
            DOLLARS,
            "What one contract paid at settlement, in dollars (1 for the winning side, 0 "
            "for the losing one; a scalar market can settle in between).",
        ),
    ]
)

#: The live/archive boundary, one row. Four timestamps rather than one because
#: Kalshi archives each kind of data on its own schedule.
HISTORICAL_CUTOFF_SCHEMA = pa.schema(
    [
        field(
            "market_settled_ts",
            TIMESTAMP,
            "Markets that settled before this are in the archive, not in markets().",
        ),
        field(
            "trades_created_ts",
            TIMESTAMP,
            "Trades executed before this are in the archive, not in trades().",
        ),
        field(
            "orders_updated_ts",
            TIMESTAMP,
            "Order archive boundary. Informational here: this worker exposes no order data.",
        ),
        field(
            "market_positions_last_updated_ts",
            TIMESTAMP,
            "Position archive boundary. Informational here: this worker exposes no positions.",
        ),
    ]
)

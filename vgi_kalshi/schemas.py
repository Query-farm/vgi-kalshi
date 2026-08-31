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
    """Parse an RFC 3339 string, or an epoch-seconds int, into an aware UTC datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def column(rows: Sequence[dict[str, Any]], key: str, field: pa.Field) -> pa.Array:
    """Extract ``key`` from every row and build the Arrow array ``field`` declares.

    The conversion is chosen from the field's declared type, so a schema edit is
    the only thing needed to change how a column is parsed.
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
        return pa.array([None if v is None else int(v) for v in values], type=field.type)
    if pa.types.is_string(field.type):
        return pa.array([None if v is None else str(v) for v in values], type=field.type)
    return pa.array(values, type=field.type)


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

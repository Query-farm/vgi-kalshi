"""Keyed market-data functions: markets, market, orderbook, candlesticks, trades, events.

All six are **blended** (:class:`~vgi.table_in_out_function.RowTransformFunction`)
table functions. Their positional arguments *are* the per-row input columns, so a
single registration serves both a literal call and a correlated LATERAL::

    SELECT * FROM kalshi.main.orderbook('KXBTCD-26SEP0417-T90000.00');

    SELECT m.ticker, o.side, o.price_dollars, o.count_fp
    FROM kalshi.main.markets('KXBTCD') m,
         LATERAL kalshi.main.orderbook(m.ticker) o;

Three constraints from the blended contract shape these signatures:

* No ``finalize``/``finish`` — DuckDB forbids ``FinalExecute`` under correlated
  LATERAL, so nothing here may accumulate across batches.
* Positional args are read off ``batch`` by declared name; they are **not**
  surfaced on ``params.args``.
* A positional ``const`` arg is rejected, so every optional knob (``depth``,
  ``period_interval``, the time window) is a *named* arg instead.

Every one of them takes a required key rather than defaulting to the whole
exchange. An unfiltered scan of ``/markets`` or ``/markets/trades`` pages past
400,000 rows, most of them zero-volume ``KXMVECROSSCATEGORY`` parlay combos, and
takes minutes; requiring the key keeps a naive ``SELECT *`` honest.

Each function is 1->N: one input row fans out to many output rows, so every
``emit`` carries ``parent_rows`` provenance mapping each output row back to the
input row that produced it. Without it the batched-LATERAL operator cannot stamp
the correlated columns onto the right rows.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, cast

import pyarrow as pa
from vgi.arguments import Arg, SecretLookupEntry
from vgi.cache_control import CacheControl
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi.table_in_out_function import RowTransformFunction
from vgi_rpc.rpc import OutputCollector

from vgi_kalshi import auth
from vgi_kalshi import kalshi_api as api
from vgi_kalshi.kalshi_api import STALE_IF_ERROR, CacheHint
from vgi_kalshi.meta import docs, examples
from vgi_kalshi.paging import PagedScanState, emit_page
from vgi_kalshi.schemas import (
    CANDLESTICK_SCHEMA,
    EVENT_METADATA_SCHEMA,
    EVENT_SCHEMA,
    MARKET_SCHEMA,
    ORDERBOOK_SCHEMA,
    TRADE_SCHEMA,
    batch_from_rows,
    flatten_candlesticks,
    flatten_event_metadata,
    flatten_orderbook,
    series_of,
    to_decimal,
)

if TYPE_CHECKING:
    from vgi.protocol import VgiOutputCollector

#: One day, the default candlestick window when the caller gives no bounds.
_DAY_SECONDS = 86_400

#: A window whose candles have all closed can never change again, so it is
#: cached for a day rather than for the origin's (absent) freshness directive.
_CLOSED_WINDOW_TTL = 86_400


def _origin_cache_control(hint: CacheHint) -> CacheControl | None:
    """Translate the origin's own Cache-Control into VGI cache metadata.

    Kalshi declares ``max-age`` on reference endpoints and declares nothing at
    all on live market data. Forwarding its actual policy — rather than
    inventing a TTL — means the result cache follows the exchange automatically
    if Kalshi ever changes it. ``None`` means "the origin called this live", and
    the result is left uncached.
    """
    if not hint.cacheable:
        return None
    return CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR)


def _candlestick_cache_control(*, end_ts: int, period_interval: int, now: int) -> CacheControl | None:
    """Cacheability for a candlestick window, decided by whether it is closed.

    A candle is immutable once its period has elapsed, so a window that ends
    before the current period began can never change and is safely cached for a
    long time. A window that runs up to now contains a candle that is still
    forming, and is not cached at all — Kalshi declares no freshness for this
    endpoint, and a half-built candle is exactly the thing you do not want
    served from a cache.
    """
    period_seconds = max(period_interval, 1) * 60
    if end_ts > now - period_seconds:
        return None
    return CacheControl(ttl=_CLOSED_WINDOW_TTL, stale_if_error=STALE_IF_ERROR, per_value=True)


def _opt_in_cache_control(ttl: int, *, per_value: bool) -> CacheControl | None:
    """Cache metadata for an endpoint the origin does **not** declare cacheable.

    Kalshi sends no freshness directive on quotes or books, so nothing here is
    cached unless the caller explicitly asks with ``cache_ttl``. That opt-in
    exists because a correlated LATERAL over a whole series issues one request
    per market: 318 markets at the Basic tier's 200 read tokens/second, 10
    tokens a call, is ~16 seconds of pure rate limiting. ``per_value``
    memoization turns a repeat of the same ticker into a cache hit — but it
    trades freshness for it, which is why the default is off.
    """
    if ttl <= 0:
        return None
    return CacheControl(ttl=ttl, stale_if_error=STALE_IF_ERROR, per_value=per_value)


#: Kalshi's `status` filter vocabulary is not the vocabulary its `status` column
#: reports, so a `WHERE status = 'active'` predicate can only be pushed down
#: through an explicit mapping. Only pairs verified against the live API are
#: listed: for one complete event, `status => 'open'` returned exactly the 50
#: markets whose column read `active`, dropping none and adding none
#: (`tests/test_live.py::TestStatusPushdown` re-checks this).
#:
#: Absent entries are deliberate. `closed` and `determined` appear in one
#: vocabulary or the other but their correspondence is unverified, and pushing
#: an unproven mapping would silently *drop* rows — the one failure a pushdown
#: must never have, since DuckDB re-applies the predicate to what we return but
#: cannot recover what we never fetched.
_STATUS_COLUMN_TO_FILTER = {
    "active": "open",
    "initialized": "unopened",
    "finalized": "settled",
}


def _pushed_market_filters(params: ProcessParams[MarketsArgs]) -> tuple[str | None, str | None]:
    """Turn a pushed-down WHERE into Kalshi query parameters, conservatively.

    Returns ``(event_ticker, status)`` to add to the request. Both are pure
    optimizations: DuckDB re-applies the predicate to whatever comes back, so a
    filter we decline to push costs bandwidth, never correctness. Pushing one
    that is *wrong* is the dangerous direction, which is why only exact
    correspondences are used.

    An explicit named argument always wins over an inferred one — the caller
    said what they wanted, and the predicate will narrow the result anyway.
    """
    filters = params.current_pushdown_filters
    if filters is None:
        return None, None

    def constant(column: str) -> str | None:
        scalar = filters.get_column_constant(column)
        value = scalar.as_py() if scalar is not None else None
        return str(value) if isinstance(value, str) and value else None

    event = None if params.args.event_ticker else constant("event_ticker")
    status = None
    if not params.args.status and (reported := constant("status")):
        # `event_ticker` needs no translation; `status` does, and only for pairs
        # proven equivalent.
        status = _STATUS_COLUMN_TO_FILTER.get(reported)
    return event, status


def _cap_depth(levels: list[dict[str, Any]], depth: int) -> list[dict[str, Any]]:
    """Keep only the best ``depth`` levels per side.

    ``/markets/orderbooks`` takes no depth parameter, unlike the per-market
    endpoint it replaces, so the cap is applied here instead. "Best" is the top
    of each side's book: highest price for the buyers resting on ``yes``, and
    likewise for ``no`` — each side's list is quoted from its own perspective.
    """
    out: list[dict[str, Any]] = []
    for side in ("yes", "no"):
        ranked = sorted(
            (level for level in levels if level["side"] == side),
            key=lambda level: to_decimal(level["price_dollars"]) or 0,
            reverse=True,
        )
        out.extend(ranked[:depth])
    return out


def _emit_fanout(
    out: OutputCollector,
    schema: pa.Schema,
    rows: Sequence[dict[str, Any]],
    parent_rows: Sequence[int],
    cache_control: CacheControl | None = None,
) -> None:
    """Emit a 1->N batch with per-output-row provenance and optional cacheability.

    ``parent_rows[i]`` is the index, within this call's input batch, of the row
    that produced output row ``i``. Cache metadata rides on the first emitted
    batch, which is this one — each of these functions emits exactly once.

    ``schema`` is the caller's ``params.output_schema``, which is the *projected*
    schema: with ``projection_pushdown`` declared, a ``SELECT ticker`` narrows it
    to one column and only that column is built. Rows are dicts, so the columns
    that were projected away simply are not read.
    """
    batch = batch_from_rows(rows, schema)
    cast("VgiOutputCollector", out).emit(batch, parent_rows=list(parent_rows), cache_control=cache_control)


@dataclass(slots=True, frozen=True, kw_only=True)
class MarketsArgs:
    """``markets(series_ticker)`` — series is the per-row key; the rest narrow it."""

    series_ticker: Annotated[str, Arg(0, doc="Series ticker input column, e.g. 'KXBTCD'")]
    # Empty string means "not supplied". A `str | None` annotation resolves to
    # the Arrow null type, which the DuckDB extension cannot cast a VARCHAR into
    # ("Unimplemented type for cast (VARCHAR -> NULL)"), so every optional arg
    # here uses a sentinel default instead of None.
    event_ticker: Annotated[str, Arg("event_ticker", doc="Narrow to one event", default="")] = ""
    #: Kalshi filters and reports market status in two different vocabularies,
    #: and this is the filter one: `unopened`, `open`, `closed`, `settled`.
    #: Anything else — `active` and `finalized` included — is a 400
    #: `invalid status filter`. The `status` *column* of the result speaks the
    #: other vocabulary, so `status => 'open'` returns rows reading `active`.
    status: Annotated[
        str,
        Arg(
            "status",
            doc="Status filter: unopened, open, closed or settled (a row reads 'active', not 'open')",
            default="",
            #: The empty sentinel is a real choice — it means "no filter".
            choices=["", "unopened", "open", "closed", "settled"],
        ),
    ] = ""


MARKETS_DOCS = docs(
    category="markets",
    result_schema=MARKET_SCHEMA,
    llm=(
        "The entry point to Kalshi's market data: one row per tradeable contract under a "
        "series, with its current quotes, volume and lifecycle state. Reach for this when you "
        "know the series (from the `series` table) and want the individual strikes. The series "
        "ticker is required, so start from `series` if you do not have one. Every deeper "
        "function keys off the `ticker` this returns, and the `series_ticker` it stamps on is "
        "what lets its output drive `candlesticks()` directly."
    ),
    md=(
        "Markets are the tradeable contracts under one Kalshi series — a series like `KXBTCD` "
        "(Bitcoin daily) opens a new event each day, and each event carries many markets, one "
        "per strike price.\n\n"
        "### Why the series ticker is required\n\n"
        "An unfiltered market scan pages past 400,000 rows, roughly 398,000 of which are "
        "zero-volume `KXMVECROSSCATEGORY` parlay combinations, and takes minutes. Requiring the "
        "series keeps a naive unfiltered scan honest.\n\n"
        "### The two status vocabularies\n\n"
        "The `status` **argument** filters on `unopened`, `open`, `closed` or `settled`. The "
        "`status` **column** reports `initialized`, `active`, `closed`, `determined`, `settled` "
        "or `finalized`. They are not interchangeable: `status => 'open'` returns rows whose "
        "column reads `active`, and passing `active` as the filter is an error from Kalshi.\n\n"
        "### Streaming\n\n"
        "Rows arrive one API page at a time, so a `LIMIT` stops early instead of paying for the "
        "whole series. The cost of that is this cannot be used as the inner side of a "
        "correlated `LATERAL` — drive a join from it, not into it.\n\n"
        "### Prices\n\n"
        "All `*_dollars` columns are dollars per contract between 0 and 1, carried as "
        "`DECIMAL(18,4)` — Kalshi sends them as exact fixed-point strings and rounding them "
        "through a float would lose that."
    ),
    example_queries=examples(
        (
            "Open Bitcoin daily markets with their best bid, busiest first",
            "SELECT ticker, title, yes_bid_dollars, volume_24h_fp "
            "FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' "
            "ORDER BY volume_24h_fp DESC",
        ),
        (
            "Ask Kalshi for only the open markets instead of filtering after the fact",
            "SELECT ticker, status FROM kalshi.main.markets('KXBTCD', status => 'open') ORDER BY ticker",
        ),
    ),
)


@init_single_worker
class MarketsFunction(TableFunctionGenerator[MarketsArgs, PagedScanState]):
    """Markets under a series, streamed one API page per tick.

    ``series_ticker`` is required rather than optional on purpose. An unfiltered
    market scan pages past 400,000 rows — roughly 398,000 of them zero-volume
    ``KXMVECROSSCATEGORY`` parlay combos — and takes minutes. Requiring the
    series keeps a naive ``SELECT *`` honest.

    This is a paging scan rather than a blended row transform because the
    endpoint behind it is cursor-paged. A blended function must emit everything
    for its input in a single ``process()`` call, so it would have to walk every
    page before DuckDB saw one row; a ``LIMIT 10`` would still pay for the whole
    series, and — since a scan blocked inside its first batch cannot be
    cancelled — would wedge the client rather than merely be slow. Functions
    whose fetch is naturally bounded (a single market, one order book, a
    candlestick window) stay blended, and so stay usable under a correlated
    LATERAL.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARKET_SCHEMA

    class Meta:
        name = "markets"
        description = "Markets under a Kalshi series (series_ticker required)"
        categories = ["market-data"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        #: `filters_exactly_applied` stays False on purpose: only some predicates
        #: become Kalshi query parameters, so DuckDB must re-check them all.
        filter_pushdown = True
        tags = MARKETS_DOCS
        examples = [
            FunctionExample(
                sql=(
                    "SELECT ticker, title, yes_bid_dollars, volume_24h_fp "
                    "FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' "
                    "ORDER BY volume_24h_fp DESC"
                ),
                description="Open Bitcoin daily markets with their best bid, busiest first",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[MarketsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[MarketsArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[MarketsArgs],
        state: PagedScanState,
        out: OutputCollector,
    ) -> None:
        """Emit one page of markets, then remember where to resume."""
        pushed_event, pushed_status = _pushed_market_filters(params)

        def stamp(rows: Sequence[dict[str, Any]]) -> None:
            # Kalshi's market payload has no series_ticker, but this call knows
            # it: it is the argument. Stamping it on is what lets `markets()`
            # drive `candlesticks()`, which needs a series ticker it could not
            # otherwise obtain.
            for row in rows:
                row["series_ticker"] = params.args.series_ticker

        emit_page(
            params,
            state,
            out,
            path="/markets",
            key="markets",
            query={
                "series_ticker": params.args.series_ticker,
                "event_ticker": params.args.event_ticker or pushed_event,
                "status": params.args.status or pushed_status,
            },
            stamp=stamp,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class TickerArgs:
    """A lone market-ticker input column, plus an opt-in cache TTL."""

    ticker: Annotated[str, Arg(0, doc="Market ticker input column")]
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to cache this result (0 = off; quotes are live)", default=0, ge=0),
    ] = 0


class MarketFunction(RowTransformFunction[TickerArgs]):
    """A single market by ticker — 1->1, so no provenance is needed."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARKET_SCHEMA

    class Meta:
        name = "market"
        description = "One Kalshi market by ticker"
        categories = ["market-data", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="markets",
            result_schema=MARKET_SCHEMA,
            llm=(
                "A single market's current snapshot, by exact ticker. Use this when you already "
                "have a ticker and want one fresh row; use `markets()` when you want to browse or "
                "filter a series. Same columns as `markets()`, so the two are interchangeable "
                "downstream. Quotes are live and uncached unless you pass `cache_ttl`."
            ),
            md=(
                "One row for one market, refetched on every call.\n\n"
                "### When to use this instead of `markets()`\n\n"
                "`markets()` is the browse path and returns a whole series; this is the point "
                "lookup. It is also the cheaper way to re-check a single contract's quote, since "
                "it fetches one market rather than paging a series.\n\n"
                "### Caching\n\n"
                "Kalshi declares no freshness at all on this endpoint, so nothing is cached by "
                "default. Pass `cache_ttl => N` to cache for N seconds and enable per-value "
                "memoization — worth it when a `LATERAL` repeats the same ticker, at the cost of "
                "a quote up to N seconds stale.\n\n"
            ),
            example_queries=examples(
                (
                    "Current quote for one market by ticker",
                    "SELECT ticker, status, yes_bid_dollars, yes_ask_dollars "
                    "FROM kalshi.main.market((SELECT ticker FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' ORDER BY volume_24h_fp DESC LIMIT 1))",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT ticker, status, yes_bid_dollars, yes_ask_dollars "
                    "FROM kalshi.main.market((SELECT ticker FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' ORDER BY volume_24h_fp DESC LIMIT 1))"
                ),
                description="Current quote for one market by ticker",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TickerArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[TickerArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tickers = batch.column("ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        with api.open_client() as client:
            for index, ticker in enumerate(tickers):
                if ticker is None:
                    continue
                found = api.market(str(ticker), client=client, credentials=credentials)
                # No input series to stamp here, so fall back to the event
                # ticker's prefix — the column means the same thing either way.
                found.setdefault("series_ticker", series_of(found.get("event_ticker")))
                rows.append(found)
                parents.append(index)
        # A null input row emits nothing, so this is 1->0/1->1 rather than a
        # strict identity map; provenance is carried for the same reason.
        _emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            _opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class OrderbookArgs:
    """``orderbook(ticker)`` with an optional named depth cap."""

    ticker: Annotated[str, Arg(0, doc="Market ticker input column")]
    #: 0 means "no cap" — see the sentinel note on MarketsArgs. Applied here
    #: rather than by Kalshi: the batch endpoint this uses takes no depth
    #: parameter, so the whole book arrives and the best levels are kept.
    depth: Annotated[int, Arg("depth", doc="Max price levels per side (0 = all)", default=0, ge=0)] = 0
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to cache this book (0 = off; books are live)", default=0, ge=0),
    ] = 0


class OrderbookFunction(RowTransformFunction[OrderbookArgs]):
    """Resting-order book for a market, one row per (side, price level)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = ORDERBOOK_SCHEMA

    class Meta:
        name = "orderbook"
        description = "Kalshi order book flattened to one row per side and price level"
        categories = ["market-data", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="market-depth",
            result_schema=ORDERBOOK_SCHEMA,
            llm=(
                "Resting order-book depth for one market, flattened to one row per side and price "
                "level so it aggregates and joins like an ordinary table. Reach for this to see "
                "liquidity behind the best quote — spread, depth at a price, or the shape of one "
                "side. For just the top of book, `markets()` already carries the best bid and ask "
                "without a second request."
            ),
            md=(
                "Kalshi returns the book as a nested object of `[price, count]` pairs; this "
                "flattens it to one row per (side, price level).\n\n"
                "### Reading the two sides\n\n"
                "A binary contract has two books that mirror each other: a YES bid at 0.40 is "
                "economically a NO ask at 0.60. Both sides are returned, labelled in `side`.\n\n"
                "### An empty result is not an error\n\n"
                "An unknown ticker is answered with HTTP 200 and two empty sides rather than a "
                "404, so zero rows means 'no resting orders' *or* 'no such market' — check "
                "the ticker against `markets()` if that distinction matters.\n\n"
                "### Cost under LATERAL\n\n"
                "One request per input row. Over a whole series that is hundreds of calls into a "
                "rate-limited API, so pass `cache_ttl => N` to enable per-value memoization when "
                "a few seconds of staleness is acceptable.\n\n"
            ),
            example_queries=examples(
                (
                    "Full depth on both sides of one market's book",
                    "SELECT side, price_dollars, count_fp "
                    "FROM kalshi.main.orderbook((SELECT ticker FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' ORDER BY volume_24h_fp DESC LIMIT 1)) "
                    "ORDER BY side, price_dollars DESC",
                ),
                (
                    "Books for every open market in a series, via LATERAL",
                    "SELECT m.ticker, o.side, o.price_dollars, o.count_fp "
                    "FROM kalshi.main.markets('KXBTCD') m, "
                    "LATERAL kalshi.main.orderbook(m.ticker, cache_ttl => 30) o "
                    "WHERE m.status = 'active' ORDER BY m.ticker, o.side, o.price_dollars",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT m.ticker, o.side, o.price_dollars, o.count_fp "
                    "FROM kalshi.main.markets('KXBTCD') m, "
                    "LATERAL kalshi.main.orderbook(m.ticker, cache_ttl => 30) o "
                    "WHERE m.status = 'active' ORDER BY m.ticker, o.side, o.price_dollars"
                ),
                description="Books for every open market in a series, via LATERAL",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[OrderbookArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[OrderbookArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tickers = batch.column("ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        with api.open_client() as client:
            # One request per 100 markets rather than one per market. The whole
            # input batch is fetched up front and then fanned back out in input
            # order, so provenance is unchanged and a repeated ticker still
            # produces its rows once per input row.
            books = api.orderbooks(
                [str(t) for t in tickers if t is not None], client=client, credentials=credentials
            )
            for index, ticker in enumerate(tickers):
                if ticker is None:
                    continue
                levels = flatten_orderbook(str(ticker), books.get(str(ticker)) or {})
                if depth := params.args.depth:
                    levels = _cap_depth(levels, depth)
                rows.extend(levels)
                parents.extend([index] * len(levels))
        _emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            _opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class CandlestickArgs:
    """``candlesticks(series_ticker, ticker)`` plus a named period and window.

    Both tickers are positional because the per-market endpoint genuinely needs
    both — its path is ``/series/{series}/markets/{ticker}/candlesticks``. The
    batched endpoint this actually calls keys on the market ticker alone, so the
    series is not used to route the request; it stays in the signature because it
    is what makes ``markets()`` output compose here, and because the per-market
    path remains the documented one. In a LATERAL the driving row supplies each.
    """

    series_ticker: Annotated[str, Arg(0, doc="Series ticker input column")]
    ticker: Annotated[str, Arg(1, doc="Market ticker input column")]
    period_interval: Annotated[
        int,
        Arg(
            "period_interval",
            doc="Candle width in minutes (1, 60, or 1440)",
            default=60,
            choices=[1, 60, 1440],
        ),
    ] = 60
    #: 0 means "not supplied" — see the sentinel note on MarketsArgs.
    start_ts: Annotated[
        int, Arg("start_ts", doc="Window start, epoch seconds (0 = 24h ago)", default=0, ge=0)
    ] = 0
    end_ts: Annotated[int, Arg("end_ts", doc="Window end, epoch seconds (0 = now)", default=0, ge=0)] = 0


class CandlesticksFunction(RowTransformFunction[CandlestickArgs]):
    """OHLC candles for a market, with the nested price structs flattened.

    The default window ends at "now", which is read once per input batch rather
    than once per scan — a scan large enough to span several batches can see the
    window advance by a few seconds between them. Only the still-forming last
    candle can differ, and that row is not reproducible anyway, which is exactly
    why it is never cached. Pass an explicit ``end_ts`` for a window that is
    identical across every batch (and cacheable).
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = CANDLESTICK_SCHEMA

    class Meta:
        name = "candlesticks"
        description = "Kalshi OHLC candlesticks for a market (series and market ticker)"
        categories = ["market-data", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="history",
            result_schema=CANDLESTICK_SCHEMA,
            llm=(
                "Historical OHLC price and quote bars for one market, at 1-minute, hourly or "
                "daily resolution. This is the time-series view — reach for it to chart or "
                "backtest how a contract's price moved, rather than where it stands now. It needs "
                "BOTH the series ticker and the market ticker, so drive it from `markets()`, "
                "whose `series_ticker` column exists precisely to feed this."
            ),
            md=(
                "OHLC bars for one market. Three nested Kalshi structs — traded `price`, "
                "`yes_bid` and `yes_ask` — are flattened into `price_*`, `yes_bid_*` and "
                "`yes_ask_*` column families.\n\n"
                "### Both tickers are required\n\n"
                "The endpoint lives under the series (`/series/{series}/markets/{ticker}/"
                "candlesticks`), not under the market, so the series ticker is not redundant. "
                "`markets()` stamps a `series_ticker` column on its output for exactly this.\n\n"
                "### Periods with no trades\n\n"
                "The `price_*` columns are NULL for any period that saw no trades — Kalshi sends "
                "an empty struct. The `yes_bid_*` and `yes_ask_*` columns still carry quotes, so "
                "prefer them when you need an unbroken series.\n\n"
                "### Window and caching\n\n"
                "`period_interval` is the bar width in minutes: 1, 60 or 1440. The window "
                "defaults to the last 24 hours. A window that ends in the past contains only "
                "closed bars, which can never change and are cached for a day; a window running "
                "up to now contains a still-forming bar and is never cached.\n\n"
            ),
            example_queries=examples(
                (
                    "Hourly closing prices for one market over the last day",
                    "SELECT c.end_period_ts, c.price_close_dollars FROM ("
                    "SELECT series_ticker, ticker FROM kalshi.main.markets('KXBTCD') "
                    "WHERE status = 'active' ORDER BY volume_24h_fp DESC LIMIT 1) m, "
                    "LATERAL kalshi.main.candlesticks(m.series_ticker, m.ticker, "
                    "period_interval => 60) c ORDER BY c.end_period_ts",
                ),
                (
                    "Hourly candles for every market in a series, driven by markets()",
                    "SELECT m.ticker, c.end_period_ts, c.price_close_dollars "
                    "FROM kalshi.main.markets('KXBTCD') m, LATERAL kalshi.main.candlesticks("
                    "m.series_ticker, m.ticker, period_interval => 60) c "
                    "ORDER BY m.ticker, c.end_period_ts",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT c.end_period_ts, c.price_close_dollars FROM ("
                    "SELECT series_ticker, ticker FROM kalshi.main.markets('KXBTCD') "
                    "WHERE status = 'active' ORDER BY volume_24h_fp DESC LIMIT 1) m, "
                    "LATERAL kalshi.main.candlesticks(m.series_ticker, m.ticker, "
                    "period_interval => 60) c ORDER BY c.end_period_ts"
                ),
                description="Hourly closing prices for the busiest open market over the last day",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[CandlestickArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[CandlestickArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        now = int(time.time())
        end_ts = params.args.end_ts or now
        start_ts = params.args.start_ts or (end_ts - _DAY_SECONDS)
        series = batch.column("series_ticker").to_pylist()
        tickers = batch.column("ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        with api.open_client() as client:
            # Batched, and sized by the window: the endpoint caps total candles
            # across the call, so a wide window means fewer markets per request.
            found = api.batch_candlesticks(
                [str(t) for s, t in zip(series, tickers, strict=True) if s is not None and t is not None],
                period_interval=params.args.period_interval,
                start_ts=start_ts,
                end_ts=end_ts,
                client=client,
                credentials=credentials,
            )
            for index, (series_ticker, ticker) in enumerate(zip(series, tickers, strict=True)):
                if series_ticker is None or ticker is None:
                    continue
                flat = flatten_candlesticks(str(ticker), found.get(str(ticker)) or [])
                rows.extend(flat)
                parents.extend([index] * len(flat))
        _emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            _candlestick_cache_control(end_ts=end_ts, period_interval=params.args.period_interval, now=now),
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class TradesArgs:
    """``trades(ticker)`` plus a named time window and row cap."""

    ticker: Annotated[str, Arg(0, doc="Market ticker input column")]
    #: 0 means "not supplied" — see the sentinel note on MarketsArgs.
    min_ts: Annotated[
        int, Arg("min_ts", doc="Only trades at or after this epoch second", default=0, ge=0)
    ] = 0
    max_ts: Annotated[
        int, Arg("max_ts", doc="Only trades at or before this epoch second", default=0, ge=0)
    ] = 0
    max_rows: Annotated[
        int, Arg("max_rows", doc="Cap on trades returned per market (0 = all)", default=0, ge=0)
    ] = 0
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to cache this tape (0 = off; the tape is live)", default=0, ge=0),
    ] = 0


class TradesFunction(RowTransformFunction[TradesArgs]):
    """The public trade tape for one market.

    This is the pull-model answer to Kalshi's WebSocket ``trade`` channel.
    Polling with ``min_ts`` is lossless where the socket is not: the cursor is
    replayable, so a dropped connection costs a retry rather than a gap.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = TRADE_SCHEMA

    class Meta:
        name = "trades"
        description = "Executed trades for a Kalshi market (the public tape)"
        categories = ["market-data", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="history",
            result_schema=TRADE_SCHEMA,
            llm=(
                "Individual executed trades on one market — the public tape, newest first. Reach "
                "for this when you need actual prints rather than quotes or aggregated bars: "
                "trade-by-trade flow, taker direction, or a precise volume-weighted price. Use "
                "`candlesticks()` instead when bars are enough; the tape is far more rows."
            ),
            md=(
                "One row per execution, with the trade quoted from both sides of the "
                "contract.\n\n"
                "### Polling, and why there is no WebSocket\n\n"
                "Kalshi's WebSocket `trade` channel pushes these, but a dropped connection is a "
                "gap you cannot recover. Polling with `min_ts` is lossless instead: the cursor is "
                "replayable, so a failed poll costs a retry rather than lost prints. Track the "
                "highest `created_time` you have seen and pass it as the next `min_ts`.\n\n"
                "### Reading the price columns\n\n"
                "`yes_price_dollars` and `no_price_dollars` are the same execution seen from "
                "each side and always sum to 1. `taker_side` tells you which side the aggressor "
                "bought, which is the direction signal.\n\n"
                "### Volume\n\n"
                "An active market can have a very long tape, so cap it with `max_rows` unless "
                "you genuinely want the whole history.\n\n"
            ),
            example_queries=examples(
                (
                    "The most recent trades on the busiest open market in a series",
                    "SELECT t.created_time, t.taker_side, t.yes_price_dollars, t.count_fp FROM ("
                    "SELECT ticker FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' "
                    "ORDER BY volume_24h_fp DESC LIMIT 1) m, "
                    "LATERAL kalshi.main.trades(m.ticker, max_rows => 100) t "
                    "ORDER BY t.created_time DESC",
                ),
                (
                    "Volume-weighted average price from the tape",
                    "SELECT sum(t.yes_price_dollars * t.count_fp) / sum(t.count_fp) AS vwap_dollars "
                    "FROM (SELECT ticker FROM kalshi.main.markets('KXBTCD') "
                    "WHERE status = 'active' ORDER BY volume_24h_fp DESC LIMIT 1) m, "
                    "LATERAL kalshi.main.trades(m.ticker, max_rows => 500) t",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT t.created_time, t.taker_side, t.yes_price_dollars, t.count_fp FROM ("
                    "SELECT ticker FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' "
                    "ORDER BY volume_24h_fp DESC LIMIT 1) m, "
                    "LATERAL kalshi.main.trades(m.ticker, max_rows => 100) t "
                    "ORDER BY t.created_time DESC"
                ),
                description="The most recent trades on the busiest open market in a series",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[TradesArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[TradesArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tickers = batch.column("ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        with api.open_client() as client:
            for index, ticker in enumerate(tickers):
                if ticker is None:
                    continue
                found = api.trades(
                    str(ticker),
                    min_ts=params.args.min_ts or None,
                    max_ts=params.args.max_ts or None,
                    limit=params.args.max_rows or None,
                    client=client,
                    hint=hint,
                    credentials=credentials,
                )
                rows.extend(found)
                parents.extend([index] * len(found))
        _emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            _origin_cache_control(hint) or _opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


EVENTS_DOCS = docs(
    category="reference",
    result_schema=EVENT_SCHEMA,
    llm=(
        "The layer between a series and its markets: one row per resolution date, with the "
        "strike date, settlement sources and whether its markets are mutually exclusive. Reach "
        "for this to find a specific `event_ticker` — which `markets()` accepts as a filter — "
        "without scanning every market in the series."
    ),
    md=(
        "A Kalshi series recurs; each occurrence is an event; each event carries many markets, "
        "one per strike. `KXBTCD` (Bitcoin daily) opens one event per day, and each event holds "
        "a market for every strike price.\n\n"
        "### Narrowing from here\n\n"
        "Take an `event_ticker` from this and pass it to `markets()` as its `event_ticker` "
        "argument to get just that day's strikes rather than the whole series. Note that has to "
        "be a literal or a scalar subquery: `event_ticker` is a named argument, and DuckDB does "
        "not allow a correlated column to be passed as one.\n\n"
        "### Settlement sources\n\n"
        "`settlement_sources` stays a nested list of `{name, url}` structs — flattening it "
        "would fan every event out into one row per source. Apply DuckDB's `unnest` to the "
        "column when you do want one row per source; the example queries show it. For the "
        "images as well, use `event_metadata()`.\n\n"
        "### Paging\n\n"
        "`/events` caps its page size at 200 rather than the usual 1000 and rejects anything "
        "larger outright. Rows stream a page at a time, so a `LIMIT` stops early."
    ),
    example_queries=examples(
        (
            "Bitcoin daily events by strike date",
            "SELECT event_ticker, title, strike_date FROM kalshi.main.events('KXBTCD') ORDER BY strike_date",
        ),
        (
            "Settlement sources for each event, one row per source",
            "SELECT event_ticker, unnest(settlement_sources).name AS source "
            "FROM kalshi.main.events('KXBTCD') ORDER BY event_ticker, source",
        ),
    ),
)


@dataclass(slots=True, frozen=True, kw_only=True)
class EventsArgs:
    """``events(series_ticker)`` with an optional named status filter."""

    series_ticker: Annotated[str, Arg(0, doc="Series ticker, e.g. 'KXBTCD'")]
    status: Annotated[str, Arg("status", doc="Event status filter", default="")] = ""
    #: `/events` declares no freshness and is the most aggressively rate-limited
    #: endpoint Kalshi exposes — roughly 4 requests a second against ~29 for the
    #: rest — so an opt-in TTL matters more here than anywhere else.
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to cache this result (0 = off)", default=0, ge=0),
    ] = 0


@init_single_worker
class EventsFunction(TableFunctionGenerator[EventsArgs, PagedScanState]):
    """Events under a series — the layer between a series and its markets.

    One event is one resolution date with many strikes under it, so this is how
    you get from ``series`` to a specific ``event_ticker`` without scanning
    every market in the series.

    Paged as a scan for the same reason as :class:`MarketsFunction`, with one
    extra wrinkle: ``/events`` caps its page size at 200 and rejects anything
    larger outright rather than clamping, so the page size is not the default.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENT_SCHEMA

    class Meta:
        name = "events"
        description = "Events under a Kalshi series (series_ticker required)"
        categories = ["market-data"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = EVENTS_DOCS
        examples = [
            FunctionExample(
                sql=(
                    "SELECT event_ticker, title, strike_date "
                    "FROM kalshi.main.events('KXBTCD') ORDER BY strike_date"
                ),
                description="Bitcoin daily events by strike date",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[EventsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[EventsArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[EventsArgs],
        state: PagedScanState,
        out: OutputCollector,
    ) -> None:
        """Emit one page of events, then remember where to resume."""
        emit_page(
            params,
            state,
            out,
            path="/events",
            key="events",
            query={
                "series_ticker": params.args.series_ticker,
                "status": params.args.status or None,
            },
            page_limit=api.EVENTS_PAGE_LIMIT,
            opt_in_ttl=params.args.cache_ttl,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class EventTickerArgs:
    """A lone event-ticker input column, plus an opt-in cache TTL."""

    event_ticker: Annotated[str, Arg(0, doc="Event ticker input column, e.g. 'KXBTCD-26SEP0417'")]
    cache_ttl: Annotated[
        int,
        Arg("cache_ttl", doc="Seconds to cache this result (0 = off)", default=0, ge=0),
    ] = 0


class EventFunction(RowTransformFunction[EventTickerArgs]):
    """One event by ticker — the point lookup to `events()`'s browse."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENT_SCHEMA

    class Meta:
        name = "event"
        description = "One Kalshi event by ticker"
        categories = ["market-data", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=EVENT_SCHEMA,
            llm=(
                "A single event by its exact ticker, with the same columns `events()` returns. "
                "Use this when you already have an `event_ticker` — from a market row, say — and "
                "want its resolution date or settlement sources without scanning the series it "
                "belongs to. `events()` is the browse path; this is the lookup."
            ),
            md=(
                "One row for one event.\n\n"
                "### When to use this instead of `events()`\n\n"
                "`events()` needs a series ticker and returns every event under it. If you "
                "already hold an `event_ticker` — every market row carries one — this fetches "
                "just that event, and composes under a LATERAL driven by markets.\n\n"
                "### What it does not return\n\n"
                "Kalshi's response also carries the event's markets. They are not returned here: "
                "`markets(series, event_ticker => ...)` is the way to ask for those, and it "
                "paginates properly where the inlined copy does not."
            ),
            example_queries=examples(
                (
                    "One event's resolution date by ticker",
                    "SELECT event_ticker, title, strike_date FROM kalshi.main.event('KXBTCD-26SEP0417')",
                ),
                (
                    "The event behind each open market in a series",
                    "SELECT DISTINCT e.event_ticker, e.strike_date "
                    "FROM kalshi.main.markets('KXBTCD', status => 'open') m, "
                    "LATERAL kalshi.main.event(m.event_ticker, cache_ttl => 60) e "
                    "ORDER BY e.strike_date",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=("SELECT event_ticker, title, strike_date FROM kalshi.main.event('KXBTCD-26SEP0417')"),
                description="One event's resolution date by ticker",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[EventTickerArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[EventTickerArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tickers = batch.column("event_ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        with api.open_client() as client:
            for index, ticker in enumerate(tickers):
                if ticker is None:
                    continue
                found = api.event(str(ticker), client=client, hint=hint, credentials=credentials)
                if not found:
                    continue
                rows.append(found)
                parents.append(index)
        _emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            _origin_cache_control(hint) or _opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


class EventMetadataFunction(RowTransformFunction[EventTickerArgs]):
    """Settlement sources and images for one event, one row per source."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EVENT_METADATA_SCHEMA

    class Meta:
        name = "event_metadata"
        description = "Settlement sources and images for a Kalshi event"
        categories = ["market-data", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=EVENT_METADATA_SCHEMA,
            llm=(
                "What an event settles against, flattened to one row per source, plus the images "
                "Kalshi displays for it. Reach for this to answer 'how is this decided?' — the "
                "named source is the thing that determines the outcome. Distinct from the "
                "`settlement_sources` column on `events()`, which is the summary inlined into "
                "the listing; the images only exist here."
            ),
            md=(
                "One row per settlement source for one event.\n\n"
                "### Why one row per source\n\n"
                "An event can settle against more than one source. Flattening them means they "
                "join and aggregate like ordinary rows, at the cost of repeating the image "
                "columns — the same trade-off `orderbook()` makes with price levels. An event "
                "with no declared sources still returns one row, so a lookup always answers "
                "with its images rather than with nothing.\n\n"
                "### Versus the events() column\n\n"
                "`events()` carries a nested `settlement_sources` list, which is the right shape "
                "when you are already scanning events. This endpoint is the one that also has "
                "the images, and it is a point lookup rather than a scan."
            ),
            example_queries=examples(
                (
                    "What decides one event's outcome",
                    "SELECT settlement_source_name, settlement_source_url "
                    "FROM kalshi.main.event_metadata('KXBTCD-26SEP0417')",
                ),
                (
                    "Settlement sources across a whole series",
                    "SELECT DISTINCT md.settlement_source_name "
                    "FROM kalshi.main.events('KXBTCD') e, "
                    "LATERAL kalshi.main.event_metadata(e.event_ticker, cache_ttl => 300) md "
                    "WHERE md.settlement_source_name IS NOT NULL "
                    "ORDER BY md.settlement_source_name",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT settlement_source_name, settlement_source_url "
                    "FROM kalshi.main.event_metadata('KXBTCD-26SEP0417')"
                ),
                description="What decides one event's outcome",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[EventTickerArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[EventTickerArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tickers = batch.column("event_ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        hint = CacheHint()
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        with api.open_client() as client:
            for index, ticker in enumerate(tickers):
                if ticker is None:
                    continue
                payload = api.event_metadata(str(ticker), client=client, hint=hint, credentials=credentials)
                found = flatten_event_metadata(str(ticker), payload)
                rows.extend(found)
                parents.extend([index] * len(found))
        _emit_fanout(
            out,
            params.output_schema,
            rows,
            parents,
            _origin_cache_control(hint) or _opt_in_cache_control(params.args.cache_ttl, per_value=True),
        )


MARKET_FUNCTIONS: list[type] = [
    MarketsFunction,
    MarketFunction,
    OrderbookFunction,
    CandlesticksFunction,
    TradesFunction,
    EventsFunction,
    EventFunction,
    EventMetadataFunction,
]

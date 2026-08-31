"""The historical archive: settled markets, and the trades and candles under them.

Kalshi moves settled markets out of the live endpoints into a separate archive,
and the live endpoints then behave as though they never existed. That failure is
silent — a query for last month's trades against ``trades()`` returns no rows
rather than an error — so the boundary itself is exposed as a table
(``historical_cutoff``) and every function here says which side it serves.

The functions mirror their live twins column for column, so a query can be moved
across the boundary by changing only the function name. ``historical_markets``
adds one column the live shape has no use for: what the contract actually paid.
"""

from __future__ import annotations

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
from vgi_kalshi.paging import PagedScanState, emit_archive_page
from vgi_kalshi.pushdown import epoch_bounds, equality
from vgi_kalshi.schemas import (
    CANDLESTICK_SCHEMA,
    HISTORICAL_CUTOFF_SCHEMA,
    HISTORICAL_MARKET_SCHEMA,
    TRADE_SCHEMA,
    batch_from_rows,
    flatten_candlesticks,
)

if TYPE_CHECKING:
    from vgi.protocol import VgiOutputCollector

#: An archived market can never change, so a result can be cached for a long
#: time. Kalshi declares no freshness on these endpoints — unlike the live
#: reference data, where the origin's own directive is forwarded — but
#: immutability is a stronger guarantee than any TTL it could have sent.
_ARCHIVE_TTL = 86_400


def _archive_cache_control() -> CacheControl:
    """Cacheability for archived data, which is immutable by definition."""
    return CacheControl(ttl=_ARCHIVE_TTL, stale_if_error=STALE_IF_ERROR, per_value=True)


def _emit(
    out: OutputCollector,
    schema: pa.Schema,
    rows: Sequence[dict[str, Any]],
    parent_rows: Sequence[int],
) -> None:
    """Emit one 1->N batch with provenance and the archive's cache policy."""
    cast("VgiOutputCollector", out).emit(
        batch_from_rows(rows, schema),
        parent_rows=list(parent_rows),
        cache_control=_archive_cache_control(),
    )


@dataclass(slots=True, frozen=True, kw_only=True)
class HistoricalMarketsArgs:
    """``historical_markets(series_ticker)`` with the same optional narrowing."""

    series_ticker: Annotated[str, Arg(0, doc="Series ticker input column, e.g. 'KXBTCD'")]
    event_ticker: Annotated[str, Arg("event_ticker", doc="Narrow to one event", default="")] = ""


@init_single_worker
class HistoricalMarketsFunction(TableFunctionGenerator[HistoricalMarketsArgs, PagedScanState]):
    """Settled markets that have left the live endpoints.

    A paging scan rather than a blended row transform: the endpoint is
    cursor-paged, and the archive of a long-running series is larger than the
    live view of it, so walking every page before emitting would be worse here
    than anywhere else. See :mod:`vgi_kalshi.paging`.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = HISTORICAL_MARKET_SCHEMA

    class Meta:
        name = "historical_markets"
        description = "Archived (settled) Kalshi markets, with their settlement value"
        categories = ["historical"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        filter_pushdown = True
        tags = docs(
            category="historical",
            result_schema=HISTORICAL_MARKET_SCHEMA,
            llm=(
                "Markets that have settled and been archived out of `markets()`. Reach for this "
                "for anything resolved — how a past question turned out, what it paid, how it "
                "was priced before it closed. Same columns as the live `markets()` plus "
                "`settlement_value_dollars`, so a query moves across the boundary by changing "
                "only the function name. Check `historical_cutoff` if you are unsure which side "
                "a date falls on; the live functions return nothing rather than erroring for "
                "anything older."
            ),
            md=(
                "The archive of settled markets.\n\n"
                "### Why this is a separate function\n\n"
                "Kalshi archives settled markets out of the live endpoints entirely. Asking "
                "`markets()` for something that settled last month returns no rows — not an "
                "error — so the split has to be visible rather than inferred.\n\n"
                "### What it adds\n\n"
                "`settlement_value_dollars` is what one contract actually paid: 1 for the "
                "winning side and 0 for the losing one, or a value in between for a scalar "
                "market. Every other column matches the live shape.\n\n"
                "### Finding the boundary\n\n"
                "`historical_cutoff` reports the timestamp where the archive begins. It moves, "
                "so read it rather than hardcoding a date."
            ),
            example_queries=examples(
                (
                    "How recent Bitcoin daily markets settled",
                    "SELECT ticker, close_time, result, settlement_value_dollars "
                    "FROM kalshi.main.historical_markets('KXBTCD') ORDER BY close_time DESC",
                ),
                (
                    "Settled markets whose last traded price disagreed with the outcome",
                    "SELECT ticker, last_price_dollars, settlement_value_dollars "
                    "FROM kalshi.main.historical_markets('KXBTCD') "
                    "WHERE abs(last_price_dollars - settlement_value_dollars) > 0.5",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT ticker, close_time, result, settlement_value_dollars "
                    "FROM kalshi.main.historical_markets('KXBTCD') ORDER BY close_time DESC"
                ),
                description="How recent Bitcoin daily markets settled",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[HistoricalMarketsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[HistoricalMarketsArgs]) -> PagedScanState:
        return PagedScanState()

    @classmethod
    def process(
        cls,
        params: ProcessParams[HistoricalMarketsArgs],
        state: PagedScanState,
        out: OutputCollector,
    ) -> None:
        """Emit one page of archived markets, then remember where to resume."""

        def stamp(rows: Sequence[dict[str, Any]]) -> None:
            # Same stamping as the live markets(), for the same reason.
            for row in rows:
                row["series_ticker"] = params.args.series_ticker

        emit_archive_page(
            params,
            state,
            out,
            path="/historical/markets",
            key="markets",
            query={
                "series_ticker": params.args.series_ticker,
                "event_ticker": params.args.event_ticker or equality(params, "event_ticker"),
            },
            ttl=_ARCHIVE_TTL,
            stamp=stamp,
        )


@dataclass(slots=True, frozen=True, kw_only=True)
class HistoricalTradesArgs:
    """``historical_trades(ticker)`` over a time window in the archive."""

    ticker: Annotated[str, Arg(0, doc="Market ticker input column")]
    min_ts: Annotated[
        int, Arg("min_ts", doc="Only trades at or after this epoch second", default=0, ge=0)
    ] = 0
    max_ts: Annotated[
        int, Arg("max_ts", doc="Only trades at or before this epoch second", default=0, ge=0)
    ] = 0
    max_rows: Annotated[
        int, Arg("max_rows", doc="Cap on trades returned per market (0 = all)", default=0, ge=0)
    ] = 0


class HistoricalTradesFunction(RowTransformFunction[HistoricalTradesArgs]):
    """The archived trade tape, identical in shape to the live one."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = TRADE_SCHEMA

    class Meta:
        name = "historical_trades"
        description = "Archived executed trades for a Kalshi market"
        categories = ["historical", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        filter_pushdown = True
        tags = docs(
            category="historical",
            result_schema=TRADE_SCHEMA,
            llm=(
                "Executed trades from before the archive cutoff — the same tape as `trades()`, "
                "for markets old enough to have been moved out of it. Identical columns, so the "
                "two can be combined with UNION ALL to span the boundary. `trades()` answers "
                "with no rows, not an error, for anything this old, which is the mistake this "
                "function exists to prevent."
            ),
            md=(
                "The archived half of the public trade tape.\n\n"
                "### Same shape as the live tape\n\n"
                "Every column matches `trades()`, deliberately: a query spanning the cutoff can "
                "`UNION ALL` the two without reshaping either side.\n\n"
                "### Where the boundary is\n\n"
                "`historical_cutoff.trades_created_ts` is the moment the archive takes over. It "
                "advances, so read it rather than assuming a fixed date.\n\n"
                "### Volume\n\n"
                "An archived market's whole life is in here, so cap it with `max_rows` or a "
                "`min_ts`/`max_ts` window unless you want all of it."
            ),
            example_queries=examples(
                (
                    "The last hundred archived trades on a settled market",
                    "SELECT created_time, taker_side, yes_price_dollars, count_fp "
                    "FROM kalshi.main.historical_trades('KXBTCD-26JUL0119-T68299.99', "
                    "max_rows => 100) ORDER BY created_time DESC",
                ),
                (
                    "Archived volume by taker side for one market",
                    "SELECT taker_side, sum(count_fp) AS contracts "
                    "FROM kalshi.main.historical_trades('KXBTCD-26JUL0119-T68299.99') "
                    "GROUP BY taker_side ORDER BY taker_side",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT created_time, taker_side, yes_price_dollars, count_fp "
                    "FROM kalshi.main.historical_trades('KXBTCD-26JUL0119-T68299.99', "
                    "max_rows => 100) ORDER BY created_time DESC"
                ),
                description="The last hundred archived trades on a settled market",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[HistoricalTradesArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[HistoricalTradesArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tickers = batch.column("ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        # An archived tape is a whole market lifetime; a WHERE on created_time
        # becomes the endpoint's window instead of a post-filter.
        pushed_min, pushed_max = epoch_bounds(params, "created_time")
        with api.open_client() as client:
            for index, ticker in enumerate(tickers):
                if ticker is None:
                    continue
                found = api.historical_trades(
                    str(ticker),
                    min_ts=params.args.min_ts or pushed_min,
                    max_ts=params.args.max_ts or pushed_max,
                    limit=params.args.max_rows or None,
                    client=client,
                    credentials=credentials,
                )
                rows.extend(found)
                parents.extend([index] * len(found))
        _emit(out, params.output_schema, rows, parents)


@dataclass(slots=True, frozen=True, kw_only=True)
class HistoricalCandlestickArgs:
    """``historical_candlesticks(ticker)`` — market ticker only, no series."""

    ticker: Annotated[str, Arg(0, doc="Market ticker input column")]
    period_interval: Annotated[
        int,
        Arg(
            "period_interval",
            doc="Candle width in minutes (1, 60, or 1440)",
            default=60,
            choices=[1, 60, 1440],
        ),
    ] = 60
    start_ts: Annotated[
        int, Arg("start_ts", doc="Window start, epoch seconds (0 = 30d before end)", default=0, ge=0)
    ] = 0
    end_ts: Annotated[
        int, Arg("end_ts", doc="Window end, epoch seconds (0 = the archive cutoff)", default=0, ge=0)
    ] = 0


class HistoricalCandlesticksFunction(RowTransformFunction[HistoricalCandlestickArgs]):
    """Archived OHLC candles for one market.

    Unlike the live :class:`~vgi_kalshi.markets.CandlesticksFunction`, this takes
    the market ticker alone: the archive's path is not scoped to a series. There
    is also no batched form, so a LATERAL over many markets costs one request
    each — narrow the driving set.
    """

    FIXED_SCHEMA: ClassVar[pa.Schema] = CANDLESTICK_SCHEMA

    class Meta:
        name = "historical_candlesticks"
        description = "Archived OHLC candlesticks for a settled Kalshi market"
        categories = ["historical", "blended"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        filter_pushdown = True
        tags = docs(
            category="historical",
            result_schema=CANDLESTICK_SCHEMA,
            llm=(
                "OHLC bars for a market that has settled and been archived. The columns match "
                "the live `candlesticks()` exactly, but the signature does not: this takes only "
                "the market ticker, because the archive is not scoped by series. Reach for it to "
                "chart or backtest a resolved question."
            ),
            md=(
                "Archived price history for one settled market.\n\n"
                "### One ticker, not two\n\n"
                "The live candlestick endpoint lives under a series and needs both tickers; the "
                "archive is keyed on the market alone. That is the only difference in how you "
                "call it — the returned columns are identical.\n\n"
                "### No batched form\n\n"
                "There is no archive equivalent of the batched live endpoint, so a LATERAL over "
                "many markets issues one request per market against a rate-limited API. Drive it "
                "from a narrow set of tickers.\n\n"
                "### Immutable, so cached\n\n"
                "Nothing in the archive can change, so results are cached for a day regardless "
                "of what the window looks like — the live function's forming-candle problem "
                "cannot arise here."
            ),
            example_queries=examples(
                (
                    "Daily closing prices for the most recently settled market in a series",
                    "SELECT c.end_period_ts, c.price_close_dollars FROM ("
                    "SELECT ticker FROM kalshi.main.historical_markets('KXBTCD') LIMIT 1) m, "
                    "LATERAL kalshi.main.historical_candlesticks(m.ticker, "
                    "period_interval => 1440) c ORDER BY c.end_period_ts",
                ),
                (
                    "Hourly high and low for a settled market",
                    "SELECT end_period_ts, price_high_dollars, price_low_dollars "
                    "FROM kalshi.main.historical_candlesticks('KXBTCD-26JUL0119-T68299.99', "
                    "period_interval => 60) ORDER BY end_period_ts",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT c.end_period_ts, c.price_close_dollars FROM ("
                    "SELECT ticker FROM kalshi.main.historical_markets('KXBTCD') LIMIT 1) m, "
                    "LATERAL kalshi.main.historical_candlesticks(m.ticker, "
                    "period_interval => 1440) c ORDER BY c.end_period_ts"
                ),
                description="Daily closing prices for the most recently settled market in a series",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[HistoricalCandlestickArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(
        cls,
        params: ProcessParams[HistoricalCandlestickArgs],
        state: None,
        batch: pa.RecordBatch,
        out: OutputCollector,
    ) -> None:
        tickers = batch.column("ticker").to_pylist()
        rows: list[dict[str, Any]] = []
        parents: list[int] = []
        credentials = auth.for_call(params.secrets, params.attach_opaque_data)
        with api.open_client() as client:
            end_ts = params.args.end_ts or _cutoff_epoch(client, credentials)
            start_ts = params.args.start_ts or (end_ts - 30 * 86_400)
            # Narrow only; see the live twin for why intersecting is safe.
            pushed_min, pushed_max = epoch_bounds(params, "end_period_ts")
            if pushed_min is not None:
                start_ts = max(start_ts, pushed_min)
            if pushed_max is not None:
                end_ts = min(end_ts, pushed_max)
            for index, ticker in enumerate(tickers):
                if ticker is None:
                    continue
                candles = api.historical_candlesticks(
                    str(ticker),
                    period_interval=params.args.period_interval,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    client=client,
                    credentials=credentials,
                )
                flat = flatten_candlesticks(str(ticker), candles)
                rows.extend(flat)
                parents.extend([index] * len(flat))
        _emit(out, params.output_schema, rows, parents)


def _cutoff_epoch(client: Any, credentials: Any) -> int:
    """The archive cutoff as epoch seconds, for a default window that ends in the archive.

    Defaulting to "now" would be wrong here in a way that is hard to see: the
    archive holds nothing recent, so a window ending now and starting 30 days ago
    would mostly cover the live period and come back thin or empty.
    """
    from vgi_kalshi.schemas import to_timestamp

    payload = api.historical_cutoff(client=client, credentials=credentials)
    moment = to_timestamp(payload.get("market_settled_ts"))
    return int(moment.timestamp()) if moment else 0


@init_single_worker
class HistoricalCutoffFunction(TableFunctionGenerator[None, None]):
    """Where the archive begins — the scan behind the ``historical_cutoff`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = HISTORICAL_CUTOFF_SCHEMA

    class Meta:
        name = "all_historical_cutoff"
        description = "The live/archive boundary (the scan backing `historical_cutoff`)"
        categories = ["historical"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="historical",
            result_schema=HISTORICAL_CUTOFF_SCHEMA,
            llm=(
                "The single row that says where Kalshi's live endpoints stop and its archive "
                "begins. Read it before querying a date range: the live functions return no "
                "rows, rather than an error, for anything older than this, so an empty result "
                "is otherwise indistinguishable from 'nothing happened'. Prefer the "
                "`historical_cutoff` table, which scans this."
            ),
            md=(
                "One row, four timestamps — Kalshi archives each kind of data on its own "
                "schedule.\n\n"
                "### Why it matters\n\n"
                "Asking a live function for something older than its cutoff is not an error. It "
                "returns nothing, which reads exactly like a market that never traded. Comparing "
                "against this row is the only way to tell those apart.\n\n"
                "### Which column\n\n"
                "`market_settled_ts` governs markets and their candles; `trades_created_ts` "
                "governs the tape. The other two describe order and position archives, which "
                "this worker does not expose — they are here because Kalshi reports them and "
                "omitting them would misrepresent the response."
            ),
            example_queries=examples(
                (
                    "Where does the archive begin?",
                    "SELECT market_settled_ts, trades_created_ts FROM kalshi.main.all_historical_cutoff()",
                ),
                (
                    "Is a date served by the live functions or the archive?",
                    "SELECT DATE '2026-08-01' >= market_settled_ts AS use_live_functions "
                    "FROM kalshi.main.all_historical_cutoff()",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=("SELECT market_settled_ts, trades_created_ts FROM kalshi.main.all_historical_cutoff()"),
                description="Where does the archive begin?",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        """Fetch the boundary. Cached briefly — it advances, but slowly."""
        hint = CacheHint()
        payload = api.historical_cutoff(
            hint=hint, credentials=auth.for_call(params.secrets, params.attach_opaque_data)
        )
        cache_control = (
            CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR)
            if hint.cacheable
            else CacheControl(ttl=3_600, stale_if_error=STALE_IF_ERROR)
        )
        out.emit(batch_from_rows([payload], params.output_schema), cache_control=cache_control)
        out.finish()


HISTORICAL_FUNCTIONS: list[type] = [
    HistoricalMarketsFunction,
    HistoricalTradesFunction,
    HistoricalCandlesticksFunction,
    HistoricalCutoffFunction,
]

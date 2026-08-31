"""Reference data: the ``series`` catalog.

Unlike the market-data functions, series data has no required key and changes
slowly, which makes it the natural fit for a *real catalog table* rather than a
function. It is not small: the whole catalog is ~13,600 rows and about 15.6 MB
of JSON, gzipped on the wire and returned in a single unpaginated response.

The table is declared in :mod:`vgi_kalshi.worker` as
``Table(name="series", function=AllSeriesFunction)``; VGI's
``ReadOnlyCatalogInterface.table_scan_function_get`` auto-wires the scan, so no
scan code is needed here. The backing function is named ``all_series`` rather
than ``series`` so the function and the table it backs do not collide in one
schema — the same convention ``vgi-tastytrade`` uses for ``all_equities``.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa
from vgi.cache_control import CacheControl
from vgi.invocation import BindResponse
from vgi.arguments import SecretLookupEntry
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc.rpc import OutputCollector

from vgi_kalshi import auth
from vgi_kalshi import kalshi_api as api
from vgi_kalshi.kalshi_api import STALE_IF_ERROR, CacheHint
from vgi_kalshi.meta import docs, examples
from vgi_kalshi.schemas import (
    EXCHANGE_STATUS_SCHEMA,
    SERIES_SCHEMA,
    batch_from_rows,
    flatten_exchange_status,
)


@init_single_worker
class AllSeriesFunction(TableFunctionGenerator[None, None]):
    """Every series on the exchange — the scan behind the ``series`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = SERIES_SCHEMA

    class Meta:
        name = "all_series"
        description = "Every Kalshi series (the scan backing the `series` table)"
        categories = ["reference"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=SERIES_SCHEMA,
            llm=(
                "Every series on the exchange, as a function. Prefer the `series` table, which "
                "scans this and reads as plain SQL; this form exists for the rare case you want "
                "the scan itself. Either way it is the discovery starting point: a series ticker "
                "from here is the required key for `markets()`, `events()` and `candlesticks()`."
            ),
            md=(
                "The whole Kalshi series catalog — roughly 13,600 rows, returned in a single "
                "unpaginated response.\n\n"
                "### Prefer the table\n\n"
                "The `series` catalog table is backed by this function, so it can be selected "
                "from directly, without parentheses. This function returns exactly the same "
                "rows; it is named `all_series` only so that the function and the table it "
                "backs can coexist in one schema.\n\n"
                "### Where to go next\n\n"
                "A series is the top of Kalshi's hierarchy: series → events → markets. Take a "
                "`ticker` from here and pass it to `markets()` or `events()`.\n\n"
            ),
            example_queries=examples(
                (
                    "Crypto series, read straight from the scan function",
                    "SELECT ticker, title, frequency FROM kalshi.main.all_series() "
                    "WHERE category = 'Crypto' ORDER BY ticker",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT ticker, title, frequency FROM kalshi.main.all_series() "
                    "WHERE category = 'Crypto' ORDER BY ticker"
                ),
                description="Crypto series, read straight from the scan function",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        """Fetch the whole series catalog, forwarding Kalshi's own freshness policy.

        ``/series`` is one of the few Kalshi endpoints that declares
        ``Cache-Control`` (``public, max-age=15``); that value is passed through
        rather than hardcoded, so the result cache follows the exchange.
        """
        hint = CacheHint()
        rows: list[dict[str, Any]] = api.series_list(
            hint=hint, credentials=auth.for_call(params.secrets, params.attach_opaque_data)
        )
        cache_control = (
            CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR) if hint.cacheable else None
        )
        out.emit(batch_from_rows(rows, params.output_schema), cache_control=cache_control)
        out.finish()


@init_single_worker
class ExchangeStatusFunction(TableFunctionGenerator[None, None]):
    """Whether the exchange is open — the scan behind the ``exchange_status`` table."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = EXCHANGE_STATUS_SCHEMA

    class Meta:
        name = "all_exchange_status"
        description = "Exchange and per-venue trading status (the scan backing `exchange_status`)"
        categories = ["reference"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="reference",
            result_schema=EXCHANGE_STATUS_SCHEMA,
            llm=(
                "Whether Kalshi is open for trading right now, per venue. Prefer the "
                "`exchange_status` table, which scans this. Worth checking before concluding "
                "that an empty order book or a stale quote means something — outside trading "
                "hours it means the venue is closed. Kalshi runs several venues (Default, "
                "Combos, Crypto, sports) that open and close independently, so this returns "
                "one row per venue plus the exchange-wide flags repeated on each."
            ),
            md=(
                "One row per trading venue, plus the exchange-wide flags on every row.\n\n"
                "### Why this is per-venue\n\n"
                "Kalshi is not one market. Crypto trades around the clock while sports venues "
                "follow their seasons, so `exchange_active` on its own does not tell you "
                "whether the contract you care about is tradeable. Match the venue by its "
                "`description`, or read the `*_overall` columns to ignore the distinction.\n\n"
                "### Freshness\n\n"
                "This is the most volatile thing Kalshi publishes and it says so: "
                "`Cache-Control: public, max-age=1`, the shortest TTL anywhere in the API. "
                "That one second is forwarded to the result cache rather than invented here."
            ),
            example_queries=examples(
                (
                    "Every venue and whether it is currently trading",
                    "SELECT description, exchange_active, trading_active "
                    "FROM kalshi.main.all_exchange_status() ORDER BY exchange_index",
                ),
                (
                    "Every venue's status, newest venue ids last",
                    "SELECT exchange_index, description, exchange_active, trading_active "
                    "FROM kalshi.main.all_exchange_status() ORDER BY exchange_index",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT description, exchange_active, trading_active "
                    "FROM kalshi.main.all_exchange_status() ORDER BY exchange_index"
                ),
                description="Every venue and whether it is currently trading",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        """Fetch the status, forwarding Kalshi's one-second freshness directive."""
        hint = CacheHint()
        payload = api.exchange_status(
            hint=hint, credentials=auth.for_call(params.secrets, params.attach_opaque_data)
        )
        cache_control = (
            CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR) if hint.cacheable else None
        )
        rows = flatten_exchange_status(payload)
        out.emit(batch_from_rows(rows, params.output_schema), cache_control=cache_control)
        out.finish()


REFERENCE_FUNCTIONS: list[type] = [AllSeriesFunction, ExchangeStatusFunction]

"""Your own Kalshi perps (margin) account: positions, balance, risk, funding paid.

These are the only functions in the worker that need credentials. Each one is a
signed ``GET`` of a fixed, allowlisted path (:data:`vgi_kalshi.kalshi_api.ACCOUNT_PATHS`)
and returns data about the key holder only; nothing here can place, amend or
cancel an order or move money. Kalshi lets an API key be created read-only, and
that is the right kind to give this worker.

Nothing is cached. Account state is exactly the thing that must never be served
stale: a position that was liquidated five minutes ago must not still appear.

Prices are per contract, as Kalshi quotes them. A KXGOLDPERP contract is 0.001
troy ounce, so ``entry_price`` 3.8643 is $3,864.30/oz; divide by the contract
size to get a per-unit price.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, SecretLookupEntry
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import BindParams, ProcessParams, TableFunctionGenerator, init_single_worker
from vgi_rpc.rpc import OutputCollector

from vgi_kalshi import auth
from vgi_kalshi import kalshi_api as api
from vgi_kalshi.meta import docs, examples
from vgi_kalshi.schemas import (
    MARGIN_BALANCE_SCHEMA,
    MARGIN_FUNDING_SCHEMA,
    MARGIN_POSITION_SCHEMA,
    MARGIN_RISK_SCHEMA,
    batch_from_rows,
    flatten_margin_balance,
    flatten_margin_funding,
    flatten_margin_risk,
)

#: Shared by every account function's documentation.
_KEY_NOTE = (
    "\n\n### Credentials\n\n"
    "Reads **your own** account, so it needs a `kalshi` secret. RSA and Ed25519 keys both work; "
    "create the key read-only in Kalshi's API key settings.\n\n"
    "```sql\nCREATE SECRET (TYPE kalshi, key_id '...',\n"
    "    private_key '-----BEGIN PRIVATE KEY-----...');\n```\n\n"
    "Without a secret the call fails rather than returning an empty result, so a missing key cannot "
    "read as a flat account."
)
_PER_CONTRACT_NOTE = (
    "\n\n### Units\n\nPrices are per contract, as Kalshi quotes them: KXGOLDPERP is 0.001 oz and "
    "KXBTCPERP 0.0001 BTC per contract, so divide by the contract size for a per-unit price."
)


def _credentials(params: Any, function: str) -> auth.Credentials:
    """This call's credentials; an account read never goes out unsigned."""
    credentials = auth.for_call(params.secrets, params.attach_opaque_data)
    if credentials is None:
        raise api.KalshiCredentialsRequired(function)
    return credentials


def _subaccount(value: int) -> int | None:
    """-1 is the "not supplied" sentinel: every subaccount."""
    return None if value < 0 else value


@dataclass(slots=True, frozen=True, kw_only=True)
class MarginPositionsArgs:
    """``margin_positions()`` with optional market and subaccount filters."""

    ticker: Annotated[
        str, Arg("ticker", doc="Only this perp market, e.g. 'KXGOLDPERP' (default: all)", default="")
    ] = ""
    subaccount: Annotated[
        int, Arg("subaccount", doc="Only this subaccount number (default -1: all)", default=-1, ge=-1, le=255)
    ] = -1


@init_single_worker
class MarginPositionsFunction(TableFunctionGenerator[MarginPositionsArgs, None]):
    """Open perps positions in the key holder's account."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARGIN_POSITION_SCHEMA

    class Meta:
        name = "margin_positions"
        description = (
            "Your open Kalshi perps positions: size, entry, unrealized P&L, margin, fees (needs an API key)"
        )
        categories = ["account"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="account",
            result_schema=MARGIN_POSITION_SCHEMA,
            llm=(
                "The caller's own open perpetual-futures positions on Kalshi, one row per position: signed "
                "size (negative = short), entry price, unrealized P&L, maintenance margin used and fees. "
                "Requires a `kalshi` API-key secret. Join to `margin_risk()` on (subaccount, market_ticker) "
                "for the liquidation price, and to `margin_funding_history()` for funding paid or received."
            ),
            md=(
                "Your open Kalshi perps positions, across every subaccount, including isolated ones. "
                "An isolated position lives in its own subaccount, so the subaccount number identifies it."
                + _PER_CONTRACT_NOTE
                + _KEY_NOTE
            ),
            example_queries=examples(
                (
                    "Every open perps position",
                    (
                        "SELECT market_ticker, subaccount_type, position, entry_price, unrealized_pnl "
                        "FROM kalshi.main.margin_positions()"
                    ),
                ),
                (
                    "Open perps positions with per-ounce entry for gold",
                    "SELECT market_ticker, position, entry_price / 0.001 AS entry_per_oz, "
                    "unrealized_pnl, fees "
                    "FROM kalshi.main.margin_positions(ticker => 'KXGOLDPERP')",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT market_ticker, subaccount_type, position, entry_price, unrealized_pnl "
                    "FROM kalshi.main.margin_positions()"
                ),
                description="Every open perps position",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[MarginPositionsArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[MarginPositionsArgs], state: None, out: OutputCollector) -> None:
        rows = api.margin_positions(
            ticker=params.args.ticker or None,
            subaccount=_subaccount(params.args.subaccount),
            credentials=_credentials(params, "margin_positions"),
        )
        out.emit(batch_from_rows(rows, params.output_schema))
        out.finish()


@init_single_worker
class MarginBalanceFunction(TableFunctionGenerator[None, None]):
    """Cash, equity and margin per subaccount."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARGIN_BALANCE_SCHEMA

    class Meta:
        name = "margin_balance"
        description = (
            "Your Kalshi perps balance per subaccount: equity, initial and maintenance margin "
            "(needs an API key)"
        )
        categories = ["account"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="account",
            result_schema=MARGIN_BALANCE_SCHEMA,
            llm=(
                "The caller's Kalshi perps balance, one row per subaccount: account equity, position value, "
                "initial and maintenance margin. Equity falling to maintenance margin means liquidation. "
                "Requires a `kalshi` API-key secret."
            ),
            md=(
                "Your Kalshi perps balance, one row per subaccount (the primary cross account is "
                "subaccount 0; each isolated position has its own). `settled_funds` is account-wide "
                "and repeated on every row." + _KEY_NOTE
            ),
            example_queries=examples(
                (
                    "Equity and maintenance margin per subaccount",
                    (
                        "SELECT subaccount, subaccount_type, account_equity, maintenance_margin "
                        "FROM kalshi.main.margin_balance()"
                    ),
                ),
                (
                    "Equity and margin headroom per subaccount",
                    "SELECT subaccount, subaccount_type, account_equity, maintenance_margin, "
                    "account_equity - maintenance_margin AS headroom FROM kalshi.main.margin_balance()",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT subaccount, subaccount_type, account_equity, maintenance_margin "
                    "FROM kalshi.main.margin_balance()"
                ),
                description="Equity and maintenance margin per subaccount",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        payload = api.margin_balance(credentials=_credentials(params, "margin_balance"))
        out.emit(batch_from_rows(flatten_margin_balance(payload), params.output_schema))
        out.finish()


@init_single_worker
class MarginRiskFunction(TableFunctionGenerator[None, None]):
    """Leverage and Kalshi's estimated liquidation price, per position."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARGIN_RISK_SCHEMA

    class Meta:
        name = "margin_risk"
        description = (
            "Your Kalshi perps leverage and estimated liquidation price per position (needs an API key)"
        )
        categories = ["account"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="account",
            result_schema=MARGIN_RISK_SCHEMA,
            llm=(
                "Per-position risk for the caller's Kalshi perps: mark, notional, maintenance margin "
                "required, leverage and Kalshi's own estimated liquidation price, plus account-wide "
                "totals on every row. The way to answer 'how far is my position from liquidation'. "
                "Requires a `kalshi` API-key secret."
            ),
            md=(
                "Kalshi's risk view of each open position. `estimated_liquidation_price` is Kalshi's "
                "estimate; "
                "it moves as funding and P&L change the position's equity." + _PER_CONTRACT_NOTE + _KEY_NOTE
            ),
            example_queries=examples(
                (
                    "Leverage and liquidation price per position",
                    (
                        "SELECT market_ticker, position_leverage, estimated_liquidation_price "
                        "FROM kalshi.main.margin_risk()"
                    ),
                ),
                (
                    "Distance to liquidation for each position",
                    "SELECT market_ticker, mark_price, estimated_liquidation_price, "
                    "estimated_liquidation_price / mark_price - 1 AS move_to_liquidation "
                    "FROM kalshi.main.margin_risk()",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT market_ticker, position_leverage, estimated_liquidation_price "
                    "FROM kalshi.main.margin_risk()"
                ),
                description="Leverage and liquidation price per position",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[None]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[None], state: None, out: OutputCollector) -> None:
        payload = api.margin_risk(credentials=_credentials(params, "margin_risk"))
        out.emit(batch_from_rows(flatten_margin_risk(payload), params.output_schema))
        out.finish()


@dataclass(slots=True, frozen=True, kw_only=True)
class MarginFundingArgs:
    """``margin_funding_history()`` over a date range, optionally one market."""

    ticker: Annotated[str, Arg("ticker", doc="Only this perp market (default: all)", default="")] = ""
    start_date: Annotated[
        str,
        Arg("start_date", doc="Inclusive UTC start date, 'YYYY-MM-DD' (default: 30 days ago)", default=""),
    ] = ""
    end_date: Annotated[
        str, Arg("end_date", doc="Inclusive UTC end date, 'YYYY-MM-DD' (default: today)", default="")
    ] = ""
    subaccount: Annotated[
        int, Arg("subaccount", doc="Only this subaccount number (default -1: all)", default=-1, ge=-1, le=255)
    ] = -1


def _date_arg(value: str, name: str, fallback: date) -> str:
    """A 'YYYY-MM-DD' argument, validated here so a typo is named rather than a bare 400."""
    if not value:
        return fallback.isoformat()
    try:
        return date.fromisoformat(value.strip()[:10]).isoformat()
    except ValueError as exc:
        raise ValueError(f"{name} must be a date like '2026-11-01', got {value!r}") from exc


@init_single_worker
class MarginFundingHistoryFunction(TableFunctionGenerator[MarginFundingArgs, None]):
    """Funding you actually paid or received, per settlement."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = MARGIN_FUNDING_SCHEMA

    class Meta:
        name = "margin_funding_history"
        description = (
            "Funding your Kalshi perps positions paid or received, per settlement (needs an API key)"
        )
        categories = ["account"]
        required_secrets = [SecretLookupEntry(secret_type=auth.SECRET_TYPE)]
        projection_pushdown = True
        tags = docs(
            category="account",
            result_schema=MARGIN_FUNDING_SCHEMA,
            llm=(
                "Funding payments booked to the caller's Kalshi perps positions, one row per settlement: "
                "the rate, "
                "the mark, the position size and the dollar amount (positive = received, negative = paid). "
                "Defaults to the last 30 days; pass start_date/end_date ('YYYY-MM-DD') for another range. "
                "Requires a `kalshi` API-key secret."
            ),
            md=(
                "What funding actually did to your account, as opposed to the published rate: "
                "`funding_amount` is signed from your side, so a short collecting positive funding shows "
                "positive "
                "amounts. Dates are inclusive UTC days." + _KEY_NOTE
            ),
            example_queries=examples(
                (
                    "Gold perp funding payments, oldest first",
                    (
                        "SELECT funding_time, funding_rate, funding_amount "
                        "FROM kalshi.main.margin_funding_history(ticker => 'KXGOLDPERP') "
                        "ORDER BY funding_time"
                    ),
                ),
                (
                    "Funding received per market over the last 30 days",
                    "SELECT market_ticker, count(*) AS settlements, sum(funding_amount) AS received "
                    "FROM kalshi.main.margin_funding_history() GROUP BY market_ticker",
                ),
            ),
        )
        examples = [
            FunctionExample(
                sql=(
                    "SELECT funding_time, funding_rate, funding_amount "
                    "FROM kalshi.main.margin_funding_history(ticker => 'KXGOLDPERP') "
                    "ORDER BY funding_time"
                ),
                description="Gold perp funding payments, oldest first",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[MarginFundingArgs]) -> BindResponse:
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def process(cls, params: ProcessParams[MarginFundingArgs], state: None, out: OutputCollector) -> None:
        today = datetime.now(UTC).date()
        rows = api.margin_funding_history(
            _date_arg(params.args.start_date, "start_date", today - timedelta(days=30)),
            _date_arg(params.args.end_date, "end_date", today),
            ticker=params.args.ticker or None,
            subaccount=_subaccount(params.args.subaccount),
            credentials=_credentials(params, "margin_funding_history"),
        )
        out.emit(batch_from_rows(flatten_margin_funding(rows), params.output_schema))
        out.finish()


ACCOUNT_FUNCTIONS: list[type] = [
    MarginPositionsFunction,
    MarginBalanceFunction,
    MarginRiskFunction,
    MarginFundingHistoryFunction,
]

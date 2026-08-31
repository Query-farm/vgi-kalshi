"""VGI worker exposing Kalshi prediction-market data to DuckDB/SQL (read-only).

    ATTACH 'kalshi' (TYPE vgi, LOCATION 'uv run kalshi_worker.py');
    SELECT ticker, title FROM kalshi.series WHERE category = 'Crypto';
    SELECT * FROM kalshi.markets('KXBTCD') WHERE status = 'active';

No credentials are required. Kalshi's entire market-data surface — markets,
order books, candlesticks, trades, events, series — is public; only portfolio,
order, and account endpoints need an API key with RSA-PSS request signing, and
this worker deliberately exposes none of them.

Function names are bare (``markets``, not ``kalshi_markets``) because they are
already qualified by the catalog they live in.
"""

from __future__ import annotations

import json
import sys
from dataclasses import replace
from typing import Any

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, ReadOnlyCatalogInterface, Schema
from vgi.catalog.attach_option import AttachOptionSpec
from vgi.catalog.catalog_interface import AttachOpaqueData, CatalogAttachResult, CatalogInfo
from vgi.catalog.descriptors import Table

from vgi_kalshi import __version__, auth
from vgi_kalshi.historical import HISTORICAL_FUNCTIONS, HistoricalCutoffFunction
from vgi_kalshi.markets import MARKET_FUNCTIONS
from vgi_kalshi.meta import column_comments, docs, examples, keywords
from vgi_kalshi.reference import REFERENCE_FUNCTIONS, AllSeriesFunction, ExchangeStatusFunction
from vgi_kalshi.schemas import EXCHANGE_STATUS_SCHEMA, HISTORICAL_CUTOFF_SCHEMA, SERIES_SCHEMA

IMPLEMENTATION_VERSION = __version__
DATA_VERSION_SPEC = f"=={__version__}"
SOURCE_URL = "https://github.com/Query-farm/vgi-kalshi"

_FUNCTIONS = [*MARKET_FUNCTIONS, *REFERENCE_FUNCTIONS, *HISTORICAL_FUNCTIONS]

_EXAMPLE_QUERIES = examples(
    (
        "Browse the crypto series catalog",
        "SELECT ticker, title, frequency FROM kalshi.main.series WHERE category = 'Crypto' ORDER BY ticker",
    ),
    (
        "Open Bitcoin daily markets with their best quotes, busiest first",
        "SELECT ticker, title, yes_bid_dollars, yes_ask_dollars, volume_24h_fp "
        "FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' "
        "ORDER BY volume_24h_fp DESC",
    ),
    (
        "Order books for a whole series in one query, via LATERAL",
        "SELECT m.ticker, o.side, o.price_dollars, o.count_fp "
        "FROM kalshi.main.markets('KXBTCD') m, "
        "LATERAL kalshi.main.orderbook(m.ticker, cache_ttl => 30) o "
        "WHERE m.status = 'active' ORDER BY m.ticker, o.side, o.price_dollars",
    ),
    (
        "Hourly candles for every market in a series",
        "SELECT m.ticker, c.end_period_ts, c.price_close_dollars "
        "FROM kalshi.main.markets('KXBTCD') m, "
        "LATERAL kalshi.main.candlesticks(m.series_ticker, m.ticker, period_interval => 60) c "
        "ORDER BY m.ticker, c.end_period_ts",
    ),
    (
        "The most recent trades on the busiest open market in a series",
        "SELECT t.created_time, t.taker_side, t.yes_price_dollars, t.count_fp FROM ("
        "SELECT ticker FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' "
        "AND volume_24h_fp > 0 LIMIT 1) m, "
        "LATERAL kalshi.main.trades(m.ticker, max_rows => 100) t "
        "ORDER BY t.created_time DESC",
    ),
    (
        "Bitcoin daily events by strike date",
        "SELECT event_ticker, title, strike_date FROM kalshi.main.events('KXBTCD') ORDER BY strike_date",
    ),
    (
        "The busiest open market's current quote",
        "SELECT ticker, status, yes_bid_dollars, yes_ask_dollars "
        "FROM kalshi.main.market((SELECT ticker FROM kalshi.main.markets('KXBTCD') WHERE status = 'active' AND volume_24h_fp > 0 LIMIT 1))",
    ),
    (
        "Crypto series read from the scan function behind the table",
        "SELECT ticker, title FROM kalshi.main.all_series() WHERE category = 'Crypto' ORDER BY ticker",
    ),
)

#: Examples the linter actually runs against a live worker, so every one of them
#: has to be true right now — no market ticker (which expires), no row-count
#: assumption about a market that may not be trading today.
_EXECUTABLE_EXAMPLES = json.dumps(
    [
        {
            "name": "series_catalog_is_populated",
            "description": "The series catalog returns the whole exchange in one response.",
            "sql": "SELECT count(*) > 1000 FROM kalshi.main.series",
            "expected_result": [[True]],
        },
        {
            "name": "series_and_scan_function_agree",
            "description": "The `series` table and the `all_series` function behind it are the same rows.",
            "sql": (
                "SELECT (SELECT count(*) FROM kalshi.main.series) "
                "= (SELECT count(*) FROM kalshi.main.all_series())"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "markets_carry_their_series",
            "description": "Every market row is stamped with the series it was fetched from.",
            # Bounded deliberately: an aggregate over the bare scan consumes
            # every page, which is the unbounded shape the paging work exists
            # to stop paying for.
            "sql": (
                "SELECT bool_and(series_ticker = 'KXBTCD') FROM ("
                "SELECT series_ticker FROM kalshi.main.markets('KXBTCD') LIMIT 200)"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "events_belong_to_their_series",
            "description": "Events under a series all report that series.",
            "sql": (
                "SELECT bool_and(series_ticker = 'KXBTCD') FROM ("
                "SELECT series_ticker FROM kalshi.main.events('KXBTCD') LIMIT 100)"
            ),
            "expected_result": [[True]],
        },
        {
            "name": "a_limit_stops_the_scan_early",
            "description": (
                "A paged scan yields its first rows without walking every page — the property "
                "that keeps a LIMIT over a large series from wedging the client."
            ),
            "sql": "SELECT count(*) FROM (SELECT ticker FROM kalshi.main.markets('KXBTCD') LIMIT 5)",
            "expected_result": [[5]],
        },
        {
            "name": "prices_are_exact_decimals",
            "description": "Money comes back as DECIMAL, never a float.",
            "sql": ("SELECT typeof(yes_bid_dollars) FROM kalshi.main.markets('KXBTCD') LIMIT 1"),
            "expected_result": [["DECIMAL(18,4)"]],
        },
        {
            "name": "orderbook_sides_are_yes_or_no",
            "description": "A book flattens to labelled sides, or to nothing when it is empty.",
            "sql": (
                "SELECT bool_and(side IN ('yes', 'no')) IS NOT FALSE FROM ("
                "SELECT o.side FROM kalshi.main.markets('KXBTCD', status => 'open') m, "
                "LATERAL kalshi.main.orderbook(m.ticker) o LIMIT 50)"
            ),
            "expected_result": [[True]],
        },
    ]
)

#: The agent-suitability suite: tasks an analyst should be able to complete from
#: the catalog metadata alone. They are written to exercise the things this
#: worker is easy to get wrong — the two status vocabularies, and the fact that
#: candlesticks needs a series ticker as well as a market ticker.
_AGENT_TEST_TASKS = json.dumps(
    [
        {
            "name": "find_a_series",
            "prompt": "Which Kalshi series cover crypto? List a few of their tickers and titles.",
            "reference_sql": (
                "SELECT ticker, title FROM kalshi.main.series WHERE category = 'Crypto' "
                "ORDER BY ticker LIMIT 10"
            ),
            "success_criteria": "Returns series tickers from the Crypto category.",
            "unordered": True,
        },
        {
            "name": "open_markets_in_a_series",
            "prompt": (
                "For the Kalshi series KXBTCD, which markets are currently open for trading, "
                "and what is the best yes bid on each?"
            ),
            "reference_sql": (
                "SELECT ticker, yes_bid_dollars FROM kalshi.main.markets('KXBTCD') "
                "WHERE status = 'active' ORDER BY ticker"
            ),
            "success_criteria": (
                "Uses the markets() function with series KXBTCD and correctly identifies open "
                "markets — either by filtering status => 'open' or by the column value 'active'. "
                "Filtering the column for 'open', or passing 'active' as the argument, is wrong."
            ),
            "unordered": True,
        },
        {
            "name": "price_history",
            "prompt": (
                "Chart the last day of hourly closing prices for the most actively traded open "
                "market in the Kalshi series KXBTCD."
            ),
            "reference_sql": (
                "SELECT c.end_period_ts, c.price_close_dollars FROM ("
                "SELECT ticker, series_ticker FROM kalshi.main.markets('KXBTCD') "
                "WHERE status = 'active' ORDER BY volume_24h_fp DESC LIMIT 1) m, "
                "LATERAL kalshi.main.candlesticks(m.series_ticker, m.ticker, "
                "period_interval => 60) c ORDER BY c.end_period_ts"
            ),
            "success_criteria": (
                "Passes BOTH a series ticker and a market ticker to candlesticks(), and picks "
                "the market by volume_24h_fp."
            ),
        },
        {
            "name": "find_a_resolution_date",
            "prompt": ("When does the next Kalshi KXBTCD event resolve, and what source settles it?"),
            "reference_sql": (
                "SELECT event_ticker, strike_date, settlement_sources "
                "FROM kalshi.main.events('KXBTCD') ORDER BY strike_date LIMIT 1"
            ),
            "success_criteria": (
                "Uses events() rather than scanning markets, and reads strike_date and settlement_sources."
            ),
        },
        {
            "name": "single_market_quote",
            "prompt": (
                "Look up the current bid and ask for one specific Kalshi market, given only its "
                "ticker: pick any open market in KXBTCD and quote it by ticker."
            ),
            "reference_sql": (
                "SELECT ticker, yes_bid_dollars, yes_ask_dollars FROM kalshi.main.market("
                "(SELECT ticker FROM kalshi.main.markets('KXBTCD', status => 'open') LIMIT 1))"
            ),
            "success_criteria": (
                "Uses the single-market lookup market() for the point read rather than paging "
                "the whole series again."
            ),
        },
        {
            "name": "recent_prints",
            "prompt": (
                "What were the last few trades on any open market in the Kalshi series KXBTCD, "
                "and which side was the aggressor on?"
            ),
            "reference_sql": (
                "SELECT t.created_time, t.taker_side, t.yes_price_dollars, t.count_fp FROM ("
                "SELECT ticker FROM kalshi.main.markets('KXBTCD', status => 'open') LIMIT 1) m, "
                "LATERAL kalshi.main.trades(m.ticker, max_rows => 10) t "
                "ORDER BY t.created_time DESC"
            ),
            "success_criteria": (
                "Uses trades() for executed prints (not candlesticks or quotes) and reads taker_side."
            ),
            "unordered": True,
        },
        {
            "name": "count_the_catalog",
            "prompt": "How many series does Kalshi list in total, and how many are about crypto?",
            "reference_sql": (
                "SELECT count(*) AS total, count(*) FILTER (WHERE category = 'Crypto') AS crypto "
                "FROM kalshi.main.all_series()"
            ),
            "success_criteria": (
                "Counts the series catalog, through either the series table or the all_series "
                "function behind it."
            ),
        },
        {
            "name": "is_the_market_open",
            "prompt": (
                "Is Kalshi open for trading right now? If some venues are open and others are not, say which."
            ),
            "reference_sql": (
                "SELECT description, exchange_active, trading_active "
                "FROM kalshi.main.exchange_status ORDER BY exchange_index"
            ),
            "success_criteria": (
                "Uses exchange_status (or the all_exchange_status function behind it) and "
                "reports per-venue rather than treating the exchange as a single switch."
            ),
            "check_sql": (
                "SELECT count(*) = (SELECT count(*) FROM kalshi.main.all_exchange_status()) "
                "FROM kalshi.main.exchange_status"
            ),
            "unordered": True,
        },
        {
            "name": "which_side_of_the_archive",
            "prompt": (
                "Kalshi moves older data into a historical archive. As of now, how far back do "
                "the live endpoints go before you have to use the historical ones?"
            ),
            "reference_sql": (
                "SELECT market_settled_ts, trades_created_ts FROM kalshi.main.historical_cutoff"
            ),
            "check_sql": "SELECT count(*) = 1 FROM kalshi.main.all_historical_cutoff()",
            "success_criteria": (
                "Finds the cutoff table rather than guessing a date, and reports the timestamps "
                "as the boundary between the live and historical functions."
            ),
        },
        {
            "name": "how_it_settled",
            "prompt": (
                "For the Kalshi series KXBTCD, find some markets that have already settled and "
                "say what each one paid out."
            ),
            "reference_sql": (
                "SELECT ticker, result, settlement_value_dollars "
                "FROM kalshi.main.historical_markets('KXBTCD') "
                "ORDER BY close_time DESC LIMIT 10"
            ),
            "success_criteria": (
                "Uses historical_markets, not markets() — settled markets have been archived "
                "out of the live endpoint, which returns nothing for them rather than erroring."
            ),
            "unordered": True,
        },
        {
            "name": "price_before_settlement",
            "prompt": (
                "Take any settled market in the Kalshi series KXBTCD. How did its price move "
                "over its final days, and what were the last trades on it?"
            ),
            "reference_sql": [
                "SELECT c.end_period_ts, c.price_close_dollars FROM ("
                "SELECT ticker FROM kalshi.main.historical_markets('KXBTCD') "
                "ORDER BY close_time DESC LIMIT 1) m, "
                "LATERAL kalshi.main.historical_candlesticks(m.ticker, period_interval => 1440) c "
                "ORDER BY c.end_period_ts",
                "SELECT t.created_time, t.yes_price_dollars FROM ("
                "SELECT ticker FROM kalshi.main.historical_markets('KXBTCD') "
                "ORDER BY close_time DESC LIMIT 1) m, "
                "LATERAL kalshi.main.historical_trades(m.ticker, max_rows => 20) t "
                "ORDER BY t.created_time DESC",
            ],
            "success_criteria": (
                "Uses the historical candlestick and trade functions for an archived market, "
                "and passes only the market ticker to historical_candlesticks — unlike the live "
                "one, it is not scoped by series."
            ),
            "unordered": True,
        },
        {
            "name": "how_is_it_decided",
            "prompt": (
                "Pick any event in the Kalshi series KXBTCD. What question does it ask, when "
                "does it resolve, and what source decides the outcome?"
            ),
            "reference_sql": [
                "SELECT event_ticker, title, strike_date FROM kalshi.main.event("
                "(SELECT event_ticker FROM kalshi.main.events('KXBTCD') "
                "ORDER BY strike_date LIMIT 1))",
                "SELECT settlement_source_name, settlement_source_url "
                "FROM kalshi.main.event_metadata("
                "(SELECT event_ticker FROM kalshi.main.events('KXBTCD') "
                "ORDER BY strike_date LIMIT 1))",
            ],
            "success_criteria": (
                "Finds the settlement source via event_metadata rather than guessing from the "
                "title, and reports the event's strike date."
            ),
            "unordered": True,
        },
        {
            "name": "book_depth",
            "prompt": (
                "How much size is resting on each side of the book for any open market in the "
                "Kalshi series KXBTCD?"
            ),
            "reference_sql": (
                "SELECT o.side, sum(o.count_fp) FROM kalshi.main.markets("
                "'KXBTCD', status => 'open') m, LATERAL kalshi.main.orderbook(m.ticker) o "
                "GROUP BY o.side"
            ),
            "success_criteria": "Uses orderbook() and aggregates count_fp by side.",
            "unordered": True,
        },
    ]
)

_CATEGORIES = json.dumps(
    [
        {
            "name": "reference",
            "title": "Reference & Discovery",
            "description": "The series and event catalog — where you find the keys everything else requires.",
            "keywords": ["series", "events", "catalog", "discovery"],
        },
        {
            "name": "markets",
            "title": "Markets & Quotes",
            "description": "Tradeable contracts with their current prices, volume and lifecycle state.",
            "keywords": ["markets", "quotes", "prices", "contracts"],
        },
        {
            "name": "market-depth",
            "title": "Order Book Depth",
            "description": "Resting liquidity behind the best quote, level by level.",
            "keywords": ["orderbook", "depth", "liquidity", "spread"],
        },
        {
            "name": "history",
            "title": "Price History",
            "description": "How a contract traded over time: OHLC bars and the raw trade tape.",
            "keywords": ["candlesticks", "ohlc", "trades", "tape", "history"],
        },
        {
            "name": "historical",
            "title": "Settled & Archived",
            "description": (
                "Markets that have resolved, and the trades and candles under them, after "
                "Kalshi moves them out of the live endpoints."
            ),
            "keywords": ["historical", "archive", "settled", "backtest", "resolved"],
        },
    ]
)

_CATALOG_TAGS = {
    "provider": "kalshi",
    "domain": "prediction-markets",
    "vgi.title": "Kalshi Prediction Markets",
    "vgi.source_url": SOURCE_URL,
    "vgi.author": "Query Farm LLC <hello@query.farm>",
    "vgi.copyright": (
        "Worker (c) 2026 Query Farm LLC. Market data (c) Kalshi Inc., redistributed under "
        "Kalshi's terms of use."
    ),
    "vgi.license": "MIT",
    "vgi.support_contact": "https://github.com/Query-farm/vgi-kalshi/issues",
    "vgi.support_policy_url": "https://github.com/Query-farm/vgi-kalshi/blob/main/README.md",
    "vgi.keywords": keywords(
        "kalshi",
        "prediction markets",
        "event contracts",
        "orderbook",
        "candlesticks",
        "trades",
        "binary options",
        "forecasting",
    ),
    "vgi.executable_examples": _EXECUTABLE_EXAMPLES,
    "vgi.agent_test_tasks": _AGENT_TEST_TASKS,
    "vgi.doc_llm": (
        "Live and historical data from Kalshi, a US-regulated prediction-market exchange where "
        "contracts settle at $1 if an event happens and $0 if it does not, so a price reads "
        "directly as a probability. Reach for this catalog to answer what the market currently "
        "thinks about a future event, how that view has moved, or how much liquidity stands "
        "behind it. Everything is keyed off a series ticker, so start at the `series` table, "
        "take a ticker, and go to `markets()` from there. Read-only and public: no credentials, "
        "no account, no order entry."
    ),
    "vgi.doc_md": (
        "Kalshi is a CFTC-regulated exchange for event contracts. A contract pays $1 if its "
        "question resolves YES and $0 if it resolves NO, so its price is the market's implied "
        "probability of that outcome.\n\n"
        "### How the data is shaped\n\n"
        "Kalshi nests in three levels, and nearly every question starts by picking one at the "
        "top:\n\n"
        "- **Series** — a recurring question, such as KXBTCD for the Bitcoin daily price. This "
        "is the level the `series` table lists, and its ticker is the key almost everything "
        "else requires.\n"
        "- **Event** — one occurrence of a series, with a single resolution date.\n"
        "- **Market** — one strike within an event. This is what actually trades, and its "
        "ticker is what the per-market lookups take.\n\n"
        "### Reading prices\n\n"
        "Every `*_dollars` column is dollars per contract between 0 and 1, so a best bid of "
        "0.62 is a 62% implied probability. They are `DECIMAL`, never `DOUBLE`: Kalshi sends "
        "exact fixed-point strings, and rounding them through a float would throw that away "
        "silently. Contract counts (`*_fp`) are decimals for the same reason.\n\n"
        "### Access\n\n"
        "No credentials are needed. Kalshi's whole market-data surface is public; only "
        "portfolio and order endpoints require an API key, and this worker exposes none of "
        "them. It is read-only by construction, not by configuration.\n\n"
        "### Rate limits\n\n"
        "Unauthenticated traffic is limited tightly enough that a handful of back-to-back "
        "requests draws an HTTP 429. A correlated `LATERAL` issues one request per input row, "
        "so a join across a whole series is hundreds of calls. Requests are retried with "
        "exponential backoff, and the per-market functions take a `cache_ttl` argument that "
        "turns repeated lookups of the same ticker into cache hits when a few seconds of "
        "staleness is acceptable."
    ),
}

_SCHEMA_TAGS = {
    "provider": "kalshi",
    "domain": "prediction-markets",
    "vgi.title": "Kalshi Market Data",
    "vgi.categories": _CATEGORIES,
    "vgi.keywords": keywords(
        "kalshi",
        "prediction markets",
        "event contracts",
        "orderbook",
        "candlesticks",
        "trades",
    ),
    "vgi.example_queries": _EXAMPLE_QUERIES,
    "vgi.doc_llm": (
        "The whole read-only Kalshi surface, in one schema. Work top-down: the `series` table "
        "lists every recurring question on the exchange and needs no arguments, so start there "
        "and take a series ticker. That ticker unlocks the tradeable contracts under it, and "
        "each contract's ticker in turn unlocks resting order-book depth, OHLC price history "
        "and the raw trade tape. Every function is keyed — none of them will scan the whole "
        "exchange for you — and every one composes under a correlated LATERAL, so a per-row "
        "lookup becomes a set-based query without a second registration."
    ),
    "vgi.doc_md": (
        "One schema holding the whole read-only Kalshi surface.\n\n"
        "### Finding your way in\n\n"
        "Almost everything here is keyed, so the unkeyed `series` table is the way in: it "
        "lists every recurring question on the exchange, filterable by category. From a series "
        "ticker you reach the tradeable contracts, and from a contract ticker you reach depth, "
        "history and the trade tape.\n\n"
        "Contracts carry a `series_ticker` column that Kalshi's own payload does not include — "
        "it is stamped on by the function that fetched them, so their output can feed the "
        "price-history lookup, which needs a series ticker as well as a contract ticker, "
        "without you repeating it by hand.\n\n"
        "### Everything composes under LATERAL\n\n"
        "Every function here is blended: one registration serves both a literal call and a "
        "correlated `LATERAL`, so a per-row lookup composes into a set-based query. That is "
        "also what makes it easy to issue hundreds of requests by accident — see the catalog "
        "notes on rate limits and the `cache_ttl` argument.\n\n"
        "### The one real trap\n\n"
        "Kalshi filters and reports contract status in two different vocabularies. The `status` "
        "**argument** accepts `unopened`, `open`, `closed` and `settled`; the `status` "
        "**column** reports `initialized`, `active`, `closed`, `determined`, `settled` and "
        "`finalized`. They are not interchangeable — filter with `status => 'open'`, but match "
        "the column against `'active'`. A wrong argument is a hard error from Kalshi; a wrong "
        "predicate silently matches nothing."
    ),
}

_SERIES_DOCS = docs(
    category="reference",
    llm=(
        "The catalog of every recurring question on the exchange — around 13,600 rows, small "
        "and slow-changing. This is the discovery entry point: nearly every other object needs "
        "a series ticker, and this is where you find one. Filter by `category` to narrow to a "
        "topic such as Crypto, Politics or Economics."
    ),
    md=(
        "Every series Kalshi lists, as an ordinary table — no argument required.\n\n"
        "### Why this is a table and everything else is a function\n\n"
        "It has no required key, it is small enough to return whole in one response, and it "
        "changes slowly. The market-data functions are none of those things, which is why they "
        "take a key.\n\n"
        "### Where to go next\n\n"
        "Take a `ticker` and pass it to `markets()` for the tradeable contracts, `events()` for "
        "resolution dates, or `candlesticks()` for price history.\n\n"
    ),
    example_queries=examples(
        (
            "Browse the crypto series catalog",
            "SELECT ticker, title, frequency FROM kalshi.main.series "
            "WHERE category = 'Crypto' ORDER BY ticker",
        ),
        (
            "Series with the most markets, by category",
            "SELECT category, count(*) AS series_count FROM kalshi.main.series "
            "GROUP BY category ORDER BY series_count DESC",
        ),
    ),
    extra={
        "provider": "kalshi",
        "domain": "prediction-markets",
        "vgi.title": "Kalshi Series Catalog",
        "vgi.keywords": keywords("series", "catalog", "kalshi", "prediction markets", "discovery"),
    },
)

_EXCHANGE_STATUS_DOCS = docs(
    category="reference",
    llm=(
        "Whether Kalshi is open for trading right now, one row per venue. Check this before "
        "concluding that an empty order book or an unmoving price means anything — outside "
        "trading hours it means the venue is closed. Kalshi runs several venues that open and "
        "close independently, so the exchange-wide flag alone is not the answer."
    ),
    md=(
        "One row per trading venue, with the exchange-wide flags repeated on each.\n\n"
        "### Read the venue, not just the exchange\n\n"
        "Crypto trades around the clock while sports venues follow their seasons, so "
        "`exchange_active` does not tell you whether the contract you care about is "
        "tradeable. Match on `description`, or use the `*_overall` columns to ignore the "
        "distinction deliberately.\n\n"
        "### Freshness\n\n"
        "The most volatile thing Kalshi publishes, and it says so: `max-age=1`, the shortest "
        "TTL in the API. That directive is forwarded to the result cache rather than invented."
    ),
    example_queries=examples(
        (
            "Is the exchange open for trading right now?",
            "SELECT description, exchange_active, trading_active "
            "FROM kalshi.main.exchange_status ORDER BY exchange_index",
        ),
        (
            "Which venues are halted, if any",
            "SELECT description, trading_active FROM kalshi.main.exchange_status "
            "ORDER BY trading_active, exchange_index",
        ),
    ),
    extra={
        "provider": "kalshi",
        "domain": "prediction-markets",
        "vgi.title": "Exchange Trading Status",
        "vgi.keywords": keywords("exchange", "status", "trading hours", "open", "halt", "venue"),
    },
)

_CUTOFF_DOCS = docs(
    category="historical",
    llm=(
        "One row saying where Kalshi's live endpoints stop and its archive begins. Read it "
        "before querying a date range: the live functions return no rows — not an error — for "
        "anything older, so an empty result is otherwise indistinguishable from a market that "
        "never traded. Compare a date against `market_settled_ts` for markets and candles, or "
        "`trades_created_ts` for the tape, and pick the live or the historical function "
        "accordingly."
    ),
    md=(
        "One row, four timestamps, because Kalshi archives each kind of data on its own "
        "schedule.\n\n"
        "### Why this is a table and not a note in the docs\n\n"
        "The boundary moves. Hardcoding a date works until it quietly does not, and the "
        "failure mode is an empty result rather than an error — which reads like an answer.\n\n"
        "### Which column governs what\n\n"
        "`market_settled_ts` covers markets and their candles; `trades_created_ts` covers the "
        "trade tape. The remaining two describe order and position archives, which this "
        "read-only worker does not expose; they are reported because Kalshi reports them."
    ),
    example_queries=examples(
        (
            "Where does the archive begin?",
            "SELECT market_settled_ts, trades_created_ts FROM kalshi.main.historical_cutoff",
        ),
        (
            "Decide which functions serve a given date",
            "SELECT DATE '2026-08-01' >= market_settled_ts AS use_live_functions "
            "FROM kalshi.main.historical_cutoff",
        ),
    ),
    extra={
        "provider": "kalshi",
        "domain": "prediction-markets",
        "vgi.title": "Live / Archive Boundary",
        "vgi.keywords": keywords("historical", "archive", "cutoff", "boundary", "settled"),
    },
)

_KALSHI_CATALOG = Catalog(
    name="kalshi",
    default_schema="main",
    comment="Read-only Kalshi prediction-market data: markets, order books, candlesticks, trades, events, series",
    tags=_CATALOG_TAGS,
    schemas=[
        Schema(
            name="main",
            comment="Kalshi public market data — no credentials required",
            tags=_SCHEMA_TAGS,
            functions=list(_FUNCTIONS),
            tables=[
                Table(
                    name="series",
                    function=AllSeriesFunction,
                    comment="Every series on the exchange (small, slow-changing reference data)",
                    tags=_SERIES_DOCS,
                    column_comments=column_comments(SERIES_SCHEMA),
                    primary_key=(("ticker",),),
                    not_null=("ticker",),
                    # Inlined so the planner does not have to ask. The catalog
                    # is one unpaginated response whose size moves slowly — it
                    # was ~13,600 rows when this was measured — so an estimate
                    # that is stale by a few hundred still beats no estimate.
                    cardinality_estimate=13_600,
                    statistics_cache_max_age_seconds=3_600,
                ),
                Table(
                    name="exchange_status",
                    function=ExchangeStatusFunction,
                    comment="Whether the exchange, and each venue within it, is open for trading",
                    tags=_EXCHANGE_STATUS_DOCS,
                    column_comments=column_comments(EXCHANGE_STATUS_SCHEMA),
                    primary_key=(("exchange_index",),),
                    not_null=("exchange_index",),
                    cardinality_estimate=4,
                ),
                Table(
                    name="historical_cutoff",
                    function=HistoricalCutoffFunction,
                    comment="Where Kalshi's live endpoints stop and its historical archive begins",
                    tags=_CUTOFF_DOCS,
                    column_comments=column_comments(HISTORICAL_CUTOFF_SCHEMA),
                    cardinality_estimate=1,
                    cardinality_max=1,
                ),
            ],
        ),
    ],
)


#: How ATTACH may treat the optional `kalshi` credential.
class KalshiCatalog(ReadOnlyCatalogInterface):
    """Advertises the worker's versions, its optional credential, and the auth mode.

    Authentication is opt-in and, at Kalshi's entry tier, not a throughput win —
    see the rate-limit section of the README. The credential is a DuckDB secret
    rather than an ATTACH option because one of its two values is an RSA private
    key, and ATTACH option strings are visible in ``duckdb_databases()``.

    The one thing worth deciding at ATTACH time is what should happen when no
    credential resolves, which is what the ``auth`` option selects.
    """

    catalog = _KALSHI_CATALOG
    catalog_name = _KALSHI_CATALOG.name
    secret_types = [auth.SECRET_SPEC]
    attach_option_specs = [
        AttachOptionSpec(
            name="auth",
            desc=(
                "How to treat the optional 'kalshi' secret: 'auto' (default) signs when one "
                "is present and uses public access otherwise; 'required' fails the query "
                "when none resolves; 'off' never signs."
            ),
            type=pa.string(),
            default=auth.AUTO,
        )
    ]

    def catalog_attach(self, *, name: str, options: dict[str, Any], **kwargs: Any) -> CatalogAttachResult:
        """Validate the ``auth`` option and carry it through to the functions.

        An unknown mode is rejected here rather than ignored, because every way
        of getting it wrong is otherwise silent: a typo'd 'require' would read as
        'auto' and quietly serve anonymous data to a caller who asked for the
        opposite.

        The mode then becomes the catalog's attach bytes, which the framework
        hands back to every ``process()`` as ``attach_opaque_data`` — the only
        route an ATTACH-time choice has into a function body. The base class
        returns a fixed constant there, which is why this replaces it.
        """
        mode = str(options.get("auth") or auth.AUTO).strip().lower()
        if mode not in auth.MODES:
            raise ValueError(f"ATTACH option auth => {mode!r} is not one of {', '.join(auth.MODES)}")
        result = super().catalog_attach(name=name, options=options, **kwargs)
        return replace(
            result,
            attach_opaque_data=AttachOpaqueData(mode.encode()),
            attach_opaque_data_required=True,
        )

    def catalogs(self) -> list[CatalogInfo]:
        """Advertise the single read-only Kalshi catalog."""
        return [
            CatalogInfo(
                name=self._effective_catalog_name,
                implementation_version=IMPLEMENTATION_VERSION,
                data_version_spec=DATA_VERSION_SPEC,
                source_url=SOURCE_URL,
                attach_option_specs=[spec.serialize() for spec in self.attach_option_specs],
            )
        ]


class KalshiWorker(Worker):
    """Worker process hosting the read-only Kalshi catalog."""

    catalog = _KALSHI_CATALOG
    catalog_interface = KalshiCatalog


def main() -> None:
    """Run the worker (stdio by default; pass ``--http`` for the HTTP server)."""
    KalshiWorker.main()


def main_http() -> None:
    """Run the worker over HTTP."""
    argv = sys.argv[1:]
    if "--http" not in argv:
        argv = ["--http", *argv]
    sys.argv = [sys.argv[0], *argv]
    KalshiWorker.main()

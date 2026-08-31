"""The catalog shape the DuckDB extension will see on ATTACH."""

from __future__ import annotations

import json
import re
from pathlib import Path

from vgi_kalshi.markets import (
    CandlesticksFunction,
    EventFunction,
    EventMetadataFunction,
    EventsFunction,
    MarketFunction,
    MarketsFunction,
    OrderbookFunction,
    TradesFunction,
)
from vgi_kalshi.historical import (
    HistoricalCandlesticksFunction,
    HistoricalCutoffFunction,
    HistoricalMarketsFunction,
    HistoricalTradesFunction,
)
from vgi_kalshi.reference import AllSeriesFunction, ExchangeStatusFunction
from vgi_kalshi.worker import _KALSHI_CATALOG

PACKAGE = Path(__file__).resolve().parent.parent / "vgi_kalshi"

#: Bounded fetches: one market, one book, one window. These stay blended, so
#: they compose as the inner side of a correlated LATERAL.
BLENDED = [
    MarketFunction,
    OrderbookFunction,
    CandlesticksFunction,
    TradesFunction,
    HistoricalTradesFunction,
    HistoricalCandlesticksFunction,
    EventFunction,
    EventMetadataFunction,
]

#: Cursor-paged endpoints. These are stateful scans: they emit one API page per
#: tick so a LIMIT stops early, which a blended function cannot do.
PAGED_SCANS = [MarketsFunction, EventsFunction, HistoricalMarketsFunction]


class TestBlendedRegistration:
    """Blended functions are what make correlated LATERAL work."""

    def test_positional_args_are_input_columns(self) -> None:
        for func in BLENDED:
            assert func.get_metadata().input_from_args is True, func.Meta.name

    def test_no_finalize_override(self) -> None:
        """DuckDB rejects LATERAL on a table function registering a finalize callback."""
        for func in BLENDED:
            assert func.has_finalize_override() is False, func.Meta.name


class TestPagedScans:
    """A cursor-paged endpoint must be a stateful scan, not a blended map.

    A blended function has to emit everything for its input in one `process()`
    call, so it walks every page before DuckDB sees a row — a `LIMIT 10` pays
    for the whole series, and since a scan blocked inside its first batch cannot
    be cancelled, the query wedges the client rather than merely being slow.
    That is what `vgi-lint`'s VGI911 caught on all three of these.
    """

    def test_paged_scans_are_not_blended(self) -> None:
        for func in PAGED_SCANS:
            assert func.get_metadata().input_from_args is False, func.Meta.name

    def test_paged_scans_carry_cursor_state(self) -> None:
        """State between ticks is the whole mechanism; without it there is no resume."""
        from vgi_kalshi.paging import PagedScanState

        for func in PAGED_SCANS:
            state = func.initial_state(None)  # type: ignore[arg-type]
            assert isinstance(state, PagedScanState), func.Meta.name
            assert state.cursor == "" and state.done is False, func.Meta.name

    def test_the_two_sets_are_disjoint_and_complete(self) -> None:
        """Every function is one or the other, so neither list can silently rot."""
        schema = _KALSHI_CATALOG.schemas[0]
        classified = {f.Meta.name for f in BLENDED} | {f.Meta.name for f in PAGED_SCANS}
        scans = {"all_series", "all_exchange_status", "all_historical_cutoff"}
        assert {f.Meta.name for f in schema.functions} == classified | scans
        assert not ({f.Meta.name for f in BLENDED} & {f.Meta.name for f in PAGED_SCANS})


class TestCatalogShape:
    def test_function_names_are_unqualified(self) -> None:
        names = {f.Meta.name for f in _KALSHI_CATALOG.schemas[0].functions}
        assert names == {
            "markets",
            "market",
            "orderbook",
            "candlesticks",
            "trades",
            "events",
            "event",
            "event_metadata",
            "all_series",
            "all_exchange_status",
            "historical_markets",
            "historical_trades",
            "historical_candlesticks",
            "all_historical_cutoff",
        }
        assert not any(n.startswith("kalshi_") for n in names)

    def test_unkeyed_scans_are_exposed_as_tables(self) -> None:
        """A scan that needs no argument reads better as a table than a function."""
        tables = {t.name: t.function for t in _KALSHI_CATALOG.schemas[0].tables}
        assert tables == {
            "series": AllSeriesFunction,
            "exchange_status": ExchangeStatusFunction,
            "historical_cutoff": HistoricalCutoffFunction,
        }

    def test_table_name_does_not_collide_with_a_function(self) -> None:
        """`series` the table is backed by `all_series` the function, so both can coexist."""
        schema = _KALSHI_CATALOG.schemas[0]
        function_names = {f.Meta.name for f in schema.functions}
        assert {t.name for t in schema.tables}.isdisjoint(function_names)


#: Kalshi speaks two status vocabularies. These are the values the `status`
#: **filter argument** accepts; `active` and `finalized` are both 400
#: `invalid status filter`.
FILTER_STATUSES = {"unopened", "open", "closed", "settled"}

#: And these are the values the `status` **column** of a market actually holds.
#: `status => 'open'` returns rows reading `active`, which is why a predicate
#: and a filter that mean the same thing are spelled differently.
COLUMN_STATUSES = {"initialized", "active", "closed", "determined", "settled", "finalized"}

_STATUS_FILTER = re.compile(r"status\s*=>\s*'([^']*)'")
_STATUS_PREDICATE = re.compile(r"status\s*=\s*'([^']*)'")


def _shipped_sql() -> list[str]:
    """Every SQL string this worker advertises, from every tag that can carry one.

    Examples reach a client through several independent channels — the schema's
    `vgi.example_queries`, each object's own, the framework's native
    `Meta.examples`, and the catalog's executable suite — so the checks below
    have to sweep all of them or they would pass while a whole channel rots.
    """
    schema = _KALSHI_CATALOG.schemas[0]
    carriers: list[dict[str, str]] = [
        _KALSHI_CATALOG.tags,
        schema.tags,
        *(table.tags for table in schema.tables),
        *(getattr(function.Meta, "tags", {}) for function in schema.functions),
    ]
    statements: list[str] = []
    for tags in carriers:
        for key in ("vgi.example_queries", "vgi.executable_examples"):
            for entry in json.loads(tags.get(key) or "[]"):
                sql = entry["sql"]
                statements.extend(sql if isinstance(sql, list) else [sql])
    for function in schema.functions:
        statements.extend(example.sql for example in getattr(function.Meta, "examples", []))
    return statements


class TestShippedExamples:
    """The examples are the discovery surface, so a wrong one is a wrong product.

    The trap they have to stay clear of is Kalshi's split status vocabulary: a
    filter and a predicate that select the same markets are spelled differently,
    and getting either wrong fails quietly — a bad filter is a 400, but a bad
    predicate just returns nothing at all.
    """

    def test_there_are_examples_to_check(self) -> None:
        """Guard the guard: these checks are vacuous if nothing is collected."""
        assert len(_shipped_sql()) >= 20

    def test_status_filters_use_the_filter_vocabulary(self) -> None:
        offenders = [
            (sql, status)
            for sql in _shipped_sql()
            for status in _STATUS_FILTER.findall(sql)
            if status not in FILTER_STATUSES
        ]
        assert offenders == [], f"status => filters Kalshi rejects with a 400: {offenders}"

    def test_status_predicates_use_the_column_vocabulary(self) -> None:
        offenders = [
            (sql, status)
            for sql in _shipped_sql()
            for status in _STATUS_PREDICATE.findall(sql)
            if status not in COLUMN_STATUSES
        ]
        assert offenders == [], f"predicates on a value the column never holds: {offenders}"

    def test_the_two_vocabularies_really_are_different(self) -> None:
        """If they ever converge, the split above stops being worth its weight."""
        assert FILTER_STATUSES != COLUMN_STATUSES
        assert "active" not in FILTER_STATUSES
        assert "open" not in COLUMN_STATUSES

    def test_examples_reference_only_registered_functions(self) -> None:
        names = {f.Meta.name for f in _KALSHI_CATALOG.schemas[0].functions}
        names |= {t.name for t in _KALSHI_CATALOG.schemas[0].tables}
        called = {m for sql in _shipped_sql() for m in re.findall(r"kalshi(?:\.main)?\.(\w+)", sql)}
        assert called, "no example names any object"
        assert called <= names, f"examples reference unknown objects: {sorted(called - names)}"

    def test_every_object_is_demonstrated(self) -> None:
        """Each function and table must be called by at least one shipped example."""
        schema = _KALSHI_CATALOG.schemas[0]
        objects = {f.Meta.name for f in schema.functions} | {t.name for t in schema.tables}
        called = {m for sql in _shipped_sql() for m in re.findall(r"kalshi(?:\.main)?\.(\w+)", sql)}
        assert objects <= called, f"never demonstrated: {sorted(objects - called)}"

    def test_examples_are_catalog_qualified(self) -> None:
        """An unqualified example does not run for a client that attached the catalog."""
        offenders = [sql for sql in _shipped_sql() if "kalshi.main." not in sql]
        assert offenders == [], f"examples not qualified as kalshi.main.*: {offenders}"


class TestExampleChannels:
    """The native `Meta.examples` and the `vgi.example_queries` tag must agree.

    A client sees both: the framework's native examples column carries only SQL,
    while the tag carries SQL plus a description. When the same example is spelled
    two slightly different ways, a consumer merging them ends up with a second,
    descriptionless copy — so every native example must be one of the tagged ones
    verbatim.
    """

    def test_native_examples_are_tagged_verbatim(self) -> None:
        offenders: list[tuple[str, str]] = []
        for function in _KALSHI_CATALOG.schemas[0].functions:
            tags = getattr(function.Meta, "tags", {})
            tagged = {e["sql"] for e in json.loads(tags.get("vgi.example_queries") or "[]")}
            offenders += [
                (function.Meta.name, example.sql)
                for example in getattr(function.Meta, "examples", [])
                if example.sql not in tagged
            ]
        assert offenders == [], f"native examples missing from vgi.example_queries: {offenders}"

    def test_every_object_declares_its_result_columns(self) -> None:
        """A table function's columns are invisible until it binds, so it must declare them."""
        for function in _KALSHI_CATALOG.schemas[0].functions:
            tags = getattr(function.Meta, "tags", {})
            declared = json.loads(tags["vgi.result_columns_schema"])
            assert [c["name"] for c in declared] == function.FIXED_SCHEMA.names, function.Meta.name
            assert all(c["description"] for c in declared), function.Meta.name


class TestNoDeadApiSurface:
    """Every endpoint wrapper must be reachable, from SQL or from a test.

    This has gone wrong twice. `event_metadata()` was written, then deleted as
    unreachable, then written again and left unreachable a second time — each
    time invisibly, because an unused function breaks nothing. A wrapper that no
    SQL function calls and no test exercises is either a missing feature or
    dead weight, and both are worth failing the build over.
    """

    #: Modules that put an endpoint on the SQL surface.
    SURFACE = ("markets.py", "reference.py", "historical.py", "paging.py")

    @staticmethod
    def _public_api_functions() -> set[str]:
        """Top-level public functions defined in kalshi_api, by name."""
        import ast

        tree = ast.parse((PACKAGE / "kalshi_api.py").read_text())
        return {
            node.name
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and not node.name.startswith("_")
        }

    def test_every_endpoint_wrapper_is_reachable(self) -> None:
        api_source = (PACKAGE / "kalshi_api.py").read_text()
        surface = "".join((PACKAGE / name).read_text() for name in self.SURFACE)
        # Every test file, conftest included — a fixture is a real caller.
        tests = "".join(path.read_text() for path in PACKAGE.parent.glob("tests/*.py"))
        # `base_url` is plumbing, not an endpoint.
        candidates = self._public_api_functions() - {"base_url"}
        orphaned = [
            name
            for name in sorted(candidates)
            if f"api.{name}(" not in surface
            and f"api.{name}(" not in tests
            and f"kalshi_api.{name}(" not in tests
            # Called from inside kalshi_api itself: the definition contributes
            # one occurrence, so more than one means a real call site.
            and api_source.count(f"{name}(") <= 1
        ]
        assert orphaned == [], (
            f"kalshi_api functions reachable from neither SQL nor a test: {orphaned}. "
            "Expose them as table functions or delete them."
        )

    def test_the_check_can_actually_fail(self) -> None:
        """Guard the guard: an obviously-unreachable name must be detected."""
        surface = "".join((PACKAGE / name).read_text() for name in self.SURFACE)
        assert "api.definitely_not_a_real_endpoint(" not in surface


class TestExamplesDoNotRot:
    """A live example must not hardcode a market ticker.

    Kalshi contracts expire. An example naming one works until that contract
    settles and then quietly returns nothing — which is how `trades` shipped an
    example that vgi-lint's execute tier flagged as empty. Live examples derive
    their ticker from a query instead. Archived tickers are exempt: the archive
    is immutable, so a settled ticker there is permanent and deterministic.
    """

    #: A Kalshi market ticker: SERIES-EVENTDATE-STRIKE.
    _MARKET_TICKER = re.compile(r"'[A-Z0-9]+-[0-9]{2}[A-Z]{3}[0-9]{2,4}-[TB][0-9.]+'")

    def test_no_live_example_hardcodes_a_market_ticker(self) -> None:
        offenders = [
            sql for sql in _shipped_sql() if "historical" not in sql and self._MARKET_TICKER.search(sql)
        ]
        assert offenders == [], (
            "these examples name a contract that will expire; derive the ticker from "
            f"a subquery instead: {offenders}"
        )

    def test_the_pattern_matches_a_real_ticker(self) -> None:
        """Guard the guard: a regex that matches nothing would pass vacuously."""
        assert self._MARKET_TICKER.search("FROM f('KXBTCD-26AUG3117-T87749.99')")

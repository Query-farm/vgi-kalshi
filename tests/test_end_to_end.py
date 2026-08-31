"""SQL executed against a real ATTACH.

Every serious defect in this worker was found by running queries through DuckDB,
not by a unit test — a scan that wedged the client uncancellably, a timestamp
that crashed every consumer of a row, and a filter-pushdown bug that made
`WHERE volume_24h_fp > 999999999` return rows. That last one is the reason this
file exists: the worker's own tests all called the API layer directly, so
nothing exercised the contract the extension actually holds it to.

Marked `live` because a real ATTACH necessarily talks to Kalshi.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

pytestmark = pytest.mark.live

#: The series the rest of the live suite uses.
SERIES = "KXBTCD"


@pytest.fixture(scope="module")
def con() -> Iterator[Any]:
    """A DuckDB connection with the worker attached.

    Unsigned extensions are allowed because the vgi extension ships from the
    community repository unsigned; this is a test process, not a deployment.
    """
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        connection.execute("INSTALL vgi FROM community")
        connection.execute("LOAD vgi")
        connection.execute("ATTACH 'kalshi' (TYPE vgi, LOCATION 'uv run kalshi_worker.py')")
    except Exception as exc:  # pragma: no cover - environment, not the worker
        pytest.skip(f"cannot attach the worker: {exc}")
    yield connection
    connection.close()


class TestFiltersAreApplied:
    """Declaring `filter_pushdown` makes the engine drop its own filter.

    So a predicate the worker fails to apply is not applied by anyone. These
    assertions are the difference between that being caught and it silently
    returning wrong rows, which is how it shipped.
    """

    @pytest.mark.parametrize(
        ("label", "sql"),
        [
            (
                "numeric predicate no row can satisfy",
                "SELECT count(*) FROM (SELECT ticker FROM kalshi.main.markets('KXBTCD') "
                "WHERE volume_24h_fp > 999999999 LIMIT 5)",
            ),
            (
                "string predicate no row can satisfy",
                "SELECT count(*) FROM (SELECT ticker FROM kalshi.main.markets('KXBTCD') "
                "WHERE ticker = 'NO-SUCH-TICKER' LIMIT 5)",
            ),
            (
                "predicate on a scan with its own API filter",
                "SELECT count(*) FROM (SELECT event_ticker FROM kalshi.main.events('KXBTCD') "
                "WHERE event_ticker = 'NO-SUCH-EVENT' LIMIT 5)",
            ),
            (
                "predicate on the series table",
                "SELECT count(*) FROM kalshi.main.series WHERE category = 'NoSuchCategory'",
            ),
        ],
    )
    def test_an_impossible_predicate_returns_nothing(self, con: Any, label: str, sql: str) -> None:
        assert con.execute(sql).fetchall() == [(0,)], label

    def test_an_unfiltered_scan_still_returns_rows(self, con: Any) -> None:
        """Guard the guard: filtering everything out would pass the tests above."""
        got = con.execute(
            "SELECT count(*) FROM (SELECT ticker FROM kalshi.main.markets('KXBTCD') LIMIT 5)"
        ).fetchall()
        assert got == [(5,)]

    def test_a_real_predicate_selects_matching_rows(self, con: Any) -> None:
        """And the rows that come back must actually satisfy it."""
        rows = con.execute(
            "SELECT ticker, volume_24h_fp FROM kalshi.main.markets('KXBTCD') "
            "WHERE status = 'active' AND volume_24h_fp > 100 LIMIT 3"
        ).fetchall()
        if not rows:
            pytest.skip("no actively traded KXBTCD market right now")
        assert all(volume > 100 for _, volume in rows)

    def test_the_series_category_filter_narrows(self, con: Any) -> None:
        """The 71x case: pushed to the API, and still correct."""
        crypto = con.execute("SELECT count(*) FROM kalshi.main.series WHERE category = 'Crypto'").fetchone()[
            0
        ]
        every = con.execute("SELECT count(*) FROM kalshi.main.series").fetchone()[0]
        assert 0 < crypto < every


class TestScansAreResponsive:
    """A scan blocked inside its first batch cannot be cancelled.

    Before paging, `SELECT * FROM markets(...) LIMIT 10` walked every page of the
    series before emitting anything and wedged the client. vgi-lint's VGI911
    caught it; this keeps it caught.
    """

    @pytest.mark.parametrize(
        "relation",
        [
            "kalshi.main.markets('KXBTCD')",
            "kalshi.main.events('KXBTCD')",
            "kalshi.main.historical_markets('KXBTCD')",
        ],
    )
    def test_a_limit_returns_promptly(self, con: Any, relation: str) -> None:
        import time

        start = time.perf_counter()
        rows = con.execute(f"SELECT * FROM {relation} LIMIT 10").fetchall()
        elapsed = time.perf_counter() - start
        assert len(rows) <= 10
        assert elapsed < 30, f"{relation} took {elapsed:.1f}s to yield its first rows"


class TestRowsAreMaterializable:
    """Every value must survive the trip into a client.

    Kalshi sends Go's zero time as "unset". Arrow held it happily at microsecond
    resolution and every nanosecond-resolution consumer then raised
    `OverflowError` — a row that could be produced but not read.
    """

    def test_every_column_of_an_event_can_be_fetched(self, con: Any) -> None:
        rows = con.execute(
            "SELECT * FROM kalshi.main.event((SELECT event_ticker FROM kalshi.main.events('KXBTCD') LIMIT 1))"
        ).fetchall()
        assert len(rows) <= 1

    def test_a_wide_market_scan_materializes(self, con: Any) -> None:
        rows = con.execute("SELECT * FROM kalshi.main.markets('KXBTCD') LIMIT 50").fetchall()
        assert rows

    def test_the_series_table_materializes(self, con: Any) -> None:
        assert con.execute("SELECT count(*) FROM kalshi.main.series").fetchone()[0] > 1000


class TestCatalogShape:
    """The objects a client sees on ATTACH."""

    def test_every_declared_object_is_queryable(self, con: Any) -> None:
        from vgi_kalshi.worker import _KALSHI_CATALOG

        tables = {t.name for t in _KALSHI_CATALOG.schemas[0].tables}
        for table in tables:
            con.execute(f"SELECT * FROM kalshi.main.{table} LIMIT 1").fetchall()

    def test_prices_arrive_as_decimals(self, con: Any) -> None:
        """Money must not become a float on the way out."""
        kind = con.execute(
            "SELECT typeof(yes_bid_dollars) FROM kalshi.main.markets('KXBTCD') LIMIT 1"
        ).fetchone()
        assert kind is None or kind[0] == "DECIMAL(18,4)"

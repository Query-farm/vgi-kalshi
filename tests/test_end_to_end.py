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
    """A Haybarn connection with the worker attached.

    Haybarn is Query Farm's DuckDB distribution and ships the vgi extension, so
    nothing is fetched from the community repository at test time.
    """
    haybarn = pytest.importorskip("haybarn")
    connection = haybarn.connect()
    try:
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


class TestMicrostructureColumns:
    """The four columns added in 1.1.0, against live payloads.

    Offline tests fix the shapes, but only a real scan proves Kalshi still
    sends them and that the nested coercion survives the round trip. A struct
    of decimals is exactly the sort of column that builds as all-NULL without
    anyone noticing.
    """

    def test_price_ranges_arrive_as_usable_decimals(self, con) -> None:
        rows = con.execute(
            "SELECT price_ranges FROM kalshi.main.markets(?, status => 'open') "
            "WHERE price_ranges IS NOT NULL LIMIT 5",
            [SERIES],
        ).fetchall()
        assert rows, "no market carried a price ladder"
        for (bands,) in rows:
            assert bands, "price_ranges present but empty"
            for band in bands:
                assert band["step"] is not None and band["step"] > 0

    def test_spread_can_be_measured_in_ticks(self, con) -> None:
        """The reason the column exists: a spread is only comparable in ticks."""
        (ticks,) = con.execute(
            "SELECT median((yes_ask_dollars - yes_bid_dollars) / list_filter(price_ranges, "
            'r -> r.start <= yes_bid_dollars AND r."end" > yes_bid_dollars)[1].step) '
            "FROM kalshi.main.markets(?, status => 'open') WHERE yes_bid_dollars > 0",
            [SERIES],
        ).fetchone()
        assert ticks is not None and ticks >= 1

    def test_updated_time_is_a_real_instant(self, con) -> None:
        (stale,) = con.execute(
            "SELECT count(*) FROM kalshi.main.markets(?, status => 'open') WHERE updated_time IS NULL",
            [SERIES],
        ).fetchone()
        assert stale == 0, "every open market should carry an updated_time"

    def test_custom_strike_reads_as_a_map(self, con) -> None:
        """KXFEDDECISION is the canonical categorical-outcome series."""
        rows = con.execute(
            "SELECT custom_strike['Hike'], custom_strike['Cut'] "
            "FROM kalshi.main.markets('KXFEDDECISION', status => 'open') "
            "WHERE custom_strike IS NOT NULL LIMIT 5"
        ).fetchall()
        assert rows, "KXFEDDECISION carried no custom_strike"
        assert any(hike or cut for hike, cut in rows)

    def test_expiration_value_survives_a_non_numeric_settlement(self, con) -> None:
        """It is VARCHAR precisely because KXFEDDECISION settles to prose."""
        rows = con.execute(
            "SELECT DISTINCT expiration_value FROM kalshi.main.historical_markets('KXFEDDECISION') "
            "WHERE expiration_value <> '' LIMIT 5"
        ).fetchall()
        if not rows:
            pytest.skip("no settled KXFEDDECISION markets in the archive window")
        assert all(isinstance(v, str) for (v,) in rows)

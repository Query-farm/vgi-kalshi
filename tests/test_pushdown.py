"""Projection and filter pushdown.

Pushdown here is a pure optimization: `filters_exactly_applied` is False, so
DuckDB re-checks every predicate against whatever we return. That makes
declining to push a filter merely wasteful, and pushing a *wrong* one the only
real hazard — it drops rows we then never fetch, and nothing downstream can
recover them. These tests are mostly about the second case.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pytest

from vgi_kalshi.markets import (
    _STATUS_COLUMN_TO_FILTER,
    MarketsArgs,
    MarketsFunction,
    _pushed_market_filters,
)
from vgi_kalshi.schemas import MARKET_SCHEMA, batch_from_rows


class _Filters:
    """The slice of PushdownFilters this code actually uses."""

    def __init__(self, constants: dict[str, str]) -> None:
        self._constants = constants

    def get_column_constant(self, column_name: str) -> pa.Scalar[Any] | None:
        value = self._constants.get(column_name)
        return pa.scalar(value) if value is not None else None


@dataclass
class _Params:
    args: MarketsArgs
    current_pushdown_filters: Any = None


def _push(constants: dict[str, str] | None, **args: str) -> tuple[str | None, str | None]:
    """Run the translation for a series scan carrying ``constants`` as its WHERE."""
    return _pushed_market_filters(
        _Params(  # type: ignore[arg-type]
            args=MarketsArgs(series_ticker="KXBTCD", **args),
            current_pushdown_filters=_Filters(constants) if constants else None,
        )
    )


class TestStatusTranslation:
    """`WHERE status = 'active'` has to become `status => 'open'`, or not push at all."""

    def test_verified_pairs_translate(self) -> None:
        assert _push({"status": "active"}) == (None, "open")
        assert _push({"status": "initialized"}) == (None, "unopened")
        assert _push({"status": "finalized"}) == (None, "settled")

    @pytest.mark.parametrize("status", ["closed", "determined", "open", "nonsense", ""])
    def test_unverified_or_wrong_vocabulary_is_not_pushed(self, status: str) -> None:
        """Pushing an unproven mapping would silently drop rows — the one thing to avoid.

        Note `open` is in this list: it is a *filter* word, so seeing it as a
        column predicate means the caller has the vocabularies backwards, and
        their query matches nothing. Pushing it would fetch the rows they did
        not ask for and DuckDB would discard them anyway.
        """
        assert _push({"status": status}) == (None, None)

    def test_the_map_never_translates_into_the_column_vocabulary(self) -> None:
        """A value must never map to something the filter argument rejects."""
        accepted = {"unopened", "open", "closed", "settled"}
        assert set(_STATUS_COLUMN_TO_FILTER.values()) <= accepted

    def test_the_map_is_injective(self) -> None:
        """Two column states collapsing to one filter would over-fetch silently."""
        values = list(_STATUS_COLUMN_TO_FILTER.values())
        assert len(values) == len(set(values))


class TestEventTicker:
    """`event_ticker` needs no translation — same field, same vocabulary."""

    def test_pushed_verbatim(self) -> None:
        assert _push({"event_ticker": "KXBTCD-26SEP0417"}) == ("KXBTCD-26SEP0417", None)

    def test_both_filters_together(self) -> None:
        assert _push({"event_ticker": "E", "status": "active"}) == ("E", "open")


class TestArgumentPrecedence:
    """An explicit argument wins; the predicate narrows the result regardless."""

    def test_named_status_beats_a_pushed_one(self) -> None:
        assert _push({"status": "active"}, status="settled") == (None, None)

    def test_named_event_beats_a_pushed_one(self) -> None:
        assert _push({"event_ticker": "A"}, event_ticker="B") == (None, None)

    def test_no_filters_pushes_nothing(self) -> None:
        assert _push(None) == (None, None)

    def test_unrelated_predicate_pushes_nothing(self) -> None:
        assert _push({"title": "Bitcoin"}) == (None, None)


class TestProjection:
    """A narrow SELECT must stop building the columns it did not ask for."""

    def test_declared_on_every_function(self) -> None:
        from vgi_kalshi.worker import _KALSHI_CATALOG

        for function in _KALSHI_CATALOG.schemas[0].functions:
            assert function.get_metadata().projection_pushdown is True, function.Meta.name

    def test_a_projected_schema_builds_only_its_columns(self) -> None:
        projected = pa.schema([MARKET_SCHEMA.field("ticker"), MARKET_SCHEMA.field("status")])
        rows = [{"ticker": "T", "status": "active", "title": "ignored", "yes_bid_dollars": "0.5000"}]
        batch = batch_from_rows(rows, projected)
        assert batch.schema == projected
        assert batch.num_columns == 2
        assert batch.column("ticker").to_pylist() == ["T"]

    def test_filters_are_rechecked_by_duckdb(self) -> None:
        """We translate only some predicates, so we must not claim exactness."""
        assert MarketsFunction.get_metadata().filters_exactly_applied is False


class _Bounds:
    def __init__(self, low: Any = None, high: Any = None) -> None:
        self.min_value = pa.scalar(low) if low is not None else None
        self.max_value = pa.scalar(high) if high is not None else None


class _RangeFilters:
    """The slice of PushdownFilters the range helpers use."""

    def __init__(self, column: str, low: Any = None, high: Any = None) -> None:
        self._column, self._bounds = column, _Bounds(low, high)

    def get_column_constant(self, column_name: str) -> None:
        return None

    def get_column_bounds(self, column_name: str) -> _Bounds | None:
        return self._bounds if column_name == self._column else None


class TestEpochBounds:
    """A time predicate becomes the endpoint's own window, or nothing at all."""

    @staticmethod
    def _params(filters: Any) -> Any:
        return _Params(args=MarketsArgs(series_ticker="K"), current_pushdown_filters=filters)

    def test_datetime_bounds_become_epoch_seconds(self) -> None:
        from datetime import UTC, datetime

        from vgi_kalshi.pushdown import epoch_bounds

        low = datetime(2026, 8, 1, tzinfo=UTC)
        high = datetime(2026, 8, 2, tzinfo=UTC)
        got = epoch_bounds(self._params(_RangeFilters("created_time", low, high)), "created_time")  # type: ignore[arg-type]
        assert got == (int(low.timestamp()), int(high.timestamp()) + 1)

    def test_the_upper_bound_is_widened_by_a_second(self) -> None:
        """Kalshi's window is inclusive; narrowing would drop a row on the boundary.

        Fetching one extra second is free — DuckDB re-checks the predicate — while
        excluding a trade that landed exactly on the bound would be a wrong answer.
        """
        from datetime import UTC, datetime

        from vgi_kalshi.pushdown import epoch_bounds

        high = datetime(2026, 8, 2, tzinfo=UTC)
        _, got_high = epoch_bounds(self._params(_RangeFilters("created_time", None, high)), "created_time")  # type: ignore[arg-type]
        assert got_high == int(high.timestamp()) + 1

    def test_no_filters_pushes_nothing(self) -> None:
        from vgi_kalshi.pushdown import epoch_bounds

        assert epoch_bounds(self._params(None), "created_time") == (None, None)  # type: ignore[arg-type]

    def test_a_different_column_pushes_nothing(self) -> None:
        from vgi_kalshi.pushdown import epoch_bounds

        filters = _RangeFilters("something_else", 1, 2)
        assert epoch_bounds(self._params(filters), "created_time") == (None, None)  # type: ignore[arg-type]

    def test_booleans_are_not_epochs(self) -> None:
        """`bool` is an `int`; True must not become 1970 plus a second."""
        from vgi_kalshi.pushdown import epoch_bounds

        got = epoch_bounds(self._params(_RangeFilters("created_time", True, True)), "created_time")  # type: ignore[arg-type]
        assert got == (None, None)


class TestPushdownCoverage:
    """Which functions push, and which deliberately do not.

    Written down because the answer is not obvious from the code: a function
    without `filter_pushdown` is either a point lookup with nothing to filter,
    or a gap. Naming both keeps the second kind visible.
    """

    #: Functions whose endpoint takes no filter worth pushing — every one of
    #: these is a single-object lookup or a one-row snapshot.
    NO_FILTERABLE_ENDPOINT = {
        "market",
        "orderbook",
        "event",
        "event_metadata",
        "all_exchange_status",
        "all_historical_cutoff",
    }

    def test_every_filterable_endpoint_pushes(self) -> None:
        from vgi_kalshi.worker import _KALSHI_CATALOG

        missing = [
            f.Meta.name
            for f in _KALSHI_CATALOG.schemas[0].functions
            if f.Meta.name not in self.NO_FILTERABLE_ENDPOINT and not f.get_metadata().filter_pushdown
        ]
        assert missing == [], f"these have a filterable endpoint but push nothing: {missing}"

    def test_projection_is_universal(self) -> None:
        """Nothing here has a reason to build columns nobody asked for."""
        from vgi_kalshi.worker import _KALSHI_CATALOG

        for f in _KALSHI_CATALOG.schemas[0].functions:
            assert f.get_metadata().projection_pushdown is True, f.Meta.name

    def test_nothing_claims_exact_application(self) -> None:
        """We translate some predicates, never all, so DuckDB must re-check."""
        from vgi_kalshi.worker import _KALSHI_CATALOG

        for f in _KALSHI_CATALOG.schemas[0].functions:
            assert f.get_metadata().filters_exactly_applied is False, f.Meta.name


class TestCursorStability:
    """A cursor is only meaningful against the query that minted it.

    `current_pushdown_filters` is refreshed before every `process()` tick (for
    Top-N, among other things). A scan that recomputed its query parameters each
    time would resume an opaque Kalshi cursor under *different* parameters — and
    Kalshi's cursor encodes the query it belongs to, so the result would be
    silently wrong rows rather than an error.
    """

    @staticmethod
    def _run(filters_by_tick: list[Any]) -> list[dict[str, str]]:
        """Drive a paged scan whose pushed filters change between ticks."""
        import httpx

        import vgi_kalshi.kalshi_api as api
        from vgi_kalshi.markets import MarketsFunction
        from vgi_kalshi.schemas import MARKET_SCHEMA

        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(dict(request.url.params))
            body: dict[str, Any] = {"markets": [{"ticker": f"T{len(requests)}"}]}
            if len(requests) < len(filters_by_tick):
                body["cursor"] = f"c{len(requests)}"
            return httpx.Response(200, json=body)

        class Out:
            done = False

            def emit(self, batch: Any, **kwargs: Any) -> None:
                pass

            def finish(self) -> None:
                self.done = True

        class Params:
            args = MarketsArgs(series_ticker="KXBTCD")
            output_schema = MARKET_SCHEMA
            secrets = None
            attach_opaque_data = None
            current_pushdown_filters = None

        original = api.open_client
        api.open_client = lambda: httpx.Client(transport=httpx.MockTransport(handler))
        try:
            state, out = MarketsFunction.initial_state(Params()), Out()
            for tick_filters in filters_by_tick:
                Params.current_pushdown_filters = tick_filters
                MarketsFunction.process(Params(), state, out)
        finally:
            api.open_client = original
        return requests

    def test_a_mid_walk_filter_change_does_not_alter_the_query(self) -> None:
        requests = self._run(
            [
                _Filters({"event_ticker": "FIRST"}),
                _Filters({"event_ticker": "CHANGED"}),
                _Filters({"event_ticker": "CHANGED_AGAIN"}),
            ]
        )
        assert len(requests) == 3
        events = [r.get("event_ticker") for r in requests]
        assert events == ["FIRST", "FIRST", "FIRST"], f"the walk changed query mid-cursor: {events}"

    def test_later_pages_still_carry_the_cursor(self) -> None:
        requests = self._run([_Filters({"event_ticker": "E"})] * 3)
        assert requests[0].get("cursor") is None
        assert [r.get("cursor") for r in requests[1:]] == ["c1", "c2"]

    def test_the_first_page_uses_the_filters_it_was_given(self) -> None:
        """Freezing must not mean ignoring — the first tick still pushes."""
        requests = self._run([_Filters({"event_ticker": "E"})])
        assert requests[0].get("event_ticker") == "E"

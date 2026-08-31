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

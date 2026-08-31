"""Turning a DuckDB ``WHERE`` into Kalshi query parameters.

Pushdown here is always a pure optimisation. No function declares
``filters_exactly_applied``, so DuckDB re-checks every predicate against
whatever comes back — which makes the two directions of error wildly asymmetric:

* Pushing **too little** costs bandwidth and rate-limit budget. Nothing else.
* Pushing **too much** drops rows the predicate would have kept, and nothing
  downstream can recover what was never fetched.

So every helper here declines when it is not certain. A filter is translated
only when the API parameter means exactly what the SQL predicate means, or
strictly more.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from vgi.table_function import ProcessParams


def _filters(params: ProcessParams[Any]) -> Any:
    return getattr(params, "current_pushdown_filters", None)


def equality(params: ProcessParams[Any], column: str) -> str | None:
    """The constant from ``WHERE <column> = '...'``, when there is exactly one.

    Only string equality is returned; anything else is left for DuckDB. An
    empty string is treated as absent, matching the sentinel convention the
    argument dataclasses use for "not supplied".
    """
    filters = _filters(params)
    if filters is None:
        return None
    scalar = filters.get_column_constant(column)
    value = scalar.as_py() if scalar is not None else None
    return str(value) if isinstance(value, str) and value else None


def epoch_bounds(params: ProcessParams[Any], column: str) -> tuple[int | None, int | None]:
    """``(min_ts, max_ts)`` in epoch seconds from range predicates on a timestamp column.

    Kalshi's time windows are inclusive on both ends, so a strict ``>``/``<``
    is widened by a second rather than translated exactly: fetching one extra
    second of data is free, and DuckDB removes it. Narrowing instead would drop
    a row that sits exactly on the boundary.

    Returns ``(None, None)`` when nothing usable was pushed.
    """
    filters = _filters(params)
    if filters is None:
        return None, None
    bounds = filters.get_column_bounds(column)
    if bounds is None:
        return None, None

    def seconds(scalar: Any, *, widen: int) -> int | None:
        value = scalar.as_py() if scalar is not None else None
        if isinstance(value, datetime):
            return int(value.timestamp()) + widen
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return int(value) + widen
        return None

    low = seconds(getattr(bounds, "min_value", None), widen=0)
    high = seconds(getattr(bounds, "max_value", None), widen=1)
    return low, high

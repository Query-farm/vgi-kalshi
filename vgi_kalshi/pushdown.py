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

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from vgi.table_function import ProcessParams

from vgi_kalshi.schemas import batch_from_rows


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


def build_filtered(
    params: ProcessParams[Any],
    rows: Sequence[dict[str, Any]],
    fixed_schema: pa.Schema,
    parent_rows: Sequence[int] | None = None,
) -> tuple[pa.RecordBatch, list[int]]:
    """Build the output batch, applying every pushed filter to it.

    **Declaring ``filter_pushdown`` is a promise to apply the filters.** The
    engine drops its own filter above the scan once a function accepts pushdown,
    so a predicate the worker receives and ignores is not re-checked by anyone —
    it simply stops being applied. Translating *some* predicates into Kalshi
    query parameters and leaving the rest is therefore not a partial
    optimisation, it is a wrong answer:

        SELECT ticker FROM markets('KXBTCD') WHERE volume_24h_fp > 999999999

    returned rows. Every one of them had volume zero.

    Everything is evaluated against ``params.output_schema`` — the *projected*
    schema — and that is load-bearing rather than incidental. A pushed filter
    carries a ``column_index``, and ``ConstantFilter.evaluate`` reads
    ``batch.column(column_index)`` positionally. The index is relative to the
    projection DuckDB asked the scan for, so evaluating against any other column
    order compares the wrong column: against the full 27-column market schema,
    a filter on ``volume_24h_fp`` (index 0 of the projection) was applied to
    ``ticker``, and every row passed.

    That is also why projection pushdown must stay **on** wherever filters are
    accepted. DuckDB does not project away a column it is filtering on — it
    needs that column from the scan — so the projected batch always holds what
    the predicate references.

    Args:
        params: The tick's parameters, carrying the filters and the projection.
        rows: Decoded API rows, keyed by the schema's column names.
        fixed_schema: Unused; kept so every call site reads the same way.
        parent_rows: 1->N provenance, filtered in lockstep with the rows so a
            blended function's mapping survives.

    Returns:
        The projected, filtered batch and its surviving ``parent_rows``.
    """
    del fixed_schema
    filters = _filters(params)
    if filters is None:
        return batch_from_rows(rows, params.output_schema), list(parent_rows or [])

    batch = batch_from_rows(rows, params.output_schema)
    mask = filters.evaluate(batch)
    projected = pc.filter(batch, mask)
    if parent_rows is None:
        return projected, []
    # `pc.filter` drops rows whose mask is null, which is the SQL meaning of a
    # predicate that did not evaluate to true; provenance follows the same rule.
    keep = mask.to_pylist()
    return projected, [parent for parent, ok in zip(parent_rows, keep, strict=True) if ok]

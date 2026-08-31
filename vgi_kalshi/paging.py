"""Cursor paging as VGI scan state.

Kalshi pages its list endpoints with an opaque cursor. Walking that cursor to
exhaustion inside one call is the obvious implementation and the wrong one: a
blended row-transform must emit everything for its input in a single
``process()``, so a ``LIMIT 10`` over a large series still pays for every page,
and because a scan blocked inside its first batch cannot be cancelled, the query
does not merely run slowly — it wedges the client until the worker gives up.

The framework already has the answer. A ``TableFunctionGenerator`` carries state
between ``process()`` ticks, so the cursor lives in that state and each tick
fetches exactly one page and emits exactly one batch. DuckDB sees rows
immediately, a ``LIMIT`` stops the walk early, and cancellation lands between
ticks instead of never.

The trade-off is real and worth stating: a stateful scan cannot be the inner
side of a correlated ``LATERAL``. So the rule this package follows is *if the
endpoint pages, the function pages* — cursor-paged endpoints become scans, while
the bounded ones (a single market, one order book, a candlestick window) stay
blended and stay composable.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from vgi.cache_control import CacheControl
from vgi.table_function import ProcessParams
from vgi_rpc import ArrowSerializableDataclass
from vgi_rpc.rpc import OutputCollector

from vgi_kalshi import auth
from vgi_kalshi import kalshi_api as api
from vgi_kalshi.kalshi_api import PAGE_LIMIT, STALE_IF_ERROR, CacheHint
from vgi_kalshi.schemas import batch_from_rows


@dataclass(kw_only=True)
class PagedScanState(ArrowSerializableDataclass):
    """Where a cursor-paged scan has got to.

    The framework persists this between ``process()`` ticks. ``cursor`` is
    Kalshi's opaque continuation token; an empty string means "not started",
    which the cursor alone cannot tell apart from "finished" — hence ``done``.
    """

    cursor: str = ""
    done: bool = False


def emit_page(
    params: ProcessParams[Any],
    state: PagedScanState,
    out: OutputCollector,
    *,
    path: str,
    key: str,
    query: dict[str, Any],
    page_limit: int = PAGE_LIMIT,
    stamp: Callable[[Sequence[dict[str, Any]]], None] | None = None,
    cacheable: bool = True,
) -> None:
    """Fetch one page into one batch, and record where to resume.

    Args:
        params: The tick's parameters; ``output_schema`` is the projected one.
        state: The cursor carried between ticks.
        out: Collector for the batch, or for ``finish()`` once the walk is done.
        path: API path below the base.
        key: Key in the response holding this page's rows.
        query: Query parameters, before the cursor and page size are added.
        page_limit: Per-request page size; ``/events`` caps lower than the rest.
        stamp: Optional hook to add derived columns to the page's rows before
            they are built, used where a column is known to the caller but
            absent from Kalshi's payload.
        cacheable: Whether to forward the origin's freshness directive. False
            for the archive, whose caller sets its own policy.
    """
    if state.done:
        out.finish()
        return
    hint = CacheHint()
    rows, cursor = api.page(
        path,
        key,
        query,
        cursor=state.cursor or None,
        page_limit=page_limit,
        hint=hint,
        credentials=auth.for_call(params.secrets, params.attach_opaque_data),
    )
    if stamp is not None:
        stamp(rows)
    state.cursor = cursor or ""
    state.done = cursor is None
    cache_control = (
        CacheControl(ttl=hint.max_age, stale_if_error=STALE_IF_ERROR)
        if cacheable and hint.cacheable
        else None
    )
    out.emit(batch_from_rows(rows, params.output_schema), cache_control=cache_control)


def emit_archive_page(
    params: ProcessParams[Any],
    state: PagedScanState,
    out: OutputCollector,
    *,
    path: str,
    key: str,
    query: dict[str, Any],
    ttl: int,
    stamp: Callable[[Sequence[dict[str, Any]]], None] | None = None,
) -> None:
    """:func:`emit_page` for archived data, which is immutable and so cacheable.

    Kalshi declares no freshness on the historical endpoints, but nothing in the
    archive can change — a stronger guarantee than any TTL it could have sent.
    """
    if state.done:
        out.finish()
        return
    rows, cursor = api.page(
        path,
        key,
        query,
        cursor=state.cursor or None,
        credentials=auth.for_call(params.secrets, params.attach_opaque_data),
    )
    if stamp is not None:
        stamp(rows)
    state.cursor = cursor or ""
    state.done = cursor is None
    out.emit(
        batch_from_rows(rows, params.output_schema),
        cache_control=CacheControl(ttl=ttl, stale_if_error=STALE_IF_ERROR),
    )

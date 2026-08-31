"""Read-only HTTP access to the Kalshi trade API.

Every function here issues ``GET`` requests only — the single chokepoint
:func:`_get` is the sole place an HTTP call is made, which is what the read-only
CI guard (``tests/test_readonly_guard.py``) asserts against. The market-data
surface this worker exposes is public: no API key, no RSA-PSS request signing.

Kalshi returns money and contract counts as **fixed-point strings**
(``"0.7000"``, ``"136798.00"``) rather than JSON numbers. They are carried
through to Arrow as ``decimal128`` by :mod:`vgi_kalshi.schemas` — never as
floats, since these are exact on the wire and rounding them is a silent
precision loss.
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

from vgi_kalshi.auth import Credentials

#: Production base URL. Kalshi kept the ``elections`` host after the rename.
DEFAULT_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

#: Demo environment, for tests that want a non-production target.
DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"

#: Kalshi pages every list endpoint with an opaque cursor, but the maximum
#: page size is **per endpoint** and undocumented. ``/markets`` and
#: ``/markets/trades`` accept 1000; ``/events`` rejects anything over 200 with
#: HTTP 400 ``bad_request``. Probed against the live API — see
#: ``tests/test_live.py::TestPageLimits``.
PAGE_LIMIT = 1000

#: ``/events`` caps its page size at 200; 201 is a 400.
EVENTS_PAGE_LIMIT = 200

#: Both batch endpoints take at most 100 market tickers and answer 400 above it.
#: Batching is the single biggest lever this client has: 100 order books cost one
#: request instead of 100, which measured ~44x faster end to end and, more to the
#: point, spends 1 of the ~29 requests/second the rate limiter allows.
BATCH_TICKER_LIMIT = 100

#: ``/markets/candlesticks`` also caps the *candles* it will return across all
#: markets in one call. Exceeding it truncates rather than erroring, so the
#: caller has to size its own batches — see :func:`candlestick_batch_size`.
BATCH_CANDLE_LIMIT = 10_000

#: Hard stop on cursor following, so a runaway scan cannot hang a query forever.
#: `markets` alone can page past 400k rows (~398k of which are zero-volume
#: KXMVECROSSCATEGORY parlay combos), which is why `markets()` requires a
#: series_ticker rather than defaulting to the whole exchange. Hitting this is
#: an error, not a quiet truncation — see :class:`KalshiPageLimitError`.
MAX_PAGES = 500

#: Connect and read timeouts for every request this module makes.
TIMEOUT = httpx.Timeout(30.0, connect=10.0)

#: Grace window (seconds) for serving a stale cached result when a refetch
#: fails. Kalshi sends no ETag and no Last-Modified, so a stale entry can never
#: be revalidated cheaply — serving it briefly on error still beats failing the
#: query. Shared by every function that advertises cacheability.
STALE_IF_ERROR = 300

#: ``max-age=N``, anchored to a directive boundary so it cannot match the tail
#: of some other token.
_MAX_AGE = re.compile(r"(?:^|[\s,])max-age=(\d+)")

#: Directives that forbid reuse outright, whatever ``max-age`` also says.
#: ``no-store``/``no-cache`` mean "do not serve this again without asking", and
#: ``private`` means "not for a shared cache", which the DuckDB result cache is.
_NO_REUSE = re.compile(r"(?:^|[\s,])(?:no-store|no-cache|private)(?:\s*[,=]|\s*$)")

#: Statuses worth retrying: the rate limiter, plus the CDN's transient 5xx.
#: Kalshi rate-limits on a token bucket and documents that it sends **no**
#: ``Retry-After`` and no ``X-RateLimit-*`` headers, so a client has nothing to
#: obey but its own backoff. A correlated LATERAL over a whole series issues one
#: request per market and will hit this, so retrying is not optional.
_RATE_LIMITED = 429
_RETRYABLE_STATUSES = frozenset({_RATE_LIMITED, 500, 502, 503, 504})

#: Exponential backoff for a retryable request: ~0.5s, 1s, 2s, 4s, 8s.
_RETRY_ATTEMPTS = 5
_RETRY_BASE_SECONDS = 0.5


@dataclass(slots=True)
class CacheHint:
    """The origin's own freshness opinion, collected across a call's responses.

    Kalshi sets ``Cache-Control: public, max-age=N`` on reference endpoints
    (``/series`` and ``/markets`` say 15s) and sends **no** ``Cache-Control`` at
    all on live market data (``/markets/{ticker}``, ``/markets/{ticker}/orderbook``,
    ``/events``). It sends no ``ETag`` and no ``Last-Modified``, so conditional
    revalidation is not available — a cached result can only expire, never be
    revalidated cheaply.

    Passing one of these into an API call lets a table function forward the
    origin's actual policy to DuckDB's result cache instead of inventing one.
    ``max_age`` is the **minimum** seen across every response folded into it,
    so the shortest-lived page bounds the result — which is what :func:`_paged`
    wants, since it collects a whole call before anything is advertised.

    A paged *scan* is different: it holds one of these per tick, so what it
    advertises is the first page's directive rather than the minimum over all
    of them. That is not a weakening in practice — a given endpoint sends the
    same policy on every page — and it is unavoidable, since cache metadata
    rides on the first emitted batch and later pages have not been fetched yet.

    ``max_age`` stays ``None`` when the origin declared nothing, which is itself
    the signal that the data is live.
    """

    max_age: int | None = None
    #: True once any response arrived without a usable freshness directive.
    saw_uncacheable: bool = field(default=False)

    def observe(self, response: httpx.Response) -> None:
        """Fold one response's Cache-Control into the running hint.

        A response is only cacheable if it declares a non-zero ``max-age`` *and*
        declares nothing that forbids reuse. ``max-age=0`` is "already stale",
        which is not a cache entry worth holding — treating it as one would pair
        a zero lifetime with :data:`STALE_IF_ERROR`, quietly licensing five
        minutes of stale serving on an origin that asked for none.
        """
        directives = response.headers.get("cache-control", "")
        match = _MAX_AGE.search(directives)
        if match is None or _NO_REUSE.search(directives):
            self.saw_uncacheable = True
            return
        seconds = int(match.group(1))
        if seconds == 0:
            self.saw_uncacheable = True
            return
        self.max_age = seconds if self.max_age is None else min(self.max_age, seconds)

    @property
    def cacheable(self) -> bool:
        """Whether every response in this call carried a usable freshness directive."""
        return self.max_age is not None and not self.saw_uncacheable


def base_url() -> str:
    """The API base URL, overridable with ``KALSHI_BASE_URL`` (e.g. for the demo env)."""
    return os.environ.get("KALSHI_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def open_client() -> httpx.Client:
    """Open a client carrying this module's timeouts.

    Callers that issue a burst of per-row fetches (a correlated LATERAL) should
    open one of these and pass it in, so the whole batch shares a connection
    pool. It is the only httpx object the rest of the package constructs, which
    keeps the timeout policy in one place.
    """
    return httpx.Client(timeout=TIMEOUT)


class KalshiError(RuntimeError):
    """A non-2xx response from the Kalshi API, carrying the status and body."""

    def __init__(self, status: int, path: str, body: str) -> None:
        super().__init__(f"Kalshi API {status} for {path}: {body[:400]}")
        self.status = status
        self.path = path


class KalshiPageLimitError(RuntimeError):
    """A paged call followed :data:`MAX_PAGES` cursors without reaching the end.

    Raised rather than returning the truncated prefix: a silently short result
    reads as real data and there is nothing downstream that can tell the
    difference. The fix is at the call site — narrow the query or pass ``limit``.
    """

    def __init__(self, path: str, rows: int) -> None:
        super().__init__(
            f"Kalshi API {path}: still paging after {MAX_PAGES} pages ({rows} rows); "
            "narrow the query or pass an explicit limit"
        )
        self.path = path
        self.rows = rows


def _get(
    path: str,
    params: dict[str, Any] | Sequence[tuple[str, Any]] | None = None,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, Any]:
    """GET ``path`` under the API base and return the decoded JSON object.

    The one and only outbound-HTTP chokepoint. It issues ``GET`` and nothing
    else; adding a write verb here is the thing the read-only guard forbids.

    A ``GET`` is idempotent, so every transient failure is retried with
    exponential backoff: the rate limiter (429), the CDN's 5xx, and transport
    errors (connect failures, read timeouts). Retrying only the rate limiter
    would let one dropped connection abort a whole LATERAL fan-out.

    Args:
        path: Path below the API base, with a leading slash.
        params: Query parameters; ``None`` values are dropped.
        client: Optional caller-owned client, so a lateral join can reuse one
            connection pool across a batch of per-row fetches.
        hint: Optional :class:`CacheHint` to fold this response's
            ``Cache-Control`` into.
        credentials: Optional API key. When given, the request is signed;
            when ``None`` it goes out anonymously, which is all the public
            market-data surface needs.

    Returns:
        The decoded JSON response body.

    Raises:
        KalshiError: The response status was not 2xx after retries.
        httpx.TransportError: Every attempt failed to reach the API.
    """
    # A sequence of pairs, not a mapping, when a parameter must repeat
    # (``?tickers=A&tickers=B``) — see :func:`orderbooks`.
    pairs = list(params.items()) if isinstance(params, dict) else list(params or ())
    clean: Any = [(k, v) for k, v in pairs if v is not None]
    url = f"{base_url()}{path}"
    owned = client is None
    http = client or open_client()
    try:
        for attempt in range(_RETRY_ATTEMPTS):
            last_attempt = attempt == _RETRY_ATTEMPTS - 1
            # Re-signed per attempt: the signature covers a timestamp, and a
            # retry after 8s of backoff would otherwise present a stale one.
            headers = credentials.headers("GET", path) if credentials else None
            try:
                response = http.get(url, params=clean, headers=headers)
            except httpx.TransportError:
                if last_attempt:
                    raise
            else:
                if response.status_code not in _RETRYABLE_STATUSES or last_attempt:
                    break
            time.sleep(_RETRY_BASE_SECONDS * (2**attempt))
    finally:
        if owned:
            http.close()
    # Only fold a served response into the freshness hint; a 429 or a 502 carries
    # the CDN's error policy, not the resource's.
    if hint is not None and response.status_code < 400:
        hint.observe(response)
    if response.status_code >= 400:
        raise KalshiError(response.status_code, path, response.text)
    return response.json()


def page(
    path: str,
    key: str,
    params: dict[str, Any] | None = None,
    *,
    cursor: str | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
    page_limit: int = PAGE_LIMIT,
) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch exactly one page, returning its rows and the cursor for the next.

    The primitive behind both paging styles. :func:`_paged` loops on this to
    collect everything, which suits a bounded call; a table function instead
    holds the cursor in its scan state and calls this once per tick, so DuckDB
    sees rows from the first page rather than waiting for the last. That
    difference is what keeps a ``LIMIT 10`` over a large series responsive —
    and, because a scan blocked inside its first batch cannot be cancelled,
    it is what keeps such a query from wedging the client outright.

    A ``None`` cursor means this was the final page.
    """
    page_params = {**(params or {}), "limit": page_limit}
    if cursor:
        page_params["cursor"] = cursor
    payload = _get(path, page_params, client=client, hint=hint, credentials=credentials)
    rows = payload.get(key) or []
    next_cursor = payload.get("cursor") or None
    # An empty page ends the walk even when a cursor comes back with it.
    return rows, (next_cursor if rows else None)


def _paged(
    path: str,
    key: str,
    params: dict[str, Any] | None = None,
    *,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
    page_limit: int = PAGE_LIMIT,
) -> list[dict[str, Any]]:
    """Follow Kalshi's opaque cursor and accumulate ``key`` from every page.

    Stops at ``limit`` rows if given, at an empty page, or at a missing cursor.
    Running out of :data:`MAX_PAGES` first raises :class:`KalshiPageLimitError`
    rather than returning a short result that looks complete.

    ``page_limit`` is the per-request page size, which differs by endpoint:
    exceeding an endpoint's cap is a hard 400, not a clamp. When the caller
    wants fewer rows than a full page, the request is sized down to match —
    asking for 1000 to return 1 is a page of wasted bandwidth on both ends.
    """
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(MAX_PAGES):
        wanted = page_limit if limit is None else max(1, min(page_limit, limit - len(rows)))
        page_params = {**(params or {}), "limit": wanted}
        if cursor:
            page_params["cursor"] = cursor
        payload = _get(path, page_params, client=client, hint=hint, credentials=credentials)
        batch = payload.get(key) or []
        rows.extend(batch)
        if limit is not None and len(rows) >= limit:
            return rows[:limit]
        cursor = payload.get("cursor") or None
        if not cursor or not batch:
            break
    else:
        raise KalshiPageLimitError(path, len(rows))
    return rows


# --------------------------------------------------------------------------
# Markets
# --------------------------------------------------------------------------


def markets(
    series_ticker: str,
    *,
    event_ticker: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """Markets under one series, optionally narrowed to an event and/or status.

    ``status`` is the **filter** vocabulary — ``unopened``, ``open``, ``closed``
    or ``settled`` — which is not the vocabulary the ``status`` field of the
    returned market uses. Kalshi maps between them: ``open`` selects markets
    reporting ``active``, ``unopened`` selects ``initialized``, and ``settled``
    selects ``finalized``. ``finalized`` is a valid market status but not a
    valid filter, and every other value is HTTP 400 ``invalid status filter``.
    """
    return _paged(
        "/markets",
        "markets",
        {"series_ticker": series_ticker, "event_ticker": event_ticker, "status": status},
        limit=limit,
        client=client,
        hint=hint,
        credentials=credentials,
    )


def market(
    ticker: str,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, Any]:
    """A single market by ticker."""
    return _get(f"/markets/{ticker}", client=client, hint=hint, credentials=credentials)["market"]


def orderbook(
    ticker: str,
    *,
    depth: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, Any]:
    """The resting-order book for one market.

    Returns the raw ``orderbook_fp`` object: ``yes_dollars`` / ``no_dollars``
    lists of ``[price, contract_count]`` fixed-point string pairs. Note that an
    unknown ticker is answered with 200 and two empty sides rather than a 404,
    so an empty book here does not mean the market exists.
    """
    payload = _get(
        f"/markets/{ticker}/orderbook", {"depth": depth}, client=client, hint=hint, credentials=credentials
    )
    return payload.get("orderbook_fp") or {}


def candlesticks(
    series_ticker: str,
    ticker: str,
    *,
    period_interval: int,
    start_ts: int,
    end_ts: int,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """OHLC candlesticks for one market.

    Note the path: candlesticks live under the **series**, not under
    ``/markets/{ticker}``. The endpoint index in ``docs.kalshi.com/llms.txt``
    lists ``GET /markets/{ticker}/candlesticks``, which 404s — the working path
    is the one used here, and it is why ``candlesticks()`` takes the series
    ticker as well as the market ticker.
    """
    payload = _get(
        f"/series/{series_ticker}/markets/{ticker}/candlesticks",
        {"period_interval": period_interval, "start_ts": start_ts, "end_ts": end_ts},
        client=client,
        hint=hint,
        credentials=credentials,
    )
    return payload.get("candlesticks") or []


def orderbooks(
    tickers: Sequence[str],
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, dict[str, Any]]:
    """Order books for many markets at once, keyed by ticker.

    The batched twin of :func:`orderbook`, and the reason a ``LATERAL`` over a
    whole series is affordable: 100 books cost one request rather than 100.
    Tickers are chunked at :data:`BATCH_TICKER_LIMIT`; duplicates are collapsed,
    since the result is a mapping and the caller fans it back out.

    Note the parameter style. ``tickers`` must be repeated
    (``?tickers=A&tickers=B``) — comma-joining them is accepted with HTTP 200 and
    answers with a single empty book for a ticker named ``"A,B"``, which is a
    wrong answer rather than an error, so it is worth not getting wrong.
    """
    unique = list(dict.fromkeys(t for t in tickers if t))
    books: dict[str, dict[str, Any]] = {}
    for start in range(0, len(unique), BATCH_TICKER_LIMIT):
        chunk = unique[start : start + BATCH_TICKER_LIMIT]
        payload = _get(
            "/markets/orderbooks",
            [("tickers", ticker) for ticker in chunk],
            client=client,
            hint=hint,
            credentials=credentials,
        )
        for entry in payload.get("orderbooks") or []:
            ticker = entry.get("ticker")
            if ticker:
                books[ticker] = entry.get("orderbook_fp") or {}
    return books


def candlestick_batch_size(*, period_interval: int, start_ts: int, end_ts: int) -> int:
    """How many markets fit in one batched candlestick call.

    ``/markets/candlesticks`` caps its response at :data:`BATCH_CANDLE_LIMIT`
    candles across *all* markets in the call, and silently returns fewer rather
    than erroring — so a 100-market batch of one-minute candles over a day would
    ask for 144,000 and quietly get a fraction. Sizing the batch by the window is
    what keeps the result complete.
    """
    period_seconds = max(period_interval, 1) * 60
    per_market = max(1, (max(end_ts - start_ts, 0) // period_seconds) + 1)
    return max(1, min(BATCH_TICKER_LIMIT, BATCH_CANDLE_LIMIT // per_market))


def batch_candlesticks(
    tickers: Sequence[str],
    *,
    period_interval: int,
    start_ts: int,
    end_ts: int,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Candlesticks for many markets at once, keyed by market ticker.

    Unlike :func:`candlesticks`, this endpoint is not scoped to a series — it
    keys on the market ticker alone — and unlike :func:`orderbooks` it wants its
    tickers **comma-separated** in a single ``market_tickers`` parameter. The two
    batch endpoints disagree about this; both spellings are Kalshi's.
    """
    unique = list(dict.fromkeys(t for t in tickers if t))
    size = candlestick_batch_size(period_interval=period_interval, start_ts=start_ts, end_ts=end_ts)
    out: dict[str, list[dict[str, Any]]] = {}
    for start in range(0, len(unique), size):
        chunk = unique[start : start + size]
        payload = _get(
            "/markets/candlesticks",
            {
                "market_tickers": ",".join(chunk),
                "period_interval": period_interval,
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
            client=client,
            hint=hint,
            credentials=credentials,
        )
        for entry in payload.get("markets") or []:
            ticker = entry.get("market_ticker")
            if ticker:
                out[ticker] = entry.get("candlesticks") or []
    return out


def trades(
    ticker: str | None = None,
    *,
    min_ts: int | None = None,
    max_ts: int | None = None,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """The public trade tape, optionally scoped to one market and a time window.

    Polling this with ``min_ts`` is the lossless alternative to the WebSocket
    ``trade`` channel: the cursor is replayable, so a dropped connection costs a
    retry rather than a gap.
    """
    return _paged(
        "/markets/trades",
        "trades",
        {"ticker": ticker, "min_ts": min_ts, "max_ts": max_ts},
        limit=limit,
        client=client,
        hint=hint,
        credentials=credentials,
    )


# --------------------------------------------------------------------------
# Events and series
# --------------------------------------------------------------------------


def events(
    series_ticker: str,
    *,
    status: str | None = None,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """Events under one series.

    Paged at 200, not the usual 1000 — ``/events`` returns HTTP 400 for any
    larger page size rather than clamping it.
    """
    return _paged(
        "/events",
        "events",
        {"series_ticker": series_ticker, "status": status},
        limit=limit,
        client=client,
        hint=hint,
        credentials=credentials,
        page_limit=EVENTS_PAGE_LIMIT,
    )


def event(
    event_ticker: str,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, Any]:
    """One event by ticker.

    The response also carries the event's markets; only the event itself is
    returned here, since ``markets(series, event_ticker => …)`` is the way to
    ask for those and it paginates properly.
    """
    payload = _get(f"/events/{event_ticker}", client=client, hint=hint, credentials=credentials)
    return payload.get("event") or {}


def event_metadata(
    event_ticker: str,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, Any]:
    """Descriptive metadata for one event: settlement sources and images.

    Distinct from the ``settlement_sources`` column on :func:`events`, which is
    the summary Kalshi inlines into the listing; this endpoint is where the
    images live.
    """
    return _get(f"/events/{event_ticker}/metadata", client=client, hint=hint, credentials=credentials)


def exchange_status(
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, Any]:
    """Whether the exchange, and each venue within it, is open.

    Kalshi runs several venues under one exchange — Default, Combos, Crypto,
    sports — which open and close independently, so this is the difference
    between "the exchange is up" and "the thing you want to trade is up".
    Declares ``Cache-Control: public, max-age=1``, the shortest TTL Kalshi
    publishes anywhere.
    """
    return _get("/exchange/status", client=client, hint=hint, credentials=credentials)


# --------------------------------------------------------------------------
# The historical archive
# --------------------------------------------------------------------------
#
# Kalshi moves settled markets, and the trades and candles under them, out of
# the live endpoints and into a separate archive. A query for last month's
# trades against the live tape does not error — it returns nothing — so the
# cutoff below is what tells a caller which side of the boundary to ask.


def historical_cutoff(
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> dict[str, Any]:
    """The boundary between the live endpoints and the archive.

    Four timestamps, one per data kind. Anything older than the relevant one has
    been archived and is invisible to the live endpoints; anything newer is not
    in the archive yet. This is the only way to know which to query, and getting
    it wrong is silent — the wrong endpoint returns an empty result, not an
    error.
    """
    return _get("/historical/cutoff", client=client, hint=hint, credentials=credentials)


def historical_markets(
    *,
    series_ticker: str | None = None,
    event_ticker: str | None = None,
    tickers: Sequence[str] | None = None,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """Archived markets — settled, and moved out of the live ``/markets``.

    Same shape as a live market plus ``settlement_value_dollars``, which is what
    the contract actually paid out.
    """
    return _paged(
        "/historical/markets",
        "markets",
        {
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "tickers": ",".join(tickers) if tickers else None,
        },
        limit=limit,
        client=client,
        hint=hint,
        credentials=credentials,
    )


def historical_trades(
    ticker: str | None = None,
    *,
    min_ts: int | None = None,
    max_ts: int | None = None,
    limit: int | None = None,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """The archived trade tape, in the same shape as the live one."""
    return _paged(
        "/historical/trades",
        "trades",
        {"ticker": ticker, "min_ts": min_ts, "max_ts": max_ts},
        limit=limit,
        client=client,
        hint=hint,
        credentials=credentials,
    )


def historical_candlesticks(
    ticker: str,
    *,
    period_interval: int,
    start_ts: int,
    end_ts: int,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """Archived candlesticks for one market.

    Note the path is ``/historical/markets/{ticker}/candlesticks`` — keyed on the
    market alone, unlike the live per-market endpoint, which is scoped to the
    series. There is no batched form of this one.
    """
    payload = _get(
        f"/historical/markets/{ticker}/candlesticks",
        {"period_interval": period_interval, "start_ts": start_ts, "end_ts": end_ts},
        client=client,
        hint=hint,
        credentials=credentials,
    )
    return payload.get("candlesticks") or []


def series_list(
    category: str | None = None,
    *,
    client: httpx.Client | None = None,
    hint: CacheHint | None = None,
    credentials: Credentials | None = None,
) -> list[dict[str, Any]]:
    """Every series, optionally filtered to one category.

    Unpaginated by design on Kalshi's side: the whole catalog — ~13,600 rows,
    about 15.6 MB of JSON — comes back in one response, gzipped on the wire.
    There is no cursor to follow and no page size to pick, which is what makes
    it viable as a table even at that size.
    """
    return (
        _get("/series", {"category": category}, client=client, hint=hint, credentials=credentials).get(
            "series"
        )
        or []
    )

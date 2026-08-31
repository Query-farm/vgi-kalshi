# vgi-kalshi

A **read-only** [VGI](https://github.com/query-farm/vgi-python) worker exposing
[Kalshi](https://kalshi.com) prediction-market data to DuckDB/SQL — markets,
order books, candlesticks, trades, events, and the series catalog, as ordinary
tables and table functions.

> **No credentials required.** Kalshi's entire market-data surface is public.
> Only portfolio, order, and account endpoints need an API key with RSA-PSS
> request signing, and this worker exposes none of them. Read-only is enforced
> structurally (one `_get` chokepoint in `kalshi_api.py`, the only module in the
> package that so much as imports `httpx`) and by a CI guard
> (`tests/test_readonly_guard.py`).

## Run

```bash
uv run kalshi_worker.py            # stdio
uv run serve.py --port 8000        # HTTP
```

```sql
ATTACH 'kalshi' (TYPE vgi, LOCATION 'uv run kalshi_worker.py');
```

Both scripts carry PEP-723 headers pinning the published dependencies, so they
run from a fresh clone with nothing installed. For a deployment, install the
package and use its console script instead — it does not depend on the working
directory, which the script form does:

```bash
pip install vgi-kalshi
```

```sql
ATTACH 'kalshi' (TYPE vgi, LOCATION 'vgi-kalshi');
```

### Developing against a local vgi-python

`[tool.uv.sources]` points `vgi-python` and `vgi-rpc` at sibling checkouts, so
`uv sync` and `uv run vgi-kalshi` pick up local edits to the framework. The
entry-point scripts deliberately do **not**: they resolve from PyPI so a clone
works anywhere. Pass `--no-sources` to ignore the local checkouts entirely,
which is what CI does — and what proves the published dependencies are enough.

## Surface

Names are bare — they are already qualified by the `kalshi` catalog.

### Table

| Table | |
|---|---|
| `series` | Every series on the exchange (~13.6k rows, ~15.6 MB, one response) |
| `exchange_status` | One row per trading venue: is it up, is it trading |
| `historical_cutoff` | One row: where the live endpoints stop and the archive begins |

`series` is a real catalog table, not a function: no required key and
slow-changing. It is backed by the `all_series` function via
`Table(function=…)`, which VGI auto-wires into a scan. It is not small — the
whole catalog is ~15.6 MB of JSON, gzipped on the wire — but Kalshi returns it
unpaginated, so there is no cursor to follow and nothing to key on.

### Functions

Functions come in two shapes, and which one an object gets is decided by the
endpoint behind it: **if it pages, the function pages.**

A **paged scan** (`markets`, `events`, `historical_markets`) sits on a
cursor-paged endpoint. It emits one API page per tick, holding the cursor in VGI
scan state, so `LIMIT` stops early and rows arrive immediately. It cannot be the
inner side of a correlated LATERAL — drive a join *from* it, not into it.

Everything else is **blended** (`RowTransformFunction`): the fetch is bounded by
construction — one market, one book, one candlestick window — so a single
registration serves both a literal call and a LATERAL.

The distinction is not stylistic. A blended function must emit everything for
its input in one `process()` call, so on a paged endpoint it walks every page
before DuckDB sees a row: a `LIMIT 10` still pays for the whole series, and
because a scan blocked inside its first batch cannot be cancelled, the query
*wedges the client* rather than merely running slowly. `vgi-lint`'s VGI911
caught exactly that on all three.

| Function | Positional (per-row) | Named |
|---|---|---|
| `markets(series_ticker)` *(scan)* | series_ticker | `event_ticker`, `status` |
| `market(ticker)` | ticker | `cache_ttl` |
| `orderbook(ticker)` | ticker | `depth`, `cache_ttl` |
| `candlesticks(series_ticker, ticker)` | both | `period_interval`, `start_ts`, `end_ts` |
| `trades(ticker)` | ticker | `min_ts`, `max_ts`, `max_rows`, `cache_ttl` |
| `events(series_ticker)` *(scan)* | series_ticker | `status` |
| `event(event_ticker)` | event_ticker | `cache_ttl` |
| `event_metadata(event_ticker)` | event_ticker | `cache_ttl` |
| `historical_markets(series_ticker)` *(scan)* | series_ticker | `event_ticker` |
| `historical_trades(ticker)` | ticker | `min_ts`, `max_ts`, `max_rows` |
| `historical_candlesticks(ticker)` | ticker | `period_interval`, `start_ts`, `end_ts` |

### The archive

Kalshi moves settled markets, and the trades and candles beneath them, out of
the live endpoints into a separate archive. **The live functions do not error
for archived data — they return nothing**, which reads exactly like a market
that never traded. `historical_cutoff` is the boundary, and it moves:

```sql
SELECT market_settled_ts, trades_created_ts FROM kalshi.historical_cutoff;
```

The historical functions mirror their live twins column for column, so a query
crosses the boundary by changing only the function name, and the two halves
`UNION ALL` without reshaping. Two differences worth knowing:
`historical_markets` adds `settlement_value_dollars` (what the contract
actually paid), and `historical_candlesticks` takes only a market ticker —
the archive is not scoped by series, unlike the live endpoint.

```sql
-- Order books for every open market in a series, in one query
SELECT m.ticker, o.side, o.price_dollars, o.count_fp
FROM kalshi.markets('KXBTCD') m,
     LATERAL kalshi.orderbook(m.ticker) o
WHERE m.status = 'active';
```

**`status` has two vocabularies, and they do not overlap.** The filter argument
takes `unopened`, `open`, `closed` or `settled`; the `status` column of the
result holds `initialized`, `active`, `closed`, `settled` or `finalized`. So
`status => 'open'` returns rows reading `active`, and the two spellings are not
interchangeable:

| `status =>` filter | `status` column |
|---|---|
| `unopened` | `initialized` |
| `open` | `active` |
| `settled` | `finalized` |

Getting either wrong fails in a different way — a bad filter is a 400
`invalid status filter`, while a bad predicate just returns nothing at all.
`tests/test_catalog.py::TestShippedExamples` checks every example this worker
advertises against the right vocabulary for its position, offline;
`tests/test_live.py::TestMarketStatus` pins the mapping itself.

## Design notes

**Every function takes a required key.** An unfiltered scan of `/markets` or
`/markets/trades` pages past 400,000 rows — ~398,000 of them zero-volume
`KXMVECROSSCATEGORY` parlay combos — and takes minutes. Requiring the series or
the ticker keeps a naive `SELECT *` honest. A paged call that still has not
reached the end after `MAX_PAGES` raises `KalshiPageLimitError` rather than
returning a prefix that reads like a complete result.

**Money is `DECIMAL`, never `DOUBLE`.** Kalshi sends prices and counts as
fixed-point *strings* (`"0.7000"`, `"136798.00"`) in two flavours: `*_dollars`
at 4dp and `*_fp` at 2dp. They map to `decimal128(18,4)` and `decimal128(18,2)`.
They arrive exact; parsing through `float` would throw that away silently. A
value that Arrow cannot hold exactly — non-finite, or needing more scale or
precision than the column declares — becomes NULL rather than either rounding
(the same silent loss) or raising (which would fail every other row in the
batch alongside it).

**`markets()` stamps on a `series_ticker` column.** Kalshi's market payload
carries `event_ticker` but no series, and `candlesticks()` needs one — so
`markets()` adds the series it was called with, and `market()` derives it from
the event ticker's prefix. That is what makes the two compose:

```sql
SELECT c.end_period_ts, c.price_close_dollars
FROM kalshi.markets('KXBTCD') m,
     LATERAL kalshi.candlesticks(m.series_ticker, m.ticker, period_interval => 60) c;
```

**Candlesticks are series-scoped.** The endpoint index at `docs.kalshi.com/llms.txt`
lists `GET /markets/{ticker}/candlesticks`, which 404s. The working path is
`/series/{series}/markets/{ticker}/candlesticks` — which is why `candlesticks()`
takes both tickers. `tests/test_live.py` guards this.

**Blended-function constraints.** Positional args *are* the per-row input
columns (read off `batch`, not `params.args`); a positional `const` arg is
rejected, so every optional knob is a named arg; and no function may define
`finalize`/`finish`, because DuckDB forbids `FinalExecute` under correlated
LATERAL. Each function is 1→N, so every `emit` carries `parent_rows` provenance
mapping output rows back to the input row that produced them.

A named argument also cannot receive a correlated column: `LATERAL f(x => t.col)`
does not bind, because only positional args are per-row inputs. Pass a literal or
a scalar subquery instead.

**Paged scans** take the other trade. They are ordinary `TableFunctionGenerator`s
with a `PagedScanState` carrying Kalshi's opaque cursor between `process()`
ticks — one page fetched, one batch emitted, `finish()` when the cursor runs
out. Positional args are literals on `params.args` rather than input columns.

**No WebSocket.** Kalshi's WS carries 7 push channels and is a strict subset of
the REST API. It is also lossy — a dropped connection is a gap. `trades()`
polled with `min_ts` on a replayable cursor is the lossless equivalent, and the
pull model is what VGI table functions actually are.

## Caching

Kalshi's own headers drive the policy — nothing is invented. Its
`Cache-Control` is forwarded to DuckDB's result cache as `vgi.cache.*`
metadata, so if Kalshi changes a TTL, this follows automatically.

| Endpoint | Kalshi sends | We advertise |
|---|---|---|
| `/series` | `public, max-age=15` | `ttl=15` |
| `/markets` (list) | `public, max-age=15` | `ttl=15` |
| `/markets/{ticker}` | *(nothing)* | uncached unless `cache_ttl` |
| `/markets/{ticker}/orderbook` | *(nothing)* | uncached unless `cache_ttl` |
| `/markets/trades` | *(nothing)* | uncached unless `cache_ttl` |
| `/events` | *(nothing)* | uncached unless `cache_ttl` — **worth setting** |
| `/series/…/candlesticks` | *(nothing)* | see below |

A response counts as cacheable only if it declares a non-zero `max-age` *and*
declares nothing that forbids reuse. `no-store`, `no-cache` and `private` beat
any `max-age` beside them, and `max-age=0` is treated as uncacheable rather than
as a zero-lifetime entry — storing one would pair an already-stale result with
`stale_if_error` and quietly license five minutes of staleness the origin never
offered.

Kalshi sends **no `ETag` and no `Last-Modified`** on anything, so a cached
result can only expire — it can never be cheaply revalidated. Every cacheable
result carries `stale_if_error=300` so a failed refetch serves briefly stale
rather than failing the query.

**Candlesticks are cached by immutability, not by TTL.** A candle can never
change once its period has closed, so a window ending before the current period
began is cached for a day. A window running up to `now` contains a candle that
is still forming and is not cached at all.

**What is cached by default is narrow, deliberately.** Only `series`,
`markets` and `exchange_status` carry a TTL without being asked, because those
are the only endpoints Kalshi declares one for (15s, 15s and 1s). Everything
else is either live-by-default or cached by immutability:

| Function | Cached because |
|---|---|
| `series`, `markets`, `exchange_status` | the origin says so |
| `candlesticks` | the window is closed, so the candles cannot change |
| `historical_*` | the archive is immutable |
| `market`, `orderbook`, `trades`, `events` | only if you pass `cache_ttl` |

`events()` is the one to watch: `/events` declares no freshness *and* is the
most aggressively rate-limited endpoint here (~4 req/s against ~29 for the
rest), so a `cache_ttl` matters more there than anywhere else.

**Opt-in caching for live data.** `market()`, `orderbook()`, `trades()` and
`events()` take a `cache_ttl` named arg, default `0` (off). Setting it also
enables `per_value` memoization, which is what makes a LATERAL over a whole
series survive the rate limit — but it trades freshness for that, which is why
you have to ask:

```sql
SELECT m.ticker, o.side, o.price_dollars
FROM kalshi.markets('KXBTCD') m,
     LATERAL kalshi.orderbook(m.ticker, cache_ttl => 30) o;
```

**Rate limiting is real, and it is per-endpoint.** A LATERAL over 318 markets
issues 318 requests, and Kalshi returns HTTP 429 with no `Retry-After` to obey.
Measured against the public API, unauthenticated:

| Endpoint | sustained | notes |
|---|---|---|
| `/markets` (`limit=1`) | ~28.5 req/s | no throttling |
| `/markets` (`limit=1000`) | ~21 req/s | occasional 429 |
| `/series` | ~17 req/s | 15.6 MB per response |
| `/events` | **~4 req/s** | throttled far harder than the rest |

`/events` is the real constraint — roughly seven times tighter than everything
else — so a query that fans out over events needs `cache_ttl`, not more
parallelism.

**Order books and candlesticks are fetched in batches of 100.** Kalshi exposes
`GET /markets/orderbooks` and `GET /markets/candlesticks`, which take up to 100
market tickers each, so `orderbook()` and `candlesticks()` under a LATERAL cost
one request per hundred markets rather than one per market — measured at 5.89s
→ 0.13s for 100 books, a ~44× improvement, and far more importantly one request
out of the ~29/second budget instead of a hundred. This happens automatically;
no query changes.

Two wire-format traps, both Kalshi's: `/markets/orderbooks` needs its `tickers`
parameter **repeated**, and comma-joining them returns HTTP 200 with a single
empty book for a market named `"A,B,C"` — a wrong answer rather than an error.
`/markets/candlesticks` wants the opposite, a comma-separated `market_tickers`.
It also caps the response at 10,000 candles across the whole call and truncates
silently past that, so the batch is sized by the window: 100 markets of hourly
candles over a day, but only 6 markets of one-minute candles.

**One connection, reused.** A paged scan fetches a page per tick, and the
framework owns the loop, so there is no scope to hold a client open across the
walk. `kalshi_api` keeps a process-wide pool for that reason — a client per page
measured 138 ms against 50 ms warm, a TLS handshake paid on every page of every
scan. `reset_shared_client()` forces a reconnect.

**Transport.** Responses are compressed: httpx advertises `br` (Brotli is a
declared dependency for this reason) and Kalshi honours it. On the `series`
catalog that is 1.35 MB on the wire against gzip's 2.06 MB and 15.6 MB raw — a
35% saving over gzip, and Brotli decodes marginally *faster* here, so it costs
nothing. Kalshi ignores `zstd`. The connection stays HTTP/1.1: the API does
negotiate HTTP/2, but requests inside a scan are issued sequentially, so
multiplexing buys nothing — measured at 34.2 ms/request over HTTP/2 against
34.6 ms over HTTP/1.1, a ~1% difference not worth the `h2` dependency. Note that
34 ms/request is ~29 requests/second, which is the rate ceiling above: **a
fan-out is already running as fast as Kalshi will allow**, so issuing the
requests concurrently would only reach the 429s sooner. `cache_ttl` is the only
real lever.

**Pushdown.** Every function honours projection pushdown — it builds only the
columns the query asked for, rather than all 27 market columns for a
`SELECT ticker`. Eight also translate predicates into Kalshi query parameters,
so a `WHERE` narrows what is *fetched* rather than only what is returned:

| Predicate | becomes | worth |
|---|---|---|
| `series WHERE category = 'Crypto'` | `/series?category=` | 16.4 MB → 0.2 MB (**71×**) |
| `markets WHERE status = 'active'` | `status => 'open'` | one page instead of a series |
| `markets/historical_markets WHERE event_ticker = …` | the endpoint's filter | one event instead of a series |
| `events WHERE status = …` | the endpoint's filter | fewer pages |
| `trades WHERE created_time > …` | `min_ts`/`max_ts` | a window instead of a whole tape |
| `candlesticks WHERE end_period_ts > …` | narrows the window | fewer candles |

The remaining six functions are point lookups — one market, one book, one event
— with nothing to filter.

No function declares `filters_exactly_applied`, so DuckDB re-checks every
predicate against whatever comes back. That asymmetry is the whole design:
declining to push a filter costs bandwidth, while pushing a *wrong* one drops
rows nothing downstream can recover. So a predicate is translated only where the
API parameter means exactly what the SQL means, or strictly more — which is why
`status` on `markets` is mapped through a verified table and why a strict `<` on
a timestamp is widened by a second rather than translated exactly.

`_get` retries five times with exponential backoff (~0.5s → 8s) on a 429, on a
transient 5xx, and on a dropped connection — a `GET` is idempotent, and
retrying only the rate limiter would let one reset connection abort a fan-out
that had already made 300 successful calls. A 4xx is not retried, and a 429 is
never folded into the freshness hint, since it carries the CDN's error policy
rather than the resource's.

## Authentication (optional, and rarely worth it)

The whole surface this worker exposes is public, and **authenticating does not
speed it up at Kalshi's entry tier.** Kalshi's documented budgets are per
account, in tokens per second, with most calls costing 10:

| Tier | budget | ≈ req/s | how you get it |
|---|---|---|---|
| *(unauthenticated)* | — | **~28.5** | measured; undocumented, presumably per-IP |
| Basic | 200 | 20 | account signup |
| Advanced | 300 | 30 | call the upgrade endpoint |
| Expert | 600 | 60 | trading volume, or assigned |
| Premier / Paragon / Prime | 1,000 / 2,000 / 4,000 | 100 / 200 / 400 | assigned |

So Basic is *slower* than anonymous access, Advanced is a rounding error, and
authentication only pays from Expert upward. Support it if you have the tier, or
if you would rather depend on a documented limit than an undocumented one — not
as a fix for a slow query.

Credentials are a **DuckDB secret**, not an ATTACH option, because one of the two
values is an RSA private key and ATTACH strings show up in `duckdb_databases()`:

```sql
CREATE SECRET kalshi (
    TYPE kalshi,
    key_id '9f8e7d6c-...',
    private_key '-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----'
);
```

`private_key` is declared redacted, so `duckdb_secrets()` masks it. Requests are
then signed per Kalshi's scheme — RSA-PSS over SHA-256 of
`timestamp + METHOD + /trade-api/v2 + path`, base64 — and re-signed on every
retry, since a signature covers a timestamp and a replayed one after 8s of
backoff is a 401.

One ATTACH option decides what happens when no secret resolves:

```sql
ATTACH 'kalshi' (TYPE vgi, LOCATION 'uv run kalshi_worker.py', auth 'required');
```

| `auth` | behaviour |
|---|---|
| `auto` *(default)* | sign when a secret is present, use public access otherwise |
| `required` | fail the query when no credential resolves |
| `off` | never sign, even if a secret exists |

`required` exists so a deployment that means to be authenticated cannot silently
end up anonymous on a different rate limit. Signing stays read-only: it adds
three headers to a `GET` and nothing else, and the credential never reaches any
endpoint outside the public market-data surface.

## Catalog metadata

Everything a client sees on `ATTACH` — object descriptions, column comments,
result schemas, examples, categories — is published as `vgi.*` tags and checked
by [vgi-lint](https://github.com/Query-farm/vgi-lint-check):

```bash
vgi-lint lint                     # config lives in vgi-lint.toml
vgi-lint lint --audit-waivers     # prove both waivers still buy something
```

Column documentation has a single source: `vgi_kalshi/schemas.py` attaches a
comment to every Arrow field via `meta.field()`, and `meta.result_columns_schema()`
reads those same strings back out to build each function's declared result
schema. A column documented once therefore shows up in `DESCRIBE`, in
`duckdb_columns()`, and in the function's `vgi.result_columns_schema` — and
cannot drift between them.

Two rules are waived, in `vgi-lint.toml`, each with a recorded kind and reason
that `--audit-waivers` re-checks. VGI311 asks that a parameterless scan be
exposed as a table, which all three of ours are — the rule matches on name, and
the names differ deliberately because a function and a table cannot share one in
a schema. VGI807 asks `historical_cutoff` for a primary key, which a one-row
snapshot of a moving boundary has no honest answer to.

## CI

`.github/workflows/ci.yml` runs on every push: ruff (lint and format), the
offline tests, a check that both entry-point scripts start without the dev
checkouts, and `vgi-lint`'s structural tier with `--audit-waivers`. All of it
resolves from PyPI (`UV_NO_SOURCES=1`), so it also proves the published
dependencies are sufficient.

`.github/workflows/live.yml` runs daily, never concurrently, and is the half
that touches Kalshi: the live tests, then `vgi-lint --execute`, which runs every
shipped example against the real API. That tier is deliberately not on push —
Kalshi rate-limits unauthenticated traffic to ~29 requests/second overall and
~4/s on `/events`, so two concurrent runs throttle each other into failures that
say nothing about the code.

It earns the wait. The behavioural tier has caught, on separate runs, a scan
that wedged the client uncancellably, a timestamp that crashed every consumer
of a row, and a shipped example that could not bind — none of which any of the
198 offline tests could see.

## Tests

```bash
pytest              # 198 offline tests
pytest -m live      # 31 tests against the public API
```

`tests/test_catalog.py` asserts the metadata the linter reads: every shipped
example is catalog-qualified, calls a real object, uses the right status
vocabulary for its position, and every function's declared result schema matches
the schema it actually returns.

`tests/test_auth.py` verifies each signature against the public half of a
throwaway key, so it checks Kalshi's scheme rather than merely that some bytes
were produced. `tests/test_packaging.py` checks the two entry-point scripts'
PEP-723 headers still cover every runtime dependency — they resolve
independently of `pyproject.toml`, so they drift silently and only an end-to-end
`ATTACH` notices.

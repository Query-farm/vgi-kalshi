# Changelog

## Unreleased

### Dependencies

- vgi-python 0.51.0 -> 0.51.1 (protocol 3.3.0) in pyproject.toml and both
  PEP-723 headers; `uv lock --upgrade` refreshes every other dependency to its
  latest release (pyarrow 26.0.1, httpx2 2.16.0, duckdb 2.0.0 for the dev
  group, pytest 9.2.0, ...).
- The explicit `numpy` dependency is gone: from 0.51.1 the SDK's `haybarn`
  extra depends on numpy itself, which the filter-binding UDF needs.
- ruff moves from the 0.16 to the 0.17 minor (0.17.5); the explicitly selected
  rule set reports nothing new on this tree.

### Fixed

- `historical_candlesticks` returned no rows for almost every archived market.
  Without `end_ts` its window was the 30 days before the archive cutoff, which
  misses any market that settled earlier; each market's window now ends at its
  own `close_time` (one `/historical/markets?tickers=` lookup per 100 tickers).
- `historical_markets(series, event_ticker => ...)`, and a pushed-down
  `WHERE event_ticker = ...`, failed with a 400: the archive endpoint rejects
  `series_ticker` and `event_ticker` together. An event now replaces the series
  in the request.
- Published examples that timed out or drew a 429 under `vgi-lint --execute`:
  the `events()` and `event_metadata()` examples that scanned every event in
  the series now ask for open events, and `historical_markets()`'s first
  example takes the newest archive page instead of sorting the whole archive.
- The `candlesticks()` example that fed every market in the series into
  `candlesticks()` now feeds only open markets, and the whole-exchange open
  interest rollup on `markets()` (a ~60 s scan of ~200k markets) is replaced by
  the series' largest open-interest markets. vgi-lint --execute is now clean.
- Example descriptions that called a `LIMIT 1` pick "the busiest open market"
  now say "an actively traded open market": nothing orders by volume there.

### Documentation

- Composed examples: an event rollup joining `events()` to `markets()` with the
  implied median from each strike ladder, and settled markets'
  volume-weighted traded price from `historical_trades()` next to what
  `historical_markets()` says they paid. New agent task `settled_event_vwap`.

## 1.3.0

### Operational

- Speaks VGI protocol 3.3: requires `vgi-python>=0.51.0` (was 0.50.0), in
  `pyproject.toml` and both PEP-723 headers. The vgi extension matches the
  protocol's major and minor version exactly, so a 3.2 worker is refused at
  ATTACH by the current extension, and this worker by a 3.2 one. The SQL
  surface is unchanged: the `series`, `exchange_status` and
  `historical_cutoff` scan functions stay callable because they are also
  listed as schema functions.
- Depends on `numpy`. vgi-python 0.51 binds pushed-down filters on a DuckDB
  connection that registers a Python UDF, which needs numpy; without it every
  filtered scan failed with "numpy is required for this operation".
- The `kalshi` secret type documents each field (`vgi_doc`) and links to
  Kalshi's API-key guide (`vgi_doc_url`).
- The README's ticks example and its end-to-end test use DuckDB's `lambda r:`
  syntax; DuckDB 2.0 rejects the old `r ->` form.

## 1.2.0

### Fixed

- A `kalshi` secret was only used if it was literally named `kalshi`. The
  framework keys resolved secrets by secret *name*, and the worker looked its
  secret up by *type*, so `CREATE SECRET my_key (TYPE kalshi, ...)` — and the
  unnamed form, which DuckDB calls `__default_kalshi` and which the
  `auth => 'required'` error message itself suggested — was ignored. Under the
  default `auth => 'auto'` the query then ran unsigned without a word. Secrets
  are now selected by type, whatever their name.
- When several `kalshi` secrets exist, `SCOPE` now chooses between them by the
  API base URL, so a production and a demo key can coexist.

### Operational

- Speaks VGI protocol 3.2: requires `vgi-python>=0.50.0` and `vgi-rpc>=0.53.1`
  (were 0.37.3 and 0.47.2), in `pyproject.toml` and both PEP-723 headers. The
  vgi extension matches the protocol's major and minor version exactly, so a
  worker on the older SDK is refused at ATTACH. The SQL surface is unchanged.
  Under the new SDK the `series` and `exchange_status` keys are declared as
  canonical `PRIMARY_KEY` and `NOT_NULL` constraint records, the form vgi.v3
  sends; the old `primary_key=`/`not_null=` keywords now fail at import.

- CI lints with `vgi-lint-check==2.2.0` (was 0.81.1, which speaks only the
  retired vgi.v2 protocol); the catalog passes its structural tier at
  `--fail-on warning`. The agent test tasks now publish only their names
  and prompts; the graders (reference SQL, success criteria) moved to
  `vgi-agent-tests.yaml`, which vgi-lint merges back in. The catalog gains a
  `vgi.subtitle`, `historical_cutoff` is titled "Archive Boundary", and the
  wide market result shapes carry a documented VGI417 waiver.

- Requires `vgi-python>=0.37.3` and `vgi-rpc>=0.47.2` (were 0.31.0 and
  0.44.1), in `pyproject.toml` and in both entry-point scripts' PEP-723
  headers. vgi-python now identifies a schema by its `path` rather than a
  `name`, so the catalog declares `Schema(path=["main"])`; under the old
  keyword the worker died at import. The SQL surface is unchanged.

- Depends on `vgi-python[haybarn]`. vgi-python 0.37 binds a pushed-down `WHERE`
  with an in-process DuckDB engine and depends on none itself, so without it
  every filtered scan failed with "No DuckDB-compatible engine is installed".
  The offline suite passed regardless — its dev group installs `duckdb`, which
  vgi-python accepts as a fallback — so a packaging test now requires the extra
  on every `vgi-python` requirement.

### Fixed

- A `WHERE` on any decimal column (`volume_24h_fp > 100`, `yes_bid_dollars > 0`)
  failed the scan with "referenced column ... changed type before evaluation".
  The worker widened decimals to `decimal128(38, s)` before evaluating pushed
  filters, a workaround for Arrow's comparison kernel; vgi-python 0.37
  evaluates filters with DuckDB, which needs no widening and rejects a batch
  whose types differ from the declared schema. The workaround is gone.

- The SQL end-to-end suite attaches through Haybarn, which ships the vgi
  extension, instead of `INSTALL vgi FROM community` — that returned 404 for
  every DuckDB 1.5.x, so all 20 tests had been skipping, in CI's `live.yml` too.
  Running them is what found the decimal bug above.

## 1.1.0

### Surface

Four columns Kalshi was already sending on every market, and that the schema
dropped. Each had been the binding constraint on a real analysis.

- `price_ranges` — the tick ladder, as `STRUCT("start", "end", step)[]` in
  dollars. The step is not constant within a book (`KXPRESNOMR` steps by 0.001
  below 0.10 and above 0.90, 0.01 in between; `KXFEDDECISION` and `KXBTCD` step
  by 0.01 throughout), so a raw spread is not comparable across markets. Divided
  by the step of the band the price sits in, all three series come back at a
  median spread of exactly 1.0 ticks — the raw numbers say the political book is
  five times tighter, and that is an artifact of the grid.

- `custom_strike` — `MAP(VARCHAR, VARCHAR)`. The machine-readable outcome for a
  `strike_type = 'custom'` market, where both strike columns are NULL because
  the outcome is categorical. The key is meaningful as well as the value:
  `KXFEDDECISION` splits into `{'Hike': '25'}` and `{'Cut': '25'}`. On an
  ordinary numeric strike it carries contract metadata instead, so it is not
  exclusive with `floor_strike`.

- `updated_time` — when Kalshi last changed the row. The staleness signal that
  volume only proxies: `KXPRESNOMR` has open contracts untouched for 55 days.

- `expiration_value` — what the underlying actually resolved to, in the series'
  own units; the column a backtest scores against, since `result` only says
  which side won. `VARCHAR`, because it is not always a number: `KXWTI` settles
  to `'85.76'`, `KXFEDDECISION` to `'Fed maintains rate'`.

All four are on `historical_markets` too, which derives from the market schema.

### Fixed

- Nested columns now convert their leaves to the declared type. Arrow refuses a
  decimal *string* inside a struct and Kalshi sends every price as one, so
  `price_ranges` would have built as silently all-NULL — a column that exists
  and carries nothing. The coercion walks the declared type and stays total, so
  a leaf Arrow would reject becomes NULL rather than costing the batch.

- `_sql_type` renders `MAP`, which it previously had no mapping for; declaring a
  map column raised `ValueError` at catalog build time.

## 1.0.0

First release considered production-ready.

### Surface

- `markets` and `historical_markets` expose `strike_type`, `floor_strike` and
  `cap_strike`. Without them a strike ladder could only be analysed by regexing
  the threshold out of the ticker — `subtitle` is not reliably populated — and
  `between` ranges were not readable at all.

- `series`, `exchange_status` and `historical_cutoff` catalog tables.
- Live market data: `markets`, `market`, `orderbook`, `candlesticks`, `trades`,
  `events`, `event`, `event_metadata`.
- The settled archive: `historical_markets`, `historical_trades`,
  `historical_candlesticks`.
- Optional API-key authentication (RSA-PSS request signing) via a DuckDB secret,
  with an `auth` ATTACH option choosing what happens when none resolves.
  Authentication is **not** a throughput win at Kalshi's entry tier — see the
  rate-limit table in the README before reaching for it.

### Performance

- Order books and candlesticks are fetched 100 markets per request, measured at
  5.89s → 0.13s for 100 books.
- Cursor-paged endpoints stream one API page per tick, so a `LIMIT` stops early.
  Previously such a scan walked every page before emitting and, because a scan
  blocked inside its first batch cannot be cancelled, wedged the client.
- Projection pushdown everywhere; filter pushdown on every endpoint that accepts
  one. `series WHERE category = 'Crypto'` fetches 0.2 MB instead of 16.4 MB.
- Brotli is negotiated, which is 35% smaller than gzip on the series catalog.

### Correctness

Three defects that only real payloads exposed, each of which would have
corrupted or killed a query rather than degrading it:

- Money that does not fit its declared decimal — non-finite, or needing more
  scale or precision — now becomes NULL rather than raising and failing every
  other row in the batch.
- Kalshi sends Go's zero time (`0001-01-01T00:00:00Z`) to mean "unset". Arrow
  stores it happily; every nanosecond-resolution consumer then raised
  `OverflowError` on materializing the row. Timestamps outside the
  nanosecond-representable window are now NULL.
- `GET` requests are retried on transient 5xx and dropped connections, not only
  on 429. A single reset connection used to abort a whole fan-out.
- Every column conversion is total: one malformed value becomes NULL rather
  than failing the batch it arrived in, including the nested `settlement_sources`
  column, which had no per-value guard at all.
- A cursor-paged scan freezes its query for the life of the walk. Pushdown
  filters are refreshed between ticks, and resuming an opaque Kalshi cursor
  under changed parameters is undefined — it would have returned plausible but
  wrong rows rather than erroring.
- A 200 carrying a CDN error page instead of JSON now raises with the path,
  content type and body rather than a bare `JSONDecodeError`.

### Security

- Path segments built from caller-supplied tickers are percent-encoded. A ticker
  of `../../portfolio/balance` previously resolved to `/trade-api/portfolio/
  balance` — outside the market-data prefix and into Kalshi's credentialed
  account surface, which this worker exists not to touch, and which the
  read-only guard could not catch because it inspects source literals rather
  than paths built at runtime. With authentication configured the request would
  have been signed as well.

### Operational

- One process-wide connection pool, so a paged scan does not pay a TLS
  handshake per page (138 ms → 50 ms).
- Live tests skip, rather than fail, when Kalshi's shared rate limit is
  exhausted after retries — that is an environment condition, not a defect.
- CI runs lint, types and the offline suite on every push against PyPI-resolved
  dependencies; the live tests and the example-executing lint tier run daily and
  never concurrently, because they share a per-IP rate limit.

### Notes

- Kalshi filters and reports market status in two different vocabularies:
  `status => 'open'` returns rows whose `status` column reads `active`. Both are
  documented, and the mapping is verified by a live test.
- `/events` is rate-limited far harder than the rest (~4 req/s against ~29), so
  pass `cache_ttl` when fanning out over events.

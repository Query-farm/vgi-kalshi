# Changelog

## Unreleased

### Operational

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

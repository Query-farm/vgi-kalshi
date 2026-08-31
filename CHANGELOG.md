# Changelog

## 1.0.0

First release considered production-ready.

### Surface

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

### Notes

- Kalshi filters and reports market status in two different vocabularies:
  `status => 'open'` returns rows whose `status` column reads `active`. Both are
  documented, and the mapping is verified by a live test.
- `/events` is rate-limited far harder than the rest (~4 req/s against ~29), so
  pass `cache_ttl` when fanning out over events.

# Kalshi notebooks

[01 · Quoted depth depends on the price band](01_quoted_depth_depends_on_the_band.ipynb) converts the YES/NO bid ladders of one public book to a common YES perspective and measures quoted quantities within a local price-distance band.

## Run

Install [uv](https://docs.astral.sh/uv/) and Git. Python 3.13–3.14 is required; uv can install it. Initial package, source and extension downloads require internet. No local worker checkout or API credentials are needed.

```sh
cd notebooks
uv sync --locked
uv run --locked python execute.py --timeout 300
```

Haybarn 2.0.0rc11 runs the notebook. The visible literal ATTACH uses `uvx --from git+https://github.com/Query-farm/vgi-kalshi@76094e91238d10683b992646ea18851c85ff1e86 ... vgi-kalshi`, with explicit vgi-python 0.51.0, vgi-rpc 0.53.1, httpx 0.28.1, httpx2 2.13.1, pyarrow 25.0.1, numpy 2.5.4 and internal worker haybarn 1.5.5rc1 pins. The internal engine supports worker filter binding and is separate from the notebook's Haybarn 2.0 engine. This is a Git install, not a PyPI worker package. uv caches the immutable revision and its environment. Upgrade the pin deliberately and validate default, experiment and live runs.

Optional editor kernel:

```sh
uv run --locked python -m ipykernel install --user --name vgi-kalshi-notebooks --display-name "Python (vgi-kalshi notebooks)"
```

Use this directory as working directory and Restart Kernel → Run All. The runner creates a temporary kernel and only replaces saved outputs after every cell succeeds.

## Experiment and live snapshot

```sh
uv run --locked python execute.py --parameters '{"BAND_CENTS":10}' --output-dir artifacts/band-10
uv run --locked python execute.py --parameters '{"DATA_MODE":"live"}' --output-dir artifacts/live
```

Restore `BAND_CENTS=2`, `DATA_MODE="replay"` and the recorded ticker for defaults. Parameter overrides require a separate destination. Branding, docs, fixture bodies, request audit, helper and lock accompany executed copies. Generated logs, images, experiments and environments are ignored.

Live uses the same exact market unless `MARKET_TICKER` is changed explicitly. The recorded contract closes at 2026-11-14 22:00:00 UTC; later live reads can have an empty or unavailable book. Empty sides preserve missing best quotes and zero returned quantities, and charts requiring levels are omitted. Unknown tickers or required-source failures raise errors. There is no synthetic fallback. The one-dollar binary-contract condition is checked before applying the reciprocal-price calculation.

## Recording and request bounds

The fixtures are complete, unmodified anonymous public API responses, captured sequentially from 2026-11-14 07:43:55.021061 through 07:43:55.243246 UTC. `fixtures/manifest.json` records exact URLs, request start/completion clocks, status, response headers, bytes and SHA-256 hashes. One `/markets/{ticker}` response verifies market identity; one `/markets/orderbooks?tickers=...` response contains its entire returned book. The ticker is an illustrative existing market, not an exchange-wide liquidity winner. The recording does not contain private data or credentials; see the root NOTICE for source terms.

`replay_source.py` verifies body hashes and serves only those two exact paths/query pairs on an ephemeral loopback port. Unknown requests fail without forwarding. The worker's documented `KALSHI_BASE_URL` setting is explicitly selected and restored during cleanup. ATTACH `auth 'off'` never signs requests. Full-book materialization uses `depth=>0` and `cache_ttl=>0`; no SQL LIMIT is presented as an HTTP bound. Replay checks exactly two requests and exact Decimal equality against every original level. The request audit is saved under `artifacts/request-audit.json`. Live may retry transient failures using the worker's bounded retry policy; it does not promise exactly two network attempts.

The market and book responses are not atomic. The displayed clock is acquisition completion, not an exchange event timestamp. The analysis constructs its spread exclusively from the book, not a mixture of separately sampled summary quotes. No other requests are issued to compute charts or change the local band.

## Reading the results

Kalshi's [official orderbook documentation](https://docs.kalshi.com/getting_started/orderbook_responses) describes both arrays as bids. For a one-dollar binary contract, a NO bid at p is equivalent to a YES ask at 1−p; its quoted quantity is retained. A missing side has no best quote and cannot produce a two-sided spread. Counts are contracts, including fractional quantities, not order counts or traders. Missing/invalid values fail checks; zero-size levels are reported and excluded.

Price and quantity calculations remain SQL decimals. Charts use floating point only for drawing. Each side measures distance from its own best quote, includes the boundary, and uses its own full positive-size book as the share denominator. Empty selected quantity is distinct from an unavailable quote. Wider bands can only retain or add levels and contracts in a fixed snapshot.

Resting quotes do not guarantee fills. Fees, queue position, cancellations, latency, counterparty behavior and changing prices are outside this static analysis. It estimates neither outcome probabilities nor trading returns and does not place orders. The supported live path remains public and read-only.

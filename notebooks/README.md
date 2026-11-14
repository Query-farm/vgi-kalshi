# Kalshi examples

[01 · How much depth is near the best quote?](01_quoted_depth_depends_on_the_band.ipynb)
compares local quoted depth with the full returned order book. It preserves Kalshi's
YES/NO bid semantics, exact decimal quantities, source provenance and missing sides.

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Query-farm/vgi-kalshi/blob/main/notebooks/01_quoted_depth_depends_on_the_band.ipynb)

In Colab, choose **Runtime → Run all**. Setup installs shared tools and retrieves
verified companions automatically. No worker checkout, Drive mount or API key is
needed. A compatible Python runtime (3.11+) runs the notebook; uv manages the worker's
separate Python 3.13 environment.

Locally, from this directory:

```sh
uv sync --locked
uv run --locked vgi-notebooks --timeout 300
uv run --locked vgi-notebooks --parameters '{"BAND_CENTS":10}' --output-dir artifacts/band-10
uv run --locked vgi-notebooks --parameters '{"DATA_MODE":"live"}' --output-dir artifacts/live
```

For Jupyter or VS Code, select this environment or register its kernel:

```sh
uv run --locked python -m ipykernel install --user --name vgi-kalshi-notebooks --display-name 'Python (Kalshi examples)'
```

The visible connection uses the published Kalshi **v1.3.0** release, with no transitive
dependency overrides in the SQL. Shared tooling is pinned to **v0.1.3** of
[vgi-example-notebooks](https://github.com/Query-farm/vgi-example-notebooks), which
owns the [standard](https://github.com/Query-farm/vgi-example-notebooks/blob/v0.1.3/docs/standard.md),
runner, consistent branding, SVG theme and Colab companion retrieval. Override runs
require a separate destination and cannot replace the last successful default.

## Data and interpretation

Default mode replays two complete, unmodified anonymous public API responses,
captured sequentially on **2026-11-14 07:43:55 UTC**. `fixtures/manifest.json` records
exact requests, capture clocks and body hashes. The market is illustrative;
it was not selected as an exchange-wide liquidity winner. The two responses are
not an atomic snapshot. `replay_source.py` serves only the recorded request pairs
on loopback, with no live fallback. Inherited SQL credentials are isolated before
connection creation; ATTACH explicitly uses `AUTH 'off'`.

The worker materializes one market and one full order book once. Every subsequent
band calculation, chart and comparison uses those in-memory tables. A NO bid at p
maps to a YES ask at 1−p only for the verified one-dollar binary contract. Quantities
are contracts, including fractional sizes, not traders or orders. Quoted sizes do
not guarantee fills or estimate returns. Fees, latency, queue position and future
price changes are outside the walkthrough.

Each side's band is inclusive and measured from its own best quote. Its share uses
that side's full positive-size book as denominator. Missing best quotes remain
unavailable rather than becoming zero. The charts show a near-quote view and a full
book view with explicitly different quantity scales; both use SVG output. The
publication runner retains native SVG and writes matching
files under `charts/` for GitHub's image renderer; those files belong in the same
commit as the notebook. This avoids GitHub's native SVG byte-string artifacts.

The recorded contract closes at **2026-11-14 22:00 UTC**. Later live reads may return
an empty/unavailable book. Live mode never substitutes recorded data after an error;
change MARKET_TICKER explicitly to inspect another market. Reset DATA_MODE='replay',
BAND_CENTS=2 and the recorded ticker to reproduce the saved example.

## Verification

Default and wider-band examples, a fresh notebook-only Python 3.12 environment,
request/hash checks and rendered charts are validated. The shared runner rejects
unexpected warnings, saved errors and machine-specific paths. A narrow workaround
handles the known JupySQL parser-docstring warning without hiding query failures.
A hosted Google Colab session has not been manually exercised; the direct link and
fresh-runtime bootstrap are tested separately.

Further planned narratives cover series/events/market discovery, trades versus
candlestick aggregation, and the live/archive cutoff. The first notebook is the
visual and execution pilot for those additional examples.

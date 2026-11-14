# Kalshi examples

[01 · How much depth is near the best quote?](01_quoted_depth_depends_on_the_band.ipynb)
compares local quoted depth with the full returned order book. It preserves Kalshi's
YES/NO bid semantics, exact decimal quantities, source provenance and missing sides.

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Query-farm/vgi-kalshi/blob/main/notebooks/01_quoted_depth_depends_on_the_band.ipynb)

In Colab, choose **Runtime → Run all**. Setup installs shared tools and retrieves
verified companions automatically. No worker checkout, Drive mount or API key is
needed. A compatible Python runtime (3.11+) runs the notebook; uv manages the worker's
separate Python 3.13 environment.

Colab installs the base shared package and retains compatible pandas, Matplotlib,
IPython and kernel libraries. Local publication uses the `[runner]` extra in this
directory’s locked environment. If an older notebook already installed conflicting
packages, save your work and choose **Runtime → Disconnect and delete runtime**,
reconnect, and run the updated example. A session restart alone does not repair
installed dependencies.

Locally, from this directory:

```sh
uv sync --locked
uv run --locked vgi-notebooks --timeout 300
uv run --locked vgi-notebooks 01_quoted_depth_depends_on_the_band.ipynb --parameters '{"BAND_CENTS":10}' --output-dir artifacts/band-10
uv run --locked vgi-notebooks 01_quoted_depth_depends_on_the_band.ipynb --parameters '{"DATA_MODE":"live"}' --output-dir artifacts/live
uv run --locked vgi-notebooks 02_discover_contracts_with_source_context.ipynb --parameters '{"MIN_VOLUME_CONTRACTS":300}' --output-dir artifacts/discovery-300
uv run --locked vgi-notebooks 03_reconcile_trades_and_candles.ipynb --parameters '{"REPORT_MINUTES":120}' --output-dir artifacts/report-120
uv run --locked vgi-notebooks 04_route_across_the_archive_boundary.ipynb --parameters '{"GAP_CENTS":60}' --output-dir artifacts/archive-60
```

For Jupyter or VS Code, select this environment or register its kernel:

```sh
uv run --locked python -m ipykernel install --user --name vgi-kalshi-notebooks --display-name 'Python (Kalshi examples)'
```

The visible connection uses the published Kalshi **v1.3.0** release, with no transitive
dependency overrides in the SQL. Shared tooling is pinned to **v0.3.6** of
[vgi-example-notebooks](https://github.com/Query-farm/vgi-example-notebooks), which
owns the [standard](https://github.com/Query-farm/vgi-example-notebooks/blob/v0.3.6/docs/standard.md),
runner, consistent branding, SVG theme and Colab companion retrieval. Override runs
require a separate destination and cannot replace the last successful default.

## Data and interpretation

Notebook 01 defaults to replaying two complete, unmodified anonymous public API responses,
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
request/hash checks and rendered charts are validated. All four notebooks also pass
with only their notebook file in a fresh directory: the shared bootstrap downloads
verified companions, and the full SQL workflow runs without worker source files. The shared runner rejects
unexpected warnings, saved errors and machine-specific paths. A narrow workaround
handles the known JupySQL parser-docstring warning without hiding query failures.
A hosted Google Colab session has not been manually exercised; the direct link and
fresh-runtime bootstrap are tested separately. Compatibility is also checked with
Python 3.12, pandas 2.2.3, IPython 8.30.0, ipykernel 6.17.1, jupyter-client 7.4.9
and Matplotlib 3.10.0 already installed and plotting libraries preloaded. The base
installation preserves those versions, does not install nbclient, and passes
`pip check`. The worker source checks pass independently: 268 offline tests, Ruff
lint/format, both entrypoint help commands and structural metadata 100/100 with
zero findings (the worker’s existing documented waivers are retained).

## Focused synthetic examples

| Notebook | Question | One-parameter experiment |
| --- | --- | --- |
| [02 · Discovery with source context](02_discover_contracts_with_source_context.ipynb) | Which contracts meet lifecycle, declared-source and activity checks without multiplying rows? | Volume threshold 100 → 300 contracts selects 3 → 1 of six contracts. |
| [03 · Trades and candles](03_reconcile_trades_and_candles.ipynb) | Can the tape reconcile hourly volume and closing price, including a quiet hour? | Reporting width 60 → 120 minutes preserves 120 contracts and the 81 USD price × quantity numerator. |
| [04 · Archive boundaries](04_route_across_the_archive_boundary.ipynb) | Which data kind belongs in the archive, and how do historical prices differ from settlement? | Gap threshold 30 → 60 cents flags 2 → 1 of three settled contracts. |

Notebooks 02–04 use **explicitly synthetic** responses from `surface_fixtures.json`.
All DEMOWX/DEMOPOWER names, contracts, prices, volumes, dates, outcomes and source
URLs are fictional. `kalshi_surfaces.py` serves only the declared loopback GET
routes with exact query validation; it has no live fallback. An unexpected route,
authorization header or write fails. Each run verifies exact request multiplicities
(10, 2 and 8 respectively), an empty SQL secret inventory, and 8, 9 or 10 visible
SQL invariants. All analytical transformations remain visible SQL. The fixture does
not establish Kalshi's current state, endpoint availability or data completeness.

Shared SVG rendering is used throughout. Review PNGs, request logs and experiments
are ignored artifacts; the published notebook keeps its executed SVG and linked
content-addressed chart assets. Experiment runs preserve successful defaults.

## Public SQL surface coverage

[coverage.json](coverage.json) maps each counted object to visible executed SQL,
source narrative, returned results and interpretation. Hash-bound catalog exports
include every function and table, with overloads counted once by `(schema, name)`.
Both the v1.3.0 release and public source baseline
`a1cfa37f22f6b71501c59bfd732df55290939e18` expose **17 distinct objects**. The four
notebooks meaningfully demonstrate **14/17 (82.4%)**, exceeding the nine-object
threshold for this interface. All examples execute v1.3.0; current-interface
compatibility is reviewed separately, not labeled as current-main execution.

The three uncovered names are `all_series`, `all_exchange_status` and
`all_historical_cutoff`, backing scans for the demonstrated catalog tables.
They remain in the denominator. No private account, order or position API is
registered in this read-only worker release. Optional authentication metadata does
not remove any object from coverage. See `coverage/` for complete raw exports,
reviewed snapshots and successful execution receipts.

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import haybarn
import matplotlib.pyplot as plt
import pandas as pd
from IPython import get_ipython
from IPython.display import display
from vgi_notebooks import finish, prose, theme

from replay_source import ReplaySource, request_key


def initialize(namespace):
    """Initialize the explicit recorded/live example, keeping SQL connection visible."""
    global BAND_CENTS, DATA_MODE, MARKET_TICKER, manifest, server, previous_base, started_at
    global fixture_secret_directory, con
    BAND_CENTS = namespace["BAND_CENTS"]
    DATA_MODE = namespace["DATA_MODE"]
    MARKET_TICKER = namespace["MARKET_TICKER"]
    assert type(BAND_CENTS) is int and 0 <= BAND_CENTS <= 100
    assert DATA_MODE in {"replay", "live"}
    assert isinstance(MARKET_TICKER, str) and re.fullmatch(r"[A-Z0-9.\-]{1,100}", MARKET_TICKER)
    manifest = json.loads(Path("fixtures/manifest.json").read_text())
    assert DATA_MODE != "replay" or manifest["ticker"] == MARKET_TICKER, (
        "Replay covers only the documented ticker"
    )
    server = ReplaySource(Path("fixtures")) if DATA_MODE == "replay" else None
    previous_base = os.environ.get("KALSHI_BASE_URL")
    os.environ["KALSHI_BASE_URL"] = server.url if server else "https://api.elections.kalshi.com/trade-api/v2"
    started_at = datetime.now(UTC).isoformat()
    fixture_secret_directory = TemporaryDirectory(prefix="vgi-fixture-secrets-")
    con = haybarn.connect(
        config={"secret_directory": fixture_secret_directory.name, "allow_persistent_secrets": False}
    )
    assert con.execute("SELECT count(*) FROM duckdb_secrets()").fetchall() == [(0,)]
    ip = get_ipython()
    from vgi_notebooks.runtime import load_sql

    load_sql(ip)
    namespace["con"] = con
    ip.run_line_magic("sql", "con")
    ip.run_line_magic("matplotlib", "inline")
    theme()


def plot_depth(curve, band, source):
    """Compare near-quote and full-book scales without changing SQL quantities."""
    if curve.empty:
        prose("**Depth curve omitted:** the returned book has no positive quoted levels.")
        return
    colors = {"YES bid": "#226B58", "YES ask (from NO bid)": "#C27638"}
    full_end = max(float(curve.distance_cents.max()), band, 1)
    near_end = min(full_end, max(10, band * 2))
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 5.8))
    for ax, endpoint, label in zip(
        axes, [near_end, full_end], ["NEAR THE BEST QUOTE", "FULL RETURNED BOOK"], strict=True
    ):
        maximum = 0
        for side, part in curve.groupby("book_side", sort=False):
            x = part.distance_cents.astype(float)
            y = part.cumulative_contracts.astype(float)
            ax.step(
                x,
                y,
                where="post",
                color=colors[side],
                linewidth=2.1,
                linestyle="-" if side == "YES bid" else "--",
                label=side,
            )
            visible = y[x <= endpoint]
            maximum = max(maximum, float(visible.max()) if len(visible) else 0)
            selected = y[x <= band]
            size = float(selected.iloc[-1]) if len(selected) else 0
            ax.plot(band, size, "o", color=colors[side], markersize=5)
        ax.axvspan(0, band, color="#D9E6DE", alpha=0.5, zorder=0)
        ax.axvline(band, color="#7F8D85", linewidth=1, linestyle=":")
        ax.set_xlim(0, endpoint * 1.02)
        ax.set_ylim(0, max(1, maximum) * 1.16)
        ax.grid(axis="y")
        ax.set_axisbelow(True)
        ax.set_xlabel("Cents worse than each side's best quote")
        ax.set_ylabel("Cumulative quoted contracts")
        ax.set_title(label, loc="left", fontsize=10, fontweight="bold", pad=14)
        ax.yaxis.set_major_formatter(lambda value, _: f"{value:,.0f}")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.065, 0.84), ncol=2, fontsize=10)
    finish(
        fig,
        "What looks deep overall can be thin near the quote",
        f"Shading marks the selected {band}¢ band. The panels use different quantity scales.",
        source,
        name="depth-band-" + str(band),
    )


def plot_band(depth, band, source):
    """Show quoted contracts within the chosen band with direct labels."""
    colors = {"YES bid": "#226B58", "YES ask (from NO bid)": "#C27638"}
    if not depth.all_positive_levels.sum():
        prose("**Band chart omitted:** no positive-size price levels are available.")
        return
    fig, ax = plt.subplots(figsize=(11.2, 5.2))
    best = depth.best_quote_contracts.astype(float)
    extra = (depth.band_contracts - depth.best_quote_contracts).astype(float)
    maximum = max(1, float(depth.band_contracts.max()))
    ax.barh(depth.book_side, best, color=[colors[s] for s in depth.book_side], height=0.45)
    ax.barh(
        depth.book_side, extra, left=best, color=[colors[s] for s in depth.book_side], alpha=0.25, height=0.45
    )
    for i, row in enumerate(depth.itertuples()):
        total = float(row.band_contracts)
        share = (
            "no full-book denominator"
            if pd.isna(row.band_share)
            else f"{row.band_share:.1%} of this side's full book"
        )
        ax.annotate(
            f"{total:,.2f} contracts",
            (total, i),
            xytext=(9, 4),
            textcoords="offset points",
            va="bottom",
            fontweight="bold",
            fontsize=11,
        )
        ax.annotate(
            share,
            (total, i),
            xytext=(9, -6),
            textcoords="offset points",
            va="top",
            color="#627468",
            fontsize=9,
        )
    ax.invert_yaxis()
    ax.set_xlim(0, maximum * 1.60)
    ax.set_xlabel("Quoted contracts · fractional quantities retained")
    ax.xaxis.set_major_formatter(lambda value, _: f"{value:,.0f}")
    ax.grid(axis="x")
    ax.set_axisbelow(True)
    finish(
        fig,
        f"Within {band}¢: separate the best quote from nearby depth",
        "Solid segments sit at the best quote; pale segments add levels within the band.",
        source,
        name="selected-band-" + str(band),
    )


def describe_snapshot():
    AS_OF = manifest["capture_completed_at_utc"] if server else datetime.now(UTC).isoformat()
    market = con.execute("SELECT * FROM market_snapshot").df()
    assert len(market) == 1 and market.ticker.iloc[0] == MARKET_TICKER
    assert con.execute(
        "SELECT market_type='binary' AND notional_value_dollars=1 FROM market_snapshot"
    ).fetchall()[0][0]
    rows = con.execute("SELECT side,price_dollars,count_fp FROM book ORDER BY side,price_dollars").fetchall()
    fingerprint = hashlib.sha256(json.dumps(rows, default=str).encode()).hexdigest()
    if server:
        raw = json.loads(Path("fixtures/orderbook.json").read_bytes())["orderbooks"][0]["orderbook_fp"]
        expected = sorted(
            (side, Decimal(price), Decimal(count))
            for side in ["yes", "no"]
            for price, count in raw.get(side + "_dollars", [])
        )
        assert rows == expected
        expected_requests = {request_key("/trade-api/v2" + e["request_path"]) for e in manifest["entries"]}
        assert len(server.requests) == 2 and {request_key(q) for q in server.requests} == expected_requests
        Path("artifacts").mkdir(exist_ok=True)
        Path("artifacts/request-audit.json").write_text(
            json.dumps(
                {"mode": "replay", "requests": server.requests, "misses": 0, "book_sha256": fingerprint},
                indent=2,
            )
        )
    if not server:
        Path("artifacts").mkdir(exist_ok=True)
        Path("artifacts/request-audit.json").write_text(
            json.dumps(
                {"mode": "live", "request_attempts": "not instrumented", "book_sha256": fingerprint}, indent=2
            )
        )
    display(market)
    prose(
        f"**{DATA_MODE.upper()} · {len(rows)} price levels** materialized for this verified "
        f"one-dollar binary market. Acquisition ended `{AS_OF}`; it is the retrieval clock, "
        "not an exchange event timestamp. All subsequent queries use local tables."
    )
    return AS_OF


def check_coverage(coverage):
    display(coverage)
    assert coverage.invalid_levels.sum() == 0
    assert (
        con.execute("SELECT count(*) FROM book WHERE side NOT IN ('yes','no') OR side IS NULL").fetchall()[0][
            0
        ]
        == 0
    )
    assert (
        con.execute(
            "SELECT count(*) FROM (SELECT side,price_dollars FROM book GROUP BY ALL HAVING count(*)<>1)"
        ).fetchall()[0][0]
        == 0
    )
    assert (
        con.execute("SELECT count(*) FROM book WHERE ticker IS DISTINCT FROM ?", [MARKET_TICKER]).fetchall()[
            0
        ][0]
        == 0
    )


def describe_quotes(quotes):
    display(quotes)
    q = quotes.iloc[0]
    if pd.isna(q.spread_cents):
        prose(
            "**The two-sided spread is unavailable:** at least one side has no positive quoted size. "
            "Available sides remain analyzable; no missing quote is replaced by zero."
        )
    else:
        prose(
            f"The book implies a **YES bid of {q.best_bid * 100:.2f} cents**, "
            f"**YES ask of {q.best_ask * 100:.2f} cents**, and **{q.spread_cents:.2f}-cent spread**. "
            "A negative observed spread would be reported as a snapshot inconsistency, "
            "not an arbitrage or execution claim. The separate market-summary quotes "
            "are not used to construct this book spread."
        )


def describe_band(depth):
    display(depth)
    assert depth.best_quote_contracts.le(depth.band_contracts).all()
    assert depth.band_contracts.le(depth.full_book_contracts).all()
    assert depth.band_levels.le(depth.all_positive_levels).all()
    for r in depth.itertuples():
        share = "unavailable" if pd.isna(r.band_share) else f"{r.band_share:.2%}"
        prose(
            f"**{r.book_side}:** within **{BAND_CENTS} cents**, **{r.band_contracts:,.2f} contracts** "
            f"across **{r.band_levels} levels**, or **{share}** of that side's full positive-size book. "
            f"The best quote alone carries **{r.best_quote_contracts:,.2f} contracts**. "
            "These are resting quotes, not completed trades."
        )


def close():
    con.close()
    fixture_secret_directory.cleanup()
    if server:
        server.close()
    if previous_base is None:
        os.environ.pop("KALSHI_BASE_URL", None)
    else:
        os.environ["KALSHI_BASE_URL"] = previous_base

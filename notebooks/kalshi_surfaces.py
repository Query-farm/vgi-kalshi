"""Strict fictional API transport and presentation for the focused SQL examples.

All domain calculations live in visible notebook SQL. This server only returns
explicit synthetic payloads; unexpected paths, queries, auth, and writes fail.
"""

import hashlib
import json
import os
import threading
from collections import Counter
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit

import haybarn
import matplotlib.pyplot as plt
import numpy as np
from IPython import get_ipython
from IPython.display import display
from vgi_notebooks import finish, prose, theme
from vgi_notebooks.runtime import load_sql

SOURCE = "Source: explicitly synthetic local API fixtures · real vgi-kalshi v1.3.0 · UTC"
GREEN, ORANGE, GRAY = "#226B58", "#C27638", "#9DA99E"


def epoch(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


class SyntheticAPI:
    def __init__(self):
        self.data = json.loads(Path("surface_fixtures.json").read_text())
        self.requests = []
        self.misses = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                parsed = urlsplit(self.path)
                query = parse_qs(parsed.query)
                path = parsed.path.removeprefix("/trade-api/v2")
                try:
                    assert not any(k.lower().startswith("kalshi-access") for k in self.headers)
                    assert "Authorization" not in self.headers
                    payload = owner.route(path, query)
                    owner.requests.append({"path": path, "query": query})
                    status = 200
                except (AssertionError, KeyError, ValueError, StopIteration) as error:
                    owner.misses.append({"path": path, "query": query, "reason": str(error)})
                    payload, status = {"error": "Request outside the declared synthetic fixture"}, 400
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                owner.misses.append({"method": "POST", "path": self.path})
                self.send_error(405)

            do_PUT = do_POST
            do_DELETE = do_POST
            do_PATCH = do_POST

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.http.server_port}/trade-api/v2"

    def route(self, path, query):
        data = self.data
        if path in {"/series", "/exchange/status", "/historical/cutoff"}:
            assert not query, f"Unexpected query for {path}"
            return {
                "/series": {"series": data["series"]},
                "/exchange/status": data["exchange_status"],
                "/historical/cutoff": data["historical_cutoff"],
            }[path]
        if path in {"/markets", "/events", "/historical/markets"}:
            expected = {"series_ticker": ["DEMOWX"], "limit": ["200" if path == "/events" else "1000"]}
            if path == "/markets":
                expected["mve_filter"] = ["exclude"]
            assert query == expected, f"Unexpected parameters for {path}"
            key = path.rsplit("/", 1)[-1]
            values = data["historical_markets"] if path == "/historical/markets" else data[key]
            return {key: values, "cursor": ""}
        if path.startswith("/events/"):
            assert not query
            ticker = path.split("/")[2]
            if path.endswith("/metadata"):
                return data["metadata"][ticker]
            assert path == f"/events/{ticker}"
            return {"event": next(e for e in data["events"] if e["event_ticker"] == ticker)}
        if path == "/markets/trades":
            assert query == {
                "ticker": ["DEMOWX-26NOV15-T20"],
                "min_ts": [str(epoch("2026-11-14T08:00:00Z"))],
                "max_ts": [str(epoch("2026-11-14T12:00:00Z"))],
                "limit": ["1000"],
            }
            return {"trades": data["trades"], "cursor": ""}
        if path == "/markets/candlesticks":
            assert query == {
                "market_tickers": ["DEMOWX-26NOV15-T20"],
                "period_interval": ["60"],
                "start_ts": [str(epoch("2026-11-14T08:00:00Z"))],
                "end_ts": [str(epoch("2026-11-14T12:00:00Z"))],
            }
            return {"markets": [{"market_ticker": "DEMOWX-26NOV15-T20", "candlesticks": data["candles"]}]}
        if path == "/historical/trades":
            ticker = query["ticker"][0]
            assert query == {"ticker": [ticker], "limit": ["1000"]}
            return {"trades": data["historical_trades"][ticker], "cursor": ""}
        if path.startswith("/historical/markets/") and path.endswith("/candlesticks"):
            ticker = path.split("/")[3]
            assert query == {
                "period_interval": ["60"],
                "start_ts": [str(epoch("2026-11-01T00:00:00Z"))],
                "end_ts": [str(epoch("2026-11-10T00:00:00Z"))],
            }
            return {"candlesticks": data["historical_candles"][ticker]}
        raise ValueError(f"Unexpected path: {path}")

    def close(self):
        self.http.shutdown()
        self.http.server_close()
        self.thread.join()


def initialize(namespace, story):
    global con, server, secret_directory, previous_base, current_story
    rules = {
        "discovery": ("MIN_VOLUME_CONTRACTS", 0, 1000),
        "tape": ("REPORT_MINUTES", 60, 120),
        "archive": ("GAP_CENTS", 0, 100),
    }
    name, low, high = rules[story]
    value = namespace[name]
    assert type(value) is int and low <= value <= high, f"{name} must be an integer from {low} to {high}"
    assert story != "tape" or value in {60, 120}, "REPORT_MINUTES must be 60 or 120"
    current_story = story
    server = SyntheticAPI()
    previous_base = os.environ.get("KALSHI_BASE_URL")
    os.environ["KALSHI_BASE_URL"] = server.url
    secret_directory = TemporaryDirectory(prefix="kalshi-example-secrets-")
    con = haybarn.connect(
        config={
            "secret_directory": secret_directory.name,
            "allow_persistent_secrets": False,
            "TimeZone": "UTC",
        }
    )
    assert con.execute("SELECT * FROM duckdb_secrets()").fetchall() == []
    ip = get_ipython()
    load_sql(ip)
    namespace["con"] = con
    ip.run_line_magic("sql", "con")
    ip.run_line_magic("matplotlib", "inline")
    theme()


def audit(expected):
    """Require exact endpoint multiplicities, no auth and no rejected requests."""
    observed = Counter(r["path"] for r in server.requests)
    assert not server.misses, server.misses
    assert observed == Counter(expected), (observed, expected)
    assert con.execute("SELECT * FROM duckdb_secrets()").fetchall() == []
    Path("artifacts").mkdir(exist_ok=True)
    Path(f"artifacts/{current_story}-request-audit.json").write_text(
        json.dumps(
            {
                "mode": "synthetic",
                "requests": server.requests,
                "unexpected_requests": server.misses,
                "fixture_sha256": hashlib.sha256(Path("surface_fixtures.json").read_bytes()).hexdigest(),
                "secret_count": 0,
            },
            indent=2,
        )
        + "\n"
    )
    prose(
        f"**Verified:** {len(server.requests)} local GET requests, no unexpected requests, "
        "and an empty SQL secret inventory. Every displayed source row came through the released worker."
    )


def check(frame):
    display(frame)
    assert frame.passed.all(), "A displayed SQL invariant failed"


def discovery_charts(markets, funnel, threshold):
    fig, ax = plt.subplots(figsize=(11.2, 6.1))
    colors = [GREEN if r.selected else GRAY for r in markets.itertuples()]
    ax.barh(
        markets.ticker.str.replace("DEMOWX-", "", regex=False),
        markets.volume_24h_fp.astype(float),
        color=colors,
        height=0.55,
    )
    ax.axvline(threshold, color=ORANGE, linestyle="--", label=f"Threshold: {threshold} contracts")
    ax.invert_yaxis()
    ax.set_xlabel("Synthetic trailing-24-hour volume · contracts")
    fig.legend(*ax.get_legend_handles_labels(), loc="upper left", bbox_to_anchor=(0.07, 0.84))
    for i, r in enumerate(markets.itertuples()):
        label = "selected" if r.selected else r.exclusion
        ax.text(float(r.volume_24h_fp) + 10, i, label, va="center", fontsize=9)
    ax.set_xlim(0, max(650, float(markets.volume_24h_fp.max()) * 1.4))
    finish(
        fig,
        "Activity alone does not establish source coverage",
        "Green contracts pass all three checks; gray labels explain the exclusion.",
        SOURCE,
        name=f"discovery-volume-{threshold}",
    )
    fig, ax = plt.subplots(figsize=(11.2, 5.4))
    bars = ax.barh(funnel.stage, funnel.contracts, color=[GRAY, GRAY, GREEN, GREEN], height=0.55)
    ax.bar_label(bars, padding=8, fmt="%.0f")
    ax.invert_yaxis()
    ax.set_xlim(0, 7)
    ax.set_xlabel("Synthetic contracts remaining · sequential checks")
    finish(
        fig,
        "Preserve the denominator as the shortlist narrows",
        "Source rows are grouped by event before joining, so two sources cannot double one contract.",
        SOURCE,
        name=f"discovery-funnel-{threshold}",
    )


def describe_discovery(markets, venues, threshold):
    n = int(markets.selected.sum())
    source_missing = int((markets.source_count == 0).sum())
    prose(
        f"At **{threshold} contracts** of trailing-24-hour volume, **{n} of {len(markets)} contracts** "
        "pass lifecycle, declared-source and activity checks. "
        f"**{source_missing}** belong to the event with no declared sources. "
        "A source URL is provenance metadata, not verification of its content. "
        f"The exchange-wide flag is true while **{int((~venues.trading_active).sum())} venue** "
        "has trading paused; these rows do not map a market to a venue, so they cannot certify "
        "its tradability. The detailed event says its threshold markets are not mutually exclusive: "
        "their YES prices should not be added as a probability partition."
    )


def tape_charts(hourly, report, minutes):
    fig, ax = plt.subplots(figsize=(11.2, 5.7))
    labels = [t.strftime("%H:%M") for t in hourly.end_period_ts]
    x = np.arange(len(hourly))
    for col, label, color, marker in [
        ("price_close_dollars", "Last execution in the hour", ORANGE, "s"),
        ("vwap_dollars", "Contract-weighted execution price", GREEN, "o"),
    ]:
        ax.plot(x, hourly[col].astype(float) * 100, marker=marker, color=color, label=label, linewidth=2)
    ax.text(1, 12, "No executions\nprice remains NULL", ha="center", fontsize=10, color=GRAY)
    ax.set_xticks(x, labels)
    ax.set_xlabel("Hourly interval end · 14 November 2026, UTC")
    ax.set_ylabel("YES price · cents per one-dollar contract")
    ax.set_ylim(0, 100)
    ax.legend(loc="upper left")
    finish(
        fig,
        "The last trade is not the volume-weighted price",
        "Both measures come from the same complete synthetic tape; the empty hour is not priced at zero.",
        SOURCE,
        name=f"tape-hourly-{minutes}",
    )
    fig, axes = plt.subplots(1, 2, figsize=(11.2, 5.7))
    labels = [t.strftime("%H:%M") for t in report.bucket_start]
    x = np.arange(len(report))
    axes[0].bar(x, report.contracts.astype(float), color=GREEN)
    axes[0].set_ylabel("Contracts executed")
    axes[1].bar(x, report.vwap_dollars.astype(float) * 100, color=GREEN, width=0.35, label="VWAP")
    axes[1].bar(
        x + 0.36,
        report.last_price_dollars.astype(float) * 100,
        color=ORANGE,
        width=0.35,
        label="Last execution",
    )
    axes[1].set_ylim(0, 100)
    axes[1].set_ylabel("YES price · cents per contract")
    axes[1].legend(loc="upper left", fontsize=9)
    for ax in axes:
        ax.set_xticks(x + (0.18 if ax == axes[1] else 0), labels)
        ax.set_xlabel("Reporting interval start · UTC")
    finish(
        fig,
        f"Regroup the tape into {minutes}-minute reporting windows",
        "The query carries price × quantity and quantity separately; "
        "averaging candle closes would not do this.",
        SOURCE,
        name=f"tape-report-{minutes}",
    )


def describe_tape(report, minutes):
    total = float(report.contracts.sum())
    numerator = float(report.yes_notional_dollars.sum())
    prose(
        f"The **{minutes}-minute** report contains **{len(report)} windows** "
        f"and **{total:,.0f} executed contracts**. Summed price × quantity is **{numerator:.2f} USD**, "
        f"so the whole-window YES VWAP is **{100 * numerator / total:.2f} cents**. "
        "Regrouping changes the within-window averages and last trades, while the quantity and "
        "numerator stay fixed. This describes executions; it does not measure resting depth or "
        "establish an executable price."
    )


def archive_charts(routing, comparison, gap):
    fig, ax = plt.subplots(figsize=(11.2, 5.6))
    x = np.arange(len(routing))
    for offset, col, label in [
        (-0.18, "markets_source", "Markets / candles"),
        (0.18, "trades_source", "Trades"),
    ]:
        archive = routing[col].eq("archive").to_numpy()
        ax.scatter(
            x + offset,
            np.where(archive, 0, 1),
            s=100,
            marker="s" if offset < 0 else "o",
            label=label,
            color=GREEN if offset < 0 else ORANGE,
        )
    ax.set_xticks(x, [t.strftime("%d Nov") for t in routing.requested_time])
    ax.set_yticks([0, 1], ["Archive", "Live"])
    ax.set_ylim(-0.45, 1.45)
    ax.set_xlabel("Illustrative requested timestamp · 2026, UTC")
    ax.legend(loc="upper right")
    finish(
        fig,
        "A timestamp can sit on different sides of two cutoffs",
        "The fixture moves markets on 10 November and trades on 12 November; equality selects the live side.",
        SOURCE,
        name=f"archive-routing-{gap}",
    )
    fig, ax = plt.subplots(figsize=(11.2, 5.8))
    x = np.arange(len(comparison))
    for col, label, marker, color in [
        ("vwap_dollars", "Executed VWAP", "o", GREEN),
        ("last_close_dollars", "Last candle close", "s", ORANGE),
        ("settlement_value_dollars", "Settlement value", "D", "#253C30"),
    ]:
        ax.scatter(comparison[col].astype(float) * 100, x, s=75, marker=marker, label=label, color=color)
    for i, row in enumerate(comparison.itertuples()):
        ax.plot(
            [float(row.last_close_dollars) * 100, float(row.settlement_value_dollars) * 100],
            [i, i],
            color=GRAY,
            linewidth=1,
            zorder=0,
        )
    ax.set_yticks(
        x,
        [
            r.ticker.removeprefix("DEMOWX-") + (" · review" if r.review else "")
            for r in comparison.itertuples()
        ],
    )
    ax.set_xlim(-3, 103)
    ax.set_xlabel("YES value · cents per one-dollar contract")
    fig.legend(
        *ax.get_legend_handles_labels(), loc="upper left", bbox_to_anchor=(0.07, 0.84), ncol=3, fontsize=10
    )
    finish(
        fig,
        "A settled payoff is distinct from its execution history",
        f"The review threshold is {gap} cents of absolute last-close-to-settlement difference; "
        "no profit is inferred.",
        SOURCE,
        name=f"archive-values-{gap}",
    )


def describe_archive(comparison, gap):
    selected = comparison.loc[comparison.review]
    prose(
        f"**{len(selected)} of {len(comparison)} archived contracts** have an absolute "
        f"last-close-to-settlement gap of at least **{gap} cents**. "
        "Each tape has **40 executed contracts**, and its two candle volumes reconcile to the same total. "
        "The review flag compares two different quantities at two different times; it is neither "
        "a return calculation nor evidence that a quoted price could have been traded. "
        "Raising the threshold changes the review list without changing the archived source rows."
    )


def close():
    assert con.execute("SELECT * FROM duckdb_secrets()").fetchall() == []
    con.close()
    server.close()
    secret_directory.cleanup()
    if previous_base is None:
        os.environ.pop("KALSHI_BASE_URL", None)
    else:
        os.environ["KALSHI_BASE_URL"] = previous_base

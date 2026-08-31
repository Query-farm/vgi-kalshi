"""Cache policy: what Kalshi declares, and what we forward to DuckDB.

Kalshi's own headers drive this. Reference endpoints send
``Cache-Control: public, max-age=15``; live market data sends no freshness
directive at all; nothing sends an ETag or Last-Modified, so no result can ever
be cheaply revalidated — only expired.
"""

from __future__ import annotations

import httpx
import pytest

from vgi_kalshi.kalshi_api import CacheHint
from vgi_kalshi.markets import (
    _candlestick_cache_control,
    _opt_in_cache_control,
    _origin_cache_control,
)


def _response(cache_control: str | None) -> httpx.Response:
    headers = {"cache-control": cache_control} if cache_control is not None else {}
    return httpx.Response(200, headers=headers)


class TestCacheHint:
    def test_reads_max_age(self) -> None:
        hint = CacheHint()
        hint.observe(_response("public, max-age=15"))
        assert hint.max_age == 15
        assert hint.cacheable

    def test_absent_directive_marks_live(self) -> None:
        hint = CacheHint()
        hint.observe(_response(None))
        assert hint.max_age is None
        assert not hint.cacheable

    def test_shortest_page_bounds_a_paged_call(self) -> None:
        hint = CacheHint()
        hint.observe(_response("max-age=60"))
        hint.observe(_response("max-age=15"))
        assert hint.max_age == 15

    def test_one_uncacheable_page_poisons_the_whole_result(self) -> None:
        """A result is only as cacheable as its least cacheable page."""
        hint = CacheHint()
        hint.observe(_response("max-age=15"))
        hint.observe(_response(None))
        assert not hint.cacheable

    def test_no_store_directive_without_max_age_is_not_cacheable(self) -> None:
        hint = CacheHint()
        hint.observe(_response("no-store"))
        assert not hint.cacheable

    @pytest.mark.parametrize(
        "directive", ["no-store, max-age=15", "max-age=15, no-cache", "private, max-age=15"]
    )
    def test_a_reuse_ban_beats_a_max_age(self, directive: str) -> None:
        """`no-store` next to a max-age must not read as cacheable.

        Reading only the max-age would pair a freshness lifetime with
        `stale_if_error`, licensing minutes of stale serving from an origin that
        asked for none.
        """
        hint = CacheHint()
        hint.observe(_response(directive))
        assert not hint.cacheable

    def test_max_age_zero_is_not_a_cache_entry(self) -> None:
        """ "Already stale" is not something worth storing."""
        hint = CacheHint()
        hint.observe(_response("public, max-age=0"))
        assert not hint.cacheable

    def test_max_age_is_matched_on_a_directive_boundary(self) -> None:
        hint = CacheHint()
        hint.observe(_response("s-maxage=99, max-age=15"))
        assert hint.max_age == 15


class TestOriginPolicy:
    def test_forwards_the_declared_ttl(self) -> None:
        hint = CacheHint(max_age=15)
        control = _origin_cache_control(hint)
        assert control is not None
        assert control.ttl == 15

    def test_live_data_is_left_uncached(self) -> None:
        assert _origin_cache_control(CacheHint(saw_uncacheable=True)) is None


class TestOptInPolicy:
    def test_off_by_default(self) -> None:
        assert _opt_in_cache_control(0, per_value=True) is None

    def test_opt_in_enables_per_value_memoization(self) -> None:
        control = _opt_in_cache_control(30, per_value=True)
        assert control is not None
        assert control.ttl == 30
        assert control.per_value is True


class TestCandlestickImmutability:
    """A closed candle can never change; a forming one must not be cached."""

    NOW = 1_788_000_000

    def test_fully_closed_window_caches_for_a_day(self) -> None:
        control = _candlestick_cache_control(end_ts=self.NOW - 7200, period_interval=60, now=self.NOW)
        assert control is not None
        assert control.ttl == 86_400
        assert control.per_value is True

    def test_window_running_up_to_now_is_not_cached(self) -> None:
        assert _candlestick_cache_control(end_ts=self.NOW, period_interval=60, now=self.NOW) is None

    @pytest.mark.parametrize("interval", [1, 60, 1440])
    def test_boundary_respects_the_period_width(self, interval: int) -> None:
        """The cutoff scales with the candle width, not a fixed constant."""
        period = interval * 60
        assert (
            _candlestick_cache_control(end_ts=self.NOW - period - 1, period_interval=interval, now=self.NOW)
            is not None
        )
        assert (
            _candlestick_cache_control(end_ts=self.NOW - period + 1, period_interval=interval, now=self.NOW)
            is None
        )


class TestRateLimitRetry:
    """Kalshi 429s under LATERAL fan-out and sends no Retry-After to obey."""

    def test_retries_then_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_kalshi import kalshi_api

        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] < 3:
                return httpx.Response(429, json={"error": {"code": "too_many_requests"}})
            return httpx.Response(200, json={"series": []}, headers={"cache-control": "max-age=15"})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        hint = CacheHint()
        assert kalshi_api.series_list(client=client, hint=hint) == []
        assert calls["n"] == 3
        assert hint.max_age == 15, "the served response's policy is what counts, not the 429s"

    def test_gives_up_and_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_kalshi import kalshi_api

        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(429, json={"error": {"code": "too_many_requests"}})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(kalshi_api.KalshiError) as excinfo:
            kalshi_api.series_list(client=client)
        assert excinfo.value.status == 429
        assert calls["n"] == kalshi_api._RETRY_ATTEMPTS

    def test_a_429_never_poisons_the_cache_hint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A rate-limit response carries the CDN's policy, not the resource's."""
        from vgi_kalshi import kalshi_api

        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        hint = CacheHint()
        seen = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["n"] += 1
            if seen["n"] == 1:
                return httpx.Response(429, headers={"cache-control": "max-age=0"}, json={})
            return httpx.Response(200, json={"series": []}, headers={"cache-control": "max-age=15"})

        kalshi_api.series_list(client=httpx.Client(transport=httpx.MockTransport(handler)), hint=hint)
        assert hint.cacheable
        assert hint.max_age == 15


class TestTransientFailures:
    """A GET is idempotent, so every transient failure is worth retrying.

    Retrying only the rate limiter would let a single dropped connection abort a
    LATERAL fan-out that had already made 300 successful calls.
    """

    def test_retries_a_transient_5xx(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_kalshi import kalshi_api

        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] < 3:
                return httpx.Response(502, text="bad gateway")
            return httpx.Response(200, json={"series": []})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        assert kalshi_api.series_list(client=client) == []
        assert calls["n"] == 3

    def test_retries_a_dropped_connection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_kalshi import kalshi_api

        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] < 3:
                raise httpx.ConnectError("connection reset", request=request)
            return httpx.Response(200, json={"series": []})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        assert kalshi_api.series_list(client=client) == []
        assert calls["n"] == 3

    def test_a_permanent_transport_failure_surfaces(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from vgi_kalshi import kalshi_api

        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            raise httpx.ConnectError("no route to host", request=request)

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(httpx.ConnectError):
            kalshi_api.series_list(client=client)
        assert calls["n"] == kalshi_api._RETRY_ATTEMPTS

    def test_a_4xx_is_not_retried(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A 400 `invalid status` will say the same thing five times over."""
        from vgi_kalshi import kalshi_api

        monkeypatch.setattr(kalshi_api.time, "sleep", lambda _s: None)
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(400, text='{"error":{"code":"bad_request"}}')

        client = httpx.Client(transport=httpx.MockTransport(handler))
        with pytest.raises(kalshi_api.KalshiError):
            kalshi_api.series_list(client=client)
        assert calls["n"] == 1


class TestPaging:
    """`_paged` must never hand back a truncated result that looks whole."""

    @staticmethod
    def _endless(seen: list[dict[str, str]]) -> httpx.Client:
        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(dict(request.url.params))
            return httpx.Response(200, json={"markets": [{"ticker": "T"}], "cursor": "more"})

        return httpx.Client(transport=httpx.MockTransport(handler))

    def test_running_out_of_pages_raises(self) -> None:
        from vgi_kalshi import kalshi_api

        seen: list[dict[str, str]] = []
        with pytest.raises(kalshi_api.KalshiPageLimitError) as excinfo:
            kalshi_api.markets("KXBTCD", client=self._endless(seen))
        assert len(seen) == kalshi_api.MAX_PAGES
        assert excinfo.value.rows == kalshi_api.MAX_PAGES

    def test_a_small_limit_asks_for_a_small_page(self) -> None:
        """Requesting 1000 rows to return 1 is a wasted page on both ends."""
        from vgi_kalshi import kalshi_api

        seen: list[dict[str, str]] = []
        rows = kalshi_api.markets("KXBTCD", limit=1, client=self._endless(seen))
        assert len(rows) == 1
        assert seen == [{"series_ticker": "KXBTCD", "limit": "1"}]

    def test_a_large_limit_still_asks_for_a_full_page(self) -> None:
        """The page size is capped by the endpoint, never raised by the caller."""
        from vgi_kalshi import kalshi_api

        seen: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(dict(request.url.params))
            return httpx.Response(200, json={"markets": [{"ticker": "T"}]})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        kalshi_api.markets("KXBTCD", limit=kalshi_api.PAGE_LIMIT + 5, client=client)
        assert seen[0]["limit"] == str(kalshi_api.PAGE_LIMIT)

    def test_events_keeps_its_own_smaller_page_cap(self) -> None:
        """`/events` 400s on anything over 200, so it must not inherit the 1000."""
        from vgi_kalshi import kalshi_api

        seen: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(dict(request.url.params))
            return httpx.Response(200, json={"events": [{"event_ticker": "E"}]})

        client = httpx.Client(transport=httpx.MockTransport(handler))
        kalshi_api.events("KXBTCD", client=client)
        assert seen[0]["limit"] == str(kalshi_api.EVENTS_PAGE_LIMIT)

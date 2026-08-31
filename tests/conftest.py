"""Shared fixtures and hooks for the test suite.

The only thing here is rate-limit handling for the live tests, and it exists to
keep a red suite meaningful. Kalshi limits unauthenticated traffic across
everything sharing an IP, so a live test can fail because something *else* was
talking to Kalshi at the same time — another CI job, a `vgi-lint --execute`
run, a developer's shell. That failure says nothing about the code, and a suite
that cries wolf is one people stop reading.

`kalshi_api._get` already retries a 429 five times with exponential backoff. A
429 that survives all of that is an environment condition, not a defect, so it
becomes a skip with a loud reason rather than a failure.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest

from vgi_kalshi import kalshi_api
from vgi_kalshi.kalshi_api import KalshiError


@pytest.fixture(autouse=True)
def _hermetic_connection_pool() -> Generator[None]:
    """Give every test a clean connection pool.

    `kalshi_api` keeps a process-wide client so a paged scan does not pay a TLS
    handshake per page. That is global state: without resetting it, a client
    built against one test's mock transport would serve the next test, and the
    order tests happen to run in would decide whether they pass.
    """
    kalshi_api.reset_shared_client()
    yield
    kalshi_api.reset_shared_client()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[Any]) -> Generator[None, Any]:
    """Rewrite an exhausted-rate-limit failure into a skip, for live tests only.

    Scoped to the ``live`` marker on purpose: an offline test seeing a 429 means
    a mocked transport produced one, which is a real failure of that test's own
    setup and must stay red.
    """
    outcome = yield
    report = outcome.get_result()
    if item.get_closest_marker("live") is None or report.outcome != "failed":
        return
    exception = getattr(call, "excinfo", None)
    if exception is None or not isinstance(exception.value, KalshiError):
        return
    if exception.value.status != 429:
        return
    # A skip report's longrepr is the (path, lineno, reason) triple pytest -rs
    # prints; a bare string here renders as an error instead.
    report.outcome = "skipped"
    report.longrepr = (
        str(item.path),
        item.location[1] or 0,
        "Skipped: Kalshi rate limit exhausted after retries — another client is "
        "sharing this IP's budget. Re-run when it is quiet.",
    )

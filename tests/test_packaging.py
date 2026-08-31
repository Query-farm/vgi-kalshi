"""The two entry-point scripts must be runnable on their own.

`uv run kalshi_worker.py` resolves dependencies from the script's PEP-723
header, not from `pyproject.toml`, so the two lists drift silently: the package
imports fine under `pytest` while the worker a client actually launches dies at
import. Adding `cryptography` for request signing broke exactly this way, and
nothing but an end-to-end ATTACH noticed.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ("kalshi_worker.py", "serve.py")

#: PEP 723 inline script metadata: a `# /// script` ... `# ///` comment block.
_BLOCK = re.compile(r"^# /// script$(.+?)^# ///$", re.MULTILINE | re.DOTALL)


def _script_dependencies(path: Path) -> set[str]:
    """The distribution names a script's inline metadata declares."""
    match = _BLOCK.search(path.read_text())
    assert match is not None, f"{path.name} has no PEP-723 script header"
    body = "".join(
        line.removeprefix("# ").removeprefix("#") for line in match.group(1).splitlines(keepends=True)
    )
    return {_name(spec) for spec in tomllib.loads(body)["dependencies"]}


def _name(requirement: str) -> str:
    """The bare distribution name from a requirement string."""
    return re.split(r"[\[><=!~;\s]", requirement, maxsplit=1)[0].strip().lower()


@pytest.fixture(scope="module")
def project_dependencies() -> set[str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return {_name(spec) for spec in data["project"]["dependencies"]}


class TestScriptHeaders:
    @pytest.mark.parametrize("script", SCRIPTS)
    def test_header_covers_every_runtime_dependency(
        self, script: str, project_dependencies: set[str]
    ) -> None:
        declared = _script_dependencies(ROOT / script)
        missing = project_dependencies - declared
        assert missing == set(), (
            f"{script} would fail at import: its PEP-723 header is missing {sorted(missing)}"
        )

    @pytest.mark.parametrize("script", SCRIPTS)
    def test_header_declares_nothing_unknown(self, script: str, project_dependencies: set[str]) -> None:
        """The scripts also pull vgi-rpc directly, but nothing beyond that."""
        extra = _script_dependencies(ROOT / script) - project_dependencies - {"vgi-rpc"}
        assert extra == set(), f"{script} declares dependencies the project does not: {sorted(extra)}"


class TestTransport:
    """Content encoding is negotiated by what is installed, not by our code.

    httpx builds `Accept-Encoding` from the codecs it can find at import time,
    so dropping the brotli dependency would not fail anything — it would just
    silently downgrade the `series` scan from 1.35 MB to 2.06 MB on the wire.
    A behavioural check is the only thing that notices.
    """

    @staticmethod
    def _request_headers() -> httpx.Headers:
        seen: dict[str, httpx.Headers] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["headers"] = request.headers
            return httpx.Response(200, json={})

        from vgi_kalshi import kalshi_api

        with httpx.Client(transport=httpx.MockTransport(handler), timeout=kalshi_api.TIMEOUT) as client:
            client.get("https://example.invalid/x")
        return seen["headers"]

    def test_brotli_is_offered(self) -> None:
        """Kalshi serves `br` when offered; it is 35% smaller than its gzip."""
        assert "br" in self._request_headers()["accept-encoding"]

    def test_gzip_is_still_offered_as_a_fallback(self) -> None:
        assert "gzip" in self._request_headers()["accept-encoding"]

# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python[http]", "vgi-rpc", "httpx", "cryptography>=42"]
#
# [tool.uv.sources]
# vgi-python = { path = "../vgi-python" }
# vgi-rpc = { path = "../vgi-rpc" }
# ///
"""HTTP entry point for the Kalshi VGI worker (``uv run serve.py --port 8000``)."""

from __future__ import annotations

from vgi_kalshi.worker import main_http

if __name__ == "__main__":
    main_http()

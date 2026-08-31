# /// script
# requires-python = ">=3.13"
# dependencies = ["vgi-python[http]", "vgi-rpc", "httpx", "cryptography>=42"]
#
# [tool.uv.sources]
# vgi-python = { path = "../vgi-python" }
# vgi-rpc = { path = "../vgi-rpc" }
# ///
"""Stdio entry point for the Kalshi VGI worker (``uv run``).

ATTACH 'kalshi' (TYPE vgi, LOCATION 'uv run kalshi_worker.py');
"""

from __future__ import annotations

from vgi_kalshi.worker import main

if __name__ == "__main__":
    main()

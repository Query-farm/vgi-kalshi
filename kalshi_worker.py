# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "vgi-python[http]>=0.31.0",
#     "vgi-rpc>=0.44.1",
#     "httpx>=0.27",
#     "cryptography>=42",
#     "brotli>=1.1",
# ]
# ///
"""Stdio entry point for the Kalshi VGI worker (``uv run``).

ATTACH 'kalshi' (TYPE vgi, LOCATION 'uv run kalshi_worker.py');
"""

from __future__ import annotations

from vgi_kalshi.worker import main

if __name__ == "__main__":
    main()

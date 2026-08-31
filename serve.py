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
"""HTTP entry point for the Kalshi VGI worker (``uv run serve.py --port 8000``)."""

from __future__ import annotations

from vgi_kalshi.worker import main_http

if __name__ == "__main__":
    main_http()

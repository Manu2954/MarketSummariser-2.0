from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, List, Optional

import requests

from .config import RequestConfig


def _to_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _base_url() -> str:
    return os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")


def _klines_path() -> str:
    path = os.getenv("BINANCE_KLINES_PATH", "/api/v3/klines")
    return path if path.startswith("/") else f"/{path}"


def fetch_klines(
    symbol: str,
    interval: str,
    start_time: datetime,
    end_time: Optional[datetime],
    request_cfg: RequestConfig,
    logger: logging.Logger,
) -> list[list[Any]]:
    """
    Paginate through Binance klines for the provided range.
    """
    params_base = {"symbol": symbol, "interval": interval, "limit": request_cfg.limit}
    current_start = _to_millis(start_time)
    end_ms = _to_millis(end_time) if end_time else None
    all_rows: List[list[Any]] = []

    while True:
        params = dict(params_base)
        params["startTime"] = current_start
        if end_ms:
            params["endTime"] = end_ms
        logger.debug("Requesting klines", extra={"symbol": symbol, "interval": interval, "start_ms": current_start})
        resp = requests.get(f"{_base_url()}{_klines_path()}", params=params, timeout=request_cfg.timeout)
        resp.raise_for_status()
        chunk: list[list[Any]] = resp.json()
        if not chunk:
            break
        all_rows.extend(chunk)
        last_close = chunk[-1][6]
        if end_ms and last_close >= end_ms:
            break
        if len(chunk) < request_cfg.limit:
            break
        current_start = last_close + 1
        time.sleep(request_cfg.rate_limit_sleep)

    return all_rows

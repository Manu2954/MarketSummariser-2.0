from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from .config import resolve_timezone

RAW_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_buy_base",
    "taker_buy_quote",
    "ignore",
]


def klines_to_dataframe(
    klines: list[list[Any]],
    symbol: str,
    interval: str,
    tz_name: Optional[str],
) -> pd.DataFrame:
    df = pd.DataFrame(klines, columns=RAW_COLUMNS)
    if df.empty:
        return df

    ts_utc = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    tz = resolve_timezone(tz_name)
    ts = ts_utc.dt.tz_convert(tz) if tz else ts_utc
    df["timestamp"] = ts

    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume", "taker_buy_base", "taker_buy_quote"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trades"] = pd.to_numeric(df["trades"], errors="coerce").astype("Int64")

    df["interval"] = interval
    df["symbol"] = symbol

    df = df.drop(columns=["ignore", "close_time", "open_time"])
    df = df.drop_duplicates(subset=["timestamp", "symbol", "interval"]).sort_values("timestamp")
    return df.reset_index(drop=True)


def count_missing_rows(df: pd.DataFrame, interval: str) -> Optional[int]:
    if df.empty:
        return 0
    step = _interval_seconds(interval)
    if not step:
        return None
    first, last = df["timestamp"].iloc[0], df["timestamp"].iloc[-1]
    expected = int((last - first).total_seconds() / step) + 1
    missing = expected - len(df)
    return max(missing, 0)


def _interval_seconds(interval: str) -> Optional[int]:
    interval = interval.strip().lower()
    units = {"m": 60, "h": 3600, "d": 86400, "w": 604800}
    value_part = "".join(ch for ch in interval if ch.isdigit())
    unit_part = "".join(ch for ch in interval if ch.isalpha())
    if not value_part or unit_part not in units:
        return None
    return int(value_part) * units[unit_part]

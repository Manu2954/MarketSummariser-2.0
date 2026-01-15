from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import ExcelConfig


def read_dataframe(excel_cfg: ExcelConfig, symbol: str, interval: str) -> pd.DataFrame:
    path = Path(str(excel_cfg.path).format(symbol=symbol, interval=interval))
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, parse_dates=["timestamp"])
    except Exception:
        return pd.DataFrame()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        if pd.api.types.is_datetime64tz_dtype(df["timestamp"]):
            df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    return df


def write_dataframe(
    df: pd.DataFrame,
    excel_cfg: ExcelConfig,
    symbol: str,
    interval: str,
    logger: logging.Logger,
) -> Optional[pd.DataFrame]:
    if df.empty:
        logger.info("No data to write", extra={"symbol": symbol, "interval": interval})
        return None

    path_str = str(excel_cfg.path).format(symbol=symbol, interval=interval)
    path: Path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    combined = df

    if path.exists() and excel_cfg.append:
        try:
            existing = pd.read_csv(path, parse_dates=["timestamp"])
            combined = pd.concat([existing, df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["timestamp", "symbol", "interval"]).sort_values("timestamp")
        except Exception:
            combined = df
    elif not excel_cfg.append:
        combined = df

    combined.to_csv(path, index=False)
    logger.info(
        "Wrote CSV",
        extra={
            "symbol": symbol,
            "interval": interval,
            "rows": len(combined),
            "path": str(path),
        },
    )
    return combined

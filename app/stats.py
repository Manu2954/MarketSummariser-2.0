from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from .binance_client import fetch_klines
from .config import AppConfig, load_config, parse_datetime, parse_duration, resolve_timezone
from .csv_writer import write_dataframe
from .ingest import build_logger
from .transform import klines_to_dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute p95 and average volume for a time slice.")
    parser.add_argument("-c", "--config", default="config.yml", help="Path to YAML config file.")
    parser.add_argument("--symbol", required=True, help="Symbol (e.g., BTCUSDT).")
    parser.add_argument("--interval", required=True, help="Interval (e.g., 1h, 15m).")
    parser.add_argument("--start", help="Start time ISO 8601 (e.g., 2024-05-01T00:00:00Z).")
    parser.add_argument("--end", help="End time ISO 8601.")
    parser.add_argument("--lookback", help="Lookback window (e.g., 3d, 12h) if start/end not provided.")
    parser.add_argument("--time-input-tz", help="Timezone to interpret start/end when no tz offset is present (e.g., Asia/Kolkata).")
    return parser.parse_args()


def load_sheet(excel_cfg, symbol: str, interval: str) -> pd.DataFrame:
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


def _ensure_naive(ts: pd.Timestamp) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tzinfo is not None:
        t = t.tz_localize(None)
    return t


def has_coverage(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if df.empty or "timestamp" not in df.columns:
        return False
    min_ts, max_ts = df["timestamp"].min(), df["timestamp"].max()
    start = _ensure_naive(start)
    end = _ensure_naive(end)
    min_ts = _ensure_naive(min_ts)
    max_ts = _ensure_naive(max_ts)
    return pd.notnull(min_ts) and pd.notnull(max_ts) and min_ts <= start and max_ts >= end


def filter_window(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    start = _ensure_naive(start)
    end = _ensure_naive(end)
    ts = df["timestamp"]
    if pd.api.types.is_datetime64tz_dtype(ts):
        ts = ts.dt.tz_localize(None)
    mask = (ts >= start) & (ts <= end)
    return df.loc[mask].reset_index(drop=True)


def compute_volume_stats(df: pd.DataFrame) -> Optional[dict]:
    if df.empty or "volume" not in df.columns:
        return None
    volumes = pd.to_numeric(df["volume"], errors="coerce").dropna()
    if volumes.empty:
        return None
    return {
        "rows": len(volumes),
        "avg_volume": volumes.mean(),
        "p95_volume": volumes.quantile(0.95),
    }


def ensure_data(
    cfg: AppConfig,
    symbol: str,
    interval: str,
    start_local: pd.Timestamp,
    end_local: pd.Timestamp,
    start_utc,
    end_utc,
    logger: logging.Logger,
) -> pd.DataFrame:
    existing = load_sheet(cfg.excel, symbol, interval)
    if has_coverage(existing, start_local, end_local):
        logger.info("Using existing Excel data", extra={"symbol": symbol, "interval": interval})
        return existing

    logger.info("Data not fully available, fetching from Binance", extra={"symbol": symbol, "interval": interval})
    raw = fetch_klines(symbol, interval, start_utc, end_utc, cfg.request, logger)
    fetched = klines_to_dataframe(raw, symbol, interval, cfg.timezone)
    write_dataframe(fetched, cfg.excel, symbol, interval, logger)
    combined = load_sheet(cfg.excel, symbol, interval)
    return combined


def to_data_timezone(dt: pd.Timestamp, tz) -> pd.Timestamp:
    ts = pd.Timestamp(dt)
    if tz:
        if ts.tzinfo is None:
            ts = ts.tz_localize(tz)
        else:
            ts = ts.tz_convert(tz)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts


def resolve_window_with_overrides(
    start: Optional[str],
    end: Optional[str],
    lookback: Optional[str],
    default_lookback: Optional[str],
    input_tz: Optional[str],
) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    start_dt = parse_datetime(start, input_tz) if start else None
    end_dt = parse_datetime(end, input_tz) if end else None
    lookback_expr = lookback if lookback else default_lookback

    if not start_dt and not end_dt:
        if not lookback_expr:
            raise ValueError("Provide start/end or lookback")
        start_time = now - parse_duration(lookback_expr)
        end_time = now
    elif start_dt and not end_dt:
        end_time = now
        start_time = start_dt
    elif end_dt and not start_dt:
        if not lookback_expr:
            raise ValueError("Provide start or lookback with end")
        start_time = end_dt - parse_duration(lookback_expr)
        end_time = end_dt
    else:
        start_time, end_time = start_dt, end_dt

    if start_time > end_time:
        raise ValueError("start_time must be before end_time")
    return start_time, end_time


def run_volume_stats(
    cfg: AppConfig,
    symbol: str,
    interval: str,
    start_override: Optional[str],
    end_override: Optional[str],
    lookback_override: Optional[str],
    input_tz: Optional[str],
    default_lookback: Optional[str],
    logger: logging.Logger,
) -> Optional[dict]:
    start_time, end_time = resolve_window_with_overrides(start_override, end_override, lookback_override, default_lookback, input_tz)
    data_tz = resolve_timezone(cfg.timezone)

    start_local = to_data_timezone(start_time, data_tz)
    end_local = to_data_timezone(end_time, data_tz)

    df = ensure_data(cfg, symbol, interval, start_local, end_local, start_time, end_time, logger)
    window_df = filter_window(df, start_local, end_local)
    stats = compute_volume_stats(window_df)
    if not stats:
        return None
    stats.update(
        {
            "symbol": symbol,
            "interval": interval,
            "start": start_local,
            "end": end_local,
        }
    )
    return stats


def main() -> int:
    args = parse_args()
    load_dotenv()
    config_path = Path(args.config)
    cfg = load_config(config_path)
    logger = build_logger(cfg.logging_level)

    stats = run_volume_stats(
        cfg,
        args.symbol,
        args.interval,
        args.start,
        args.end,
        args.lookback,
        args.time_input_tz,
        default_lookback=None,
        logger=logger,
    )
    if not stats:
        logger.warning("No volume data available for requested window", extra={"symbol": args.symbol, "interval": args.interval})
        return 1

    logger.info(
        "Volume stats",
        extra={
            "symbol": stats["symbol"],
            "interval": stats["interval"],
            "rows": stats["rows"],
            "avg_volume": stats["avg_volume"],
            "p95_volume": stats["p95_volume"],
            "start": stats["start"].isoformat(),
            "end": stats["end"].isoformat(),
        },
    )
    print(f"{stats['symbol']} {stats['interval']} {stats['start']} -> {stats['end']}")
    print(f"rows={stats['rows']}, avg_volume={stats['avg_volume']:.6f}, p95_volume={stats['p95_volume']:.6f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

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
from .config import AppConfig, load_config, parse_datetime, parse_duration
from .csv_writer import read_dataframe, write_dataframe
from .transform import count_missing_rows, klines_to_dataframe


def build_logger(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("ohlcv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Binance OHLCV data into Excel.")
    parser.add_argument("-c", "--config", default="config.yml", help="Path to YAML config file.")
    parser.add_argument("--symbol", required=True, help="Symbol to fetch (e.g., BTCUSDT).")
    parser.add_argument("--interval", required=True, help="Interval (e.g., 1h, 5m).")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and transform without writing Excel.")
    parser.add_argument("--start", help="Start time (ISO 8601, interpreted using --time-input-tz if provided).")
    parser.add_argument("--end", help="End time (ISO 8601).")
    parser.add_argument("--lookback", help="Lookback window (e.g., 7d, 12h) if start/end not provided.")
    parser.add_argument("--time-input-tz", help="Timezone to interpret start/end when no tz offset is present (e.g., Asia/Kolkata).")
    return parser.parse_args()


def resolve_window(start: Optional[str], end: Optional[str], lookback: Optional[str], input_tz: Optional[str]) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    start_dt = parse_datetime(start, input_tz) if start else None
    end_dt = parse_datetime(end, input_tz) if end else None
    lb = lookback
    if not start_dt and not end_dt:
        if not lb:
            raise ValueError("Provide either start/end or lookback")
        start_dt = now - parse_duration(lb)
        end_dt = now
    elif start_dt and not end_dt:
        end_dt = now
    elif end_dt and not start_dt:
        if not lb:
            raise ValueError("Provide start or lookback")
        start_dt = end_dt - parse_duration(lb)

    if start_dt > end_dt:
        raise ValueError("start time must be before end time")
    return start_dt, end_dt


def compute_fetch_ranges(existing: pd.DataFrame, start_time: datetime, end_time: datetime) -> list[tuple[datetime, datetime]]:
    if existing.empty or "timestamp" not in existing.columns:
        return [(start_time, end_time)]
    ts = existing["timestamp"]
    if pd.api.types.is_datetime64tz_dtype(ts):
        ts = ts.dt.tz_localize(None)
    min_ts, max_ts = ts.min(), ts.max()
    if pd.notnull(min_ts) and pd.notnull(max_ts) and min_ts <= start_time and max_ts >= end_time:
        return []
    ranges: list[tuple[datetime, datetime]] = []
    if pd.notnull(min_ts) and start_time < min_ts:
        ranges.append((start_time, min_ts))
    if pd.notnull(max_ts) and end_time > max_ts:
        ranges.append((max_ts, end_time))
    if not ranges:
        ranges.append((start_time, end_time))
    return ranges


def main() -> int:
    args = parse_args()
    load_dotenv()
    config_path = Path(args.config)
    cfg = load_config(config_path)
    logger = build_logger(cfg.logging_level)

    try:
        start_time, end_time = resolve_window(args.start, args.end, args.lookback, args.time_input_tz)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error(f"Invalid time window: {exc}")
        return 1

    logger.info(
        "Running ingest",
        extra={
            "config": str(config_path),
            "symbol": args.symbol,
            "interval": args.interval,
            "start": start_time.isoformat(),
            "end": end_time.isoformat(),
            "dry_run": args.dry_run,
        },
    )

    symbol = args.symbol
    interval = args.interval

    existing = read_dataframe(cfg.excel, symbol, interval)
    fetch_ranges = compute_fetch_ranges(existing, start_time, end_time)
    df_parts = [existing] if not existing.empty else []

    for fetch_start, fetch_end in fetch_ranges:
        try:
            raw = fetch_klines(symbol, interval, fetch_start, fetch_end, cfg.request, logger)
        except Exception as exc:
            logger.error(f"Failed to fetch {symbol} {interval}: {exc}")
            return 1

        df_new = klines_to_dataframe(raw, symbol, interval, cfg.timezone)
        missing = count_missing_rows(df_new, interval)
        logger.info(
            "Fetched klines",
            extra={
                "symbol": symbol,
                "interval": interval,
                "rows": len(df_new),
                "missing": missing,
                "first": df_new["timestamp"].iloc[0].isoformat() if not df_new.empty else None,
                "last": df_new["timestamp"].iloc[-1].isoformat() if not df_new.empty else None,
            },
        )
        df_parts.append(df_new)

    df = pd.concat(df_parts, ignore_index=True) if df_parts else existing
    df = df.drop_duplicates(subset=["timestamp", "symbol", "interval"]).sort_values("timestamp")

    if args.dry_run:
        return 0

    try:
        write_dataframe(df, cfg.excel, symbol, interval, logger)
    except Exception as exc:
        logger.error(f"Failed to write Excel for {symbol} {interval}: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

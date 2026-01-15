from __future__ import annotations

import argparse
import sys
from datetime import timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from .binance_client import fetch_klines
from .config import load_config, resolve_timezone
from .csv_writer import read_dataframe, write_dataframe
from .ingest import build_logger
from .operations import load_operations, OperationSpec
from .stats import run_volume_stats, resolve_window_with_overrides
from .transform import count_missing_rows, klines_to_dataframe


def to_naive_local(dt: pd.Timestamp, data_tz) -> pd.Timestamp:
    ts = pd.Timestamp(dt)
    if data_tz:
        if ts.tzinfo is None:
            ts = ts.tz_localize(timezone.utc)
        ts = ts.tz_convert(data_tz)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts


def convert_local_range_to_utc(start_local: pd.Timestamp, end_local: pd.Timestamp, data_tz) -> tuple[pd.Timestamp, pd.Timestamp]:
    if data_tz:
        s = start_local.tz_localize(data_tz).astimezone(timezone.utc)
        e = end_local.tz_localize(data_tz).astimezone(timezone.utc)
    else:
        s = start_local.replace(tzinfo=timezone.utc)
        e = end_local.replace(tzinfo=timezone.utc)
    return s, e


def normalize_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" not in df.columns:
        return df
    out = df.copy()
    ts = pd.to_datetime(out["timestamp"], errors="coerce")
    if isinstance(ts.dtype, pd.DatetimeTZDtype):
        ts = ts.dt.tz_localize(None)
    out["timestamp"] = ts
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a named operation using predefined parameters.")
    parser.add_argument("--config", default="config.yml", help="Path to base config file.")
    parser.add_argument("--ops", default="operations.yml", help="Path to operations file.")
    parser.add_argument("--operation", required=True, help="Name of the operation to run.")
    return parser.parse_args()


def run_volume(op: OperationSpec, cfg, logger):
    stats = run_volume_stats(
        cfg,
        op.symbol,
        op.interval,
        op.start_time,
        op.end_time,
        op.lookback,
        op.time_input_timezone,
        default_lookback=None,
        logger=logger,
    )
    if not stats:
        logger.warning("No volume data available", extra={"operation": op.name, "symbol": op.symbol, "interval": op.interval})
        return 1
    logger.info(
        "Volume stats",
        extra={
            "operation": op.name,
            "symbol": stats["symbol"],
            "interval": stats["interval"],
            "rows": stats["rows"],
            "avg_volume": stats["avg_volume"],
            "p95_volume": stats["p95_volume"],
            "start": stats["start"].isoformat(),
            "end": stats["end"].isoformat(),
        },
    )
    print(f"{op.name} -> {stats['symbol']} {stats['interval']} {stats['start']} -> {stats['end']}")
    print(f"rows={stats['rows']}, avg_volume={stats['avg_volume']:.6f}, p95_volume={stats['p95_volume']:.6f}")
    return 0


def run_generate_slice(op: OperationSpec, cfg, logger):
    start_time, end_time = resolve_window_with_overrides(
        op.start_time,
        op.end_time,
        op.lookback,
        default_lookback=None,
        input_tz=op.time_input_timezone,
    )
    data_tz = resolve_timezone(cfg.timezone)
    start_local = to_naive_local(start_time, data_tz)
    end_local = to_naive_local(end_time, data_tz)

    existing = normalize_timestamp(read_dataframe(cfg.excel, op.symbol, op.interval))
    ts = existing["timestamp"] if ("timestamp" in existing.columns) else pd.Series(dtype="datetime64[ns]")
    if isinstance(ts.dtype, pd.DatetimeTZDtype):
        ts = ts.dt.tz_localize(None)
    min_ts, max_ts = (ts.min(), ts.max()) if not ts.empty else (None, None)

    fetch_ranges_local = []
    if ts.empty:
        fetch_ranges_local = [(start_local, end_local)]
    else:
        if pd.notnull(min_ts) and start_local < min_ts:
            fetch_ranges_local.append((start_local, min_ts))
        if pd.notnull(max_ts) and end_local > max_ts:
            fetch_ranges_local.append((max_ts, end_local))
        if not fetch_ranges_local and pd.notnull(min_ts) and pd.notnull(max_ts) and min_ts <= start_local and max_ts >= end_local:
            logger.info("Using existing CSV coverage; skipping API fetch", extra={"operation": op.name, "symbol": op.symbol})

    df_parts = [existing] if not existing.empty else []
    for fetch_start_local, fetch_end_local in fetch_ranges_local:
        fetch_start_utc, fetch_end_utc = convert_local_range_to_utc(fetch_start_local, fetch_end_local, data_tz)
        raw = fetch_klines(op.symbol, op.interval, fetch_start_utc, fetch_end_utc, cfg.request, logger)
        df_new = normalize_timestamp(klines_to_dataframe(raw, op.symbol, op.interval, cfg.timezone))
        missing = count_missing_rows(df_new, op.interval)
        logger.info(
            "Fetched klines",
            extra={
                "operation": op.name,
                "symbol": op.symbol,
                "interval": op.interval,
                "rows": len(df_new),
                "missing": missing,
                "first": df_new["timestamp"].iloc[0].isoformat() if not df_new.empty else None,
                "last": df_new["timestamp"].iloc[-1].isoformat() if not df_new.empty else None,
            },
        )
        df_parts.append(df_new)

    combined = normalize_timestamp(pd.concat(df_parts, ignore_index=True) if df_parts else existing)
    combined = combined.drop_duplicates(subset=["timestamp", "symbol", "interval"]).sort_values("timestamp")

    # Update the base CSV with combined data
    base_path = Path(str(cfg.excel.path).format(symbol=op.symbol, interval=op.interval))
    combined.to_csv(base_path, index=False)

    # Write the sliced CSV (overwrite)
    slice_path = (
        Path(op.slice_output_path)
        if op.slice_output_path
        else base_path.with_name(f"{base_path.stem}_sliced{base_path.suffix}")
    )
    slice_df = combined[(combined["timestamp"] >= start_local) & (combined["timestamp"] <= end_local)]
    slice_df.to_csv(slice_path, index=False)
    logger.info(
        "Generated sliced CSV",
        extra={
            "operation": op.name,
            "symbol": op.symbol,
            "interval": op.interval,
            "rows": len(slice_df),
            "path": str(slice_path),
            "start": start_local,
            "end": end_local,
        },
    )
    return 0


def run_fetch(op: OperationSpec, cfg, logger):
    start_time, end_time = resolve_window_with_overrides(
        op.start_time,
        op.end_time,
        op.lookback,
        default_lookback=None,
        input_tz=op.time_input_timezone,
    )
    data_tz = resolve_timezone(cfg.timezone)
    existing = normalize_timestamp(read_dataframe(cfg.excel, op.symbol, op.interval))
    ts = existing["timestamp"] if ("timestamp" in existing.columns) else pd.Series(dtype="datetime64[ns]")
    if isinstance(ts.dtype, pd.DatetimeTZDtype):
        ts = ts.dt.tz_localize(None)
    min_ts, max_ts = (ts.min(), ts.max()) if not ts.empty else (None, None)
    start_local = to_naive_local(start_time, data_tz)
    end_local = to_naive_local(end_time, data_tz)

    fetch_ranges_local = []
    if ts.empty:
        fetch_ranges_local = [(start_local, end_local)]
    else:
        if pd.notnull(min_ts) and start_local < min_ts:
            fetch_ranges_local.append((start_local, min_ts))
        if pd.notnull(max_ts) and end_local > max_ts:
            fetch_ranges_local.append((max_ts, end_local))
        if not fetch_ranges_local and pd.notnull(min_ts) and pd.notnull(max_ts) and min_ts <= start_local and max_ts >= end_local:
            logger.info("Using existing CSV coverage; skipping API fetch", extra={"operation": op.name, "symbol": op.symbol})

    df_parts = [existing] if not existing.empty else []

    for fetch_start_local, fetch_end_local in fetch_ranges_local:
        fetch_start_utc, fetch_end_utc = convert_local_range_to_utc(fetch_start_local, fetch_end_local, data_tz)
        raw = fetch_klines(op.symbol, op.interval, fetch_start_utc, fetch_end_utc, cfg.request, logger)
        df_new = normalize_timestamp(klines_to_dataframe(raw, op.symbol, op.interval, cfg.timezone))
        missing = count_missing_rows(df_new, op.interval)
        logger.info(
            "Fetched klines",
            extra={
                "operation": op.name,
                "symbol": op.symbol,
                "interval": op.interval,
                "rows": len(df_new),
                "missing": missing,
                "first": df_new["timestamp"].iloc[0].isoformat() if not df_new.empty else None,
                "last": df_new["timestamp"].iloc[-1].isoformat() if not df_new.empty else None,
            },
        )
        df_parts.append(df_new)

    df_out = normalize_timestamp(pd.concat(df_parts, ignore_index=True) if df_parts else existing)
    df_out = df_out.drop_duplicates(subset=["timestamp", "symbol", "interval"]).sort_values("timestamp")
    write_dataframe(df_out, cfg.excel, op.symbol, op.interval, logger)
    return 0


def main() -> int:
    args = parse_args()
    load_dotenv()

    cfg = load_config(Path(args.config))
    logger = build_logger(cfg.logging_level)

    ops = load_operations(Path(args.ops))
    if args.operation not in ops:
        available = ", ".join(ops.keys()) if ops else "none"
        logger.error(f"Operation '{args.operation}' not found (available: {available})")
        return 1

    op = ops[args.operation]
    if op.type == "volume_stats":
        return run_volume(op, cfg, logger)
    if op.type == "fetch":
        return run_fetch(op, cfg, logger)
    if op.type == "generate_sliced_csv":
        return run_generate_slice(op, cfg, logger)

    logger.error(f"Unsupported operation type: {op.type}")
    return 1


if __name__ == "__main__":
    sys.exit(main())

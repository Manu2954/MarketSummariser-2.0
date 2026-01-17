from __future__ import annotations

import streamlit as st

from pathlib import Path
import sys
import pandas as pd
from datetime import datetime, date, time

if __package__ in (None, ""):
    # Allow running via `streamlit run app/ui.py` without package context.
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.ingest import build_logger
from app.run_op import run_fetch, run_generate_slice
from app.operations import load_operations
from app.config import load_config
from app.stats import run_volume_stats


def render_op_form(op):
    st.subheader(f"Operation: {op.name}")
    symbol = st.text_input("Symbol", op.symbol)
    interval = st.text_input("Interval", op.interval)

    def parse_iso(dt_str: str | None):
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return dt.date(), dt.timetz()
        except Exception:
            return None

    start_defaults = parse_iso(op.start_time) or (date.today(), time(0, 0))
    end_defaults = parse_iso(op.end_time) or (date.today(), time(0, 0))

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start date", value=start_defaults[0])
        start_hour = st.select_slider("Start hour", options=list(range(24)), value=start_defaults[1].hour)
        start_min = st.select_slider("Start minute", options=list(range(0, 60, 5)), value=start_defaults[1].minute - start_defaults[1].minute % 5)
        lookback = st.text_input("Lookback (e.g., 1d, optional)", op.lookback or "")
    with col2:
        end_date = st.date_input("End date", value=end_defaults[0])
        end_hour = st.select_slider("End hour", options=list(range(24)), value=end_defaults[1].hour)
        end_min = st.select_slider("End minute", options=list(range(0, 60, 5)), value=end_defaults[1].minute - end_defaults[1].minute % 5)
        time_tz = st.text_input("Time input timezone", op.time_input_timezone or "")

    def combine(d: date, hh: int, mm: int) -> str | None:
        return datetime.combine(d, time(hh, mm)).isoformat()

    start_time = combine(start_date, start_hour, start_min)
    end_time = combine(end_date, end_hour, end_min)
    slice_output = st.text_input("Slice output path (only for slice op, optional)", op.slice_output_path or "")
    return {
        "symbol": symbol,
        "interval": interval,
        "start_time": start_time or None,
        "end_time": end_time or None,
        "lookback": lookback or None,
        "time_input_timezone": time_tz or None,
        "slice_output_path": slice_output or None,
    }


def main():
    st.title("OHLCV Ops UI")
    config_path = st.sidebar.text_input("Config path", "config.yml")
    ops_path = st.sidebar.text_input("Operations path", "operations.yml")
    load_button = st.sidebar.button("Load")

    if load_button:
        st.experimental_set_query_params(reload="1")

    try:
        cfg = load_config(Path(config_path))
    except Exception as exc:
        st.error(f"Failed to load config: {exc}")
        return

    try:
        ops = load_operations(Path(ops_path))
    except Exception as exc:
        st.error(f"Failed to load operations: {exc}")
        return

    if not ops:
        st.warning("No operations found.")
        return

    op_names = list(ops.keys())
    chosen = st.selectbox("Select operation", op_names)
    op = ops[chosen]
    form_data = render_op_form(op)
    run = st.button("Run operation")
    if not run:
        return

    # Update op fields with form data
    op.symbol = form_data["symbol"]
    op.interval = form_data["interval"]
    op.start_time = form_data["start_time"]
    op.end_time = form_data["end_time"]
    op.lookback = form_data["lookback"]
    op.time_input_timezone = form_data["time_input_timezone"]
    op.slice_output_path = form_data["slice_output_path"]

    logger = build_logger(cfg.logging_level)
    code = 0

    def show_csv_preview(cfg, symbol: str, interval: str, suffix: str = "", caption: str | None = None):
        csv_path = Path(str(cfg.excel.path).format(symbol=symbol, interval=interval))
        if suffix:
            csv_path = csv_path.with_name(f"{csv_path.stem}{suffix}{csv_path.suffix}")
        if not csv_path.exists():
            st.info(f"No CSV found at {csv_path}")
            return
        df = pd.read_csv(csv_path)
        st.caption(caption or str(csv_path))
        st.dataframe(df.tail(50))

    if op.type == "fetch":
        code = run_fetch(op, cfg, logger)
        if code == 0:
            show_csv_preview(cfg, op.symbol, op.interval, caption="Fetch result (latest CSV)")
    elif op.type == "volume_stats":
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
            code = 1
        else:
            st.success("Volume stats")
            st.json(
                {
                    "symbol": stats["symbol"],
                    "interval": stats["interval"],
                    "rows": stats["rows"],
                    "avg_volume": stats["avg_volume"],
                    "p95_volume": stats["p95_volume"],
                    "start": stats["start"].isoformat(),
                    "end": stats["end"].isoformat(),
                }
            )
    elif op.type == "generate_sliced_csv":
        code = run_generate_slice(op, cfg, logger)
        if code == 0:
            show_csv_preview(cfg, op.symbol, op.interval, caption="Base CSV")
            show_csv_preview(cfg, op.symbol, op.interval, suffix="_sliced", caption="Sliced CSV")
    else:
        st.error(f"Unsupported operation type: {op.type}")
        return

    if code == 0:
        st.success("Operation completed")
    else:
        st.error(f"Operation failed with code {code}")


if __name__ == "__main__":
    main()

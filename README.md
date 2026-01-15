# Binance OHLCV → CSV Pipeline

Python utility to pull OHLCV (candlestick) data from Binance and push it into a CSV file for downstream analysis/automation.

## Setup

-   Create a venv and install deps:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```
-   Environment variables (optional overrides):
    ```bash
    cp .env.example .env
    # edit BINANCE_BASE_URL and BINANCE_KLINES_PATH if you need a non-default endpoint
    ```
-   Copy the sample config and adjust:
    ```bash
    cp config.example.yml config.yml
    # edit csv path template (e.g., ./data/{symbol}_{interval}.csv), request pacing, timezone, logging level
    ```
    -   Config now only holds global settings; per-operation symbol/interval/time windows live in `operations.yml`.
-   For predefined operations (fetch or stats), copy and edit:
    ```bash
    cp operations.example.yml operations.yml
    # add your named operations with symbol, interval, time window, and optional time_input_timezone
    ```
    -   `start_time`/`end_time` in `operations.yml` are interpreted using `time_input_timezone` on that operation (or as-is if omitted).

## Usage

-   Direct fetch (ad-hoc, no operations file): supply symbol/interval/time window via CLI:
    ```bash
    python -m app.ingest --config config.yml --symbol BTCUSDT --interval 1h --lookback 1d
    # or explicit window (times interpreted with --time-input-tz if naive):
    python -m app.ingest --config config.yml --symbol BTCUSDT --interval 1h --start 2024-05-01T00:00:00 --end 2024-05-02T00:00:00 --time-input-tz Asia/Kolkata
    ```

-   Compute average and p95 volume for a time slice; uses CSV data if present, otherwise fetches missing data, writes it, and then calculates:
    ```bash
    python -m app.stats --config config.yml --symbol BTCUSDT --interval 1h --lookback 3d --time-input-tz Asia/Kolkata
    # or explicit window:
    python -m app.stats --config config.yml --symbol BTCUSDT --interval 1h --start 2024-05-01T00:00:00 --end 2024-05-02T00:00:00 --time-input-tz Asia/Kolkata
    ```

-   Run a predefined operation (name from `operations.yml`):
    ```bash
    python -m app.run_op --config config.yml --ops operations.yml --operation fetch_kline_data
    python -m app.run_op --config config.yml --ops operations.yml --operation fetch_volume_stats
    ```
-   Interactive TUI (prompt-toolkit): select an operation and override parameters via prompts:
    ```bash
    python -m app.tui
    ```

## How it works

1.  Load YAML config (CSV output path, request limits, timezone, logging).
2.  Fetch klines via Binance `/api/v3/klines` with pagination and light rate-limit sleeps.
3.  Transform to a standardized dataframe (`timestamp` in your chosen timezone, OHLCV, quote_volume, trades, taker buy volumes, symbol, interval) and dedupe.
4.  Append/replace the CSV file (dedupe by timestamp/symbol/interval), storing timestamps without timezone info for spreadsheet compatibility.

## Notes

-   Time windows are supplied per operation (CLI or `operations.yml`). If both start/end are missing, you must provide a lookback.
-   Gaps: a rough missing-candle count is logged when the interval size is known (m/h/d/w).
-   Output: parent directories for `excel.path` are created automatically; timestamps are stored tz-naive in the chosen data timezone.

## Next steps (customize later)

-   Add spreadsheet-side formulas/macros in your tooling of choice using the stable columns.
-   Extend logging/metrics or add a dry-run preview that prints the top/bottom rows.

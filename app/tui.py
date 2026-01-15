from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path
from typing import Optional

from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator

from .config import load_config
from .ingest import build_logger
from .operations import OperationSpec, load_operations
from .run_op import run_fetch, run_generate_slice, run_volume


def _input_with_default(message: str, default: Optional[str]) -> Optional[str]:
    value = prompt(f"{message} [{default or ''}]: ")
    return value.strip() or default


def _choose_operation(names: list[str]) -> str:
    print("Select an operation:")
    for idx, name in enumerate(names, start=1):
        print(f"  {idx}) {name}")
    while True:
        choice = prompt("Enter number: ", validator=_NumberValidator(len(names)))
        try:
            idx = int(choice)
            if 1 <= idx <= len(names):
                return names[idx - 1]
        except ValueError:
            pass
        print("Invalid choice, try again.")


class _NumberValidator(Validator):
    def __init__(self, max_value: int) -> None:
        self.max_value = max_value

    def validate(self, document) -> None:
        text = document.text.strip()
        if not text.isdigit():
            raise Exception("Enter a number")
        value = int(text)
        if not (1 <= value <= self.max_value):
            raise Exception(f"Enter a number between 1 and {self.max_value}")


def _prompt_overrides(op: OperationSpec) -> OperationSpec:
    symbol = _input_with_default("Symbol", op.symbol)
    interval = _input_with_default("Interval", op.interval)
    start_time = _input_with_default("Start time (ISO, optional)", op.start_time)
    end_time = _input_with_default("End time (ISO, optional)", op.end_time)
    lookback = _input_with_default("Lookback (e.g., 1d, optional)", op.lookback)
    time_tz = _input_with_default("Time input timezone", op.time_input_timezone)
    slice_path = _input_with_default("Slice output path (optional)", op.slice_output_path)
    return replace(
        op,
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        lookback=lookback,
        time_input_timezone=time_tz,
        slice_output_path=slice_path,
    )


def main() -> int:
    config_path = Path(_input_with_default("Config path", "config.yml"))
    ops_path = Path(_input_with_default("Operations path", "operations.yml"))

    try:
        cfg = load_config(config_path)
    except Exception as exc:
        print(f"Failed to load config: {exc}")
        return 1

    try:
        ops = load_operations(ops_path)
    except Exception as exc:
        print(f"Failed to load operations: {exc}")
        return 1

    if not ops:
        print("No operations found.")
        return 1

    op_name = _choose_operation(list(ops.keys()))
    op_spec = _prompt_overrides(ops[op_name])

    logger = build_logger(cfg.logging_level)

    if op_spec.type == "fetch":
        return run_fetch(op_spec, cfg, logger)
    if op_spec.type == "volume_stats":
        return run_volume(op_spec, cfg, logger)
    if op_spec.type == "generate_sliced_csv":
        return run_generate_slice(op_spec, cfg, logger)

    print(f"Unsupported operation type: {op_spec.type}")
    return 1


if __name__ == "__main__":
    sys.exit(main())

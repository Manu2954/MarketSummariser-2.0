from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class OperationSpec:
    name: str
    type: str
    symbol: str
    interval: str
    lookback: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    time_input_timezone: Optional[str] = None
    slice_output_path: Optional[str] = None


def load_operations(path: Path) -> dict[str, OperationSpec]:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    defaults = data.get("defaults") or {}
    items = data.get("operations") or []
    specs: dict[str, OperationSpec] = {}

    def field(item: dict, key: str):
        return item.get(key, defaults.get(key))


    for item in items:
        name = item.get("name")
        if not name:
            raise ValueError("Operation missing name")
        symbol = field(item, "symbol")
        interval = field(item, "interval")
        if not symbol:
            raise ValueError(f"Operation '{name}' missing symbol (and no default provided)")
        if not interval:
            raise ValueError(f"Operation '{name}' missing interval (and no default provided)")

        specs[name] = OperationSpec(
            name=name,
            type=item.get("type"),
            symbol=symbol,
            interval=interval,
            lookback=field(item, "lookback"),
            start_time=field(item, "start_time"),
            end_time=field(item, "end_time"),
            time_input_timezone=field(item, "time_input_timezone"),
            slice_output_path=field(item, "slice_output_path"),
        )
    return specs

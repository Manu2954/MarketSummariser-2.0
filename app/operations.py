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
    items = data.get("operations") or []
    specs: dict[str, OperationSpec] = {}
    for item in items:
        name = item.get("name")
        if not name:
            raise ValueError("Operation missing name")
        specs[name] = OperationSpec(
            name=name,
            type=item.get("type"),
            symbol=item.get("symbol"),
            interval=item.get("interval"),
            lookback=item.get("lookback"),
            start_time=item.get("start_time"),
            end_time=item.get("end_time"),
            time_input_timezone=item.get("time_input_timezone"),
            slice_output_path=item.get("slice_output_path"),
        )
    return specs

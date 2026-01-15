from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import yaml
from zoneinfo import ZoneInfo


@dataclass
class ExcelConfig:
    path: Path
    append: bool = True
    sheet_name: str = "{symbol}_{interval}"


@dataclass
class RequestConfig:
    limit: int = 1000
    rate_limit_sleep: float = 0.2
    timeout: int = 30


@dataclass
class AppConfig:
    excel: ExcelConfig
    request: RequestConfig
    timezone: Optional[str]
    logging_level: str = "INFO"


def parse_duration(expr: str) -> timedelta:
    if not expr:
        raise ValueError("lookback value is empty")
    units = {"m": "minutes", "h": "hours", "d": "days"}
    expr = expr.strip().lower()
    value_part = "".join(ch for ch in expr if ch.isdigit())
    unit_part = "".join(ch for ch in expr if ch.isalpha())
    if not value_part or unit_part not in units:
        raise ValueError(f"Unsupported duration '{expr}'. Use formats like 30m, 12h, 3d.")
    value = int(value_part)
    return timedelta(**{units[unit_part]: value})


def parse_datetime(raw: Optional[datetime | str], input_tz: Optional[str] = None) -> Optional[datetime]:
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        parsed = raw
    else:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))

    # If a preferred input timezone is provided, interpret the value in that zone (ignoring any embedded tz).
    if input_tz:
        parsed = parsed.replace(tzinfo=None).replace(tzinfo=ZoneInfo(input_tz))
    elif parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def load_config(path: Path | str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    excel_cfg = data.get("excel") or {}
    request_cfg = data.get("request") or {}
    return AppConfig(
        excel=ExcelConfig(
            path=Path(excel_cfg.get("path", "./data/ohlcv.xlsx")),
            append=bool(excel_cfg.get("append", True)),
            sheet_name=str(excel_cfg.get("sheet_name", "{symbol}_{interval}")),
        ),
        request=RequestConfig(
            limit=int(request_cfg.get("limit", 1000)),
            rate_limit_sleep=float(request_cfg.get("rate_limit_sleep", 0.2)),
            timeout=int(request_cfg.get("timeout", 30)),
        ),
        timezone=data.get("timezone") or None,
        logging_level=str(data.get("logging_level", "INFO")).upper(),
    )


def resolve_timezone(tz_name: Optional[str]) -> Optional[ZoneInfo]:
    if not tz_name:
        return None
    return ZoneInfo(tz_name)

"""設備時系列v1の点を検証する。外部データベースやArrowは使わない。"""

import json
import math
import re
from datetime import datetime, timezone

IDENTITY = (
    "factory_id",
    "line_id",
    "equipment_id",
    "sensor_id",
    "measurement",
    "unit",
)

COLUMNS = {
    *IDENTITY,
    "timestamp",
    "sequence_no",
    "value",
    "quality_code",
}


def point(text):
    """v1の値の型を検証し、時刻をUTC・マイクロ秒精度へ正規化する。"""
    value = json.loads(text)
    if not isinstance(value, dict) or set(value) != COLUMNS:
        raise ValueError("invalid_fields")
    if not all(
        isinstance(value[key], str)
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._%/-]{0,127}", value[key])
        for key in IDENTITY
    ):
        raise ValueError("invalid_identity")
    if (
        type(value["sequence_no"]) is not int
        or not 0 <= value["sequence_no"] < 2**63
    ):
        raise ValueError("invalid_sequence")
    reading = value["value"]
    if type(reading) not in (int, float) or not math.isfinite(reading):
        raise ValueError("invalid_value")
    if value["quality_code"] not in ("GOOD", "UNCERTAIN", "BAD"):
        raise ValueError("invalid_quality")
    timestamp = value["timestamp"]
    if not isinstance(timestamp, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?"
        r"(?:Z|[+-]\d{2}:\d{2})",
        timestamp,
    ):
        raise ValueError("invalid_timestamp")
    try:
        value["timestamp"] = datetime.fromisoformat(timestamp).astimezone(
            timezone.utc
        )
    except (ValueError, OverflowError):
        raise ValueError("invalid_timestamp") from None
    return value

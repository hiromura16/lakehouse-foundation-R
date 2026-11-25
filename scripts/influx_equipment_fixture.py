"""合成設備点に限った、閉域Influxの送信元と照合。"""

import json
import re
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

from native.sensor_contract import IDENTITY, point

DATABASE = "equipment_fixture"
TABLE = "equipment_probe_v1"
ORIGIN = "http://influxdb-local:8181"
FIELDS = ("sequence_no", "value", "quality_code")


def fixture_lines(moment):
    """合成専用の異なる時刻・品質を、新しい順に用意する。"""
    sample = dict(
        zip(IDENTITY, ("F01", "L01", "M01", "T01", "temp", "C"), strict=True)
    )
    current = moment.replace(microsecond=123456)
    return [
        json.dumps(
            {
                **sample,
                "timestamp": (current - timedelta(minutes=age)).isoformat(),
                "sequence_no": sequence,
                "value": reading,
                "quality_code": quality,
            }
        )
        for age, sequence, reading, quality in (
            (1, 2, 24.5, "GOOD"),
            (2, 1, 22.5, "UNCERTAIN"),
        )
    ]


def project_points(lines):
    """v1検証済みの小さなfixtureを、同一時刻衝突を拒否して投影する。"""
    if not 1 <= len(lines) <= 4:
        raise ValueError("EQUIPMENT_FIXTURE_SIZE")
    seen = set()
    payload = []
    expected = []
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    for line in lines:
        if len(line) > 2048:
            raise ValueError("EQUIPMENT_FIXTURE_SIZE")
        row = point(line)
        key = tuple(row[name] for name in IDENTITY)
        stamp = row.pop("timestamp")
        identity = (*key, stamp)
        if identity in seen:
            raise ValueError("EQUIPMENT_FIXTURE_TIMESTAMP_COLLISION")
        seen.add(identity)
        offset = stamp - epoch
        micros = (
            offset.days * 86400 + offset.seconds
        ) * 1_000_000 + offset.microseconds
        tags = ",".join(f"{name}={row[name]}" for name in IDENTITY)
        fields = (
            f"sequence_no={row['sequence_no']}i,value={float(row['value'])},"
            f'quality_code="{row["quality_code"]}"'
        )
        payload.append(f"{TABLE},{tags} {fields} {micros}")
        expected.append({"time": stamp, **row})
    return "\n".join(payload), sorted(expected, key=lambda row: row["time"])


def normalize_rows(body):
    """SQL結果の時刻・型を狭く確認し、v1の点と同じ表現にする。"""
    rows = json.loads(body)
    if not isinstance(rows, list) or len(rows) > 4:
        raise ValueError("EQUIPMENT_FIXTURE_BAD_ROWS")
    checked = []
    for row in rows:
        stamp = row["time"]
        iso = re.fullmatch(
            r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
            r"(?:\.([0-9]{1,6})(?:0{0,3}))?(?:Z|\+00:00)?",
            stamp,
        )
        if iso is None or set(row) != {*IDENTITY, *FIELDS, "time"}:
            raise ValueError("EQUIPMENT_FIXTURE_BAD_ROW")
        value = {key: item for key, item in row.items() if key != "time"}
        fraction = "." + iso[2] if iso[2] else ""
        value["timestamp"] = iso[1] + fraction + "Z"
        normalized = point(json.dumps(value))
        parsed_time = normalized.pop("timestamp")
        checked.append({"time": parsed_time, **normalized})
    return checked


def write_path():
    """WAL確定を待ち、合成バッチの部分書込みを許可しない。"""
    return "/api/v3/write_lp?" + urlencode(
        {
            "db": DATABASE,
            "precision": "microsecond",
            "accept_partial": "false",
            "no_sync": "false",
        }
    )


def read_path():
    """別DBや任意SQLではなく、固定した設備点だけを照会する。"""
    columns = ", ".join(f'"{field}"' for field in (*IDENTITY, *FIELDS, "time"))
    return "/api/v3/query_sql?" + urlencode(
        {
            "db": DATABASE,
            "format": "json",
            "q": f"SELECT {columns} FROM {TABLE} ORDER BY time LIMIT 5",
        }
    )

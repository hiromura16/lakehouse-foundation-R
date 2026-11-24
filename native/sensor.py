"""受け渡しが完了した設備JSONLを、Icebergの長期時系列データへ変換する。

原本の整合性と取込台帳は共通のファイル取込側が管理する。このモジュールは
計測点の識別、品質、重複排除、時間窓を扱い、結果をMES・ERPと同じ世代に公開する。
"""

import json
import math
import re
from datetime import datetime, timezone
from hashlib import sha256
from urllib.parse import urlsplit

import pyarrow as pa

IDENTITY = (
    "factory_id",
    "line_id",
    "equipment_id",
    "sensor_id",
    "measurement",
    "unit",
)
INPUT = re.compile(r"sensor/[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.jsonl")
COLUMNS = {
    *IDENTITY,
    "timestamp",
    "sequence_no",
    "value",
    "quality_code",
}
RAW_SCHEMA = pa.schema(
    [(name, pa.string()) for name in IDENTITY]
    + [
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("sequence_no", pa.int64()),
        ("value", pa.float64()),
        ("quality_code", pa.string()),
        ("source_hash", pa.string()),
    ]
)
REJECT_SCHEMA = pa.schema(
    [
        ("source_hash", pa.string()),
        ("line_no", pa.int64()),
        ("reason", pa.string()),
        ("original_json", pa.string()),
    ]
)
MAX_FILES = 256
MAX_BYTES = 32 * 1024 * 1024
MAX_ROWS = 100_000
MAX_LINE_BYTES = 65536


def ledger(core, catalog, prefix):
    """非公開の取込台帳から、現在有効な設備原本とその版だけを取得する。"""
    if prefix:
        return []
    table = catalog.load_table("bronze.files")
    from pyiceberg.expressions import StartsWith

    rows = (
        table.scan(
            row_filter=StartsWith("path", "sensor/"), limit=MAX_FILES + 1
        )
        .to_arrow()
        .to_pylist()
    )
    if (
        len(rows) > MAX_FILES
        or sum(int(row["bytes"]) for row in rows) > MAX_BYTES
    ):
        raise ValueError("SENSOR_FILE_LIMIT")
    return sorted(
        (row for row in rows if INPUT.fullmatch(row["path"])),
        key=lambda row: row["path"],
    )


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


def read_objects(core, entries):
    """変更しない原本を読み、バイト数の上限とハッシュを再検査する。"""
    accepted = {}
    conflicts = set()
    rejected = []
    lines = 0
    for entry in entries:
        uri = urlsplit(entry["s3_uri"])
        bucket = core.os.environ["S3_BUCKET_RAW"]
        if uri.scheme != "s3" or uri.netloc != bucket:
            raise ValueError("SENSOR_ARCHIVE_LOCATION")
        with core.S3.get_object(Bucket=bucket, Key=uri.path[1:])[
            "Body"
        ] as stream:
            size = int(entry["bytes"])
            data = stream.read(size + 1)
        if len(data) != size or sha256(data).hexdigest() != entry["sha256"]:
            raise ValueError("SENSOR_ARCHIVE_HASH")
        for line_no, raw in enumerate(data.splitlines(), 1):
            lines += 1
            if lines > MAX_ROWS or len(raw) > MAX_LINE_BYTES:
                raise ValueError("SENSOR_ROW_LIMIT")
            evidence = {
                "source_hash": entry["sha256"],
                "line_no": line_no,
                "original_json": raw.decode("utf-8", errors="replace"),
            }
            try:
                row = point(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeError, OverflowError):
                rejected.append(
                    {**evidence, "reason": "invalid_json_or_number"}
                )
                continue
            except ValueError as error:
                rejected.append({**evidence, "reason": str(error)})
                continue
            key = tuple(
                row[name] for name in (*IDENTITY, "timestamp", "sequence_no")
            )
            if key in conflicts:
                rejected.append(
                    {**evidence, "reason": "conflicting_duplicate"}
                )
            elif key not in accepted:
                accepted[key] = (row, evidence)
            elif row != accepted[key][0]:
                _, original = accepted.pop(key)
                conflicts.add(key)
                rejected.extend(
                    {**detail, "reason": "conflicting_duplicate"}
                    for detail in (original, evidence)
                )
    return (
        [
            {**row, "source_hash": origin["source_hash"]}
            for row, origin in accepted.values()
        ],
        rejected,
    )


def tables(core, db, entries):
    """型付けした履歴、隔離、最新値、1分窓の集計表を返す。"""
    rows, rejected = read_objects(core, entries)
    raw = pa.Table.from_pylist(rows, schema=RAW_SCHEMA)
    group = ", ".join(IDENTITY)
    db.register("sensor_rows", raw)
    latest = db.sql(
        f"""
        SELECT * FROM sensor_rows
        QUALIFY row_number() OVER (
            PARTITION BY {group} ORDER BY timestamp DESC, sequence_no DESC
        ) = 1
        """
    ).to_arrow_table()
    minute = db.sql(
        f"""
        SELECT {group}, date_trunc('minute', timestamp) AS minute,
            count(*) AS sample_count,
            count(*) FILTER (WHERE quality_code = 'GOOD') AS good_count,
            avg(value) FILTER (WHERE quality_code = 'GOOD') AS good_avg,
            min(value) FILTER (WHERE quality_code = 'GOOD') AS good_min,
            max(value) FILTER (WHERE quality_code = 'GOOD') AS good_max
        FROM sensor_rows GROUP BY {group}, minute
        """
    ).to_arrow_table()
    return {
        "silver.sensor_history": raw,
        "quarantine.sensor_events": pa.Table.from_pylist(
            rejected, schema=REJECT_SCHEMA
        ),
        "gold.sensor_latest": latest,
        "gold.sensor_minute": minute,
    }

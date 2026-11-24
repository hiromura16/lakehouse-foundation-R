"""ドライバーや実接続情報を使わず、固定CDCの合成試験データを用意する。"""

import calendar
import copy
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

# 任意パッケージが無いときだけスキップし、導入済みの破損は異常とする。
try:
    import pyarrow as pa
except ModuleNotFoundError as error:
    if (
        error.name != "pyarrow"
        or importlib.util.find_spec("pyarrow") is not None
    ):
        raise
    raise unittest.SkipTest(
        "Optional tests need pyarrow. In a uv virtual environment, run "
        "uv pip sync --require-hashes requirements-dev.lock."
    ) from error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "infra/cdc"))
import config  # noqa: E402
import consumer  # noqa: E402
import fixed_source  # noqa: E402

VALUES = config.settings(
    {
        "CDC_SOURCE_MODE": "fixed-demo",
        "SOURCE_SQLSERVER_HOST": "sql-source.example.invalid",
        "SOURCE_SQLSERVER_PORT": "1433",
        "SOURCE_SQLSERVER_DATABASE": "demo_database",
        "SOURCE_SQLSERVER_USER": "example_reader",
        "SOURCE_SQLSERVER_PASSWORD": "synthetic-reader-value",
        "DEMO_SQLSERVER_INSTALLATION_ID": "a" * 32,
        "DEMO_SQLSERVER_OWNERSHIP_TOKEN": "b" * 64,
    }
)
MAPPING = fixed_source.table_mapping()
TOPICS = config.topic_names(VALUES, MAPPING)
IDENTITY = consumer.source_identity(VALUES, MAPPING)
NANOS = calendar.timegm((2026, 4, 1, 12, 0, 0)) * 1000000000 + 123456700
ORIGINAL_TIME = "2026-02-01T08:00:00.0000001Z"


def record(
    target="erp_products",
    offset=0,
    operation="r",
    sequence=1,
    *,
    name="日本語の製品",
    key=None,
):
    """CDCの型付き項目と、業務上の正本になる元のJSONを表す。"""
    item = MAPPING[target]
    columns = list(item["columns"])
    primary = columns[0]
    row = {column: "value" for column in columns}
    row[primary] = key or target + "-1"
    row["updated_at"] = ORIGINAL_TIME
    if target == "erp_products":
        row.update(
            product_name=name,
            product_family=None,
            unit="",
            standard_cycle_seconds="123456789.123",
        )
    after = {
        **row,
        "updated_at": NANOS,
        "created_at": NANOS,
        "run_id": "012345abcdef",
        "original_values": json.dumps(row, ensure_ascii=False),
    }
    source = {
        "db": VALUES["SOURCE_SQLSERVER_DATABASE"],
        "schema": item["source_schema"],
        "table": item["source_table"],
        "commit_lsn": None if operation == "r" else "00000001:00000001:0001",
        "change_lsn": None
        if operation == "r"
        else (f"00000001:00000001:{sequence:04x}"),
        "event_serial_no": sequence,
        "ts_ms": NANOS // 1000000,
    }
    envelope = {
        "op": operation,
        "before": copy.deepcopy(after) if operation in {"u", "d"} else None,
        "after": None if operation == "d" else after,
        "source": source,
        "transaction": None if operation == "r" else {"id": "example-tx"},
    }
    topic = ".".join(
        (
            fixed_source.FIXED_PREFIX,
            VALUES["SOURCE_SQLSERVER_DATABASE"],
            item["source_schema"],
            item["source_table"],
        )
    )
    return {
        "topic": topic,
        "partition": 0,
        "offset": offset,
        "key": json.dumps({primary: row[primary]}),
        "value": json.dumps(envelope, ensure_ascii=False),
        "ingested_at": "2026-04-01T12:00:01+00:00",
    }


def change_envelope(original, change):
    """合成イベントだけを変更し、Kafka上の識別情報は保つ。"""
    result = dict(original)
    envelope = json.loads(result["value"])
    change(envelope)
    result["value"] = json.dumps(envelope, ensure_ascii=False)
    return result


def complete_history():
    """固定の各表に、オフセット0のスナップショットイベントを用意する。"""
    return [record(target) for target in MAPPING]


class ReplayTable:
    """実SDKを読み込まず、固定世代のIceberg読取をArrowで再現する。"""

    def __init__(self, records, identity):
        self.properties = {"lakehouse.fixed-cdc.identity": identity}
        self.snapshot_id = 42
        self.calls = []
        self.set_records(records)

    def set_records(self, records):
        """合成スナップショットを明示的に差し替える。"""
        self.rows = consumer.raw_rows(records, fixed=True)
        self.pinned = copy.deepcopy(self.rows)

    def current_snapshot(self):
        """変わらない公開スナップショット識別子を返す。"""
        return SimpleNamespace(snapshot_id=self.snapshot_id)

    def scan(self, *, snapshot_id, selected_fields, limit, row_filter=None):
        """指定したスナップショット、列、条件、行数上限を読取に反映する。"""
        self.calls.append((snapshot_id, selected_fields, limit))
        if snapshot_id != self.snapshot_id:
            raise AssertionError("Replay did not use the pinned snapshot.")
        rows = self.pinned
        if row_filter is not None:
            field, value = row_filter
            rows = [row for row in rows if row[field] == value]
        rows = [
            {name: row[name] for name in selected_fields}
            for row in rows[:limit]
        ]
        schema = pa.schema([(name, pa.string()) for name in selected_fields])
        return SimpleNamespace(
            to_arrow=lambda: pa.Table.from_pylist(rows, schema=schema)
        )


class ReplayCatalog:
    """再生試験には隔離した固定CDCのRaw表だけを見せる。"""

    def __init__(self, table):
        self.table = table

    def table_exists(self, name):
        """汎用CDCのRaw名前空間は試験対象から除外する。"""
        return name == "sqlcdc_events.events"

    def load_table(self, name):
        """別の表への意図しない読取・書込を拒否する。"""
        if name != "sqlcdc_events.events":
            raise AssertionError("Unexpected replay table.")
        return self.table


def partition(topic, number, offset=None):
    """Kafkaクライアントを使わず、必要なTopicPartitionの属性を表す。"""
    return SimpleNamespace(topic=topic, partition=number, offset=offset)


class Broker:
    """ネットワークに接続せず、メインスレッドの配送操作を観測する。"""

    def __init__(self, records):
        self.bounds = {}
        for row in records:
            key = row["topic"], row["partition"]
            self.bounds[key] = (
                0,
                max(self.bounds.get(key, (0, 0))[1], row["offset"] + 1),
            )
        self.calls = []
        self.fail_commit = False

    def get_watermark_offsets(self, item, timeout):
        """明示的に設定した試験ブローカーのオフセット範囲を返す。"""
        del timeout
        return self.bounds[(item.topic, item.partition)]

    def assign(self, items):
        """イベントを読まずに、割り当てられた位置を記録する。"""
        self.calls.append(
            (
                "assign",
                [(item.topic, item.partition, item.offset) for item in items],
            )
        )

    def pause(self, items):
        """保守開始前のメインスレッドによる一時停止を記録する。"""
        self.calls.append(("pause", len(items)))

    def resume(self, items):
        """ローカル公開とブローカーへの配送確認の後に再開を記録する。"""
        self.calls.append(("resume", len(items)))

    def seek(self, item):
        """ローカル状態から選んだ正確な次の配送位置を記録する。"""
        self.calls.append(("seek", item.topic, item.offset))

    def commit(self, *, offsets, asynchronous):
        """配送確認の失敗と、永続化済みのローカル確定を区別する。"""
        if asynchronous:
            raise AssertionError("CDC must use synchronous acknowledgements.")
        self.calls.append(
            (
                "commit",
                [
                    (item.topic, item.partition, item.offset)
                    for item in offsets
                ],
            )
        )
        if self.fail_commit:
            raise RuntimeError("synthetic-private-broker-detail")

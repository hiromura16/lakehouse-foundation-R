"""メモリ上のカタログモデルで、バッチの順序と失敗からの復旧を試す。

アプリの判断とコミット順序を検証するモデルである。
Icebergの並行処理、SQL Server、Trinoの実行、実際のS3は対象外とする。
"""

import copy
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from native import sql_status
from scripts.sql_batch_worker import SqlBatchError

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "sql_batch_worker_subject", ROOT / "scripts/sql_batch_worker.py"
)
WORKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKER)
CONTRACTS = json.loads((ROOT / "medallion/sources.json").read_text())[
    "sources"
]
OBSERVED = "2026-04-01T12:00:00.0000001Z"
LATER = "2026-04-01T12:01:00.0000001Z"
LAST = "2026-04-01T12:02:00.0000001Z"
SOURCE_UPDATED = "2026-04-01T11:00:00.7654321Z"


class Arrow:
    """カタログの判断試験で、NULLを含むPythonの行をそのまま保持する。"""

    def __init__(self, rows):
        self.rows = copy.deepcopy(rows)

    def to_pylist(self):
        """実体化したArrow表と同様に、互いに独立した行を返す。"""
        return copy.deepcopy(self.rows)

    @property
    def num_rows(self):
        """行数だけを公開し、エンジンが検証したようには扱わない。"""
        return len(self.rows)


class Schema:
    """宣言した列名を表す。実行環境では実際のArrow型を使う。"""

    def __init__(self, fields=()):
        self.fields = list(fields)

    def as_arrow(self):
        """模擬Arrowのコンストラクターに、この簡易スキーマを渡す。"""
        return self


class Table:
    """コミットを個別に記録し、途中失敗を確認できるようにする。"""

    def __init__(self, name, schema, events):
        self.name = name
        self.declared = schema
        self.events = events
        self.rows = []
        self.fail_overwrite = False
        self.fail_append = False

    def schema(self):
        """宣言された模擬スキーマを返す。"""
        return self.declared

    def scan(self, *, limit=None):
        """スキャン上限を守り、毎回同じ見え方の結果を返す。"""
        rows = self.rows if limit is None else self.rows[:limit]
        return SimpleNamespace(to_arrow=lambda: Arrow(rows))

    def overwrite(self, arrow):
        """1表だけを独立して確定するか、その確定前に失敗する。"""
        self.events.append("overwrite:" + self.name)
        if self.fail_overwrite:
            raise RuntimeError("synthetic-private-storage-error")
        self.rows = arrow.to_pylist()

    def append(self, arrow):
        """既存レコードを変更せず、メタデータのコミットを1件追記する。"""
        self.events.append("append:" + self.name)
        if self.fail_append:
            raise RuntimeError("synthetic-private-ledger-error")
        self.rows += arrow.to_pylist()


class Catalog:
    """本体のローダーで実際に利用するカタログ操作だけを備える。"""

    def __init__(self, events):
        self.namespaces = set()
        self.tables = {}
        self.events = events

    def namespace_exists(self, name):
        """模擬の名前空間集合を確認する。"""
        return name in self.namespaces

    def create_namespace(self, name):
        """名前空間の作成を変更として記録する。"""
        self.events.append("namespace:" + name)
        self.namespaces.add(name)

    def table_exists(self, name):
        """管理対象の模擬テーブルを確認する。"""
        return name in self.tables

    def create_table(self, name, *, schema, properties):
        """実際の呼出元が宣言した項目で空表を作る。"""
        if properties != {"format-version": "2"}:
            raise AssertionError("Unexpected table version")
        self.events.append("table:" + name)
        self.tables[name] = Table(name, schema, self.events)

    def load_table(self, name):
        """アプリが未宣言の表を読もうとしたら失敗する。"""
        return self.tables[name]


def document(keys=("K1",)):
    """6件の契約を作り、再照合の試験に使う商品を変更する。"""
    tables = {}
    for contract in CONTRACTS:
        name = contract["table"]
        rows = []
        for key in keys if name == "erp_products" else ("K1",):
            values = {column: "value" for column in contract["columns"]}
            values[contract["columns"][0]] = key
            values["updated_at"] = "2026-01-05T08:00:00.0000001Z"
            if name == "erp_products":
                values.update(
                    product_name="日本語製品",
                    product_family=None,
                    unit="",
                    standard_cycle_seconds="25.123",
                )
            rows.append(
                {
                    "values": values,
                    "source_pk": key,
                    "source_updated_at": SOURCE_UPDATED,
                    "run_id": "012345abcdef",
                }
            )
        tables[name] = {
            "schema": "lh_demo_erp"
            if name.startswith("erp_")
            else ("lh_demo_mes"),
            "table": name.split("_", 1)[1],
            "primary_key": contract["columns"][0],
            "rows": rows,
        }
    return {
        "format_version": 1,
        "source_system": "sqlserver-fixed-demo",
        "source_identity": "a" * 64,
        "source_database": "demo_database",
        "mode": "snapshot",
        "tables": tables,
    }


def incremental_document(baseline, *, keys=("K1",), observed=LATER):
    """既知の完了済みスナップショットから重複期間の契約を作る。"""
    body = document(keys)
    body.update(
        mode="incremental",
        before_watermarks={
            source["table"]: {
                "timestamp": SOURCE_UPDATED,
                "primary_key": "K1",
                "snapshot_id": baseline,
            }
            for source in CONTRACTS
        },
        overlap_seconds=300,
        safety_lag_seconds=5,
        upper_bound=WORKER.subtract_seconds(observed, 5),
    )
    return body


class SnapshotWorkerTests(unittest.TestCase):
    """ソース識別、完了、同じ結果を返す再実行を試す。"""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="sql-worker-test-")
        self.addCleanup(temporary.cleanup)
        self.landing = Path(temporary.name)
        self.events = []
        self.catalog = Catalog(self.events)
        arrow = SimpleNamespace(
            schema=Schema,
            string=lambda: "string",
            Table=SimpleNamespace(
                from_pylist=lambda rows, schema: Arrow(rows)
            ),
        )

        def ensure(prefix):
            """隔離したSQL Bronzeスキーマだけを準備する。"""
            self.assertEqual(prefix, "sql_")
            for source in CONTRACTS:
                name = "sql_bronze." + source["table"]
                if not self.catalog.table_exists(name):
                    fields = source["columns"] + [
                        "_source_file",
                        "_source_hash",
                        "_ingested_at",
                        *WORKER.SQL_SOURCE_COLUMNS,
                    ]
                    self.catalog.create_table(
                        name,
                        schema=Schema(fields),
                        properties={"format-version": "2"},
                    )

        def medallion(prefix):
            """SQLを実行せず、派生処理段階への到達を記録する。"""
            self.assertEqual(prefix, "sql_")
            self.events.append("medallion")
            return {"sql_statements": 14}

        self.admin = SimpleNamespace(
            pa=arrow,
            CONTRACT=CONTRACTS,
            catalog=lambda: self.catalog,
            ensure_tables=ensure,
            medallion=mock.Mock(side_effect=medallion),
            S3=SimpleNamespace(
                put_object=mock.Mock(
                    side_effect=lambda **_: self.events.append("raw")
                )
            ),
            os=SimpleNamespace(environ={"S3_BUCKET_RAW": "synthetic-raw"}),
        )

    def publish(self, body):
        """APIの受け渡し契約に従い、合成バイト列を公開する。"""
        payload = json.dumps(
            body, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        batch_id = sha256(payload).hexdigest()
        directory = self.landing / "sql-batches"
        directory.mkdir(exist_ok=True)
        target = directory / (batch_id + ".json")
        target.write_bytes(payload)
        target.with_name(target.name + ".ready").write_text(batch_id + "\n")
        return batch_id

    def load(self, body=None, observed=OBSERVED):
        """宣言したメモリ内カタログで、本体のローダーを実行する。"""
        return WORKER.load(
            self.admin,
            self.publish(body or document()),
            observed,
            landing=self.landing,
        )

    def test_raw_then_all_tables_then_derived_then_completion(self):
        result = self.load()
        writes = [
            event
            for event in self.events
            if event.startswith("overwrite:sql_bronze.")
        ]
        self.assertEqual(len(writes), 6)
        self.assertEqual(self.events[0], "raw")
        self.assertGreater(
            self.events.index(writes[0]),
            self.events.index("append:sql_control.identity"),
        )
        self.assertGreater(
            self.events.index("medallion"), self.events.index(writes[-1])
        )
        self.assertLess(
            self.events.index("overwrite:sql_control.progress"),
            self.events.index(writes[0]),
        )
        self.assertEqual(
            self.events[-2:],
            [
                "append:sql_control.batches",
                "overwrite:sql_control.progress",
            ],
        )
        self.assertTrue(result["completed"])
        self.assertFalse(result["skipped"])
        self.assertEqual(
            result["apply_id"],
            WORKER.apply_identifier(result["batch_id"], OBSERVED),
        )
        archived = self.admin.S3.put_object.call_args.kwargs["Body"]
        source = self.landing / "sql-batches" / (result["batch_id"] + ".json")
        self.assertEqual(archived, source.read_bytes())
        self.assertEqual(sha256(archived).hexdigest(), result["batch_id"])
        self.assertNotIn("source_database", result)
        row = self.catalog.tables["sql_bronze.erp_products"].rows[0]
        self.assertEqual(row["product_name"], "日本語製品")
        self.assertIsNone(row["product_family"])
        self.assertEqual(row["unit"], "")
        self.assertEqual(row["_source_updated_at"], SOURCE_UPDATED)
        self.assertEqual(row["_source_schema"], "lh_demo_erp")
        self.assertEqual(row["_run_id"], "012345abcdef")
        self.assertEqual(
            set(result["counts"]), {source["table"] for source in CONTRACTS}
        )

    def test_same_observation_replay_skips_without_mutations(self):
        first = self.load()
        before = list(self.events)
        rows = copy.deepcopy(
            self.catalog.tables["sql_bronze.erp_products"].rows
        )
        second = self.load()
        self.assertTrue(second["skipped"])
        self.assertEqual(first["apply_id"], second["apply_id"])
        self.assertEqual(self.events, before)
        self.assertEqual(
            self.catalog.tables["sql_bronze.erp_products"].rows, rows
        )

    def test_new_observation_of_unchanged_current_data_skips_business_rewrite(
        self,
    ):
        first = self.load()
        before = sum(
            event.startswith("overwrite:sql_bronze.") for event in self.events
        )
        second = self.load(observed=LATER)
        self.assertEqual(first["batch_id"], second["batch_id"])
        self.assertNotEqual(first["apply_id"], second["apply_id"])
        self.assertTrue(second["skipped"])
        self.assertEqual(
            sum(
                event.startswith("overwrite:sql_bronze.")
                for event in self.events
            ),
            before,
        )
        self.assertEqual(
            len(self.catalog.tables["sql_control.batches"].rows), 2
        )

    def test_full_reconciliation_handles_a_to_b_to_a_after_physical_delete(
        self,
    ):
        first = self.load(document(("K1",)), OBSERVED)
        self.load(document(("K1", "K2")), LATER)
        final = self.load(document(("K1",)), LAST)
        self.assertEqual(first["batch_id"], final["batch_id"])
        self.assertNotEqual(first["apply_id"], final["apply_id"])
        self.assertFalse(final["skipped"])
        rows = self.catalog.tables["sql_bronze.erp_products"].rows
        self.assertEqual([row["product_id"] for row in rows], ["K1"])

    def test_unknown_stale_or_same_time_conflicting_observation_is_rejected(
        self,
    ):
        self.load(document(("K1", "K2")), LATER)
        before = list(self.events)
        for stamp in (OBSERVED, LATER):
            with self.subTest(stamp=stamp):
                with self.assertRaises(WORKER.SqlBatchError):
                    self.load(document(("K1",)), stamp)
                self.assertEqual(self.events, before)

    def test_completed_old_observation_cannot_revert_newer_state(self):
        self.load(document(("K1",)), OBSERVED)
        self.load(document(("K1", "K2")), LATER)
        before = list(self.events)
        result = self.load(document(("K1",)), OBSERVED)
        self.assertTrue(result["skipped"])
        self.assertEqual(self.events, before)
        self.assertEqual(
            len(self.catalog.tables["sql_bronze.erp_products"].rows), 2
        )

    def test_partial_table_failure_leaves_no_completion_and_replay_converges(
        self,
    ):
        self.admin.ensure_tables("sql_")
        target = self.catalog.tables["sql_bronze.erp_shipments"]
        target.fail_overwrite = True
        with self.assertRaises(RuntimeError):
            self.load()
        self.assertEqual(self.catalog.tables["sql_control.batches"].rows, [])
        self.assertEqual(
            len(self.catalog.tables["sql_bronze.erp_products"].rows), 1
        )
        target.fail_overwrite = False
        result = self.load()
        self.assertTrue(result["completed"])
        self.assertEqual(
            len(self.catalog.tables["sql_control.batches"].rows), 1
        )
        self.assertTrue(
            all(
                len(self.catalog.tables["sql_bronze." + source["table"]].rows)
                == 1
                for source in CONTRACTS
            )
        )

    def test_derived_failure_leaves_no_completion_then_retry_succeeds(self):
        self.admin.medallion.side_effect = RuntimeError("private-query-detail")
        with self.assertRaises(RuntimeError):
            self.load()
        self.assertEqual(self.catalog.tables["sql_control.batches"].rows, [])
        self.admin.medallion.side_effect = lambda _: {"sql_statements": 14}
        self.assertTrue(self.load()["completed"])

    def test_ledger_failure_leaves_written_tables_unacknowledged(self):
        WORKER.create_control(self.admin, self.catalog, "a" * 64)
        ledger = self.catalog.tables["sql_control.batches"]
        ledger.fail_append = True
        with self.assertRaises(RuntimeError):
            self.load()
        self.assertEqual(ledger.rows, [])
        ledger.fail_append = False
        result = self.load()
        self.assertTrue(result["completed"])
        self.assertEqual(len(ledger.rows), 1)

    def test_status_reads_published_gold_even_if_completion_write_failed(self):
        """先に確定したGoldと、処理中の反映、直前の完了を同時に返す。"""
        releases = []
        engine = SimpleNamespace(
            GOLD={"test_total": (), "test_empty": ()},
            latest_release=mock.Mock(
                side_effect=lambda *_args, **_kwargs: (
                    releases[-1] if releases else None
                )
            ),
        )
        self.assertEqual(
            sql_status.collect(self.admin, engine),
            {"last_completed": None, "apply": None, "published": None},
        )

        def publish(prefix):
            """完了台帳の追記より先に、公開世代だけを切り替える。"""
            self.assertEqual(prefix, "sql_")
            releases.append(
                {
                    "release_snapshot_id": str(len(releases) + 1),
                    "released_at": "2026-04-01T12:05:00+00:00",
                    "row_counts": {
                        "gold.test_total": 1,
                        "gold.test_empty": 0,
                        "gold.private.customer.invalid": 42,
                        "silver.private_path": 1,
                    },
                    "source_identity": "private-gold-value",
                    "outputs": {"gold.test_total": "s3://private.invalid/x"},
                }
            )
            return {"sql_statements": 14}

        self.admin.medallion.side_effect = publish
        first = self.load()
        self.load(observed=LATER)
        initial = sql_status.collect(self.admin, engine)
        self.assertEqual(initial["apply"]["status"], "idle")
        self.assertEqual(initial["published"]["release_snapshot_id"], "1")
        self.assertEqual(initial["last_completed"]["observed_at"], LATER)
        self.assertEqual(initial["last_completed"]["sql_statements"], 0)
        self.assertEqual(
            initial["last_completed"]["changed_counts"]["erp_products"], 0
        )

        ledger = self.catalog.tables["sql_control.batches"]
        ledger.fail_append = True
        with self.assertRaises(RuntimeError):
            self.load(document(("K1", "K2")), LAST)
        events_before = list(self.events)
        observed = sql_status.collect(self.admin, engine)
        self.assertEqual(self.events, events_before)
        self.assertEqual(observed["last_completed"], initial["last_completed"])
        self.assertEqual(observed["apply"]["status"], "applying")
        self.assertEqual(observed["apply"]["observed_at"], LAST)
        self.assertNotEqual(observed["apply"]["batch_id"], first["batch_id"])
        self.assertEqual(observed["published"]["release_snapshot_id"], "2")
        self.assertEqual(
            observed["published"]["gold_rows"],
            {"test_empty": 0, "test_total": 1},
        )
        self.assertEqual(set(observed["apply"]), set(sql_status.APPLY_FIELDS))
        self.assertNotIn("private", json.dumps(observed))
        self.assertNotIn("source_identity", json.dumps(observed))
        engine.latest_release.assert_called_with(
            self.admin, self.catalog, prefix="sql_"
        )

    def test_sql_status_keeps_legacy_counts_unobserved(self):
        """旧台帳の計測していない値は0件と区別する。"""
        self.load()
        row = self.catalog.tables["sql_control.batches"].rows[0]
        row["counts_json"] = json.dumps(
            json.loads(row["counts_json"])["selected"]
        )
        engine = SimpleNamespace(
            GOLD={},
            latest_release=mock.Mock(
                return_value={
                    "release_snapshot_id": "42",
                    "released_at": "2026-04-01T12:05:00+00:00",
                }
            ),
        )
        state = sql_status.collect(self.admin, engine)
        self.assertIsNone(state["last_completed"]["changed_counts"])
        self.assertIsNone(state["last_completed"]["current_counts"])
        self.assertIsNone(state["published"]["gold_rows"])

    def test_sql_status_refuses_history_over_the_existing_bound(self):
        """読取専用の診断も、処理本体の完了台帳の走査上限を維持する。"""
        self.load()
        ledger = self.catalog.tables["sql_control.batches"]
        size = WORKER.MAX_HISTORY + 1
        rows = SimpleNamespace(to_pylist=lambda: range(size))
        ledger.scan = mock.Mock(
            return_value=SimpleNamespace(to_arrow=lambda: rows)
        )
        engine = SimpleNamespace(latest_release=mock.Mock())
        with self.assertRaisesRegex(SqlBatchError, "row limit"):
            sql_status.collect(self.admin, engine)
        ledger.scan.assert_called_once_with(limit=size)
        engine.latest_release.assert_not_called()

    def test_sql_status_cli_reports_only_fixed_diagnostic_on_failure(self):
        """接続失敗の例外に含まれる内部URIはCLIに出力されない。"""
        storage = SimpleNamespace(
            catalog=mock.Mock(
                side_effect=RuntimeError(
                    "https://private.invalid/user@example.invalid"
                )
            )
        )
        stdout, stderr = io.StringIO(), io.StringIO()
        modules = {
            "native.storage": storage,
            "native.engine": SimpleNamespace(),
        }
        with (
            mock.patch.dict(sys.modules, modules),
            mock.patch("native.storage", storage, create=True),
            mock.patch("native.engine", modules["native.engine"], create=True),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            self.assertEqual(sql_status.main([]), 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "SQL_STATUS_UNAVAILABLE\n")

    def test_committed_bronze_count_mismatch_prevents_completion(self):
        def corrupt(_):
            self.catalog.tables["sql_bronze.erp_products"].rows.clear()
            return {"sql_statements": 14}

        self.admin.medallion.side_effect = corrupt
        with self.assertRaisesRegex(WORKER.SqlBatchError, "counts differ"):
            self.load()
        self.assertEqual(self.catalog.tables["sql_control.batches"].rows, [])

    def test_partial_first_apply_still_binds_the_source_identity(self):
        self.admin.medallion.side_effect = RuntimeError("synthetic-failure")
        with self.assertRaises(RuntimeError):
            self.load()
        alternate = document()
        alternate["source_identity"] = "b" * 64
        before = list(self.events)
        with self.assertRaisesRegex(WORKER.SqlBatchError, "different source"):
            self.load(alternate, LATER)
        self.assertEqual(self.events, before)

    def test_fresh_a_repairs_partial_b_even_if_a_is_last_completed_content(
        self,
    ):
        self.load(document(("K1",)), OBSERVED)
        broken = self.catalog.tables["sql_bronze.erp_work_orders"]
        broken.fail_overwrite = True
        with self.assertRaises(RuntimeError):
            self.load(document(("K1", "K2")), LATER)
        self.assertEqual(
            len(self.catalog.tables["sql_bronze.erp_products"].rows), 2
        )
        pending = self.catalog.tables["sql_control.progress"].rows[0]
        self.assertEqual(pending["status"], "applying")
        broken.fail_overwrite = False
        before = sum(
            event.startswith("overwrite:sql_bronze.") for event in self.events
        )
        result = self.load(document(("K1",)), LAST)
        self.assertFalse(result["skipped"])
        self.assertEqual(
            sum(
                event.startswith("overwrite:sql_bronze.")
                for event in self.events
            )
            - before,
            6,
        )
        self.assertEqual(
            [
                row["product_id"]
                for row in self.catalog.tables["sql_bronze.erp_products"].rows
            ],
            ["K1"],
        )
        self.assertEqual(
            self.catalog.tables["sql_control.progress"].rows[0]["status"],
            "idle",
        )

    def test_pending_b_rejects_unseen_older_a_and_completed_a_ack(self):
        self.load(document(("K1",)), OBSERVED)
        broken = self.catalog.tables["sql_bronze.erp_work_orders"]
        broken.fail_overwrite = True
        with self.assertRaises(RuntimeError):
            self.load(document(("K1", "K2")), LATER)
        before = list(self.events)
        for stamp in (OBSERVED, "2026-04-01T12:00:30.0000001Z"):
            with self.subTest(observation=stamp):
                with self.assertRaises(WORKER.SqlBatchError):
                    self.load(document(("K1",)), stamp)
                self.assertEqual(self.events, before)

    def test_same_pending_b_observation_replays_to_complete_state(self):
        self.load(document(("K1",)), OBSERVED)
        broken = self.catalog.tables["sql_bronze.erp_work_orders"]
        broken.fail_overwrite = True
        with self.assertRaises(RuntimeError):
            self.load(document(("K1", "K2")), LATER)
        broken.fail_overwrite = False
        result = self.load(document(("K1", "K2")), LATER)
        self.assertTrue(result["completed"])
        self.assertEqual(
            len(self.catalog.tables["sql_bronze.erp_products"].rows), 2
        )
        self.assertEqual(
            self.catalog.tables["sql_control.progress"].rows[0]["status"],
            "idle",
        )

    def test_clear_failure_keeps_pending_and_retry_clears_completed_apply(
        self,
    ):
        original = WORKER.set_progress

        def fail_clear(admin, catalog, observation, status):
            if status == "idle":
                raise RuntimeError("synthetic-clear-failure")
            original(admin, catalog, observation, status)

        with mock.patch.object(WORKER, "set_progress", side_effect=fail_clear):
            with self.assertRaises(RuntimeError):
                self.load()
        self.assertEqual(
            len(self.catalog.tables["sql_control.batches"].rows), 1
        )
        self.assertEqual(
            self.catalog.tables["sql_control.progress"].rows[0]["status"],
            "applying",
        )
        before = list(self.events)
        result = self.load()
        self.assertTrue(result["completed"])
        self.assertTrue(result["skipped"])
        start = len(before)
        self.assertEqual(
            self.events[start:], ["overwrite:sql_control.progress"]
        )
        self.assertEqual(
            len(self.catalog.tables["sql_control.batches"].rows), 1
        )

    def test_pending_marker_write_failure_prevents_business_changes(self):
        WORKER.create_control(self.admin, self.catalog, "a" * 64)
        self.catalog.tables["sql_control.progress"].fail_overwrite = True
        with self.assertRaises(RuntimeError):
            self.load()
        self.assertFalse(
            any(
                event.startswith("overwrite:sql_bronze.")
                for event in self.events
            )
        )
        self.assertEqual(self.catalog.tables["sql_control.batches"].rows, [])

    def test_completed_data_rejects_missing_or_empty_control_metadata(self):
        self.load()
        for name in ("sql_control.identity", "sql_control.progress"):
            table = self.catalog.tables[name]
            saved_rows = copy.deepcopy(table.rows)
            for lost in ("table", "row"):
                with self.subTest(metadata=name, lost=lost):
                    if lost == "table":
                        del self.catalog.tables[name]
                    else:
                        table.rows = []
                    before = list(self.events)
                    try:
                        with self.assertRaises(WORKER.SqlBatchError):
                            self.load(observed=LATER)
                        self.assertEqual(self.events, before)
                    finally:
                        self.catalog.tables[name] = table
                        table.rows = copy.deepcopy(saved_rows)

    def test_partial_first_apply_rejects_a_lost_progress_marker(self):
        self.admin.medallion.side_effect = RuntimeError("synthetic-failure")
        with self.assertRaises(RuntimeError):
            self.load()
        self.catalog.tables["sql_control.progress"].rows.clear()
        before = list(self.events)
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(observed=LATER)
        self.assertEqual(self.events, before)

    def test_empty_first_creation_tables_can_resume_without_a_progress_row(
        self,
    ):
        WORKER.create_control(self.admin, self.catalog, "a" * 64)
        self.catalog.tables["sql_control.identity"].rows.clear()
        del self.catalog.tables["sql_control.progress"]
        self.assertTrue(self.load()["completed"])

    def test_other_source_cannot_replace_completed_or_partial_owned_data(self):
        self.load()
        alternate = document()
        alternate["source_identity"] = "b" * 64
        before = list(self.events)
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(alternate, LATER)
        self.assertEqual(self.events, before)

    def test_invalid_six_table_contract_fails_before_any_mutation(self):
        cases = []
        missing = document()
        missing["tables"].pop("mes_events")
        cases.append(missing)
        rogue = document()
        rogue["tables"]["erp_products"]["schema"] = "dbo"
        cases.append(rogue)
        duplicate = document()
        product = duplicate["tables"]["erp_products"]
        product["rows"].append(copy.deepcopy(product["rows"][0]))
        cases.append(duplicate)
        changed = document()
        changed["tables"]["erp_products"]["rows"][0]["values"]["extra"] = "x"
        cases.append(changed)
        for body in cases:
            with self.subTest(case="invalid source contract"):
                with self.assertRaises(WORKER.SqlBatchError):
                    self.load(body)
                self.assertEqual(self.events, [])

    def test_ready_hash_size_and_symlink_checks_precede_catalog_mutation(self):
        batch_id = self.publish(document())
        path = self.landing / "sql-batches" / (batch_id + ".json")
        marker = path.with_name(path.name + ".ready")
        marker.write_text("b" * 64 + "\n")
        with self.assertRaises(WORKER.SqlBatchError):
            WORKER.load(self.admin, batch_id, OBSERVED, landing=self.landing)
        marker.write_text(batch_id + "\n")
        path.write_bytes(path.read_bytes() + b" ")
        with self.assertRaises(WORKER.SqlBatchError):
            WORKER.load(self.admin, batch_id, OBSERVED, landing=self.landing)
        self.assertEqual(self.events, [])
        path.unlink()
        path.symlink_to(marker)
        with self.assertRaises(WORKER.SqlBatchError):
            WORKER.load(self.admin, batch_id, OBSERVED, landing=self.landing)
        self.assertEqual(self.events, [])

    def test_unclaimed_existing_business_rows_are_not_adopted(self):
        self.admin.ensure_tables("sql_")
        self.catalog.tables["sql_bronze.erp_products"].rows = [
            {"legacy": True}
        ]
        with self.assertRaisesRegex(WORKER.SqlBatchError, "Unclaimed"):
            self.load()
        self.assertFalse(self.catalog.table_exists("sql_control.identity"))
        self.admin.S3.put_object.assert_not_called()

    def test_future_source_revisions_fail_without_s3_or_catalog_writes(self):
        body = document()
        body["tables"]["erp_products"]["rows"][0]["source_updated_at"] = LAST
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(body)
        self.assertEqual(self.events, [])

    def test_incremental_merges_equal_timestamp_keys_and_replay_changes_zero(
        self,
    ):
        first = self.load()
        original = copy.deepcopy(
            self.catalog.tables["sql_bronze.erp_products"].rows[0]
        )
        body = incremental_document(first["batch_id"], keys=("K1", "K2"))
        result = self.load(body, LATER)
        self.assertEqual(result["counts"]["erp_products"], 2)
        self.assertEqual(result["changed_counts"]["erp_products"], 1)
        self.assertEqual(result["current_counts"]["erp_products"], 2)
        rows = self.catalog.tables["sql_bronze.erp_products"].rows
        self.assertEqual(rows[0], original)
        before = list(self.events)
        replay = self.load(body, LATER)
        self.assertTrue(replay["skipped"])
        self.assertTrue(
            all(count == 0 for count in replay["changed_counts"].values())
        )
        self.assertEqual(self.events, before)

    def test_incremental_later_observation_updates_a_tied_clock_key(
        self,
    ):
        first = self.load()
        body = incremental_document(first["batch_id"])
        body["tables"]["erp_products"]["rows"][0]["values"]["product_name"] = (
            "更新"
        )
        result = self.load(body, LATER)
        self.assertEqual(result["changed_counts"]["erp_products"], 1)
        rows = self.catalog.tables["sql_bronze.erp_products"].rows
        self.assertEqual(rows[0]["product_name"], "更新")
        self.assertEqual(rows[0]["_source_updated_at"], SOURCE_UPDATED)

    def test_incremental_has_no_implicit_delete_or_regression(self):
        first = self.load(document(("K1", "K2")))
        body = incremental_document(first["batch_id"], keys=("K1",))
        row = body["tables"]["erp_products"]["rows"][0]
        row["source_updated_at"] = "2026-04-01T10:59:00.0000001Z"
        row["values"]["product_name"] = "Old replay"
        result = self.load(body, LATER)
        self.assertTrue(result["skipped"])
        rows = self.catalog.tables["sql_bronze.erp_products"].rows
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["product_name"], "日本語製品")
        self.assertEqual(result["changed_counts"]["erp_products"], 0)

    def test_incremental_requires_a_completed_known_snapshot_generation(self):
        body = incremental_document("a" * 64)
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(body, LATER)
        self.assertEqual(self.events, [])
        first = self.load()
        body = incremental_document("b" * 64)
        before = list(self.events)
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(body, LATER)
        self.assertEqual(self.events, before)
        self.assertNotEqual(first["batch_id"], "b" * 64)

    def test_incremental_rejects_late_rows_outside_declared_window(self):
        first = self.load()
        body = incremental_document(first["batch_id"])
        body["tables"]["erp_products"]["rows"][0]["source_updated_at"] = (
            "2026-04-01T10:50:00.0000001Z"
        )
        before = list(self.events)
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(body, LATER)
        self.assertEqual(self.events, before)
        body["tables"]["erp_products"]["rows"][0]["source_updated_at"] = (
            SOURCE_UPDATED
        )
        body["upper_bound"] = LAST
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(body, LATER)
        self.assertEqual(self.events, before)

    def test_incremental_partial_failure_replays_all_layers_before_completion(
        self,
    ):
        first = self.load()
        body = incremental_document(first["batch_id"], keys=("K1", "K2"))
        self.admin.medallion.side_effect = RuntimeError("synthetic-failure")
        with self.assertRaises(RuntimeError):
            self.load(body, LATER)
        self.assertEqual(
            len(self.catalog.tables["sql_control.batches"].rows), 1
        )
        self.assertEqual(
            self.catalog.tables["sql_control.progress"].rows[0]["mode"],
            "incremental",
        )
        self.admin.medallion.side_effect = lambda _: {"sql_statements": 14}
        result = self.load(body, LATER)
        self.assertFalse(result["skipped"])
        self.assertEqual(result["current_counts"]["erp_products"], 2)
        self.assertEqual(
            len(self.catalog.tables["sql_control.batches"].rows), 2
        )

    def test_incremental_cannot_repair_an_unfinished_full_snapshot(self):
        first = self.load()
        self.admin.medallion.side_effect = RuntimeError("synthetic-failure")
        with self.assertRaises(RuntimeError):
            self.load(document(("K1", "K2")), LATER)
        before = list(self.events)
        body = incremental_document(first["batch_id"], observed=LAST)
        with self.assertRaises(WORKER.SqlBatchError):
            self.load(body, LAST)
        self.assertEqual(self.events, before)

    def test_materialization_memory_bound_precedes_storage_mutation(self):
        with mock.patch.object(WORKER, "MAX_CURRENT_BYTES", 1):
            with self.assertRaises(WORKER.SqlBatchError):
                self.load()
        self.assertEqual(self.events, [])


if __name__ == "__main__":
    unittest.main()

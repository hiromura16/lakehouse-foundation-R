"""標準CDC Rawの採取範囲と隔離再生の拒否規則を検査する。"""

import json
import sqlite3
import unittest
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pyarrow as pa

from native import cdc_shadow as shadow

# cdc_shadowが実行時と同じモジュール探索先を準備する。
# isort: split
from raw_writer import append_raw, generic_rows  # noqa: E402

TABLE_ID = UUID("a8e34267-adfd-4216-b7b9-316bcc94268b")


class RawReader:
    """起動済みRESTから読み取る表の最小インターフェース。"""

    metadata = SimpleNamespace(format_version=2, table_uuid=TABLE_ID)
    config = {"scan-planning-mode": "client"}
    catalog = SimpleNamespace(supports_server_side_planning=lambda: False)

    def __init__(self, records, *, with_markers=False, arrow=None):
        rows = (
            generic_rows(records) if with_markers else shadow.raw_rows(records)
        )
        fields = [(name, pa.string()) for name in shadow.RAW_COLUMNS]
        if with_markers:
            fields.extend(
                (name, pa.bool_()) for name in ("key_is_null", "value_is_null")
            )
        self.arrow = (
            arrow
            if arrow is not None
            else pa.Table.from_pylist(rows, schema=pa.schema(fields))
        )
        self.selected = []

    def location(self):
        return "s3://synthetic-warehouse/cdc_events/events"

    def snapshot_by_id(self, identifier):
        return object() if identifier == 41 else None

    def scan(self, *, snapshot_id, limit):
        self.selected.append((snapshot_id, limit))

        def selection(*names):
            arrow = self.arrow.select(names)
            return SimpleNamespace(
                to_arrow_batch_reader=lambda: (
                    pa.RecordBatchReader.from_batches(
                        arrow.schema, arrow.to_batches()
                    )
                )
            )

        return SimpleNamespace(
            projection=lambda: SimpleNamespace(
                as_arrow=lambda: self.arrow.schema
            ),
            select=selection,
        )


class NativeShadowTests(unittest.TestCase):
    """承認範囲、欠落、トゥームストーンと既存出力を確認する。"""

    def setUp(self):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        source = self.root / "source.json"
        source.write_bytes(
            shadow.encoded(
                {
                    "source": {
                        "CDC_SQLSERVER_DATABASE": "synthetic",
                        "CDC_SQLSERVER_HOST": "sql.invalid",
                        "CDC_TOPIC_PREFIX": "fixture",
                    },
                    "topic_has_database": True,
                }
            )
        )
        self.config = shadow.source_from_files(
            source,
            shadow.ROOT / "infra/source/sqlserver-tables.example.json",
        )
        self.topic = next(iter(self.config.topics))
        self.bundle = self.root / "bundle"
        self.output = self.root / "shadow"
        self.approval = {
            "raw_snapshot_id": 41,
            "raw_table_uuid": str(TABLE_ID),
            "source_sha256": shadow.identity_digest(self.config.identity),
            "completed": {
                topic: 0 if topic == self.topic else -1
                for topic in self.config.topics
            },
        }

    def record(self, offset=0, **overrides):
        return {
            "topic": self.topic,
            "partition": 0,
            "offset": offset,
            "key": '{"id":"synthetic"}',
            "value": '{"op":"c"}',
            "ingested_at": "2026-04-01T00:00:00+00:00",
            **overrides,
        }

    def export(self, *records, with_markers=False):
        table = RawReader(records, with_markers=with_markers)
        loader = Mock(return_value=table)
        digest = shadow.export_bundle(
            loader, self.config, self.approval, self.bundle
        )
        loader.assert_called_once_with("cdc_events.events")
        self.assertEqual(table.selected[0][0], 41)
        return digest

    def test_approved_snapshot_ignores_pending_empty_body(self):
        self.export(self.record(), self.record(offset=1, value=None))
        manifest = json.loads(self.bundle.read_bytes())
        self.assertEqual(manifest["records"], [self.record()])
        self.assertEqual(manifest["approval"], self.approval)
        self.assertEqual(self.bundle.stat().st_mode & 0o777, 0o600)

    def test_legacy_approved_empty_value_has_no_lossless_restore(self):
        for value in ("", None):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(shadow.ShadowError, "AMBIGUOUS_RAW"),
            ):
                self.export(self.record(value=value))
            self.assertFalse(self.bundle.exists())

    def test_new_confirmed_tombstone_keeps_null_and_empty_keys(self):
        self.approval["completed"][self.topic] = 1
        records = [
            self.record(key=None, value=None),
            self.record(offset=1, key="", value=None),
        ]
        self.export(*records, with_markers=True)
        document = json.loads(self.bundle.read_bytes())
        self.assertEqual(document["format"], shadow.FORMAT)
        self.assertEqual(document["records"], records)

    def test_new_confirmed_empty_body_is_invalid_business_delivery(self):
        with self.assertRaisesRegex(shadow.ShadowError, "INVALID_EVENT_BODY"):
            self.export(self.record(value=""), with_markers=True)
        self.assertFalse(self.bundle.exists())

    def test_marker_true_with_nonempty_value_is_wrong_raw(self):
        rows = generic_rows([self.record()])
        rows[0]["value_is_null"] = True
        arrow = pa.Table.from_pylist(rows)
        # 実テーブルに合わせ、判定列はoptional Booleanとする。
        table = RawReader([], arrow=arrow)
        with self.assertRaisesRegex(shadow.ShadowError, "RAW_MISMATCH"):
            shadow.export_bundle(
                lambda _: table, self.config, self.approval, self.bundle
            )
        self.assertFalse(self.bundle.exists())

    def test_gap_duplicate_and_other_partition_are_rejected(self):
        for code, rows in (
            ("HISTORY_GAP", [self.record(offset=1)]),
            ("HISTORY_GAP", [self.record(), self.record()]),
            ("PARTITION_UNSUPPORTED", [self.record(partition=1)]),
        ):
            with (
                self.subTest(code=code),
                self.assertRaisesRegex(shadow.ShadowError, code),
            ):
                self.export(*rows)
            self.assertFalse(self.bundle.exists())

    def partition_approval(self, *offsets):
        """試験対象以外の購読区画も、空として明示的に承認する。"""
        self.approval["completed"] = {
            topic: {"0": -1} for topic in self.config.topics
        }
        self.approval["completed"][self.topic] = {
            str(index): offset for index, offset in enumerate(offsets)
        }

    def product(self, key, partition, offset, op, *, lsn=0, name=None):
        """通常のSQL Server CDCと同じ、単一業務キーの行を作る。"""
        mapping = self.config.mapping["erp_products"]
        row = {
            "product_id": str(key),
            "product_name": name or str(key),
            "product_family": "合成",
            "unit": "piece",
            "standard_cycle_seconds": "25.000",
            "updated_at": "2026-04-01T00:00:00Z",
        }
        return self.record(
            offset,
            partition=partition,
            key=json.dumps({"product_id": key}),
            value=json.dumps(
                {
                    "op": op,
                    "before": row if op == "d" else None,
                    "after": None if op == "d" else row,
                    "source": {
                        "db": "synthetic",
                        "schema": mapping["source_schema"],
                        "table": mapping["source_table"],
                        "commit_lsn": f"00000001:00000001:{lsn:04x}",
                        "change_lsn": f"00000001:00000001:{lsn:04x}",
                        "event_serial_no": 1,
                    },
                },
                ensure_ascii=False,
            ),
        )

    def replay(self, document):
        """この試験専用の承認済みバイト列から標準処理を起動する。"""
        body = shadow.encoded(document)
        self.bundle.write_bytes(body)
        return shadow.replay_bundle(
            self.bundle, sha256(body).hexdigest(), self.config, self.output
        )

    def test_real_two_partition_raw_and_shadow(self):
        """実Icebergの固定世代から、同じキーの新版と削除だけを反映する。"""
        self.assertEqual(self.config.topics[self.topic], "erp_products")
        self.partition_approval(2, 4)
        records = [
            self.product("A", 0, 0, "r", name="初回"),
            self.product("A", 0, 1, "u", lsn=20, name="最新版"),
            self.product("A", 0, 2, "u", lsn=15, name="遅延した旧版"),
            self.product("B", 1, 0, "c", lsn=5),
            self.product("C", 1, 1, "c", lsn=9),
            self.product("C", 1, 2, "d", lsn=12),
            self.record(3, partition=1, key='{"product_id":"C"}', value=None),
            self.record(4, partition=1, key="", value=None),
        ]
        source_root = self.root / "raw-source"
        source_root.mkdir()
        source_admin = shadow.ShadowAdmin(source_root)
        public_catalog = source_admin.catalog()
        self.addCleanup(public_catalog.engine.dispose)
        append_raw(public_catalog, list(reversed(records)))
        source_table = public_catalog.load_table(shadow.RAW)
        approved = source_table.current_snapshot().snapshot_id
        self.approval.update(
            raw_table_uuid=str(source_table.metadata.table_uuid),
            raw_snapshot_id=approved,
        )
        append_raw(public_catalog, [self.product("D", 1, 5, "c", lsn=50)])
        refreshed = public_catalog.load_table(shadow.RAW)
        self.assertNotEqual(refreshed.current_snapshot().snapshot_id, approved)
        kind, positions = shadow.check_approval(self.config, self.approval)
        self.assertEqual(kind, shadow.PARTITION_FORMAT)
        selected = shadow.records_from_table(
            refreshed, approved, positions, source=self.config
        )
        self.assertEqual(selected, records)
        self.assertEqual(
            shadow.records_from_table(
                refreshed,
                refreshed.current_snapshot().snapshot_id,
                positions,
                source=self.config,
            ),
            records,
        )
        evidence = self.replay(
            {
                "approval": self.approval,
                "format": kind,
                "recipe": shadow.replay_recipe(),
                "records": selected,
            }
        )
        self.assertEqual(evidence["raw_events_appended"], 8)
        self.assertEqual(evidence["sql_statements"], 14)
        state_uri = (self.output / "state.sqlite3").as_uri() + "?mode=ro"
        with closing(sqlite3.connect(state_uri, uri=True)) as state:
            positions = {
                (topic, partition): offset
                for topic, partition, offset in state.execute(
                    "SELECT topic, partition_id, offset_id FROM offsets"
                )
            }
            self.assertEqual(
                positions, {(self.topic, 0): 2, (self.topic, 1): 4}
            )
            (digest,) = state.execute(
                "SELECT value FROM metadata WHERE key='identity'"
            ).fetchone()
            self.assertEqual(digest, self.approval["source_sha256"])
        shadow_catalog = shadow.ShadowAdmin(self.output).catalog()
        self.addCleanup(shadow_catalog.engine.dispose)
        silver = shadow_catalog.load_table("cdc_silver.erp_products")
        self.assertEqual(
            sorted(
                (row["product_id"], row["product_name"])
                for row in silver.scan().to_arrow().to_pylist()
            ),
            [("A", "最新版"), ("B", "B")],
        )
        raw = shadow_catalog.load_table(shadow.RAW).scan().to_arrow()
        self.assertEqual(raw.num_rows, 8)
        self.assertEqual(raw["value_is_null"].to_pylist().count(True), 2)
        self.assertTrue((self.output / "shadow-ready.json").is_file())

    def test_v3_rejects_unapproved_partition_and_gaps(self):
        self.partition_approval(0, 1)
        first = self.product("A", 0, 0, "r")
        other = self.product("B", 1, 1, "r")
        for rows, code in (
            ([first, other], "HISTORY_GAP"),
            ([first, {**other, "partition": 2}], "PARTITION_UNSUPPORTED"),
        ):
            with (
                self.subTest(code=code),
                self.assertRaisesRegex(shadow.ShadowError, code),
            ):
                self.export(*rows, with_markers=True)
            self.assertFalse(self.bundle.exists())

    def test_v3_rejects_migration_for_business_and_tombstone_keys(self):
        self.partition_approval(0, 0)
        first = self.product(42, 0, 0, "r")
        # 数値42と文字列42はSQLiteの現在値に対して同じキーになる。
        migrated = self.product("42", 1, 0, "u", lsn=10)
        for value in (migrated["value"], None):
            with (
                self.subTest(tombstone=value is None),
                self.assertRaisesRegex(
                    shadow.ShadowError, "KEY_CHANGED_PARTITION"
                ),
            ):
                self.export(
                    first, {**migrated, "value": value}, with_markers=True
                )
            self.assertFalse(self.bundle.exists())

    def test_v3_rejects_wrong_order_and_format_before_creating_output(self):
        self.partition_approval(0, 0)
        ordered = [self.product("A", 0, 0, "r"), self.product("B", 1, 0, "c")]
        malformed = dict(ordered[1])
        envelope = json.loads(malformed["value"])
        envelope["source"]["change_lsn"] = "not-an-lsn"
        malformed["value"] = json.dumps(envelope)
        wrong_origin = dict(ordered[1])
        envelope = json.loads(wrong_origin["value"])
        envelope["source"]["db"] = "other-source"
        wrong_origin["value"] = json.dumps(envelope)
        self.export(*reversed(ordered), with_markers=True)
        document = json.loads(self.bundle.read_bytes())
        self.assertEqual(document["format"], shadow.PARTITION_FORMAT)
        self.assertEqual(document["records"], ordered)
        for changes, code in (
            ({"records": list(reversed(ordered))}, "HISTORY_GAP"),
            ({"records": [ordered[0], malformed]}, "EVENT_ORDER_INVALID"),
            ({"records": [ordered[0], wrong_origin]}, "EVENT_SOURCE_MISMATCH"),
            ({"format": shadow.FORMAT}, "BUNDLE_INVALID"),
        ):
            with (
                self.subTest(code=code),
                self.assertRaisesRegex(shadow.ShadowError, code),
            ):
                self.replay({**document, **changes})
            self.assertFalse(self.output.exists())

    def test_v3_topology_bounds_and_old_approval_compatibility(self):
        old_digest = shadow.identity_digest(self.config.identity)
        fmt, completed = shadow.check_approval(self.config, self.approval)
        self.assertEqual(fmt, shadow.FORMAT)
        self.assertEqual(completed[(self.topic, 0)], 0)
        for fmt in (shadow.LEGACY_FORMAT, shadow.FORMAT):
            document = {
                "format": fmt,
                "approval": self.approval,
                "recipe": shadow.replay_recipe(),
                "records": [self.product("A", 0, 0, "r")],
            }
            self.output = self.root / fmt
            self.assertEqual(
                self.replay(document)["source_sha256"], old_digest
            )
        self.partition_approval(0, -1)
        for partitions in (
            {"0": 0, "2": -1},
            {"0": 0, "01": -1},
            {"0": True},
            {str(index): -1 for index in range(17)},
            {"0": 600, "1": 600},
            0,
        ):
            self.approval["completed"][self.topic] = partitions
            with (
                self.subTest(partitions=partitions),
                self.assertRaises(shadow.ShadowError),
            ):
                shadow.check_approval(self.config, self.approval)

    def test_wrong_table_or_source_rejected_before_scan(self):
        table = RawReader([self.record()])
        loader = Mock(return_value=table)
        for mutation, code in (
            ({"source_sha256": "0" * 64}, "APPROVAL_INVALID"),
            ({"raw_table_uuid": str(UUID(int=0))}, "RAW_MISMATCH"),
            ({"raw_snapshot_id": 42}, "RAW_MISMATCH"),
            ({"completed": {self.topic: 0}}, "APPROVAL_INVALID"),
        ):
            with (
                self.subTest(code=code),
                self.assertRaisesRegex(shadow.ShadowError, code),
            ):
                shadow.export_bundle(
                    loader,
                    self.config,
                    {**self.approval, **mutation},
                    self.bundle,
                )
        self.assertEqual(table.selected, [])

    def test_server_side_planning_is_rejected_before_any_scan(self):
        table = RawReader([self.record()])
        table.catalog = SimpleNamespace(
            supports_server_side_planning=lambda: True
        )
        with self.assertRaisesRegex(shadow.ShadowError, "RAW_MISMATCH"):
            shadow.export_bundle(
                lambda _: table, self.config, self.approval, self.bundle
            )
        self.assertEqual(table.selected, [])
        self.assertFalse(self.bundle.exists())

    def test_tamper_and_preexisting_output_cannot_publish(self):
        proof = self.export(self.record())
        with self.assertRaisesRegex(
            shadow.ShadowError, "APPROVAL_HASH_MISMATCH"
        ):
            shadow.replay_bundle(
                self.bundle, "0" * 64, self.config, self.output
            )
        self.assertFalse(self.output.exists())
        parent = self.root / "outside"
        parent.mkdir()
        self.output.symlink_to(parent, target_is_directory=True)
        with self.assertRaises(FileExistsError):
            shadow.replay_bundle(self.bundle, proof, self.config, self.output)
        self.assertEqual(list(parent.iterdir()), [])

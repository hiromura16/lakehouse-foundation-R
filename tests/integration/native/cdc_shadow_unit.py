"""標準CDC Rawの採取範囲と隔離再生の拒否規則を検査する。"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pyarrow as pa

from native import cdc_shadow as shadow

# cdc_shadowが実行時と同じモジュール探索先を準備する。
# isort: split
from raw_writer import generic_rows  # noqa: E402

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

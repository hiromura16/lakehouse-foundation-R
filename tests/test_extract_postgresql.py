"""PostgreSQLスナップショットと既存SQLバッチの契約境界を確認する。"""

import importlib.util
import json
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from test_sql_batch_worker import CONTRACTS, document

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "extract_postgresql", ROOT / "scripts" / "extract-postgresql.py"
)
EXTRACT = importlib.util.module_from_spec(SPEC)
with patch.object(sys, "path", [str(ROOT / "scripts"), *sys.path]):
    SPEC.loader.exec_module(EXTRACT)
MAPPING = ROOT / "infra" / "source" / "postgresql-tables.example.json"


def postgres_document():
    """既存テストの台帳形状を保ち、PostgreSQLの実際の表名を設定する。"""
    value = document()
    value["source_database"] = "factory_db"
    value["source_system"] = EXTRACT.worker.POSTGRESQL_SOURCE
    for name, table in value["tables"].items():
        domain, relation = name.split("_", 1)
        table.update(schema="Factory_" + domain, table=relation)
    return value


class PostgreSQLSnapshotTests(unittest.TestCase):
    """共通バッチの互換性、型、秘密、再発行の規則を点で検証する。"""

    def test_exact_business_values(self):
        """UTCのマイクロ秒と日付、NUMERICの末尾0とNULLを区別する。"""
        japan = timezone(timedelta(hours=9))
        stamp = datetime(2026, 1, 5, 12, 0, 0, 123456, tzinfo=japan)
        self.assertEqual(
            EXTRACT.serialize(stamp), "2026-01-05T03:00:00.123456Z"
        )
        self.assertEqual(EXTRACT.serialize(date(2026, 1, 5)), "2026-01-05")
        self.assertEqual(EXTRACT.serialize(Decimal("12.3400")), "12.3400")
        self.assertEqual(EXTRACT.serialize(""), "")
        self.assertIsNone(EXTRACT.serialize(None))
        for unsupported in (stamp.replace(tzinfo=None), Decimal("NaN"), 1.2):
            with self.assertRaises(EXTRACT.BatchError):
                EXTRACT.serialize(unsupported)

    def test_private_settings_mode_and_single_host(self):
        """秘密JSONの過大な読取権限と複数ホストを拒否する。"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ca = root / "synthetic-ca.pem"
            ca.touch()
            ca.chmod(0o644)
            path = root / "source.json"
            config = {
                "host": "SOURCE.example.invalid",
                "port": 5432,
                "database": "factory_db",
                "user": "fixture_reader",
                "password": "synthetic-only",
                "ca_file": str(ca),
                "source_id": "3cab43f8-bd2f-499d-9ec6-3a7dc440c624",
            }
            path.write_text(json.dumps(config))
            path.chmod(0o600)
            self.assertEqual(EXTRACT.read_settings(path), config)
            path.chmod(0o644)
            with self.assertRaises(EXTRACT.BatchError):
                EXTRACT.read_settings(path)
            path.chmod(0o600)
            config["host"] = "first.example.invalid,second.example.invalid"
            path.write_text(json.dumps(config))
            with self.assertRaises(EXTRACT.BatchError):
                EXTRACT.read_settings(path)

    def test_mapping_and_source_identity(self):
        """6表を明示し、資格情報の更新とデータ源の変更を区別する。"""
        mapping = EXTRACT.read_mapping(MAPPING, CONTRACTS)
        config = {
            "host": "SOURCE.example.invalid",
            "port": 5432,
            "database": "factory_db",
            "source_id": "3cab43f8-bd2f-499d-9ec6-3a7dc440c624",
        }
        before = EXTRACT.source_identity(config, mapping)
        self.assertEqual(
            before,
            EXTRACT.source_identity(config | {"password": "new"}, mapping),
        )
        self.assertNotEqual(
            before,
            EXTRACT.source_identity(config | {"database": "second"}, mapping),
        )
        exported = json.loads(MAPPING.read_text())
        exported["tables"][0]["columns"]["product_id"] = "x + y"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "columns.json"
            path.write_text(json.dumps(exported))
            with self.assertRaises(EXTRACT.BatchError):
                EXTRACT.read_mapping(path, CONTRACTS)

    def test_worker_keeps_sql_server_and_rejects_postgres_incremental(self):
        """新経路は全件専用。固定SQL Serverの従来条件を維持する。"""
        legacy = document()
        current = postgres_document()
        self.assertEqual(
            EXTRACT.worker.validate_document(legacy, CONTRACTS),
            EXTRACT.worker.validate_document(current, CONTRACTS),
        )
        with patch.object(EXTRACT.worker, "MAX_ROWS", 1):
            EXTRACT.worker.validate_document(legacy, CONTRACTS)
            with self.assertRaisesRegex(
                EXTRACT.worker.SqlBatchError, "total row limit"
            ):
                EXTRACT.worker.validate_document(current, CONTRACTS)
        current["mode"] = "incremental"
        with self.assertRaises(EXTRACT.worker.SqlBatchError):
            EXTRACT.worker.validate_document(current, CONTRACTS)
        legacy["tables"]["erp_products"]["schema"] = "different"
        with self.assertRaises(EXTRACT.worker.SqlBatchError):
            EXTRACT.worker.validate_document(legacy, CONTRACTS)

    def test_exact_republication_reuses_payload_and_ready_marker(self):
        """同じバイトの観測は内容IDを共有し、異なる観測時刻を返す。"""
        value = postgres_document()
        first = "2026-01-05T00:00:00.0000000Z"
        second = "2026-01-05T00:00:01.0000000Z"
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(
                EXTRACT,
                "snapshot",
                side_effect=[(value, first), (value, second)],
            ):
                a = EXTRACT.prepare({}, {}, CONTRACTS, directory)
                b = EXTRACT.prepare({}, {}, CONTRACTS, directory)
            self.assertEqual(a["batch_id"], b["batch_id"])
            self.assertNotEqual(a["observed_at"], b["observed_at"])
            payload, result = EXTRACT.worker.read_document(
                a["batch_id"], Path(directory)
            )
            self.assertEqual(payload, EXTRACT.encode(value))
            self.assertEqual(result, value)

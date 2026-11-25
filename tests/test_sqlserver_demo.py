"""固定構成の合成SQLを対象に安全性を試す。SQL Serverへの接続は行わない。

値の変換やカーソルとドライバーを差し替えた試験は、
DDLを静的に確認する契約試験と分ける。模擬結果は、SQL Serverで
文が実行されたことやCDCから実イベントが配送されたことを示さない。
"""

import copy
import importlib.util
import json
import os
import re
import sys
import unittest
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


def load(name, relative):
    """CLIや接続を起動せずに、本体の実装を読み込む。"""
    specification = importlib.util.spec_from_file_location(
        name, ROOT / relative
    )
    result = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(result)
    return result


DEMO = load("fixed_sql_unit", "scripts/sqlserver_demo.py")
GENERATOR = load("fixed_sql_generator", "scripts/generate-demo.py")
RUN = "0123456789ab"
VALUES = {
    "DEMO_SQLSERVER_HOST": "demo-db.example.invalid",
    "DEMO_SQLSERVER_PORT": "1433",
    "DEMO_SQLSERVER_DATABASE": "synthetic_demo",
    "DEMO_SQLSERVER_USER": "synthetic_writer",
    "DEMO_SQLSERVER_PASSWORD": "synthetic-writer-secret",
    "SOURCE_SQLSERVER_HOST": "demo-db.example.invalid",
    "SOURCE_SQLSERVER_PORT": "1433",
    "SOURCE_SQLSERVER_DATABASE": "synthetic_demo",
    "SOURCE_SQLSERVER_USER": "synthetic_reader",
    "SOURCE_SQLSERVER_PASSWORD": "synthetic-reader-secret",
    "SOURCE_SQLSERVER_CONTROL_USER": "synthetic_control",
    "SOURCE_SQLSERVER_CONTROL_PASSWORD": "synthetic-control-secret",
    "DEMO_SQLSERVER_ALLOW_SHARED_SOURCE_DATABASE": "true",
    "DEMO_SQLSERVER_INSTALLATION_ID": "a" * 32,
    "DEMO_SQLSERVER_OWNERSHIP_TOKEN": "b" * 64,
}


class Cursor:
    """パラメーター付きDMLを記録し、合成のメモリ内行だけを保持する。"""

    def __init__(self):
        self.calls = []
        self.rows = {}
        self.next = None
        self.fail_insert = None

    def execute(self, statement, arguments=()):
        """試験対象のDMLだけを解釈し、SQLドライバーには処理を渡さない。"""
        self.calls.append((statement, arguments))
        self.next = None
        table = re.search(r"\[lh_demo_(?:erp|mes)\]\.\[[a-z_]+\]", statement)
        if (
            statement.startswith("SELECT ")
            and ", run_id, original_values FROM " in statement
        ):
            # 大文字小文字を区別せず、空白で埋めるSQL照合を模擬する。
            # 実際に保存された主キーの値は、応答行から確認できるようにする。
            requested = arguments[0].casefold().rstrip(" ")
            for (origin, key), values in self.rows.items():
                if (
                    origin == table[0]
                    and key.casefold().rstrip(" ") == requested
                ):
                    self.next = (key, *values)
                    break
        elif statement.startswith("INSERT INTO ") and table:
            if self.fail_insert == table[0]:
                raise RuntimeError("synthetic-driver-private-information")
            columns = re.search(r"\(([^)]+)\) VALUES", statement)[1]
            names = [part.strip("[] ") for part in columns.split(",")]
            row = dict(zip(names, arguments, strict=True))
            self.rows[(table[0], arguments[0])] = (
                row["run_id"],
                row["original_values"],
            )
        elif statement.startswith("UPDATE ") and table:
            self.rows[(table[0], arguments[-2])] = (
                arguments[-1],
                arguments[-3],
            )
        elif statement.startswith("DELETE FROM ") and table:
            self.rows.pop((table[0], arguments[0]), None)

    def fetchone(self):
        """直前の文に対して、用意した合成応答を返す。"""
        return self.next


class Handle:
    """コミットとロールバックを、カーソルの準備済み行と別に記録する。"""

    def __init__(self):
        self.reader = Cursor()
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def cursor(self):
        """この隔離試験の接続が保持する合成カーソルを返す。"""
        return self.reader

    def commit(self):
        """実データベースをコミットせずに、正常な完了を観測する。"""
        self.commits += 1

    def rollback(self):
        """実データベースに接続せずに、トランザクションの失敗を観測する。"""
        self.rollbacks += 1

    def close(self):
        """本体の後処理で、接続が常に閉じられることを観測する。"""
        self.closes += 1


@contextmanager
def injected_connection(handle):
    """ロールバックを模擬し、途中失敗を準備済みの合成行で確認する。"""
    before = copy.deepcopy(handle.reader.rows)
    try:
        yield handle
    except Exception:
        handle.reader.rows = before
        handle.rollback()
        raise


class BoundaryTests(unittest.TestCase):
    """ドライバーやファイルシステムの操作前に、危険な入力を拒否する。"""

    def test_shared_database_is_default_deny(self):
        values = dict(VALUES)
        values.pop("DEMO_SQLSERVER_ALLOW_SHARED_SOURCE_DATABASE")
        with self.assertRaisesRegex(DEMO.SqlServerError, "Shared source"):
            DEMO.validate_settings(values)

    def test_explicit_shared_flag_requires_exact_host_port_database(self):
        self.assertEqual(
            DEMO.validate_settings(VALUES)[
                "DEMO_SQLSERVER_ALLOW_SHARED_SOURCE_DATABASE"
            ],
            "true",
        )
        for key, value in (
            ("SOURCE_SQLSERVER_HOST", "another.example.invalid"),
            ("SOURCE_SQLSERVER_PORT", "1444"),
            ("SOURCE_SQLSERVER_HOST", VALUES["SOURCE_SQLSERVER_HOST"].upper()),
            (
                "SOURCE_SQLSERVER_DATABASE",
                VALUES["SOURCE_SQLSERVER_DATABASE"].upper(),
            ),
        ):
            with self.subTest(setting=key):
                with self.assertRaisesRegex(
                    DEMO.SqlServerError, "Shared source"
                ):
                    DEMO.validate_settings({**VALUES, key: value})

    def test_dbo_arbitrary_schema_and_unknown_table_are_rejected(self):
        for value in ("dbo", "arbitrary_schema", "lh_demo_erp;DELETE"):
            with self.subTest(schema=value):
                with self.assertRaises(DEMO.SqlServerError):
                    DEMO.validate_settings(
                        {**VALUES, "DEMO_SQLSERVER_ERP_SCHEMA": value}
                    )
        with self.assertRaises(DEMO.SqlServerError):
            DEMO.table_name("dbo.legacy_table")

    def test_write_and_mutate_require_strict_consent_before_connection(self):
        with mock.patch.object(DEMO, "connection") as driver:
            for value in (False, "true", 1):
                with self.subTest(consent=value):
                    with self.assertRaises(DEMO.SqlServerError):
                        DEMO.write_data({}, VALUES, run_id=RUN, consent=value)
                    with self.assertRaises(DEMO.SqlServerError):
                        DEMO.mutate(
                            VALUES, "insert", run_id=RUN, consent=value
                        )
        driver.assert_not_called()

    def test_mismatching_marker_fails_before_permission_or_business_queries(
        self,
    ):
        cursor = mock.Mock()
        cursor.fetchone.side_effect = [
            ("synthetic_demo",),
            (1,),
            (1, "a" * 32, "incorrect-hash"),
        ]
        with self.assertRaisesRegex(DEMO.SqlServerError, "ownership"):
            DEMO.check_writer(cursor, VALUES)
        self.assertFalse(
            any(
                "INSERT INTO" in call.args[0] or "UPDATE " in call.args[0]
                for call in cursor.execute.call_args_list
            )
        )

    def test_connected_database_case_alias_is_not_adopted(self):
        """どのサーバーでも、DBA用SQLに記載のデータベース識別値と照合する。"""
        cursor = mock.Mock()
        cursor.fetchone.return_value = (
            VALUES["DEMO_SQLSERVER_DATABASE"].upper(),
        )
        with self.assertRaisesRegex(DEMO.SqlServerError, "database"):
            DEMO.ownership(cursor, VALUES)
        self.assertEqual(cursor.execute.call_count, 1)

    def test_missing_control_credentials_never_fall_back_to_reader(self):
        for key in (
            "SOURCE_SQLSERVER_CONTROL_USER",
            "SOURCE_SQLSERVER_CONTROL_PASSWORD",
        ):
            values = dict(VALUES)
            values.pop(key)
            with self.subTest(setting=key):
                with self.assertRaises(DEMO.SqlServerError):
                    DEMO.validate_settings(values, role="control")

    def test_control_principal_cannot_reuse_source_or_demo_identity(self):
        for reused in ("SOURCE_SQLSERVER_USER", "DEMO_SQLSERVER_USER"):
            with self.subTest(reused=reused):
                settings = {
                    **VALUES,
                    "SOURCE_SQLSERVER_CONTROL_USER": VALUES[reused].upper(),
                }
                driver = mock.Mock()
                with mock.patch.dict(
                    sys.modules, {"pymssql": SimpleNamespace(connect=driver)}
                ):
                    with self.assertRaisesRegex(
                        DEMO.SqlServerError, "must differ"
                    ):
                        with DEMO.connection(settings, role="control"):
                            self.fail("A reused control principal connected")
                driver.assert_not_called()

    def test_control_missing_comparison_identity_is_not_assumed_safe(self):
        for missing in ("SOURCE_SQLSERVER_USER", "DEMO_SQLSERVER_USER"):
            values = dict(VALUES)
            values.pop(missing)
            with self.subTest(missing=missing):
                with self.assertRaisesRegex(DEMO.SqlServerError, "identities"):
                    DEMO.validate_settings(values, role="control")

    def test_missing_writer_is_reported_without_echoing_secrets(self):
        values = {**VALUES, "DEMO_SQLSERVER_USER": ""}
        result = DEMO.diagnose(values)
        self.assertFalse(result["connected"])
        rendered = json.dumps(result)
        for private in (
            "synthetic-writer-secret",
            "demo-db.example.invalid",
            "synthetic_reader",
            "b" * 64,
        ):
            self.assertNotIn(private, rendered)


class DiagnosticTests(unittest.TestCase):
    """差し替えたメタデータ応答を通して、安全な診断を試す。"""

    def test_success_shape_contains_no_endpoint_user_or_ownership_token(self):
        handle = Handle()
        cursor = mock.Mock()
        cursor.fetchone.side_effect = [
            value
            for _ in DEMO.TABLES
            for value in ((1,), (3, "2026-02-01T08:00:00.1234567"), (1,))
        ]
        cursor.fetchall.side_effect = [
            [(DEMO.CAPTURE_INSTANCES[name],), ("unrelated_private_capture",)]
            for name in DEMO.TABLES
        ]
        handle.cursor = lambda: cursor
        with (
            mock.patch.object(
                DEMO, "connection", return_value=injected_connection(handle)
            ),
            mock.patch.object(DEMO, "ownership", return_value=1),
            mock.patch.object(DEMO, "permissions_safe", return_value=True),
        ):
            result = DEMO.diagnose(VALUES)
        self.assertEqual(
            set(result),
            {
                "connected",
                "ownership",
                "schema_version",
                "permissions_safe",
                "tables",
            },
        )
        self.assertTrue(result["connected"])
        self.assertTrue(result["ownership"])
        for name, table in result["tables"].items():
            self.assertEqual(table["rows"], 3)
            self.assertEqual(
                table["last_updated_at"], "2026-02-01T08:00:00.1234567Z"
            )
            self.assertEqual(
                table["capture_instance"], DEMO.CAPTURE_INSTANCES[name]
            )
        rendered = json.dumps(result)
        for hidden in (
            "demo-db.example.invalid",
            "synthetic_writer",
            "synthetic_demo",
            "synthetic-writer-secret",
            "unrelated_private_capture",
            "b" * 64,
        ):
            self.assertNotIn(hidden, rendered)

    def test_hidden_cdc_metadata_never_gates_writer_or_other_tables(self):
        handle = Handle()
        cursor = mock.Mock()
        current = None

        def operate(statement, _args=()):
            nonlocal current
            if (
                "is_tracked_by_cdc" in statement
                or "cdc.change_tables" in statement
            ):
                raise RuntimeError("opaque error")
            current = (4, None) if "COUNT_BIG" in statement else (1,)

        cursor.execute.side_effect = operate
        cursor.fetchone.side_effect = lambda: current
        handle.cursor = lambda: cursor
        with (
            mock.patch.object(
                DEMO, "connection", return_value=injected_connection(handle)
            ),
            mock.patch.object(DEMO, "ownership", return_value=1),
            mock.patch.object(DEMO, "permissions_safe", return_value=True),
        ):
            result = DEMO.diagnose(VALUES)
        self.assertTrue(result["permissions_safe"])
        for table in result["tables"].values():
            self.assertTrue(table["exists"])
            self.assertEqual(table["rows"], 4)
            self.assertIsNone(table["capture_instance"])
        self.assertNotIn("opaque error", json.dumps(result))

    def test_connection_failure_does_not_invent_connection_or_row_success(
        self,
    ):
        with mock.patch.object(
            DEMO,
            "connection",
            side_effect=RuntimeError("synthetic-secret-vendor-error"),
        ):
            result = DEMO.diagnose(VALUES)
        self.assertFalse(result["connected"])
        self.assertFalse(result["ownership"])
        self.assertTrue(
            all(table["rows"] is None for table in result["tables"].values())
        )
        self.assertNotIn("synthetic-secret", json.dumps(result))


class TransportTests(unittest.TestCase):
    """pymssqlを差し替え、本体のコンテキスト管理の動作を確認する。"""

    def test_source_is_readonly_and_control_uses_distinct_credentials(self):
        for role, user, readonly in (
            ("source", "synthetic_reader", True),
            ("control", "synthetic_control", False),
            ("demo", "synthetic_writer", False),
        ):
            with self.subTest(role=role):
                handle = Handle()
                driver = mock.Mock(return_value=handle)
                with mock.patch.dict(
                    sys.modules, {"pymssql": SimpleNamespace(connect=driver)}
                ):
                    with DEMO.connection(VALUES, role=role):
                        config = Path(os.environ["FREETDSCONF"])
                        contents = config.read_text()
                        self.assertIn("encryption = require", contents)
                        self.assertIn(
                            "check certificate hostname = yes", contents
                        )
                        self.assertEqual(config.stat().st_mode & 0o777, 0o600)
                self.assertEqual(
                    driver.call_args.kwargs["read_only"], readonly
                )
                self.assertEqual(driver.call_args.kwargs["user"], user)
                self.assertEqual(
                    driver.call_args.kwargs["encryption"], "require"
                )
                self.assertEqual(handle.closes, 1)
                self.assertFalse(config.exists())

    def test_explicit_poc_trust_preserves_encryption_and_restores_environment(
        self,
    ):
        handle = Handle()
        with (
            mock.patch.dict(
                sys.modules,
                {
                    "pymssql": SimpleNamespace(
                        connect=mock.Mock(return_value=handle)
                    )
                },
            ),
            mock.patch.dict(os.environ, {"FREETDS": "/example/old.conf"}),
        ):
            previous = dict(os.environ)
            with DEMO.connection(
                {**VALUES, "DEMO_SQLSERVER_TRUST_SERVER_CERTIFICATE": "true"}
            ):
                config = Path(os.environ["FREETDSCONF"]).read_text()
                self.assertIn("encryption = require", config)
                self.assertNotIn("check certificate hostname = yes", config)
            self.assertEqual(dict(os.environ), previous)
        with self.assertRaisesRegex(DEMO.SqlServerError, "require encryption"):
            DEMO.validate_settings(
                {**VALUES, "DEMO_SQLSERVER_ENCRYPT": "false"}
            )

    def test_driver_failure_rolls_back_and_returns_no_vendor_text(self):
        handle = Handle()
        with mock.patch.dict(
            sys.modules,
            {
                "pymssql": SimpleNamespace(
                    connect=mock.Mock(return_value=handle)
                )
            },
        ):
            with self.assertRaises(DEMO.SqlServerError) as error:
                with DEMO.connection(VALUES):
                    raise RuntimeError("synthetic-secret-from-driver")
        self.assertEqual(handle.rollbacks, 1)
        self.assertEqual(handle.closes, 1)
        self.assertNotIn("synthetic-secret", str(error.exception))


class TypedSourceTests(unittest.TestCase):
    """SQL互換の変換後の値とは別に、元の値が保持されることを試す。"""

    def test_exact_decimal_null_empty_and_japanese_survive_originals(self):
        data = GENERATOR.build_data(42, 1, 7, "clean")
        data["erp_products"][0].update(
            product_name="日本語の合成部品",
            product_family=None,
            unit="",
            standard_cycle_seconds="25.123",
        )
        prepared, _, _ = DEMO.prepare_data(data)
        _, typed, payload = prepared["erp_products"][0]
        self.assertEqual(typed["standard_cycle_seconds"], Decimal("25.123"))
        self.assertIsNone(typed["product_family"])
        self.assertEqual(typed["unit"], "")
        self.assertEqual(
            json.loads(payload)["product_name"], "日本語の合成部品"
        )
        self.assertIsNone(json.loads(payload)["product_family"])
        self.assertEqual(json.loads(payload)["unit"], "")

    def test_invalid_typed_values_become_null_but_raw_is_unchanged(self):
        data = GENERATOR.build_data(42, 1, 7, "clean")
        data["erp_products"][0]["standard_cycle_seconds"] = "invalid-number"
        data["erp_work_orders"][0]["planned_date"] = "2026-99-99"
        prepared, _, normalized = DEMO.prepare_data(data)
        _, typed, payload = prepared["erp_products"][0]
        self.assertIsNone(typed["standard_cycle_seconds"])
        self.assertEqual(
            json.loads(payload)["standard_cycle_seconds"], "invalid-number"
        )
        self.assertGreaterEqual(normalized, 2)

    def test_datetime2_fraction_is_not_rounded_to_python_microseconds(self):
        self.assertEqual(
            DEMO.timestamp("2026-02-01T12:34:56.1234567+09:00"),
            "2026-02-01T03:34:56.1234567",
        )
        self.assertEqual(
            DEMO.timestamp("2026-02-01T00:00:00Z"),
            "2026-02-01T00:00:00.0000000",
        )

    def test_latest_revision_and_exact_duplicate_collapse_deterministically(
        self,
    ):
        data = GENERATOR.build_data(42, 1, 7, "edge")
        prepared, duplicates, _ = DEMO.prepare_data(data)
        reversed_data = {
            key: list(reversed(rows)) for key, rows in data.items()
        }
        reversed_prepared, reversed_duplicates, _ = DEMO.prepare_data(
            reversed_data
        )
        self.assertEqual(prepared, reversed_prepared)
        self.assertEqual(duplicates, reversed_duplicates)
        self.assertEqual(duplicates, 2)
        first = [
            row for row in prepared["mes_events"] if row[0] == "EV00001-1"
        ][0]
        self.assertEqual(
            json.loads(first[2])["updated_at"], "2026-01-14T10:00:00Z"
        )

    def test_missing_key_and_extra_contract_cannot_be_written(self):
        data = GENERATOR.build_data(42, 1, 7, "clean")
        data["erp_products"][0]["product_id"] = ""
        with self.assertRaisesRegex(DEMO.SqlServerError, "primary key"):
            DEMO.prepare_data(data)
        with self.assertRaises(DEMO.SqlServerError):
            DEMO.prepare_data({**data, "arbitrary": []})


class WriteTests(unittest.TestCase):
    """再実行、所有権の衝突、トランザクション失敗を試す。"""

    def setUp(self):
        self.handle = Handle()
        self.guard = mock.patch.object(DEMO, "check_writer")
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.patch = mock.patch.object(
            DEMO,
            "connection",
            side_effect=lambda *args, **kwargs: injected_connection(
                self.handle
            ),
        )
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_replay_skips_without_changing_source_watermark(self):
        data = GENERATOR.build_data(42, 1, 7, "clean")
        first = DEMO.write_data(data, VALUES, run_id=RUN, consent=True)
        count = len(self.handle.reader.calls)
        replay = DEMO.write_data(data, VALUES, run_id=RUN, consent=True)
        self.assertGreater(first["inserted"], 0)
        self.assertEqual(replay["inserted"], 0)
        self.assertEqual(replay["updated"], 0)
        self.assertEqual(replay["skipped"], first["inserted"])
        self.assertFalse(
            any(
                statement.startswith("UPDATE [lh_demo_erp]")
                for statement, _ in self.handle.reader.calls[count:]
            )
        )
        self.assertEqual(self.handle.commits, 2)

    def test_case_or_space_equivalent_source_key_does_not_update_any_row(self):
        for physical in ("p001", "P001 "):
            with self.subTest(physical=physical):
                self.handle = Handle()
                self.handle.reader.rows[
                    ("[lh_demo_erp].[products]", physical)
                ] = (RUN, "existing-original")
                before = copy.deepcopy(self.handle.reader.rows)
                with self.assertRaisesRegex(DEMO.SqlServerError, "collation"):
                    DEMO.write_data(
                        GENERATOR.build_data(42, 1, 7, "clean"),
                        VALUES,
                        run_id=RUN,
                        consent=True,
                    )
                self.assertEqual(self.handle.reader.rows, before)
                self.assertEqual(self.handle.commits, 0)
                self.assertEqual(self.handle.rollbacks, 1)
                self.assertFalse(
                    any(
                        statement.startswith(("INSERT ", "UPDATE ", "DELETE "))
                        for statement, _ in self.handle.reader.calls
                    )
                )

    def test_case_or_space_equivalent_cdc_key_rejects_each_mutation(self):
        key = "CDC-" + RUN
        for physical in (key.lower(), key + " "):
            for action in ("insert", "update", "delete"):
                with self.subTest(physical=physical, action=action):
                    self.handle = Handle()
                    self.handle.reader.rows[
                        ("[lh_demo_erp].[products]", physical)
                    ] = (RUN, "existing-original")
                    before = copy.deepcopy(self.handle.reader.rows)
                    with self.assertRaisesRegex(
                        DEMO.SqlServerError, "collation"
                    ):
                        DEMO.mutate(VALUES, action, run_id=RUN, consent=True)
                    self.assertEqual(self.handle.reader.rows, before)
                    self.assertEqual(self.handle.commits, 0)
                    self.assertEqual(self.handle.rollbacks, 1)
                    self.assertFalse(
                        any(
                            statement.startswith(
                                ("INSERT ", "UPDATE ", "DELETE ")
                            )
                            for statement, _ in self.handle.reader.calls
                        )
                    )

    def test_source_updated_at_is_server_owned_not_generator_history(self):
        data = GENERATOR.build_data(42, 1, 7, "clean")
        DEMO.write_data(data, VALUES, run_id=RUN, consent=True)
        inserts = [
            (statement, values)
            for statement, values in self.handle.reader.calls
            if statement.startswith("INSERT INTO [lh_demo_")
        ]
        self.assertTrue(inserts)
        for statement, values in inserts:
            columns = re.search(r"\(([^)]+)\) VALUES", statement)[1]
            self.assertNotIn("[updated_at]", columns)
            self.assertIn("[original_values]", columns)
            self.assertIn("updated_at", json.loads(values[-1]))

    def test_partial_failure_does_not_commit_any_synthetic_rows(self):
        self.handle.reader.fail_insert = "[lh_demo_mes].[events]"
        with self.assertRaises(RuntimeError):
            DEMO.write_data(
                GENERATOR.build_data(42, 1, 7, "clean"),
                VALUES,
                run_id=RUN,
                consent=True,
            )
        self.assertEqual(self.handle.commits, 0)
        self.assertEqual(self.handle.rollbacks, 1)
        self.assertEqual(self.handle.reader.rows, {})

    def test_other_run_key_is_preserved(self):
        self.handle.reader.rows[("[lh_demo_erp].[products]", "P001")] = (
            "fedcba987654",
            "existing-original",
        )
        before = copy.deepcopy(self.handle.reader.rows)
        with self.assertRaisesRegex(DEMO.SqlServerError, "another run"):
            DEMO.write_data(
                GENERATOR.build_data(42, 1, 7, "clean"),
                VALUES,
                run_id=RUN,
                consent=True,
            )
        self.assertEqual(self.handle.reader.rows, before)
        self.assertEqual(self.handle.commits, 0)

    def test_fixed_cdc_insert_update_delete_and_no_arbitrary_target(self):
        for action in ("insert", "update", "delete"):
            with self.subTest(action=action):
                result = DEMO.mutate(VALUES, action, run_id=RUN, consent=True)
                self.assertEqual(
                    result,
                    {
                        "action": action,
                        "changed_rows": 1,
                        "run_id": RUN,
                        "source_only": True,
                    },
                )
        self.assertEqual(self.handle.reader.rows, {})
        mutations = [
            statement
            for statement, _ in self.handle.reader.calls
            if statement.startswith(("INSERT INTO", "UPDATE ", "DELETE FROM"))
        ]
        self.assertTrue(
            all(
                "[lh_demo_erp].[products]" in statement
                for statement in mutations
            )
        )
        with self.assertRaises(DEMO.SqlServerError):
            DEMO.mutate(VALUES, "DROP DATABASE", run_id=RUN, consent=True)


class SideEffectTests(unittest.TestCase):
    """手順を模擬して検証する。SQL Server実機の受入は別途必要となる。"""

    def cursor(self, *, definition=None, identity=1, safe=1, missing=False):
        cursor = mock.Mock()
        definition = (
            DEMO.SAFETY_DEFINITION if definition is None else definition
        )
        cursor.fetchone.side_effect = [
            None if missing else (definition, identity),
            (safe,),
            None,
        ]
        return cursor

    def test_exact_dbo_checker_is_required(self):
        cursor = self.cursor()
        self.assertTrue(DEMO.side_effects_safe(cursor))
        self.assertEqual(
            cursor.execute.call_args_list[-1].args,
            ("EXEC [lh_demo_ctl].[check_write_safety_v1]", ()),
        )

    def test_missing_modified_hidden_or_non_dbo_checker_is_refused(self):
        for options in (
            {"missing": True},
            {"definition": ""},
            {"definition": DEMO.SAFETY_DEFINITION + "--tampered"},
            {"identity": 0},
            {"identity": None},
        ):
            with self.subTest(options=options):
                cursor = self.cursor(**options)
                self.assertFalse(DEMO.side_effects_safe(cursor))
                self.assertEqual(cursor.execute.call_count, 1)

    def test_hidden_legacy_cascade_or_active_trigger_is_refused(self):
        # 既存の子オブジェクトは、呼出元には見えない場合がある。
        # dboの確認処理は完全な情報で危険を判定する。
        for role in ("demo", "control"):
            with self.subTest(role=role):
                self.assertFalse(
                    DEMO.side_effects_safe(self.cursor(safe=0), role)
                )
                self.assertTrue(DEMO.side_effects_safe(self.cursor(), role))

    def test_unknown_extra_rows_and_checker_failure_never_pass(self):
        self.assertFalse(DEMO.side_effects_safe(self.cursor(safe=None)))
        cursor = self.cursor()
        cursor.fetchone.side_effect = [(DEMO.SAFETY_DEFINITION, 1), (1,), (1,)]
        self.assertFalse(DEMO.side_effects_safe(cursor))
        cursor = self.cursor()
        cursor.execute.side_effect = [None, RuntimeError("checker failed")]
        with self.assertRaises(RuntimeError):
            DEMO.side_effects_safe(cursor)

    def test_source_reader_does_not_execute_checker(self):
        cursor = mock.Mock()
        self.assertTrue(DEMO.side_effects_safe(cursor, role="source"))
        cursor.execute.assert_not_called()


class PermissionTests(unittest.TestCase):
    """固定のカーソル応答で、権限が不明な場合の拒否を確認する。"""

    def cursor(
        self,
        *,
        broad=False,
        dangerous=False,
        unknown=False,
        denied_object=None,
        column_permission=None,
        role="demo",
        object_type="U",
        unknown_object=False,
        unknown_permission=False,
        metadata_only=False,
        missing_writer=False,
        missing_required_column=False,
        schema_grant=False,
        principals_visible=False,
        impersonates=False,
    ):
        cursor = mock.Mock()
        result = [None]
        statements = []

        def execute(statement, arguments=()):
            statements.append((statement, arguments))
            if statement == "SELECT OBJECT_ID(%s)":
                result[0] = (None,)
            elif statement == "SELECT IS_MEMBER(N'lh_demo_writer')":
                result[0] = (int(not missing_writer),)
            elif statement.startswith("SELECT IS_MEMBER"):
                count = statement.count("IS_MEMBER(") + statement.count(
                    "IS_SRVROLEMEMBER("
                )
                result[0] = tuple(
                    [None if unknown else int(broad)] + [0] * (count - 1)
                )
            elif statement == "SELECT DB_NAME()":
                result[0] = (VALUES["DEMO_SQLSERVER_DATABASE"],)
            elif statement.startswith(
                "SELECT schema_version, installation_id"
            ):
                result[0] = (
                    DEMO.SCHEMA_VERSION,
                    VALUES["DEMO_SQLSERVER_INSTALLATION_ID"],
                    DEMO.sha256(
                        VALUES["DEMO_SQLSERVER_OWNERSHIP_TOKEN"].encode()
                    ).hexdigest(),
                )
            elif "WITH (UPDLOCK,HOLDLOCK)" in statement:
                result[0] = None
            elif statement.startswith("SELECT permission_name FROM"):
                result[0] = [("CONTROL",)] if dangerous else []
            elif "FROM sys.sql_modules m" in statement:
                result[0] = (DEMO.SAFETY_DEFINITION, 1)
            elif statement.startswith("EXEC "):
                result[0] = (1,)
            elif "FROM sys.columns c" in statement:
                # ロールを通じて列単位だけに実効権限が付与されている。
                # 下のオブジェクト単位の確認では引き続き権限なしと報告される。
                result[0] = None
                if column_permission:
                    operation, name, column, effective = column_permission
                    self.assertIn("c.name, N'COLUMN'", statement)
                    strict = " IS NULL OR " in statement
                    allowed = arguments[2:] if strict else arguments[1:]
                    if operation == arguments[0] and (
                        (strict and effective is None)
                        or (effective == 1 and name not in allowed)
                    ):
                        result[0] = (999, 1)
            elif "o.type NOT IN" in statement:
                result[0] = (999,) if unknown_object else None
            elif "sys.objects o" in statement:
                operation = arguments[0]
                name = denied_object[1] if denied_object else None
                audited_kind = (
                    "CROSS APPLY" in statement
                    or "'" + object_type + "'" in statement
                )
                unsafe = (
                    audited_kind
                    and denied_object
                    and operation == denied_object[0]
                    and name not in arguments[1:]
                )
                denied = (
                    unsafe
                    or unknown_permission
                    or (metadata_only and "OUTER APPLY" in statement)
                )
                result[0] = (999,) if denied else None
            elif "SELECT COUNT_BIG(*) FROM sys.tables" in statement:
                result[0] = (len(arguments),)
            elif "sys.schemas s WHERE" in statement:
                result[0] = (999,) if schema_grant else None
            elif "FROM sys.database_principals" in statement:
                if principals_visible:
                    self.assertIn("N'IMPERSONATE')=1", statement)
                result[0] = (999,) if impersonates else None
            elif "N'COLUMN')" in statement:
                permissions = [1] * (len(arguments) // 3)
                if missing_required_column:
                    permissions[0] = None
                result[0] = tuple(permissions)
            elif (
                "FROM sys.triggers" in statement
                or "FROM sys.foreign_keys" in statement
            ):
                result[0] = None
            else:
                result[0] = (1,)

        cursor.execute.side_effect = execute

        def fetchone():
            value = result[0]
            if statements and statements[-1][0].startswith("EXEC "):
                result[0] = None
            return value

        cursor.fetchone.side_effect = fetchone
        cursor.fetchall.side_effect = lambda: result[0]
        cursor.statements = statements
        return cursor

    def test_clr_synonym_external_sequence_and_queue_grants_are_refused(self):
        cases = (
            ("PC", "EXECUTE", "source"),
            ("FN", "EXECUTE", "source"),
            ("FS", "EXECUTE", "source"),
            ("AF", "EXECUTE", "source"),
            ("IF", "SELECT", "demo"),
            ("TF", "SELECT", "demo"),
            ("FT", "SELECT", "demo"),
            ("SN", "INSERT", "demo"),
            ("SN", "UPDATE", "source"),
            ("SN", "DELETE", "control"),
            ("SN", "EXECUTE", "source"),
            ("ET", "SELECT", "demo"),
            ("SO", "UPDATE", "source"),
            ("SQ", "RECEIVE", "source"),
        )
        for kind, operation, role in cases:
            with self.subTest(kind=kind, operation=operation, role=role):
                cursor = self.cursor(
                    object_type=kind,
                    denied_object=(operation, "legacy.unrelated"),
                    role=role,
                )
                self.assertFalse(
                    DEMO.permissions_safe(cursor, role=role, full_audit=True)
                )

    def test_unknown_object_or_missing_permission_metadata_is_refused(self):
        for options in (
            {"unknown_object": True},
            {"unknown_permission": True},
        ):
            with self.subTest(options=options):
                self.assertFalse(
                    DEMO.permissions_safe(
                        self.cursor(**options), full_audit=True
                    )
                )

    def test_query_contract_uses_applicable_permissions_not_guessed_pairs(
        self,
    ):
        """SQLの契約を静的に検証する。"""
        cursor = self.cursor()
        self.assertTrue(DEMO.permissions_safe(cursor, full_audit=True))
        object_queries = [
            statement
            for statement, _ in cursor.statements
            if "sys.objects o" in statement
        ]
        self.assertTrue(object_queries)
        unknown = [
            statement
            for statement in object_queries
            if "o.type NOT IN" in statement
        ]
        self.assertEqual(len(unknown), 1)
        applicable = [
            statement
            for statement in object_queries
            if "OUTER APPLY sys.fn_my_permissions" in statement
        ]
        self.assertEqual(len(applicable), 9)
        for statement in applicable:
            self.assertIn("COALESCE(o.parent_object_id,0)>0", statement)
            self.assertIn("p.permission_name IS NULL", statement)
            self.assertNotIn("HAS_PERMS_BY_NAME", statement)
            for kind in ("PC", "FT", "SN", "ET", "SO", "SQ"):
                self.assertIn("'" + kind + "'", statement)
        self.assertTrue(
            {"C", "D", "F", "PK", "UQ", "EC"}
            <= set(DEMO.NON_GRANTABLE_CHILD_TYPES)
        )
        self.assertFalse(
            {"C", "F", "PK", "UQ", "EC"} & set(DEMO.PERMISSION_OBJECT_TYPES)
        )

    def test_broad_role_unknown_membership_and_dangerous_grant_are_denied(
        self,
    ):
        for options in (
            {"broad": True},
            {"unknown": True},
            {"dangerous": True},
        ):
            with self.subTest(options=options):
                self.assertFalse(DEMO.permissions_safe(self.cursor(**options)))

    def test_installation_marker_update_and_external_access_are_denied(self):
        for operation, target in (
            ("UPDATE", "lh_demo_ctl.installation"),
            ("SELECT", "legacy.unrelated"),
            ("ALTER", "lh_demo_erp.products"),
        ):
            with self.subTest(operation=operation):
                self.assertFalse(
                    DEMO.permissions_safe(
                        self.cursor(denied_object=(operation, target))
                    )
                )

    def test_execute_exception_is_only_the_fixed_checker_for_writers(self):
        for role, target, expected in (
            ("demo", DEMO.SAFETY_CHECKER, True),
            ("control", DEMO.SAFETY_CHECKER, True),
            ("source", DEMO.SAFETY_CHECKER, False),
            ("demo", "legacy.any_procedure", False),
        ):
            with self.subTest(role=role, target=target):
                self.assertEqual(
                    DEMO.permissions_safe(
                        self.cursor(denied_object=("EXECUTE", target)),
                        role=role,
                        full_audit=True,
                    ),
                    expected,
                )

    def test_column_grants_cannot_tamper_with_ownership(self):
        for column in ("ownership_hash", "installation_id", "schema_version"):
            with self.subTest(column=column):
                self.assertFalse(
                    DEMO.permissions_safe(
                        self.cursor(
                            column_permission=(
                                "UPDATE",
                                "lh_demo_ctl.installation",
                                column,
                                1,
                            )
                        )
                    )
                )

    def test_legacy_column_only_grants_are_denied(self):
        for operation in ("SELECT", "UPDATE"):
            with self.subTest(operation=operation):
                self.assertFalse(
                    DEMO.permissions_safe(
                        self.cursor(
                            column_permission=(
                                operation,
                                "legacy.orders",
                                "amount",
                                1,
                            )
                        )
                    )
                )

    def test_fixed_column_permissions_follow_roles(self):
        for role, operation, target, expected in (
            ("demo", "UPDATE", "lh_demo_erp.products", True),
            ("control", "UPDATE", "lh_demo_ctl.batch_watermarks", True),
            ("source", "SELECT", "legacy.orders", True),
            ("source", "UPDATE", "lh_demo_erp.products", False),
            ("control", "UPDATE", "lh_demo_erp.products", False),
        ):
            with self.subTest(role=role, operation=operation):
                self.assertEqual(
                    DEMO.permissions_safe(
                        self.cursor(
                            column_permission=(operation, target, "value", 1)
                        ),
                        role=role,
                        full_audit=True,
                    ),
                    expected,
                )

    def test_unknown_column_permission_fails_closed(self):
        self.assertFalse(
            DEMO.permissions_safe(
                self.cursor(
                    column_permission=(
                        "UPDATE",
                        "lh_demo_erp.products",
                        "name",
                        None,
                    )
                ),
                full_audit=True,
            )
        )

    def test_writer_has_strict_default_even_with_visible_principals(self):
        cursor = self.cursor(principals_visible=True)
        self.assertTrue(DEMO.permissions_safe(cursor))
        statements = [query for query, _ in cursor.statements]
        self.assertTrue(any("OUTER APPLY" in query for query in statements))
        for options in (
            {"metadata_only": True},
            {"unknown_object": True},
            {"column_permission": ("SELECT", "legacy.orders", "amount", None)},
            {"missing_writer": True},
            {"missing_required_column": True},
            {"schema_grant": True},
            {"impersonates": True},
            {"unknown_permission": True},
        ):
            with self.subTest(options=options):
                self.assertFalse(DEMO.permissions_safe(self.cursor(**options)))

    def test_writer_policy_never_reads_unverified_or_writable_source(self):
        cursor = mock.Mock()
        cursor.fetchone.side_effect = [(90,), (0,)]
        self.assertIsNone(DEMO.writer_policy(cursor, "unused"))
        self.assertEqual(cursor.execute.call_count, 2)
        cursor.reset_mock()
        cursor.fetchone.side_effect = [(90,), (1,)]
        cursor.fetchall.side_effect = [
            [(*column, 0, 0, 0) for column in DEMO.POLICY_COLUMNS],
            [(name,) for name in DEMO.POLICY_KEY],
            [("SELECT",), ("UPDATE",)],
        ]
        self.assertIsNone(DEMO.writer_policy(cursor, "unused"))
        self.assertEqual(cursor.execute.call_count, 5)

    def test_writer_policy_rejects_changed_storage_contract(self):
        columns = [(*column, 0, 0, 0) for column in DEMO.POLICY_COLUMNS]
        key = [(name,) for name in DEMO.POLICY_KEY]
        for actual_columns, actual_key in (
            (
                [
                    ("installation_id", "varchar", *columns[0][2:]),
                    *columns[1:],
                ],
                key,
            ),
            ([(*columns[0][:2], 16, *columns[0][3:]), *columns[1:]], key),
            (
                [
                    (*columns[0][:3], "incorrect", *columns[0][4:]),
                    *columns[1:],
                ],
                key,
            ),
            (columns, list(reversed(key))),
        ):
            with self.subTest(columns_equal=actual_columns == columns):
                cursor = mock.Mock()
                cursor.fetchone.side_effect = [(90,), (1,)]
                cursor.fetchall.side_effect = [actual_columns, actual_key]
                self.assertIsNone(DEMO.writer_policy(cursor, "unused"))
                self.assertLessEqual(cursor.execute.call_count, 4)

    def test_writer_policy_exact_target_identity_and_object_level_operation(
        self,
    ):
        sid = b"fixture"
        stamp = DEMO.datetime(2026, 1, 1)
        installation = VALUES["DEMO_SQLSERVER_INSTALLATION_ID"]
        row = (
            installation,
            sid,
            7,
            42,
            11,
            "legacy",
            "items",
            stamp,
            "UPDATE",
            11,
            "legacy",
            "items",
            stamp,
        )
        for altered, level, expected in (
            ([row], 1, {"SELECT": [90], "EXECUTE": [91], "UPDATE": [42]}),
            ([row], 0, None),
            ([row, row], 1, None),
            ([row] * 65, 1, None),
            ([("wrong", *row[1:])], 1, None),
            ([(installation, b"other", *row[2:])], 1, None),
            ([(*row[:8], "EXECUTE", *row[9:])], 1, None),
            ([(installation, sid, 8, *row[3:])], 1, None),
            ([(*row[:-1], None)], 1, None),
            ([(*row[:9], 12, *row[10:])], 1, None),
        ):
            with self.subTest(
                count=len(altered), level=level, expected=expected
            ):
                cursor = mock.Mock()
                cursor.fetchone.side_effect = [
                    (90,),
                    (1,),
                    (0,),
                    (sid, 7),
                    (level,),
                    (level,),
                ]
                cursor.fetchall.side_effect = [
                    [(*column, 0, 0, 0) for column in DEMO.POLICY_COLUMNS],
                    [(name,) for name in DEMO.POLICY_KEY],
                    [("SELECT",)],
                    altered,
                ]
                with mock.patch.object(
                    DEMO, "writer_delegation_checker", return_value=91
                ):
                    self.assertEqual(
                        DEMO.writer_policy(cursor, installation), expected
                    )

    def test_writer_requires_exact_dbo_delegation_checker_and_single_bit(self):
        definition = (
            DEMO.Path(DEMO.__file__)
            .with_name("sqlserver_writer_legacy_delegation.sql")
            .read_text(encoding="utf-8")
            .rstrip("\n")
        )
        for response, expected in (
            ([(91, definition), (1,), None], 91),
            ([(91, definition), (0,)], None),
            ([(91, definition), (1,), (1,)], None),
            ([(91, definition + " ")], None),
            ([None], None),
        ):
            with self.subTest(
                expected=expected, response_length=len(response)
            ):
                cursor = mock.Mock()
                cursor.fetchone.side_effect = response
                self.assertEqual(
                    DEMO.writer_delegation_checker(cursor), expected
                )

    def test_real_mutate_entry_uses_writer_boundary_before_dml(self):
        for legacy_write in (False, True):
            with self.subTest(legacy_write=legacy_write):
                cursor = self.cursor(
                    principals_visible=True,
                    denied_object=("UPDATE", "legacy.item")
                    if legacy_write
                    else None,
                )
                handle = Handle()
                handle.cursor = lambda: cursor
                with mock.patch.object(
                    DEMO,
                    "connection",
                    return_value=injected_connection(handle),
                ):
                    if legacy_write:
                        with self.assertRaises(DEMO.SqlServerError):
                            DEMO.mutate(
                                VALUES, "insert", run_id=RUN, consent=True
                            )
                    else:
                        result = DEMO.mutate(
                            VALUES, "insert", run_id=RUN, consent=True
                        )
                        self.assertEqual(result["changed_rows"], 1)
                written = [
                    stmt
                    for stmt, _ in cursor.statements
                    if stmt.startswith("INSERT INTO")
                ]
                self.assertEqual(handle.commits, 0 if legacy_write else 1)
                self.assertEqual(len(written), 0 if legacy_write else 1)
                self.assertFalse(
                    any(
                        "cdc.change_tables" in stmt
                        for stmt, _ in cursor.statements
                    )
                )

    def test_source_dml_and_control_business_dml_are_denied(self):
        for role in ("source", "control"):
            with self.subTest(role=role):
                self.assertFalse(
                    DEMO.permissions_safe(
                        self.cursor(
                            denied_object=("UPDATE", "lh_demo_erp.products"),
                            role=role,
                        ),
                        role=role,
                        full_audit=True,
                    )
                )
        self.assertTrue(DEMO.permissions_safe(self.cursor()))
        self.assertTrue(
            DEMO.permissions_safe(
                self.cursor(), role="source", full_audit=True
            )
        )
        self.assertTrue(DEMO.permissions_safe(self.cursor(), role="control"))


if __name__ == "__main__":
    unittest.main()

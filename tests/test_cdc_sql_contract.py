"""固定SQL CDC契約の入口と失敗時の無変更境界を確認する。"""

import importlib.util
import json
import sys
import unittest
from contextlib import nullcontext, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "cdc_sql_unit", ROOT / "scripts/cdc_sql_contract.py"
)
SQL = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SQL
SPEC.loader.exec_module(SQL)

DATABASE = "FixtureCDC"
GUID = "a871e04e-baa1-4c40-87ee-7327e4270239"
INSTALL = "1" * 32
INFO = (DATABASE, GUID, 1, 0, 1, 0, 16, 0, 0, 0, 1)


class ContractTests(unittest.TestCase):
    """専用契約は既存データ契約を変更せず、確かな状態だけ進める。"""

    def test_six_tables_match_current_landing_column_order(self):
        current = json.loads(
            (ROOT / "medallion/sources.json").read_text(encoding="utf-8")
        )
        names = (
            "products",
            "work_orders",
            "sales_orders",
            "shipments",
            "equipment_events",
            "equipment_stops",
        )
        self.assertEqual(tuple(SQL.TABLES), names)
        for table, existing in zip(
            SQL.TABLES.values(), current["sources"], strict=True
        ):
            self.assertFalse(table[0].nullable)
            self.assertEqual([col.name for col in table], existing["columns"])
        self.assertEqual(len({SQL.capture_name(name) for name in names}), 6)
        with self.assertRaisesRegex(SQL.ContractError, "INVALID_TABLE"):
            SQL.capture_name("other")

    def test_database_requires_exact_name_guid_roles_and_state(self):
        for replacements, reason in (
            ({0: DATABASE.lower()}, "DATABASE_MISMATCH"),
            ({1: None}, "DATABASE_MISMATCH"),
            ({4: 0}, "DBA_REQUIRED"),
            ({2: 0}, "DATABASE_PREREQUISITES"),
            ({3: 1}, "DATABASE_PREREQUISITES"),
            ({6: 15}, "DATABASE_PREREQUISITES"),
            ({9: 1}, "DATABASE_PREREQUISITES"),
            ({10: 0}, "DEDICATED_TRANSACTION_REQUIRED"),
            ({10: 2}, "DEDICATED_TRANSACTION_REQUIRED"),
        ):
            info = tuple(
                replacements.get(i, value) for i, value in enumerate(INFO)
            )
            with self.subTest(reason=reason, replacements=replacements):
                with patch.object(SQL, "query", return_value=(info,)):
                    with self.assertRaisesRegex(SQL.ContractError, reason):
                        SQL.db_gate(Mock(), DATABASE, GUID)

    def test_owner_marker_digest_rejects_new_generation_and_is_order_stable(
        self,
    ):
        schema = ((5, SQL.SCHEMA, 1),)
        captures = ((1, 2, "one", "stamp"), (3, 4, "two", "stamp"))
        roles = ((5, "a"), (6, "b"))
        artifacts = ((21, "capture_fn", "IF", "date", "definition"),)
        expected = SQL.fingerprint(schema, roles, captures, artifacts)
        self.assertEqual(
            expected,
            SQL.fingerprint(schema, roles[::-1], captures[::-1], artifacts),
        )
        self.assertNotEqual(
            expected, SQL.fingerprint(schema, roles, captures[:-1], artifacts)
        )
        self.assertNotEqual(
            expected, SQL.fingerprint(schema, roles, captures, ())
        )

    def test_cdc_function_signature_never_accepts_missing_digest(self):
        self.assertTrue(SQL.complete_definition_digest(bytes(32)))
        for invalid in (None, b"", bytes(31), bytes(33), "f" * 64):
            with self.subTest(invalid=type(invalid)):
                self.assertFalse(SQL.complete_definition_digest(invalid))

    def test_scope_missing_or_partial_is_not_adopted(self):
        for namespace, expected in (
            (((), (), (), ()), "ABSENT"),
            (((), ((1,),), (), ()), "NAMESPACE_COLLISION"),
            (((), (), ((1,),), ()), "NAMESPACE_COLLISION"),
            (((), (), (), ((1,),)), "NAMESPACE_COLLISION"),
        ):
            with patch.object(SQL, "namespace", return_value=namespace):
                if expected == "ABSENT":
                    self.assertEqual(SQL.inspect(Mock(), INSTALL), expected)
                else:
                    with self.assertRaisesRegex(SQL.ContractError, expected):
                        SQL.inspect(Mock(), INSTALL)

    def test_apply_only_changes_an_empty_and_validated_contract(self):
        for apply, state, outcome in (
            (False, "ABSENT", "ABSENT"),
            (False, "VERIFIED", "VERIFIED"),
            (True, "VERIFIED", "VERIFIED"),
            (True, "ABSENT", "CREATED"),
        ):
            connection = Mock()
            with (
                self.subTest(apply=apply, state=state),
                patch.object(SQL, "db_gate"),
                patch.object(SQL, "query", side_effect=[((0,),), ((1,),)]),
                patch.object(SQL, "jobs", return_value={"same": True}),
                patch.object(SQL, "incumbents", return_value=((1,),)),
                patch.object(SQL, "inspect", side_effect=[state, "VERIFIED"]),
                patch.object(SQL, "create") as create,
            ):
                result = SQL.configure(
                    connection,
                    DATABASE,
                    INSTALL,
                    expected_database_guid=GUID,
                    apply=apply,
                )
                self.assertEqual(result, outcome)
                self.assertEqual(create.call_count, int(outcome == "CREATED"))
                self.assertEqual(
                    connection.commit.call_count, int(outcome == "CREATED")
                )

    def test_jobs_or_postcondition_failure_does_not_commit(self):
        for before, after, reason in (
            ({}, {"drift": True}, "POSTCONDITION_MISMATCH"),
            (
                SQL.ContractError("PREEXISTING_CDC_JOBS_REQUIRED"),
                {},
                "PREEXISTING_CDC_JOBS_REQUIRED",
            ),
        ):
            connection = Mock()
            with (
                patch.object(SQL, "db_gate"),
                patch.object(SQL, "query", side_effect=[((0,),), ((1,),)]),
                patch.object(
                    SQL, "inspect", side_effect=["ABSENT", "VERIFIED"]
                ),
                patch.object(SQL, "create"),
                patch.object(SQL, "jobs", side_effect=[before, after]),
                patch.object(SQL, "incumbents", return_value=((1,),)),
            ):
                with self.assertRaisesRegex(SQL.ContractError, reason):
                    SQL.configure(
                        connection,
                        DATABASE,
                        INSTALL,
                        expected_database_guid=GUID,
                        apply=True,
                    )
            connection.commit.assert_not_called()
            connection.rollback.assert_called_once()

    def test_incumbent_must_be_live_unrelated_and_unchanged(self):
        connection = Mock()
        with (
            patch.object(SQL, "db_gate"),
            patch.object(SQL, "query", side_effect=[((0,),), ()]) as query,
            patch.object(SQL, "jobs", return_value={"same": True}),
            patch.object(SQL, "inspect") as inspect,
            patch.object(SQL, "create") as create,
        ):
            with self.assertRaisesRegex(
                SQL.ContractError, "LIVE_INCUMBENT_CAPTURE_REQUIRED"
            ):
                SQL.configure(
                    connection,
                    DATABASE,
                    INSTALL,
                    expected_database_guid=GUID,
                    apply=True,
                )
            statement = query.call_args.args[1]
            for required in (
                "WITH (HOLDLOCK)",
                "JOIN sys.tables src",
                "JOIN sys.tables ct",
                "src.is_tracked_by_cdc=1",
                "(x.has_drop_pending=0 OR x.has_drop_pending IS NULL)",
                "DATALENGTH(x.start_lsn)=10",
                "ct.name COLLATE Latin1_General_100_BIN2",
                "x.start_lsn=sys.fn_cdc_get_min_lsn(x.capture_instance)",
                "x.start_lsn<>0x00000000000000000000",
                "src.schema_id<>",
                "NOT LIKE",
            ):
                self.assertIn(required, statement)
            inspect.assert_not_called()
            create.assert_not_called()
        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()

        with (
            patch.object(SQL, "db_gate"),
            patch.object(SQL, "query", return_value=((0,),)),
            patch.object(SQL, "jobs", return_value={"same": True}),
            patch.object(SQL, "incumbents", side_effect=[((1,),), ((2,),)]),
            patch.object(SQL, "inspect", side_effect=["ABSENT", "VERIFIED"]),
            patch.object(SQL, "create"),
        ):
            with self.assertRaisesRegex(
                SQL.ContractError, "POSTCONDITION_MISMATCH"
            ):
                SQL.configure(
                    connection,
                    DATABASE,
                    INSTALL,
                    expected_database_guid=GUID,
                    apply=True,
                )
        connection.commit.assert_not_called()

    def test_owned_only_normalizes_null_drop_and_rejects_positive(self):
        cursor = Mock()
        with patch.object(SQL, "query", return_value=()) as call:
            SQL.namespace(cursor)
        sku = call.call_args_list[2].args[1]
        self.assertIn("COALESCE(x.has_drop_pending,0)", sku)
        expected = (
            42,
            96,
            SQL.capture_name("products"),
            SQL.READER_ROLE,
            SQL.pk_name("products"),
            0,
            0,
            "fixture-date",
        )
        self.assertTrue(SQL.owned_capture_matches(expected, 42, "products"))
        self.assertFalse(SQL.owned_capture_matches(expected, 43, "products"))
        self.assertFalse(
            SQL.owned_capture_matches(
                (*expected[:6], 1, *expected[7:]), 42, "products"
            )
        )

    def test_incumbent_rejects_duplicate_ct_or_capture_identity(self):
        one = (1, 2, "first", "stamp", 6, "source", "ct")
        for rows in ((), (one, one), (one, (4, 2, "second"))):
            with (
                self.subTest(count=len(rows)),
                patch.object(SQL, "query", return_value=rows),
                self.assertRaisesRegex(
                    SQL.ContractError, "LIVE_INCUMBENT_CAPTURE_REQUIRED"
                ),
            ):
                SQL.incumbents(Mock())

    def test_cli_uses_explicit_identity_and_redacts_native_errors(self):
        source = Mock()
        source.tls_configuration.return_value = nullcontext()
        values = {
            "CDC_EXPECTED_DATABASE": DATABASE,
            "CDC_EXPECTED_BROKER_GUID": GUID,
            "CDC_INSTALLATION_ID": INSTALL,
            "SOURCE_SQLSERVER_DATABASE": DATABASE,
            "SOURCE_SQLSERVER_PORT": "1433",
            "SOURCE_SQLSERVER_USER": "fixture",
            "SOURCE_SQLSERVER_PASSWORD": "fixture-local-value",
        }
        driver = Mock()
        for error in (None, RuntimeError("fixture-local-value")):
            out = StringIO()
            driver.connect.side_effect = error
            with (
                patch.dict(sys.modules, {"pymssql": driver}),
                patch.object(
                    SQL, "connection_settings", return_value=(source, values)
                ),
                patch.object(SQL, "configure", return_value="ABSENT") as run,
                redirect_stdout(out),
            ):
                self.assertEqual(
                    SQL.main(["--settings", "fixture"]), int(bool(error))
                )
            if error is None:
                self.assertIs(run.call_args.kwargs["apply"], False)
                self.assertEqual(out.getvalue().strip(), "ABSENT")
            else:
                run.assert_not_called()
                self.assertEqual(
                    out.getvalue().strip(), "FAIL: DBA_SESSION_UNAVAILABLE"
                )

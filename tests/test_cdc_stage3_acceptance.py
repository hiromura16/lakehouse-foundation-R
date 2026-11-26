"""段階3 hosted fixtureの観測器を実Docker・実SQLなしで検査する。"""

import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

EVIDENCE_PATH = (
    Path(__file__).parent / "integration/sqlserver/cdc_stage3_evidence.py"
)
SPEC = importlib.util.spec_from_file_location(
    "cdc_stage3_evidence", EVIDENCE_PATH
)
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)


def record(*, op="u", database="FixtureStageThree", commit="0001:0002:0003"):
    """厳密な合成ソースとキーだけのテストレコードを作る。"""
    key = {"product_id": "Stage3Stream"}
    before = key | {"product_name": "original"}
    after = key | {"product_name": "changed"}
    body = {
        "source": {
            "schema": "foundation_r_demo",
            "table": "products",
            "db": database,
            "commit_lsn": commit,
        },
        "op": op,
        "before": before,
        "after": after,
    }
    return f"{json.dumps(key)}\t{json.dumps(body)}\n".encode()


class HostedEvidenceTest(unittest.TestCase):
    """不正な再生や旧JVMをpositiveチェック済みとみなさない。"""

    def test_actual_event_and_tombstone(self):
        """実効キー・before・after・commit と tombstone を別々に読む。"""
        encoded = record() + b'{"product_id":"Stage3Stream"}\tnull\n'
        self.assertEqual(
            evidence.decode_events(encoded, 2),
            [
                ("Stage3Stream", "u", "original", "changed", "0001:0002:0003"),
                ("Stage3Stream", "t", None, None, None),
            ],
        )

    def test_unexpected_source_or_empty_commit_never_accepted(self):
        """schemaが正しくても別DB、stream LSN欠落、件数欠落を拒否する。"""
        for payload in (
            record(database="OtherSynthetic"),
            record(commit=None),
            b"",
        ):
            with (
                self.subTest(payload=payload),
                self.assertRaises(evidence.FixtureError),
            ):
                evidence.decode_events(payload, 1)

    def test_old_jvm_can_not_pass(self):
        """JDK21の必要ビルド未満・別major・EAは拒否する。"""
        check = evidence.patched_runtime
        self.assertFalse(check("21.0.13+100"))
        self.assertFalse(check("21.0.13.1+1-LTS"))
        self.assertFalse(check("21.0.13.1+2-ea"))
        self.assertFalse(check("25.0.4+10"))
        self.assertTrue(check("21.0.13.1+2-LTS"))
        self.assertTrue(check("21.0.14+1"))

    def test_comparing_offset_change_alone_is_not_recovery_proof(self):
        """別トランザクションへ進んだoffsetは期待イベントの確定にならない。"""
        old = [{"offset": {"commit_lsn": "0001:0000:0000"}}]
        wrong = [{"offset": {"commit_lsn": "0001:0001:0000"}}]
        correct = [{"offset": {"commit_lsn": "0001:0002:0000"}}]
        runtime = Mock()
        runtime.offsets.side_effect = [old, wrong, correct]

        def try_all(observe, valid, *, seconds):
            """実時間を使わず、同じ順序の証拠を明示して通す。"""
            self.assertGreater(seconds, 0)
            self.assertFalse(valid(observe()))
            self.assertFalse(valid(observe()))
            result = observe()
            self.assertTrue(valid(result))
            return result

        with patch.object(evidence, "poll", try_all):
            selected = evidence.persisted(
                runtime, old, commit="0001:0002:0000"
            )
        self.assertEqual(selected, correct)

    def test_before_actual_worker_start_keep_broker_and_fence_state(self):
        """未対応のcreate flagや、preflight前のworker開始を再導入しない。"""
        calls = []
        evidence.prepare_and_start_worker(
            lambda *args: calls.append(args),
            lambda: calls.append(("checkpoint",)),
            lambda: calls.append(("private-sql",)),
            "stage3-connect",
        )
        self.assertEqual(
            calls,
            [
                ("checkpoint",),
                (
                    "up",
                    "--no-start",
                    "--no-deps",
                    "--pull",
                    "never",
                    "stage3-connect",
                ),
                ("private-sql",),
                ("start", "stage3-connect"),
            ],
        )
        calls.clear()
        denied = Mock(side_effect=evidence.FixtureError("DENY"))
        with self.assertRaises(evidence.FixtureError):
            evidence.prepare_and_start_worker(
                calls.append, denied, calls.append, "stage3-connect"
            )
        self.assertEqual(calls, [])

    def test_stage_specific_capture_job_destination_is_still_exact(self):
        """段階3が段階2や他のDB・ホストのジョブを起動可能にしない。"""
        stage2 = {"FixtureCdcContract", "FixtureCdcCollision"}
        accepts = evidence.approved_capture_fixture
        for db in stage2:
            self.assertTrue(
                accepts(
                    "localhost", db, stage3=False, stage_two_targets=stage2
                )
            )
            self.assertFalse(
                accepts("localhost", db, stage3=True, stage_two_targets=stage2)
            )
        self.assertTrue(
            accepts(
                "localhost",
                evidence.DATABASE,
                stage3=True,
                stage_two_targets=stage2,
            )
        )
        for host, db, stage3 in (
            ("localhost", evidence.DATABASE, False),
            ("localhost", "DifferentSynthetic", True),
            ("sql.example.invalid", evidence.DATABASE, True),
        ):
            self.assertFalse(
                accepts(host, db, stage3=stage3, stage_two_targets=stage2)
            )

    def test_snapshot_diagnostics_do_not_forward_trace_or_unknown_values(self):
        """認証情報風のtraceも固定分類にし、任意の状態語を公開しない。"""
        private_sample = "not-for-output synthetic field"
        raw = {
            "connector": {"state": private_sample},
            "tasks": [
                {
                    "state": "FAILED",
                    "trace": "The SELECT permission was denied "
                    + private_sample,
                }
            ],
        }
        state = evidence.worker_receipt(raw)
        self.assertEqual(state, ("UNKNOWN", "FAILED", "SQL_PERMISSION"))
        self.assertNotIn(private_sample, " ".join(state))
        self.assertEqual(
            evidence.worker_receipt(
                {"connector": {"state": "RUNNING"}, "tasks": []}
            ),
            ("RUNNING", "UNKNOWN", "NOT_REPORTED"),
        )
        self.assertEqual(evidence.high_receipt(0), "EMPTY")
        self.assertEqual(evidence.high_receipt(7), "PRESENT")
        self.assertEqual(evidence.high_receipt(None), "UNKNOWN")
        self.assertEqual(
            evidence.console_receipt(
                b"TimeoutException " + private_sample.encode()
            ),
            "CDC_READ_TIMEOUT_AFTER_HIGH",
        )
        self.assertEqual(
            evidence.console_receipt(b"unrecognized option"),
            "CDC_READ_CLI_OPTION_REJECTED",
        )

    def test_synthetic_catalog_acl_is_exact_and_owned_fixture_only(self):
        """2種類の定数SQLと、同じloopbackの所有ラベルだけを受理。"""
        site = "foundation-r-sqlserver-" + "a" * 16
        port = "14333"
        binding = "127.0.0.1:" + port
        image = evidence.SQL_FIXTURE_IMAGE
        good = [True, image, site]
        check = evidence.allows_catalog_fixture
        self.assertTrue(check(good, binding, site, port))
        for other, endpoint in (
            ([False, image, site], binding),
            ([True, image, "old-site"], binding),
            ([True, "some-other-image", site], binding),
            (good, "127.0.0.1:1433"),
            (good, ""),
        ):
            self.assertFalse(check(other, endpoint, site, port))
        self.assertEqual(
            evidence.catalog_fixture_grants(),
            (
                "GRANT SELECT ON OBJECT::[cdc].[change_tables] "
                "TO [fixture_cdc_reader]",
                "GRANT SELECT ON OBJECT::[cdc].[captured_columns] "
                "TO [fixture_cdc_reader]",
            ),
        )

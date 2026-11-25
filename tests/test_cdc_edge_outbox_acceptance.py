"""実Dockerを使わず、再送を支える試験オラクルを検査する。"""

import io
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

# 公開ソース用のSDK無し環境でも既存テストの例外形状を使う。
from test_edge_outbox import outbox

from scripts import cdc_edge_outbox_acceptance as scenario
from scripts.influx_local_acceptance import AcceptanceError


class CombinedOracleTest(unittest.TestCase):
    """ファイルと待合せは実行し、Core側の観測だけを合成する。"""

    def setUp(self):
        temp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(temp)
        self.output = self.enterContext(redirect_stdout(io.StringIO()))
        # 受入のどの経路からも本物のクライアントを生成させない。
        self.enterContext(
            patch.object(outbox.r2, "r2_client", side_effect=AssertionError)
        )
        self.advance = Mock(side_effect=self.observe)
        self.boundary = Mock()

    @staticmethod
    def observe(phase, offset):
        """実行済みの最終読取りだけは直前と同じ世代を返す。"""
        return offset + 1, str(offset + 2)

    def test_exact_attempts_and_both_core_progress_gates(self):
        scenario.run_scenario(self.root, self.advance, self.boundary)
        self.assertEqual(
            [
                ("storage-baseline", 8),
                ("continuity-insert", 9),
                ("continuity-update", 10),
                ("continuity-delete", 11),
                ("continuity-confirm", 11),
            ],
            [call.args for call in self.advance.call_args_list],
        )
        self.assertEqual(self.boundary.call_count, 3)
        rows = scenario.ledger(self.root / "queue.sqlite")
        self.assertEqual(
            [row[1:3] for row in rows],
            [("acked", 1), ("acked", 2), ("acked", 1)],
        )
        self.assertEqual(
            len(
                scenario.Destination(
                    self.root / "destination.sqlite"
                ).objects()
            ),
            9,
        )

    def test_outage_that_never_fails_is_not_a_success(self):
        with (
            patch.object(scenario.Destination, "ensure_available"),
            self.assertRaises(AcceptanceError),
        ):
            scenario.run_scenario(self.root, self.advance, self.boundary)
        self.assertEqual(
            self.output.getvalue().strip(), "FAIL: EDGE_DESTINATION_DOWN"
        )
        self.advance.assert_called_once_with("storage-baseline", 8)

    def test_stalled_raw_or_silver_prevents_replay_success_report(self):
        self.advance.side_effect = [(1, "2"), (2, "2")]
        with self.assertRaises(AcceptanceError):
            scenario.run_scenario(self.root, self.advance, self.boundary)
        self.assertEqual(
            self.output.getvalue().strip(), "FAIL: EDGE_CORE_WHILE_DOWN"
        )

    def test_primary_error_releases_and_joins_replay_worker(self):
        finished = []
        destination = scenario.Destination(self.root / "destination.sqlite")
        original = RuntimeError("synthetic callback error")

        def deliver(client):
            """送信待合せ後にだけ配送を完了する。"""
            client.before_put("first")
            finished.append(True)
            return "acked"

        def interrupted():
            """再送待合せ中に主処理を中断する。"""
            raise original

        with self.assertRaises(RuntimeError) as failure:
            scenario.replay_while(deliver, destination, "first", interrupted)
        self.assertIs(failure.exception, original)
        self.assertEqual(finished, [True])
        self.assertIsNone(destination.before_put)

    def test_wrong_authoritative_bytes_never_publish_manifest(self):
        read = scenario.Destination.get_object

        def mismatch(client, **params):
            """永続格納されても、照合時のバイトが一致しない宛先。"""
            response = read(client, **params)
            response["Body"].close()
            return {"Body": io.BytesIO(b"wrong")}

        with (
            patch.object(scenario.Destination, "get_object", mismatch),
            self.assertRaises(AcceptanceError),
        ):
            scenario.run_scenario(self.root, self.advance, self.boundary)
        self.assertEqual(self.output.getvalue().strip(), "FAIL: EDGE_WARM")
        rows = scenario.ledger(self.root / "queue.sqlite")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1:3], ("quarantined", 1))
        self.assertEqual(rows[0][4:], (None, None))
        objects = scenario.Destination(
            self.root / "destination.sqlite"
        ).objects()
        self.assertEqual(len(objects), 1)
        self.assertTrue(objects[0][0].endswith("/result.csv"))

    def test_readback_after_manifest_fails_trace(self):
        with self.assertRaises(AcceptanceError):
            scenario.require_trace(
                [
                    ("put", "a"),
                    ("put", "manifest"),
                    ("get", "a"),
                    ("get", "manifest"),
                ],
                [("a", b"1"), ("manifest", b"2")],
            )

    def test_non_hosted_entry_does_not_enter_fixture(self):
        with (
            patch.dict(scenario.os.environ, {}, clear=True),
            patch.object(scenario.closed, "exercise") as exercise,
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(
                scenario.closed.main(
                    fixture=scenario.edge_case, overlay=scenario.OVERLAY
                ),
                1,
            )
        exercise.assert_not_called()

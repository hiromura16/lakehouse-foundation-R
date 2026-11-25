"""障害を再試行や空の成功と取り違えないよう、制御順序を検査する。"""

import errno
import json
import runpy
import subprocess
import sys
import unittest
from unittest.mock import Mock, call, patch

from scripts import cdc_storage_failure as outage
from scripts.influx_local_acceptance import AcceptanceError

IDS = {
    "storage": "storage-id",
    "native-cdc": "consumer-id",
    "test-broker": "b",
}
ORIGINAL = (IDS, {})
PROJECT = "foundation-r-cdc-local-ci-0123456789abcdef"


def snapshot(raw, release, cursor):
    """観測された独立世代とbrokerの位置を合成する。"""
    return json.dumps(
        {
            "raw_snapshot_id": raw,
            "release_snapshot_id": release,
            "broker_next_offset": cursor,
        }
    )


def raw_proof(snapshot_id=1, sha="a" * 64):
    """合成の認証付きRaw読み戻し結果。"""
    return json.dumps({"snapshot": snapshot_id, "sha256": sha})


def failure(kind="OSError", frames=None):
    """実ログを読まず、安全な固定診断に相当する構造を組み立てる。"""
    return json.dumps(
        {
            "event": "native.operation_failed",
            "error_type": kind,
            "locations": frames
            or [
                "consumer_runtime.py:272",
                "consumer.py:104",
                "raw_writer.py:9",
            ],
        }
    )


class StorageFailureTest(unittest.TestCase):
    """停止から復旧までの順序と中断条件。"""

    def test_only_raw_io_that_matches_connectivity_error_is_accepted(self):
        match = {"raw_writer.py:9"}
        self.assertTrue(outage.relevant_failure(failure(), match))
        self.assertFalse(outage.relevant_failure(failure("ValueError"), match))
        self.assertFalse(
            outage.relevant_failure(failure(frames=["raw_writer.py:9"]), match)
        )
        self.assertFalse(
            outage.relevant_failure(failure(), {"raw_writer.py:92"})
        )
        self.assertFalse(
            outage.relevant_failure(failure() + "\n" + failure(), match)
        )
        self.assertFalse(
            outage.relevant_failure(
                failure(
                    frames=[
                        "raw_writer.py:9",
                        "consumer.py:104",
                        "consumer_runtime.py:272",
                    ]
                ),
                match,
            )
        )

    def test_real_source_locations_are_only_scan_and_append_expressions(self):
        locations = outage.raw_io_locations()
        source = (outage.closed.ROOT / "infra/cdc/raw_writer.py").read_text()
        lines = source.splitlines()
        self.assertEqual(
            {
                lines[int(location.split(":")[1]) - 1].strip()
                for location in locations
            },
            {".to_arrow()", "table.append("},
        )

    def scenario(self, *, log, unpublished_same=True, raw_proof_same=True):
        """コンテナは一切起動せず、呼出の列だけを記録する。"""
        events = []
        status = {
            "State": {
                "Running": True,
                "StartedAt": "old",
                "Status": "running",
            },
            "HostConfig": {"RestartPolicy": {"Name": "no"}},
            "RestartCount": 0,
        }
        broker = {"State": {"StartedAt": "broker-initial"}}
        stored = {"up": True}
        before = snapshot(1, "1", 9)
        after = snapshot(2, "2", 10)
        restored = before if unpublished_same else snapshot(2, "1", 9)

        def docker(*args, **_):
            events.append(args)
            if args[0] == "stop":
                stored["up"] = False
            elif args[0] == "logs":
                return log
            elif args[:2] == ("start", "storage-id"):
                stored["up"] = True
            elif args[:2] == ("start", "consumer-id"):
                status["State"].update(Running=True, StartedAt="new")

        def compose(*args, **_):
            phase = args[5] if len(args) > 5 else args[-1]
            events.append((phase,))
            if phase == "storage-raw-proof":
                self.assertEqual(args[6:], ("--raw-snapshot", "1"))
                if stored["up"] and events.count((phase,)) == 2:
                    return raw_proof(sha=("a" if raw_proof_same else "b") * 64)
                return raw_proof()
            if phase == "-":
                return "HTTP_403" if stored["up"] else "TCP_UNAVAILABLE"
            if phase == "storage-baseline":
                return before
            if phase == "storage-restored":
                return restored
            if phase == "storage-publish":
                status["State"].update(
                    Status="exited", ExitCode=1, Running=False, OOMKilled=False
                )
                return "DELIVERED"
            if phase == "storage-old-cursor":
                return '{"broker_next_offset": 9}'
            if phase == "storage-recovered":
                return after
            return "seed"

        def inspect(_docker, _kind, identity):
            if identity == "storage-id":
                return {"State": {"Health": {"Status": "healthy"}}}
            return broker if identity == "b" else status

        return events, docker, compose, inspect

    def test_real_fixture_plan_no_resend_and_positions_preserved(self):
        events, docker, compose, inspect = self.scenario(log=failure())
        checkpoint = Mock()
        with (
            patch.object(outage, "inspect", new=inspect),
            patch.object(
                outage, "raw_io_locations", return_value={"raw_writer.py:9"}
            ),
            patch.object(
                outage, "storage_volume", return_value=("volume", "old")
            ),
            patch.object(
                outage.closed, "runtime_boundary", return_value=ORIGINAL
            ),
        ):
            outage.storage_case(
                docker, compose, PROJECT, ORIGINAL, checkpoint, ""
            )
        stop = events.index(("stop", "--time", "20", "storage-id"))
        publish = events.index(("storage-publish",))
        restore = events.index(("start", "storage-id"))
        resume = events.index(("start", "consumer-id"))
        self.assertLess(stop, publish)
        self.assertLess(publish, restore)
        self.assertLess(restore, resume)
        self.assertEqual(
            events[restore:resume],
            [
                ("start", "storage-id"),
                ("-",),
                ("storage-raw-proof",),
                ("storage-restored",),
            ],
        )
        self.assertEqual(events.count(("storage-publish",)), 1)
        self.assertEqual(
            checkpoint.call_args_list,
            [call(8), call(8), call(8), call(9), call(9)],
        )

    def test_fixture_keeps_restored_event_pending_on_same_broker(self):
        imports = {"confluent_kafka": Mock(), "native": Mock()}
        source = outage.closed.FIXTURES / "cdc_acceptance.py"
        with patch.dict(sys.modules, imports):
            run_storage = runpy.run_path(str(source))["run_storage"]
        fixture = run_storage.__globals__
        core = fixture["api"].core_module.return_value
        for phase, total, high, continuity in (
            ("storage-baseline", 9, 9, None),
            ("storage-restored", 9, 10, None),
            ("storage-recovered", 10, 10, 1),
        ):
            with self.subTest(phase=phase):
                check = Mock(return_value={"raw_snapshot_id": 1})
                cursor = Mock(return_value=total)
                with (
                    patch.dict(
                        fixture, {"check": check, "broker_committed": cursor}
                    ),
                    patch("builtins.print") as printed,
                ):
                    run_storage(phase)
                product_names = [fixture["PRIOR_PRODUCT"]]
                if continuity is not None:
                    product_names.append(
                        fixture["CONTINUITY"]["continuity-insert"][2]
                    )
                labels = {}
                if phase == "storage-restored":
                    label = check.call_args.kwargs["note_stage"]
                    self.assertTrue(callable(label))
                    labels = {"note_stage": label}
                check.assert_called_once_with(
                    core,
                    total,
                    product_names,
                    continuity=continuity,
                    snapshots=True,
                    **labels,
                )
                cursor.assert_called_once_with(total, high=high, **labels)
                self.assertEqual(
                    json.loads(printed.call_args.args[0]),
                    {"raw_snapshot_id": 1, "broker_next_offset": total},
                )

    def test_restored_child_failures_keep_nonzero_and_only_fixed_markers(self):
        source = outage.closed.FIXTURES / "cdc_acceptance.py"
        with patch.dict(
            sys.modules, {"confluent_kafka": Mock(), "native": Mock()}
        ):
            run_storage = runpy.run_path(str(source))["run_storage"]
        codepaths = run_storage.__globals__
        for where, stage in (
            ("check", "RAW_READ"),
            ("check", "RAW_COUNT"),
            ("check", "SILVER_READ"),
            ("broker_committed", "BROKER_COMMITTED"),
            ("broker_committed", "BROKER_WATERMARK"),
        ):
            with self.subTest(stage=stage):

                def fail(*_args, note_stage, **_kwargs):
                    """実値が出ないことを確認する固定の合成障害。"""
                    note_stage(stage)
                    raise ValueError("redacted fixture detail")

                with (
                    patch.dict(
                        codepaths,
                        {
                            "check": Mock(return_value={"raw_snapshot_id": 1}),
                            where: fail,
                        },
                    ),
                    patch("builtins.print") as output,
                    self.assertRaises(SystemExit) as failed,
                ):
                    run_storage("storage-restored")
                self.assertEqual(failed.exception.code, 1)
                output.assert_called_once_with(
                    "CDC_RESTORE_" + stage, flush=True
                )

    def test_exact_observers_report_io_vs_high_mismatch(self):
        source = outage.closed.FIXTURES / "cdc_acceptance.py"
        with patch.dict(
            sys.modules, {"confluent_kafka": Mock(), "native": Mock()}
        ):
            fixture = runpy.run_path(str(source))["run_storage"].__globals__
        core = Mock()
        table = core.catalog.return_value.load_table.return_value
        table.scan.side_effect = OSError("synthetic storage not ready")
        seen = []
        with self.assertRaises(OSError):
            fixture["check"](core, 9, [], note_stage=seen.append)
        self.assertEqual(seen, ["CATALOG", "RAW_READ"])

        broker = fixture["Consumer"].return_value
        broker.committed.return_value = [Mock(error=None, offset=9)]
        broker.get_watermark_offsets.return_value = (0, 9)
        seen.clear()
        with self.assertRaises(AssertionError):
            fixture["broker_committed"](9, high=10, note_stage=seen.append)
        self.assertEqual(seen, ["BROKER_COMMITTED", "BROKER_WATERMARK"])
        broker.close.assert_called_once_with()
        broker.get_watermark_offsets.assert_called_once_with(
            fixture["TopicPartition"].return_value, timeout=3, cached=False
        )

    def test_restored_host_whitelists_exact_token_and_checks_both_snapshots(
        self,
    ):
        prior = outage.evidence(snapshot(1, "1", 9), 9)
        process_error = subprocess.CalledProcessError
        command = ["synthetic"]
        cases = (
            (snapshot(2, "1", 9), "RAW_SNAPSHOT_CHANGED"),
            (snapshot(1, "2", 9), "RELEASE_SNAPSHOT_CHANGED"),
            (snapshot(1, "1", 10), "EVIDENCE_INVALID"),
            (
                process_error(1, command, output="CDC_RESTORE_RAW_READ"),
                "RAW_READ",
            ),
            (
                process_error(
                    1, command, output="CDC_RESTORE_BROKER_WATERMARK"
                ),
                "BROKER_WATERMARK",
            ),
            (
                process_error(1, command, output="CDC_RESTORE_RAW_READ extra"),
                "OBSERVATION_PROCESS",
            ),
            (
                process_error(2, command, output="CDC_RESTORE_RAW_READ"),
                "OBSERVATION_PROCESS",
            ),
            (subprocess.TimeoutExpired(command, 130), "OBSERVATION_TIMEOUT"),
        )
        for result, expected in cases:
            with self.subTest(expected=expected):
                fixture = (
                    Mock(side_effect=result)
                    if isinstance(result, Exception)
                    else Mock(return_value=result)
                )
                with (
                    patch("builtins.print") as output,
                    self.assertRaises(AcceptanceError),
                ):
                    outage.restored_evidence(fixture, prior)
                output.assert_called_once_with(
                    "FAIL: STORAGE_RESTORE_" + expected, flush=True
                )
                fixture.assert_called_once_with("storage-restored")

    def test_authenticated_probe_retries_only_transport_and_is_bounded(self):
        command = ["synthetic"]
        transient = subprocess.CalledProcessError(
            1, command, output="CDC_RAW_PROOF_TRANSPORT"
        )
        read = Mock(side_effect=[transient, raw_proof()])
        with patch.object(outage.time, "sleep") as wait:
            self.assertEqual(
                outage.read_raw_proof(read, 1, attempts=4),
                json.loads(raw_proof()),
            )
        self.assertEqual(read.call_count, 2)
        read.assert_called_with(
            "storage-raw-proof", "--raw-snapshot", "1", timeout=20
        )
        wait.assert_called_once_with(1)

        for outcome, count in (
            (transient, 4),
            (
                subprocess.CalledProcessError(
                    1, command, output="CDC_RAW_PROOF_READ"
                ),
                1,
            ),
            (
                subprocess.CalledProcessError(
                    1, command, output="CDC_RAW_PROOF_CONTENT"
                ),
                1,
            ),
            (
                subprocess.CalledProcessError(
                    1, command, output="CDC_RAW_PROOF_SNAPSHOT"
                ),
                1,
            ),
            (
                subprocess.CalledProcessError(
                    2, command, output="CDC_RAW_PROOF_TRANSPORT"
                ),
                1,
            ),
            (
                subprocess.CalledProcessError(
                    1, command, output="CDC_RAW_PROOF_TRANSPORT hint"
                ),
                1,
            ),
            (subprocess.TimeoutExpired(command, 20), 1),
            (raw_proof(snapshot_id=2), 1),
            (raw_proof(sha="not-a-digest"), 1),
        ):
            read = (
                Mock(side_effect=outcome)
                if isinstance(outcome, Exception)
                else Mock(return_value=outcome)
            )
            with (
                patch.object(outage.time, "sleep") as wait,
                patch("builtins.print"),
                self.assertRaises(AcceptanceError),
            ):
                outage.read_raw_proof(read, 1, attempts=4)
            self.assertEqual(read.call_count, count)
            self.assertEqual(wait.call_count, count - 1)

    def test_raw_proof_uses_exact_snapshot_and_sanitizes_refusals(self):
        source = outage.closed.FIXTURES / "cdc_acceptance.py"
        with patch.dict(
            sys.modules, {"confluent_kafka": Mock(), "native": Mock()}
        ):
            proof = runpy.run_path(str(source))["raw_proof"]
        fixture = proof.__globals__
        core = Mock()
        table = core.catalog.return_value.load_table.return_value
        fixture["engine"].current_id.return_value = 44
        result = table.scan.return_value.to_arrow.return_value
        result.schema = []
        result.to_pylist.return_value = [
            {"event_id": str(n), "offset": str(n)} for n in range(9)
        ]
        with patch("builtins.print") as logged:
            proof(core, 44)
        first = json.loads(logged.call_args.args[0])
        self.assertEqual(first["snapshot"], 44)
        core.catalog.return_value.load_table.assert_called_once_with(
            "cdc_events.events"
        )
        table.scan.assert_called_once_with(snapshot_id=44)
        result.schema = [Mock(name="synthetic field", nullable=True)]
        result.schema[0].name = "added-field"
        with patch("builtins.print") as logged:
            proof(core, 44)
        self.assertNotEqual(
            first["sha256"], json.loads(logged.call_args.args[0])["sha256"]
        )
        result.to_pylist.return_value.pop()
        with patch("builtins.print") as logged, self.assertRaises(SystemExit):
            proof(core, 44)
        logged.assert_called_once_with("CDC_RAW_PROOF_CONTENT", flush=True)

        for error, expected in (
            (OSError(errno.ECONNRESET, "fixture detail"), "TRANSPORT"),
            (
                OSError(
                    "AWS Error NETWORK_CONNECTION during GetObject "
                    "operation: curlCode: 7, example"
                ),
                "TRANSPORT",
            ),
            (
                OSError(
                    "AWS Error NETWORK_CONNECTION during GetObject "
                    "operation: curlCode: 60, certificate"
                ),
                "READ",
            ),
            (PermissionError(errno.EACCES, "fixture detail"), "READ"),
            (FileNotFoundError(errno.ENOENT, "object absent"), "READ"),
            (
                OSError(
                    "AWS Error ACCESS_DENIED during HeadObject operation: 403"
                ),
                "READ",
            ),
            (ValueError("schema detail"), "READ"),
        ):
            table.scan.return_value.to_arrow.side_effect = error
            with (
                patch("builtins.print") as logged,
                self.assertRaises(SystemExit),
            ):
                proof(core, 44)
            logged.assert_called_once_with(
                "CDC_RAW_PROOF_" + expected, flush=True
            )
        table.scan.reset_mock()
        with patch("builtins.print") as logged, self.assertRaises(SystemExit):
            proof(core, 45)
        logged.assert_called_once_with("CDC_RAW_PROOF_SNAPSHOT", flush=True)
        table.scan.assert_not_called()

    def test_uncertain_failure_or_changed_snapshot_never_resumes(self):
        cases = (
            (failure("ValueError"), True, True),
            (failure(), False, True),
            (failure(), True, False),
        )
        for log, same, raw_same in cases:
            with self.subTest(same=same, raw_same=raw_same):
                events, docker, compose, inspect = self.scenario(
                    log=log, unpublished_same=same, raw_proof_same=raw_same
                )
                with (
                    patch.object(outage, "inspect", new=inspect),
                    patch.object(
                        outage,
                        "raw_io_locations",
                        return_value={"raw_writer.py:9"},
                    ),
                    patch.object(
                        outage,
                        "storage_volume",
                        return_value=("volume", "old"),
                    ),
                    patch.object(
                        outage.closed,
                        "runtime_boundary",
                        return_value=ORIGINAL,
                    ),
                    patch("builtins.print"),
                    self.assertRaises(AcceptanceError),
                ):
                    outage.storage_case(
                        docker, compose, PROJECT, ORIGINAL, Mock(), ""
                    )
                self.assertNotIn(("start", "consumer-id"), events)

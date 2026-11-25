"""障害を再試行や空の成功と取り違えないよう、制御順序を検査する。"""

import json
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

    def scenario(self, *, log, unpublished_same=True):
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
        baselines = iter(
            (before, before if unpublished_same else (snapshot(2, "1", 9)))
        )

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
            phase = args[-1]
            events.append((phase,))
            if phase == "-":
                return "HTTP_403" if stored["up"] else "TCP_UNAVAILABLE"
            if phase == "storage-baseline":
                return next(baselines)
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
                ("storage-baseline",),
            ],
        )
        self.assertEqual(events.count(("storage-publish",)), 1)
        self.assertEqual(
            checkpoint.call_args_list,
            [call(8), call(8), call(8), call(9), call(9)],
        )

    def test_uncertain_failure_or_changed_snapshot_never_resumes(self):
        for log, same in ((failure("ValueError"), True), (failure(), False)):
            with self.subTest(same=same):
                events, docker, compose, inspect = self.scenario(
                    log=log, unpublished_same=same
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

"""consumerだけを操作し、停止中の入力と完了を混同しない試験。"""

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from scripts import cdc_consumer_pause as pause
from scripts.influx_local_acceptance import AcceptanceError

IDS = {"native-cdc": "consumer", "test-broker": "broker", "storage": "storage"}
RESOURCES = (IDS, {"writer": "synthetic-network"})
PROJECT = "foundation-r-cdc-local-ci-0123456789abcdef"


def observation(raw=1, release="1", broker=9):
    """世代とbrokerの位置は独立の観測値。"""
    return json.dumps(
        {
            "raw_snapshot_id": raw,
            "release_snapshot_id": release,
            "broker_next_offset": broker,
        }
    )


class ConsumerPauseTest(unittest.TestCase):
    """Dockerの代わりに資源状態を固定し、制御順序を照合する。"""

    def setUp(self):
        self.events = []
        self.paused = False
        self.started = "consumer-initial"
        self.epochs = {
            "broker": "broker-initial",
            "storage": "storage-initial",
        }
        self.bad_health = None
        self.restart_peer = None
        self.on_pending = observation()
        self.on_repeat = observation(2, "2", 10)
        self.output = self.enterContext(redirect_stdout(io.StringIO()))
        self.enterContext(patch.object(pause, "inspect", new=self.inspect))
        self.enterContext(
            patch.object(
                pause, "storage_volume", return_value=("volume", "initial")
            )
        )
        self.enterContext(
            patch.object(
                pause.closed, "runtime_boundary", new=self.runtime_boundary
            )
        )

    def docker(self, *args, **_kwargs):
        """コマンド全列を採取し、対象がconsumerの時だけ状態を変える。"""
        self.events.append(args)
        if args == ("stop", "--time", "20", IDS["native-cdc"]):
            self.paused = True
        if args == ("start", IDS["native-cdc"]):
            self.paused = False
            self.started = "consumer-resumed"

    def compose(self, *args, **kwargs):
        """固定phaseを既存のデータ観測に相当する結果へ対応づける。"""
        phase = args[-1]
        self.assertEqual(args[:5], ("exec", "-T", "lakehouse", "python", "-"))
        self.assertEqual(kwargs["input_text"], "synthetic code")
        self.events.append((phase,))
        if phase in ("storage-baseline", "storage-restored"):
            self.assertEqual(self.paused, phase == "storage-restored")
            return (
                self.on_pending
                if phase == "storage-restored"
                else observation()
            )
        if phase == "storage-publish":
            self.assertTrue(self.paused)
            if self.restart_peer is not None:
                self.epochs[self.restart_peer] = "unexpected-restart"
            return "DELIVERED"
        if phase == "storage-recovered":
            self.assertFalse(self.paused)
            return (
                observation(2, "2", 10)
                if self.events.count((phase,)) == 1
                else self.on_repeat
            )
        return "seed"

    def inspect(self, _docker, kind, container):
        """停止中もKafkaとstorageの実行状態は独立に失敗できる。"""
        self.assertEqual(kind, "container")
        if container != IDS["native-cdc"]:
            return {
                "State": {
                    "Running": True,
                    "Status": "running",
                    "Health": {
                        "Status": (
                            "unhealthy"
                            if self.paused and container == self.bad_health
                            else "healthy"
                        )
                    },
                    "StartedAt": self.epochs[container],
                }
            }
        return {
            "HostConfig": {"RestartPolicy": {"Name": "no"}},
            "RestartCount": 0,
            "State": {
                "Running": not self.paused,
                "Status": "exited" if self.paused else "running",
                "OOMKilled": False,
                "StartedAt": self.started,
            },
        }

    def runtime_boundary(self, _docker, _compose, project, *, inactive):
        """停止許可はconsumerだけ。既存の資源境界チェックを通る。"""
        self.assertEqual(project, PROJECT)
        self.assertEqual(inactive, {"native-cdc"} if self.paused else set())
        return RESOURCES

    def checkpoint(self, offset):
        """両カーソルのうちSQLiteを別の観測として記録する。"""
        self.events.append(("checkpoint", offset))

    def case(self):
        """実ネットワークに出ず、受入入口のオーケストレーションを使う。"""
        pause.pause_case(
            self.docker,
            self.compose,
            PROJECT,
            RESOURCES,
            self.checkpoint,
            "synthetic code",
        )

    def test_only_consumer_restarts_and_input_is_never_resent(self):
        self.case()
        self.assertEqual(
            self.events,
            [
                ("first",),
                ("delete",),
                ("resume",),
                ("storage-baseline",),
                ("checkpoint", 8),
                ("stop", "--time", "20", IDS["native-cdc"]),
                ("storage-publish",),
                ("storage-restored",),
                ("checkpoint", 8),
                ("start", IDS["native-cdc"]),
                ("storage-recovered",),
                ("checkpoint", 9),
                ("storage-recovered",),
                ("checkpoint", 9),
            ],
        )

    def test_unhealthy_peer_cannot_be_mistaken_for_consumer_only_pause(self):
        self.bad_health = "storage"
        with self.assertRaises(AcceptanceError):
            self.case()
        self.assertNotIn(("storage-publish",), self.events)
        self.assertNotIn(("start", IDS["native-cdc"]), self.events)
        self.assertIn(
            "CONSUMER_PAUSE_STOP_OWNED_CONSUMER", self.output.getvalue()
        )

    def test_healthy_broker_with_different_start_epoch_blocks_resume(self):
        self.restart_peer = "broker"
        with self.assertRaises(AcceptanceError):
            self.case()
        self.assertIn(("storage-publish",), self.events)
        self.assertNotIn(("start", IDS["native-cdc"]), self.events)
        self.assertIn(
            "CONSUMER_PAUSE_PAUSED_PERSISTENCE", self.output.getvalue()
        )

    def test_paused_snapshot_or_commit_change_blocks_resume(self):
        for snapshot in (observation(raw=2), observation(broker=10)):
            with self.subTest(snapshot=snapshot):
                self.on_pending = snapshot
                self.events.clear()
                self.paused = False
                with self.assertRaises(AcceptanceError):
                    self.case()
                self.assertNotIn(("start", IDS["native-cdc"]), self.events)

    def test_second_read_must_observe_same_confirmed_generation(self):
        self.on_repeat = observation(3, "2", 10)
        with self.assertRaises(AcceptanceError):
            self.case()
        self.assertEqual(self.events.count(("storage-publish",)), 1)
        self.assertIn(
            "CONSUMER_PAUSE_READ_ONLY_CONFIRM", self.output.getvalue()
        )

    def test_non_hosted_runner_never_enters_fixture(self):
        with (
            patch.dict(pause.closed.os.environ, {}, clear=True),
            patch.object(pause.closed, "exercise") as exercise,
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(
                pause.closed.main(
                    fixture=pause.pause_case, overlay=pause.OVERLAY
                ),
                1,
            )
        exercise.assert_not_called()

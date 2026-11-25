"""ネットワークなしで観測値と台帳の進み方を検証する。"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from test_edge_outbox import Destination, bundle, outbox

from native.edge import cloud_status as status


def known_channels(*, when=100, failed=None):
    """名前付きの完全な観測を作る。"""
    return tuple(
        status.Observation(
            channel,
            status.Result.FAILED
            if channel == failed
            else status.Result.HEALTHY,
            when,
        )
        for channel in status.Channel
    )


def summary(*, records=0, pending=0, acked=0, **overrides):
    """Outboxが返す公開安全な合成集計を用意する。"""
    return {
        "records": records,
        "retained": pending,
        "retained_bytes": pending * 100,
        "pending": pending,
        "in_flight": 0,
        "confirmed_without_ack": 0,
        "scheduled_retries": 0,
        "quarantined": 0,
        "acked": acked,
        "attempts": acked,
        "oldest_unacked_at": 75 if pending else None,
        "last_attempt_at": 80 if acked else None,
        "last_confirmed_at": 80 if acked else None,
        "last_ack_at": 80 if acked else None,
        **overrides,
    }


class ExternalStatusTest(unittest.TestCase):
    """外部依存の失敗・未観測がCoreの状態に混ざらない。"""

    def test_one_external_failure_impacts_only_named_group(self):
        for channel in status.Channel:
            with self.subTest(channel=channel):
                report = status.cloud_status(
                    True,
                    known_channels(failed=channel),
                    now=105,
                    max_age_seconds=5,
                )
                self.assertIs(report["local_core_ready"], True)
                self.assertEqual(report["cloud_status"], "cloud-degraded")
                self.assertEqual(report["channels"][channel.value], "failed")
                group = next(
                    name
                    for name, members in status.PATHS.items()
                    if channel.value in members
                )
                self.assertEqual(
                    report["paths"],
                    {
                        name: "cloud-degraded" if name == group else "healthy"
                        for name in status.PATHS
                    },
                )
        report = status.cloud_status(
            False, known_channels(), now=105, max_age_seconds=5
        )
        self.assertEqual(report["cloud_status"], "healthy")
        self.assertIs(report["local_core_ready"], False)

    def test_initial_unobserved_stale_and_partial_are_not_healthy(self):
        report = status.cloud_status(True, (), now=100, max_age_seconds=5)
        self.assertEqual(report["cloud_status"], "unknown")
        self.assertEqual(set(report["channels"].values()), {"unknown"})
        old_failure = status.Observation(
            status.Channel.R2, status.Result.FAILED, 94
        )
        healthy_dns = status.Observation(
            status.Channel.DNS, status.Result.HEALTHY, 95
        )
        report = status.cloud_status(
            True, (old_failure, healthy_dns), now=100, max_age_seconds=5
        )
        self.assertEqual(report["channels"]["r2"], "stale")
        self.assertEqual(report["channels"]["dns"], "healthy")
        self.assertEqual(set(report["paths"].values()), {"unknown"})
        self.assertEqual(report["cloud_status"], "unknown")
        observed = status.cloud_status(
            True,
            (
                status.Observation(
                    status.Channel.ACCESS, status.Result.FAILED, 100
                ),
            ),
            now=100,
            max_age_seconds=5,
        )
        self.assertEqual(observed["paths"]["human_access"], "cloud-degraded")
        self.assertEqual(observed["paths"]["delivery"], "unknown")

    def test_invalid_probes_and_out_of_range_times_are_rejected(self):
        valid = known_channels()
        for core, probes, now, limit in (
            (1, valid, 100, 1),
            (True, list(valid), 100, 1),
            (True, valid, False, 1),
            (True, valid, 100, 0),
            (True, valid, 100, 86401),
            (True, valid, 99, 1),
            (True, (*valid, valid[0]), 100, 1),
            (
                True,
                (status.Observation("r2", status.Result.HEALTHY, 99),),
                100,
                1,
            ),
            (
                True,
                (status.Observation(status.Channel.R2, "healthy", 99),),
                100,
                1,
            ),
        ):
            with self.subTest(core=core, now=now, limit=limit):
                with self.assertRaises(status.StatusError):
                    status.cloud_status(
                        core, probes, now=now, max_age_seconds=limit
                    )


class BacklogTest(unittest.TestCase):
    """未ACKと隔離を分け、実測できない回復時間を捏造しない。"""

    def setUp(self):
        self.epoch = object()

    def observe(self, data, at=200):
        return status.QueueObservation(self.epoch, at, data)

    def test_estimate_requires_fresh_same_epoch_net_acks_after_new_work(self):
        before = self.observe(summary(records=7, pending=6, acked=1), 100)
        after = self.observe(
            summary(
                records=8,
                pending=3,
                acked=5,
                last_attempt_at=195,
                last_confirmed_at=195,
                last_ack_at=196,
            ),
        )
        report = status.backlog_metrics(after, now=205, previous=before)
        self.assertEqual(report["oldest_unacked_age_seconds"], 125)
        self.assertEqual(report["sample_age_seconds"], 5)
        self.assertEqual(
            report["drain_projection"],
            {
                "window_seconds": 100,
                "new_queued_bundles": 1,
                "local_acks": 4,
                "net_drained_bundles": 3,
                "estimated_remaining_seconds": 100,
            },
        )
        self.assertEqual(report["last_remote_confirmed_at_unix_seconds"], 195)
        self.assertEqual(report["last_local_ack_at_unix_seconds"], 196)

    def test_no_estimate_empty_drain_grows_quarantine_stale_or_reset(self):
        baseline = self.observe(summary(records=5, pending=5), 100)
        short = self.observe(summary(records=5, pending=5), 155)
        positive = summary(
            records=5,
            pending=2,
            acked=3,
            last_attempt_at=190,
            last_confirmed_at=190,
            last_ack_at=191,
        )
        for label, before, after, now in (
            ("no prior", None, self.observe(positive), 205),
            ("short", baseline, short, 155),
            ("short positives", short, self.observe(positive), 200),
            (
                "old baseline",
                self.observe(baseline.summary, 80),
                self.observe(positive, 4000),
                4000,
            ),
            ("old current", baseline, self.observe(positive), 231),
            (
                "foreign epoch",
                baseline,
                status.QueueObservation(object(), 200, positive),
                205,
            ),
            (
                "regressed",
                self.observe(baseline.summary, 225),
                self.observe(positive),
                225,
            ),
            (
                "old ACK",
                baseline,
                self.observe({**positive, "last_ack_at": 100}),
                205,
            ),
            (
                "record reset",
                baseline,
                self.observe({**positive, "records": 4, "acked": 2}),
                205,
            ),
            (
                "no drain",
                baseline,
                self.observe(summary(records=5, pending=5)),
                200,
            ),
            (
                "growth",
                baseline,
                self.observe(summary(records=6, pending=6)),
                200,
            ),
            (
                "drained",
                baseline,
                self.observe(summary(records=5, acked=5)),
                200,
            ),
            (
                "quarantine",
                baseline,
                self.observe({**positive, "pending": 1, "quarantined": 1}),
                205,
            ),
        ):
            with self.subTest(label=label):
                report = status.backlog_metrics(
                    after, now=now, previous=before
                )
                self.assertIsNone(report["drain_projection"])

    def test_invalid_unknown_or_inconsistent_values_never_escape(self):
        for invalid in (
            {"records": True},
            {"acked": 1},
            {"last_ack_at": 201},
            {"attempts": -1},
            {"pending": 2},
            {"retained_bytes": 40},
            {"oldest_unacked_at": "private-path"},
            {"last_confirmed_at": 0},
            {"in_flight": 1},
            {"scheduled_retries": 1},
            {"private-id": "no"},
        ):
            with self.subTest(keys=list(invalid)):
                bad = self.observe(summary(**invalid))
                with self.assertRaises(status.StatusError) as failure:
                    status.backlog_metrics(bad, now=201)
                self.assertNotIn("private", str(failure.exception))
        empty = status.backlog_metrics(self.observe(summary()), now=201)
        self.assertEqual(empty["pending_count"], 0)
        self.assertIsNone(empty["oldest_unacked_age_seconds"])
        self.assertIsNone(empty["last_local_ack_at_unix_seconds"])

    def test_actual_outbox_preserves_distinct_remote_and_local_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = 100
            queue = outbox.Outbox(
                root / "local.db",
                outbox.Limits(attempts=2, delay=5),
                clock=lambda: now,
            )
            destination = Destination(root / "destination.db")
            self.assertTrue(queue.enqueue(bundle(root / "first", "1")))
            self.assertTrue(queue.enqueue(bundle(root / "second", "2")))
            self.assertTrue(queue.enqueue(bundle(root / "third", "3")))
            prior = self.observe(queue.summary(), now)

            now = 150
            offline = Mock()
            offline.put_object.side_effect = OSError("private-test-message")
            self.assertEqual(queue.deliver_one(offline), "pending")
            report = status.backlog_metrics(
                self.observe(queue.summary(), now), now=now
            )
            self.assertEqual(report["scheduled_retry_count"], 1)
            self.assertIsNone(report["last_remote_confirmed_at_unix_seconds"])

            now = 160
            with patch.object(queue, "_ack", side_effect=InterruptedError):
                with self.assertRaises(InterruptedError):
                    queue.deliver_one(destination)
            report = status.backlog_metrics(
                self.observe(queue.summary(), now), now=now
            )
            self.assertEqual(report["confirmed_without_ack_count"], 1)
            self.assertEqual(
                report["last_remote_confirmed_at_unix_seconds"], 160
            )
            self.assertIsNone(report["last_local_ack_at_unix_seconds"])

            now = 170
            self.assertEqual(queue.deliver_one(offline), "acked")
            report = status.backlog_metrics(
                self.observe(queue.summary(), now), now=now, previous=prior
            )
            self.assertEqual(
                report["last_remote_confirmed_at_unix_seconds"], 160
            )
            self.assertEqual(report["last_local_ack_at_unix_seconds"], 170)
            self.assertEqual(
                report["drain_projection"]["net_drained_bundles"], 1
            )
            self.assertEqual(report["pending_count"], 2)
            self.assertNotIn("private-test-message", json.dumps(report))

            now = 180
            self.assertEqual(queue.deliver_one(offline), "pending")
            now = 185
            self.assertEqual(queue.deliver_one(offline), "quarantined")
            report = status.backlog_metrics(
                self.observe(queue.summary(), now), now=now, previous=prior
            )
            self.assertEqual(report["quarantine_count"], 1)
            self.assertEqual(report["pending_count"], 1)
            self.assertEqual(report["unacked_count"], 2)
            self.assertIsNone(report["drain_projection"])

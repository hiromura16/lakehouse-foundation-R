"""模擬サービスと実際のローカル状態で、CDCのスレッド境界を試す。"""

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "infra/cdc"))
import config  # noqa: E402
import consumer  # noqa: E402
import consumer_runtime  # noqa: E402
import runtime_control  # noqa: E402
import state  # noqa: E402
from cdc_fixtures import (  # noqa: E402
    IDENTITY,
    MAPPING,
    TOPICS,
    VALUES,
    Broker,
    ReplayCatalog,
    ReplayTable,
    complete_history,
    partition,
)


class RuntimeTests(unittest.TestCase):
    """キュー登録、ローカル確定、ブローカーの配送確認を区別する。"""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="cdc-runtime-test-")
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "state.sqlite3"
        store = state.State(self.path, IDENTITY)
        store.set("initialized", "true")
        self.records = complete_history()
        self.table = ReplayTable(self.records, store.get("identity"))
        self.admin = SimpleNamespace(
            catalog=lambda: ReplayCatalog(self.table),
            CONTRACT=config.CONTRACTS,
            STATE={"blocked": False},
        )
        self.broker = Broker(self.records)
        self.control = runtime_control.Control("fixed-demo", "t" * 64, MAPPING)
        self.materialize = mock.Mock(return_value={"sql_statements": 14})
        self.runtime = consumer_runtime.Runtime(
            self.admin,
            store,
            VALUES,
            MAPPING,
            TOPICS,
            self.control,
            self.broker,
            partition,
            consumer.process,
            self.materialize,
        )
        self.addCleanup(lambda: self.runtime.state.connection.close())
        expressions = SimpleNamespace(
            EqualTo=lambda field, value: (field, value)
        )
        modules = {
            "pyiceberg": SimpleNamespace(expressions=expressions),
            "pyiceberg.expressions": expressions,
        }
        previous = {name: sys.modules.get(name) for name in modules}
        sys.modules.update(modules)

        def restore():
            """SDKの差し替えを戻し、Arrowの遅延読込は維持する。"""
            for name, module in previous.items():
                if module is None:
                    sys.modules.pop(name, None)
                else:
                    sys.modules[name] = module

        self.addCleanup(restore)

    def assign(self):
        """別名を含めず、合成トピックの実際のパーティションだけを観測する。"""
        self.runtime.assign(
            self.broker, [partition(row["topic"], 0) for row in self.records]
        )

    def queue(self):
        """状態やブローカーに触れず、HTTP 202に相当する受付結果を得る。"""
        status, request = self.control.dispatch(
            "POST", "/rebuild", "Bearer " + "t" * 64, b'{"confirm":true}'
        )
        self.assertEqual(status, 202)
        self.assertFalse(request["completed"])
        return request["request_id"]

    def message(self, index=0, *, payload=None):
        """テストの配達位置と本文を別に指定する。"""
        rec = self.records[index]
        return SimpleNamespace(
            topic=lambda: rec["topic"],
            partition=lambda: rec["partition"],
            offset=lambda: rec["offset"],
            key=lambda: rec["key"].encode(),
            value=lambda: (
                rec["value"].encode() if payload is None else payload
            ),
            error=lambda: None,
        )

    def observed_partition(self, index=0):
        """非公開トピック名を公開せず、対応する合成表を選ぶ。"""
        target = TOPICS[self.records[index]["topic"]]
        return next(
            row
            for row in self.control.snapshot()["partitions"]
            if row["table"] == target
        )

    def test_poll_cursor_exists_before_parse_without_new_watermark_reads(self):
        self.assign()
        observed_at = self.control.snapshot()["observation_time"]
        with (
            mock.patch.object(
                self.broker,
                "get_watermark_offsets",
                side_effect=AssertionError("extra Kafka calls"),
            ),
            mock.patch.object(
                self.runtime.state,
                "counts",
                side_effect=AssertionError("extra DB"),
            ),
        ):
            with self.assertRaises(UnicodeDecodeError):
                consumer.observed_record(
                    self.runtime, self.message(payload=b"\xff")
                )
            unauthed = self.control.dispatch("GET", "/status", None)
            status, view = self.control.dispatch(
                "GET", "/status", "Bearer " + "t" * 64
            )
        self.assertEqual(unauthed, (401, {"error_code": "AUTH_REQUIRED"}))
        self.assertEqual(status, 200)
        self.assertEqual(view["observation_time"], observed_at)
        row = self.observed_partition()
        self.assertEqual(row["last_read_offset"], 0)
        self.assertEqual(row["completed_offset"], -1)
        self.assertEqual(row["lag"], 1)
        self.assertIsNone(row["broker_ack_next_offset"])
        self.assertNotIn(self.records[0]["topic"], json.dumps(view))

    def test_read_advances_even_if_raw_write_fails(self):
        self.assign()
        self.runtime.note_read(self.message())
        with mock.patch.object(
            consumer, "append_raw", side_effect=RuntimeError("synthetic")
        ):
            self.assertIsNone(self.runtime.process([self.records[0]]))
        row = self.observed_partition()
        self.assertEqual(row["last_read_offset"], 0)
        self.assertEqual(row["completed_offset"], -1)
        self.assertEqual(row["next_offset"], 0)
        self.assertEqual(row["lag"], 1)
        self.assertIsNone(row["broker_ack_next_offset"])
        self.assertFalse(any(x[0] == "commit" for x in self.broker.calls))

    def test_partial_or_unverified_commit_reply_does_not_invent_ack(self):
        self.assign()
        first, second, third = self.records[:3]
        keys = [part["topic"] for part in self.records]
        plan = [partition(topic, 0, 1) for topic in keys]
        response = [
            SimpleNamespace(
                topic=first["topic"], partition=0, offset=1, error=None
            ),
            SimpleNamespace(
                topic=second["topic"],
                partition=0,
                offset=1,
                error=Exception("synthetic"),
            ),
            SimpleNamespace(
                topic=third["topic"], partition=0, offset=2, error=None
            ),
            SimpleNamespace(topic=keys[3], partition=0, offset=1, error=None),
            SimpleNamespace(topic=keys[3], partition=0, offset=1, error=None),
            partition(keys[4], 0, 1),
        ]
        self.runtime.note_ack(plan, None)
        self.assertIsNone(self.observed_partition()["broker_ack_next_offset"])
        self.runtime.note_ack(plan, response)
        self.runtime.observe()
        self.assertEqual(
            self.observed_partition()["broker_ack_next_offset"], 1
        )
        for index in range(1, len(self.records)):
            self.assertIsNone(
                self.observed_partition(index)["broker_ack_next_offset"]
            )
        self.assertIsNone(self.observed_partition()["last_read_offset"])
        self.assertEqual(self.observed_partition()["completed_offset"], -1)
        self.assertEqual(self.observed_partition()["lag"], 1)

    def test_revoke_reassign_watermark_reset_and_restart_clear_witnesses(self):
        self.assign()
        topic = self.records[0]["topic"]
        self.runtime.state.complete([self.records[0]])
        plan = [partition(topic, 0, 1)]
        reply = [
            SimpleNamespace(topic=topic, partition=0, offset=1, error=None)
        ]

        def mark():
            """同じ有効区画を読んで同期ACKを観測した状態にする。"""
            self.runtime.note_read(self.message())
            self.runtime.note_ack(plan, reply)
            self.runtime.observe()
            self.assertEqual(self.observed_partition()["last_read_offset"], 0)
            self.assertEqual(
                self.observed_partition()["broker_ack_next_offset"], 1
            )

        def unknown():
            """別のepochに前の実績を引き継がない。"""
            statuses = self.observed_partition()
            self.assertIsNone(statuses["last_read_offset"])
            self.assertIsNone(statuses["broker_ack_next_offset"])

        mark()
        self.runtime.revoke_cursors(self.broker, plan)
        self.runtime.note_read(self.message())
        self.runtime.note_ack(plan, reply)
        unknown()
        self.assign()
        mark()
        self.assign()
        unknown()
        mark()
        self.broker.bounds[(topic, 0)] = (0, 0)
        self.runtime.observe()
        unknown()
        self.assertEqual(self.observed_partition()["state"], "broker_reset")
        self.assertIsNone(self.observed_partition()["lag"])

        self.broker.bounds[(topic, 0)] = (0, 1)
        self.assign()
        mark()
        self.runtime.state.connection.close()
        self.runtime = consumer_runtime.Runtime(
            self.admin,
            state.State(self.path, IDENTITY),
            VALUES,
            MAPPING,
            TOPICS,
            self.control,
            self.broker,
            partition,
            consumer.process,
            self.materialize,
        )
        self.assertEqual(self.control.snapshot()["partitions"], [])
        self.assign()
        unknown()
        self.assertEqual(self.observed_partition()["completed_offset"], 0)
        self.assertEqual(self.observed_partition()["lag"], 0)

    def test_high_equal_to_local_next_clears_truncated_read_only(self):
        self.assign()
        topic = self.records[0]["topic"]
        consumer.observed_record(self.runtime, self.message())
        self.assertEqual(self.observed_partition()["last_read_offset"], 0)
        self.broker.bounds[(topic, 0)] = (0, 0)
        self.runtime.observe()
        row = self.observed_partition()
        self.assertIsNone(row["last_read_offset"])
        self.assertEqual(row["state"], "observed")
        self.assertEqual(row["completed_offset"], -1)
        self.assertEqual(row["next_offset"], row["broker_high"])
        self.assertEqual(row["lag"], 0)
        self.broker.bounds[(topic, 0)] = (0, 1)
        self.runtime.observe()
        self.assertIsNone(self.observed_partition()["last_read_offset"])

    def test_low_equal_to_local_next_clears_expired_cursor_witnesses(self):
        self.assign()
        initial = self.records[0]
        topic = initial["topic"]
        self.runtime.state.complete([initial])
        self.runtime.note_read(self.message())
        plan = [partition(topic, 0, 1)]
        reply = [
            SimpleNamespace(topic=topic, partition=0, offset=1, error=None)
        ]
        self.runtime.note_ack(plan, reply)
        self.broker.bounds[(topic, 0)] = (1, 2)
        self.runtime.observe()
        row = self.observed_partition()
        self.assertIsNone(row["last_read_offset"])
        # ACKは次位置なので、境界と同じなら矛盾しない。
        self.assertEqual(row["broker_ack_next_offset"], 1)
        self.runtime.state.complete([dict(initial, offset=1)])
        self.broker.bounds[(topic, 0)] = (2, 3)
        self.runtime.observe()
        row = self.observed_partition()
        self.assertIsNone(row["broker_ack_next_offset"])
        self.assertEqual(row["state"], "observed")
        self.assertEqual(row["completed_offset"], 1)
        self.assertEqual(row["next_offset"], row["broker_low"])
        self.assertEqual(row["lag"], 1)

    def test_ack_waits_for_atomic_publication_with_local_completion(self):
        self.assign()
        initial = self.records[0]
        topic = initial["topic"]
        read_bounds = self.broker.get_watermark_offsets
        plan = [partition(topic, 0, 1)]
        reply = [
            SimpleNamespace(topic=topic, partition=0, offset=1, error=None)
        ]

        during = []

        def old_status(row):
            """最新のローカル位置を含む状態までは新しいACKを表示しない。"""
            self.assertEqual(row["last_read_offset"], 0)
            self.assertEqual(row["completed_offset"], -1)
            self.assertIsNone(row["broker_ack_next_offset"])

        def bounds(*args, **kwargs):
            during.append(self.observed_partition())
            return read_bounds(*args, **kwargs)

        self.runtime.state.complete([initial])
        self.runtime.note_read(self.message())
        self.runtime.note_ack(plan, reply)
        old_status(self.observed_partition())
        self.runtime.note_read(self.message())
        old_status(self.observed_partition())
        with mock.patch.object(
            self.broker, "get_watermark_offsets", side_effect=bounds
        ) as observed:
            self.runtime.observe()
        observed.assert_called()
        self.assertTrue(during)
        for row in during:
            old_status(row)
        row = self.observed_partition()
        self.assertEqual(row["last_read_offset"], 0)
        self.assertEqual(row["completed_offset"], 0)
        self.assertEqual(row["broker_ack_next_offset"], 1)
        self.assertEqual(row["lag"], 0)

    def test_startup_needs_assignment_and_a_completed_derived_generation(self):
        self.assertFalse(self.control.snapshot()["ready"])
        self.assertEqual(self.control.snapshot()["error_code"], "CDC_STARTING")
        self.assign()
        self.assertFalse(self.control.snapshot()["ready"])
        self.runtime.state.complete(self.records)
        self.runtime.observe()
        self.assertTrue(self.control.snapshot()["ready"])
        self.assertTrue(
            all(
                row["lag"] == 0
                for row in self.control.snapshot()["partitions"]
            )
        )

    def test_broker_observation_failure_is_unknown_and_not_ready(self):
        self.runtime.state.complete(self.records)
        self.assign()
        with mock.patch.object(
            self.broker,
            "get_watermark_offsets",
            side_effect=RuntimeError("private broker text"),
        ):
            self.runtime.observe()
        snapshot = self.control.snapshot()
        self.assertFalse(snapshot["ready"])
        for row in snapshot["partitions"]:
            self.assertEqual(row["state"], "unavailable")
            self.assertIsNone(row["broker_high"])
            self.assertIsNone(row["lag"])
        self.assertNotIn("private broker", json.dumps(snapshot))

    def test_retention_gap_pauses_fixed_delivery_and_leaves_rebuild_api_alive(
        self,
    ):
        for key in self.broker.bounds:
            self.broker.bounds[key] = (1, 2)
        self.assign()
        self.assertTrue(self.runtime.maintenance)
        self.assertFalse(self.control.snapshot()["ready"])
        self.assertEqual(
            self.control.snapshot()["error_code"], "CDC_REBUILD_REQUIRED"
        )
        self.assertTrue(any(call[0] == "pause" for call in self.broker.calls))
        self.assertFalse(
            any(call[0] == "commit" for call in self.broker.calls)
        )
        self.queue()

    def test_generic_assignment_still_refuses_missing_broker_history(self):
        identity = {
            name: value
            for name, value in IDENTITY.items()
            if name not in {"source_mode", "schema_version", "installation_id"}
        }
        generic = state.State(self.path.with_name("generic.sqlite3"), identity)
        self.addCleanup(generic.connection.close)
        control = runtime_control.Control("generic", "t" * 64, MAPPING)
        runtime = consumer_runtime.Runtime(
            self.admin,
            generic,
            {**VALUES, "CDC_SOURCE_MODE": "generic"},
            MAPPING,
            TOPICS,
            control,
            self.broker,
            partition,
            consumer.process,
            self.materialize,
        )
        topic = self.records[0]["topic"]
        self.broker.bounds[(topic, 0)] = (1, 2)
        with self.assertRaises(RuntimeError):
            runtime.assign(self.broker, [partition(topic, 0)])

    def test_pending_generation_survives_restart_without_consuming_or_ack(
        self,
    ):
        self.runtime.state.set("materialization_pending", "true")
        self.runtime.state.connection.close()
        self.runtime.state = state.State(self.path, IDENTITY)
        self.runtime = consumer_runtime.Runtime(
            self.admin,
            self.runtime.state,
            VALUES,
            MAPPING,
            TOPICS,
            self.control,
            self.broker,
            partition,
            consumer.process,
            self.materialize,
        )
        self.assign()
        self.assertTrue(self.runtime.maintenance)
        self.assertFalse(self.control.snapshot()["ready"])
        self.assertEqual(self.runtime.state.completed_offsets(), {})
        self.assertFalse(
            any(call[0] == "commit" for call in self.broker.calls)
        )

    def test_http_dispatch_reads_only_cache_and_queues_without_touching_state(
        self,
    ):
        with mock.patch.object(
            self.runtime.state,
            "counts",
            side_effect=AssertionError("HTTP read SQLite"),
        ):
            before = list(self.broker.calls)
            self.control.dispatch("GET", "/status", "Bearer " + "t" * 64)
            self.queue()
            self.assertEqual(self.broker.calls, before)
            self.assertEqual(
                self.control.snapshot()["rebuild"]["status"], "queued"
            )

    def test_checkpoint_is_durable_before_successful_broker_ack(self):
        self.assign()
        original_commit = self.broker.commit

        def commit(*, offsets, asynchronous):
            """外部への配送確認時点で、SQLiteの状態を観測する。"""
            self.assertTrue(
                all(
                    self.runtime.state.offset(item.topic, item.partition) == 0
                    for item in offsets
                )
            )
            original_commit(offsets=offsets, asynchronous=asynchronous)
            return [
                SimpleNamespace(
                    topic=item.topic,
                    partition=item.partition,
                    offset=item.offset,
                    error=None,
                )
                for item in offsets
            ]

        with (
            mock.patch.object(consumer, "append_raw", return_value=6),
            mock.patch.object(
                consumer, "materialize", return_value={"sql_statements": 14}
            ),
            mock.patch.object(self.broker, "commit", side_effect=commit),
        ):
            result = self.runtime.process(self.records)
        self.assertEqual(result["raw_events_appended"], 6)
        self.assertEqual(
            self.control.snapshot()["raw_events_appended_since_start"], 6
        )
        self.assertTrue(self.control.snapshot()["ready"])
        self.assertFalse(self.runtime.state.get("materialization_pending"))
        self.assertEqual(
            self.observed_partition()["broker_ack_next_offset"], 1
        )
        self.assertEqual(self.observed_partition()["lag"], 0)

    def test_partial_sql_failure_halts_without_checkpoint_or_broker_ack(self):
        self.assign()
        with (
            mock.patch.object(consumer, "append_raw", return_value=6),
            mock.patch.object(
                consumer,
                "materialize",
                side_effect=RuntimeError("private driver text"),
            ),
        ):
            result = self.runtime.process(self.records)
        self.assertIsNone(result)
        self.assertEqual(self.runtime.state.completed_offsets(), {})
        self.assertFalse(
            any(call[0] == "commit" for call in self.broker.calls)
        )
        self.assertEqual(
            self.control.snapshot()["error_code"], "CDC_PROCESSING_FAILED"
        )
        self.assertFalse(self.control.snapshot()["ready"])

    def test_broker_ack_failure_keeps_valid_local_checkpoint_and_counts_once(
        self,
    ):
        self.assign()
        self.runtime.note_read(self.message())
        self.broker.fail_commit = True
        with (
            mock.patch.object(consumer, "append_raw", return_value=6),
            mock.patch.object(
                consumer, "materialize", return_value={"sql_statements": 14}
            ),
        ):
            result = self.runtime.process(self.records)
        self.assertIsNone(result)
        self.assertEqual(len(self.runtime.state.completed_offsets()), 6)
        self.assertEqual(
            self.control.snapshot()["error_code"], "CDC_KAFKA_ACK_FAILED"
        )
        self.assertEqual(
            self.control.snapshot()["raw_events_appended_since_start"], 6
        )
        self.assertFalse(self.control.snapshot()["ready"])
        row = self.observed_partition()
        self.assertEqual(row["last_read_offset"], 0)
        self.assertEqual(row["completed_offset"], 0)
        self.assertEqual(row["lag"], 0)
        self.assertIsNone(row["broker_ack_next_offset"])

    def test_raw_failure_on_completed_offset_is_not_a_broker_ack_failure(self):
        self.runtime.state.complete(self.records)
        self.assign()
        with mock.patch.object(
            consumer,
            "append_raw",
            side_effect=ValueError("synthetic-private-changed-payload"),
        ):
            self.assertIsNone(self.runtime.process(self.records))
        self.assertEqual(
            self.control.snapshot()["error_code"], "CDC_PROCESSING_FAILED"
        )
        codes = self.runtime.state.connection.execute(
            "SELECT DISTINCT error_code FROM processing_errors"
        ).fetchall()
        self.assertEqual(codes, [("CDC_RAW_WRITE_FAILED",)])
        self.assertFalse(
            any(call[0] == "commit" for call in self.broker.calls)
        )

    def test_rebuild_publishes_then_seeks_acks_and_resumes_with_matching_id(
        self,
    ):
        self.assign()
        self.runtime.note_read(self.message())
        request_id = self.queue()
        prior_commit = self.broker.commit

        def witnessed_commit(*, offsets, asynchronous):
            prior_commit(offsets=offsets, asynchronous=asynchronous)
            return [
                SimpleNamespace(
                    topic=item.topic,
                    partition=item.partition,
                    offset=item.offset,
                    error=None,
                )
                for item in offsets
            ]

        with mock.patch.object(
            self.broker, "commit", side_effect=witnessed_commit
        ):
            self.assertTrue(self.runtime.handle_rebuild())
        self.assertIsNone(self.observed_partition()["last_read_offset"])
        self.assertEqual(
            self.observed_partition()["broker_ack_next_offset"], 1
        )
        rebuilt = self.control.snapshot()["rebuild"]
        self.assertEqual(rebuilt["request_id"], request_id)
        self.assertEqual(rebuilt["status"], "succeeded")
        self.assertTrue(rebuilt["completed"])
        self.assertEqual(rebuilt["raw_snapshot_id"], 42)
        calls = [row[0] for row in self.broker.calls]
        self.assertLess(calls.index("pause"), calls.index("seek"))
        self.assertLess(calls.index("seek"), calls.index("commit"))
        self.assertLess(calls.index("commit"), calls.index("resume"))
        self.assertEqual(
            self.runtime.state.get("rebuild_generation"), request_id
        )
        self.assertTrue(self.control.snapshot()["ready"])

    def test_rebuild_failure_retains_durable_pending_and_never_acknowledges(
        self,
    ):
        self.assign()
        self.materialize.side_effect = RuntimeError("private query failure")
        self.queue()
        self.runtime.handle_rebuild()
        self.assertEqual(self.runtime.state.completed_offsets(), {})
        self.assertTrue(self.runtime.state.get("rebuild_pending"))
        self.assertFalse(
            any(call[0] == "commit" for call in self.broker.calls)
        )
        rebuilt = self.control.snapshot()["rebuild"]
        self.assertEqual(rebuilt["status"], "failed")
        self.assertFalse(rebuilt["completed"])
        self.assertEqual(rebuilt["error_code"], "CDC_REBUILD_FAILED")
        self.assertNotIn("private query", json.dumps(self.control.snapshot()))

    def test_ack_failure_after_rebuild_retains_published_local_generation(
        self,
    ):
        self.assign()
        self.broker.fail_commit = True
        request_id = self.queue()
        self.runtime.handle_rebuild()
        self.assertEqual(
            self.runtime.state.get("rebuild_generation"), request_id
        )
        self.assertEqual(len(self.runtime.state.completed_offsets()), 6)
        self.assertFalse(self.runtime.state.get("rebuild_pending"))
        self.assertEqual(
            self.control.snapshot()["error_code"], "CDC_KAFKA_ACK_FAILED"
        )
        self.assertFalse(self.control.snapshot()["ready"])
        self.assertFalse(self.control.snapshot()["rebuild"]["completed"])

    def test_public_status_excludes_source_identifiers_and_business_data(
        self,
    ):
        self.assign()
        text = json.dumps(self.control.snapshot())
        for private in (
            VALUES["SOURCE_SQLSERVER_HOST"],
            VALUES["SOURCE_SQLSERVER_DATABASE"],
            VALUES["SOURCE_SQLSERVER_USER"],
            VALUES["SOURCE_SQLSERVER_PASSWORD"],
            "product_name",
        ):
            self.assertNotIn(private, text)


if __name__ == "__main__":
    unittest.main()

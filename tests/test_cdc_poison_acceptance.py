"""型不正を隔離し、不正JSONの停止と混同しないことを試す。"""

import json
import runpy
import sys
import tempfile
import traceback
import unittest
from contextlib import closing
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import cdc_poison_acceptance as poison
from scripts.influx_local_acceptance import AcceptanceError


def load_fixture():
    """実クライアントを必要とせず、閉域fixtureの純粋な観測を呼び出す。"""
    imports = {"confluent_kafka": Mock(), "native": Mock()}
    with patch.dict(sys.modules, imports):
        return runpy.run_path(
            str(poison.closed.FIXTURES / "cdc_acceptance.py")
        )


class PoisonOracleTest(unittest.TestCase):
    """原本と固定理由がずれれば合格させない。"""

    def test_all_three_outcomes_and_exact_malformed_bytes(self):
        fixture = load_fixture()
        messages = fixture["poison_messages"]()
        topic = "foundation_r.synthetic_sales.dbo.erp_products"
        raw = [
            {
                "topic": topic,
                "partition": "0",
                "offset": str(offset),
                "event_id": f'["{topic}",0,{offset}]',
                "key_json": json.dumps({"product_id": key}),
                "envelope_json": value,
                "value_is_null": False,
                "key_is_null": False,
            }
            for offset, (key, value) in enumerate(messages)
        ]
        self.assertEqual(messages[3][1], "{")
        with self.assertRaises(json.JSONDecodeError):
            json.loads(messages[3][1])

        def digest(i):
            return sha256(messages[i][1].encode()).hexdigest()

        def live_row(i):
            return {
                **json.loads(messages[i][1])["after"],
                "_source_hash": digest(i),
            }

        healthy, followup = live_row(0), live_row(2)
        invalid = {
            "product_id": messages[1][0],
            "_source_hash": digest(1),
            "raw_record_json": '{"standard_cycle_seconds":"not-a-decimal"}',
            "validation_reason": "invalid_type",
        }
        verify = fixture["verify_poison_rows"]
        verify(raw[:1], [healthy], [], 1)
        verify(raw[:2], [healthy], [invalid], 2)
        verify(raw[:3], [healthy, followup], [invalid], 3)
        verify(raw, [healthy, followup], [invalid], 4)
        for index, field, value in (
            (1, "offset", "0"),
            (1, "partition", "1"),
            (3, "envelope_json", "{}"),
        ):
            with self.subTest(field=field), self.assertRaises(AssertionError):
                changed = deepcopy(raw)
                changed[index][field] = value
                verify(changed, [healthy, followup], [invalid], 4)
        for bad in (
            {"validation_reason": "missing_key"},
            {"_source_hash": "0" * 64},
            {"product_id": healthy["product_id"]},
        ):
            with self.subTest(bad=bad), self.assertRaises(AssertionError):
                verify(raw, [healthy, followup], [{**invalid, **bad}], 4)
        with self.assertRaises(AssertionError):
            verify(raw, [healthy, healthy, followup], [invalid], 4)
        with self.assertRaises(AssertionError):
            verify(raw, [healthy, followup], [], 4)

    def test_exact_existing_generic_json_failure_not_wrong_exception(self):
        location = poison.parser_location()
        report = {
            "event": "native.operation_failed",
            "error_type": "JSONDecodeError",
            "locations": ["consumer_runtime.py:90", location],
        }
        self.assertTrue(
            poison.is_malformed_failure(json.dumps(report), location)
        )
        for item in (
            {**report, "error_type": "UnicodeDecodeError"},
            {
                **report,
                "locations": ["consumer_runtime.py:90", "consumer.py:4"],
            },
            {**report, "locations": [location]},
        ):
            self.assertFalse(
                poison.is_malformed_failure(json.dumps(item), location)
            )

    def test_real_generic_parser_fails_after_raw_without_sqlite_or_kafka_ack(
        self,
    ):
        from test_cdc_runtime import (
            consumer,
            consumer_runtime,
            runtime_control,
        )
        from test_cdc_runtime import state as store_type

        topic = "foundation_r.synthetic_sales.dbo.erp_products"
        record = {
            "topic": topic,
            "partition": 0,
            "offset": 3,
            "key": '{"product_id":"CDC-JSON-MALFORMED"}',
            "value": "{",
            "ingested_at": "2026-01-01T00:00:00Z",
        }
        broker = Mock()
        submitted = Mock(return_value=1)
        admin = SimpleNamespace(STATE={}, catalog=lambda: "synthetic catalog")
        control = runtime_control.Control("generic", "x" * 24, {})
        with tempfile.TemporaryDirectory() as home:
            store = store_type.State(Path(home) / "state.sqlite3", {})
            with closing(store.connection):
                store.complete([dict(record, offset=2)])
                runtime = consumer_runtime.Runtime(
                    admin,
                    store,
                    {"CDC_SQLSERVER_DATABASE": "synthetic_sales"},
                    {},
                    {topic: "erp_products"},
                    control,
                    broker,
                    Mock(),
                    consumer.process,
                    Mock(),
                )
                with patch.object(consumer, "append_raw", submitted):
                    try:
                        runtime.process([record])
                    except json.JSONDecodeError as error:
                        report = {
                            "event": "native.operation_failed",
                            "error_type": type(error).__name__,
                            "locations": [
                                f"{Path(frame.filename).name}:{frame.lineno}"
                                for frame in traceback.extract_tb(
                                    error.__traceback__
                                )
                            ],
                        }
                    else:
                        self.fail("invalid JSON unexpectedly reached ACK")
                self.assertEqual(store.offset(topic, 0), 2)
        broker.commit.assert_not_called()
        submitted.assert_called_once_with("synthetic catalog", [record])
        self.assertTrue(
            poison.is_malformed_failure(
                json.dumps(report), poison.parser_location()
            )
        )

    def test_healthy_change_or_replaced_snapshot_fails(self):
        def sample(raw, release, digest):
            return {
                "raw_snapshot_id": raw,
                "release_snapshot_id": release,
                "healthy_digest": digest,
            }

        before = sample(1, "1", "a" * 64)
        poison.require_new(before, sample(2, "2", "a" * 64))
        for after in (
            sample(1, "2", "a" * 64),
            sample(2, "1", "a" * 64),
            sample(2, "2", "b" * 64),
        ):
            with self.assertRaises(AcceptanceError):
                poison.require_new(before, after)

    def test_phase_uses_exact_delivery_then_durable_broker_before_catalog(
        self,
    ):
        fixture = load_fixture()
        run = fixture["run_poison"]
        result = Mock()
        result.to_pylist.return_value = [
            {"product_id": fixture["POISON_KEYS"][0]}
        ]
        publish = Mock()
        events = []

        def verify(*args):
            events.append(("verify", args[-1]))

        def acknowledge(value, *, high):
            events.append(("broker", value, high))
            return value

        def submit(*args):
            events.append(("publish", args[1][0][2]))
            publish(*args)

        table = Mock()
        catalog = Mock()
        catalog.load_table.return_value = table
        engine = SimpleNamespace(
            latest_release=Mock(
                return_value={
                    "release_snapshot_id": "1",
                    "outputs": {
                        "silver.erp_products": 5,
                        "quarantine.erp_products": 6,
                    },
                }
            ),
            bounded_scan=Mock(return_value=result),
            current_id=Mock(return_value=1),
        )
        names = {
            "publish_messages": submit,
            "broker_committed": acknowledge,
            "verify_poison_rows": verify,
            "engine": engine,
        }
        with (
            patch.dict(run.__globals__, names),
            patch.object(
                run.__globals__["api"],
                "core_module",
                return_value=SimpleNamespace(catalog=lambda: catalog),
            ),
            patch("builtins.print"),
        ):
            for phase in fixture["POISON_PHASES"]:
                run(phase)
        self.assertEqual(
            events,
            [
                ("publish", 0),
                ("broker", 1, 1),
                ("verify", 1),
                ("publish", 1),
                ("broker", 2, 2),
                ("verify", 2),
                ("publish", 2),
                ("broker", 3, 3),
                ("verify", 3),
                ("broker", 3, 3),
                ("verify", 3),
                ("publish", 3),
                ("broker", 3, 4),
                ("verify", 4),
            ],
        )
        self.assertEqual(publish.call_count, 4)
        self.assertEqual(publish.call_args.args[0][0][2], "{")

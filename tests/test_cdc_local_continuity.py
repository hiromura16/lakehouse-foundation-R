"""閉域CDC受入の構成と実行時境界を合成データだけで検査する。"""

import json
import runpy
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import cdc_local_continuity as cdc

PROJECT = "foundation-r-cdc-local-ci-0123456789abcdef"
IDS = {name: f"{index + 1:064x}" for index, name in enumerate(cdc.NETWORKS)}


def model():
    """受入に必要な正規化構成を小さく表す。"""
    return {
        "name": PROJECT,
        "services": {
            name: {"networks": {net: {} for net in nets}}
            for name, nets in cdc.NETWORKS.items()
        },
        "networks": {
            net: {"name": f"{PROJECT}_{net}", "internal": True}
            for nets in cdc.NETWORKS.values()
            for net in nets
        },
        "volumes": {"storage": {"name": f"{PROJECT}_storage"}},
    }


def live_docker(*, exposed=False, outside=False, extra=False):
    """Dockerの読取応答を合成し、外部接続はしない。"""

    def inspect(*args):
        if args[0] == "ps":
            return "\n".join(IDS.values())
        if args[0] == "inspect":
            service = next(
                name for name, value in IDS.items() if value == args[-1]
            )
            running = service != "catalog-migrate"
            return json.dumps(
                {
                    "Id": IDS[service],
                    "Config": {
                        "Labels": {
                            "com.docker.compose.project": PROJECT,
                            "com.docker.compose.service": service,
                        }
                    },
                    "State": {
                        "Running": running,
                        "Status": "exited",
                        "ExitCode": 0,
                    },
                    "Mounts": [
                        {
                            "Destination": "/var/lib/redpanda/data",
                            "Type": "volume",
                            "Name": f"{PROJECT}_continuity-broker",
                        }
                    ]
                    if service == "test-broker"
                    else [],
                    "HostConfig": {"PortBindings": {}},
                    "NetworkSettings": {
                        "Ports": {"1234/tcp": [{}]}
                        if exposed
                        else {"1234/tcp": None},
                        "Networks": {
                            f"{PROJECT}_{key}": {}
                            for key in cdc.NETWORKS[service]
                        },
                    },
                }
            )
        name = args[-1]
        key = name.removeprefix(f"{PROJECT}_")
        endpoints = {
            value: {}
            for service, value in IDS.items()
            if key in cdc.NETWORKS[service] and service != "catalog-migrate"
        }
        if extra:
            endpoints["unexpected"] = {}
        return json.dumps(
            {
                "Id": "a" * 64,
                "Name": name,
                "Driver": "bridge",
                "Internal": not outside,
                "Containers": endpoints,
                "Labels": {
                    "com.docker.compose.project": PROJECT,
                    "com.docker.compose.network": key,
                },
            }
        )

    return inspect


def phase_evidence():
    """各段階でRawと公開世代が進み、再読込だけでは維持される。"""
    return [
        {
            "raw_snapshot_id": snap,
            "release_snapshot_id": str(snap + 10),
            "broker_next_offset": offset,
        }
        for snap, offset in (
            (1, 10),
            (2, 11),
            (3, 12),
            (3, 12),
            (4, 13),
            (5, 14),
            (5, 14),
        )
    ]


def close_fixture():
    """モジュール読込みだけを模擬し、Kafkaへ接続しない。"""
    imports = {"confluent_kafka": Mock(), "native": Mock()}
    with patch.dict(sys.modules, imports):
        return runpy.run_path(str(cdc.FIXTURES / "cdc_acceptance.py"))


class ContinuityTest(unittest.TestCase):
    """意図しないホストport・外向きネット・他コンテナを拒否する。"""

    def test_static_model_requires_internal_owned_boundary(self):
        cdc.config_boundary(model(), PROJECT)
        for kind in ("published", "outside", "external", "unexpected-service"):
            with self.subTest(kind=kind):
                cfg = model()
                if kind == "published":
                    cfg["services"]["lakehouse"]["ports"] = [
                        {"host_ip": "127.0.0.1"}
                    ]
                elif kind == "outside":
                    cfg["networks"]["api_origin"]["internal"] = False
                elif kind == "external":
                    cfg["volumes"]["storage"]["external"] = True
                else:
                    cfg["services"]["cloudflared"] = {}
                with self.assertRaises(cdc.AcceptanceError):
                    cdc.config_boundary(cfg, PROJECT)

    def test_runtime_membership_requires_owned_active_endpoints_only(self):
        def compose(*args):
            return IDS[args[-1]]

        self.assertEqual(
            cdc.runtime_boundary(live_docker(), compose, PROJECT)[0], IDS
        )
        for options in ({"exposed": True}, {"outside": True}, {"extra": True}):
            with (
                self.subTest(options=options),
                self.assertRaises(cdc.AcceptanceError),
            ):
                cdc.runtime_boundary(live_docker(**options), compose, PROJECT)

    def test_closed_workload_separates_broker_and_sqlite_positions(self):
        results = phase_evidence()
        compose = Mock(side_effect=map(json.dumps, results))
        checkpoint = Mock()
        read_current = Mock(return_value={"proof": "same synthetic state"})
        cdc.verify_closed_workload(
            compose, checkpoint, "fixture source", read_current
        )
        self.assertEqual(read_current.call_count, 4)
        self.assertEqual(
            [call.args[-1] for call in compose.call_args_list],
            [
                "continuity-insert",
                "continuity-update",
                "continuity-delete",
                "continuity-confirm",
                "continuity-late-update",
                "continuity-duplicate",
                "continuity-order-confirm",
            ],
        )
        self.assertEqual(
            [call.args[0] for call in checkpoint.call_args_list],
            [9, 10, 11, 11, 12, 13, 13],
        )

    def test_cursor_mismatch_or_post_delete_generation_change_is_not_success(
        self,
    ):
        results = phase_evidence()
        for position, field, value in (
            (0, "broker_next_offset", 9),
            (0, "broker_next_offset", 10.0),
            (0, "broker_next_offset", True),
            (0, "release_snapshot_id", "0"),
            (0, "release_snapshot_id", "１２"),
            (1, "raw_snapshot_id", 1),
            (3, "release_snapshot_id", "44"),
            (4, "broker_next_offset", 12),
            (5, "raw_snapshot_id", 4),
            (6, "release_snapshot_id", "99"),
        ):
            with self.subTest(field=field, position=position):
                changed = [item.copy() for item in results]
                changed[position][field] = value
                with self.assertRaises(cdc.AcceptanceError):
                    cdc.verify_closed_workload(
                        Mock(side_effect=map(json.dumps, changed)),
                        Mock(),
                        "fixture source",
                        Mock(return_value={"same": True}),
                    )

    def test_raw_verifies_exact_delivery_and_real_before_after(self):
        # 受入コンテナ外ではKafkaクライアントを起動しない。
        fixture = close_fixture()
        verify = fixture["verify_continuity_raw"]
        base = {**fixture["DATA"], "product_id": "CDC-CONTINUITY"}
        first = {**base, "product_name": "Continuity initial"}
        changed = {**base, "product_name": "Continuity updated"}
        _, delete_before = fixture["late_images"]()
        topic = "foundation_r.synthetic_sales.dbo.erp_products"
        rows = [
            {
                "key_json": '{"product_id":"CDC-CONTINUITY"}',
                "topic": topic,
                "partition": "0",
                "offset": str(offset),
                "event_id": f'["{topic}",0,{offset}]',
                "envelope_json": json.dumps(
                    {
                        "op": op,
                        "source": {"event_serial_no": serial},
                        "before": before,
                        "after": after,
                    }
                ),
            }
            for offset, op, serial, before, after in (
                (9, "c", 10, None, first),
                (10, "u", 12, first, changed),
                (11, "d", 14, delete_before, None),
            )
        ]
        live = [fixture["DATA"]]
        verify(rows, live, 3)
        for field, value in (
            ("topic", topic + "_other"),
            ("partition", "1"),
            ("event_id", f'["{topic}",1,10]'),
        ):
            with self.subTest(field=field), self.assertRaises(AssertionError):
                corrupt = deepcopy(rows)
                corrupt[1][field] = value
                verify(corrupt, live, 3)
        for index, field, value in (
            (0, "before", first),
            (1, "before", changed),
            (2, "before", first),
            (2, "before", changed),
            (2, "after", changed),
        ):
            with (
                self.subTest(index=index, field=field),
                self.assertRaises(AssertionError),
            ):
                corrupt = deepcopy(rows)
                envelope = json.loads(corrupt[index]["envelope_json"])
                envelope[field] = value
                corrupt[index]["envelope_json"] = json.dumps(envelope)
                verify(corrupt, live, 3)

    def test_ordering_requires_exact_late_update_and_duplicate_delivery(self):
        fixture = close_fixture()
        products = fixture["PRODUCTS_TOPIC"]
        before, after = fixture["late_images"]()
        original = fixture["resume_row"]()
        event = fixture["event"]

        def raw(offset, point, payload):
            return {
                "topic": products,
                "partition": "0",
                "offset": str(offset),
                "event_id": json.dumps(
                    [products, 0, offset], separators=(",", ":")
                ),
                "key_json": json.dumps({"product_id": point["product_id"]}),
                "value_is_null": False,
                "envelope_json": payload,
            }

        original_body = event("erp_products", "c", 5, original)
        new = {
            8: raw(8, original, original_body),
            12: raw(
                12,
                after,
                event("erp_products", "u", 13, after, previous=before),
            ),
            13: raw(13, original, original_body),
        }
        rows = [new.get(i, {"offset": str(i)}) for i in range(14)]
        verify = fixture["verify_ordering_raw"]
        verify(rows[:-1], 13)
        verify(rows, 14)
        for index, field, invalid in (
            (12, "envelope_json", event("erp_products", "c", 13, after)),
            (
                12,
                "envelope_json",
                event("erp_products", "u", 15, after, previous=before),
            ),
            (
                12,
                "envelope_json",
                event("erp_products", "u", 13, after, previous=after),
            ),
            (13, "event_id", rows[8]["event_id"]),
            (13, "envelope_json", event("erp_products", "c", 6, original)),
            (13, "offset", rows[8]["offset"]),
        ):
            with self.subTest(index=index, field=field):
                corrupt = deepcopy(rows)
                corrupt[index][field] = invalid
                with self.assertRaises(AssertionError):
                    verify(corrupt, len(corrupt))

    def test_ordering_rejects_changed_current_state_even_with_valid_ack(self):
        proofs = [{"same": True}, {"same": True}, {"same": False}]
        with self.assertRaises(cdc.AcceptanceError):
            cdc.verify_closed_workload(
                Mock(side_effect=map(json.dumps, phase_evidence())),
                Mock(),
                "fixture source",
                Mock(side_effect=proofs),
            )

    def test_ordering_state_requires_retained_delete_version(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "scratch.sqlite3"
            with closing(sqlite3.connect(path)) as db:
                db.execute(
                    "CREATE TABLE current_rows (table_name, row_key, version, "
                    "row_json, source_hash, ingested_at)"
                )
                db.executemany(
                    "INSERT INTO current_rows VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            "erp_products",
                            "CDC-P1",
                            "[1,1,1,1,1,1,5,5]",
                            '{"product_name":"Reinserted after restart"}',
                            "synthetic",
                            "fixture-time",
                        ),
                        (
                            "erp_products",
                            "CDC-CONTINUITY",
                            "[1,1,1,1,1,1,14,14]",
                            None,
                            "synthetic",
                            "fixture-time",
                        ),
                    ],
                )
                db.commit()
                self.assertIsNone(
                    cdc.ordering_state(path)["CDC-CONTINUITY"][1]
                )
                db.execute(
                    "UPDATE current_rows SET row_json='{}' WHERE row_key=?",
                    ("CDC-CONTINUITY",),
                )
                db.commit()
                with self.assertRaises(cdc.AcceptanceError):
                    cdc.ordering_state(path)

    def test_order_phases_on_actual_sqlite_without_kafka(self):
        fixture = close_fixture()
        point = fixture["resume_row"]()
        prior, delete_before = fixture["late_images"]()
        start = {**prior, "product_name": "Continuity initial"}
        product_topic = fixture["PRODUCTS_TOPIC"]
        app = runpy.run_path(str(cdc.ROOT / "infra/cdc/state.py"))
        mapping = {
            "source_schema": "dbo",
            "source_table": "erp_products",
            "columns": {key: key for key in point},
        }
        timeline = []
        offset = 11
        state = None

        def stage(operation, serial, row, *, previous=None):
            record = {
                "topic": product_topic,
                "partition": 0,
                "offset": offset,
                "key": json.dumps({"product_id": row["product_id"]}),
                "value": fixture["event"](
                    "erp_products", operation, serial, row, previous=previous
                ),
                "ingested_at": str(offset),
            }
            state.stage(record, "erp_products", mapping, list(point))
            state.complete([record])

        def deliver(groups, *, previous=None, expected_delivery=None):
            nonlocal offset
            offset += 1
            self.assertEqual(expected_delivery, [(product_topic, 0, offset)])
            self.assertEqual(len(groups), 1)
            [(table, key, events)] = groups
            self.assertEqual((table, key), ("erp_products", "product_id"))
            self.assertEqual(len(events), 1)
            stage(*events[0], previous=previous)
            timeline.append("publish")

        def acknowledge(expected, *, high):
            self.assertEqual(expected, offset + 1)
            self.assertEqual(high, expected)
            self.assertEqual(state.offset(product_topic, 0), offset)
            timeline.append("broker")
            return expected

        def catalog(*_args, **_kwargs):
            timeline.append("catalog")
            return {}

        run_ordering = fixture["run_ordering"]
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.sqlite3"
            state = app["State"](path, {"source_mode": "generic"})
            with closing(state.connection):
                offset = 8
                stage("c", 5, point)
                offset = 9
                stage("c", 10, start)
                offset = 10
                stage("u", 12, prior, previous=start)
                offset = 11
                stage("d", 14, delete_before)
                baseline = cdc.ordering_state(path)
                with (
                    patch.dict(
                        run_ordering.__globals__,
                        {
                            "publish": deliver,
                            "broker_committed": acknowledge,
                            "check": catalog,
                        },
                    ),
                    patch("builtins.print"),
                ):
                    for phase in fixture["ORDERING"]:
                        run_ordering(phase)
                        self.assertEqual(cdc.ordering_state(path), baseline)
        self.assertEqual(
            timeline,
            [
                "publish",
                "broker",
                "catalog",
                "publish",
                "broker",
                "catalog",
                "broker",
                "catalog",
            ],
        )

    def test_outside_host_fails_before_docker(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch(
                "scripts.cdc_local_continuity.exercise", new=Mock()
            ) as exercise,
            patch("builtins.print"),
        ):
            self.assertEqual(cdc.main(), 1)
        exercise.assert_not_called()


if __name__ == "__main__":
    unittest.main()

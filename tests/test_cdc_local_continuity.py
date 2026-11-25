"""閉域CDC受入の構成と実行時境界を合成データだけで検査する。"""

import json
import runpy
import sys
import unittest
from copy import deepcopy
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
        results = [
            {
                "raw_snapshot_id": snap,
                "release_snapshot_id": str(snap + 10),
                "broker_next_offset": offset,
            }
            for snap, offset in ((1, 10), (2, 11), (3, 12), (3, 12))
        ]
        compose = Mock(side_effect=map(json.dumps, results))
        checkpoint = Mock()
        cdc.verify_closed_workload(compose, checkpoint, "fixture source")
        self.assertEqual(
            [call.args[-1] for call in compose.call_args_list],
            [
                "continuity-insert",
                "continuity-update",
                "continuity-delete",
                "continuity-confirm",
            ],
        )
        self.assertEqual(
            [call.args[0] for call in checkpoint.call_args_list],
            [9, 10, 11, 11],
        )

    def test_cursor_mismatch_or_post_delete_generation_change_is_not_success(
        self,
    ):
        results = [
            {
                "raw_snapshot_id": snap,
                "release_snapshot_id": str(snap + 10),
                "broker_next_offset": offset,
            }
            for snap, offset in ((1, 10), (2, 11), (3, 12), (3, 12))
        ]
        for position, field, value in (
            (0, "broker_next_offset", 9),
            (0, "broker_next_offset", 10.0),
            (0, "broker_next_offset", True),
            (0, "release_snapshot_id", "0"),
            (0, "release_snapshot_id", "１２"),
            (1, "raw_snapshot_id", 1),
            (3, "release_snapshot_id", "44"),
        ):
            with self.subTest(field=field, position=position):
                changed = [item.copy() for item in results]
                changed[position][field] = value
                with self.assertRaises(cdc.AcceptanceError):
                    cdc.verify_closed_workload(
                        Mock(side_effect=map(json.dumps, changed)),
                        Mock(),
                        "fixture source",
                    )

    def test_raw_verifies_exact_delivery_and_real_before_after(self):
        # 受入コンテナ外ではKafkaクライアントを起動しない。
        imports = {"confluent_kafka": Mock(), "native": Mock()}
        with patch.dict(sys.modules, imports):
            fixture = runpy.run_path(str(cdc.FIXTURES / "cdc_acceptance.py"))
        verify = fixture["verify_continuity_raw"]
        base = {**fixture["DATA"], "product_id": "CDC-CONTINUITY"}
        first = {**base, "product_name": "Continuity initial"}
        changed = {**base, "product_name": "Continuity updated"}
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
                        "source": {"event_serial_no": offset + 1},
                        "before": before,
                        "after": after,
                    }
                ),
            }
            for offset, op, before, after in (
                (9, "c", None, first),
                (10, "u", first, changed),
                (11, "d", changed, None),
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

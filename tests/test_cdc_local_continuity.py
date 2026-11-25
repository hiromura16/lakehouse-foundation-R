"""閉域CDC受入の構成と実行時境界を合成データだけで検査する。"""

import json
import unittest
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

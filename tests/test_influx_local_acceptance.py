"""Hosted受入の名前境界と後片付け条件を、Dockerなしで検査する。"""

import copy
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from scripts import influx_local_acceptance as acceptance

PROJECT = "foundation-r-influx-local-ci-0123456789abcdef"
SECRET = Path("/synthetic/new-fixture.json")


def model():
    """有効なランナー候補を最低限の正規化形式で表す。"""
    services = {
        acceptance.INFLUX: {
            "volumes": [
                {
                    "type": "volume",
                    "source": "influx-local-data",
                    "target": "/var/lib/influxdb3/data",
                }
            ],
            "networks": {"influx_local": {}},
        },
        "health-relay": {"networks": {"influx_local": {}}},
        "blackbox": {},
        "prometheus": {},
    }
    return {
        "name": PROJECT,
        "services": services,
        "networks": {
            "influx_local": {
                "name": PROJECT + "_influx_local",
                "internal": True,
            }
        },
        "volumes": {
            "influx-local-data": {"name": PROJECT + "_influx-local-data"}
        },
        "secrets": {"influx-local-admin": {"file": str(SECRET)}},
    }


class BoundaryTest(unittest.TestCase):
    """既存資源の共有や安全境界の変更を起動前に止める。"""

    def test_normalization_refuses_old_or_shared_resources(self):
        base = model()
        acceptance.config_boundary(base, PROJECT, SECRET)
        for category, modify in (
            ("wrong-project", lambda m: m.update(name="wrong-project")),
            ("tools", lambda m: m["volumes"].update(**{"tools-influx": {}})),
            (
                "port",
                lambda m: m["services"][acceptance.INFLUX].update(
                    ports=[{"host_ip": "127.0.0.1"}]
                ),
            ),
            (
                "extra-net",
                lambda m: m["services"]["blackbox"].update(
                    networks={"influx_local": {}}
                ),
            ),
            (
                "other-name",
                lambda m: m["volumes"]["influx-local-data"].update(
                    name="different"
                ),
            ),
            (
                "external-vol",
                lambda m: m["volumes"]["influx-local-data"].update(
                    external=True
                ),
            ),
            (
                "old-token",
                lambda m: m["secrets"]["influx-local-admin"].update(
                    file="/synthetic/other.json"
                ),
            ),
        ):
            with self.subTest(category=category):
                candidate = copy.deepcopy(base)
                modify(candidate)
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.config_boundary(candidate, PROJECT, SECRET)

    def test_started_monitor_sidecars_must_have_no_host_port(self):
        for sidecar in acceptance.TARGETS[1:]:
            with self.subTest(sidecar=sidecar):
                candidate = model()
                candidate["services"][sidecar]["ports"] = [
                    {
                        "host_ip": "127.0.0.1",
                        "target": 8120,
                        "published": "30124",
                    }
                ]
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.config_boundary(candidate, PROJECT, SECRET)

    def test_labeled_or_colliding_physical_resource_is_not_owned(self):
        for outputs in (
            ["pre-existing-container"],
            ["", "", "", "", PROJECT + "_influx-local-data"],
        ):
            with self.subTest(outputs=outputs):
                docker = Mock(side_effect=outputs)
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.no_prior_resources(model(), PROJECT, docker)
                self.assertIn("--filter", docker.call_args_list[0].args)

    def test_non_hosted_main_does_not_enter_runtime(self):
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("scripts.influx_local_acceptance.exercise") as run,
            patch("builtins.print"),
        ):
            self.assertEqual(acceptance.main(), 1)
        run.assert_not_called()

    def test_cleanup_not_started_when_project_already_contains_a_resource(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp:
            # Dockerの動作はすべて偽の代入に限定する。
            with (
                patch(
                    "scripts.influx_local_acceptance.secrets.token_hex",
                    return_value="0123456789abcdef",
                ),
                patch("scripts.influx_local_acceptance.initialize"),
                patch("scripts.influx_local_acceptance.config_boundary"),
                patch(
                    "scripts.influx_local_acceptance.run",
                    side_effect=[json.dumps(model()), "existing"],
                ),
                patch("builtins.print"),
            ):
                self.assertEqual(acceptance.exercise(Path(temp)), 1)
            # JSON合成と先在するコンテナの確認だけで終了。
            # Docker run mock側に3つ目の応答はなく、破棄すると試験は失敗する。


class PrometheusFreshnessTest(unittest.TestCase):
    """評価時刻をサンプル時刻と取り違えない。"""

    @staticmethod
    def response(sample):
        # 評価時刻だけが新しくても採取は古い場合がある。
        return json.dumps({"data": {"result": [{"value": [2000, sample]}]}})

    def test_timestamp_is_strictly_newer_than_docker_restart(self):
        compose = Mock(
            side_effect=[
                "",
                self.response("99.9"),
                self.response("100"),
                self.response("100.001"),
            ]
        )
        with (
            patch(
                "scripts.influx_local_acceptance.time.monotonic",
                return_value=0,
            ),
            patch("scripts.influx_local_acceptance.time.sleep") as sleep,
        ):
            acceptance.check_probe(compose, Decimal("100"))
        self.assertEqual(sleep.call_count, 2)
        query = parse_qs(urlsplit(compose.call_args.args[-1]).query)["query"][
            0
        ]
        self.assertIn("timestamp(probe_success", query)
        self.assertIn("== 1", query)

    def test_recent_evaluation_time_cannot_rescue_stale_success(self):
        compose = Mock(side_effect=["", self.response("99.9")])
        with (
            patch(
                "scripts.influx_local_acceptance.time.monotonic",
                side_effect=[0, 0, 90],
            ),
            patch("scripts.influx_local_acceptance.time.sleep"),
            self.assertRaises(acceptance.AcceptanceError),
        ):
            acceptance.check_probe(compose, Decimal("100"))

    def test_docker_started_at_fraction_is_not_rounded(self):
        self.assertEqual(
            acceptance.started_epoch("1970-01-01T00:01:40.000000123Z"),
            Decimal("100.000000123"),
        )


class ContainerHttpTest(unittest.TestCase):
    """合成認証をargvから分離し、生エラーを出さない。"""

    def test_single_write_uses_stdin_and_read_is_not_a_write(self):
        token = "apiv3_" + ("A" * 64)
        data = [
            {"time": stamp, "device": "demo-press", "celsius": value}
            for stamp, value in (("t3", 24.5), ("t2", 23.5), ("t1", 22.5))
        ]
        compose = Mock(side_effect=["\n204", json.dumps(data) + "\n200"])
        acceptance.write_points(compose, token)
        self.assertEqual(acceptance.read_points(compose, token), data)
        initial, query = compose.call_args_list
        for call in (initial, query):
            self.assertNotIn(token, " ".join(call.args))
            self.assertIn(token, call.kwargs["input_text"])
            self.assertEqual(
                call.args[:4], ("exec", "-T", acceptance.INFLUX, "curl")
            )
            self.assertIn("--disable", call.args)
            self.assertTrue(call.args[-1].startswith("http://127.0.0.1:8181/"))
        self.assertIn("/api/v3/write_lp?", initial.args[-1])
        self.assertIn("data-binary =", initial.kwargs["input_text"])
        self.assertIn("/api/v3/query_sql?", query.args[-1])
        self.assertNotIn("data-binary", query.kwargs["input_text"])

    def test_error_status_does_not_reveal_body(self):
        compose = Mock(return_value="untrusted or confidential content\n403")
        with self.assertRaises(acceptance.FixtureError) as caught:
            acceptance.fixture_request(
                compose, "apiv3_" + ("A" * 64), "/api/v3/query_sql", "200"
            )
        self.assertEqual(str(caught.exception), "HTTP_403")

    def test_effective_host_binding_is_rejected_even_if_config_empty(self):
        container = "a" * 64
        compose = Mock(return_value=container)
        for effective in (None, {"8181/tcp": None}, {"8181/tcp": []}):
            docker = Mock(
                side_effect=[
                    "null",
                    json.dumps(effective),
                    "1970-01-01T00:01:40Z",
                ]
            )
            self.assertEqual(
                acceptance.verify_container_boundary(docker, compose),
                (container, Decimal(100)),
            )
        docker = Mock(
            side_effect=[
                "null",
                '{"8181/tcp":[{"HostIp":"127.0.0.1","HostPort":"30123"}]}',
            ]
        )
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.verify_container_boundary(docker, compose)


if __name__ == "__main__":
    unittest.main()

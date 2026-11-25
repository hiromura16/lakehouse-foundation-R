"""ローカル時系列のCompose境界をEngineなしで検査する。"""

import shutil
import unittest
from unittest.mock import patch

import yaml

from scripts import dokploy_capacity as capacity
from scripts import dokploy_plan as plan


class SelectionTest(unittest.TestCase):
    """既存モードを変更せず、coreにだけ明示追加する。"""

    def test_opt_in_can_pair_with_existing_cdc_and_alerting(self):
        files, profiles, expected = plan.recipe(
            "core-local", local_influx=True, cdc=True, alerting=True
        )
        self.assertEqual(
            files,
            [
                "compose.yaml",
                "compose.cdc.yaml",
                "compose.alerting.yaml",
                plan.LOCAL_INFLUX,
            ],
        )
        self.assertEqual(profiles, [])
        self.assertEqual(
            expected - plan.CORE,
            {
                "influxdb-local",
                "native-cdc",
                "alertmanager",
            },
        )
        self.assertIn(
            {
                "family": "local-influx",
                "mode": "core-local",
                "local_influx": True,
                "status": "renderable",
            },
            capacity.lab_choices(),
        )

    def test_capacity_alternative_retains_selected_local_timeseries(self):
        options = {
            "cdc": True,
            "alerting": False,
            "tools_metrics": False,
            "local_influx": True,
        }
        self.assertEqual(
            capacity.alternatives("core-local", options),
            [
                {
                    "mode": "core-local",
                    "options": ["--local-influx"],
                    "fit": "not_assessed",
                }
            ],
        )

    def test_rejects_unrelated_mode_or_tools_metrics(self):
        for mode in (
            "local-operations",
            "standard",
            "legacy-gold-read",
            "evaluation",
            "zero-trust-access",
            "data-egress",
        ):
            with self.subTest(mode=mode), self.assertRaises(plan.PlanError):
                plan.recipe(mode, local_influx=True)
        with self.assertRaises(plan.PlanError):
            plan.recipe("core-local", local_influx=True, tools_metrics=True)


@unittest.skipUnless(
    shutil.which("docker", path=plan.SYSTEM_PATH), "Docker CLI required"
)
class OfflineRenderTest(unittest.TestCase):
    """実Compose CLIを隔離した合成入力だけで使う。"""

    def test_influx_reuses_runtime_spec_with_distinct_identities(self):
        files, profiles, expected = plan.recipe(
            "core-local", local_influx=True
        )
        local = plan.normalize(files, profiles)
        plan.report_model(local, expected)
        old_files, old_profiles, _ = plan.recipe("standard")
        old = plan.normalize(old_files, old_profiles)
        local_spec = local["services"]["influxdb-local"]
        old_spec = old["services"]["influxdb3"]
        self.assertEqual(local["name"], plan.LOCAL_INFLUX_PROJECT)
        self.assertNotEqual(local["name"], old["name"])
        self.assertNotIn("influxdb3", local["services"])
        self.assertNotIn("influxdb-local", old["services"])
        self.assertEqual(
            local_spec["networks"],
            {
                "influx_local": {"aliases": ["influxdb3"]},
            },
        )
        self.assertEqual(local_spec["labels"], {"traefik.enable": "false"})
        differing = {"networks", "labels", "volumes", "secrets", "ports"}
        self.assertEqual(
            {
                key: value
                for key, value in local_spec.items()
                if key not in differing
            },
            {
                key: value
                for key, value in old_spec.items()
                if key not in differing
            },
        )
        self.assertNotIn("tools-influx", local["volumes"])
        self.assertNotIn("influx-local-data", old["volumes"])
        self.assertEqual(
            local["volumes"]["influx-local-data"]["name"],
            plan.LOCAL_INFLUX_PROJECT + "_influx-local-data",
        )
        self.assertEqual(len(local_spec["volumes"]), 1)
        self.assertEqual(
            local_spec["volumes"][0]["source"], "influx-local-data"
        )
        self.assertEqual(
            local_spec["volumes"][0]["target"],
            old_spec["volumes"][0]["target"],
        )
        self.assertNotIn("influx-admin", local["secrets"])
        self.assertEqual(
            local_spec["secrets"],
            [{"source": "influx-local-admin", "target": "influx-admin"}],
        )
        self.assertTrue(
            local["secrets"]["influx-local-admin"]["file"].endswith(
                "/influx-local.placeholder"
            )
        )
        self.assertEqual(local_spec["ports"][0]["host_ip"], "127.0.0.1")
        self.assertEqual(local_spec["ports"][0]["target"], 8181)
        self.assertNotEqual(
            local_spec["ports"][0]["published"],
            old_spec["ports"][0]["published"],
        )

    def test_plan_refuses_unexpected_effective_project(self):
        files, profiles, _ = plan.recipe("core-local", local_influx=True)
        with patch("scripts.dokploy_plan.subprocess.run") as render:
            render.return_value.stdout = b'{"name":"foundation-r-plan"}'
            with self.assertRaisesRegex(plan.PlanError, "UNEXPECTED_PROJECT"):
                plan.normalize(files, profiles)

    def test_missing_operator_port_or_token_fails_closed(self):
        original = plan.synthetic_env
        files, profiles, _ = plan.recipe("core-local", local_influx=True)
        for missing in (
            "LOCAL_INFLUX_PORT",
            "LOCAL_INFLUX_TOKEN_FILE",
            "LOCAL_INFLUX_INSTANCE",
        ):

            def omit(directory, files, key=missing):
                values = original(directory, files)
                values.pop(key)
                return values

            with (
                self.subTest(missing=missing),
                patch("scripts.dokploy_plan.synthetic_env", omit),
                self.assertRaisesRegex(
                    plan.PlanError, "COMPOSE_CONFIG_FAILED"
                ),
            ):
                plan.normalize(files, profiles)

    def test_only_fixed_probe_and_private_influx_network_are_added(self):
        files, profiles, expected = plan.recipe(
            "core-local", local_influx=True, cdc=True, alerting=True
        )
        model = plan.normalize(files, profiles)
        plan.report_model(model, expected)
        services = model["services"]
        self.assertEqual(
            {
                name
                for name, service in services.items()
                if "influx_local" in service.get("networks", {})
            },
            {"influxdb-local", "health-relay"},
        )
        self.assertTrue(model["networks"]["influx_local"]["internal"])
        self.assertFalse(model["networks"]["influx_local"].get("external"))
        self.assertNotIn("ports", services["health-relay"])
        self.assertNotIn("ports", services["prometheus"])
        self.assertIn(
            {
                "source": "influx-local-target",
                "target": "/etc/prometheus/tool-targets/influx-local.yml",
            },
            services["prometheus"]["configs"],
        )
        probe = yaml.safe_load(
            model["configs"]["influx-local-target"]["content"]
        )
        self.assertEqual(
            probe[0]["targets"], ["http://health-relay:8120/influxdb"]
        )
        self.assertEqual(probe[0]["labels"]["module"], "http_ready")
        relay = (plan.ROOT / "native/network/health-relay.conf").read_text()
        self.assertIn("/influxdb http://influxdb3:8181/health;", relay)
        for fixed in (
            "proxy_pass_request_headers off;",
            "proxy_pass_request_body off;",
            "if ($request_method != GET) { return 405; }",
        ):
            self.assertIn(fixed, relay)
        self.assertTrue(
            {"tools_apps", "edge", "edge_read"}.isdisjoint(model["networks"])
        )
        self.assertEqual(
            set(services["native-cdc"]["networks"]), {"writer", "cdc_egress"}
        )
        self.assertEqual(
            services["native-cdc"]["image"], services["lakehouse"]["image"]
        )
        self.assertTrue(
            {"broker", "mosquitto", "greptime", "edge-gold"}.isdisjoint(
                services
            )
        )

    def test_new_capacity_rows_report_local_influx_keys(self):
        files, profiles, _ = plan.recipe("core-local", local_influx=True)
        model = plan.normalize(files, profiles)
        info = capacity.summarize(model, capacity.source_layers(files))
        row = next(
            service
            for service in info["services"]
            if service["service"] == "influxdb-local"
        )
        self.assertEqual(row["compose_files"], [plan.LOCAL_INFLUX])
        self.assertEqual(
            row["memory_mib"], {"status": "bounded", "value": 1536}
        )
        self.assertEqual(info["totals"]["memory_mib"]["known_total"], 6624)
        self.assertEqual(
            {
                name
                for name in row["environment_names"]
                if name.startswith("LOCAL_INFLUX_")
            },
            {"LOCAL_INFLUX_TOKEN_FILE", "LOCAL_INFLUX_PORT"},
        )


if __name__ == "__main__":
    unittest.main()

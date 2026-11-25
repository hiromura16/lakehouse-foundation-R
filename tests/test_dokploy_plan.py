"""合成配備計画が既存Composeと診断の境界を保つか検査する。"""

import contextlib
import io
import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import dokploy_plan as plan


class RecipeTest(unittest.TestCase):
    """未実装や広すぎる組合せを組み立てる前に拒否する。"""

    def test_base_is_unchanged(self):
        self.assertEqual(
            plan.recipe("core-local"), ([plan.BASE], [], plan.CORE)
        )

    def test_order_and_extensions(self):
        files, profiles, services = plan.recipe(
            "standard", cdc=True, alerting=True, tools_metrics=True
        )
        self.assertEqual(
            files,
            [
                "compose.yaml",
                "compose.cdc.yaml",
                "compose.alerting.yaml",
                "compose.gui.yaml",
                "compose.tools.yaml",
            ],
        )
        self.assertEqual(profiles, ["tools-metrics"])
        self.assertIn("influxdb3", services)
        self.assertIn("native-cdc", services)
        self.assertIn("alertmanager", services)
        self.assertTrue(services.isdisjoint(plan.EDGE))

    def test_unsupported_combinations(self):
        for mode, options in (
            ("zero-trust-access", {}),
            ("data-egress", {}),
            ("legacy-gold-read", {"alerting": True}),
            ("core-local", {"tools_metrics": True}),
            ("standard", {"profile": "stream"}),
            ("evaluation", {}),
            ("evaluation", {"profile": "*"}),
            ("evaluation", {"profile": "stream", "cdc": True}),
        ):
            with self.subTest(mode=mode, options=options):
                with self.assertRaises(plan.PlanError):
                    plan.recipe(mode, **options)

    def test_required_values_are_mode_scoped(self):
        directory = Path("/synthetic")
        base = plan.synthetic_env(directory, [plan.BASE])
        self.assertTrue(set(plan.SITE_KEYS).issubset(base))
        self.assertFalse(
            any(key.startswith(("CDC_", "CF_", "TOOLS_")) for key in base)
        )
        files, _, _ = plan.recipe("standard", cdc=True)
        full = plan.synthetic_env(directory, files)
        self.assertIn("CDC_SETTINGS_FILE", full)
        self.assertIn("TOOLS_OBSERVE_USER", full)
        self.assertIn("LAKEHOUSE_OPS_HTPASSWD", full)
        self.assertFalse(any(key.startswith("CF_") for key in full))


class IsolationTest(unittest.TestCase):
    """合成環境と値を出さない失敗経路を確認する。"""

    def test_normalizer_ignores_ambient_settings(self):
        ambient = {
            "CD_STATE_MARKER": "test-should-not-pass",
            "DOCKER_HOST": "unix:///ambient.sock",
            "COMPOSE_FILE": "unexpected.yaml",
            "COMPOSE_PROFILES": "stream",
            "HTTP_PROXY": "ambient",
            "S3_SECRET_KEY": "ambient",
        }
        with (
            patch.dict(os.environ, ambient),
            patch("scripts.dokploy_plan.subprocess.run") as run,
        ):
            run.return_value.stdout = b"{}"
            self.assertEqual(plan.normalize([plan.BASE], []), {})
        args, kwargs = run.call_args
        self.assertEqual(args[0][0:2], ["docker", "compose"])
        self.assertEqual(
            args[0][-4:], ["config", "--no-env-resolution", "--format", "json"]
        )
        env = kwargs["env"]
        self.assertNotIn("COMPOSE_FILE", env)
        self.assertNotIn("COMPOSE_PROFILES", env)
        self.assertNotIn("HTTP_PROXY", env)
        self.assertNotIn("CD_STATE_MARKER", env)
        self.assertNotEqual(env["S3_SECRET_KEY"], "ambient")
        self.assertTrue(env["DOCKER_HOST"].endswith("/no-engine.sock"))
        self.assertEqual(Path(env["DOCKER_CONFIG"]).parent, kwargs["cwd"])
        self.assertNotEqual(Path(env["HOME"]), Path.home())
        self.assertEqual(kwargs["stderr"], subprocess.DEVNULL)
        self.assertEqual(kwargs["stdin"], subprocess.DEVNULL)

    def test_errors_are_fixed_and_do_not_print_command_or_payload(self):
        value = "synthetic-should-not-leave-process"
        stderr, stdout = io.StringIO(), io.StringIO()
        with (
            patch(
                "scripts.dokploy_plan.subprocess.run",
                side_effect=subprocess.CalledProcessError(
                    1, [value], output=value
                ),
            ),
            contextlib.redirect_stderr(stderr),
            contextlib.redirect_stdout(stdout),
        ):
            self.assertEqual(plan.main(["core-local"]), 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            json.loads(stderr.getvalue()), {"error": "COMPOSE_CONFIG_FAILED"}
        )

    def test_catalog_does_not_invoke_docker(self):
        with patch("scripts.dokploy_plan.normalize") as render:
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(plan.main(["catalog"]), 0)
        render.assert_not_called()
        model = json.loads(output.getvalue())
        self.assertEqual(model["planned_not_rendered"], list(plan.PLANNED))
        self.assertEqual(model["evaluation_profiles"], list(plan.EVALUATION))


class ModelTest(unittest.TestCase):
    """出力する項目を限定し、意図しない依存やサービスを拒否する。"""

    def test_model_excludes_values(self):
        service = {
            "environment": {"TOKEN_VALUE": "do-not-print"},
            "ports": [
                {"host_ip": "127.0.0.1", "target": 8080, "published": "99"}
            ],
        }
        summary = plan.report_model(
            {
                "services": {"app": service},
                "volumes": {"data": {"name": "secret-path-or-value"}},
            },
            {"app"},
        )
        self.assertEqual(summary["volumes"], ["data"])
        self.assertEqual(
            summary["ports"],
            [
                {
                    "service": "app",
                    "scope": "loopback",
                    "protocol": "tcp",
                    "target": 8080,
                    "published": 99,
                }
            ],
        )
        for value in ("do-not-print", "secret-path-or-value", "127.0.0.1"):
            self.assertNotIn(value, json.dumps(summary))

    def test_unknown_service_or_relationship_fails(self):
        for service, expected in (
            ({}, {"app", "missing"}),
            ({"depends_on": {"missing": {}}}, {"app"}),
            ({"network_mode": "host"}, {"app"}),
            ({"links": ["different-project"]}, {"app"}),
            ({"volumes_from": ["container:external"]}, {"app"}),
            ({"ports": [{"target": 80, "published": "101-105"}]}, {"app"}),
        ):
            with self.subTest(service=service, expected=expected):
                with self.assertRaises(plan.PlanError):
                    plan.report_model({"services": {"app": service}}, expected)


class ComposeTest(unittest.TestCase):
    """既存Docker CLIで合成構成を正規化する。Engineは使用しない。"""

    @unittest.skipUnless(
        shutil.which("docker", path=plan.SYSTEM_PATH), "Docker CLI required"
    )
    def test_actual_modes(self):
        cases = [
            ("core-local", {}, 2),
            ("core-local", {"cdc": True, "alerting": True}, 2),
            ("local-operations", {}, 5),
            ("standard", {}, 10),
            ("standard", {"tools_metrics": True}, 10),
            ("legacy-gold-read", {}, 2),
        ]
        cases += [
            ("evaluation", {"profile": name}, None) for name in plan.EVALUATION
        ]
        for mode, options, port_count in cases:
            with self.subTest(mode=mode, options=options):
                files, profiles, expected = plan.recipe(mode, **options)
                result = plan.report_model(
                    plan.normalize(files, profiles), expected
                )
                if port_count is not None:
                    self.assertEqual(len(result["ports"]), port_count)
                self.assertEqual(
                    mode == "legacy-gold-read",
                    bool(plan.EDGE.intersection(result["services"])),
                )


if __name__ == "__main__":
    unittest.main()

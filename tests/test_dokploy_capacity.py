"""配備容量の既知分、未計測、宣言責任を分けて検査する。"""

import contextlib
import io
import json
import shutil
import unittest
from unittest.mock import patch

import yaml

from scripts import dokploy_capacity as capacity
from scripts import dokploy_plan as plan


class SourceTest(unittest.TestCase):
    """ソースは値でなく変数名の来歴と構文分類だけに投影する。"""

    def test_compose_interpolation_marks_nested_and_escaped_names(self):
        node = yaml.compose(
            "x: &defaults\n"
            "  build: '${ACTIVE:-${SECOND:?test-message}} $$ESCAPED'\n"
            "services:\n"
            "  app:\n"
            "    <<: *defaults\n"
            "    variable: '${PRESENT?test-message} ${CONDITION:+other}'\n"
            "    plain: '$PLAIN ${SIMPLE}'\n"
            "    escaped: '$${HIDDEN}'\n"
        )
        tree = capacity.source_mapping(node)
        service = capacity.source_mapping(tree["services"])["app"]
        self.assertEqual(
            capacity.env_items([service]),
            {
                "ACTIVE": ["has_default"],
                "CONDITION": ["conditional"],
                "PLAIN": ["no_default"],
                "PRESENT": ["required_present"],
                "SECOND": ["required_nonempty"],
                "SIMPLE": ["no_default"],
            },
        )

    def test_duplicate_yaml_cannot_silently_win(self):
        with self.assertRaisesRegex(
            plan.PlanError, "SOURCE_INVENTORY_UNAVAILABLE"
        ):
            capacity.source_mapping(yaml.compose("x: one\nx: two\n"))

    def test_external_file_directives_rejected_even_through_anchor(self):
        for spec in (
            "include: another.yaml",
            "services: {app: {extends: {file: another.yaml, service: app}}}",
            "x-a: &outside {env_file: .env}\nservices: {app: {<<: *outside}}",
            "services: {app: {label_file: labels.txt}}",
        ):
            with self.subTest(spec=spec):
                with self.assertRaisesRegex(
                    plan.PlanError, "SOURCE_INVENTORY_UNAVAILABLE"
                ):
                    capacity.validate_layers(
                        capacity.source_mapping(yaml.compose(spec))
                    )

    def test_lab_families_show_planned_and_replacement_metadata(self):
        choices = capacity.lab_choices()
        families = {item["family"]: item for item in choices}
        self.assertEqual(len(capacity.MODES), 7)
        self.assertEqual(families["data-egress"]["status"], "planned")
        self.assertEqual(
            families["stream-nisshi"]["replaces"],
            "evaluation:stream:default-broker",
        )
        self.assertEqual(
            families["storage-rustfs"]["replaces"], "core:seaweedfs"
        )
        self.assertEqual(
            {i["profile"] for i in choices if i["family"] == "evaluation"},
            set(plan.EVALUATION),
        )


class ResourceTest(unittest.TestCase):
    """確定している上限だけを資源予算に計上する。"""

    def test_millicpu_rounding_and_unknown_status(self):
        def cpu(service):
            return capacity.limit(service, "cpus", "cpus", 1000, 10**9)

        self.assertEqual(
            cpu({"cpus": "0.2501"}),
            {
                "status": "bounded",
                "value": 251,
            },
        )
        for spec in (
            {"cpus": -1},
            {"cpus": 0},
            {"cpus": True},
            {"cpus": float("nan")},
            {"cpus": 1, "scale": 5},
            {"deploy": {"resources": {"limits": {"cpus": "1.0"}}}},
            {"cpus": 1, "deploy": {"resources": {"limits": {"cpus": "1.01"}}}},
        ):
            with self.subTest(spec=spec):
                self.assertEqual(
                    cpu(spec), {"status": "unknown", "value": None}
                )
        self.assertEqual(cpu({}), {"status": "unbounded", "value": None})

    def test_invalid_deploy_shapes_and_non_singletons_are_unknown(self):
        for service in (
            {"deploy": None},
            {"deploy": False},
            {"deploy": []},
            {"deploy": 0},
            {"deploy": {"resources": False}},
            {"deploy": {"resources": []}},
            {"deploy": {"resources": {"limits": False}}},
            {"deploy": {"resources": {"limits": None}}},
            {"deploy": {"mode": "global"}},
            {"deploy": {"mode": "replicated-job"}},
            {"deploy": {"mode": None}},
            {"deploy": {"replicas": True}},
            {"deploy": {"replicas": "1"}},
            {"deploy": {"replicas": 2}},
            {"scale": True},
            {"scale": "1"},
        ):
            with self.subTest(service=service):
                self.assertEqual(
                    capacity.limit(
                        {"cpus": 1, **service}, "cpus", "cpus", 1000, 10**9
                    ),
                    {"status": "unknown", "value": None},
                )

    def test_compare_exact_before_rounding_and_reject_extreme_exponents(self):
        def cpu(value, other):
            spec = {
                "cpus": value,
                "deploy": {
                    "mode": "replicated",
                    "replicas": 1,
                    "resources": {"limits": {"cpus": other}},
                },
                "scale": 1,
            }
            return capacity.limit(spec, "cpus", "cpus", 1000, 10**9)

        self.assertEqual(
            cpu("1.0001", "1.000100"),
            {"status": "bounded", "value": 1001},
        )
        for lhs, rhs in (
            ("1.0001", "1.0002"),
            ("1e99999999", "1e99999999"),
            ("1e-99999999", "1e-99999999"),
            (1, None),
            (None, 1),
        ):
            with self.subTest(lhs=lhs, rhs=rhs):
                self.assertEqual(
                    cpu(lhs, rhs), {"status": "unknown", "value": None}
                )

    def test_unbounded_is_not_fit_even_if_capacity_exceeds_known_total(self):
        total = {
            dimension: {"known_total": 100, "unknown": [], "unbounded": []}
            for dimension in capacity.DIMS
        }
        reserve = {
            key: capacity.synthetic_pair("10:10") for key in capacity.RESERVES
        }
        self.assertEqual(
            capacity.budget(
                total, capacity.synthetic_pair("160:160"), reserve
            )["assessment"],
            "fits_static_envelope",
        )
        total["memory_mib"]["unbounded"].append("one-shot")
        self.assertEqual(
            capacity.budget(
                total, capacity.synthetic_pair("160:160"), reserve
            )["assessment"],
            "indeterminate",
        )

    def test_deny_if_known_shortfall_even_if_inputs_missing(self):
        total = {
            dimension: {
                "known_total": 100,
                "unknown": ["service"],
                "unbounded": [],
            }
            for dimension in capacity.DIMS
        }
        reserve = {"os": capacity.synthetic_pair("10:20")}
        result = capacity.budget(
            total, capacity.synthetic_pair("109:119"), reserve
        )
        self.assertEqual(result["assessment"], "deny")
        self.assertEqual(
            result["dimensions"]["cpu_milli"]["known_minimum_shortfall"], 1
        )
        self.assertEqual(
            result["dimensions"]["memory_mib"]["known_minimum_capacity"], 120
        )
        self.assertTrue(
            result["dimensions"]["memory_mib"]["unknown_or_unbounded"]
        )
        self.assertIn("traefik", result["missing_reserves"])

    def test_out_of_scope_or_invalid_numbers_are_not_output(self):
        stderr, stdout = io.StringIO(), io.StringIO()
        with (
            patch("scripts.dokploy_plan.normalize") as normalize,
            contextlib.redirect_stderr(stderr),
            contextlib.redirect_stdout(stdout),
        ):
            self.assertEqual(
                capacity.main(
                    ["standard", "--synthetic-capacity", "do-not-log"]
                ),
                1,
            )
        normalize.assert_not_called()
        self.assertEqual(
            json.loads(stderr.getvalue()), {"error": "INVALID_INPUT"}
        )
        self.assertEqual(stdout.getvalue(), "")

    def test_planned_modes_are_inventory_not_renders(self):
        for mode in ("all-lab", *plan.PLANNED):
            with (
                patch("scripts.dokploy_plan.normalize") as normalize,
                contextlib.redirect_stdout(io.StringIO()) as out,
            ):
                self.assertEqual(capacity.main([mode]), 0)
            normalize.assert_not_called()
            output = json.loads(out.getvalue())
            self.assertEqual(output["mode"], mode)
            self.assertEqual(output["static_status"], "NOT_IMPLEMENTED")
            self.assertFalse(output["startup_verified"])


@unittest.skipUnless(
    shutil.which("docker", path=plan.SYSTEM_PATH), "Docker CLI required"
)
class ComposeInventoryTest(unittest.TestCase):
    """既存CLIの合成結果をdaemonなしで投影する。"""

    def capture(self, *argv):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            result = capacity.main(list(argv))
        return result, json.loads(out.getvalue())

    def test_core_and_tools_tallies_have_one_row_per_active_service(self):
        for mode, cpu, memory, service_count in (
            ("core-local", 2000, 5088, 10),
            ("local-operations", 3000, 5696, 12),
            ("standard", 3000, 11168, 18),
        ):
            with self.subTest(mode=mode):
                status, result = self.capture(mode)
                self.assertEqual(status, 0)
                self.assertEqual(len(result["services"]), service_count)
                self.assertEqual(
                    result["totals"]["cpu_milli"]["known_total"], cpu
                )
                self.assertEqual(
                    result["totals"]["memory_mib"]["known_total"], memory
                )
                services = {row["service"]: row for row in result["services"]}
                self.assertIn(
                    "S3_ACCESS_KEY", services["storage"]["environment_names"]
                )
                self.assertEqual(
                    services["catalog-migrate"]["memory_mib"]["status"],
                    "unbounded",
                )
                self.assertNotIn("synthetic-only", json.dumps(services))
                self.assertNotIn(
                    "operator@example.invalid", json.dumps(services)
                )
                if mode != "core-local":
                    self.assertEqual(
                        services["catalog"]["compose_files"],
                        ["compose.yaml", "compose.gui.yaml"],
                    )
                    self.assertEqual(
                        services["ops-proxy"]["environment_names"][
                            "LAKEHOUSE_OPS_HTPASSWD"
                        ],
                        ["required_nonempty"],
                    )

    def test_budget_exit_codes_and_explicit_alternatives(self):
        denial, tiny = self.capture("standard", "--synthetic-capacity", "1:1")
        self.assertEqual(denial, 3)
        self.assertEqual(tiny["budget"]["assessment"], "deny")
        self.assertTrue(tiny["budget"]["comparison_candidates"])
        unknown, large = self.capture(
            "standard",
            "--synthetic-capacity",
            "20000:65536",
            "--reserve-os",
            "1000:512",
            "--reserve-docker",
            "1000:512",
            "--reserve-dokploy",
            "1000:512",
            "--reserve-traefik",
            "1000:512",
            "--reserve-existing-poc",
            "1000:2048",
            "--reserve-headroom",
            "1000:1024",
        )
        self.assertEqual(unknown, 4)
        self.assertEqual(large["budget"]["assessment"], "indeterminate")
        self.assertEqual(large["budget"]["missing_reserves"], [])

    def test_all_implemented_plans_report_attributed_services(self):
        cases = [
            ("standard", "--cdc", "--alerting", "--tools-metrics"),
            ("legacy-gold-read",),
            *(("evaluation", "--profile", name) for name in plan.EVALUATION),
        ]
        for args in cases:
            with self.subTest(args=args):
                status, result = self.capture(*args)
                self.assertEqual(status, 0)
                for row in result["services"]:
                    self.assertTrue(row["compose_files"])
                    self.assertTrue(
                        set(row["compose_files"]).issubset(
                            result["compose_files"]
                        )
                    )
                self.assertIn(
                    "LANDING_DIR", result["source_environment_names"]
                )

    def test_comparison_run_discloses_external_runtime_prerequisite(self):
        _, result = self.capture(
            "evaluation", "--profile", "distributed-check"
        )
        self.assertEqual(
            result["runtime_prerequisites"], ["distributed-runtime"]
        )
        self.assertIn(
            "ballista-check", {row["service"] for row in result["services"]}
        )


if __name__ == "__main__":
    unittest.main()

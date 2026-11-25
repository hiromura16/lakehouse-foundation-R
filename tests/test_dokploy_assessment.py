"""オフラインの資源判定が未知・衝突・閾値を区別するか検査する。"""

import contextlib
import io
import json
import unittest
from pathlib import Path

from scripts import dokploy_assessment as assessment

SAMPLE = (
    Path(__file__).resolve().parents[1]
    / "examples/dokploy/operator-assessment.synthetic.json"
)


def example():
    """明示の合成値を毎回別のレコードとして返す。"""
    return json.loads(SAMPLE.read_bytes())


def complete_example():
    """既知分が予算内である合成観測を組み立てる。"""
    record = example()
    for dim in assessment.DIMS:
        record["workload"][dim]["complete"] = True
        record["reserves"]["existing_poc"][dim] = 0
    record["workload"]["disk_mib"]["known_floor"] = 1024
    record["measured"].update(
        cpu_busy_pct=40,
        cpu_window_minutes=15,
        memory_available_mib=22000,
        disk_free_mib=64000,
        swap_in_delta_pages=0,
        swap_out_delta_pages=0,
        swap_window_minutes=15,
    )
    record["collisions"] = {
        kind: {"conflicts": 0, "complete": True}
        for kind in assessment.COLLISIONS
    }
    return record


def small_complete_example():
    """閾値ちょうどを整数のまま比較できる合成VM。"""
    record = complete_example()
    for dimension in assessment.DIMS:
        record["capacity"][dimension] = 1000
        record["workload"][dimension]["known_floor"] = 200
        for reservation in record["reserves"].values():
            reservation[dimension] = 0
    record["measured"].update(memory_available_mib=450, disk_free_mib=501)
    return record


def check(record):
    """正式入力検査と算定の両方を通す。"""
    content = io.BytesIO(json.dumps(record).encode("utf-8"))
    return assessment.assess(assessment.parse_snapshot(content))


class OfflineEvidenceTest(unittest.TestCase):
    """起動や機器inventoryの証拠に読み替えない。"""

    def test_safe_unknown_default_sample(self):
        result = check(example())
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertTrue(result["resources"]["memory_mib"]["incomplete_bound"])
        self.assertFalse(result["startup_verified"])
        self.assertFalse(result["deployment_approved"])

    def test_documented_pass_is_aggregate_only(self):
        result = check(complete_example())
        self.assertEqual(result["status"], "PASS")
        self.assertFalse(result["startup_verified"])
        self.assertFalse(result["deployment_approved"])

    def test_positive_shortfall_survives_missing_evidence(self):
        snapshot = example()
        snapshot["capacity"]["memory_mib"] = 8192
        result = check(snapshot)
        self.assertEqual(result["status"], "INSUFFICIENT")
        row = result["resources"]["memory_mib"]
        self.assertEqual(row["known_lower_bound_capacity"], 22400)
        self.assertEqual(row["known_lower_bound_expansion"], 14208)
        self.assertTrue(row["incomplete_bound"])

    def test_warning_thresholds_and_swap_deltas(self):
        snapshot = complete_example()
        snapshot["thresholds"] = {"cpu_warn_pct": 82, "memory_margin_pct": 20}
        snapshot["workload"]["memory_mib"]["known_floor"] = 5000
        snapshot["measured"].update(
            cpu_busy_pct=82,
            memory_available_mib=6500,
            disk_free_mib=19000,
            swap_in_delta_pages=1,
        )
        result = check(snapshot)
        self.assertEqual(result["status"], "WARN")
        reasons = {item["reason"] for item in result["findings"]}
        self.assertIn("SUSTAINED_CPU_WARN", reasons)
        self.assertIn("MEMORY_AVAILABLE_BELOW_MARGIN", reasons)
        self.assertIn("DISK_CRITICAL", reasons)
        self.assertIn("SWAP_ACTIVITY_OBSERVED", reasons)

    def test_disk_stop_and_collision_are_definite_denials(self):
        snapshot = complete_example()
        snapshot["measured"]["disk_free_mib"] = 1000
        snapshot["collisions"]["network"] = {"conflicts": 1, "complete": False}
        result = check(snapshot)
        self.assertEqual(result["status"], "INSUFFICIENT")
        reasons = {item["reason"] for item in result["findings"]}
        self.assertIn("DISK_STOP", reasons)
        self.assertIn("EXISTING_POC_COLLISION", reasons)
        self.assertIn("COLLISION_COMPARE_INCOMPLETE", reasons)

    def test_observed_free_may_not_hold_additional_workload(self):
        snapshot = complete_example()
        snapshot["measured"]["memory_available_mib"] = 10000
        result = check(snapshot)
        self.assertEqual(result["status"], "INSUFFICIENT")
        self.assertIn(
            {
                "check": "memory_mib",
                "status": "INSUFFICIENT",
                "reason": "OBSERVED_FREE_SHORTFALL",
            },
            result["findings"],
        )

    def test_prechecks_pass_but_projected_resources_do_not(self):
        snapshot = small_complete_example()
        snapshot["workload"]["disk_mib"]["known_floor"] = 290
        snapshot["measured"].update(
            memory_available_mib=251, disk_free_mib=301
        )
        result = check(snapshot)
        self.assertEqual(result["status"], "INSUFFICIENT")
        self.assertEqual(
            result["projected_remaining_upper_bound_mib"],
            {"memory_mib": 51, "disk_mib": 11},
        )
        found = {item["reason"]: item["status"] for item in result["findings"]}
        self.assertEqual(found["MEMORY_MARGIN_OBSERVED"], "PASS")
        self.assertEqual(found["DISK_BELOW_WARN"], "PASS")
        self.assertEqual(
            found["PROJECTED_MEMORY_AVAILABLE_BELOW_MARGIN"], "WARN"
        )
        self.assertEqual(found["PROJECTED_DISK_STOP"], "INSUFFICIENT")

    def test_inclusive_disk_boundaries_and_inclusive_ram_margin(self):
        for start, verdict, disk_reason in (
            (501, "PASS", "PROJECTED_DISK_BELOW_WARN"),
            (500, "WARN", "PROJECTED_DISK_WARN"),
            (351, "WARN", "PROJECTED_DISK_WARN"),
            (350, "WARN", "PROJECTED_DISK_CRITICAL"),
            (251, "WARN", "PROJECTED_DISK_CRITICAL"),
            (250, "INSUFFICIENT", "PROJECTED_DISK_STOP"),
        ):
            with self.subTest(start=start):
                snapshot = small_complete_example()
                snapshot["measured"]["disk_free_mib"] = start
                result = check(snapshot)
                self.assertEqual(result["status"], verdict)
                found = {item["reason"] for item in result["findings"]}
                self.assertIn(disk_reason, found)
                self.assertIn("PROJECTED_MEMORY_MARGIN_OBSERVED", found)
        snapshot = small_complete_example()
        snapshot["measured"]["memory_available_mib"] -= 1
        self.assertEqual(check(snapshot)["status"], "WARN")

    def test_projection_subtracts_only_new_floor_and_keeps_unknown(self):
        snapshot = small_complete_example()
        snapshot["workload"]["memory_mib"]["complete"] = False
        snapshot["reserves"]["existing_poc"]["memory_mib"] = 80
        result = check(snapshot)
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(
            result["projected_remaining_upper_bound_mib"]["memory_mib"], 250
        )
        self.assertIn(
            {
                "check": "projected_memory",
                "status": "UNKNOWN",
                "reason": "PROJECTED_WORKLOAD_NOT_BOUNDED",
            },
            result["findings"],
        )

    def test_short_sampling_does_not_certify_idle(self):
        snapshot = complete_example()
        snapshot["measured"]["cpu_window_minutes"] = 14
        snapshot["measured"]["swap_window_minutes"] = 14
        self.assertEqual(check(snapshot)["status"], "UNKNOWN")

    def test_modes_without_deployable_recipe_are_not_implemented(self):
        for name in ("zero-trust-access", "data-egress", "all-lab"):
            self.assertEqual(
                check(complete_example() | {"mode": name})["status"],
                "NOT_IMPLEMENTED",
            )


class InputSafetyTest(unittest.TestCase):
    """未知フィールド、不正な型、個別情報の漏えいを拒否する。"""

    def test_invalid_types_thresholds_and_identifiers_are_rejected(self):
        for section, key, value in (
            ("capacity", "cpu_milli", True),
            ("capacity", "disk_mib", "100 GiB"),
            ("measured", "memory_available_mib", 2**20),
            ("measured", "total_swap_mib", 0),
            ("thresholds", "disk_stop_pct", 30),
            ("thresholds", "memory_margin_pct", 0),
            ("collisions", "network", {"names": ["do-not-log"]}),
        ):
            with self.subTest(section=section, key=key):
                snapshot = example()
                snapshot[section][key] = value
                with self.assertRaises(assessment.SnapshotError):
                    check(snapshot)

    def test_duplicate_json_nan_and_oversize_are_rejected(self):
        for value in (
            b'{"version": 1, "version": 2}',
            b'{"version": NaN}',
            b" " * (assessment.MAX_BYTES + 1),
        ):
            with self.subTest(length=len(value)):
                with self.assertRaises(assessment.SnapshotError):
                    assessment.parse_snapshot(io.BytesIO(value))

    def test_invalid_usage_is_not_warn_and_does_not_echo_argument(self):
        with contextlib.redirect_stderr(io.StringIO()) as err:
            with self.assertRaises(SystemExit) as raised:
                assessment.main(["--do-not-echo"], io.BytesIO(b""))
        self.assertEqual(raised.exception.code, 6)
        self.assertEqual(
            json.loads(err.getvalue()), {"error": "INVALID_USAGE"}
        )

    def test_cli_exit_codes_and_bad_input_redaction(self):
        for record, expected in (
            (example(), 4),
            (complete_example(), 0),
            (complete_example() | {"mode": "data-egress"}, 5),
            (complete_example() | {"private_field": "do-not-log"}, 1),
        ):
            source = io.BytesIO(json.dumps(record).encode("utf-8"))
            with (
                contextlib.redirect_stdout(io.StringIO()) as out,
                contextlib.redirect_stderr(io.StringIO()) as err,
            ):
                self.assertEqual(assessment.main([], source), expected)
            if expected == 1:
                self.assertEqual(
                    json.loads(err.getvalue()),
                    {
                        "error": "INVALID_SNAPSHOT",
                    },
                )
                self.assertEqual(out.getvalue(), "")


if __name__ == "__main__":
    unittest.main()

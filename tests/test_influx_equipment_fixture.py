"""合成設備送り手の契約と隔離境界を、実ネットワークなしで検査する。"""

import importlib.util
import json
import unittest
from datetime import UTC, datetime
from unittest.mock import Mock

from native import sensor_contract
from scripts import influx_equipment_fixture as fixture
from scripts import influx_local_acceptance as acceptance

PROJECT = "foundation-r-influx-local-ci-0123456789abcdef"
INFLUX = "1" * 64
RELAY = "2" * 64
NETWORK = "3" * 64
IMAGE = "sha256:" + "4" * 64


def docker_outputs(*, bad_owner=False, additional=False, volume=False):
    """合成のDocker観測だけを返す。"""

    def label(name):
        return json.dumps(
            {
                "com.docker.compose.project": PROJECT,
                "com.docker.compose.service": name,
            }
        )

    members = {INFLUX: {}, RELAY: {}}
    if additional:
        members["5" * 64] = {}
    network = {
        "Id": NETWORK,
        "Name": PROJECT + "_influx_local",
        "Labels": {
            "com.docker.compose.project": "different"
            if bad_owner
            else PROJECT,
            "com.docker.compose.network": "influx_local",
        },
        "Driver": "bridge",
        "Internal": True,
        "Containers": members,
    }
    return [
        label(acceptance.INFLUX),
        INFLUX,
        label("health-relay"),
        RELAY,
        json.dumps(network),
        IMAGE,
        json.dumps({"/auto-volume": {}} if volume else None),
    ]


class ContractTest(unittest.TestCase):
    """既存の設備契約と合成専用投影を区別する。"""

    @unittest.skipUnless(
        importlib.util.find_spec("pyarrow"), "Arrow unavailable"
    )
    def test_native_sensor_legacy_exports_are_identical(self):
        from native import sensor

        self.assertIs(sensor.point, sensor_contract.point)
        self.assertIs(sensor.IDENTITY, sensor_contract.IDENTITY)
        self.assertIs(sensor.COLUMNS, sensor_contract.COLUMNS)

    def test_projection_and_exact_readback_keep_fields_and_microseconds(self):
        lines = fixture.fixture_lines(datetime(2026, 1, 1, tzinfo=UTC))
        payload, expected = fixture.project_points(lines)
        self.assertEqual(len(payload.splitlines()), 2)
        self.assertEqual(expected[0]["sequence_no"], 1)
        self.assertEqual(expected[0]["quality_code"], "UNCERTAIN")
        self.assertEqual(expected[1]["value"], 24.5)
        self.assertEqual(expected[0]["time"].microsecond, 123456)
        rows = [
            {**item, "time": item["time"].isoformat().replace("+00:00", "000")}
            for item in expected
        ]
        self.assertEqual(fixture.normalize_rows(json.dumps(rows)), expected)
        rows[0]["time"] = rows[0]["time"][:-1] + "1"
        with self.assertRaises(ValueError):
            fixture.normalize_rows(json.dumps(rows))

    def test_collision_is_refused_without_assuming_influx_identity(self):
        lines = fixture.fixture_lines(datetime(2026, 1, 1, tzinfo=UTC))
        conflicting = json.loads(lines[0])
        conflicting["sequence_no"] += 1
        with self.assertRaisesRegex(ValueError, "TIMESTAMP_COLLISION"):
            fixture.project_points([lines[0], json.dumps(conflicting)])
        conflicting["quality_code"] = "unsupported"
        with self.assertRaisesRegex(ValueError, "invalid_quality"):
            fixture.project_points([json.dumps(conflicting)])


class ScopeTest(unittest.TestCase):
    """別の送信元は所有確認済みの内部networkにだけ接続する。"""

    def test_unexpected_membership_owner_and_implicit_mount_are_denied(self):
        compose = Mock(return_value=RELAY)
        for change in (
            {"bad_owner": True},
            {"additional": True},
            {"volume": True},
        ):
            with self.subTest(change=change):
                docker = Mock(side_effect=docker_outputs(**change))
                with self.assertRaises(acceptance.AcceptanceError):
                    acceptance.producer_context(
                        docker, compose, PROJECT, INFLUX
                    )
        self.assertEqual(
            acceptance.producer_context(
                Mock(side_effect=docker_outputs()), compose, PROJECT, INFLUX
            ),
            (NETWORK, IMAGE),
        )

    def test_fixture_client_uses_existing_image_and_stdin_only(self):
        docker = Mock(return_value="\n204")
        compose = Mock()
        producer = acceptance.producer_command(
            docker, PROJECT, (NETWORK, IMAGE)
        )
        token = "apiv3_" + ("A" * 64)
        acceptance.fixture_request(
            compose,
            token,
            fixture.write_path(),
            "204",
            "synthetic",
            producer=producer,
        )
        compose.assert_not_called()
        args = docker.call_args.args
        self.assertEqual(args[:4], ("run", "--rm", "-i", "--pull"))
        self.assertIn("never", args)
        self.assertIn(NETWORK, args)
        self.assertIn(IMAGE, args)
        self.assertIn("65534:65534", args)
        self.assertTrue(
            args[-1].startswith(fixture.ORIGIN + "/api/v3/write_lp")
        )
        self.assertFalse(
            {"-p", "-P", "--publish", "--volume", "--mount"} & set(args)
        )
        self.assertNotIn(token, " ".join(args))
        self.assertIn(token, docker.call_args.kwargs["input_text"])
        with self.assertRaises(acceptance.AcceptanceError):
            producer("sh", timeout=1, input_text="")


if __name__ == "__main__":
    unittest.main()

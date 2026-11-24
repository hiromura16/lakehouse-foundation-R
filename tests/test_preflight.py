"""Compose配備前のファイル・ネットワーク診断を読み取り専用で検査する。"""

import io
import json
import unittest
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from ipaddress import ip_network
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts import preflight


def subnet(value):
    """テスト値をネットワークとして比較する。"""
    return ip_network(value)


def model(networks, used=None):
    """Composeから取得する有効サービスのJSON構造にそろえる。"""
    return {
        "name": "fixture",
        "services": {"app": {"networks": dict.fromkeys(used or networks)}},
        "networks": networks,
    }


def live(key, prefix, *, project="fixture", scope="local"):
    """Docker Engineで存在する割当を模擬する。"""
    return {
        "name": f"{project}_{key}",
        "scope": scope,
        "ipam": "default",
        "config": [{"Subnet": prefix}],
        "project": project,
        "key": key,
    }


class RouteTest(unittest.TestCase):
    """経路の所属にかかわらず、重複判定に使うネットワークを取得する。"""

    def test_policy_table_and_direct_route(self):
        """VPNの追加経路を拾い、経路の重複を生むローカル行を省く。"""
        with patch.object(
            preflight,
            "read_json",
            side_effect=[
                [
                    {"dst": "default"},
                    {"dst": "192.0.2.0/24", "scope": "link"},
                    {"dst": "198.51.100.0/24", "table": "52"},
                    {"dst": "192.0.2.4", "type": "local"},
                ],
                [{"dst": "2001:db8:5::/64", "table": "52"}],
            ],
        ) as read:
            direct, other = preflight.routes()
        self.assertEqual(direct, {subnet("192.0.2.0/24")})
        self.assertEqual(
            other, {subnet("198.51.100.0/24"), subnet("2001:db8:5::/64")}
        )
        for args in read.call_args_list:
            self.assertEqual(args.args[0][-2:], ["table", "all"])


class FileTest(unittest.TestCase):
    """使用中の参照元だけを診断し、パスやファイル内容を公開しない。"""

    def test_required_bind_and_build_secret_are_checked(self):
        """未使用の定義を除き、必須パスと通常ファイルを確認する。"""
        with TemporaryDirectory() as directory:
            data = Path(directory)
            missing = data / "required"
            optional = data / "create"
            doc = {
                "services": {
                    "app": {
                        "build": {"secrets": [{"source": "build-id"}]},
                        "secrets": [{"source": "from-env"}],
                        "configs": [{"source": "from-inline"}],
                        "volumes": [
                            {
                                "type": "bind",
                                "source": str(missing),
                                "bind": {"create_host_path": False},
                            },
                            {"type": "bind", "source": str(optional)},
                        ],
                    }
                },
                "secrets": {
                    "build-id": {"file": str(data)},
                    "from-env": {"environment": "OPERATOR_VALUE"},
                    "unused": {"file": str(data / "unused")},
                },
                "configs": {"from-inline": {"content": "configured"}},
            }
            result = preflight.file_findings(doc)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(
            [result[k] for k in ("total", "missing", "invalid", "review")],
            [3, 1, 1, 1],
        )
        self.assertNotIn(str(data), json.dumps(result))


class NetworkTest(unittest.TestCase):
    """自社経路、既存Compose、未使用プロファイルを分けて扱う。"""

    def test_used_networks_only_and_reused_network(self):
        """同じ割当の再利用を競合扱いせず、新規bridgeだけ数える。"""
        spec = model(
            {
                "reuse": {
                    "name": "fixture_reuse",
                    "ipam": {"config": [{"subnet": "172.22.0.0/16"}]},
                },
                "new": {"name": "fixture_new"},
                "unused": {"name": "fixture_unused"},
                "v6": {"name": "fixture_v6", "enable_ipv4": False},
            },
            used=["reuse", "new", "v6"],
        )
        counts, _reserved, required = preflight.bridge_plan(
            spec, [live("reuse", "172.22.0.0/16")], {subnet("172.22.0.0/16")}
        )
        self.assertEqual(required, 1)
        self.assertFalse(any(counts.values()))

    def test_static_collision_warnings_and_external_network(self):
        """標準IPAMは競合を検知し、会社用VPNと外部構成を区別する。"""
        spec = model(
            {
                "first": {
                    "name": "fixture_first",
                    "ipam": {"config": [{"subnet": "10.20.0.0/24"}]},
                },
                "second": {
                    "name": "fixture_second",
                    "ipam": {"config": [{"subnet": "10.20.0.32/27"}]},
                },
                "external": {"name": "external", "external": True},
                "plugin": {"name": "plugin", "driver": "custom"},
            }
        )
        counts, _reserved, required = preflight.bridge_plan(
            spec,
            [live("other", "10.20.0.0/16", project="other")],
            {subnet("10.20.0.0/16"), subnet("fd00::/64")},
        )
        self.assertEqual(required, 0)
        self.assertEqual(counts["docker_overlap"], 2)
        self.assertEqual(counts["route_overlap"], 2)
        self.assertEqual(counts["external_missing"], 1)
        self.assertEqual(counts["custom_driver"], 1)

    def test_changed_owned_network_is_checked_against_other_projects(self):
        """同じ名前でも範囲が変わる場合は他の割当と重複しないか調べる。"""
        spec = model(
            {
                "reuse": {
                    "name": "fixture_reuse",
                    "ipam": {"config": [{"subnet": "10.11.0.0/16"}]},
                },
            }
        )
        counts, *_ = preflight.bridge_plan(
            spec,
            [
                live("reuse", "10.10.0.0/16"),
                live("n", "10.11.1.0/24", project="other"),
            ],
            set(),
        )
        self.assertEqual(counts["recreate_review"], 1)
        self.assertEqual(counts["docker_overlap"], 1)

    def test_stock_and_large_custom_pool_without_duplicate_blocking(self):
        """複数の重複する予約と大きなプールで空き数を正しく数える。"""
        self.assertEqual(
            preflight.available_slots(preflight.ipv4_pools(None), []), 31
        )
        pools = preflight.ipv4_pools([{"Base": "10.0.0.0/8", "Size": 32}])
        reservations = [
            subnet("10.0.0.0/9"),
            subnet("10.0.0.0/24"),
            subnet("fd00::/64"),
        ]
        self.assertEqual(preflight.available_slots(pools, reservations), 2**23)

    def test_pool_shortage_is_advisory(self):
        """空きが足りない場合も予測結果を警告として返す。"""
        with patch.object(preflight, "read_json", return_value=None):
            result = preflight.pool_finding(
                ["docker"],
                [subnet("172.16.0.0/12")],
                {subnet("192.168.0.0/16")},
                1,
            )
        self.assertEqual(result["status"], "WARN")
        self.assertEqual(result["available_estimate"], 0)


class CommandTest(unittest.TestCase):
    """Docker CLIの読み取りだけに限定し、入力と生出力を報告しない。"""

    def test_compose_flags_keep_repeat_order(self):
        args = Namespace(
            env_file=["one", "two"],
            file=["base", "overlay"],
            profile=["a", "b"],
            project_name="fixture",
        )
        command = preflight.compose_command(["sudo", "-n", "docker"], args)
        self.assertEqual(
            command,
            [
                "sudo",
                "-n",
                "docker",
                "compose",
                "--env-file",
                "one",
                "--env-file",
                "two",
                "-f",
                "base",
                "-f",
                "overlay",
                "--profile",
                "a",
                "--profile",
                "b",
                "-p",
                "fixture",
                "config",
                "--format",
                "json",
            ],
        )

    def test_remote_checks_skip_host_and_leave_private_model_internal(self):
        config = model({"default": {"name": "fixture_default"}})
        config["services"]["app"]["environment"] = {
            "AUTH": "secret-fixture-value"
        }
        out = io.StringIO()
        with (
            patch("sys.argv", ["preflight", "--json"]),
            patch.object(preflight, "read_json", return_value=config),
            patch.object(preflight, "read", return_value="linux"),
            patch.object(preflight, "host_mode", return_value=(False, False)),
            patch.object(preflight, "active_networks", return_value=[]),
            patch.object(preflight, "file_findings") as files,
            patch.object(preflight, "routes") as routes,
            redirect_stdout(out),
        ):
            self.assertEqual(preflight.main(), 0)
        files.assert_not_called()
        routes.assert_not_called()
        self.assertNotIn("secret-fixture-value", out.getvalue())
        data = {
            item["code"]: item["status"]
            for item in json.loads(out.getvalue())["findings"]
        }
        self.assertEqual(data["HOST_FILES"], "SKIP")
        self.assertEqual(data["HOST_ROUTES"], "SKIP")
        self.assertEqual(data["IPV4_POOL"], "SKIP")

    def test_bad_arg_diagnostic_has_no_private_argument(self):
        err = io.StringIO()
        with (
            patch("sys.argv", ["preflight", "--PRIVATE-PATH"]),
            redirect_stderr(err),
            self.assertRaises(SystemExit) as error,
        ):
            preflight.main()
        self.assertEqual(error.exception.code, 2)
        self.assertNotIn("PRIVATE-PATH", err.getvalue())

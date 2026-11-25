#!/usr/bin/env python3
"""信頼したComposeを合成値だけで展開し、配備計画を確認する。"""

import argparse
import ipaddress
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SYSTEM_PATH = "/usr/local/bin:/usr/bin:/bin"
BASE = "compose.yaml"
CORE = frozenset(
    (
        "catalog-db",
        "catalog-migrate",
        "catalog",
        "storage",
        "lakehouse",
        "blackbox",
        "prometheus",
        "grafana",
        "read-api",
        "health-relay",
    )
)
GUI = frozenset(("data-browser", "ops-proxy"))
TOOLS = frozenset(
    (
        "tools-db",
        "n8n",
        "openobserve",
        "influxdb3",
        "influx-explorer",
        "tools-portal",
    )
)
EDGE = frozenset(("edge-gold", "cloudflared"))
EVALUATION = {
    "sail": frozenset(("sail",)),
    "stream": frozenset(("broker", "risingwave")),
    "timeseries": frozenset(("greptime",)),
    "distributed": frozenset(("ballista-scheduler", "ballista-executor")),
    "distributed-check": frozenset(("ballista-check",)),
    "accelerator": frozenset(("comet",)),
}
MODES = (
    "core-local",
    "local-operations",
    "standard",
    "legacy-gold-read",
    "evaluation",
    "catalog",
)
PLANNED = ("zero-trust-access", "data-egress")
KEY = re.compile(r"[A-Za-z0-9_.-]+", flags=re.ASCII)

SITE_KEYS = (
    "CATALOG_DB_PASSWORD",
    "CATALOG_ENCRYPTION_KEY",
    "GRAFANA_ADMIN_PASSWORD",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "WORKER_TOKEN",
    "READER_TOKEN",
)
TOOLS_KEYS = (
    "TOOLS_DB_PASSWORD",
    "TOOLS_N8N_KEY",
    "TOOLS_EXPLORER_KEY",
    "TOOLS_OBSERVE_PASSWORD",
)
PATH_KEYS = {
    "LANDING_DIR": "landing",
    "LAKEHOUSE_OPS_HTPASSWD": "ops.htpasswd",
    "TOOLS_HTPASSWD": "tools.htpasswd",
    "TOOLS_INFLUX_TOKEN_FILE": "influx.placeholder",
    "TOOLS_OBSERVE_PASSCODE_FILE": "observe.placeholder",
    "CDC_STATE_DIR": "cdc",
    "CDC_SETTINGS_FILE": "cdc/settings.placeholder",
    "CDC_TABLE_MAP_FILE": "cdc/table-map.placeholder",
    "CF_TUNNEL_TOKEN_FILE": "edge/token.placeholder",
    "CF_EDGE_POLICY_FILE": "edge/policy.placeholder",
    "CF_EDGE_READER_FILE": "edge/reader.placeholder",
}


class PlanError(Exception):
    """固定理由だけを外部に伝える。"""


class Parser(argparse.ArgumentParser):
    """誤って追加された秘密値を使用法エラーに印字しない。"""

    def error(self, _message):
        self.print_usage(sys.stderr)
        self.exit(2, "引数を確認してください。詳細は --help にあります。\n")


def recipe(
    mode, *, cdc=False, alerting=False, profile=None, tools_metrics=False
):
    """既存の合成順だけを選び、未知・未実装の組合せを展開しない。"""
    files = [BASE]
    expected = CORE
    if mode in PLANNED:
        raise PlanError("MODE_NOT_IMPLEMENTED")
    if mode not in MODES or mode == "catalog":
        raise PlanError("INVALID_SELECTION")
    if cdc or alerting:
        if mode not in ("core-local", "local-operations", "standard"):
            raise PlanError("INVALID_SELECTION")
    if cdc:
        files.append("compose.cdc.yaml")
        expected |= {"native-cdc"}
    if alerting:
        files.append("compose.alerting.yaml")
        expected |= {"alertmanager"}
    profiles = []
    if mode in ("local-operations", "standard"):
        files.append("compose.gui.yaml")
        expected |= GUI
    if mode == "standard":
        files.append("compose.tools.yaml")
        expected |= TOOLS
    if tools_metrics:
        if mode != "standard":
            raise PlanError("INVALID_SELECTION")
        profiles.append("tools-metrics")
        expected |= {"observe-forwarder"}
    if mode == "legacy-gold-read":
        files.append("compose.cloudflare.yaml")
        expected |= EDGE
    if mode == "evaluation":
        if profile not in EVALUATION:
            raise PlanError("INVALID_SELECTION")
        files.append("compose.evaluation.yaml")
        profiles.append(profile)
        expected |= EVALUATION[profile]
    elif profile is not None:
        raise PlanError("INVALID_SELECTION")
    return files, profiles, expected


def synthetic_env(directory, files):
    """選択したoverlayが要求する合成入力だけを渡す。"""
    values = dict.fromkeys(SITE_KEYS, "synthetic-only")
    values.update(LANDING_UID="1000", LANDING_GID="1000")
    paths = ["LANDING_DIR"]
    if "compose.gui.yaml" in files:
        paths.append("LAKEHOUSE_OPS_HTPASSWD")
    if "compose.tools.yaml" in files:
        values.update(dict.fromkeys(TOOLS_KEYS, "synthetic-only"))
        values["TOOLS_OBSERVE_USER"] = "operator@example.invalid"
        paths.extend(
            (
                "TOOLS_HTPASSWD",
                "TOOLS_INFLUX_TOKEN_FILE",
                "TOOLS_OBSERVE_PASSCODE_FILE",
            )
        )
    if "compose.cdc.yaml" in files:
        paths.extend(
            ("CDC_STATE_DIR", "CDC_SETTINGS_FILE", "CDC_TABLE_MAP_FILE")
        )
    if "compose.cloudflare.yaml" in files:
        paths.extend(
            (
                "CF_TUNNEL_TOKEN_FILE",
                "CF_EDGE_POLICY_FILE",
                "CF_EDGE_READER_FILE",
            )
        )
    for key in paths:
        values[key] = str(directory / PATH_KEYS[key])
    return values


def normalize(files, profiles):
    """明示したconfigだけを実行し、生の標準出力やエラーを返さない。"""
    try:
        with tempfile.TemporaryDirectory(prefix="foundation-r-plan-") as tmp:
            scratch = Path(tmp)
            env_file = scratch / "empty.env"
            env_file.touch()
            env = synthetic_env(scratch, files)
            env.update(
                PATH=SYSTEM_PATH,
                HOME=tmp,
                DOCKER_CONFIG=str(scratch / "docker-config"),
                DOCKER_HOST=f"unix://{scratch}/no-engine.sock",
                COMPOSE_DISABLE_ENV_FILE="1",
                LC_ALL="C",
            )
            command = [
                "docker",
                "compose",
                "--project-name",
                "foundation-r-plan",
                "--env-file",
                str(env_file),
            ]
            for name in files:
                source = ROOT / name
                if source.is_symlink() or not source.is_file():
                    raise PlanError("COMPOSE_SOURCE_UNAVAILABLE")
                command.extend(("-f", str(source)))
            for profile in profiles:
                command.extend(("--profile", profile))
            command.extend(
                ("config", "--no-env-resolution", "--format", "json")
            )
            result = subprocess.run(
                command,
                cwd=scratch,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=True,
            )
            return json.loads(result.stdout)
    except (OSError, UnicodeError, ValueError, subprocess.SubprocessError):
        raise PlanError("COMPOSE_CONFIG_FAILED") from None


def names(mapping):
    """Compose宣言の論理名だけを表示できる形にする。"""
    if not isinstance(mapping, dict) or not all(
        isinstance(key, str) and KEY.fullmatch(key) for key in mapping
    ):
        raise PlanError("UNEXPECTED_MODEL")
    return sorted(mapping)


def port_summary(port):
    """合成設定の公開portとbind種別を返し、アドレスを返さない。"""
    address = port.get("host_ip")
    if address is None:
        scope = "unspecified"
    else:
        ip = ipaddress.ip_address(address)
        scope = (
            "wildcard"
            if ip.is_unspecified
            else "loopback"
            if ip.is_loopback
            else "specific"
        )
    protocol = port.get("protocol", "tcp")
    if protocol not in ("tcp", "udp", "sctp"):
        raise PlanError("UNEXPECTED_MODEL")

    def number(key):
        return int(port[key])

    target = number("target")
    published = number("published")
    if not all(1 <= value <= 65535 for value in (target, published)):
        raise PlanError("UNEXPECTED_MODEL")
    return dict(
        scope=scope, protocol=protocol, target=target, published=published
    )


def report_model(model, expected):
    """有効service集合・内部依存を照合し、公開可能な静的要約を作る。"""
    try:
        services = model["services"]
        if set(names(services)) != expected:
            raise PlanError("UNEXPECTED_SERVICES")
        ports = []
        for name, service in sorted(services.items()):
            deps = names(service.get("depends_on", {}))
            if any(dep not in expected for dep in deps):
                raise PlanError("UNEXPECTED_MODEL")
            if (
                service.get("network_mode") not in (None, "none")
                or service.get("links")
                or service.get("volumes_from")
            ):
                raise PlanError("UNEXPECTED_MODEL")
            for port in service.get("ports", []):
                ports.append({"service": name, **port_summary(port)})
        return {
            "services": sorted(expected),
            "ports": ports,
            **{
                key: names(model.get(key, {}))
                for key in ("networks", "volumes", "configs", "secrets")
            },
        }
    except (AttributeError, TypeError, KeyError, ValueError):
        raise PlanError("UNEXPECTED_MODEL") from None


def main(argv=None):
    """出力は固定失敗理由、または合成された構成要約JSON。"""
    parser = Parser(description=__doc__)
    parser.add_argument("mode", choices=(*MODES, *PLANNED))
    parser.add_argument("--cdc", action="store_true")
    parser.add_argument("--alerting", action="store_true")
    parser.add_argument("--tools-metrics", action="store_true")
    parser.add_argument("--profile", choices=tuple(EVALUATION))
    args = parser.parse_args(argv)
    options = dict(
        cdc=args.cdc,
        alerting=args.alerting,
        profile=args.profile,
        tools_metrics=args.tools_metrics,
    )
    try:
        if args.mode == "catalog":
            if any(options.values()):
                raise PlanError("INVALID_SELECTION")
            model = {
                "implemented": list(MODES[:-1]),
                "planned_not_rendered": list(PLANNED),
                "evaluation_profiles": list(EVALUATION),
            }
        else:
            files, profiles, expected = recipe(args.mode, **options)
            model = {
                "compose_files": files,
                "profiles": profiles,
                **report_model(normalize(files, profiles), expected),
            }
        output = {"mode": args.mode, "scope": "synthetic-only", **model}
        print(json.dumps(output, ensure_ascii=False, sort_keys=True))
    except PlanError as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

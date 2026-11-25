#!/usr/bin/env python3
"""使い捨てGitHub runner専用のInfluxDB再起動受入。"""

import json
import os
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from decimal import Decimal, DecimalException
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from native.setup import initialize
from native.tools.demo import NoRedirect, check
from native.tools.setup import create_private

ROOT = Path(__file__).resolve().parents[1]
INFLUX = "influxdb-local"
TARGETS = (INFLUX, "health-relay", "blackbox", "prometheus")
PROJECT = re.compile(r"foundation-r-influx-local-ci-[0-9a-f]{16}", re.ASCII)


class AcceptanceError(RuntimeError):
    """ログに残すのは固定した工程名のみ。"""


def require(condition):
    """境界の不一致を起動成功へ読み替えない。"""
    if not condition:
        raise AcceptanceError


def check_runner(env):
    """この受入を通常のホスト、手動ジョブ、他リポジトリで実行しない。"""
    require(
        env.get("GITHUB_ACTIONS") == "true"
        and env.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and env.get("GITHUB_EVENT_NAME") == "pull_request"
        and env.get("GITHUB_REPOSITORY") == "hiromura16/lakehouse-foundation-R"
        and env.get("RUNNER_OS") == "Linux"
        and env.get("GITHUB_BASE_REF") == "main"
        and env.get("RUNNER_ARCH") == "X64"
    )
    root = Path(env["RUNNER_TEMP"])
    require(root.is_absolute() and root.is_dir() and os.getuid() != 0)
    return root


def run(command, env, *, timeout=45):
    """合成秘密やDocker生出力をログへ持ち出さない。"""
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=True,
        timeout=timeout,
        text=True,
    )
    return result.stdout.strip()


def singular_loopback(ports, port):
    """既定値／範囲／他NICを受け入れず一点の公開だけを認める。"""
    return len(ports) == 1 and (
        str(ports[0].get("target")),
        str(ports[0].get("published")),
        ports[0].get("host_ip"),
        ports[0].get("protocol", "tcp"),
    ) == ("8181", str(port), "127.0.0.1", "tcp")


def config_boundary(model, project, port, token_path):
    """起動前に、共有資源・ホスト名前空間・別ホストbindを拒否する。"""
    require(PROJECT.fullmatch(project) and model["name"] == project)
    services = model["services"]
    require(all(name in services for name in TARGETS))
    require(
        "influxdb3" not in services and "tools-influx" not in model["volumes"]
    )
    tables = ("networks", "volumes")
    for table in tables:
        for key, item in model[table].items():
            require(
                not item.get("external") and item["name"] == f"{project}_{key}"
            )
    for service in services.values():
        require(
            all(
                port.get("host_ip") == "127.0.0.1"
                for port in service.get("ports", [])
            )
            and not any(
                service.get(key)
                for key in (
                    "container_name",
                    "network_mode",
                    "pid",
                    "ipc",
                    "privileged",
                )
            )
        )
    require(all(not services[name].get("ports") for name in TARGETS[1:]))
    influx = services[INFLUX]
    require(singular_loopback(influx["ports"], port))
    require(
        len(influx["volumes"]) == 1
        and influx["volumes"][0].get("type") == "volume"
        and influx["volumes"][0].get("source") == "influx-local-data"
        and influx["volumes"][0].get("target") == "/var/lib/influxdb3/data"
        and model["secrets"]["influx-local-admin"]["file"] == str(token_path)
        and "influx-admin" not in model["secrets"]
    )
    require(
        model["networks"]["influx_local"].get("internal") is True
        and {
            name
            for name, spec in services.items()
            if "influx_local" in spec.get("networks", {})
        }
        == {INFLUX, "health-relay"}
    )


def no_prior_resources(model, project, docker):
    """ラベルと実効名を照合し、既存資源があれば後片付けにも触れない。"""
    label = f"label=com.docker.compose.project={project}"
    for kind, flags in (
        ("ps", ["-a"]),
        ("network", ["ls"]),
        ("volume", ["ls"]),
    ):
        require(not docker(kind, *flags, "--quiet", "--filter", label))
    for kind, table in (("network", "networks"), ("volume", "volumes")):
        occupied = set(
            docker(kind, "ls", "--format", "{{.Name}}").splitlines()
        )
        proposed = {resource["name"] for resource in model[table].values()}
        require(occupied.isdisjoint(proposed))


def select_port():
    """ランナーのループバックから未割当候補を1つ選ぶ。"""
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        return reserved.getsockname()[1]


def read_points(port, token):
    """書き直さず、既存合成点の時刻と値を照合する。"""
    options = urlencode(
        {
            "db": "foundation_demo",
            "q": (
                "SELECT time, device, celsius FROM temperature "
                "WHERE device='demo-press' ORDER BY time DESC LIMIT 3"
            ),
            "format": "json",
        }
    )
    request = Request(
        f"http://127.0.0.1:{port}/api/v3/query_sql?{options}",
        headers={"Authorization": f"Bearer {token}"},
    )
    http = build_opener(ProxyHandler({}), NoRedirect)
    with http.open(request, timeout=8) as result:
        points = json.load(result)
    require(
        [point["celsius"] for point in points] == [24.5, 23.5, 22.5]
        and all(point["device"] == "demo-press" for point in points)
        and len({point["time"] for point in points}) == 3
    )
    return points


def started_epoch(value):
    """DockerのUTCナノ秒表記を丸めずにUnix秒へ変換する。"""
    match = re.fullmatch(
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
        r"(?:\.([0-9]{1,9}))?Z",
        value,
    )
    require(match and not value.startswith("0001-"))
    whole = int(datetime.fromisoformat(match[1] + "+00:00").timestamp())
    return Decimal(whole) + Decimal("0." + (match[2] or "0"))


def observed_binding(docker, compose, port):
    """実際に割り当てたホスト経路もループバック1つであると照合する。"""
    container = compose("ps", "-q", INFLUX)
    require(re.fullmatch(r"[0-9a-f]{12,64}", container))
    bindings = json.loads(
        docker(
            "inspect",
            "--format",
            "{{json .HostConfig.PortBindings}}",
            container,
        )
    )
    require(
        bindings
        == {"8181/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(port)}]}
    )
    started = docker("inspect", "--format", "{{.State.StartedAt}}", container)
    return container, started_epoch(started)


def check_probe(compose, started):
    """固定リレーの成功サンプルが実際の起動より新しくなるまで待つ。"""
    compose(
        "exec",
        "-T",
        "health-relay",
        "wget",
        "-q",
        "-O",
        "/dev/null",
        "http://127.0.0.1:8120/influxdb",
    )
    metric = (
        'probe_success{job="app-probes",'
        'instance="http://influxdb-local:8181/health"}'
    )
    # APIのresult.value[0]は評価時刻であり、採取時刻には使わない。
    query = urlencode({"query": f"timestamp({metric}) and ({metric} == 1)"})
    deadline = time.monotonic() + 85
    while time.monotonic() < deadline:
        response = compose(
            "exec",
            "-T",
            "prometheus",
            "wget",
            "-q",
            "-O",
            "-",
            f"http://127.0.0.1:9090/api/v1/query?{query}",
            timeout=10,
        )
        rows = json.loads(response)["data"]["result"]
        if len(rows) == 1:
            try:
                sample = Decimal(rows[0]["value"][1])
            except (DecimalException, IndexError, TypeError):
                raise AcceptanceError from None
            if sample.is_finite() and sample > started:
                return
        time.sleep(3)
    raise AcceptanceError


def exercise(tmp):
    """新規プロジェクト／専用資格の範囲に限定して実測する。"""
    instance = f"ci-{secrets.token_hex(8)}"
    project = f"foundation-r-influx-local-{instance}"
    port = select_port()
    site = tmp / "site.env"
    initialize(site, tmp / "landing")
    token_path = tmp / "influx-auth.json"
    token = "apiv3_" + secrets.token_urlsafe(48)
    create_private(token_path, json.dumps({"name": "_admin", "token": token}))
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(tmp),
        "DOCKER_CONFIG": str(tmp / "docker-config"),
        "DOCKER_HOST": "unix:///var/run/docker.sock",
        "COMPOSE_DISABLE_ENV_FILE": "1",
        "LC_ALL": "C",
        "LOCAL_INFLUX_INSTANCE": instance,
        "LOCAL_INFLUX_PORT": str(port),
        "LOCAL_INFLUX_TOKEN_FILE": str(token_path),
    }

    def docker(*args, timeout=45):
        return run(["docker", *args], env, timeout=timeout)

    def compose(*args, timeout=45):
        return docker(
            "compose",
            "--env-file",
            str(site),
            "-f",
            str(ROOT / "compose.yaml"),
            "-f",
            str(ROOT / "compose.influx.local.yaml"),
            *args,
            timeout=timeout,
        )

    stage = "CONFIG_BOUNDARY"
    owned = False
    failed = False
    try:
        model = json.loads(
            compose("config", "--no-env-resolution", "--format", "json")
        )
        config_boundary(model, project, port, token_path)
        no_prior_resources(model, project, docker)
        stage = "BUILD_PINNED_TARGETS"
        compose("build", *TARGETS, timeout=480)
        stage = "START_OWNED_TARGETS"
        no_prior_resources(model, project, docker)
        owned = True
        compose(
            "up",
            "-d",
            "--no-build",
            "--pull",
            "never",
            "--no-deps",
            "--wait",
            "--wait-timeout",
            "150",
            *TARGETS,
            timeout=180,
        )
        stage = "OBSERVED_LOOPBACK"
        container_before, start_before = observed_binding(
            docker, compose, port
        )
        stage = "WRITE_AND_QUERY"
        require(
            check(port, token_path)
            == {"database": "foundation_demo", "points": 3}
        )
        before = read_points(port, token)
        stage = "FIXED_RELAY_PROBE"
        check_probe(compose, start_before)
        stage = "RESTART_AND_READ_ONLY_QUERY"
        compose("restart", "--timeout", "30", INFLUX, timeout=120)
        compose(
            "up",
            "-d",
            "--no-recreate",
            "--no-build",
            "--pull",
            "never",
            "--no-deps",
            "--wait",
            "--wait-timeout",
            "90",
            INFLUX,
            timeout=110,
        )
        require(read_points(port, token) == before)
        container_after, start_after = observed_binding(docker, compose, port)
        require(
            container_before == container_after and start_before < start_after
        )
        check_probe(compose, start_after)
    except (
        AcceptanceError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.SubprocessError,
    ):
        print(f"FAIL: {stage}", file=sys.stderr)
        failed = True
    finally:
        if owned:
            try:
                compose("down", "--volumes", "--timeout", "30", timeout=100)
                no_prior_resources(model, project, docker)
            except (
                AcceptanceError,
                OSError,
                ValueError,
                KeyError,
                TypeError,
                subprocess.SubprocessError,
            ):
                print("FAIL: OWNED_PROJECT_CLEANUP", file=sys.stderr)
                failed = True
    if failed:
        return 1
    print("PASS: isolated Influx write/query, restart, relay and cleanup")
    return 0


def main():
    """ホストでの直接実行を拒否し、不正入力の詳細を表示しない。"""
    try:
        parent = check_runner(os.environ)
        with tempfile.TemporaryDirectory(
            prefix="r-influx-ci-", dir=parent
        ) as name:
            return exercise(Path(name))
    except (AcceptanceError, OSError, ValueError, KeyError):
        print("FAIL: HOSTED_FIXTURE_SETUP", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

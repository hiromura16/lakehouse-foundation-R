"""GitHub専用の閉域CDCキャッシュ済み再開試験。"""

import json
import os
import re
import secrets
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from pathlib import Path

from native.setup import initialize
from scripts.influx_local_acceptance import (
    AcceptanceError,
    check_runner,
    no_prior_resources,
    require,
    run,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/integration/native"
FILES = (
    ROOT / "compose.yaml",
    ROOT / "compose.cdc.yaml",
    FIXTURES / "compose.broker.yaml",
    FIXTURES / "compose.cdc-continuity.yaml",
)
NETWORKS = {
    "catalog-db": {"catalog_db"},
    "catalog-migrate": {"catalog_db"},
    "catalog": {"catalog_db", "writer", "catalog_view"},
    "storage": {"writer"},
    "lakehouse": {"writer", "api_origin", "cdc_egress"},
    "native-cdc": {"writer", "cdc_egress"},
    "test-broker": {"cdc_egress"},
}
PROJECT = re.compile(r"foundation-r-cdc-local-ci-[0-9a-f]{16}", re.ASCII)


def require_network(value, project, key):
    """名前と所属だけでなく、外部gatewayを持たない宣言を確認する。"""
    require(value["name"] == f"{project}_{key}")
    require(value["internal"] is True and not value.get("external"))
    require(value.get("driver", "bridge") == "bridge")


def config_boundary(model, project):
    """開始対象を固定し、合成時のポートと外部networkを拒否する。"""
    require(PROJECT.fullmatch(project) and model["name"] == project)
    services = model["services"]
    require(set(NETWORKS).issubset(services))
    require(
        {"cloudflared", "edge-gold", "influxdb-local"}.isdisjoint(services)
    )
    for service, nets in NETWORKS.items():
        spec = services[service]
        require(set(spec["networks"]) == nets)
        require(set(spec.get("depends_on", {})).issubset(NETWORKS))
        require(
            not any(
                spec.get(key)
                for key in (
                    "ports",
                    "container_name",
                    "network_mode",
                    "pid",
                    "ipc",
                    "privileged",
                    "links",
                    "volumes_from",
                )
            )
        )
        for key in nets:
            require_network(model["networks"][key], project, key)
    for key, value in model["volumes"].items():
        require(
            value["name"] == f"{project}_{key}" and not value.get("external")
        )


def runtime_boundary(docker, compose, project, *, inactive=frozenset()):
    """実行された全コンテナ・networkの所有と非公開を照合する。"""
    require(inactive <= {"storage", "native-cdc"})
    ids = {}
    attached = {}
    network_ids = {}
    for service, nets in NETWORKS.items():
        short = compose("ps", "--all", "-q", service)
        require(re.fullmatch(r"[0-9a-f]{12,64}", short))
        value = json.loads(docker("inspect", "--format", "{{json .}}", short))
        actual = value["Id"]
        require(
            re.fullmatch(r"[0-9a-f]{64}", actual) and actual.startswith(short)
        )
        ids[service] = actual
        labels = value["Config"]["Labels"]
        require(labels.get("com.docker.compose.project") == project)
        require(labels.get("com.docker.compose.service") == service)
        if service == "test-broker":
            mounts = [
                mount
                for mount in value["Mounts"]
                if mount["Destination"] == "/var/lib/redpanda/data"
            ]
            require(
                len(mounts) == 1
                and mounts[0]["Type"] == "volume"
                and mounts[0]["Name"] == f"{project}_continuity-broker"
            )
        network_settings = value["NetworkSettings"]
        require(value["HostConfig"].get("PortBindings") in (None, {}))
        mappings = network_settings.get("Ports") or {}
        require(all(binding in (None, []) for binding in mappings.values()))
        mapped = set(network_settings["Networks"] or {})
        expected = {f"{project}_{key}" for key in nets}
        if service == "catalog-migrate":
            require(
                value["State"]["Status"] == "exited"
                and value["State"]["ExitCode"] == 0
            )
            require(mapped in (set(), expected))
        elif service in inactive:
            require(
                value["State"]["Running"] is False
                and value["State"]["Status"] == "exited"
                and mapped in (set(), expected)
            )
        else:
            require(value["State"]["Running"] is True and mapped == expected)
            for key in nets:
                attached.setdefault(key, set()).add(actual)
    require(len(set(ids.values())) == len(NETWORKS))
    listed = docker(
        "ps",
        "--all",
        "--no-trunc",
        "--quiet",
        "--filter",
        f"label=com.docker.compose.project={project}",
    )
    require(set(listed.splitlines()) == set(ids.values()))
    for key, members in attached.items():
        value = json.loads(
            docker(
                "network",
                "inspect",
                "--format",
                "{{json .}}",
                f"{project}_{key}",
            )
        )
        require_network(
            {
                "name": value["Name"],
                "internal": value["Internal"],
                "driver": value["Driver"],
            },
            project,
            key,
        )
        network_ids[key] = value["Id"]
        require(re.fullmatch(r"[0-9a-f]{64}", value["Id"]))
        labels = value["Labels"]
        require(labels.get("com.docker.compose.project") == project)
        require(labels.get("com.docker.compose.network") == key)
        observed = set(value["Containers"])
        stopped = {
            ids[service]
            for service in inactive | {"catalog-migrate"}
            if key in NETWORKS[service]
        }
        require(members <= observed <= members | stopped)
    return ids, network_ids


def observe_workload(compose, checkpoint, body, phase, offset):
    """合成Kafkaと両方の確定位置を照合し、Raw・Silverの世代を返す。"""
    observation = json.loads(
        compose(
            "exec",
            "-T",
            "lakehouse",
            "python",
            "-",
            phase,
            timeout=130,
            input_text=body,
        )
    )
    require(
        isinstance(observation, dict)
        and set(observation)
        == {
            "raw_snapshot_id",
            "release_snapshot_id",
            "broker_next_offset",
        }
        and type(observation["raw_snapshot_id"]) is int
        and observation["raw_snapshot_id"] > 0
        and isinstance(observation["release_snapshot_id"], str)
        and re.fullmatch(r"[1-9][0-9]*", observation["release_snapshot_id"])
        and type(observation["broker_next_offset"]) is int
        and observation["broker_next_offset"] == offset + 1
    )
    checkpoint(offset)
    return observation["raw_snapshot_id"], observation["release_snapshot_id"]


def ordering_state(path):
    """使い捨てSQLiteから合成2キーの版・内容・由来だけを読み取る。"""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        rows = db.execute(
            "SELECT row_key, version, row_json, source_hash, ingested_at "
            "FROM current_rows WHERE table_name = ? AND row_key IN (?, ?)",
            ("erp_products", "CDC-CONTINUITY", "CDC-P1"),
        ).fetchall()
    result = {row[0]: row[1:] for row in rows}
    require(set(result) == {"CDC-CONTINUITY", "CDC-P1"})
    deleted = result["CDC-CONTINUITY"]
    live = result["CDC-P1"]
    require(
        json.loads(deleted[0]) == [1, 1, 1, 1, 1, 1, 14, 14]
        and deleted[1] is None
        and json.loads(live[0]) == [1, 1, 1, 1, 1, 1, 5, 5]
        and json.loads(live[1])["product_name"] == "Reinserted after restart"
    )
    return result


def verify_closed_workload(compose, checkpoint, body, read_current):
    """同じ閉域のまま確定を個別に待ち、最後は読取だけで照合する。"""
    previous = None
    ordering_baseline = None
    for phase, offset in (
        ("continuity-insert", 9),
        ("continuity-update", 10),
        ("continuity-delete", 11),
        ("continuity-confirm", 11),
        ("continuity-late-update", 12),
        ("continuity-duplicate", 13),
        ("continuity-order-confirm", 13),
    ):
        raw, release = observe_workload(
            compose, checkpoint, body, phase, offset
        )
        if phase in {"continuity-confirm", "continuity-order-confirm"}:
            require(previous == (raw, release))
        elif previous is not None:
            require(raw != previous[0] and release != previous[1])
        previous = (raw, release)
        if phase == "continuity-confirm":
            ordering_baseline = read_current()
        elif offset >= 12:
            require(read_current() == ordering_baseline)


def exercise(stage, *, fixture=None, overlay=None, fixture_name="STORAGE"):
    """別枠の合成資源で、閉域起動とbroker/consumer再開だけを観測する。"""
    project = f"foundation-r-cdc-local-ci-{secrets.token_hex(8)}"
    site = stage / "site.env"
    initialize(site, stage / "landing")
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "HOME": str(stage),
        "DOCKER_CONFIG": str(stage / "docker-config"),
        "DOCKER_HOST": "unix:///var/run/docker.sock",
        "COMPOSE_DISABLE_ENV_FILE": "1",
        "LC_ALL": "C",
    }
    run([sys.executable, str(FIXTURES / "prepare_cdc.py"), str(stage)], env)
    base = ["docker", "compose", "-p", project, "--env-file", str(site)]
    for path in FILES + ((overlay,) if overlay is not None else ()):
        base.extend(("-f", str(path)))

    def docker(*args, timeout=45):
        return run(["docker", *args], env, timeout=timeout)

    def compose(*args, timeout=45, input_text=None):
        return run([*base, *args], env, timeout=timeout, input_text=input_text)

    def checkpoint(offset):
        run(
            [
                sys.executable,
                str(FIXTURES / "cdc_checkpoint.py"),
                str(stage / "state/state.sqlite3"),
                str(offset),
            ],
            env,
            timeout=50,
        )

    phase = "CONFIG_BOUNDARY"
    owned = False
    failed = False
    try:
        model = json.loads(
            compose("config", "--no-env-resolution", "--format", "json")
        )
        config_boundary(model, project)
        if fixture is not None:
            require(model["services"]["native-cdc"]["restart"] == "no")
        no_prior_resources(model, project, docker)
        phase = "PREPARE_IMAGES"
        compose(
            "pull",
            "test-broker",
            "catalog-db",
            "catalog-migrate",
            "catalog",
            "storage",
            timeout=360,
        )
        compose("build", "lakehouse", timeout=540)
        phase = "START_OWNED_CACHED_PROJECT"
        no_prior_resources(model, project, docker)
        owned = True
        cached = ("--no-build", "--pull", "never", "--no-recreate")
        wait = ("--wait", "--wait-timeout", "150")
        compose("up", "-d", *cached, *wait, "test-broker", timeout=175)
        topics = (
            (stage / "topics.txt").read_text(encoding="utf-8").splitlines()
        )
        require(len(set(topics)) == len(topics) == 6)
        for topic in topics:
            require(
                re.fullmatch(
                    r"foundation_r\.synthetic_sales\.dbo\.[a-z_]+", topic
                )
            )
            compose(
                "exec",
                "-T",
                "test-broker",
                "rpk",
                "topic",
                "create",
                topic,
                "--partitions",
                "1",
                "--replicas",
                "1",
            )
        compose(
            "up", "-d", *cached, *wait, "lakehouse", "native-cdc", timeout=175
        )
        phase = "LIVE_BOUNDARY_BEFORE"
        before = runtime_boundary(docker, compose, project)
        body = (FIXTURES / "cdc_acceptance.py").read_text(encoding="utf-8")
        if fixture is not None:
            phase = f"OWNED_{fixture_name}_FAILURE_AND_RECOVERY"
            fixture(docker, compose, project, before, checkpoint, body)
        else:
            phase = "SYNTHETIC_WORKLOAD_BEFORE"
            for action in ("first", "delete"):
                compose(
                    "exec",
                    "-T",
                    "lakehouse",
                    "python",
                    "-",
                    action,
                    timeout=110,
                    input_text=body,
                )
            checkpoint(7)
            phase = "CACHED_BROKER_AND_CONSUMER_RESUME"
            compose("stop", "native-cdc", "test-broker", timeout=70)
            checkpoint(7)
            for service in ("test-broker", "native-cdc"):
                compose(
                    "up",
                    "-d",
                    "--no-deps",
                    *cached,
                    *wait,
                    service,
                    timeout=175,
                )
            phase = "LIVE_BOUNDARY_AFTER"
            require(runtime_boundary(docker, compose, project) == before)
            phase = "SYNTHETIC_WORKLOAD_AFTER"
            compose(
                "exec",
                "-T",
                "lakehouse",
                "python",
                "-",
                "resume",
                timeout=110,
                input_text=body,
            )
            checkpoint(8)
            phase = "CLOSED_WORKLOAD_AND_CURSORS"
            verify_closed_workload(
                compose,
                checkpoint,
                body,
                lambda: ordering_state(stage / "state/state.sqlite3"),
            )
            require(runtime_boundary(docker, compose, project) == before)
    except (
        AcceptanceError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        subprocess.SubprocessError,
        sqlite3.Error,
    ):
        print(f"FAIL: {phase}", file=sys.stderr)
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
    if fixture is None:
        print(
            "PASS: closed CDC resume, I/U/D, late UPDATE, duplicate, cursors"
        )
    else:
        print(
            "PASS: closed CDC "
            + fixture_name.lower()
            + " failure and exact recovery"
        )
    return 0


def main(*, fixture=None, overlay=None, fixture_name="STORAGE"):
    """他の環境ではDockerに触れず終了する。"""
    try:
        parent = check_runner(os.environ)
        with tempfile.TemporaryDirectory(
            prefix="r-cdc-ci-", dir=parent
        ) as stage:
            return exercise(
                Path(stage),
                fixture=fixture,
                overlay=overlay,
                fixture_name=fixture_name,
            )
    except (
        AcceptanceError,
        OSError,
        ValueError,
        KeyError,
        subprocess.SubprocessError,
    ):
        print("FAIL: HOSTED_CDC_FIXTURE_SETUP", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

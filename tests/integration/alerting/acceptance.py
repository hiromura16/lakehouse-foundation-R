"""ホストポートを公開せず、4サービスで通知の受入試験を行う。"""

import json
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlencode

FIXTURE = Path(__file__).resolve().parent
ROOT = FIXTURE.parents[2]
SERVICES = ("prometheus", "blackbox", "alertmanager", "alert-fixture")
APP = "Synthetic Alert Probe"
ALERT = "LakehouseApplicationDown"
TARGET = "http://alert-fixture:8080/health"


def execute(command, *, capture=False, timeout=40):
    """ログに設定内容を出さず、試験用のコマンドを制限時間内で実行する。"""
    result = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=capture,
        timeout=timeout,
        check=True,
    )
    return result.stdout


def wait(label, condition, *, seconds=110):
    """実際の監視状態を待ち、条件が長引いた場合は試験を止める。"""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            print(label, flush=True)
            return
        time.sleep(2)
    raise AssertionError(label + " の状態を確認できませんでした。")


def prepare(stage):
    """設定を並行試験と共有せず、非rootコンテナが読める形にする。"""
    for name in ("endpoint.py", "routing.yml"):
        shutil.copyfile(FIXTURE / name, stage / name)
    (stage / "target.yml").write_text(
        json.dumps(
            [
                {
                    "targets": [TARGET],
                    "labels": {"app": APP, "module": "http_ready"},
                }
            ]
        )
    )
    for name in ("endpoint.py", "routing.yml", "target.yml"):
        (stage / name).chmod(0o644)
    site = stage / "site.env"
    execute(
        [
            sys.executable,
            "native/setup.py",
            "--output",
            str(site),
            "--landing",
            str(stage / "landing"),
        ]
    )
    with site.open("a") as handle:
        handle.write(f"ALERTING_FIXTURE_DIR={stage}\n")
    # 既定状態を確認したあと、試験用ルートだけを同じコンテナの設定先へ載せる。
    route = stage / "route.yaml"
    route.write_text(
        json.dumps(
            {
                "services": {
                    "alertmanager": {
                        "volumes": [
                            {
                                "type": "bind",
                                "source": str(stage / "routing.yml"),
                                "target": "/etc/alertmanager/alertmanager.yml",
                                "read_only": True,
                                "bind": {"create_host_path": False},
                            }
                        ]
                    }
                },
            }
        )
    )
    return site, route


def verify_delivery(compose, route):
    """既定の受入、Webhookの再送、復旧を通常ルールで確認する。"""

    def run(*args, capture=False, timeout=40):
        return execute([*compose, *args], capture=capture, timeout=timeout)

    def read_json(service, url):
        return json.loads(
            run(
                "exec",
                "-T",
                service,
                "wget",
                "-q",
                "-O",
                "-",
                url,
                capture=True,
            )
        )

    def probe(expected):
        query = urlencode({"query": f'probe_success{{app="{APP}"}}'})
        values = read_json(
            "prometheus", f"http://127.0.0.1:9090/api/v1/query?{query}"
        )
        return any(
            row["value"][1] == expected for row in values["data"]["result"]
        )

    def prom_alerts():
        reply = read_json("prometheus", "http://127.0.0.1:9090/api/v1/alerts")
        return [
            row
            for row in reply["data"]["alerts"]
            if row["labels"].get("alertname") == ALERT
            and row["labels"].get("app") == APP
        ]

    def attempts():
        return read_json("alertmanager", "http://alert-fixture:8080/attempts")

    base = run(
        "exec",
        "-T",
        "alertmanager",
        "cat",
        "/etc/alertmanager/alertmanager.yml",
        capture=True,
    )
    expected = ROOT / "native" / "monitoring" / "alertmanager.yml"
    assert base == expected.read_text()
    assert base.split("\nreceivers:\n", 1)[1].strip() == "- name: no-delivery"
    wait("合成プローブの初期成功を確認", lambda: probe("1"))
    run("exec", "-T", "alert-fixture", "touch", "/tmp/fail-probe")
    wait("合成HTTP 503を検知", lambda: probe("0"))
    wait(
        "Prometheusで発報状態を確認",
        lambda: any(a["state"] == "firing" for a in prom_alerts()),
    )
    wait(
        "宛先未設定のAlertmanagerで発報を受信",
        lambda: any(
            row["labels"].get("app") == APP
            for row in read_json(
                "alertmanager", "http://127.0.0.1:9093/api/v2/alerts"
            )
        ),
    )
    assert not attempts(), "宛先未設定の状態で配信がありました。"

    run(
        "-f",
        str(route),
        "up",
        "-d",
        "--no-deps",
        "--no-build",
        "--pull",
        "never",
        "--force-recreate",
        "--wait",
        "--wait-timeout",
        "75",
        "alertmanager",
        timeout=100,
    )
    wait(
        "Webhook 503のあとに発報を再送",
        lambda: any(a["http_status"] == 204 for a in attempts()),
    )
    run("exec", "-T", "alert-fixture", "rm", "-f", "/tmp/fail-probe")
    wait("合成HTTP 200に復旧", lambda: probe("1"))
    wait("Prometheusの発報が解除", lambda: not prom_alerts())
    wait(
        "Webhookで復旧通知を受理",
        lambda: any(a["status"] == "resolved" for a in attempts()),
    )

    received = attempts()
    assert [(a["status"], a["http_status"]) for a in received] == [
        ("firing", 503),
        ("firing", 204),
        ("resolved", 204),
    ]
    labels = {
        "alertname": ALERT,
        "app": APP,
        "instance": TARGET,
        "job": "app-probes",
        "module": "http_ready",
        "severity": "warning",
    }
    fingerprint = set()
    for attempt in received:
        assert attempt["receiver"] == "fixture" and len(attempt["alerts"]) == 1
        alert = attempt["alerts"][0]
        assert alert["status"] == attempt["status"]
        assert all(
            alert["labels"].get(key) == value for key, value in labels.items()
        )
        fingerprint.add(alert["fingerprint"])
    assert len(fingerprint) == 1
    print(
        "合成障害1回について、発報503・再送204・復旧204と同じ指紋を確認しました。"
    )


def main():
    """専用の4サービスで検査し、終了時に試験資源を片付ける。"""
    with tempfile.TemporaryDirectory(prefix="foundation-r-alerting-") as name:
        project = "foundation-r-alerting-" + secrets.token_hex(6)
        site, route = prepare(Path(name).resolve())
        compose = [
            "docker",
            "compose",
            "--env-file",
            str(site),
            "-p",
            project,
            "-f",
            "compose.yaml",
            "-f",
            "compose.alerting.yaml",
            "-f",
            str(FIXTURE / "compose.fixture.yaml"),
        ]
        plan = json.loads(
            execute([*compose, "config", "--format", "json"], capture=True)
        )
        assert all(
            not plan["services"][service].get("ports") for service in SERVICES
        )
        assert all(
            plan["networks"][name]["internal"]
            for name in ("probes", "alert_delivery")
        )
        try:
            execute(
                [
                    *compose,
                    "up",
                    "-d",
                    "--no-deps",
                    "--build",
                    "--wait",
                    "--wait-timeout",
                    "150",
                    *SERVICES,
                ],
                timeout=250,
            )
            running = execute(
                [*compose, "ps", "--status", "running", "--services"],
                capture=True,
            )
            assert set(running.splitlines()) == set(SERVICES)
            verify_delivery(compose, route)
        finally:
            execute(
                [*compose, "down", "--volumes", "--remove-orphans"], timeout=80
            )
            for service in SERVICES[:-1]:
                subprocess.run(
                    ["docker", "image", "rm", f"{project}-{service}"],
                    cwd=ROOT,
                    capture_output=True,
                    timeout=20,
                    check=False,
                )


if __name__ == "__main__":
    main()

"""専用ComposeでMQTTのACLと購読停止中の永続化を検証する。"""

import argparse
import json
import secrets
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener

TOPIC = "factory/measurements/evaluation"
METRIC = "factory_temperature_celsius"
FIRST_MS = 1767614400000
MOSQUITTO = (
    "docker.io/library/eclipse-mosquitto:2.0.23@sha256:"
    "6a332cc7a8dbe011cbc076180cbb76b37c6f0f55809682ea08e7980ef0112ad9"
)


def until(operation, label, timeout=60):
    """起動時の待機に上限を設け、最後まで条件に達しなければ失敗にする。"""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if result := operation():
                return result
        except (URLError, TimeoutError):
            pass
        time.sleep(1)
    raise RuntimeError(f"{label}の条件に達しませんでした。")


def verify(docker, root):
    """合成送信元だけを使い、保存先で時刻・値・件数を確認する。"""
    project = f"foundation-r-telemetry-{secrets.token_hex(4)}"
    passwords = root / "passwords"
    passwords.touch(mode=0o600)
    simulator, collector = secrets.token_hex(24), secrets.token_hex(24)
    subprocess.run(
        [
            *docker,
            "run",
            "--rm",
            "-i",
            "--network",
            "none",
            "--mount",
            f"type=bind,source={root},target=/work",
            "--entrypoint",
            "/bin/sh",
            MOSQUITTO,
            "-ec",
            "read -r s && read -r c && chown 0:0 /work/passwords && "
            'mosquitto_passwd -b -c /work/passwords simulator "$s" && '
            'mosquitto_passwd -b /work/passwords collector "$c" && '
            "chown 1883:1883 /work/passwords && chmod 400 /work/passwords",
        ],
        input=f"{simulator}\n{collector}\n",
        text=True,
        check=True,
        timeout=180,
    )
    env = root / "trial.env"
    env.touch(mode=0o600)
    env.write_text(
        f"MQTT_PASSWORD_HASH_FILE={passwords}\n"
        f"MQTT_COLLECTOR_PASSWORD={collector}\n"
        "EVAL_GREPTIME_PORT=0\n",
        encoding="utf-8",
    )
    compose = [
        *docker,
        "compose",
        "-p",
        project,
        "--env-file",
        str(env),
        "-f",
        "compose.evaluation.yaml",
        "-f",
        "compose.telemetry.yaml",
        "--profile",
        "stream",
        "--profile",
        "timeseries",
        "--profile",
        "telemetry",
    ]

    def run(*args, input_text=None, check=True, timeout=120):
        """試験プロジェクト内でだけComposeコマンドを実行する。"""
        return subprocess.run(
            [*compose, *args],
            input=input_text,
            capture_output=True,
            text=True,
            check=check,
            timeout=timeout,
        )

    def publish(offset, value, *, user="simulator", check=True):
        """1点をQoS 1で送信し、資格情報は標準入力に限定する。"""
        base = 'mosquitto_pub -h mosquitto -q 1 -V mqttv5 -t "$1" -s'
        args = [TOPIC]
        payload = (
            f"factory_temperature,device=sim01 celsius={value} "
            f"{(FIRST_MS + offset * 1000) * 1000000}\n"
        )
        if user is not None:
            base = f'read -r password && exec {base} -u "$2" -P "$password"'
            args.append(user)
            passwords = {"simulator": simulator, "collector": collector}
            payload = passwords[user] + "\n" + payload
        return run(
            "exec",
            "-T",
            "mosquitto",
            "sh",
            "-ec",
            base,
            "publish",
            *args,
            input_text=payload,
            check=check,
            timeout=10,
        )

    def rows_for(size):
        """DBへの保存結果を別経路で読み、MQTTのACKとは分けて照合する。"""
        sql = f"SELECT ts, device, val FROM {METRIC} ORDER BY ts"
        with http.open(
            endpoint + "/v1/sql?" + urlencode({"sql": sql}), timeout=3
        ) as reply:
            result = json.load(reply)
        records = result["output"][0]["records"]["rows"]
        assert len(records) <= size, "未承認または重複した点が保存されました。"
        return records if len(records) == size else None

    try:
        run("config", "--quiet")
        run("up", "-d", "broker", "greptime", "mosquitto", timeout=300)
        until(
            lambda: (
                run(
                    "exec",
                    "-T",
                    "broker",
                    "rpk",
                    "cluster",
                    "info",
                    check=False,
                    timeout=15,
                ).returncode
                == 0
            ),
            "Kafka応答",
        )
        run(
            "exec",
            "-T",
            "broker",
            "rpk",
            "topic",
            "create",
            "foundation-r-evaluation-sensors",
            "--partitions",
            "1",
            "--replicas",
            "1",
        )
        endpoint = "http://" + run("port", "greptime", "4000").stdout.strip()
        http = build_opener(ProxyHandler({}))
        run("up", "-d", "telegraf", "vector", timeout=300)
        until(
            lambda: (
                "Connected" in run("logs", "telegraf").stdout
                and "[inputs.mqtt_consumer]" in run("logs", "telegraf").stdout
            ),
            "MQTT購読",
        )
        assert publish(9, 99.0, user=None, check=False).returncode != 0
        denied = publish(9, 99.0, user="collector", check=False)
        # MQTT 5のPUBACKで拒否されてもCLIが0を返す版がある。
        assert "not authorized" in denied.stderr.lower(), denied.stderr
        publish(0, 22.5)
        assert until(lambda: rows_for(1), "最初の保存") == [
            [FIRST_MS, "sim01", 22.5]
        ]
        run("stop", "telegraf")
        publish(1, 23.5)
        publish(2, 24.5)
        run("restart", "mosquitto")
        run("start", "telegraf")
        assert until(lambda: rows_for(3), "永続購読の再送") == [
            [FIRST_MS + offset * 1000, "sim01", value]
            for offset, value in enumerate((22.5, 23.5, 24.5))
        ]
        print("MQTTの認証・ACL・購読停止中の2点再送、GreptimeDBの3点を照合。")
    except Exception:
        # 合成入力の接続失敗だけを記録し、生成した資格情報は伏せる。
        logs = run(
            "logs",
            "--tail",
            "20",
            "mosquitto",
            "telegraf",
            "vector",
            check=False,
        )
        details = (
            (logs.stdout + logs.stderr)
            .replace(simulator, "[非表示]")
            .replace(collector, "[非表示]")
        )
        print(details)
        raise
    finally:
        # 試験のたびに一意なプロジェクト名を取り、合成状態だけを取り除く。
        run("down", "--volumes", "--remove-orphans")


def main():
    """Docker実行権限の違いを吸収して固定の合成受入を起動する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    docker = ["sudo", "-n", "docker"] if args.sudo else ["docker"]
    with tempfile.TemporaryDirectory(prefix="foundation-r-telemetry-") as temp:
        verify(docker, Path(temp))


if __name__ == "__main__":
    main()

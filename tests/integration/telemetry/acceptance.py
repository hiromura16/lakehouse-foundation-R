"""専用ComposeでMQTTのACLと購読停止中の永続化を検証する。"""

import argparse
import json
import secrets
import shutil
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


def greptime_rows(http, endpoint, table, columns):
    """時系列表の結果から、確定した計測点のみを読み取る。"""
    sql = f"SELECT {columns} FROM {table} ORDER BY ts"
    address = endpoint + "/v1/sql?" + urlencode({"sql": sql})
    with http.open(address, timeout=3) as reply:
        result = json.load(reply)
    return result["output"][0]["records"]["rows"]


def opcua_acceptance(docker, project, run, root, http, endpoint):
    """誤った相手証明書、暗号化購読、時刻、品質悪化の扱いを照合する。"""
    keys = root / "opcua"
    run("build", "opcua-simulator", timeout=360)
    subprocess.run(
        [
            *docker,
            "run",
            "--rm",
            "--network",
            "none",
            "--user",
            f"{root.stat().st_uid}:{root.stat().st_gid}",
            "--mount",
            f"type=bind,source={keys},target=/keys",
            f"{project}-opcua-simulator",
            "--certificates",
            "/keys",
        ],
        check=True,
        timeout=90,
    )
    remote = keys / "remote-cert.pem"
    shutil.copyfile(keys / "other-cert.pem", remote)
    run("up", "-d", "opcua-simulator", "telegraf-opcua")
    until(
        lambda: "試験用OPC UAを開始" in run("logs", "opcua-simulator").stdout,
        "模擬設備の起動",
    )

    def wrong_remote_key_seen():
        """正常な設定の上で、模擬設備が初回暗号化を復号できないことを判定する。"""
        logs = run("logs", "opcua-simulator")
        return "ValueError: Decryption failed" in logs.stdout + logs.stderr

    until(wrong_remote_key_seen, "接続先の鍵が異なる場合の拒否")
    with http.open(endpoint + "/v1/sql?sql=SHOW+TABLES", timeout=3) as reply:
        tables = json.load(reply)["output"][0]["records"]["rows"]
    assert ["opc_temperature_celsius"] not in tables

    shutil.copyfile(keys / "server-cert.pem", remote)
    run("up", "-d", "--no-deps", "--force-recreate", "telegraf-opcua")
    semantic_first_ms = FIRST_MS + 3_600_000

    def rows():
        """Goodに限定した線形計測列を返す。"""
        return greptime_rows(
            http,
            endpoint,
            "opc_temperature_celsius",
            "ts, device, quality, val",
        )

    assert until(rows, "暗号化した最初のOPC UA通知") == [
        [semantic_first_ms, "opc01", "GOOD", 22.5],
    ]
    # 模擬設備にだけ接続する独立した認証クライアントを起動する。
    probe_mounts = []
    for name in ("client-cert.pem", "client-key.pem", "server-cert.pem"):
        probe_mounts.extend(
            (
                "--mount",
                f"type=bind,source={keys / name},target=/keys/{name},readonly",
            )
        )
    subprocess.run(
        [
            *docker,
            "run",
            "--rm",
            "--network",
            f"{project}_default",
            "--hostname",
            "telegraf-opcua",
            "--cap-drop",
            "ALL",
            "--user",
            f"{root.stat().st_uid}:{root.stat().st_gid}",
            *probe_mounts,
            f"{project}-opcua-simulator",
            "--check-read-only",
            "/keys",
        ],
        check=True,
        timeout=30,
    )

    def trigger(name):
        """観測した段階の次だけを模擬設備へ指示する。"""
        run("exec", "-T", "opcua-simulator", "touch", f"/tmp/{name}")

    def bad_quality_arrived():
        """時系列へ書く前に、収集器まで非OK通知が届いたことを判定する。"""
        logs = run("logs", "telegraf-opcua")
        return "StatusBadNoCommunication" in logs.stdout + logs.stderr

    def observed(size):
        """送信順の揺れを避けるため、保存が進んだ段階を照合する。"""
        points = rows()
        return points if len(points) >= size else None

    trigger("next-bad")
    until(bad_quality_arrived, "通信異常の受信")
    trigger("next-good")
    assert until(lambda: observed(2), "復帰後の正常値の保存") == [
        [semantic_first_ms, "opc01", "GOOD", 22.5],
        [semantic_first_ms + 2000, "opc01", "GOOD", 23.5],
    ]
    trigger("last-good")
    assert until(lambda: observed(3), "最後の正常値の保存") == [
        [semantic_first_ms + offset * 1000, "opc01", "GOOD", val]
        for offset, val in ((0, 22.5), (2, 23.5), (3, 24.5))
    ]
    print("OPC UA暗号化、相手鍵不一致・書込みの拒否、発生元時刻と品質を照合。")


def verify(docker, root, *, opcua=False, nisshi=False):
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
    if opcua:
        keys = root / "opcua"
        keys.mkdir(mode=0o700)
        with env.open("a", encoding="utf-8") as settings:
            settings.write(
                f"OPCUA_FIXTURE_DIR={keys}\n"
                f"OPCUA_FIXTURE_UID={root.stat().st_uid}\n"
                f"OPCUA_FIXTURE_GID={root.stat().st_gid}\n"
                f"OPCUA_REMOTE_CERT_FILE={keys / 'remote-cert.pem'}\n"
            )
    if nisshi:
        broker_state = root / "nisshi"
        broker_state.mkdir(mode=0o700)
        with env.open("a", encoding="utf-8") as settings:
            settings.write(
                f"NISSHI_UID={root.stat().st_uid}\n"
                f"NISSHI_GID={root.stat().st_gid}\n"
                f"NISSHI_STATE_DIR={broker_state}\n"
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

    if nisshi:
        compose.extend(("-f", "compose.telemetry.nisshi.yaml"))
    if opcua:
        compose.extend(
            (
                "-f",
                "tests/integration/telemetry/compose.opcua.yaml",
                "--profile",
                "opcua",
            )
        )

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
        records = greptime_rows(http, endpoint, METRIC, "ts, device, val")
        assert len(records) <= size, "未承認または重複した点が保存されました。"
        return records if len(records) == size else None

    broker_probe = (
        ("/nisshi", "topic", "list") if nisshi else ("rpk", "cluster", "info")
    )
    topic = "foundation-r-evaluation-sensors"
    broker_create = (
        ("/nisshi", "topic", "create", topic, "--partitions", "1")
        if nisshi
        else (
            "rpk",
            "topic",
            "create",
            topic,
            "--partitions",
            "1",
            "--replicas",
            "1",
        )
    )

    def broker_ready():
        """共通入口のKafka要求が完了してから送信・再送を始める。"""
        return (
            run(
                "exec", "-T", "broker", *broker_probe, check=False, timeout=15
            ).returncode
            == 0
        )

    try:
        run("config", "--quiet")
        services = ["broker", "greptime"]
        if not opcua:
            services.append("mosquitto")
        run("up", "-d", *services, timeout=300)
        until(broker_ready, "Kafka応答")
        run("exec", "-T", "broker", *broker_create)
        endpoint = "http://" + run("port", "greptime", "4000").stdout.strip()
        http = build_opener(ProxyHandler({}))
        run("up", "-d", "vector", timeout=300)
        if opcua:
            opcua_acceptance(docker, project, run, root, http, endpoint)
            return
        run("up", "-d", "telegraf", timeout=300)

        def mqtt_connected():
            """収集器が認証済みのMQTTセッションを確認してから送信する。"""
            response = run("logs", "telegraf")
            log = response.stdout + response.stderr
            return any(
                "Connected" in line and "[inputs.mqtt_consumer]" in line
                for line in log.splitlines()
            )

        until(mqtt_connected, "MQTT購読")
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
        if nisshi:
            run("restart", "broker")
            until(broker_ready, "Kafkaの再起動")
        run("start", "telegraf")
        assert until(lambda: rows_for(3), "永続購読の再送") == [
            [FIRST_MS + offset * 1000, "sim01", value]
            for offset, value in enumerate((22.5, 23.5, 24.5))
        ]
        print("MQTTの認証・ACL・購読停止中の2点再送、GreptimeDBの3点を照合。")
    except Exception as error:
        # 合成入力の接続失敗だけを記録し、生成した資格情報は伏せる。
        services = (
            ["vector", "opcua-simulator", "telegraf-opcua"]
            if opcua
            else ["vector", "mosquitto", "telegraf"]
        )
        states = run("ps", "--all", "--format", "json", check=False)
        for line in states.stdout.splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            print(row.get("Service"), row.get("State"), row.get("ExitCode"))
        logs = run("logs", "--tail", "20", *services, check=False)
        error_log = (
            error.stderr or ""
            if isinstance(error, subprocess.CalledProcessError)
            else ""
        )
        details = (
            (error_log + logs.stdout + logs.stderr)
            .replace(simulator, "[非表示]")
            .replace(collector, "[非表示]")
        )
        print(details)
        raise
    finally:
        # 試験のたびに一意なプロジェクト名を取り、合成状態だけを取り除く。
        run("down", "--volumes", "--remove-orphans", "--rmi", "local")


def main():
    """Docker実行権限の違いを吸収して固定の合成受入を起動する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sudo", action="store_true")
    parser.add_argument("--opcua", action="store_true")
    parser.add_argument("--nisshi", action="store_true")
    args = parser.parse_args()
    docker = ["sudo", "-n", "docker"] if args.sudo else ["docker"]
    with tempfile.TemporaryDirectory(prefix="foundation-r-telemetry-") as temp:
        verify(docker, Path(temp), opcua=args.opcua, nisshi=args.nisshi)


if __name__ == "__main__":
    main()

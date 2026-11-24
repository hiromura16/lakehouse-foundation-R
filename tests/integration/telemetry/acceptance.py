"""専用ComposeでMQTTのACLと購読停止中の永続化を検証する。"""

import argparse
import json
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing, suppress
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener

# ファイルを直接起動する受入CLIから共通の取得手順を使う。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compose_images import command_tail, pull_images  # noqa: E402

TOPIC = "factory/measurements/evaluation"
METRIC = "factory_temperature_celsius"
CAPACITY_METRIC = "factory_capacity_temperature_celsius"
CAPACITY_FIRST_MS = 1767618000000
CAPACITY_DEVICE_COUNT = 40
CAPACITY_BATCH_ROWS = 250
CAPACITY_OUTAGE_ROWS = 500
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


def greptime_select(http, endpoint, sql):
    """独立した試験用HTTPポートへ固定SQLを送り、保存された行を読む。"""
    address = endpoint + "/v1/sql?" + urlencode({"sql": sql})
    with http.open(address, timeout=5) as reply:
        result = json.load(reply)
    return result["output"][0]["records"]["rows"]


def greptime_rows(http, endpoint, table, columns):
    """既存の少量試験では、時系列表の計測点を時刻順で読み取る。"""
    return greptime_select(
        http, endpoint, f"SELECT {columns} FROM {table} ORDER BY ts"
    )


def capacity_point(index):
    """連番から一意な時刻と40設備のタグ、照合しやすい値を作る。"""
    return [
        CAPACITY_FIRST_MS + index * 100,
        f"capacity{index % CAPACITY_DEVICE_COUNT:02d}",
        index / 4,
    ]


def telemetry_capacity(run, publish, http, endpoint, size, *, nisshi):
    """稼働中の受信とVector停止中の滞留を、全件・全値で照合する。"""

    def count():
        """連番の送信完了とは独立に、保存先に確定した件数を数える。"""
        rows = greptime_select(
            http, endpoint, f"SELECT COUNT(*) FROM {CAPACITY_METRIC}"
        )
        return rows[0][0]

    def saved(expected):
        """前のバッチの確認後に送信を進め、無制限の滞留を起こさない。"""
        actual = count()
        assert actual <= expected, f"想定外の保存件数: {actual} > {expected}"
        return actual == expected

    def payload(start, stop):
        """各行を独立したQoS 1メッセージとして送るLine Protocolを返す。"""
        lines = []
        for index in range(start, stop):
            timestamp, device, value = capacity_point(index)
            lines.append(
                f"factory_capacity_temperature,device={device} "
                f"celsius={value} {timestamp * 1_000_000}\n"
            )
        return "".join(lines)

    started = time.monotonic()
    active_end = size - CAPACITY_OUTAGE_ROWS
    for start in range(0, active_end, CAPACITY_BATCH_ROWS):
        end = min(start + CAPACITY_BATCH_ROWS, active_end)
        publish(payload(start, end), multiline=True)
        until(lambda: saved(end), f"平常時の{end}点の確定", timeout=120)
    active_seconds = time.monotonic() - started

    run("stop", "vector")
    publish(payload(active_end, size), multiline=True)
    assert saved(active_end), "転送停止中に保存件数が変化しました。"
    recovery_started = time.monotonic()
    run("start", "vector")
    until(
        lambda: saved(size),
        "転送器の復帰後に滞留した計測点の確定",
        timeout=120,
    )
    recovery_seconds = time.monotonic() - recovery_started

    # 送信の最後に初出点を加え、同じ区画で再送した点の後まで処理を進める。
    publish(payload(0, 50) + payload(size, size + 1), multiline=True)
    until(lambda: saved(size + 1), "同一50点の再送と初出1点の確定")
    for offset in range(0, size + 1, 1000):
        actual = greptime_select(
            http,
            endpoint,
            f"SELECT ts, device, val FROM {CAPACITY_METRIC} "
            f"ORDER BY ts LIMIT 1000 OFFSET {offset}",
        )
        expected = [
            capacity_point(index)
            for index in range(offset, min(offset + 1000, size + 1))
        ]
        assert actual == expected, f"時刻・設備・値の違い: 行{offset}以降"
    print(
        json.dumps(
            {
                "broker": "nisshi" if nisshi else "redpanda",
                "devices": CAPACITY_DEVICE_COUNT,
                "active_rows": active_end,
                "active_seconds": round(active_seconds, 3),
                "recovered_rows": CAPACITY_OUTAGE_ROWS,
                "recovery_seconds": round(recovery_seconds, 3),
                "replayed_rows": 50,
                "verified_distinct_rows": size + 1,
            },
            ensure_ascii=False,
        )
    )


def opcua_acceptance(
    docker, project, run, root, http, endpoint, *, nisshi=False
):
    """誤った相手証明書、暗号化購読、時刻、品質悪化の扱いを照合する。"""
    keys = root / "opcua"
    run("build", "opcua-simulator", timeout=360)
    subprocess.run(
        [
            *docker,
            "run",
            "--rm",
            "--pull=never",
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
    run(
        "up",
        "-d",
        "--pull",
        "never",
        "--no-build",
        "opcua-simulator",
        "telegraf-opcua",
    )
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
    run(
        "up",
        "-d",
        "--pull",
        "never",
        "--no-build",
        "--no-deps",
        "--force-recreate",
        "telegraf-opcua",
    )
    semantic_first_ms = FIRST_MS + 3_600_000

    def rows():
        """Goodに限定した線形計測列を返す。"""
        return greptime_rows(
            http,
            endpoint,
            "opc_temperature_celsius",
            "ts, device, quality, val",
        )

    def expect_points(label, expected):
        """失敗時は、ブローカーと時系列DBの到達段階を別々に記録する。"""
        last_points = None

        def observed():
            nonlocal last_points
            last_points = rows()
            return last_points if len(last_points) >= len(expected) else None

        try:
            assert until(observed, label) == expected
        except (AssertionError, RuntimeError):
            evidence = {"phase": label, "greptime_last_rows": last_points}
            if nisshi:
                evidence["nisshi"] = nisshi_fixture_progress(
                    root / "nisshi" / "nisshi.db",
                    "foundation-r-evaluation-sensors",
                )
            print(json.dumps(evidence, ensure_ascii=False), flush=True)
            raise

    expect_points(
        "暗号化した最初のOPC UA通知",
        [
            [semantic_first_ms, "opc01", "GOOD", 22.5],
        ],
    )
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

    trigger("next-bad")
    until(bad_quality_arrived, "通信異常の受信")
    trigger("next-good")
    expect_points(
        "復帰後の正常値の保存",
        [
            [semantic_first_ms, "opc01", "GOOD", 22.5],
            [semantic_first_ms + 2000, "opc01", "GOOD", 23.5],
        ],
    )
    trigger("last-good")
    expect_points(
        "最後の正常値の保存",
        [
            [semantic_first_ms + offset * 1000, "opc01", "GOOD", val]
            for offset, val in ((0, 22.5), (2, 23.5), (3, 24.5))
        ],
    )
    print("OPC UA暗号化、相手鍵不一致・書込みの拒否、発生元時刻と品質を照合。")


def nisshi_fixture_progress(path, topic):
    """失敗時のみ、固定版Nisshiの合成SQLiteからoffsetと発生元時刻を読む。"""
    records_sql = """
        SELECT record.offset_id, substr(record.v, 1, 256)
        FROM record
        JOIN topition ON record.topition = topition.id
        JOIN topic ON topition.topic = topic.id
        WHERE topic.name = ? AND topition.partition = 0
        ORDER BY record.offset_id LIMIT 16
    """
    committed_sql = """
        SELECT consumer_offset.committed_offset
        FROM consumer_offset
        JOIN consumer_group
          ON consumer_offset.consumer_group = consumer_group.id
        JOIN topition ON consumer_offset.topition = topition.id
        JOIN topic ON topition.topic = topic.id
        WHERE consumer_group.name = ? AND topic.name = ?
          AND topition.partition = 0
    """
    try:
        uri = path.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=1)) as db:
            # 保存点と確認位置は同じ読み取り時点で比較する。
            db.execute("BEGIN")
            events = db.execute(records_sql, (topic,)).fetchall()
            committed = db.execute(
                committed_sql, ("foundation-r-evaluation-greptime", topic)
            ).fetchone()
    except sqlite3.Error as error:
        return {"read_error": type(error).__name__}
    records = []
    for offset, value in events:
        # Line Protocolのペイロード自体はログに出さない。
        payload = value if isinstance(value, bytes) else b""
        token = payload.strip().rpartition(b" ")[2]
        timestamp = int(token) // 1_000_000 if token.isdigit() else None
        records.append({"offset": offset, "source_ms": timestamp})
    return {
        "first_partition_records": records,
        "vector_committed_next_offset": committed[0] if committed else None,
    }


def verify(docker, root, *, opcua=False, nisshi=False, capacity_rows=None):
    """合成送信元だけを使い、保存先で時刻・値・件数を確認する。"""
    project = f"foundation-r-telemetry-{secrets.token_hex(4)}"
    passwords = root / "passwords"
    passwords.touch(mode=0o600)
    simulator, collector = secrets.token_hex(24), secrets.token_hex(24)
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

    def prepare_passwords():
        """取得済みのMosquittoをネットワークなしで使い、合成パスワードを作る。"""
        subprocess.run(
            [
                *docker,
                "run",
                "--rm",
                "--pull=never",
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

    def publish(payload, *, user="simulator", check=True, multiline=False):
        """QoS 1で送信し、合成資格情報をホスト上の引数に入れない。"""
        mode = "-l" if multiline else "-s"
        base = f'mosquitto_pub -h mosquitto -q 1 -V mqttv5 -t "$1" {mode}'
        args = [TOPIC]
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
            timeout=20,
        )

    def one_point(offset, value, *, user="simulator", check=True):
        """少量試験で、単一の計測点に対するACLの応答も確認できる。"""
        return publish(
            f"factory_temperature,device=sim01 celsius={value} "
            f"{(FIRST_MS + offset * 1000) * 1_000_000}\n",
            user=user,
            check=check,
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
        collector_service = "telegraf-opcua" if opcua else "telegraf"
        pull_images(
            run,
            "broker",
            "greptime",
            "mosquitto",
            "vector",
            collector_service,
            timeout=420,
            redactions=(simulator, collector),
        )
        prepare_passwords()
        services = ["broker", "greptime"]
        if not opcua:
            services.append("mosquitto")
        run("up", "-d", "--pull", "never", "--no-build", *services)
        until(broker_ready, "Kafka応答")
        run("exec", "-T", "broker", *broker_create)
        endpoint = "http://" + run("port", "greptime", "4000").stdout.strip()
        http = build_opener(ProxyHandler({}))
        run("up", "-d", "--pull", "never", "--no-build", "vector")
        if opcua:
            opcua_acceptance(
                docker, project, run, root, http, endpoint, nisshi=nisshi
            )
            return
        run("up", "-d", "--pull", "never", "--no-build", "telegraf")

        def mqtt_connected():
            """収集器が認証済みのMQTTセッションを確認してから送信する。"""
            response = run("logs", "telegraf")
            log = response.stdout + response.stderr
            return any(
                "Connected" in line and "[inputs.mqtt_consumer]" in line
                for line in log.splitlines()
            )

        until(mqtt_connected, "MQTT購読")
        assert one_point(9, 99.0, user=None, check=False).returncode != 0
        denied = one_point(9, 99.0, user="collector", check=False)
        # MQTT 5のPUBACKで拒否されてもCLIが0を返す版がある。
        assert "not authorized" in denied.stderr.lower(), denied.stderr
        one_point(0, 22.5)
        assert until(lambda: rows_for(1), "最初の保存") == [
            [FIRST_MS, "sim01", 22.5]
        ]
        run("stop", "telegraf")
        one_point(1, 23.5)
        one_point(2, 24.5)
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
        if capacity_rows is not None:
            telemetry_capacity(
                run, publish, http, endpoint, capacity_rows, nisshi=nisshi
            )
    except Exception as error:
        # 合成入力の接続失敗だけを記録し、生成した資格情報は伏せる。
        services = (
            ["vector", "opcua-simulator", "telegraf-opcua"]
            if opcua
            else ["vector", "mosquitto", "telegraf"]
        )
        error_log = (
            command_tail(error, (simulator, collector))
            if isinstance(
                error,
                (subprocess.CalledProcessError, subprocess.TimeoutExpired),
            )
            else ""
        )
        if error_log:
            print(error_log)
        # 状態確認が失敗しても、先に起きた取得・起動の例外を残す。
        with suppress(OSError, subprocess.TimeoutExpired):
            states = run(
                "ps", "--all", "--format", "json", check=False, timeout=20
            )
            for line in states.stdout.splitlines():
                try:
                    result = json.loads(line)
                except ValueError:
                    continue
                rows = result if isinstance(result, list) else [result]
                for row in rows:
                    if isinstance(row, dict):
                        print(
                            row.get("Service"),
                            row.get("State"),
                            row.get("ExitCode"),
                        )
            logs = run(
                "logs",
                "--tail",
                "20",
                "broker",
                "greptime",
                *services,
                check=False,
                timeout=30,
            )
            details = (
                (logs.stdout + logs.stderr)
                .replace(simulator, "[非表示]")
                .replace(collector, "[非表示]")
            )
            if details:
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
    parser.add_argument(
        "--capacity-rows",
        type=int,
        help="MQTTの容量試験に送る合成点数（1000〜100000）",
    )
    args = parser.parse_args()
    if args.capacity_rows is not None and (
        args.opcua or not 1_000 <= args.capacity_rows <= 100_000
    ):
        parser.error("--capacity-rows はMQTTの1000〜100000点に使えます。")
    docker = ["sudo", "-n", "docker"] if args.sudo else ["docker"]
    with tempfile.TemporaryDirectory(prefix="foundation-r-telemetry-") as temp:
        verify(
            docker,
            Path(temp),
            opcua=args.opcua,
            nisshi=args.nisshi,
            capacity_rows=args.capacity_rows,
        )


if __name__ == "__main__":
    main()

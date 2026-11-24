"""RisingWaveの合成イベントをIceberg V2で確定し、PyIcebergで照合する。"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent
PROJECT = "fr-iceberg-" + ROOT.name.split(".")[-1].lower()
if not re.fullmatch(r"fr-iceberg-[a-z0-9]{8}", PROJECT):
    raise RuntimeError("合成試験の専用プロジェクト名が不正です。")

# 専用Composeだけを使う。任意の接続先や資格情報を受け付けない。
PRIVATE = dict(
    line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
)
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["NO_PROXY"] = "127.0.0.1,localhost"
os.environ["no_proxy"] = "127.0.0.1,localhost"
HTTP = requests.Session()
HTTP.trust_env = False
WAREHOUSE = "fixture-warehouse"
BUCKET = PROJECT + "-warehouse"
TOPIC = "fixture-events-v1"

# 時刻の正規化と小数の値を同時に確認するため、異なるUTCオフセットを混ぜる。
INPUT = [
    ("E001", "2026-01-05T21:00:00.000101+09:00", "temperature", 22.5),
    ("E002", "2026-01-05T12:00:01.000202Z", "temperature", 23.5),
    ("E003", "2026-01-05T07:00:02.000303-05:00", "temperature", 24.5),
    ("E004", "2026-01-05T12:00:03.000404Z", "pressure", 2.125),
    ("E005", "2026-01-05T21:00:04.000505+09:00", "pressure", 2.25),
    ("E006", "2026-01-05T07:00:05.000606-05:00", "pressure", 2.375),
    ("E007", "2026-01-05T12:00:06.000707Z", "temperature", 25.75),
]
EXPECTED = [
    ("E001", "2026-01-05T12:00:00.000101+00:00", "temperature", 22.5),
    ("E002", "2026-01-05T12:00:01.000202+00:00", "temperature", 23.5),
    ("E003", "2026-01-05T12:00:02.000303+00:00", "temperature", 24.5),
    ("E004", "2026-01-05T12:00:03.000404+00:00", "pressure", 2.125),
    ("E005", "2026-01-05T12:00:04.000505+00:00", "pressure", 2.25),
    ("E006", "2026-01-05T12:00:05.000606+00:00", "pressure", 2.375),
    ("E007", "2026-01-05T12:00:06.000707+00:00", "temperature", 25.75),
]


def redact(value: str) -> str:
    """合成環境の短命資格情報も実行結果に転記しない。"""
    for secret in PRIVATE.values():
        value = value.replace(secret, "<redacted>")
    return value


def compose(
    *args: str, input_text: str | None = None, timeout: int = 60
) -> str:
    """固定ファイル・固定プロジェクトのComposeだけを呼び出す。"""
    command = (
        *(
            ("sudo", "-n")
            if os.environ.get("FIXTURE_DOCKER_SUDO") == "1"
            else ()
        ),
        "docker",
        "compose",
        "--project-name",
        PROJECT,
        "--env-file",
        str(ROOT / ".env"),
        "-f",
        str(ROOT / "compose.yaml"),
        *args,
    )
    result = subprocess.run(
        command,
        input=input_text,
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            redact(result.stderr.strip() or result.stdout.strip())
        )
    return result.stdout.strip()


def sql(statement: str, timeout: int = 60) -> str:
    """SQLを標準入力に渡し、資格情報はSQLに含めない。"""
    return compose(
        "exec",
        "-T",
        "catalog-db",
        "psql",
        "-XAtq",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        "risingwave",
        "-p",
        "4566",
        "-U",
        "root",
        "-d",
        "dev",
        input_text=statement,
        timeout=timeout,
    )


def loopback(service: str, port: str) -> str:
    """Composeが割り当てたループバックアドレスだけを採用する。"""
    target = compose("port", service, port)
    if not re.fullmatch(r"127\.0\.0\.1:[1-9][0-9]{0,4}", target):
        raise RuntimeError("合成サービスがループバックに限定されていません。")
    return "http://" + target


def wait_for(operation, label: str, *, seconds: int = 75):
    """依存サービスやコミットの準備を期限付きで待つ。"""
    deadline = time.monotonic() + seconds
    while True:
        try:
            return operation()
        except Exception as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"{label}: {redact(str(error))}") from None
            time.sleep(1)


def catalog_http(method: str, path: str, payload: dict | None = None) -> dict:
    """許可したHTTP応答だけを受け取り、要求本文を診断出力に含めない。"""
    result = HTTP.request(
        method,
        loopback("catalog", "8181") + path,
        json=payload,
        timeout=45,
        allow_redirects=False,
    )
    if result.status_code not in {200, 201, 204}:
        message = result.json().get("error", {}).get("message", "")
        raise RuntimeError(
            f"Lakekeeper HTTP {result.status_code}: {redact(str(message))}"
        )
    return result.json() if result.content else {}


def bootstrap() -> None:
    """専用S3バケットと、静的S3鍵を採用する試験warehouseを準備する。"""
    s3 = boto3.client(
        "s3",
        endpoint_url=loopback("storage", "8333"),
        region_name="us-east-1",
        aws_access_key_id=PRIVATE["S3_ACCESS_KEY"],
        aws_secret_access_key=PRIVATE["S3_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}, proxies={}),
    )
    wait_for(s3.list_buckets, "SeaweedFS S3")

    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {
            "404",
            "NoSuchBucket",
            "NotFound",
        }:
            raise
        s3.create_bucket(Bucket=BUCKET)

    info = wait_for(
        lambda: catalog_http("GET", "/management/v1/info"), "Lakekeeper"
    )
    if not info["bootstrapped"]:
        catalog_http(
            "POST", "/management/v1/bootstrap", {"accept-terms-of-use": True}
        )
    warehouses = catalog_http("GET", "/management/v1/warehouse").get(
        "warehouses", []
    )
    if not any(
        item.get("name", item.get("warehouse-name")) == WAREHOUSE
        for item in warehouses
    ):
        catalog_http(
            "POST",
            "/management/v1/warehouse",
            {
                "warehouse-name": WAREHOUSE,
                "project-id": "00000000-0000-0000-0000-000000000000",
                "storage-profile": {
                    "type": "s3",
                    "bucket": BUCKET,
                    "key-prefix": "fixture",
                    "endpoint": "http://storage:8333",
                    "region": "us-east-1",
                    "path-style-access": True,
                    "flavor": "s3-compat",
                    "sts-enabled": False,
                    "remote-signing-enabled": False,
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "access-key-id": PRIVATE["S3_ACCESS_KEY"],
                    "secret-access-key": PRIVATE["S3_SECRET_KEY"],
                },
            },
        )
    compose("run", "--no-deps", "--rm", "-T", "reader", "namespace")


def initialize() -> None:
    """Kafkaソースと、非JVMのIceberg V2シンクを作る。"""
    bootstrap()
    wait_for(lambda: sql("SELECT 1;", timeout=5), "RisingWave SQL")
    compose(
        "exec",
        "-T",
        "broker",
        "rpk",
        "cluster",
        "health",
        "--exit-when-healthy",
    )
    topic_list = compose("exec", "-T", "broker", "rpk", "topic", "list")
    topics = {
        line.split()[0] for line in topic_list.splitlines() if line.split()
    }
    if TOPIC not in topics:
        compose("exec", "-T", "broker", "rpk", "topic", "create", TOPIC)

    # RisingWaveは、独立したComposeプロセスのAWS環境変数を使う。
    statement = """
CREATE CONNECTION IF NOT EXISTS fixture_rest WITH (
    type = 'iceberg',
    catalog.type = 'rest_rust',
    catalog.uri = 'http://catalog:8181/catalog',
    warehouse.path = 'fixture-warehouse',
    s3.endpoint = 'http://storage:8333',
    s3.region = 'us-east-1',
    s3.path.style.access = 'true'
);
CREATE SOURCE IF NOT EXISTS fixture_source (
    event_id VARCHAR,
    event_time TIMESTAMPTZ,
    measurement VARCHAR,
    metric_value DOUBLE PRECISION
)
WITH (
    connector = 'kafka',
    topic = 'fixture-events-v1',
    properties.bootstrap.server = 'broker:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
SET sink_decouple = true;
CREATE SINK IF NOT EXISTS fixture_sink AS
SELECT event_id, event_time, measurement, metric_value FROM fixture_source
WITH (
    connector = 'iceberg',
    type = 'append-only',
    connection = fixture_rest,
    database.name = 'fixture',
    table.name = 'events_v2',
    write_mode = 'merge-on-read',
    create_table_if_not_exists = 'true',
    format_version = '2',
    commit_checkpoint_interval = '1',
    is_exactly_once = 'true',
    enable_snapshot_expiration = 'false'
);
"""
    sql(statement)
    if sql("SELECT is_decouple FROM rw_sink_decouple;") != "t":
        raise RuntimeError("評価シンクのdecouplingが無効です。")
    print(
        json.dumps(
            {
                "phase": "initialize",
                "risingwave": sql("select version();"),
                "catalog_type": "rest_rust",
                "sink_decouple": True,
            },
            ensure_ascii=False,
        )
    )


def publish(start: int, end: int) -> None:
    """1イベントを1 Kafkaメッセージとして、専用トピックに発行する。"""
    names = ("event_id", "event_time", "measurement", "metric_value")
    data = "".join(
        json.dumps(dict(zip(names, row))) + "\n" for row in INPUT[start:end]
    )
    compose(
        "exec",
        "-T",
        "broker",
        "rpk",
        "topic",
        "produce",
        TOPIC,
        input_text=data,
    )


def inspect(expected: int) -> dict:
    """実ファイルをPyIcebergで読み、形式、件数、各列、重複なしを照合する。"""
    result = json.loads(
        compose("run", "--no-deps", "--rm", "-T", "reader", "read")
    )
    rows = [tuple(row) for row in result["rows"]]
    if rows != EXPECTED[:expected]:
        raise RuntimeError(f"期待した{expected}行と一致しません: {rows!r}")
    if result["pyiceberg"] != "0.12.0":
        raise RuntimeError("PyIcebergのバージョンが固定値と一致しません。")
    return result


def baseline() -> None:
    """初回3行を書き込み、Icebergへのコミットが完了するまで待つ。"""
    publish(0, 3)
    sql("FLUSH;")
    result = wait_for(lambda: inspect(3), "初回Iceberg書込み")
    (ROOT / "baseline.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"phase": "baseline", **result}))


def recover() -> None:
    """ボリュームを保った再起動と、その後の追加書込みを確認する。"""
    before = json.loads((ROOT / "baseline.json").read_text())
    compose("restart", "risingwave")
    wait_for(
        lambda: sql("FLUSH;", timeout=10),
        "RisingWave 再起動とチェックポイント",
        seconds=120,
    )
    after = wait_for(lambda: inspect(3), "再起動後のIceberg読取り")
    if (
        before["snapshot_id"] != after["snapshot_id"]
        or before["snapshots"] != after["snapshots"]
    ):
        raise RuntimeError(
            "入力なしの再起動後にスナップショットが増えました。"
        )
    result = {"phase": "restart", **after}
    print(json.dumps(result))
    publish(3, 5)
    sql("FLUSH;")
    final = wait_for(lambda: inspect(5), "再起動後の追加書込み")
    if final["snapshot_id"] == before["snapshot_id"]:
        raise RuntimeError(
            "追加書込みでIcebergのスナップショットが進みませんでした。"
        )
    print(json.dumps({"phase": "final", **final}))


def replay() -> None:
    """Icebergコミットができない区間で上流だけ永続化し、再起動後の再送を照合する。"""
    before = inspect(5)
    sql(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS fixture_receipts "
        "AS SELECT COUNT(*) AS total FROM fixture_source;"
    )

    def receipts(expected: int) -> int:
        """Icebergと別に、RisingWaveのチェックポイントに到達した入力数を読む。"""
        sql("FLUSH;", timeout=10)
        total = int(sql("SELECT total FROM fixture_receipts;", timeout=5))
        if total != expected:
            raise RuntimeError(
                f"RisingWaveの受領数が{expected}件に達していません。"
            )
        return total

    wait_for(lambda: receipts(5), "停止前の受領数")
    compose("stop", "catalog")
    try:
        publish(5, 7)
        total = wait_for(
            lambda: receipts(7), "カタログ停止中のRisingWaveチェックポイント"
        )
        print(
            json.dumps({"phase": "catalog_offline", "upstream_count": total})
        )
        compose("restart", "risingwave")
    finally:
        compose("start", "catalog")

    wait_for(
        lambda: sql("FLUSH;", timeout=10),
        "再開後のRisingWaveチェックポイント",
        seconds=120,
    )
    after = wait_for(
        lambda: inspect(7), "カタログ再開後のIcebergコミット", seconds=120
    )
    if before["snapshot_id"] == after["snapshot_id"] or not set(
        before["snapshots"]
    ) <= set(after["snapshots"]):
        raise RuntimeError(
            "復旧後のスナップショット系列が停止前の系列を継承していません。"
        )
    before_restart = after["snapshot_id"]
    compose("restart", "risingwave")
    wait_for(lambda: sql("FLUSH;", timeout=10), "再送後の再起動", seconds=120)
    stable = wait_for(lambda: inspect(7), "再送後のIceberg読取り", seconds=120)
    if stable["snapshot_id"] != before_restart:
        raise RuntimeError(
            "再送完了後の再起動でスナップショットが増えました。"
        )
    print(json.dumps({"phase": "replay", **stable}))


def audit() -> None:
    """秘密の値自体を出力せず、起動したプロジェクトのログに混入していないか調べる。"""
    services = (
        "risingwave",
        "catalog",
        "catalog-db",
        "catalog-migrate",
        "storage",
        "broker",
    )
    for service in services:
        log = compose("logs", service)
        if any(secret in log for secret in PRIVATE.values()):
            raise RuntimeError(
                f"{service}のログに隔離試験の資格情報が含まれています。"
            )
    print(
        json.dumps(
            {
                "phase": "audit",
                "services": list(services),
                "credentials_in_logs": False,
            }
        )
    )


def main() -> int:
    """選択された受入段階を実行し、資格情報を含まない証拠を出力する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=("initialize", "baseline", "recover", "replay", "audit"),
    )
    arguments = parser.parse_args()
    phases = {
        "initialize": initialize,
        "baseline": baseline,
        "recover": recover,
        "replay": replay,
        "audit": audit,
    }
    try:
        phases[arguments.phase]()
    except Exception as error:
        print(redact(str(error)), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

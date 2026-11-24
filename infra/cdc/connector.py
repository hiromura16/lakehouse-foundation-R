"""SQL ServerのCDCコネクターを1件作成し、既存時は設定の一致を確認する。

非公開設定と検証済みの6表の対応を読み、内部またはloopbackの
Kafka Connect RESTエンドポイントへ接続する。未作成の場合はPOSTする。
既存コネクターは設定値すべての一致を求める。この処理はコネクターの更新、
削除、オフセットやスキーマ履歴のリセットを行わない。
データベース側のCDC有効化と権限の設定はDBAが別途行う。
"""

import argparse
import json
import os
import re
import sys
import time
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from config import read_env, selected_mapping, settings
from fixed_source import EXTRA_COLUMNS, preflight, validate_mapping


def configuration(values, mapping):
    """検証した表・列一覧から、TLSを必須にしたコネクター設定を作る。

    返す設定では認証情報をConnect側の環境変数参照として保持する。
    ホスト名とデータベース名は非公開の設定値なので、
    設定全体を公開ログや文書へ掲載しない。
    """
    fixed = values.get("CDC_SOURCE_MODE") == "fixed-demo"
    if fixed:
        validate_mapping(mapping)
    selected = [
        re.escape(item["source_schema"] + "." + item["source_table"])
        for item in mapping.values()
    ]
    selected_columns = [
        re.escape(
            item["source_schema"] + "." + item["source_table"] + "." + column
        )
        for item in mapping.values()
        for column in item["columns"]
    ]
    if fixed:
        selected_columns += [
            re.escape(
                item["source_schema"]
                + "."
                + item["source_table"]
                + "."
                + column
            )
            for item in mapping.values()
            for column in EXTRA_COLUMNS
        ]
    credential_prefix = "SOURCE_SQLSERVER" if fixed else "CDC_SQLSERVER"
    result = {
        "connector.class": (
            "io.debezium.connector.sqlserver.SqlServerConnector"
        ),
        "tasks.max": "1",
        "database.hostname": values["CDC_SQLSERVER_HOST"],
        "database.port": values["CDC_SQLSERVER_PORT"],
        "database.names": values["CDC_SQLSERVER_DATABASE"],
        "database.user": "${env:" + credential_prefix + "_USER}",
        "database.password": "${env:" + credential_prefix + "_PASSWORD}",
        "topic.prefix": values["CDC_TOPIC_PREFIX"],
        "table.include.list": ",".join(selected),
        "column.include.list": ",".join(selected_columns),
        "schema.history.internal.kafka.bootstrap.servers": "cdc-kafka:9092",
        "schema.history.internal.kafka.topic": (
            "_history_" + values["CDC_TOPIC_PREFIX"]
        ),
        "snapshot.mode": "initial",
        # 固定構成の概念実証では、書込の並行性より基準の一貫性を優先する。
        # 初回スナップショットが完了するまで排他ロックを保持する。
        "snapshot.isolation.mode": "exclusive" if fixed else "read_committed",
        "include.schema.changes": "false",
        "tombstones.on.delete": "true",
        "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive" if fixed else "connect",
        "driver.encrypt": "true",
        "driver.trustServerCertificate": (
            values.get("SOURCE_SQLSERVER_TRUST_SERVER_CERTIFICATE", "false")
            if fixed
            else values["CDC_TEST_TRUST_SERVER_CERTIFICATE"]
        ),
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "topic.creation.default.partitions": "1",
        "topic.creation.default.replication.factor": "1",
    }
    if fixed:
        result.update(
            {
                "provide.transaction.metadata": "true",
                "event.processing.failure.handling.mode": "fail",
                "event.converting.failure.handling.mode": "fail",
            }
        )
        for scope in ("tables", "databases"):
            key = "schema.history.internal.store.only.captured." + scope
            result[key + ".ddl"] = "true"
    truststore = values.get("CDC_SQLSERVER_TRUSTSTORE", "")
    if truststore:
        if not truststore.startswith("/run/cdc/") or ".." in truststore:
            raise ValueError("Mount the SQL truststore under /run/cdc/.")
        result["driver.trustStore"] = truststore
        result["driver.trustStorePassword"] = (
            "${env:CDC_SQLSERVER_TRUSTSTORE_PASSWORD}"
        )
    hostname = values.get(credential_prefix + "_HOSTNAME_IN_CERTIFICATE", "")
    if hostname:
        if not re.fullmatch(r"[A-Za-z0-9_.*-]+", hostname):
            raise ValueError("Invalid SQL certificate hostname.")
        result["driver.hostNameInCertificate"] = hostname
    agent = values.get("CDC_SQLSERVER_AGENT_STATUS_QUERY", "")
    if agent:
        # DBAが署名済みで読取専用の状態確認UDFを公開している場合がある。
        # 共有の設定項目で任意のSQLを受け付けない。
        if not re.fullmatch(
            r"SELECT \[#db\]\.\[?[A-Za-z_][A-Za-z0-9_]*\]?"
            r"\.\[?[A-Za-z_][A-Za-z0-9_]*\]?\(\)",
            agent,
            re.IGNORECASE,
        ):
            raise ValueError("Use a DBA-approved scalar status function.")
        result["database.sqlserver.agent.status.query"] = agent
    return result


def register(values, mapping, endpoint):
    """Connectの起動を待ち、1回だけ作成するか既存設定の一致を確認する。

    接続エラーは起動確認として180秒まで再試行する。
    HTTPや設定の異常では停止する。
    既存設定にConnectが同じ値の name を追加した場合だけ許容する。
    他の差分は拒否し、PUT、PATCH、DELETEを発行しない。
    """
    if values.get("CDC_SOURCE_MODE") == "fixed-demo":
        # SQLの所有権か読取範囲が異なる場合、ConnectにPOSTする前に拒否する。
        preflight(values, mapping)
    opener = build_opener(ProxyHandler({}))
    name = values["CDC_CONNECTOR_NAME"]
    candidate = configuration(values, mapping)
    deadline = time.monotonic() + 180
    while True:
        try:
            with opener.open(endpoint + "/connectors", timeout=5) as reply:
                if reply.status != 200:
                    raise RuntimeError("Kafka Connect discovery was rejected.")
                existing = json.load(reply)
            if not isinstance(existing, list) or not all(
                isinstance(item, str) for item in existing
            ):
                raise ValueError("Invalid Kafka Connect connector list.")
            break
        except HTTPError:
            # 認証やHTTPの異常は確認が必要なため、作成を再試行しない。
            raise RuntimeError(
                "Kafka Connect discovery was rejected."
            ) from None
        except OSError:
            if time.monotonic() >= deadline:
                raise RuntimeError("Kafka Connect did not become ready.")
            time.sleep(2)
    if name in existing:
        with opener.open(
            endpoint + "/connectors/" + name + "/config", timeout=5
        ) as reply:
            if reply.status != 200:
                raise RuntimeError("Connector configuration was unavailable.")
            current = json.load(reply)
        if not isinstance(current, dict):
            raise ValueError("Invalid existing connector configuration.")
        # Connectが name を追加する場合は、一致する値だけ除外できる。
        # 他のキーと値は、型変換をせず厳密に一致する必要がある。
        if "name" in current:
            if current["name"] != name:
                raise ValueError("Existing connector name did not match.")
            current = {
                key: value for key, value in current.items() if key != "name"
            }
        if current != candidate:
            raise ValueError(
                "Connector configuration differs; review it before changing."
            )
        print(
            json.dumps(
                {"registered": True, "unchanged": True, "tables": len(mapping)}
            )
        )
        return
    payload = {"name": name, "config": candidate}
    request = Request(
        endpoint + "/connectors",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with opener.open(request, timeout=30) as response:
        if response.status != 201:
            raise RuntimeError("Connector registration was not accepted.")
    print(json.dumps({"registered": True, "tables": len(mapping)}))


def main():
    """認証情報や非公開のデータベース名を表示せずに登録する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        help="private envの入力先。省略時は起動プロセスの環境変数を使用",
    )
    parser.add_argument(
        "--table-map",
        default="/run/cdc/table-map.json",
        help="許可する6表・列の対応JSON。既定: /run/cdc/table-map.json",
    )
    parser.add_argument(
        "--endpoint",
        default="http://cdc-connect:8083",
        help=(
            "Connect RESTの内部URL、または設定portのloopback URL。"
            "既定: http://cdc-connect:8083。任意の外部URLは使えません"
        ),
    )
    args = parser.parse_args()
    values = settings(read_env(args.env_file) if args.env_file else os.environ)
    if args.endpoint not in {
        "http://cdc-connect:8083",
        "http://127.0.0.1:" + values["CDC_CONNECT_PORT"],
        "http://localhost:" + values["CDC_CONNECT_PORT"],
    }:
        raise ValueError(
            "Use the internal or loopback Kafka Connect endpoint."
        )
    register(values, selected_mapping(values, args.table_map), args.endpoint)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, HTTPError, OSError):
        print("CDC connector registration failed; inspect private status.")
        sys.exit(1)

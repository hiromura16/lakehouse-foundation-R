"""合成センサーデータと自動設定された画面で標準Composeを検証する。"""

import argparse
import base64
import hashlib
import json
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def fixture(root):
    """合成したセンサー5行と、隣接する整合性確認用の完了印を配置する。"""
    point = {
        "factory_id": "F-SYNTHETIC",
        "line_id": "L1",
        "equipment_id": "E1",
        "sensor_id": "temperature",
        "measurement": "temperature",
        "unit": "C",
        "timestamp": "2026-01-05T12:00:00Z",
        "sequence_no": 1,
        "value": 24.5,
        "quality_code": "GOOD",
    }
    later = {
        **point,
        "sequence_no": 2,
        "timestamp": "2026-01-05T21:00:30+09:00",
        "value": 90,
        "quality_code": "BAD",
    }
    conflict = {**point, "sequence_no": 3}
    rows = [point, dict(point), later, conflict, {**conflict, "value": 99}]
    data = b"".join(json.dumps(row).encode() + b"\n" for row in rows)
    path = root / "sensor/acceptance.jsonl"
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(data)
    marker = path.with_suffix(".jsonl.ready")
    marker.write_text(hashlib.sha256(data).hexdigest() + "\n")


def sql_fixture(root):
    """抽出側の6表形式に合わせ、合成製品1件を含むバッチを作る。"""
    contracts = json.loads(
        (
            Path(__file__).resolve().parents[3] / "medallion/sources.json"
        ).read_text()
    )["sources"]
    tables = {}
    for contract in contracts:
        name = contract["table"]
        domain, table = name.split("_", 1)
        rows = []
        if name == "erp_products":
            rows.append(
                {
                    "source_pk": "SQL-SYNTHETIC",
                    "source_updated_at": "2026-01-05T11:00:00.0000001Z",
                    "run_id": "012345abcdef",
                    "values": {
                        "product_id": "SQL-SYNTHETIC",
                        "product_name": "SQL synthetic sample",
                        "product_family": "synthetic",
                        "unit": "pcs",
                        "standard_cycle_seconds": "12.500",
                        "updated_at": "2026-01-05T11:00:00.0000001Z",
                    },
                }
            )
        tables[name] = {
            "schema": f"lh_demo_{domain}",
            "table": table,
            "primary_key": contract["columns"][0],
            "rows": rows,
        }
    data = json.dumps(
        {
            "format_version": 1,
            "source_system": "sqlserver-fixed-demo",
            "source_identity": "a" * 64,
            "source_database": "synthetic_sales",
            "mode": "snapshot",
            "tables": tables,
        }
    ).encode()
    batch_id = hashlib.sha256(data).hexdigest()
    directory = root / "sql-batches"
    directory.mkdir(exist_ok=True)
    (directory / f"{batch_id}.json").write_bytes(data)
    (directory / f"{batch_id}.json.ready").write_text(batch_id + "\n")
    return {
        "action": "sql-load",
        "batch_id": batch_id,
        "observed_at": "2026-01-05T12:00:00.0000001Z",
    }


def fetch(url, *, authorization=None, body=None):
    """タイムアウト付きでHTTP要求を送り、復号したJSONを返す。"""
    headers = {"Content-Type": "application/json"}
    if authorization:
        headers["Authorization"] = authorization
    payload = None if body is None else json.dumps(body).encode()
    with urlopen(
        Request(url, headers=headers, data=payload), timeout=300
    ) as resp:
        return json.load(resp)


def load_test_env(path):
    """生成済みのテスト設定だけを、シェルで評価せずに読み込む。"""
    return dict(
        line.split("=", 1)
        for line in path.read_text().splitlines()
        if line and not line.startswith("#")
    )


def check(args, env):
    """トークン分離、再実行、固定したGold、Grafanaの自動設定を確認する。"""
    reader = "Bearer " + env["READER_TOKEN"]
    writer = "Bearer " + env["WORKER_TOKEN"]
    http = f"http://127.0.0.1:{args.api_port}"
    assert fetch(http + "/health")["ready"]
    for supplied in (None, writer):
        try:
            fetch(http + "/v1/gold/sensor_latest", authorization=supplied)
        except HTTPError as error:
            assert error.code == 401
        else:
            raise AssertionError("Gold APIが無効な資格情報を受け付けました。")
    try:
        fetch(
            http + "/run", authorization=reader, body={"action": "medallion"}
        )
    except HTTPError as error:
        assert error.code == 401
    else:
        raise AssertionError("書込APIが読取用の資格情報を受け付けました。")

    build = fetch(
        http + "/run", authorization=writer, body={"action": "medallion"}
    )
    assert build["imported"] >= 1 and build["sql_statements"] == 14, build
    latest = fetch(http + "/v1/gold/sensor_latest", authorization=reader)
    assert latest["total_rows"] == 1 and latest["rows"][0]["value"] == 90
    assert latest["rows"][0]["quality_code"] == "BAD"
    release = latest["release_snapshot_id"]
    minute = fetch(
        http + f"/v1/gold/sensor_minute?release={release}&limit=1",
        authorization=reader,
    )
    (row,) = minute["rows"]
    assert minute["release_snapshot_id"] == release
    assert (row["sample_count"], row["good_count"], row["good_avg"]) == (
        2,
        1,
        24.5,
    )
    replay = fetch(
        http + "/run", authorization=writer, body={"action": "medallion"}
    )
    assert replay["imported"] == 0 and replay["unchanged"], replay
    assert (
        fetch(http + "/v1/releases/latest", authorization=reader)[
            "release_snapshot_id"
        ]
        == release
    )
    verified = fetch(
        http + "/run", authorization=writer, body={"action": "verify"}
    )
    assert verified["ok"] and len(verified["checks"]) == 11

    batch = sql_fixture(args.landing)
    sql_first = fetch(http + "/run", authorization=writer, body=batch)
    assert (
        sql_first["counts"]["erp_products"] == 1 and not sql_first["skipped"]
    )
    sql_same = fetch(http + "/run", authorization=writer, body=batch)
    assert sql_same["skipped"]
    sql_gold = fetch(
        http + "/v1/sql/gold/sales_fulfillment", authorization=reader
    )
    assert sql_gold["table"] == "sql_gold.sales_fulfillment"
    assert sql_gold["total_rows"] == 0

    creds = base64.b64encode(
        ("admin:" + env["GRAFANA_ADMIN_PASSWORD"]).encode()
    ).decode()
    grafana = f"http://127.0.0.1:{args.grafana_port}"
    for _ in range(20):
        try:
            result = fetch(
                grafana + "/api/dashboards/uid/lakehouse-r-runtime",
                authorization="Basic " + creds,
            )
            break
        except (OSError, HTTPError):
            time.sleep(1)
    else:
        raise AssertionError(
            "Grafanaにダッシュボードが自動設定されませんでした。"
        )
    assert len(result["dashboard"]["panels"]) >= 6
    assert result["meta"]["provisioned"]
    print(
        json.dumps(
            {
                "fixture_parity_checks": len(verified["checks"]),
                "tested_release_snapshot_id": release,
                "sensor_count": row["sample_count"],
                "grafana_panels": len(result["dashboard"]["panels"]),
                "dashboard_file_provisioned": True,
                "reader_writer_token_isolation": True,
                "sql_batch_replay": True,
                "identical_run_kept_release": True,
            }
        )
    )


def check_gui(args):
    """同じ認証で3画面が開き、カタログの書込みが拒否されることを確認する。"""
    env = load_test_env(args.gui_env_file)
    encoded = base64.b64encode(
        (env["OPS_USER"] + ":" + env["OPS_PASSWORD"]).encode()
    ).decode()
    auth = "Basic " + encoded
    checks = (
        (18503, "/_stcore/health"),
        (18504, "/ui/"),
        (18505, "/targets"),
    )
    for port, path in checks:
        url = f"http://127.0.0.1:{port}{path}"
        try:
            urlopen(url, timeout=5)
        except HTTPError as error:
            assert error.code == 401, (port, error.code)
        else:
            raise AssertionError(f"認証なしの閲覧が可能です。ポート {port}")
        request = Request(url, headers={"Authorization": auth})
        with urlopen(request, timeout=5) as response:
            assert response.status == 200, port

    try:
        fetch(
            "http://127.0.0.1:18504/health",
            authorization=auth,
            body={},
        )
    except HTTPError as error:
        assert error.code == 403
    else:
        raise AssertionError("Lakekeeperへの書込みが可能です。")

    query = urlencode({"query": "probe_success"})
    url = f"http://127.0.0.1:18505/api/v1/query?{query}"
    deadline = time.monotonic() + 65
    while time.monotonic() < deadline:
        result = fetch(url, authorization=auth)["data"]["result"]
        probes = {item["metric"]["app"]: item["value"][1] for item in result}
        if len(probes) == 9 and set(probes.values()) == {"1"}:
            print(
                "閲覧3画面の認証、Lakekeeperの書込み拒否、全9監視先を確認しました。"
            )
            return
        time.sleep(2)
    raise AssertionError(f"監視先の初回取得が完了しませんでした。{probes}")


def main():
    """新規の資格情報を使い、隔離したComposeプロジェクトを検証する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--landing", required=True, type=Path)
    parser.add_argument("--api-port", required=True, type=int)
    parser.add_argument("--grafana-port", required=True, type=int)
    parser.add_argument("--gui-env-file", type=Path)
    args = parser.parse_args()
    fixture(args.landing)
    check(args, load_test_env(args.env_file))
    if args.gui_env_file:
        check_gui(args)


if __name__ == "__main__":
    main()

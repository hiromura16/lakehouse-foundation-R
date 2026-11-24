"""合成センサーデータと自動設定された画面で標準Composeを検証する。"""

import argparse
import base64
import hashlib
import json
import secrets
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


def reject_bad_header(root, http, writer, reader, release):
    """列の違う0行CSVを拒否し、直前の公開世代が保持されることを確認する。"""
    path = root / f"csv/erp_products__{secrets.token_hex(6)}_0001.csv"
    path.parent.mkdir(exist_ok=True)
    marker = path.with_suffix(".csv.ready")
    payload = b"unexpected_column\n"
    with path.open("xb") as file:
        file.write(payload)
    created_marker = False
    try:
        with marker.open("x") as file:
            created_marker = True
            file.write(hashlib.sha256(payload).hexdigest() + "\n")
        try:
            fetch(
                http + "/run",
                authorization=writer,
                body={"action": "medallion"},
            )
        except HTTPError as error:
            assert error.code == 500
            assert json.load(error) == {"error": "JOB_FAILED"}
        else:
            raise AssertionError("契約外のCSVヘッダーを受け入れました。")
        latest = fetch(http + "/v1/releases/latest", authorization=reader)
        assert latest["release_snapshot_id"] == release
        status = fetch(http + "/v1/ingestion/status", authorization=reader)
        attempt = status["last_attempt"]
        assert attempt["status"] == "failed"
        assert attempt["error"] == "JOB_FAILED"
        assert status["published"]["release_snapshot_id"] == release
    finally:
        if created_marker:
            marker.unlink()
        path.unlink()


def check_archive(root, http, writer, reader, release):
    """通常取込より大きな原本と再送・訂正を、標準の操作APIで確かめる。"""
    folder = root / "large-raw"
    folder.mkdir(exist_ok=True)
    origin = folder / f"inspection-{secrets.token_hex(4)}.bin"
    marker = origin.with_suffix(".bin.sealed")
    endpoint = http + "/run"
    job = {"action": "archive", "file": origin.name}
    size = 20 * 1024 * 1024

    def write_revision(pattern):
        """原本を分割して作り、最後に同じ原本の完了印を置く。"""
        digest = hashlib.sha256()
        chunk = pattern * (1024 * 1024 // len(pattern))
        with origin.open("wb") as stream:
            for _ in range(size // len(chunk)):
                stream.write(chunk)
                digest.update(chunk)
        marker.write_text(digest.hexdigest() + "\n")
        return digest.hexdigest()

    def reject(body, actor, code, error_code):
        """書込権限と原本の受入条件を別々に検査する。"""
        try:
            fetch(endpoint, authorization=actor, body=body)
        except HTTPError as exc:
            assert exc.code == code
            assert json.load(exc) == {"error": error_code}
        else:
            raise AssertionError("失敗するはずの原本保全を受け入れました。")

    reject(job, reader, 401, "AUTHENTICATION_REQUIRED")
    reject({**job, "file": "../inspection.bin"}, writer, 400, "INVALID_JOB")
    reject(job, writer, 422, "ARCHIVE_LOCAL_INPUT_INVALID")
    first_hash = write_revision(b"A")
    marker.write_text("0" * 64 + "\n")
    reject(job, writer, 422, "ARCHIVE_COMPLETION_HASH_MISMATCH")
    marker.write_text(first_hash + "\n")
    first = fetch(endpoint, authorization=writer, body=job)
    assert first == {
        "file": origin.name,
        "bytes": size,
        "sha256": first_hash,
        "object_created": True,
        "registered": True,
    }
    replay = fetch(endpoint, authorization=writer, body=job)
    assert replay == {**first, "object_created": False, "registered": False}
    second_hash = write_revision(b"B")
    changed = fetch(endpoint, authorization=writer, body=job)
    assert changed == {**first, "sha256": second_hash}
    assert write_revision(b"A") == first_hash
    reverted = fetch(endpoint, authorization=writer, body=job)
    assert reverted == {**first, "object_created": False}
    assert (
        fetch(http + "/v1/releases/latest", authorization=reader)[
            "release_snapshot_id"
        ]
        == release
    )


def check_independent_lanes(root, http, writer, reader):
    """破損した設備完了印の横で業務が更新され、修復後に設備が再開する。"""
    source = root / "sensor/acceptance.jsonl"
    marker = source.with_suffix(".jsonl.ready")
    original = marker.read_text(encoding="utf-8")
    marker.write_text("invalid", encoding="utf-8")
    before = fetch(http + "/v1/releases/latest", authorization=reader)
    product = root / "csv/erp_products__000000000009_0001.csv"
    payload = (
        "product_id,product_name,product_family,unit,"
        "standard_cycle_seconds,updated_at\n"
        "SYNTH-001,合成部材,合成部門,EA,25,2026-01-05T12:00:00Z\n"
    ).encode()
    product.write_bytes(payload)
    product.with_suffix(".csv.ready").write_text(
        hashlib.sha256(payload).hexdigest() + "\n"
    )
    try:
        partial = fetch(
            http + "/run", authorization=writer, body={"action": "medallion"}
        )
        assert partial["sensor"]["status"] == "retained"
        assert partial["sensor"]["error"] == "SENSOR_INTAKE_FAILED"
        assert partial["sql_statements"] == 14
        status = fetch(http + "/v1/ingestion/status", authorization=reader)
        assert status["last_attempt"]["status"] == "partial"
        assert status["published"]["sensor"] == partial["sensor"]
        old_sensor = fetch(
            http + "/v1/gold/sensor_minute", authorization=reader
        )
        assert old_sensor["sensor"] == partial["sensor"]
        current = fetch(http + "/v1/releases/latest", authorization=reader)
        assert current["release_snapshot_id"] != before["release_snapshot_id"]
        assert isinstance(current["inputs"]["erp_products"], str)
        assert (
            current["inputs"]["erp_products"]
            != before["inputs"]["erp_products"]
        )
        for dataset in ("gold.sensor_minute", "gold.sensor_latest"):
            assert current["outputs"][dataset] == before["outputs"][dataset]
    finally:
        marker.write_text(original, encoding="utf-8")
    healed = fetch(
        http + "/run", authorization=writer, body={"action": "medallion"}
    )
    assert healed["sensor"]["status"] == "ready"
    assert healed["sql_statements"] == 0
    assert healed["sensor"]["built_at"] == before["sensor"]["built_at"]
    original_release = before["release_snapshot_id"]
    fixed_sensor = fetch(
        http + f"/v1/gold/sensor_minute?release={original_release}",
        authorization=reader,
    )
    assert fixed_sensor["sensor"]["status"] == "ready"
    assert fixed_sensor["snapshot_id"] == old_sensor["snapshot_id"]
    assert fixed_sensor["recipe"] == old_sensor["recipe"]
    retained_release = current["release_snapshot_id"]
    retained = fetch(
        http + f"/v1/gold/sensor_minute?release={retained_release}",
        authorization=reader,
    )
    assert retained["sensor"]["status"] == "retained"


def check_filters(http, reader, release):
    """世代固定の条件をIcebergへ渡し、空の結果と不正条件を分ける。"""
    base = http + f"/v1/gold/sensor_minute?release={release}&"
    criteria = {
        "factory_id": "F-SYNTHETIC",
        "equipment_id": "E1",
        "date_from": "2026-01-05",
        "date_to": "2026-01-06",
    }
    selected = fetch(base + urlencode(criteria), authorization=reader)
    assert selected["release_snapshot_id"] == release
    assert selected["filters"] == criteria
    assert selected["total_rows"] == 1
    assert selected["rows"][0]["good_avg"] == 24.5
    empty = fetch(base + "date_to=2026-01-05", authorization=reader)
    assert empty["total_rows"] == 0 and empty["rows"] == []
    for params in (
        {"date_from": "2026-1-5"},
        {"date_from": "2026-01-05", "date_to": "2026-01-05"},
        {"sales_order_id": "F-SYNTHETIC"},
    ):
        try:
            fetch(base + urlencode(params), authorization=reader)
        except HTTPError as error:
            assert error.code == 400
            assert json.load(error)["error"] == "INVALID_FILTER"
        else:
            raise AssertionError("不正な絞り込み条件を受け入れました。")


def check(args, env):
    """トークン分離、再実行、固定したGold、Grafanaの自動設定を確認する。"""
    reader = "Bearer " + env["READER_TOKEN"]
    writer = "Bearer " + env["WORKER_TOKEN"]
    http = f"http://127.0.0.1:{args.api_port}"
    assert fetch(http + "/health")["ready"]
    spec = fetch(http + "/openapi.json")
    assert spec["openapi"] == "3.2.1"
    assert "workerToken" in spec["paths"]["/run"]["post"]["security"][0]
    assert (
        "readerToken"
        in spec["paths"]["/v1/ingestion/status"]["get"]["security"][0]
    )
    products = fetch(http + "/v1/data-products", authorization=reader)
    assert products["format_version"] == 1
    assert len(products["datasets"]) == 4
    production = products["datasets"]["factory_daily_production"]
    assert production["key"] == ["factory_id", "production_date"]
    assert production["owner_role"] == "生産管理担当"
    try:
        fetch(http + "/v1/data-products", authorization=writer)
    except HTTPError as error:
        assert error.code == 401
    else:
        raise AssertionError("データ製品APIが操作トークンを受け付けました。")
    initial = fetch(http + "/v1/ingestion/status", authorization=reader)
    assert initial == {"last_attempt": None, "published": None}
    try:
        fetch(http + "/v1/ingestion/status", authorization=writer)
    except HTTPError as error:
        assert error.code == 401
    else:
        raise AssertionError("取込状態を操作トークンで取得できました。")
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
    status = fetch(http + "/v1/ingestion/status", authorization=reader)
    attempt = status["last_attempt"]
    assert attempt["status"] == "succeeded"
    assert attempt["imported_files"] == build["imported"]
    assert attempt["received_deliveries"] == build["received_deliveries"]
    assert status["published"]["gold_rows"]["sensor_latest"] == 1
    latest = fetch(http + "/v1/gold/sensor_latest", authorization=reader)
    assert latest["total_rows"] == 1 and latest["rows"][0]["value"] == 90
    assert latest["rows"][0]["quality_code"] == "BAD"
    release = latest["release_snapshot_id"]
    check_filters(http, reader, release)
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
    check_archive(args.landing, http, writer, reader, release)
    reject_bad_header(args.landing, http, writer, reader, release)
    check_independent_lanes(args.landing, http, writer, reader)
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
    panels = {item["id"]: item for item in result["dashboard"]["panels"]}
    assert len(panels) == 6
    assert result["meta"]["provisioned"]
    # 実際にプロビジョニングされたJSONで現在値と履歴の表示意図を検査する。
    for panel_id in (1, 6):
        panel = panels[panel_id]
        mappings = panel["fieldConfig"]["defaults"]["mappings"][0]["options"]
        assert mappings["0"]["text"] == "×" and mappings["1"]["text"] == "✓"
        assert panel["options"]["colorMode"] == "value"
    assert panels[2]["options"]["showValue"] == "never"
    assert {target["expr"] for target in panels[4]["targets"]} == {
        "lakehouse_successful_runs",
        "lakehouse_failed_runs",
        "lakehouse_partial_runs",
    }
    assert panels[5]["options"]["colorMode"] == "none"
    print(
        json.dumps(
            {
                "fixture_parity_checks": len(verified["checks"]),
                "tested_release_snapshot_id": release,
                "sensor_count": row["sample_count"],
                "grafana_panels": len(result["dashboard"]["panels"]),
                "dashboard_file_provisioned": True,
                "dashboard_status_symbols": True,
                "reader_writer_token_isolation": True,
                "sql_batch_replay": True,
                "identical_run_kept_release": True,
                "invalid_header_kept_release": True,
                "independent_business_sensor_release": True,
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
        (args.browser_port, "/_stcore/health"),
        (args.catalog_port, "/ui/"),
        (args.prometheus_port, "/targets"),
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
            f"http://127.0.0.1:{args.catalog_port}/health",
            authorization=auth,
            body={},
        )
    except HTTPError as error:
        assert error.code == 403
    else:
        raise AssertionError("Lakekeeperへの書込みが可能です。")

    query = urlencode({"query": "probe_success"})
    url = f"http://127.0.0.1:{args.prometheus_port}/api/v1/query?{query}"
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
    parser.add_argument("--browser-port", type=int, default=18503)
    parser.add_argument("--catalog-port", type=int, default=18504)
    parser.add_argument("--prometheus-port", type=int, default=18505)
    args = parser.parse_args()
    fixture(args.landing)
    check(args, load_test_env(args.env_file))
    if args.gui_env_file:
        check_gui(args)


if __name__ == "__main__":
    main()

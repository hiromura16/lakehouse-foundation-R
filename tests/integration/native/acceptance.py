"""Verify native Compose with synthetic sensor data and a provisioned UI."""

import argparse
import base64
import hashlib
import json
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def fixture(root):
    """Publish five synthetic sensor lines and an adjacent integrity marker."""
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
    """Prepare the extractor's six-table format with one synthetic product."""
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
    """Use a bounded request and return only decoded JSON."""
    headers = {"Content-Type": "application/json"}
    if authorization:
        headers["Authorization"] = authorization
    payload = None if body is None else json.dumps(body).encode()
    with urlopen(
        Request(url, headers=headers, data=payload), timeout=300
    ) as resp:
        return json.load(resp)


def load_test_env(path):
    """Read only generated test settings without shell evaluation."""
    return dict(
        line.split("=", 1)
        for line in path.read_text().splitlines()
        if line and not line.startswith("#")
    )


def check(args, env):
    """Check token roles, replay, pinned Gold, and Grafana provisioning."""
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
            raise AssertionError("Gold accepted an invalid credential.")
    try:
        fetch(
            http + "/run", authorization=reader, body={"action": "medallion"}
        )
    except HTTPError as error:
        assert error.code == 401
    else:
        raise AssertionError("Writer accepted the reader credential.")

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
        raise AssertionError("Grafana did not provision the dashboard.")
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


def main():
    """Run against an isolated Compose project with fresh credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--landing", required=True, type=Path)
    parser.add_argument("--api-port", required=True, type=int)
    parser.add_argument("--grafana-port", required=True, type=int)
    args = parser.parse_args()
    fixture(args.landing)
    check(args, load_test_env(args.env_file))


if __name__ == "__main__":
    main()

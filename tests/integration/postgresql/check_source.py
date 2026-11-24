"""合成PostgreSQLのTLSと、通常の抽出CLIの全6表を確認する。"""

import argparse
import hashlib
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg import sql

FIXTURE = Path(__file__).resolve().parent
ROOT = FIXTURE.parents[2]
READER = "factory_reader"


def settings(stage, port):
    """試験内のループバック以外への接続設定を受け付けない。"""
    return {
        "host": "localhost",
        "port": port,
        "database": "factory",
        "user": READER,
        "password": secrets.token_hex(24),
        "ca_file": str(stage / "ca.pem"),
        "source_id": str(uuid4()),
    }


def connect(config):
    """Psycopgで試験用証明書を検証し、設定した一時ロールで接続する。"""
    return psycopg.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
        sslmode="verify-full",
        sslrootcert=config["ca_file"],
        sslcertmode="disable",
        gssencmode="disable",
        connect_timeout=5,
    )


def seed(stage, config):
    """固定6表とSELECT専用ロールを、使い捨てDBにだけ作る。"""
    admin = config | {
        "user": "postgres",
        "password": (stage / "source-input" / "admin-password")
        .read_text()
        .strip(),
    }
    with connect(admin) as connection:
        connection.execute((FIXTURE / "source.sql").read_text())
        connection.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(READER), sql.Literal(config["password"])
            )
        )
        connection.execute(
            """
            REVOKE ALL ON DATABASE factory FROM PUBLIC;
            GRANT CONNECT ON DATABASE factory TO factory_reader;
            GRANT USAGE ON SCHEMA factory_erp, factory_mes TO factory_reader;
            GRANT SELECT ON ALL TABLES IN SCHEMA factory_erp, factory_mes
                TO factory_reader;
            """
        )

    with connect(config) as connection:
        tls = connection.execute(
            "SELECT ssl FROM pg_catalog.pg_stat_ssl WHERE pid=pg_backend_pid()"
        ).fetchone()
    assert tls == (True,), "合成接続でTLSを確認できませんでした。"


def extract(stage, label, config, landing):
    """標準CLIを使い、ソース秘密が応答に含まれないことを確認する。"""
    private = stage / f"{label}.json"
    private.write_text(json.dumps(config))
    private.chmod(0o600)
    answer = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "extract-postgresql.py"),
            "--settings",
            str(private),
            "--mapping",
            str(stage / "tables.json"),
            "--landing",
            str(landing),
        ],
        capture_output=True,
        check=False,
        timeout=35,
        env={"PATH": os.defpath, "HOME": str(stage)},
    )
    if config["password"].encode() in answer.stdout + answer.stderr:
        raise AssertionError("抽出がソース秘密を出力しました。")
    return answer


def check_rejection(stage, label, config):
    """誤CA・ホスト名では観測も完了印も作られないことを確認する。"""
    landing = stage / f"rejected-{label}"
    answer = extract(stage, label, config, landing)
    assert answer.returncode == 1 and not answer.stdout, label
    assert "TLS接続".encode() in answer.stderr, label
    assert not list(landing.glob("sql-batches/*")), label


def assert_batch(landing, job):
    """原本・完了印・6表の型と明示した列名を照合する。"""
    assert set(job) == {"action", "batch_id", "observed_at"}
    assert job["action"] == "sql-load"
    batch = landing / "sql-batches" / (job["batch_id"] + ".json")
    payload = batch.read_bytes()
    assert hashlib.sha256(payload).hexdigest() == job["batch_id"]
    assert (
        batch.with_suffix(".json.ready").read_text() == job["batch_id"] + "\n"
    )
    source = json.loads(payload)
    assert source["source_system"] == "postgresql-direct"
    assert source["mode"] == "snapshot"
    tables = source["tables"]
    assert set(tables) == {
        "erp_products",
        "erp_work_orders",
        "erp_sales_orders",
        "erp_shipments",
        "mes_events",
        "mes_downtime",
    }
    assert all(len(table["rows"]) == 1 for table in tables.values())
    product = tables["erp_products"]["rows"][0]
    assert product["values"]["product_name"] == "軸受"
    assert product["values"]["standard_cycle_seconds"] == "12.3400"
    assert product["source_updated_at"] == "2026-01-05T03:00:00.1234560Z"
    sale = tables["erp_sales_orders"]["rows"][0]["values"]
    assert (
        sale["order_date"] == "2026-01-05" and sale["unit_price"] == "2.3000"
    )
    shipped = tables["erp_shipments"]["rows"][0]["values"]
    assert shipped["shipped_at"] == "2026-01-05T23:00:00.234567Z"
    assert tables["mes_downtime"]["rows"][0]["values"]["reason_code"] is None


def main():
    """TLSの拒否ケースと、実際に公開する一つの観測を作る。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    stage = args.stage.resolve()
    assert 1 <= args.port <= 65535
    config = settings(stage, args.port)
    seed(stage, config)
    mapping = json.loads(
        (
            ROOT / "infra" / "source" / "postgresql-tables.example.json"
        ).read_text()
    )
    for entry in mapping["tables"]:
        if entry["target_table"] == "erp_sales_orders":
            entry["columns"]["order_date"] = "placed_day"
    (stage / "tables.json").write_text(json.dumps(mapping))
    check_rejection(
        stage, "wrong-ca", config | {"ca_file": str(stage / "wrong-ca.pem")}
    )
    check_rejection(stage, "wrong-host", config | {"host": "127.0.0.1"})
    landing = stage / "landing"
    answer = extract(stage, "source", config, landing)
    assert answer.returncode == 0, (
        "正しいCAと接続名での6表抽出に失敗しました。"
    )
    job = json.loads(answer.stdout)
    assert_batch(landing, job)
    (landing / ".postgresql-ci-job.json").write_text(json.dumps(job))
    print("合成PostgreSQLのTLS、二種類の拒否、6表の型と原本を確認しました。")


if __name__ == "__main__":
    main()

"""合成SQLiteでDB原本・型・完了印・拒否時の無出力を検査する。"""

import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from shutil import copyfile

ROOT = Path(__file__).resolve().parents[3]
MAPPING = ROOT / "infra/source/sqlite-tables.example.json"
INSTALLATION_ID = "8d419179-93b1-4abe-9d78-5214b1953a0e"
REVISION = "2026-01-05T03:00:00.1234567Z"
ROWS = {
    "products": ("P01", "軸受", "標準", "EA", "12.3400"),
    "work_orders": ("WO1", "F1", "P01", "2026-01-05", 20),
    "sales_orders": (
        "SO1",
        "F1",
        "P01",
        "2026-01-05",
        "2026-01-07",
        10,
        "2.3000",
    ),
    "shipments": ("SH1", "SO1", "2026-01-05T23:00:00.234567Z", 3),
    "events": ("E1", "WO1", "2026-01-05T08:00:00.876543Z", 8, 1),
    "downtime": (
        "DT1",
        "WO1",
        "2026-01-05T08:00:00.012345Z",
        "2026-01-05T08:01:30.012345Z",
        None,
    ),
}


def fixture(path):
    """論理名と実名が異なる6つのSQLite通常表を作る。"""
    mapping = json.loads(MAPPING.read_text(encoding="utf-8"))["tables"]
    with closing(sqlite3.connect(path)) as db:
        for entry in mapping:
            name = entry["source_table"]
            values = (*ROWS[name], REVISION)
            declared = [
                f'"{column}" ' + ("INTEGER" if type(value) is int else "TEXT")
                for column, value in zip(
                    entry["columns"].values(), values, strict=True
                )
            ]
            db.execute(f'CREATE TABLE "{name}" (' + ", ".join(declared) + ")")
            bind = ", ".join("?" for _ in values)
            db.execute(f'INSERT INTO "{name}" VALUES ({bind})', values)
        db.commit()


def extract(path, landing, *, max_rows=None, source_id=INSTALLATION_ID):
    """operatorが実行するCLIから取得し、秘密を含まない操作要求を返す。"""
    command = [
        sys.executable,
        str(ROOT / "scripts/extract-sqlite.py"),
        "--db",
        str(path),
        "--mapping",
        str(MAPPING),
        "--source-name",
        "local_machining",
        "--source-id",
        source_id,
        "--landing",
        str(landing),
    ]
    if max_rows is not None:
        command.extend(("--max-rows", str(max_rows)))
    return subprocess.run(
        command, capture_output=True, timeout=35, check=False
    )


def accepted(path, landing):
    """入力もののSHA-256と、作成したバッチ・完了印を独立に比較する。"""
    before = sha256(path.read_bytes()).hexdigest()
    child = extract(path, landing)
    assert child.returncode == 0, child.stderr.decode()
    job = json.loads(child.stdout)
    batch_id = job["batch_id"]
    file = landing / "sql-batches" / f"{batch_id}.json"
    payload = file.read_bytes()
    assert sha256(payload).hexdigest() == batch_id
    assert Path(str(file) + ".ready").read_text() == batch_id + "\n"
    assert sha256(path.read_bytes()).hexdigest() == before
    assert str(path).encode() not in payload
    document = json.loads(payload)
    assert document["source_system"] == "sqlite-snapshot"
    assert document["source_database"] == "local_machining"
    assert document["mode"] == "snapshot"
    assert len(document["tables"]) == 6
    return job, document


def rejected(path, landing, **options):
    """無効な入力でも着地ディレクトリと完了印を作らない。"""
    child = extract(path, landing, **options)
    assert child.returncode == 1
    assert "SQLiteの抽出に失敗しました" in child.stderr.decode()
    assert str(path).encode() not in child.stderr + child.stdout
    assert not landing.exists()


def change_copy(path, target, statement):
    """元のバックアップを保持し、否定ケースだけを別DBで作る。"""
    copyfile(path, target)
    with closing(sqlite3.connect(target)) as db:
        db.executescript(statement)
    return target


def main(stage, landing):
    """内容A→B→Aと拒否条件を、合成DBと実行ファイルで検証する。"""
    # URI上で特別な文字を含むファイル名でも、DBとして一意に開ける。
    path = stage / "factory #1 ?.sqlite3"
    fixture(path)
    a, document = accepted(path, landing)
    tables = document["tables"]
    values = tables["erp_products"]["rows"][0]["values"]
    assert values["product_name"] == "軸受"
    assert values["standard_cycle_seconds"] == "12.3400"
    assert tables["mes_downtime"]["rows"][0]["values"]["reason_code"] is None
    sales = tables["erp_sales_orders"]["rows"][0]
    assert sales["values"]["unit_price"] == "2.3000"
    assert sales["values"]["order_date"] == "2026-01-05"
    assert sales["source_updated_at"] == REVISION
    b_path = change_copy(
        path,
        stage / "revised.sqlite3",
        "UPDATE events SET good_qty=9; DELETE FROM shipments;",
    )
    b, changed = accepted(b_path, landing)
    assert not changed["tables"]["erp_shipments"]["rows"]
    restored, _ = accepted(path, landing)
    assert a["batch_id"] == restored["batch_id"] != b["batch_id"]
    assert a["observed_at"] < b["observed_at"] < restored["observed_at"]

    rejected(path, stage / "limited", max_rows=5)
    rejected(
        path, stage / "uuid", source_id="00000000-0000-0000-0000-000000000000"
    )
    for label, statement in (
        (
            "strict-clock",
            "UPDATE sales_orders SET updated_at='2026-01-05 01:00';",
        ),
        ("blob", "UPDATE sales_orders SET unit_price=CAST(2.3 AS BLOB);"),
        (
            "real",
            "ALTER TABLE sales_orders RENAME COLUMN unit_price TO former;"
            "ALTER TABLE sales_orders ADD COLUMN unit_price REAL;"
            "UPDATE sales_orders SET unit_price = 2.3;",
        ),
        ("duplicate", "INSERT INTO products SELECT * FROM products;"),
        (
            "view",
            "ALTER TABLE products RENAME TO hidden;"
            "CREATE VIEW products AS SELECT * FROM hidden;",
        ),
    ):
        wrong = change_copy(path, stage / f"{label}.db", statement)
        rejected(wrong, stage / f"reject-{label}")
    wal = change_copy(path, stage / "open-wal.db", "PRAGMA journal_mode=WAL;")
    rejected(wal, stage / "reject-wal")
    (landing / ".sqlite-ci-jobs.json").write_text(json.dumps([a, b, restored]))
    print(
        "SQLite六表、原本無変更、内容と観測の分離、拒否8経路を確認しました。"
    )


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))

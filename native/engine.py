"""Foundationの業務SQLをDuckDBで実行し、Icebergのスナップショットを公開する。

表の永続状態はカタログとオブジェクトストレージに置く。DuckDBは処理ごとに
メモリ上で起動し、ネットワーク拡張、ローカル入力ファイル、利用者指定のSQLは扱わない。
表名の解決とコミット済みSQLの移植処理はバッチ実行側に集約する。
"""

import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import duckdb
import pyarrow as pa

from native import sensor
from scripts.sql_batch_worker import with_sql_provenance

ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = ROOT / "medallion"
MAX_ROWS = 100_000
MAX_TABLE_BYTES = 64 * 1024 * 1024
LAYERS = ("bronze", "silver", "quarantine", "gold")
GOLD = {
    "factory_daily_production": ("factory_id", "production_date"),
    "sales_fulfillment": ("sales_order_id",),
    "sensor_latest": (
        "factory_id",
        "line_id",
        "equipment_id",
        "sensor_id",
        "measurement",
        "unit",
    ),
    "sensor_minute": (
        "factory_id",
        "line_id",
        "equipment_id",
        "sensor_id",
        "measurement",
        "unit",
        "minute",
    ),
}
RELEASE_SCHEMA = pa.schema(
    [("released_at", pa.string()), ("manifest_json", pa.string())]
)
CTAS = re.compile(
    r"^CREATE OR REPLACE TABLE ((?:silver|quarantine|gold)\.\w+) AS\s+",
    re.IGNORECASE,
)


def connect():
    """明示的に登録するArrowを入力とする新しいUTC接続を作る。"""
    db = duckdb.connect(
        ":memory:",
        config={
            "threads": "2",
            "memory_limit": "640MB",
            "enable_external_access": "false",
            "autoinstall_known_extensions": "false",
            "autoload_known_extensions": "false",
        },
    )
    db.execute("SET TimeZone = 'UTC'")
    return db


def sql_for_duckdb(sql):
    """業務規則を保ったまま、検証済みのFoundation SQLをDuckDB向けに調整する。

    固定済みの業務SQLを契約とする。ここで調整するのは業務ルールと独立した
    受入検査SQLに限定する。新しい構文へ対応する際は、同期元ハッシュと期待値テストも更新する。
    """
    sql = re.sub(r"^\s*--.*$", "", sql, flags=re.MULTILINE)
    sql = re.sub(
        r"\biceberg\.(?=bronze\.|silver\.|quarantine\.|gold\.)", "", sql
    )
    sql = re.sub(
        r"TRY\(FROM_ISO8601_TIMESTAMP\((\w+)\)\)",
        r"TRY_CAST(\1 AS TIMESTAMPTZ)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"JSON_FORMAT\(CAST\(MAP\((.*?)\) AS JSON\)\)",
        r"CAST(TO_JSON(MAP(\1)) AS VARCHAR)",
        sql,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # DuckDBは小数文字列を丸めてBIGINTにする。整数形式を先に検証し、
    # 型変換エラーを隔離する業務ルールに合わせる。
    sql = re.sub(
        r"TRY_CAST\((\w+) AS BIGINT\)",
        r"CASE WHEN regexp_full_match(trim(\1), '[+-]?[0-9]+') "
        r"THEN TRY_CAST(\1 AS BIGINT) END",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"date_diff\('second', (\w+\.\w+), (\w+\.\w+)\)",
        r"CAST(trunc(epoch(\2 - \1)) AS BIGINT)",
        sql,
        flags=re.IGNORECASE,
    )
    return sql.replace("json_extract_scalar(", "json_extract_string(").strip()


def bounded_scan(table, snapshot_id):
    """固定したスナップショットを読み、途中でもサイズ上限の超過を検出する。"""
    if snapshot_id is None:
        return table.schema().as_arrow().empty_table()
    batches = []
    row_count = byte_count = 0
    reader = table.scan(
        snapshot_id=snapshot_id, limit=MAX_ROWS + 1
    ).to_arrow_batch_reader()
    for batch in reader:
        row_count += batch.num_rows
        byte_count += batch.nbytes
        if row_count > MAX_ROWS or byte_count > MAX_TABLE_BYTES:
            raise ValueError("NATIVE_BATCH_LIMIT")
        batches.append(batch)
    return pa.Table.from_batches(batches, schema=reader.schema)


def current_id(table):
    """mainのスナップショットIDを返す。未作成または空の初期表ではNoneを返す。"""
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot else None


def write_table(catalog, identifier, arrow):
    """スキーマを保ってIceberg V2の1表を上書きし、スナップショットIDを返す。"""
    if not catalog.table_exists(identifier):
        catalog.create_table(
            identifier, schema=arrow.schema, properties={"format-version": "2"}
        )
    table = catalog.load_table(identifier)
    expected = table.schema().as_arrow()
    if not arrow.schema.equals(expected, check_metadata=False):
        # DuckDBはArrowのタイムゾーン別名や文字列型を正規化する。
        # 互換性のない変換は、カタログへの確定前にArrowが拒否する。
        arrow = arrow.cast(expected)
    if arrow.num_rows or current_id(table) is not None:
        table.overwrite(arrow)
    return current_id(table)


def release_table(core, prefix):
    """バッチまたはCDCの名前空間から公開マニフェスト表を特定する。"""
    namespace = core.schema(prefix, "gold")
    return f"{namespace}._release"


def latest_release(core, catalog, prefix="", release_id=None):
    """指定世代または最新の正常公開のマニフェストを、自身のIDとともに取得する。"""
    identifier = release_table(core, prefix)
    if not catalog.table_exists(identifier):
        return None
    table = catalog.load_table(identifier)
    snapshot_id = current_id(table) if release_id is None else release_id
    if snapshot_id is None or table.snapshot_by_id(snapshot_id) is None:
        return None
    (row,) = table.scan(snapshot_id=snapshot_id).to_arrow().to_pylist()
    return {
        "release_snapshot_id": str(snapshot_id),
        "released_at": row["released_at"],
        **json.loads(row["manifest_json"]),
    }


def input_tables(core, catalog, prefix):
    """ストレージから行を読み始める前に、6つのBronze表の読み取り版を確定する。"""
    identifiers = {
        source["table"]: catalog.load_table(
            f"{core.schema(prefix, 'bronze')}.{source['table']}"
        )
        for source in core.CONTRACT
    }
    return {
        name: (table, current_id(table)) for name, table in identifiers.items()
    }


def add_arrow(db, relation, target, arrow):
    """Arrowの入力を表に取り込み、Python側の参照を解放する。"""
    db.register(relation, arrow)
    try:
        db.execute(f"CREATE TABLE {target} AS SELECT * FROM {relation}")
    finally:
        db.unregister(relation)


def medallion(core, prefix=""):
    """派生表を保存し、全表のスナップショットIDを最後に公開する。

    Icebergは表ごとに確定する。全表の保存が完了するまで、利用者は前回の
    マニフェストを参照する。途中で失敗したら復旧後に再実行する。
    同じ接頭辞へ書き込む実行元は、プロセスをまたいで1つに限定する。
    """
    core.ensure_tables(prefix)
    catalog = core.catalog()
    tables = input_tables(core, catalog, prefix)
    inputs = {name: snap for name, (_, snap) in tables.items()}
    sensor_entries = sensor.ledger(core, catalog, prefix)
    sensor_inputs = {row["path"]: row["sha256"] for row in sensor_entries}
    recipe_hash = sha256(Path(__file__).read_bytes())
    recipe_hash.update(Path(sensor.__file__).read_bytes())
    statements = []
    for path in sorted(SQL_ROOT.glob("*.sql")):
        source = path.read_bytes()
        recipe_hash.update(source)
        query = source.decode()
        if prefix in {"sql_", "sqlcdc_"}:
            query = with_sql_provenance(query)
        statements.extend(
            part.strip()
            for part in sql_for_duckdb(query).split(";")
            if part.strip()
        )
    recipe = recipe_hash.hexdigest()
    previous = latest_release(core, catalog, prefix)
    if (
        previous
        and previous["inputs"] == inputs
        and previous.get("sensor_inputs", {}) == sensor_inputs
        and previous["recipe"] == recipe
    ):
        return {"sql_statements": 0, "unchanged": True}
    outputs = {}
    with connect() as db:
        for layer in LAYERS:
            db.execute(f"CREATE SCHEMA {layer}")
        for name, (table, snap) in tables.items():
            add_arrow(
                db, "input_arrow", f"bronze.{name}", bounded_scan(table, snap)
            )
        for statement in statements:
            match = CTAS.match(statement)
            if not match:
                raise ValueError("NATIVE_SQL_CONTRACT")
            local = match[1]
            db.execute(statement)
            arrow = db.sql(f"SELECT * FROM {local}").to_arrow_table()
            # TrinoのSUM(BIGINT)はBIGINT、DuckDBの整数SUMはHUGEINTを返す。
            # 集計した整数を明示的に縮め、小数の列は精度を保つ。
            columns = [
                pa.field(field.name, pa.int64())
                if pa.types.is_decimal(field.type) and field.type.scale == 0
                else field
                for field in arrow.schema
            ]
            arrow = arrow.cast(pa.schema(columns))
            layer, name = local.split(".")
            identifier = f"{core.schema(prefix, layer)}.{name}"
            outputs[local] = write_table(catalog, identifier, arrow)
        if not prefix:
            for local, arrow in sensor.tables(
                core, db, sensor_entries
            ).items():
                outputs[local] = write_table(catalog, local, arrow)
    release = pa.Table.from_pylist(
        [
            {
                "released_at": datetime.now(timezone.utc).isoformat(),
                "manifest_json": json.dumps(
                    {
                        "inputs": inputs,
                        "outputs": outputs,
                        "recipe": recipe,
                        "sensor_inputs": sensor_inputs,
                    },
                    sort_keys=True,
                ),
            }
        ],
        schema=RELEASE_SCHEMA,
    )
    write_table(catalog, release_table(core, prefix), release)
    return {"sql_statements": len(statements), "unchanged": False}


def verify_query(core, sql, prefix):
    """公開済みのSilver・Quarantine・Goldと、固定したBronzeをDuckDBで検査する。"""
    catalog = core.catalog()
    release = latest_release(core, catalog, prefix)
    with connect() as db:
        for layer in LAYERS:
            db.execute(f"CREATE SCHEMA {layer}")
        for name, snapshot_id in release["inputs"].items():
            table = catalog.load_table(
                f"{core.schema(prefix, 'bronze')}.{name}"
            )
            add_arrow(
                db,
                "verify_arrow",
                f"bronze.{name}",
                bounded_scan(table, snapshot_id),
            )
        for name, snapshot_id in release["outputs"].items():
            layer, table_name = name.split(".")
            table = catalog.load_table(
                f"{core.schema(prefix, layer)}.{table_name}"
            )
            add_arrow(
                db, "verify_arrow", name, bounded_scan(table, snapshot_id)
            )
        rows = db.execute(sql_for_duckdb(sql)).fetchall()

    # APIの型契約に合わせ、日付と小数をJSONに変換する。
    def oracle_value(value):
        """独立した期待値JSONで使われるスカラー値の表現へそろえる。"""
        if isinstance(value, date):
            return value.isoformat()
        return float(value) if isinstance(value, Decimal) else value

    return [[oracle_value(value) for value in row] for row in rows]


def gold_rows(core, name, limit, prefix="", release_id=None):
    """由来情報付きのGoldを、一定の順序で指定行数の上限まで返す。"""
    if name not in GOLD or not 1 <= limit <= 200:
        raise ValueError("INVALID_GOLD_REQUEST")
    catalog = core.catalog()
    release = latest_release(core, catalog, prefix, release_id)
    local = f"gold.{name}"
    if not release or local not in release["outputs"]:
        return None
    snapshot_id = release["outputs"][local]
    identifier = f"{core.schema(prefix, 'gold')}.{name}"
    table = catalog.load_table(identifier)
    arrow = bounded_scan(table, snapshot_id)
    with connect() as db:
        db.register("gold_data", arrow)
        sort = ", ".join(GOLD[name])
        records = (
            db.sql(
                f"SELECT * FROM gold_data ORDER BY {sort} LIMIT ?",
                params=[limit],
            )
            .to_arrow_table()
            .to_pylist()
        )
    return {
        "table": identifier,
        "snapshot_id": str(snapshot_id) if snapshot_id is not None else None,
        "release_snapshot_id": release["release_snapshot_id"],
        "released_at": release["released_at"],
        "recipe": release["recipe"],
        "rows": records,
        "total_rows": arrow.num_rows,
        "truncated": arrow.num_rows > limit,
    }

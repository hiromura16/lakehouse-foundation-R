"""業務20表をローカルIceberg V2と2つの実行エンジンで照合する。"""

import importlib.util
import json
import re
import tempfile
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
from datafusion import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
    SQLOptions,
    udf,
)
from pyiceberg.catalog import load_catalog

from native import catalog_schema as schema
from native import engine
from scripts.landing_contract import source_rows

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "synthetic", ROOT / "scripts/generate-demo.py"
)
DEMO = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DEMO)
READ_ONLY = (
    SQLOptions()
    .with_allow_statements(False)
    .with_allow_ddl(False)
    .with_allow_dml(False)
)
REFERENCE = re.compile(
    r"NOT EXISTS \(\s*SELECT 1\s*FROM (silver\.\w+) AS (\w+)"
    r"\s*WHERE \2\.(\w+) = latest\.\3\s*\)",
    re.I,
)
INTEGER_SUM = re.compile(
    r"\bsum\(((?:[a-z]\.)?(?:planned_qty|good_qty|reject_qty|ordered_qty|"
    r"shipped_qty|total_qty|open_qty|late_delivered_qty|"
    r"late_shipment_count))\)"
    r"|\bsum\((CASE\b.*?\bEND)\)",
    re.I | re.S,
)


def raw_json(keys, values):
    """Mapを生成する既存SQLと同じ順序で、元の文字列をJSONに保存する。"""
    return pa.array(
        [
            json.dumps(
                dict(zip(k, v, strict=True)),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for k, v in zip(keys.to_pylist(), values.to_pylist(), strict=True)
        ],
        type=pa.string(),
    )


def json_extract(records, paths):
    """受入の固定スカラーJSONパスだけを読み取る。"""
    return pa.array(
        [
            json.loads(record).get(path.removeprefix("$."))
            if record is not None
            else None
            for record, path in zip(
                records.to_pylist(), paths.to_pylist(), strict=True
            )
        ],
        type=pa.string(),
    )


def sql_datafusion(sql):
    """固定の業務SQLで差が実測された演算だけをDataFusion向けに置換する。"""
    sql = re.sub(r"^\s*--.*$", "", sql, flags=re.M)
    sql = re.sub(
        r"\biceberg\.(?=bronze\.|silver\.|quarantine\.|gold\.)", "", sql
    )
    sql = re.sub(
        r"TRY\(FROM_ISO8601_TIMESTAMP\((\w+)\)\)",
        r"TRY_CAST(\1 AS TIMESTAMP(6) WITH TIME ZONE)",
        sql,
        flags=re.I,
    )
    sql = re.sub(
        r"JSON_FORMAT\(CAST\(MAP\((.*?)\) AS JSON\)\)",
        r"foundation_raw_json(\1)",
        sql,
        flags=re.S | re.I,
    )
    sql = re.sub(
        r"TRY_CAST\((\w+) AS BIGINT\)",
        r"CASE WHEN regexp_like(trim(\1), '^[+-]?[0-9]+$') "
        r"THEN TRY_CAST(trim(\1) AS BIGINT) END",
        sql,
        flags=re.I,
    )
    sql = re.sub(
        r"date_diff\('second', (\w+\.\w+), (\w+\.\w+)\)",
        r"CAST(trunc(date_part('epoch', \2 - \1)) AS BIGINT)",
        sql,
        flags=re.I,
    )
    sql = INTEGER_SUM.sub(
        lambda m: f"sum(CAST({m[1] or m[2]} AS DECIMAL(38, 0)))",
        sql,
    )
    # 対象は版管理済みSQL内の条件。どちらも括弧の内側はCAST一つまで。
    sql = re.sub(
        r"count_if\(((?:[^()]|\([^()]*\))*)\)",
        r"count(*) FILTER (WHERE \1)",
        sql,
        flags=re.I,
    )
    sql = re.sub(
        r"(_source_hash|_ingested_at|raw_record_json) DESC(?! NULLS)",
        r"\1 DESC NULLS LAST",
        sql,
        flags=re.I,
    )
    return sql.replace(
        "json_extract_scalar(", "foundation_json_extract("
    ).strip()


def rewrite_reference(query):
    """一意化済みSilver参照を結合し、CASE内の相関EXISTSと同じ判定を行う。"""
    if (match := REFERENCE.search(query)) is None:
        return query
    table, _, key = match.groups()
    query = REFERENCE.sub("foundation_parent_key IS NULL", query)
    return query.replace(
        "    FROM latest",
        f"""    FROM latest
    LEFT JOIN (
        SELECT DISTINCT {key} AS foundation_parent_key FROM {table}
    ) AS foundation_parent
      ON foundation_parent.foundation_parent_key = latest.{key}""",
    )


def connect_datafusion():
    """Rustの実行セッションへ、試験用の確定済みArrow表だけを渡す。"""
    ctx = SessionContext(
        SessionConfig()
        .with_target_partitions(2)
        .set("datafusion.execution.time_zone", "UTC"),
        RuntimeEnvBuilder()
        .with_disk_manager_disabled()
        .with_greedy_memory_pool(640 * 1024 * 1024),
    )
    ctx.register_udf(
        udf(
            raw_json,
            [pa.list_(pa.string()), pa.list_(pa.string())],
            pa.string(),
            "immutable",
            "foundation_raw_json",
        )
    )
    ctx.register_udf(
        udf(
            json_extract,
            [pa.string(), pa.string()],
            pa.string(),
            "immutable",
            "foundation_json_extract",
        )
    )
    return ctx


def normal_type(type_):
    """現行Iceberg出力契約に対応するArrow型へそろえる。"""
    if (
        pa.types.is_string(type_)
        or pa.types.is_large_string(type_)
        or pa.types.is_string_view(type_)
    ):
        return pa.string()
    if pa.types.is_timestamp(type_):
        return pa.timestamp("us", tz="UTC")
    if pa.types.is_decimal(type_) and type_.scale == 0:
        return pa.int64()
    return type_


def output_schema(schema_):
    """論理計画の型を、安全に縮小できるIcebergの出力型へ合わせる。"""
    return pa.schema([(f.name, normal_type(f.type)) for f in schema_])


def local_catalog(path, name):
    """各エンジン専用のローカルSQLiteとIceberg warehouseを作る。"""
    target = path / name
    target.mkdir()
    warehouse = target / "warehouse"
    warehouse.mkdir()
    return load_catalog(
        name,
        type="sql",
        uri="sqlite:///" + str(target / "catalog.db"),
        warehouse=warehouse.as_uri(),
    )


def fixture(path, scenario):
    """ファイルの列検査を通した合成原本を、固定Iceberg V2世代にする。"""
    catalog = local_catalog(path, "source")
    schema.ensure_tables(catalog, schema.CONTRACT)
    data = DEMO.build_data(seed=42, scenario="edge")
    if scenario == "empty":
        data = {name: [] for name in data}
    elif scenario == "extended":
        extended_rows(data)
    elif scenario == "legacy":
        data = legacy_rows(data)
    sources = DEMO.encoded_sources(data)
    for item in schema.CONTRACT:
        relative = item["path"]
        payload = sources[relative]
        values = source_rows(Path(relative).suffix, payload, item["columns"])
        table = catalog.load_table("bronze." + item["table"])
        rows = [
            {
                **{
                    col: None if value is None else str(value)
                    for col, value in row.items()
                },
                "_source_file": relative,
                "_source_hash": sha256(payload).hexdigest(),
                "_ingested_at": "2026-01-12T12:00:00+00:00",
            }
            for row in values
        ]
        if rows:
            table.append(
                pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
            )
    return engine.input_tables(schema, catalog, "")


def extended_rows(data):
    """UTC日付境界・小数秒・Unicode・不正整数を既存ケースに追加する。"""
    event, downtime = data["mes_events"][0], data["mes_downtime"][0]
    data["mes_events"].extend(
        [
            {
                **event,
                "event_id": "PARITY-OFFSET",
                "event_time": "2026-01-08T00:00:00.123456+09:00",
                "good_qty": "+7",
                "reject_qty": "3",
            },
            {
                **event,
                "event_id": "PARITY-BAD-INT",
                "good_qty": '2.3 漢字\\n"\\t',
            },
        ]
    )
    data["mes_downtime"].append(
        {
            **downtime,
            "downtime_id": "PARITY-INTERVAL",
            "start_time": "2026-01-08T00:00:00.125456+09:00",
            "end_time": "2026-01-08T00:00:01.124456+09:00",
        }
    )


def legacy_rows(data):
    """同じ業務キーへ、変換の採否が業務結果を変える値を配置する。"""
    product = {
        **data["erp_products"][0],
        "product_id": "P-LEGACY",
        "product_name": "旧版",
        "updated_at": "2024-10-30T00:00:00Z",
    }
    return {
        "erp_products": [
            product,
            {
                **product,
                "product_name": "新版",
                "updated_at": "2024/11/01T00:00:00+09:00",
            },
        ],
        "erp_work_orders": [
            {
                **data["erp_work_orders"][0],
                "work_order_id": "WO-CIVIL",
                "factory_id": "F-LEGACY",
                "product_id": "P-LEGACY",
                "planned_date": "2024-11-01T00:00:00+09:00",
                "planned_qty": "17",
            },
        ],
        "erp_sales_orders": [
            {
                **data["erp_sales_orders"][0],
                "sales_order_id": "SO-EXPONENT",
                "factory_id": "F-LEGACY",
                "product_id": "P-LEGACY",
                "order_date": "2024-10-31",
                "promised_date": "2024-11-01",
                "ordered_qty": "4",
                "unit_price": "1e6",
            },
        ],
        "erp_shipments": [
            {
                **data["erp_shipments"][0],
                "sales_order_id": "SO-EXPONENT",
                "shipped_at": "2024-11-01T01:00:00Z",
                "shipped_qty": "3",
            },
        ],
        "mes_events": [
            {
                **data["mes_events"][0],
                "work_order_id": "WO-CIVIL",
                "event_time": "2024/11/01T00:00:00.1234567+09:00",
                "good_qty": "11",
                "reject_qty": "2",
            },
        ],
        "mes_downtime": [
            {
                **data["mes_downtime"][0],
                "work_order_id": "WO-CIVIL",
                "start_time": "2024-10-31T15:00:00.100000Z",
                "end_time": "2024-10-31T15:01:30.899999Z",
            },
        ],
    }


def read_result(ctx, query):
    """公式SQL APIで計画し、ストリームのArrow上限を既存実装で調べる。"""
    frame = ctx.sql(query, options=READ_ONLY)
    stream = (batch.to_pyarrow() for batch in frame.execute_stream())
    expected = output_schema(frame.schema())
    batches = engine.ScanProgress().batches(stream)
    # CASEの実ストリームがStringViewを返しても、論理出力と同じ型で結合する。
    return pa.Table.from_batches(
        (batch.cast(expected) for batch in batches), schema=expected
    )


def statements(translator):
    """同一の起点SQLから表名と実行対象を得る。"""
    for filename in sorted((ROOT / "medallion").glob("*.sql")):
        for sql in translator(filename.read_text()).split(";"):
            if sql.strip():
                query = sql.strip()
                yield (
                    rewrite_reference(query)
                    if translator is sql_datafusion
                    else query
                )


def read_outputs(catalog, refs):
    """書込みの直後のIceberg世代を読戻し、投影済みの値を照合対象にする。"""
    return {
        name: engine.bounded_scan(catalog.load_table(name), snap)
        for name, snap in refs.items()
    }


def datafusion_outputs(sources, catalog, *, gold_from_silver=False):
    """固定Bronzeの派生14表か、型付きSilverからのGold2表を試験用に確定する。"""
    ctx = connect_datafusion()
    for layer in engine.LAYERS:
        ctx.sql(f"CREATE SCHEMA {layer}").collect()
    input_layer = "silver" if gold_from_silver else "bronze"
    for name, (table, snap) in sources.items():
        arrow = engine.bounded_scan(table, snap)
        ctx.register_table(f"{input_layer}.{name}", ctx.from_arrow(arrow))
    refs = {}
    for statement in statements(sql_datafusion):
        target = engine.CTAS.match(statement)
        if target is None:
            raise ValueError("MEDALLION_CTAS")
        name = target[1]
        if gold_from_silver and not name.startswith("gold."):
            continue
        # CTASをカタログ操作から切り離し、結果だけを登録・確定する。
        body_start = target.end()
        out = read_result(ctx, statement[body_start:])
        ctx.register_table(name, ctx.from_arrow(out))
        refs[name] = engine.write_table(catalog, name, out)
    return refs


def run_checks(sources, outputs, kind):
    """確定済みの入力・出力で独立した業務オラクルを実行する。"""

    def canonical(value):
        """日付とDecimalを既存オラクルのJSON表現に揃える。"""
        if isinstance(value, date):
            return value.isoformat()
        return float(value) if isinstance(value, Decimal) else value

    datafusion = kind == "datafusion"
    ctx = connect_datafusion() if datafusion else engine.connect()
    for layer in engine.LAYERS:
        if datafusion:
            ctx.sql(f"CREATE SCHEMA {layer}").collect()
        else:
            ctx.execute(f"CREATE SCHEMA {layer}")
    tables = {
        f"bronze.{name}": engine.bounded_scan(table, snap)
        for name, (table, snap) in sources.items()
    }
    for name, arrow in (tables | outputs).items():
        if datafusion:
            ctx.register_table(name, ctx.from_arrow(arrow))
        else:
            engine.add_arrow(ctx, "oracle_arrow", name, arrow)
    checks = json.loads((ROOT / "medallion/expected-demo.json").read_text())
    try:
        for check in checks["checks"]:
            query = (
                check.get("sql")
                or (ROOT / "medallion" / check["sql_file"]).read_text()
            )
            if datafusion:
                arrow = read_result(ctx, sql_datafusion(query))
            else:
                arrow = ctx.sql(engine.sql_for_duckdb(query)).to_arrow_table()
            rows = [
                [canonical(col) for col in record.values()]
                for record in arrow.to_pylist()
            ]
            assert rows == check["expected"], (kind, check["name"], rows)
    finally:
        if not datafusion:
            ctx.close()
    return len(checks["checks"])


def assert_same_table(name, left, right):
    """Icebergから読戻した行とArrowスキーマを、順序非依存で照合する。"""
    assert left.schema.equals(right.schema, check_metadata=True), (
        name,
        left.schema,
        right.schema,
    )
    assert sorted(left.to_pylist(), key=repr) == sorted(
        right.to_pylist(), key=repr
    ), name


def check_legacy(duck, datafusion):
    """変換差が採用版、隔離理由、業務日、公開件数を変えることを固定する。"""

    def daily(outputs):
        """計画と実績が集約される営業日と数量を返す。"""
        return sorted(
            (
                row["production_date"].isoformat(),
                row["planned_qty"],
                row["good_qty"],
            )
            for row in outputs["gold.factory_daily_production"].to_pylist()
        )

    left, right = daily(duck), daily(datafusion)
    assert left == [("2024-10-31", 0, 11), ("2024-11-01", 17, 0)], left
    assert right == [("2024-10-31", 17, 0)], right
    assert duck["silver.erp_products"]["product_name"].to_pylist() == ["新版"]
    assert datafusion["silver.erp_products"]["product_name"].to_pylist() == [
        "旧版"
    ]
    assert duck["silver.mes_events"]["event_time"].to_pylist() == [
        datetime.fromisoformat("2024-10-31T15:00:00.123456+00:00")
    ]
    assert datafusion["silver.mes_events"].num_rows == 0
    (order,) = duck["gold.sales_fulfillment"].to_pylist()
    assert order["unit_price"] == Decimal("1000000.00")
    assert order["shipped_qty"] == 3
    assert datafusion["gold.sales_fulfillment"].num_rows == 0
    for table, reason in (
        ("erp_sales_orders", "invalid_type"),
        ("erp_shipments", "unknown_sales_order"),
        ("mes_events", "invalid_timestamp"),
    ):
        name = f"quarantine.{table}"
        assert duck[name].num_rows == 0, name
        assert datafusion[name]["validation_reason"].to_pylist() == [reason], (
            name
        )
    return {"duckdb_daily": left, "direct_datafusion_daily": right}


def check_gold_from_silver(directory, duckdb, duck_refs, duck):
    """標準Silverの版を固定し、Rustで得たGoldを別カタログで照合する。"""
    silver = {
        name.removeprefix("silver."): (duckdb.load_table(name), snap)
        for name, snap in duck_refs.items()
        if name.startswith("silver.")
    }
    assert len(silver) == 6
    assert all(snap is not None for _, snap in silver.values())
    evaluation = local_catalog(directory, "gold-from-silver")
    evaluation.create_namespace("gold")
    refs = datafusion_outputs(silver, evaluation, gold_from_silver=True)
    assert set(refs) == {
        "gold.factory_daily_production",
        "gold.sales_fulfillment",
    }
    for name, result in read_outputs(evaluation, refs).items():
        assert_same_table(name, duck[name], result)
        # Iceberg自身のスキーマ比較には、各列のフィールドIDも含まれる。
        assert (
            duckdb.load_table(name).schema()
            == evaluation.load_table(name).schema()
        ), name
    return sorted(refs)


def run(scenario):
    """ローカルの一時環境で業務表と確定後のスキーマをすべて照合する。"""
    with tempfile.TemporaryDirectory(prefix="fr-ordinary-") as tmp:
        directory = Path(tmp)
        sources = fixture(directory, scenario)
        counts = len(sources)
        duckdb = local_catalog(directory, "duckdb")
        datafusion = local_catalog(directory, "datafusion")
        for catalog in (duckdb, datafusion):
            for layer in engine.LAYERS:
                catalog.create_namespace(layer)
        _, queries = engine.business_queries("")
        duck_refs, _ = engine.business_outputs(
            schema, duckdb, "", sources, queries
        )
        data_refs = datafusion_outputs(sources, datafusion)
        duck = read_outputs(duckdb, duck_refs)
        other = read_outputs(datafusion, data_refs)
        assert duck.keys() == other.keys() and len(duck) == 14
        if scenario == "legacy":
            legacy = check_legacy(duck, other)
            gold = check_gold_from_silver(directory, duckdb, duck_refs, duck)
            print(
                json.dumps(
                    {
                        "scenario": scenario,
                        "inputs": counts,
                        "outputs": len(duck),
                        **legacy,
                        "gold_from_fixed_silver": gold,
                        "status": "pass",
                    }
                )
            )
            return
        for name in duck:
            left = duck[name]
            assert_same_table(name, left, other[name])
            print(
                json.dumps(
                    {
                        "scenario": scenario,
                        "table": name,
                        "rows": left.num_rows,
                    }
                )
            )
        checks = 0
        if scenario == "edge":
            checks = run_checks(sources, duck, "duckdb") + run_checks(
                sources, other, "datafusion"
            )
        print(
            json.dumps(
                {
                    "scenario": scenario,
                    "inputs": counts,
                    "outputs": len(duck),
                    "oracle_checks_across_both": checks,
                    "status": "pass",
                }
            )
        )


def difference_checks():
    """現時点で標準切替を妨げる型解釈差と、整数オーバーフローを再現する。"""
    df = connect_datafusion()
    with engine.connect() as duck:
        golden = duck.execute("""
SELECT
    CAST(TRY_CAST('2024-11-01T00:00:00+09:00' AS DATE) AS VARCHAR) AS day,
    CAST(TRY_CAST('1e6' AS DECIMAL(18, 2)) AS VARCHAR) AS amount,
    TRY_CAST('2024/11/01' AS TIMESTAMPTZ) IS NULL AS rejected
""").fetchone()
    rejected = read_result(
        df,
        """
SELECT
    CAST(TRY_CAST('2024-11-01T00:00:00+09:00' AS DATE) AS VARCHAR) AS day,
    CAST(TRY_CAST('1e6' AS DECIMAL(18, 2)) AS VARCHAR) AS amount,
    TRY_CAST('2024/11/01' AS TIMESTAMP(6) WITH TIME ZONE) IS NULL AS rejected
""",
    ).to_pylist()[0]
    assert golden == ("2024-11-01", "1000000.00", False), golden
    assert rejected == {"day": "2024-10-31", "amount": None, "rejected": True}
    source = "FROM (VALUES (9223372036854775807), (1)) AS quantities(q)"
    wrapped = read_result(df, "SELECT SUM(q) AS total " + source)
    assert wrapped["total"][0].as_py() == -(2**63)
    try:
        read_result(
            df,
            "SELECT SUM(CAST(q AS DECIMAL(38, 0))) AS total " + source,
        )
    except pa.ArrowInvalid:
        pass
    else:
        raise AssertionError("符号付き64ビット範囲を超えた数量が受理された。")
    print(
        json.dumps(
            {
                "legacy_cast_differences": 3,
                "dec128_integer_overflow": "rejected",
            }
        )
    )


if __name__ == "__main__":
    for scenario in ("edge", "extended", "empty", "legacy"):
        run(scenario)
    difference_checks()

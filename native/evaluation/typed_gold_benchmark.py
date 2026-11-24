"""型付きSilverから業務Goldを計算するDuckDBとDataFusionを測る。"""

import argparse
import json
import os
from pathlib import Path
from statistics import median
from tempfile import TemporaryDirectory
from time import perf_counter

from native import engine
from native.evaluation import business_parity


def gold_queries(translate):
    """版管理されたSQLから、セッション内でGoldに使うSELECTを得る。"""
    result = {}
    for statement in business_parity.statements(translate):
        target = engine.CTAS.match(statement)
        if target is None:
            raise ValueError("業務SQLの出力テーブルが未定義です。")
        if target[1].startswith("gold."):
            body_start = target.end()
            result[target[1]] = statement[body_start:]
    return result


def verify(table, expected, *, name):
    """Icebergの文字列表現を正規化して、型と全行を照合する。"""
    expected_schema = business_parity.output_schema(expected.schema)
    if not table.schema.equals(expected_schema, check_metadata=False):
        raise ValueError(f"Goldの集計型が一致しません。{name}")
    if sorted(table.to_pylist(), key=repr) != sorted(
        expected.to_pylist(), key=repr
    ):
        raise ValueError(f"Goldの全行が一致しません。{name}")


def measure(kind, silver, expected):
    """セッション準備と、Gold全行をArrow化した時間を分けて記録する。"""
    rust = kind == "datafusion"
    start = perf_counter()
    context = (
        business_parity.connect_datafusion() if rust else engine.connect()
    )
    try:
        if rust:
            context.sql("CREATE SCHEMA silver").collect()
            for name, table in silver.items():
                context.register_table(name, context.from_arrow(table))
        else:
            context.execute("CREATE SCHEMA silver")
            for name, table in silver.items():
                engine.add_arrow(context, "input_arrow", name, table)
        prepare = 1000 * (perf_counter() - start)
        queries = gold_queries(
            business_parity.sql_datafusion if rust else engine.sql_for_duckdb
        )
        if set(queries) != set(expected):
            raise ValueError("比較するGoldの集合が一致しません。")
        runs = []
        for _ in range(5):
            # 結果照合やファイルI/Oを含めず、計画からArrow化まで測る。
            start = perf_counter()
            results = {}
            for name, query in queries.items():
                if rust:
                    table = business_parity.read_result(context, query)
                else:
                    table = context.sql(query).to_arrow_table()
                    table = table.cast(
                        business_parity.output_schema(table.schema)
                    )
                results[name] = table
            runs.append(1000 * (perf_counter() - start))
            for name, table in results.items():
                verify(table, expected[name], name=name)
        return {
            "prepare_ms": round(prepare, 2),
            "first_ms": round(runs[0], 2),
            "repeat_median_ms": round(median(runs[1:]), 2),
            "all_sql_ms": [round(duration, 2) for duration in runs],
        }
    finally:
        if not rust:
            context.close()


def report(directory, orders, *, reverse=False):
    """標準の型付きSilver 6表を固定し、同じ入力で両方を測定する。"""
    sources = business_parity.fixture(
        directory, "benchmark", orders=orders, days=28
    )
    catalog = business_parity.local_catalog(directory, "standard")
    try:
        for layer in engine.LAYERS:
            catalog.create_namespace(layer)
        _, queries = engine.business_queries("")
        refs, _ = engine.business_outputs(
            business_parity.schema, catalog, "", sources, queries
        )
        values = business_parity.read_outputs(catalog, refs)
        silver = {
            name: table
            for name, table in values.items()
            if name.startswith("silver.")
        }
        expected = {
            name: table
            for name, table in values.items()
            if name.startswith("gold.")
        }
        order = ["duckdb", "datafusion"]
        if reverse:
            order.reverse()
        return {
            "orders": orders,
            "silver_rows": sum(table.num_rows for table in silver.values()),
            "silver_bytes": sum(table.nbytes for table in silver.values()),
            "gold_rows": {
                name: table.num_rows for name, table in expected.items()
            },
            "engines": {
                kind: measure(kind, silver, expected) for kind in order
            },
        }
    finally:
        catalog.engine.dispose()


def main():
    """Linuxの同じ論理CPU二つに固定してCSV生成から照合まで実行する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, required=True)
    parser.add_argument(
        "--reverse", action="store_true", help="Rust側を先に計測"
    )
    args = parser.parse_args()
    cpus = sorted(os.sched_getaffinity(0))[:2]
    if len(cpus) < 2:
        parser.error("比較には利用可能な論理CPUを二つ用意してください。")
    os.sched_setaffinity(0, cpus)
    with TemporaryDirectory(prefix="foundation-r-typed-gold-") as directory:
        result = report(Path(directory), args.orders, reverse=args.reverse)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

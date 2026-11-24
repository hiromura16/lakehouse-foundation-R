"""同じ合成CDCからDataFusionとSailの現在値を照合する。"""

import argparse
import json
import resource
import statistics
import tempfile
import time
from importlib.metadata import version
from pathlib import Path

SQL = Path(__file__).with_name("current_state.sql").read_text(encoding="utf-8")
EXPECTED = [("P1", "accepted", "14.25"), ("P3", "invalid_type", "-")]


def datafusion():
    """DataFusionのセッションとクエリ実行関数を作る。"""
    from datafusion import SessionContext

    context = SessionContext()

    def collect():
        """順序を固定したSQLの全行を共通の比較形式にする。"""
        return [tuple(row.values()) for row in context.sql(SQL).to_pylist()]

    return collect, version("datafusion"), lambda: None


def sail(port):
    """JVMを使わないSpark Connectクライアントを接続する。"""
    from pyspark.sql import SparkSession

    client = (
        SparkSession.builder.remote(f"sc://127.0.0.1:{port}")
        .appName("Foundation R evaluation")
        .getOrCreate()
    )
    # Spark ConnectのSQLでは文字列型名をSTRINGとして指定する。
    query = SQL.replace("AS VARCHAR", "AS STRING")

    def collect():
        """Sailが返す結果を比較できる列の並びにする。"""
        return [tuple(row) for row in client.sql(query).collect()]

    return collect, "0.7.3", client.stop


def convert_files():
    """CSVとJSONLを読み、Parquetの件数と集計を再検査する。"""
    from datafusion import SessionContext

    with tempfile.TemporaryDirectory(prefix="foundation-r-datafusion-") as tmp:
        root = Path(tmp)
        csv = root / "measurements.csv"
        csv.write_text("sequence,value\n1,22.5\n2,23.5\n", encoding="utf-8")
        events = root / "measurements.json"
        events.write_text('{"sequence":3,"value":24.5}\n', encoding="utf-8")
        context = SessionContext()
        combined = context.read_csv(csv).union(context.read_json(events))
        output = root / "parquet"
        combined.write_parquet(output)
        context.register_parquet("measurements", output)
        report = context.sql(
            "SELECT COUNT(*) AS samples, AVG(value) AS average "
            "FROM measurements"
        ).to_pylist()
        if report != [{"samples": 3, "average": 23.5}]:
            raise ValueError("Parquetの結果が合成入力と一致しません。")
        return sum(file.stat().st_size for file in output.glob("*.parquet"))


def main():
    """比較対象と反復回数を選び、計測の条件もJSONとして返す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("engine", choices=("datafusion", "sail"))
    parser.add_argument("--sail-port", type=int, default=18601)
    args = parser.parse_args()
    if not 1 <= args.sail_port <= 65535:
        parser.error("ローカルポートの範囲は1〜65535です。")
    before = time.monotonic_ns()
    if args.engine == "datafusion":
        collect, engine_version, close = datafusion()
    else:
        collect, engine_version, close = sail(args.sail_port)
    duration = []
    try:
        initialize_ms = (time.monotonic_ns() - before) / 1_000_000
        for _ in range(4):
            start = time.monotonic_ns()
            if collect() != EXPECTED:
                raise ValueError("SilverとQuarantineの期待値に一致しません。")
            duration.append((time.monotonic_ns() - start) / 1_000_000)
    finally:
        close()
    report = {
        "candidate": args.engine,
        "version": engine_version,
        "fixture_rows": 8,
        "result_rows": len(EXPECTED),
        "quarantine_rows": 1,
        "initialization_ms": initialize_ms,
        "first_query_ms": duration[0],
        "warm_query_ms": duration[1:],
        "warm_median_ms": statistics.median(duration[1:]),
        "warm_max_ms": max(duration[1:]),
        "client_peak_rss_kib_linux": resource.getrusage(
            resource.RUSAGE_SELF
        ).ru_maxrss,
    }
    if args.engine == "datafusion":
        report["parquet_bytes"] = convert_files()
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()

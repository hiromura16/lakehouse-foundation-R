"""固定ParquetでDuckDBとDataFusionのフィルター・集計を比較する。"""

import argparse
import hashlib
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from importlib.metadata import version
from pathlib import Path

SIZES = (10_000, 100_000, 1_000_000)
ENGINES = ("duckdb", "datafusion")
ROW_GROUP = 8192
SHARDS = 4
WARM_RUNS = 3
EXPECTED_VERSIONS = {
    "duckdb": "2.0.1",
    "datafusion": "55.0.0",
    "pyarrow": "26.0.1",
    "cloudpickle": "3.1.3",
    "typing-extensions": "4.16.0",
}
EXPECTED = {
    10_000: (
        303,
        "f0d91ab012531f3bc3bf774e832a68f65c6f6323457bb936df95be30aed291a6",
    ),
    100_000: (
        1625,
        "9b148586411cc237128bb14e03209f936b8c0e97ad86849cdca0f8c4085f175a",
    ),
    1_000_000: (
        1792,
        "60dad250a59fb0d20268eaf87408391e48978e562d8c8310fd8eabefd2fb507e",
    ),
}
SQL = """
SELECT event_day, equipment_id,
       NULLIF(COUNT(*), 0) AS sample_count,
       CAST(SUM(CAST(value_milli AS BIGINT)) AS BIGINT) AS value_sum_milli,
       MIN(value_milli) AS value_min_milli,
       MAX(value_milli) AS value_max_milli
FROM measurements
WHERE quality_code = 0 AND equipment_id BETWEEN 0 AND 127
  AND value_milli BETWEEN 20000 AND 80000
GROUP BY event_day, equipment_id
ORDER BY event_day, equipment_id
"""


def schema(arrow, fields):
    """読み書き双方の列名・順序・型・NULL制約を固定する。"""
    return arrow.schema([(name, type_()) for name, type_ in fields])


def source_schema(arrow):
    """合成計測データはすべて整数として保存する。"""
    return schema(
        arrow,
        [
            ("event_day", arrow.int32),
            ("equipment_id", arrow.int32),
            ("quality_code", arrow.int8),
            ("value_milli", arrow.int32),
        ],
    )


def result_schema(arrow):
    """件数と合計は64ビット整数で返す。"""
    return schema(
        arrow,
        [
            ("event_day", arrow.int32),
            ("equipment_id", arrow.int32),
            ("sample_count", arrow.int64),
            ("value_sum_milli", arrow.int64),
            ("value_min_milli", arrow.int32),
            ("value_max_milli", arrow.int32),
        ],
    )


def rows(first, stop):
    """乱数や入力時刻に依存しない計測値を作る。"""
    for i in range(first, stop):
        yield (
            (i // 2048) % 14,
            (i * 73 + i // 89) % 192,
            (i * 13 + 17) % 11,
            15_000 + (i * 7639 + i // 31) % 70_001,
        )


def payload_sha(value):
    """空白と改行を含まない整数のJSON配列をSHA-256で照合する。"""
    encoded = json.dumps(value, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def reference(size):
    """各エンジンと独立したPython集計で期待する全行の順序と値を求める。"""
    groups = defaultdict(list)
    for day, equipment, quality, reading in rows(0, size):
        if quality == 0 and equipment <= 127 and 20_000 <= reading <= 80_000:
            groups[day, equipment].append(reading)
    expected = [
        [day, equipment, len(values), sum(values), min(values), max(values)]
        for (day, equipment), values in sorted(groups.items())
    ]
    return len(expected), payload_sha(expected)


def fixture(root, size):
    """4ファイルのParquetを書き、同じソースを両エンジンへ渡す。"""
    import pyarrow as pa
    import pyarrow.parquet as pq

    directory = root / f"rows-{size}"
    directory.mkdir()
    layout = source_schema(pa)
    files = []
    for shard in range(SHARDS):
        start, stop = size * shard // SHARDS, size * (shard + 1) // SHARDS
        file = directory / f"part-{shard:02}.parquet"
        with pq.ParquetWriter(
            file,
            layout,
            compression="zstd",
            compression_level=3,
            use_dictionary=False,
            write_statistics=True,
        ) as writer:
            for first in range(start, stop, ROW_GROUP):
                batch = list(rows(first, min(first + ROW_GROUP, stop)))
                writer.write_table(
                    pa.Table.from_arrays(list(zip(*batch)), schema=layout)
                )
        parquet = pq.ParquetFile(file)
        if (
            parquet.metadata.num_rows != stop - start
            or parquet.schema_arrow != layout
        ):
            raise ValueError(
                "Parquetの件数またはスキーマが生成条件と異なります。"
            )
        with file.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        files.append(
            {
                "name": file.name,
                "bytes": file.stat().st_size,
                "rows": parquet.metadata.num_rows,
                "row_groups": parquet.num_row_groups,
                "sha256": digest,
            }
        )
    return directory, files


def setup(engine, directory, threads):
    """同じParquetを等しい論理名で登録し、Arrow全行を取得できるようにする。"""
    if engine == "duckdb":
        import duckdb

        con = duckdb.connect(config={"threads": str(threads)})
        con.read_parquet(
            [str(path) for path in sorted(directory.glob("*.parquet"))]
        ).create_view("measurements")

        def query():
            """SQLの結果をArrowへ実体化する。"""
            return con.execute(SQL).to_arrow_table()

        return query, con.close

    from datafusion import SessionConfig, SessionContext

    context = SessionContext(SessionConfig().with_target_partitions(threads))
    context.register_parquet("measurements", str(directory))

    def query():
        """SQLの結果をArrowへ実体化する。"""
        return context.sql(SQL).to_arrow_table()

    return query, lambda: None


def peak_rss_kib():
    """Linuxが記録した当該プロセスの最大常駐メモリを取得する。"""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def worker(engine, directory, cpus, expected_rows, expected_sha):
    """新規プロセスで初回と同一セッションの反復3回を計測する。"""
    os.sched_setaffinity(0, cpus)
    start = time.perf_counter_ns()
    import pyarrow as pa

    query, close = setup(engine, directory, len(cpus))
    setup_ms = (time.perf_counter_ns() - start) / 1_000_000
    target = result_schema(pa)
    setup_peak = peak_rss_kib()
    samples = []
    try:
        for _ in range(1 + WARM_RUNS):
            start = time.perf_counter_ns()
            table = query()
            elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
            if not table.schema.equals(target, check_metadata=False):
                raise ValueError(
                    f"{engine}の結果スキーマが期待値と異なります。\n{table.schema}\n{target}"
                )
            observed_sha = payload_sha(
                [
                    [row[name] for name in target.names]
                    for row in table.to_pylist()
                ]
            )
            if table.num_rows != expected_rows or observed_sha != expected_sha:
                raise ValueError(
                    f"{engine}の結果件数またはSHA-256が期待値と異なります。"
                )
            samples.append(elapsed_ms)
            del table
    finally:
        close()
    return {
        "engine": engine,
        "version": version(engine),
        "setup_ms": setup_ms,
        "first_query_ms": samples[0],
        "warm_ms": samples[1:],
        "warm_median_ms": statistics.median(samples[1:]),
        "setup_process_peak_rss_kib": setup_peak,
        "process_peak_rss_kib": peak_rss_kib(),
        "checked_iterations": len(samples),
        "result_rows": expected_rows,
        "result_sha256": observed_sha,
    }


def limits(path):
    """コンテナのcgroup v2制約が見える環境では記録する。"""
    try:
        group = next(
            line.split(":", 2)[2]
            for line in Path("/proc/self/cgroup").read_text().splitlines()
            if line.startswith("0::")
        )
        return (
            (Path("/sys/fs/cgroup") / group.lstrip("/") / path)
            .read_text()
            .strip()
        )
    except (OSError, StopIteration):
        return None


def cpu_model():
    """Linuxが公開するCPUのモデル名を取得する。"""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            name, _, value = line.partition(":")
            if name.strip() in {"model name", "Hardware"}:
                return value.strip()
    except OSError:
        return None
    return None


def run(output, threads):
    """順序効果を抑えるために各規模で2通りの順序を連続実行する。"""
    import pyarrow as pa

    actual = {name: version(name) for name in EXPECTED_VERSIONS}
    if actual != EXPECTED_VERSIONS:
        raise ValueError(f"固定した依存関係が必要です。実際の版={actual}")
    available = sorted(os.sched_getaffinity(0))
    if not 1 <= threads <= len(available):
        raise ValueError(
            "スレッド数を実行環境のCPU affinityの範囲で指定してください。"
        )
    cpus = available[:threads]
    output.mkdir()
    workloads = []
    for size in SIZES:
        directory, files = fixture(output, size)
        expected_rows, expected_sha = reference(size)
        if (expected_rows, expected_sha) != EXPECTED[size]:
            raise ValueError(
                f"{size}行の参照結果が固定した件数またはSHA-256と異なります。"
            )
        sessions = []
        for order in (ENGINES, tuple(reversed(ENGINES))):
            for engine in order:
                arguments = [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    engine,
                    str(directory),
                    ",".join(map(str, cpus)),
                    str(expected_rows),
                    expected_sha,
                ]
                completed = subprocess.run(
                    arguments,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                if completed.returncode:
                    raise RuntimeError(
                        f"{engine}の{size}行測定が失敗しました。\n{completed.stderr}"
                    )
                sessions.append(json.loads(completed.stdout))
        workloads.append(
            {
                "input_rows": size,
                "fixtures": files,
                "file_set_sha256": payload_sha(
                    [[f["name"], f["sha256"]] for f in files]
                ),
                "result_rows": expected_rows,
                "result_sha256": expected_sha,
                "sessions": sessions,
            }
        )
    report = {
        "python": platform.python_version(),
        "kernel": platform.release(),
        "architecture": platform.machine(),
        "cpu_model": cpu_model(),
        "cpu_affinity": cpus,
        "cgroup_v2_cpu_max": limits("cpu.max"),
        "cgroup_v2_memory_max": limits("memory.max"),
        "dependencies": actual,
        "source_schema": str(source_schema(pa)),
        "result_schema": str(result_schema(pa)),
        "query": SQL.strip(),
        "session_order": ["duckdb", "datafusion", "datafusion", "duckdb"],
        "cache": (
            "first query in new process; "
            "OS page cache neither cleared nor measured"
        ),
        "elapsed": (
            "SQL parse/plan/execute and Arrow materialization; "
            "validation excluded"
        ),
        "peak_rss": (
            "Linux process maximum RSS in KiB; "
            "includes imports, engine, Arrow and result validation"
        ),
        "workloads": workloads,
    }
    (output / "report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main():
    """Linuxで測定を起動し、内部ワーカーでは1セッションだけを実行する。"""
    if not sys.platform.startswith("linux"):
        raise SystemExit("この測定はLinuxのaffinityとRSS単位を前提とします。")
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        engine, directory, cpus, expected_rows, expected_sha = sys.argv[2:]
        report = worker(
            engine,
            Path(directory),
            set(map(int, cpus.split(","))),
            int(expected_rows),
            expected_sha,
        )
        print(json.dumps(report))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", required=True, type=Path, help="新規の結果ディレクトリ"
    )
    parser.add_argument(
        "--threads", type=int, default=2, help="両エンジンに割り当てるCPU数"
    )
    args = parser.parse_args()
    output = args.output.resolve()
    run(output, args.threads)
    print(output / "report.json")


if __name__ == "__main__":
    main()

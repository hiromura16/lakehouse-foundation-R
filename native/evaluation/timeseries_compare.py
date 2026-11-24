"""同じ24時間の合成Line Protocolを2種類の時系列DBで照合する。"""

import argparse
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from native.tools.demo import NoRedirect

# UTCの同じ24時間を使い、日をまたいだ実行でも入力のSHAを固定する。
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
MINUTES = 24 * 60
DEVICES = ("P01", "P02")


def fixture():
    """入力、設備ごとの1分集計、末尾2点をPythonで独立に求める。"""
    base = int(START.timestamp())
    lines, minutes, latest = [], [], []
    for minute in range(MINUTES):
        start = base + minute * 60
        for offset, device in enumerate(DEVICES):
            low = 20 + 10 * offset + 0.25 * (minute % 60)
            for second, value in ((0, low), (30, low + 0.5)):
                epoch_ms = (start + second) * 1000
                lines.append(
                    f"temperature,device={device} celsius={value} {epoch_ms}"
                )
            minutes.append([start * 1000, device, 2, low + 0.25])
            if minute == MINUTES - 1:
                latest.append([(start + 30) * 1000, device, low + 0.5])
    return ("\n".join(lines) + "\n").encode(), minutes, latest


def digest(items):
    """両製品の結果を同じJSON表現に揃えてから指紋を計算する。"""
    data = json.dumps(items, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(data.encode()).hexdigest()


def epoch_ms(value):
    """それぞれのHTTP応答で使われる数値またはISO時刻をUTCに揃える。"""
    if isinstance(value, int):
        # GreptimeDBのLine Protocol経路はナノ秒を数値で返す。
        millis, remainder = divmod(value, 1_000_000)
        if remainder:
            raise ValueError("試験にないサブミリ秒の時刻を受け取りました。")
        return millis
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return int(stamp.timestamp() * 1000)


def sql_pair(column):
    """同じ半開区間と集計を使い、相違は各製品の時刻列だけに限る。"""
    window = (
        f"FROM temperature WHERE {column} >= TIMESTAMP '2026-01-01 00:00:00' "
        f"AND {column} < TIMESTAMP '2026-01-02 00:00:00'"
    )
    return (
        f"SELECT date_bin(INTERVAL '1 minute', {column}) AS stamp, device, "
        f"COUNT(*) AS points, AVG(celsius) AS celsius {window} "
        "GROUP BY 1, 2 ORDER BY 1, 2",
        f"SELECT {column} AS stamp, device, celsius {window} "
        "ORDER BY 1 DESC, 2 LIMIT 2",
    )


def client(port, headers):
    """試験用のループバック以外へHTTPや認証値を送らない。"""
    http = build_opener(ProxyHandler({}), NoRedirect)
    origin = f"http://127.0.0.1:{port}"

    def send(path, data=None):
        """無入力のGETと本文付きのPOSTだけを許可する。"""
        request = Request(origin + path, data=data, headers=headers)
        with http.open(request, timeout=15) as reply:
            return json.load(reply) if reply.status != 204 else None

    return send


def endpoints(args):
    """APIごとの応答を同じ列順に揃え、書込・SQLの関数を返す。"""
    token = json.loads(args.token_file.read_text(encoding="utf-8"))["token"]
    if not isinstance(token, str) or not token.startswith("apiv3_"):
        raise ValueError("InfluxDBのオフライン管理トークンが必要です。")
    influx = client(args.influx_port, {"Authorization": f"Bearer {token}"})
    greptime = client(args.greptime_port, {"X-Greptime-Timezone": "UTC"})
    database = args.database
    influx_write = "/api/v3/write_lp?" + urlencode(
        {
            "db": database,
            "precision": "millisecond",
            "accept_partial": "false",
        }
    )
    greptime_write = (
        f"/v1/influxdb/api/v2/write?bucket={database}&precision=ms"
    )

    def influx_sql(query):
        """InfluxDBのJSONオブジェクトを列順の配列にする。"""
        parameters = urlencode({"db": database, "q": query, "format": "json"})
        rows = influx("/api/v3/query_sql?" + parameters)
        # レスポンスのJSONキー順はAPIの契約に含まれない。
        return [
            [
                row[k]
                for k in ("stamp", "device", "points", "celsius")
                if k in row
            ]
            for row in rows
        ]

    def greptime_sql(query, db=database):
        """GreptimeDBのSQLエラーを検出し、行だけを返す。"""
        result = greptime(
            f"/v1/sql?db={db}",
            urlencode({"sql": query}).encode(),
        )
        if "output" not in result:
            raise ValueError("GreptimeDBがSQLを受理しませんでした。")
        return result["output"][0].get("records", {}).get("rows", [])

    if not args.verify_only:
        # 新規DBの名前に限定済み。GreptimeDBは着地前にDBを用意する。
        greptime_sql(f"CREATE DATABASE IF NOT EXISTS {database}", db="public")
    return (
        (influx, influx_write, influx_sql, "time"),
        (greptime, greptime_write, greptime_sql, "greptime_timestamp"),
    )


def verify(args):
    """初回書込、1分集計、再送後の値を各DBで同じ条件で検査する。"""
    raw, minutes, latest = fixture()
    expected = digest([minutes, latest])
    report = {
        "points": len(minutes) * 2,
        "minute_groups": len(minutes),
        "input_sha256": hashlib.sha256(raw).hexdigest(),
        "result_sha256": expected,
        "engines": {},
    }
    engines = endpoints(args)
    for name, (send, write_path, query, column) in zip(
        ("influxdb3", "greptimedb"), engines, strict=True
    ):
        aggregate_sql, latest_sql = sql_pair(column)

        def normalized(sql):
            """各行の時刻を検証し、DB間で比較できる値にする。"""
            return [[epoch_ms(row[0]), *row[1:]] for row in query(sql)]

        times = {}
        if not args.verify_only:
            start = time.perf_counter()
            assert send(write_path, raw) is None, name
            times["write_ms"] = round((time.perf_counter() - start) * 1000, 2)

        query_ms = []
        for _ in range(4):
            start = time.perf_counter()
            result = normalized(aggregate_sql)
            query_ms.append(1000 * (time.perf_counter() - start))
            assert result == minutes, f"{name}: 1分集計"
        assert digest([result, normalized(latest_sql)]) == expected, name
        times.update(
            first_sql_ms=round(query_ms[0], 2),
            repeated_sql_median_ms=round(median(query_ms[1:]), 2),
        )
        if not args.verify_only:
            assert send(write_path, raw) is None, name
            assert (
                digest([normalized(aggregate_sql), normalized(latest_sql)])
                == expected
            ), f"{name}: 再送"
        report["engines"][name] = times
    return report


def main():
    """合成DB名とローカルポートを受け、測定値だけをJSONで表示する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--influx-port", type=int, required=True)
    parser.add_argument("--greptime-port", type=int, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"foundation_eval_ts_[a-f0-9]{8}", args.database):
        parser.error(
            "試験専用DB名に foundation_eval_ts_ と小文字16進8桁を指定します。"
        )
    if not all(
        1 <= port <= 65535 for port in (args.influx_port, args.greptime_port)
    ):
        parser.error("ポート番号は1から65535です。")
    try:
        report = verify(args)
    except HTTPError as error:
        parser.exit(1, f"時系列APIのHTTP応答が異常です（{error.code}）。\n")
    except AssertionError as error:
        parser.exit(1, f"時系列の期待値と一致しません（{error}）。\n")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(
            1, f"時系列照合に失敗しました（{type(error).__name__}）。\n"
        )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()

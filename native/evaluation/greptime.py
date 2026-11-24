"""GreptimeDBへ合成時系列3点を書き、件数、平均、最新値を照合する。"""

import argparse
import json
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from native.tools.demo import NoRedirect


def main():
    """ループバックの専用テーブルで固定したSQLだけを動かす。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", default=18603, type=int)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("ポートの範囲は1〜65535です。")
    client = build_opener(ProxyHandler({}), NoRedirect)
    url = f"http://127.0.0.1:{args.port}/v1/sql"

    def sql(statement):
        """HTTPでSQLを実行し、結果セットの確認に必要な項目を返す。"""
        body = urlencode({"sql": statement}).encode()
        with client.open(Request(url, data=body), timeout=10) as response:
            return json.load(response)["output"][0]

    sql(
        "CREATE TABLE IF NOT EXISTS foundation_eval_temperature "
        "(device STRING, ts TIMESTAMP(3) TIME INDEX, celsius DOUBLE, "
        "PRIMARY KEY (device));"
    )
    sql(
        "INSERT INTO foundation_eval_temperature VALUES "
        "('PRESS-1', '2026-01-01 00:00:00', 22.5), "
        "('PRESS-1', '2026-01-01 00:00:01', 23.5), "
        "('PRESS-1', '2026-01-01 00:00:02', 24.5);"
    )
    aggregate = sql(
        "SELECT count(*), avg(celsius) FROM foundation_eval_temperature"
    )["records"]["rows"]
    latest = sql(
        "SELECT celsius FROM foundation_eval_temperature "
        "ORDER BY ts DESC LIMIT 1"
    )["records"]["rows"]
    if aggregate != [[3, 23.5]] or latest != [[24.5]]:
        raise ValueError("時系列のSQL結果が期待値に一致しません。")
    print(json.dumps({"samples": 3, "average": 23.5, "latest": 24.5}))


if __name__ == "__main__":
    main()

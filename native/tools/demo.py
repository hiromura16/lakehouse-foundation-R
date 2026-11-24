"""InfluxDB 3へ合成設備データを書き、SQLで書込結果を照合する。"""

import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)


class NoRedirect(HTTPRedirectHandler):
    """ローカルAPIから先へ管理用トークンを転送しない。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """リダイレクトをエラーとして呼び出し元へ返す。"""


def check(port, token_file):
    """固定のデモ用DBだけに3点を書き、時刻順の値を確認する。"""
    if not 1 <= port <= 65535:
        raise ValueError("ローカルポートを指定してください。")
    token = json.loads(token_file.read_text())["token"]
    if not isinstance(token, str) or not token.startswith("apiv3_"):
        raise ValueError("InfluxDB 3のオフライン管理トークンが必要です。")
    http = build_opener(ProxyHandler({}), NoRedirect)
    origin = f"http://127.0.0.1:{port}"
    auth = {"Authorization": f"Bearer {token}"}
    sample_time = int(time.time()) // 60 * 60
    values = [22.5, 23.5, 24.5]
    payload = "\n".join(
        f"temperature,device=demo-press celsius={value} "
        f"{sample_time - (2 - index) * 60}"
        for index, value in enumerate(values)
    )
    write_path = "/api/v3/write_lp?db=foundation_demo&precision=second"
    request = Request(origin + write_path, data=payload.encode(), headers=auth)
    with http.open(request, timeout=8) as response:
        if response.status != 204:
            raise ValueError("書き込みが成功しませんでした。")
    sql = (
        "SELECT celsius FROM temperature WHERE device='demo-press' "
        "ORDER BY time DESC LIMIT 3"
    )
    query = urlencode({"db": "foundation_demo", "q": sql, "format": "json"})
    request = Request(origin + "/api/v3/query_sql?" + query, headers=auth)
    with http.open(request, timeout=8) as response:
        records = json.load(response)
    if [row["celsius"] for row in records] != values[::-1]:
        raise ValueError("SQLの応答が合成入力と一致しません。")
    return {"database": "foundation_demo", "points": len(records)}


def main():
    """ローカルの合成デモを実行し、秘密や元応答を出さずに結果を返す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=18510, help="ローカルAPI")
    parser.add_argument(
        "--token-file",
        type=Path,
        required=True,
        help="Git外の管理トークンJSON",
    )
    args = parser.parse_args()
    try:
        result = check(args.port, args.token_file)
    except (OSError, KeyError, TypeError, ValueError, HTTPError) as error:
        parser.exit(1, f"時系列デモの検証失敗（{type(error).__name__}）。\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

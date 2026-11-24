"""ローカルのOpenObserveを初期化し、転送とダッシュボードを設定する。"""

import argparse
import base64
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from native.tools.demo import NoRedirect
from native.tools.setup import create_private

DASHBOARD = Path(__file__).with_name("openobserve-dashboard.json")


def local_api(directory, port):
    """生成済みの設定からループバック専用のAPIクライアントを作る。"""
    if not 1 <= port <= 65535:
        raise ValueError("ポート番号が範囲外です。")
    with (directory / "tools.env").open(encoding="utf-8") as stream:
        env = dict(
            line.strip().split("=", 1) for line in stream if "=" in line
        )
    user, password = env["TOOLS_OBSERVE_USER"], env["TOOLS_OBSERVE_PASSWORD"]
    raw = f"{user}:{password}".encode()
    authorization = "Basic " + base64.b64encode(raw).decode()
    opener = build_opener(ProxyHandler({}), NoRedirect)

    def request(path, body=None):
        """管理者の認証情報をファイルや標準出力へ追加せずに送る。"""
        headers = {
            "Authorization": authorization,
            "Content-Type": "application/json",
        }
        data = None if body is None else json.dumps(body).encode()
        address = f"http://127.0.0.1:{port}/api/default{path}"
        with opener.open(Request(address, data, headers), timeout=15) as reply:
            return json.load(reply)

    return request


def provision(directory, request):
    """転送用パスコードを初回だけ保存し、同名のダッシュボードを保つ。"""
    passcode_file = directory / "observe-passcode"
    if not passcode_file.exists():
        data = request("/passcode")["data"]
        passcode = data["passcode"]
        if not isinstance(passcode, str) or not passcode.strip():
            raise ValueError("取得したパスコードが空です。")
        create_private(passcode_file, passcode)
    document = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    boards = request("/dashboards")["dashboards"]
    if any(board["title"] == document["title"] for board in boards):
        return
    request("/dashboards", document)


def series_count(request):
    """受信側で直近の疎通メトリクスを検索し、系列数を数える。"""
    parameters = urlencode({"query": "probe_success"})
    reply = request(f"/prometheus/api/v1/query?{parameters}")
    series = reply["data"]["result"]
    if reply["status"] != "success" or not series:
        raise ValueError("受信した監視メトリクスがありません。")
    return len(series)


def main():
    """接続先と処理を選び、結果の概要だけを表示する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("provision", "verify"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--port", type=int, default=18508)
    args = parser.parse_args()
    try:
        request = local_api(args.directory, args.port)
        if args.action == "provision":
            provision(args.directory, request)
            print("転送用の設定とダッシュボードを確認しました。")
        else:
            print(f"監視メトリクスを{series_count(request)}系列受信しました。")
    except (OSError, KeyError, TypeError, ValueError) as error:
        parser.exit(
            1, f"OpenObserveの設定確認に失敗（{type(error).__name__}）。\n"
        )


if __name__ == "__main__":
    main()

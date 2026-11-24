"""分析ツールに公開済みGoldの固定読取操作を提供する。"""

import json
import os
import re
from typing import Literal, get_args
from urllib.parse import urlencode, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

Dataset = Literal[
    "factory_daily_production",
    "sales_fulfillment",
    "sensor_latest",
    "sensor_minute",
]
DATASETS = get_args(Dataset)
LOOPBACK = {"127.0.0.1", "localhost", "::1"}
MAX_RESPONSE_BYTES = 16 * 1024 * 1024


class NoRedirect(HTTPRedirectHandler):
    """固定した接続先に認証ヘッダーを留める。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """サーバーの指示による送信先の変更を禁止する。"""
        return None


class GoldClient:
    """起動時に接続先と読取専用トークンを確定し、固定ルートのみ呼ぶ。"""

    def __init__(self, *, local_only: bool = False):
        """MCPではループバックへ直接接続し、LangChainではHTTPSも許可する。"""
        origin = os.environ.get("LAKEHOUSE_API_URL", "http://127.0.0.1:18502")
        url = urlsplit(origin)
        local_http = url.scheme == "http" and url.hostname in LOOPBACK
        if (
            not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
            or url.path not in ("", "/")
            or not (local_http or (url.scheme == "https" and not local_only))
        ):
            raise ValueError("許可されたAPI接続先を指定してください。")
        token = os.environ.get("LAKEHOUSE_READER_TOKEN", "")
        if not re.fullmatch(r"[a-f0-9]{64}", token):
            raise ValueError("64文字の読取トークンを設定してください。")
        self._origin = origin.rstrip("/")
        self._token = token
        # ローカル接続のトークンは環境変数の外部プロキシへ渡さない。
        proxy = ProxyHandler({}) if local_http else ProxyHandler()
        self._http = build_opener(proxy, NoRedirect)

    def _get(self, path: str) -> dict:
        """契約で確定したパスからサイズ上限付きのJSONを取得する。"""
        request = Request(
            f"{self._origin}{path}",
            headers={"Authorization": "Bearer " + self._token},
        )
        with self._http.open(request, timeout=15) as response:
            raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError(
                "応答が大きすぎます。取得行数を減らしてください。"
            )
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError("APIの応答形式を確認してください。")
        return result

    def latest_release(self) -> dict:
        """最新の公開世代・時刻・出力スナップショットを選択して返す。"""
        result = self._get("/v1/releases/latest")
        return {
            "release_snapshot_id": result["release_snapshot_id"],
            "released_at": result["released_at"],
            "sensor": result.get("sensor"),
            "outputs": {
                name: str(snapshot) if snapshot is not None else None
                for name, snapshot in result["outputs"].items()
            },
        }

    def rows(
        self, dataset: Dataset, limit: int = 25, release: str | None = None
    ) -> dict:
        """許可された表から、指定世代の行を最大200件取得する。"""
        if (
            dataset not in DATASETS
            or type(limit) is not int
            or not 1 <= limit <= 200
        ):
            raise ValueError(
                "登録済みの表名と1〜200の件数を指定してください。"
            )
        params = {"limit": str(limit)}
        if release is not None:
            if not re.fullmatch(r"[0-9]{1,19}", release):
                raise ValueError(
                    "公開世代IDを数字の文字列で指定してください。"
                )
            params["release"] = release
        return self._get(f"/v1/gold/{dataset}?{urlencode(params)}")

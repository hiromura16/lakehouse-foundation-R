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
Channel = Literal["files", "sql", "cdc"]
CHANNELS = get_args(Channel)
BUSINESS_DATASETS = {"factory_daily_production", "sales_fulfillment"}
ROUTES = {
    "files": ("/v1/gold", "gold"),
    "sql": ("/v1/sql/gold", "sql_gold"),
    "cdc": ("/v1/cdc/gold", "cdc_gold"),
}
SNAPSHOT_ID = re.compile(r"[0-9]{1,19}")
FILE_OUTPUT = re.compile(r"(?:silver|quarantine|gold)\.[a-zA-Z0-9_]+")
LOOPBACK = {"127.0.0.1", "localhost", "::1"}
MAX_RESPONSE_BYTES = 16 * 1024 * 1024


def valid_snapshot(value):
    """64-bitのIceberg IDは、JSONの整数に丸めずに文字列で照合する。"""
    return isinstance(value, str) and SNAPSHOT_ID.fullmatch(value) is not None


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
        try:
            url = urlsplit(origin)
            port = url.port
        except ValueError:
            raise ValueError(
                "許可されたAPI接続先を指定してください。"
            ) from None
        local_http = url.scheme == "http" and url.hostname in LOOPBACK
        if (
            not url.hostname
            or url.username is not None
            or url.password is not None
            or port == 0
            or url.query
            or url.fragment
            or url.path not in ("", "/")
            or not (local_http or (url.scheme == "https" and not local_only))
        ):
            raise ValueError("許可されたAPI接続先を指定してください。")
        token = os.environ.get("LAKEHOUSE_READER_TOKEN", "")
        if not re.fullmatch(r"[a-f0-9]{64}", token):
            raise ValueError("64文字の読取トークンを設定してください。")
        channels = os.environ.get("LAKEHOUSE_GOLD_CHANNELS", "files")
        self._channels = frozenset(
            name.strip() for name in channels.split(",")
        )
        if not self._channels <= ROUTES.keys():
            raise ValueError(
                "許可する取込経路をfiles、sql、cdcから指定してください。"
            )
        self._origin = f"{url.scheme}://{url.netloc}"
        self._token = token
        # ローカル接続のトークンは環境変数の外部プロキシへ渡さない。
        proxy = ProxyHandler({}) if local_http else ProxyHandler()
        self._http = build_opener(proxy, NoRedirect)

    def _get(self, path: str) -> dict:
        """契約で確定したパスからサイズ上限付きのJSONを取得する。"""
        request = Request(
            f"{self._origin}{path}",
            headers={"Authorization": "Bearer " + self._token},
            method="GET",
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
        """ファイル経路の公開世代だけを、既存の一覧APIで取得する。"""
        if "files" not in self._channels:
            raise ValueError("ファイル経路の参照は許可されていません。")
        result = self._get("/v1/releases/latest")
        outputs = result.get("outputs")
        if (
            not valid_snapshot(result.get("release_snapshot_id"))
            or not isinstance(result.get("released_at"), str)
            or not isinstance(outputs, dict)
            or any(
                not FILE_OUTPUT.fullmatch(name)
                or (snapshot is not None and not valid_snapshot(snapshot))
                for name, snapshot in outputs.items()
            )
        ):
            raise ValueError("APIの公開世代を照合できませんでした。")
        return {
            "release_snapshot_id": result["release_snapshot_id"],
            "released_at": result["released_at"],
            "sensor": result.get("sensor"),
            "outputs": outputs,
        }

    def rows(
        self,
        dataset: Dataset,
        limit: int = 25,
        release: str | None = None,
        *,
        channel: Channel = "files",
    ) -> dict:
        """許可された経路と世代に対応する固定表から、最大200件取得する。"""
        if channel not in CHANNELS or channel not in self._channels:
            raise ValueError("指定した取込経路の参照は許可されていません。")
        if (
            dataset not in DATASETS
            or (channel != "files" and dataset not in BUSINESS_DATASETS)
            or type(limit) is not int
            or not 1 <= limit <= 200
        ):
            raise ValueError(
                "登録済みの表名と1〜200の件数を指定してください。"
            )
        params = {"limit": str(limit)}
        if release is not None:
            if not valid_snapshot(release):
                raise ValueError(
                    "公開世代IDを数字の文字列で指定してください。"
                )
            # APIは受け取ったIDを整数として解決し、規格化した文字列を返す。
            params["release"] = str(int(release))
        path, namespace = ROUTES[channel]
        result = self._get(f"{path}/{dataset}?{urlencode(params)}")
        public_id = result.get("release_snapshot_id")
        snapshot = result.get("snapshot_id")
        rows, total = result.get("rows"), result.get("total_rows")
        if (
            result.get("table") != f"{namespace}.{dataset}"
            or not valid_snapshot(public_id)
            or ("release" in params and public_id != params["release"])
            or "snapshot_id" not in result
            or (snapshot is not None and not valid_snapshot(snapshot))
            or type(total) is not int
            or total < 0
            or (snapshot is None and total != 0)
            or not isinstance(rows, list)
            or len(rows) != min(total, limit)
            or not all(isinstance(row, dict) for row in rows)
            or result.get("truncated") is not (total > limit)
        ):
            raise ValueError("APIの表・公開世代・件数を照合できませんでした。")
        return result

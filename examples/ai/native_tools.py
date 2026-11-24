"""Foundation Rの公開スナップショットをLangChainから読み取る。"""

import argparse
import json
import os
from typing import Literal
from urllib.parse import urlencode, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    Request,
    build_opener,
)

from langchain_core.tools import tool

Dataset = Literal[
    "factory_daily_production",
    "sales_fulfillment",
    "sensor_latest",
    "sensor_minute",
]


class NoRedirect(HTTPRedirectHandler):
    """読取用の資格情報を設定済みの接続先内に留める。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """資格情報が他の接続先へ渡らないよう、リダイレクトを拒否する。"""
        return None


@tool
def lakehouse_gold(
    dataset: Dataset, limit: int = 25, release: str | None = None
) -> dict:
    """公開済みのGold表を読み、行データとIcebergの参照情報を返します。

    最初の応答のrelease_snapshot_idを後続の呼出しに渡すと、同じ公開世代の
    データを比較できます。登録済みのGoldデータセットを1回につき200行まで取得できます。
    """
    if dataset not in Dataset.__args__ or not 1 <= limit <= 200:
        raise ValueError(
            "登録済みのデータセット名と1〜200の件数を指定してください。"
        )
    origin = os.environ.get("LAKEHOUSE_API_URL", "http://127.0.0.1:18502")
    url = urlsplit(origin)
    if (
        url.username is not None
        or url.password is not None
        or url.query
        or url.fragment
        or url.path not in ("", "/")
        or not url.hostname
        or (
            url.scheme != "https"
            and not (
                url.scheme == "http"
                and url.hostname in ("localhost", "127.0.0.1", "::1")
            )
        )
    ):
        raise ValueError(
            "HTTPSの接続先またはHTTPのループバック接続先を指定してください。"
        )
    params = {"limit": str(limit)}
    if release is not None:
        params["release"] = release
    request = Request(
        f"{origin.rstrip('/')}/v1/gold/{dataset}?{urlencode(params)}",
        headers={
            "Authorization": "Bearer " + os.environ["LAKEHOUSE_READER_TOKEN"]
        },
    )
    with build_opener(NoRedirect).open(request, timeout=15) as response:
        return json.load(response)


def main():
    """モデルやトレーシングを初期化せずに、ツールを1回呼び出す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=Dataset.__args__)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument(
        "--release", help="後続の参照で使用するIcebergの公開世代ID"
    )
    args = parser.parse_args()
    result = lakehouse_gold.invoke(vars(args))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

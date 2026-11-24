"""Foundation Rの公開スナップショットをLangChainから読み取る。"""

import argparse
import json

from gold_client import CHANNELS, DATASETS, Channel, Dataset, GoldClient
from langchain_core.tools import tool


@tool
def lakehouse_gold(
    dataset: Dataset,
    limit: int = 25,
    release: str | None = None,
    channel: Channel = "files",
) -> dict:
    """公開済みのGold表を読み、行データとIcebergの参照情報を返します。

    既定はfilesです。SQL・CDCでは日次生産と受注・出荷を選べます。
    同じ世代で比較する場合は同じchannelと最初の公開世代IDを指定します。
    登録済みのデータセットを1回につき200行まで取得できます。
    """
    return GoldClient().rows(dataset, limit, release, channel=channel)


def main():
    """モデルやトレーシングを初期化せずに、ツールを1回呼び出す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=DATASETS)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--channel", choices=CHANNELS, default="files")
    parser.add_argument(
        "--release", help="後続の参照で使用するIcebergの公開世代ID"
    )
    args = parser.parse_args()
    result = lakehouse_gold.invoke(vars(args))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

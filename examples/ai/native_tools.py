"""Foundation Rの公開スナップショットをLangChainから読み取る。"""

import argparse
import json

from gold_client import DATASETS, Dataset, GoldClient
from langchain_core.tools import tool


@tool
def lakehouse_gold(
    dataset: Dataset, limit: int = 25, release: str | None = None
) -> dict:
    """公開済みのGold表を読み、行データとIcebergの参照情報を返します。

    最初の応答のrelease_snapshot_idを後続の呼出しに渡すと、同じ公開世代の
    データを比較できます。登録済みのGoldデータセットを1回につき200行まで取得できます。
    """
    return GoldClient().rows(dataset, limit, release)


def main():
    """モデルやトレーシングを初期化せずに、ツールを1回呼び出す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=DATASETS)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument(
        "--release", help="後続の参照で使用するIcebergの公開世代ID"
    )
    args = parser.parse_args()
    result = lakehouse_gold.invoke(vars(args))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

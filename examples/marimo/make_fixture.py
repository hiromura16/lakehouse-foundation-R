"""コミット済みの独立オラクルから、秘密を含まない検証用Goldを作る。"""

import argparse
import hashlib
import json
from pathlib import Path

from notebook.approved_gold import COLUMNS


def main():
    """既知の期待値に一致した場合だけ新しい承認ディレクトリを準備する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("oracle", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    oracle = json.loads(args.oracle.read_text(encoding="utf-8"))
    expected = next(
        check["expected"]
        for check in oracle["checks"]
        if check["name"] == "factory daily measures avoid join multiplication"
    )
    if oracle.get("seed") != 42 or len(expected) != 14:
        raise ValueError("固定合成データの期待値が変わっています。")
    gold = {
        "table": "gold.factory_daily_production",
        "release_snapshot_id": None,
        "snapshot_id": None,
        "total_rows": len(expected),
        "truncated": False,
        "rows": [dict(zip(COLUMNS, row, strict=True)) for row in expected],
    }
    content = (json.dumps(gold, sort_keys=True, indent=2) + "\n").encode()
    approval = {
        "format_version": 1,
        "source": "committed_synthetic_oracle",
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    args.destination.mkdir(mode=0o700)
    (args.destination / "gold.json").write_bytes(content)
    (args.destination / "approval.json").write_text(
        json.dumps(approval, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()

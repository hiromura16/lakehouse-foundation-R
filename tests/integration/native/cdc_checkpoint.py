"""試験用SQLiteで削除処理が確定するまで待つ。"""

import argparse
import sqlite3
import time
from pathlib import Path

TOPIC = "foundation_r.synthetic_sales.dbo.erp_products"


def wait_for_checkpoint(path, expected, seconds=45):
    """読み取り専用でoffsetを確認して、再起動の境界を固定する。"""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if path.is_file():
            with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
                row = db.execute(
                    "SELECT offset_id FROM offsets "
                    "WHERE topic=? AND partition_id=0",
                    (TOPIC,),
                ).fetchone()
            if row == (expected,):
                return
        time.sleep(1)
    raise ValueError("期待したCDCの完了offsetを確認できませんでした。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite_file", type=Path)
    parser.add_argument("expected", type=int)
    args = parser.parse_args()
    wait_for_checkpoint(args.sqlite_file.resolve(), args.expected)

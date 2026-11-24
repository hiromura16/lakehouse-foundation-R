#!/usr/bin/env python3
"""コピーを終えた原本にSHA-256の完了印を付ける。"""

import argparse
import os
import tempfile
from hashlib import sha256
from pathlib import Path

from raw_file_input import source_bytes


def publish_marker(path):
    """検査した原本の隣で完了印を書き切ってから、一度に公開する。"""
    digest = sha256(source_bytes(path)).hexdigest()
    target = path.with_name(path.name + ".ready")
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="ascii",
        dir=path.parent,
        prefix=".ready-",
        suffix=".part",
        delete=False,
    ) as staging:
        temporary = Path(staging.name)
        try:
            staging.write(digest + "\n")
            staging.flush()
            os.fsync(staging.fileno())
            staging.close()
            # 同じディレクトリでrenameすると、内容を丸ごと差し替えられる。
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)


def main():
    """原本のパスを読み、完了印だけを公開する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "file", type=Path, help="コピーとcloseが済んだ通常ファイル"
    )
    path = parser.parse_args().file
    if path.name.endswith((".part", ".ready")):
        parser.error("原本のファイル名を指定してください。")
    try:
        publish_marker(path)
    except (OSError, ValueError) as error:
        parser.exit(
            1, f"完了印を作成できませんでした（{type(error).__name__}）。\n"
        )
    print("完了印を公開しました。取込時に原本のハッシュを再照合します。")


if __name__ == "__main__":
    main()

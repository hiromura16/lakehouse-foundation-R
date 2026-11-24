"""同期したFoundationの業務契約のハッシュを検証する。"""

import json
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    """コミット済みのマニフェストと手元のソースのバイト列を照合する。"""
    path = ROOT / "contracts/native/foundation-source.json"
    manifest = json.loads(path.read_text())
    files = manifest["source_sha256"]
    for name, recorded in files.items():
        source = (ROOT / name).resolve()
        if not source.is_relative_to(ROOT):
            raise SystemExit("契約マニフェストがリポジトリ外を指しています。")
        if sha256(source.read_bytes()).hexdigest() != recorded:
            raise SystemExit(f"契約内容が変わっています。対象ファイル {name}")
    print(f"Foundationの契約ファイル{len(files)}件を照合しました。")


if __name__ == "__main__":
    main()

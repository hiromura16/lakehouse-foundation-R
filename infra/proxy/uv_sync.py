"""BuildKit内で、固定依存を指定された仮想環境へインストールする。"""

import argparse
import os
import subprocess
from pathlib import Path

UV = "/usr/local/bin/uv"
SECRETS = {
    "UV_CONFIG_FILE": Path("/run/secrets/uv.toml"),
    "NETRC": Path("/run/secrets/pypi.netrc"),
}


def main():
    """ミラー設定と認証を、依存を取得するプロセスの間だけ適用する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lock", type=Path)
    parser.add_argument("environment", type=Path)
    args = parser.parse_args()

    subprocess.run([UV, "--no-cache", "venv", args.environment], check=True)
    environment = os.environ.copy()
    for setting, source in SECRETS.items():
        # 未設定時の空ファイルは読み込まず、uvの通常動作を保つ。
        if source.is_file() and source.stat().st_size:
            environment[setting] = str(source)
    subprocess.run(
        [
            UV,
            "--no-cache",
            "pip",
            "sync",
            "--python",
            args.environment / "bin/python",
            "--require-hashes",
            "--only-binary",
            ":all:",
            args.lock,
        ],
        env=environment,
        check=True,
    )


if __name__ == "__main__":
    main()

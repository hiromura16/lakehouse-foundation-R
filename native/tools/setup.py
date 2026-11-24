"""運用ツールの資格情報を、新規の非公開ディレクトリに一度だけ生成する。"""

import argparse
import json
import os
import secrets
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def create_private(path, contents):
    """同名ディレクトリや既存ファイルを上書きせず、0600で保存する。"""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(contents + "\n")


def initialize(directory):
    """各サービスの秘密を分けて生成し、値を画面に出さずに保存する。"""
    directory = directory.expanduser().absolute()
    if (
        directory.resolve().is_relative_to(ROOT)
        or any(path.is_symlink() for path in (directory, *directory.parents))
        or any(ch in str(directory) for ch in "\\'\n\r")
    ):
        raise ValueError("チェックアウト外の通常のパスを指定してください。")
    password = secrets.token_hex(24)
    # NGINXと互換性のあるハッシュを作り、パスワードは標準入力で渡す。
    hashed = subprocess.check_output(
        ["openssl", "passwd", "-6", "-stdin"],
        input=password + "\n",
        text=True,
    ).strip()
    directory.mkdir(mode=0o700)
    values = {
        "TOOLS_DB_PASSWORD": secrets.token_hex(24),
        "TOOLS_N8N_KEY": secrets.token_hex(32),
        "TOOLS_OBSERVE_USER": "operator@example.invalid",
        "TOOLS_OBSERVE_PASSWORD": "Aa1!" + secrets.token_hex(24),
        "TOOLS_EXPLORER_KEY": secrets.token_hex(32),
        "TOOLS_HTPASSWD": f"'{directory / 'operator.htpasswd'}'",
        "TOOLS_INFLUX_TOKEN_FILE": f"'{directory / 'influx-admin.json'}'",
        "TOOLS_OBSERVE_PASSCODE_FILE": f"'{directory / 'observe-passcode'}'",
    }
    files = {
        "operator.htpasswd": "operator:" + hashed,
        "operator.json": json.dumps(
            {"user": "operator", "password": password}
        ),
        "influx-admin.json": json.dumps(
            {"name": "_admin", "token": "apiv3_" + secrets.token_urlsafe(48)}
        ),
        "tools.env": "\n".join(
            f"{key}={value}" for key, value in values.items()
        ),
    }
    created = []
    try:
        for name, contents in files.items():
            path = directory / name
            create_private(path, contents)
            created.append(path)
    except OSError:
        for path in created:
            path.unlink()
        # 途中失敗した場所に再初期化をかける前に内容を確認できるよう残す。
        raise
    return directory / "tools.env"


def main():
    """初回の設定ファイルを生成し、保存先だけを表示する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path.home() / ".config/lakehouse-foundation-r/tools",
        help="新規作成する専用ディレクトリ。既存の設定は再利用してください。",
    )
    try:
        path = initialize(parser.parse_args().directory)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"追加ツールの設定を作成できません。{error}\n")
    print(f"設定ファイルを作成しました。{path}")


if __name__ == "__main__":
    main()

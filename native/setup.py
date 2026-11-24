"""Foundation R用の非公開設定とファイル受け渡しディレクトリを作る。"""

import argparse
import os
import secrets
from pathlib import Path


def initialize(output: Path, landing: Path) -> None:
    """URLで安全に扱える資格情報を一度だけ生成し、所有者限定で保存する。

    Dockerは数値のUID・GIDを作成者に合わせ、受け渡し先を読取専用でマウントする。
    ホスト側の書込み元は原本とSHA-256完了印を別々に確定する。
    既存環境を再開する場合は、同じ設定ファイルを引き継ぐ。
    """
    if not landing.is_absolute() or not output.is_absolute():
        raise ValueError("設定と受け渡し先は絶対パスで指定してください。")
    if any(ch in str(landing) for ch in ("'", "\n", "\r", "\\")):
        raise ValueError(
            "受け渡し先のパスには引用符やエスケープ文字を使えません。"
        )
    if not os.getuid() or not os.getgid():
        raise ValueError(
            "受け渡し先を所有する一般ユーザーでセットアップしてください。"
        )
    output.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    landing.mkdir(parents=True, mode=0o750, exist_ok=True)
    values = {
        "LANDING_DIR": f"'{landing}'",
        "LANDING_UID": str(os.getuid()),
        "LANDING_GID": str(os.getgid()),
        "CATALOG_DB_PASSWORD": secrets.token_hex(24),
        "CATALOG_ENCRYPTION_KEY": secrets.token_hex(32),
        "S3_ACCESS_KEY": secrets.token_hex(10),
        "S3_SECRET_KEY": secrets.token_hex(24),
        "WORKER_TOKEN": secrets.token_hex(32),
        "READER_TOKEN": secrets.token_hex(32),
        "GRAFANA_ADMIN_PASSWORD": secrets.token_hex(24),
    }
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            for key, value in values.items():
                file.write(f"{key}={value}\n")
            file.flush()
            os.fsync(file.fileno())
    except BaseException:
        output.unlink()
        raise


def main() -> None:
    """生成した値を表示せず、設定ファイルの場所だけを返す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    config = Path.home() / ".config/lakehouse-foundation-r"
    parser.add_argument("--output", type=Path, default=config / "site.env")
    parser.add_argument("--landing", type=Path, default=config / "landing")
    args = parser.parse_args()
    try:
        initialize(args.output, args.landing)
    except (OSError, ValueError) as error:
        parser.exit(1, f"セットアップに失敗しました。{error}\n")
    print(f"非公開設定を作成しました。{args.output}")


if __name__ == "__main__":
    main()

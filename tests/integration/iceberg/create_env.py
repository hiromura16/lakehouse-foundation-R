"""隔離した合成試験専用のCompose資格情報を生成する。"""

import os
import secrets
from pathlib import Path


def main() -> None:
    """既存の資格情報は上書きせず、値も端末に表示しない。"""
    env_file = Path(__file__).resolve().with_name(".env")
    values = {
        "CATALOG_DB_PASSWORD": secrets.token_hex(18),
        "CATALOG_ENCRYPTION_KEY": secrets.token_hex(24),
        "S3_ACCESS_KEY": secrets.token_hex(10),
        "S3_SECRET_KEY": secrets.token_hex(20),
    }
    descriptor = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        for key, value in values.items():
            stream.write(f"{key}={value}\n")


if __name__ == "__main__":
    main()

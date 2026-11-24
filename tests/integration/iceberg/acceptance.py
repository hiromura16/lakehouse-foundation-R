"""使い捨てのComposeで、RisingWaveからIcebergへ書き戻す。"""

import argparse
import gzip
import hashlib
import os
import platform
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.request import Request, urlopen

SOURCE = Path(__file__).resolve().parent
REPO = SOURCE.parents[2]
FIXTURES = (
    ".dockerignore",
    "compose.yaml",
    "create_env.py",
    "fixture.py",
    "readback.py",
    "reader.Dockerfile",
)
SECRETS = (
    "CATALOG_DB_PASSWORD",
    "CATALOG_ENCRYPTION_KEY",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
)


# 公式配布のLinux amd64用拡張。照合後、DuckDBのLOADでも署名を確認する。
DUCKDB_EXTENSIONS = {
    "httpfs": (
        "b66f800b84197768a9cc4d65c293d824c94f37f04b3d640c970d2e79aad8f7b0"
    ),
    "iceberg": (
        "42d781e40bdb23f9c89e9b7218d2bbe17568449fa1cbdae3bf9d9e0f773ef2b0"
    ),
}


def prepare_extensions(work):
    """実行ホストの標準プロキシ設定を使い、専用ディレクトリに拡張を固定する。"""
    if platform.machine() != "x86_64":
        raise RuntimeError("upsertの受入にはLinux amd64が必要です。")
    directory = work / "extensions"
    directory.mkdir()
    # UID 10001の読取コンテナにだけ、このディレクトリを読取専用で渡す。
    directory.chmod(0o755)
    for name, expected in DUCKDB_EXTENSIONS.items():
        filename = f"{name}.duckdb_extension"
        url = f"https://extensions.duckdb.org/v2.0.1/linux_amd64/{filename}.gz"
        # 公式クライアントを示すUAで配布先へ接続する。
        request = Request(url, headers={"User-Agent": "DuckDB/v2.0.1"})
        sha256 = hashlib.sha256()
        size = 0
        target = directory / filename
        with (
            urlopen(request, timeout=30) as response,
            gzip.GzipFile(fileobj=response) as archive,
            target.open("xb") as output,
        ):
            while chunk := archive.read(1024 * 1024):
                size += len(chunk)
                if size > 64 * 1024 * 1024:
                    raise RuntimeError(f"DuckDB拡張が上限を超えました: {name}")
                sha256.update(chunk)
                output.write(chunk)
        if sha256.hexdigest() != expected:
            raise RuntimeError(f"DuckDB拡張のSHA-256が不一致です: {name}")
        target.chmod(0o644)


def run(work, use_sudo, *, upsert=False):
    """新しい資格情報と固有のprojectを用い、検証後にその環境を片付ける。"""
    for name in FIXTURES:
        shutil.copyfile(SOURCE / name, work / name)
    # 一般ユーザーのPyIcebergコンテナが、検証スクリプトを読み取れるようにする。
    (work / "readback.py").chmod(0o644)
    for source, filename in (
        ("native/requirements.lock", "requirements.lock"),
        ("native/evaluation/risingwave.toml", "risingwave.toml"),
        ("native/evaluation/telemetry-wire.sql", "telemetry-wire.sql"),
    ):
        shutil.copyfile(REPO / source, work / filename)
    environment = dict(
        os.environ, FIXTURE_DOCKER_SUDO="1" if use_sudo else "0"
    )
    for key in SECRETS:
        environment.pop(key, None)

    def execute(*command, timeout=360):
        """Composeを実行する場合も、暗黙のカレントディレクトリに依存しない。"""
        subprocess.run(
            command, check=True, cwd=work, env=environment, timeout=timeout
        )

    docker = ["sudo", "-n", "docker"] if use_sudo else ["docker"]
    project = "fr-iceberg-" + work.name.split(".")[-1].lower()
    compose = [
        *docker,
        "compose",
        "-p",
        project,
        "--env-file",
        ".env",
        "-f",
        "compose.yaml",
        "--profile",
        "reader",
    ]
    if upsert:
        shutil.copyfile(SOURCE / "upsert.py", work / "upsert.py")
        prepare_extensions(work)
    execute(sys.executable, "create_env.py")
    try:
        execute(*compose, "config", "--quiet")
        execute(
            *compose, "up", "-d", "catalog", "storage", "broker", "risingwave"
        )
        execute(*compose, "build", "reader")
        if upsert:
            execute(sys.executable, "upsert.py", timeout=720)
        else:
            for step in (
                "initialize",
                "baseline",
                "recover",
                "replay",
                "telemetry-wire",
                "audit",
            ):
                execute(sys.executable, "fixture.py", step, timeout=720)
    finally:
        execute(
            *compose, "down", "--volumes", "--remove-orphans", "--rmi", "local"
        )


def main():
    """専用環境を毎回新規作成し、他のDockerプロジェクトから分離する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sudo", action="store_true")
    parser.add_argument(
        "--upsert",
        action="store_true",
        help="CDC更新・削除とカタログ停止時の復旧を検証する",
    )
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="foundation-r-iceberg-") as temp:
        work = Path(temp) / f"fixture.{secrets.token_hex(4)}"
        work.mkdir(mode=0o700)
        run(work, args.sudo, upsert=args.upsert)


if __name__ == "__main__":
    main()

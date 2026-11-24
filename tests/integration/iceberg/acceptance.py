"""使い捨てのComposeで、RisingWaveからIcebergへ書き戻す。"""

import argparse
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

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


def run(work, use_sudo):
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
    execute(sys.executable, "create_env.py")
    try:
        execute(*compose, "config", "--quiet")
        execute(
            *compose, "up", "-d", "catalog", "storage", "broker", "risingwave"
        )
        execute(*compose, "build", "reader")
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
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="foundation-r-iceberg-") as temp:
        work = Path(temp) / f"fixture.{secrets.token_hex(4)}"
        work.mkdir(mode=0o700)
        run(work, args.sudo)


if __name__ == "__main__":
    main()

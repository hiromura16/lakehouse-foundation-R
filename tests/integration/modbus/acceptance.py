"""読取専用Modbusの試験を、実機の接続先を受け取らずに実行する。"""

import argparse
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

FIXTURES = (
    ".dockerignore",
    "compose.yaml",
    "fixture.py",
    "probe.py",
    "requirements.lock",
    "simulator.Dockerfile",
    "simulator.py",
    "telegraf.toml",
)


def verify(directory, use_sudo):
    """専用の内部ネットワークに模擬PLCと読み取り収集器を起動する。"""
    source = Path(__file__).resolve().parent
    for name in FIXTURES:
        shutil.copyfile(source / name, directory / name)
    (directory / "telegraf.toml").chmod(0o644)
    env = dict(os.environ, FIXTURE_DOCKER_SUDO="1" if use_sudo else "0")
    env["FOUNDATION_MODBUS_FIXTURE_MODE"] = "normal"
    docker = ["sudo", "-n", "docker"] if use_sudo else ["docker"]
    project = "fr-modbus-" + directory.name.split(".")[-1]
    compose = [*docker, "compose", "-p", project, "-f", "compose.yaml"]

    def run(*command, timeout=180):
        """隔離されたファイルと環境へ実行対象を限定する。"""
        subprocess.run(
            command, cwd=directory, env=env, check=True, timeout=timeout
        )

    try:
        run(*compose, "config", "--quiet")
        run(*compose, "up", "-d", "--build")
        run(sys.executable, "fixture.py", timeout=240)
    finally:
        run(
            *compose, "down", "--volumes", "--remove-orphans", "--rmi", "local"
        )


def main():
    """使い捨てのproject名を生成し、実行後は試験のファイルだけ片付ける。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="foundation-r-modbus-") as temp:
        target = Path(temp) / f"fixture.{secrets.token_hex(4)}"
        target.mkdir(mode=0o700)
        verify(target, args.sudo)


if __name__ == "__main__":
    main()

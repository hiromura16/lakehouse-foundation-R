"""Modbus RTUの仮想バスだけを実行し、専用のDocker資源を破棄する。"""

import argparse
import secrets
import shutil
import subprocess
import tempfile
from pathlib import Path

FILES = ("compose.yaml", "rtu.Dockerfile", "rtu_fixture.py", ".dockerignore")


def verify(directory, *, sudo):
    """動的な名前付きプロジェクトへ、固定の試験ファイルだけを渡す。"""
    source = Path(__file__).resolve().parent
    for name in FILES:
        shutil.copyfile(source / name, directory / name)
    # 同じ行・型・倍率の測定を、共有しているTCP試験と比較する。
    shutil.copyfile(
        source.parent / "telegraf.toml", directory / "telegraf-reference.toml"
    )
    docker = ["sudo", "-n", "docker"] if sudo else ["docker"]
    name = "fr-modbus-rtu-" + secrets.token_hex(4)
    compose = [*docker, "compose", "-p", name, "-f", "compose.yaml"]

    def run(*args, timeout):
        """試験時間に上限を持ち、Dockerの終了コードを結果に反映する。"""
        subprocess.run(
            [*compose, *args], cwd=directory, timeout=timeout, check=True
        )

    try:
        run("config", "--quiet", timeout=20)
        run("build", "fixture", timeout=160)
        run("run", "--rm", "-T", "fixture", timeout=75)
    finally:
        run(
            "down",
            "--volumes",
            "--remove-orphans",
            "--rmi",
            "local",
            timeout=40,
        )


def main():
    """実機・物理デバイス名・接続先を指定する引数を持たない。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="fr-modbus-rtu-") as temp:
        verify(Path(temp), sudo=args.sudo)


if __name__ == "__main__":
    main()

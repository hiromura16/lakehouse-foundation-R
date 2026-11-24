#!/usr/bin/env python3
"""停止済みFoundation RのDockerボリュームを採取し、後で検証する。"""

import argparse
import json
import os
import re
import stat
import subprocess
import sys
from datetime import datetime, timezone
from hashlib import file_digest, sha256
from pathlib import Path, PurePosixPath

REPO = Path(__file__).resolve().parents[1]
POSTGRES = (
    "postgres:18.6-alpine@sha256:"
    "300ebd02c6d4de9ee27ad7f6855d5d21b4d3a4a5117767c1197d284c635d9736"
)
REQUIRED = {"catalog-db", "storage", "lakehouse"}
DOCKER_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,253}")
PROJECT = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
SHA256 = re.compile(r"[a-f0-9]{64}")
ARCHIVE_NAME = re.compile(r"volume-[0-9]{3}\.tar")
MAX_VOLUMES = 100
MANIFEST_BYTES = 256 * 1024


class BackupError(ValueError):
    """公開ログへ出せる、値やパスを含まないエラー。"""


def call(docker, *arguments, stdout=subprocess.PIPE, timeout=30):
    """指定Dockerの応答だけを使い、停止や展開を行うコマンドは呼ばない。"""
    return subprocess.run(
        [*docker, *arguments],
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=subprocess.PIPE,
        check=True,
        timeout=timeout,
    ).stdout


def stopped_services(docker, project):
    """ラベルが一致する対象を一括で読み、稼働中や終了異常なら止める。"""
    identifiers = call(
        docker,
        "ps",
        "--all",
        "--quiet",
        "--filter",
        f"label=com.docker.compose.project={project}",
    ).split()
    if not identifiers or len(identifiers) > MAX_VOLUMES:
        raise BackupError("対象Composeのサービスを特定できません。")
    values = json.loads(call(docker, "inspect", *identifiers))
    services = {}
    for item in values:
        labels = item["Config"]["Labels"]
        service = labels.get("com.docker.compose.service")
        state = item["State"]
        if (
            labels.get("com.docker.compose.project") != project
            or not isinstance(service, str)
            or service in services
            or state["Status"] != "exited"
        ):
            raise BackupError(
                "専用Composeをstopしてから、単一の停止済みサービスを確認します。"
            )
        services[service] = item
    if not REQUIRED <= services.keys():
        raise BackupError("Lakehouse、カタログDB、保存先が揃っていません。")
    return services


def identities(services):
    """採取の前後でコンテナの作成と稼働履歴が変わっていないか確認する。"""
    return {
        name: (
            item["Id"],
            item["State"]["StartedAt"],
            item["State"]["FinishedAt"],
        )
        for name, item in services.items()
    }


def volume_maps(services):
    """対象コンテナが使う名前付き・匿名ボリュームの実名を収集する。"""
    volumes = {}
    for service, item in sorted(services.items()):
        for mount in item["Mounts"]:
            if mount["Type"] != "volume":
                continue
            name = mount["Name"]
            if not DOCKER_NAME.fullmatch(name):
                raise BackupError("対象のボリューム名が不正です。")
            volumes.setdefault(name, []).append(
                {"service": service, "mount": mount["Destination"]}
            )
    if not 1 <= len(volumes) <= MAX_VOLUMES:
        raise BackupError("採取できるボリューム数の範囲外です。")
    return volumes


def idle_volume(docker, name):
    """共有されたボリュームを含め、起動中の使用者が残っていないか調べる。"""
    call(docker, "volume", "inspect", "--format", "{{.Name}}", name)
    running = call(docker, "ps", "--quiet", "--filter", f"volume={name}")
    if running.strip():
        raise BackupError("稼働中のコンテナと共有するボリュームがあります。")


def volume_command(name, binary, *arguments):
    """固定イメージからネットワーク外で読取専用のボリュームを参照する。"""
    return (
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "none",
        "--read-only",
        "--env",
        "LC_ALL=C",
        "--cap-drop",
        "ALL",
        "--cap-add",
        "DAC_OVERRIDE",
        "--security-opt",
        "no-new-privileges:true",
        "--mount",
        f"type=volume,source={name},target=/archive,readonly",
        "--entrypoint",
        binary,
        POSTGRES,
        *arguments,
    )


def checked_postgres(docker, services):
    """PostgreSQLの物理データを保存できる正常な停止時点か調べる。"""
    postgres_services = ("catalog-db", "tools-db")
    for name in postgres_services:
        if name not in services:
            continue
        service = services[name]
        root = PurePosixPath("/var/lib/postgresql")
        pgdata = next(
            (
                entry[7:]
                for entry in service["Config"]["Env"]
                if entry.startswith("PGDATA=")
            ),
            "",
        )
        target = PurePosixPath(pgdata)
        matches = [
            mount["Name"]
            for mount in service["Mounts"]
            if mount["Type"] == "volume" and mount["Destination"] == str(root)
        ]
        if (
            service["State"]["ExitCode"] != 0
            or len(matches) != 1
            or not target.is_relative_to(root)
            or ".." in target.parts
            or target.relative_to(root).parts[:1] != ("18",)
        ):
            raise BackupError("PostgreSQL 18の正常な停止点を確認できません。")
        local = str(PurePosixPath("/archive") / target.relative_to(root))
        idle_volume(docker, matches[0])
        report = call(
            docker, *volume_command(matches[0], "pg_controldata", local)
        )
        if not any(
            line.startswith(b"Database cluster state:")
            and line.partition(b":")[2].strip() == b"shut down"
            for line in report.splitlines()
        ):
            raise BackupError(
                "PostgreSQLの物理データは正常停止の状態が必要です。"
            )


def digest(path):
    """シンボリックリンク・FIFOを辿らず、原本とサイズをSDK外で検証する。"""
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(path, flags), "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise BackupError("アーカイブは通常のファイルを指定します。")
        checksum = file_digest(handle, "sha256").hexdigest()
        after = os.fstat(handle.fileno())
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise BackupError("照合中にアーカイブが変わりました。")
    return before.st_size, checksum


def publish(directory, manifest):
    """すべて確定した後に、manifestを書き込み同期して出力を確定する。"""
    body = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode()
    with (directory / "manifest.partial").open("xb") as output:
        output.write(body)
        output.flush()
        os.fsync(output.fileno())
    (directory / "manifest.partial").rename(directory / "manifest.json")
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return sha256(body).hexdigest()


def capture(docker, project, directory):
    """停止点のボリュームを専用先へ採取し、SHA-256の台帳を最後に作る。"""
    directory = directory.absolute()
    if directory.resolve() != directory or directory.is_relative_to(REPO):
        raise BackupError(
            "リポジトリの外の新規の非公開ディレクトリを使います。"
        )
    services = stopped_services(docker, project)
    volumes = volume_maps(services)
    for name in volumes:
        idle_volume(docker, name)
    checked_postgres(docker, services)
    directory.mkdir(mode=0o700)
    entries = []
    for number, (name, mounts) in enumerate(sorted(volumes.items()), 1):
        idle_volume(docker, name)
        filename = f"volume-{number:03}.tar"
        artifact = directory / filename
        with artifact.open("xb") as output:
            call(
                docker,
                *volume_command(
                    name, "/bin/tar", "-C", "/archive", "-cf", "-", "."
                ),
                stdout=output,
                timeout=1800,
            )
            output.flush()
            os.fsync(output.fileno())
        size, checksum = digest(artifact)
        if size == 0:
            raise BackupError("アーカイブの書込みが終わっていません。")
        entries.append(
            {
                "file": filename,
                "size": size,
                "sha256": checksum,
                "volume": name,
                "mounts": mounts,
            }
        )
    if identities(stopped_services(docker, project)) != identities(services):
        raise BackupError("採取中に対象プロジェクトの状態が変わりました。")
    for name in volumes:
        idle_volume(docker, name)
    result = publish(
        directory,
        {
            "format_version": 1,
            "project": project,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "volumes": entries,
        },
    )
    return {"volumes": len(entries), "manifest_sha256": result}


def verify(directory, expected):
    """同梱台帳とすべての原本を照合する。Dockerや元の環境に接続しない。"""
    path = directory / "manifest.json"
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(path, flags), "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise BackupError("台帳は通常のファイルを指定します。")
        body = handle.read(MANIFEST_BYTES + 1)
    checksum = sha256(body).hexdigest()
    if len(body) > MANIFEST_BYTES or (expected and expected != checksum):
        raise BackupError("台帳のサイズまたはSHA-256が一致しません。")
    document = json.loads(body)
    if (
        not isinstance(document, dict)
        or document.get("format_version") != 1
        or not isinstance(document.get("volumes"), list)
        or not 1 <= len(document["volumes"]) <= MAX_VOLUMES
    ):
        raise BackupError("台帳の形式が不正です。")
    seen = set()
    for entry in document["volumes"]:
        name, size, hash_value = entry["file"], entry["size"], entry["sha256"]
        if (
            not isinstance(name, str)
            or not ARCHIVE_NAME.fullmatch(name)
            or name in seen
            or type(size) is not int
            or size <= 0
            or not isinstance(hash_value, str)
            or not SHA256.fullmatch(hash_value)
            or digest(directory / name) != (size, hash_value)
        ):
            raise BackupError("台帳とアーカイブが一致しません。")
        seen.add(name)
    return {"volumes": len(seen), "manifest_sha256": checksum}


def main():
    """採取・検証の報告には処理数と台帳のSHA-256だけを出す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    take = commands.add_parser("capture", help="停止済みのボリュームを採取")
    take.add_argument("--project", required=True)
    take.add_argument("--sudo", action="store_true")
    take.add_argument("directory", type=Path)
    check = commands.add_parser("verify", help="原本と台帳をオフライン照合")
    check.add_argument("directory", type=Path)
    check.add_argument("--expected-manifest-sha256")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.action == "capture":
            if not PROJECT.fullmatch(args.project):
                raise BackupError(
                    "Docker ComposeのApp Nameを指定してください。"
                )
            docker = ["sudo", "-n", "docker"] if args.sudo else ["docker"]
            result = capture(docker, args.project, args.directory)
        else:
            result = verify(args.directory, args.expected_manifest_sha256)
    except BackupError as error:
        print(error, file=sys.stderr)
        return 1
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        subprocess.SubprocessError,
    ):
        print("ボリュームの採取または照合に失敗しました。", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

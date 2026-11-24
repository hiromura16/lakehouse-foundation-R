#!/usr/bin/env python3
"""停止済みのDockerボリュームを採取、照合、または隔離復元する。"""

import argparse
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from hashlib import file_digest, sha256
from pathlib import Path, PurePosixPath

from native_restore_tar import RestoreTarError, audit_archive

REPO = Path(__file__).resolve().parents[1]
LEGACY_POSTGRES = (
    "postgres:18.6-alpine@sha256:"
    "300ebd02c6d4de9ee27ad7f6855d5d21b4d3a4a5117767c1197d284c635d9736"
)
POSTGRES = (
    "postgres:18.7-alpine3.24@sha256:"
    "0115267ab5a509022df9319c77be365cef335adb71556c75f60f5360ee247e71"
)
POSTGRES_IMAGES = frozenset((LEGACY_POSTGRES, POSTGRES))
# 物理形式が同じだと確認できる、標準構成の固定イメージだけを対象にする。
CORE_IMAGES = {
    "catalog-db": POSTGRES,
    "catalog": (
        "quay.io/lakekeeper/catalog:v0.15.1@sha256:"
        "22dcb6044c106a962c242b70051bd226bd50c9134c763121d8713c37fe9a70ec"
    ),
    "storage": (
        "chrislusf/seaweedfs:4.55@sha256:"
        "7ca46aeae2f223b37e5d91593c17ab37738eec6df6ab69b48a0487f7643342b9"
    ),
}
# カタログの移行前に取得済みの物理アーカイブは、
# 台帳に記録された旧カタログとS3を組にして復元する。
LEGACY_CORE_IMAGES = {
    "catalog-db": POSTGRES,
    "catalog": (
        "quay.io/lakekeeper/catalog:v0.14.3@sha256:"
        "0009e7ccb26db811b8fc485974b624a999fd096f4da549a8704d3121dc20e98a"
    ),
    "storage": (
        "chrislusf/seaweedfs:4.53@sha256:"
        "972ba30c529dfc72a6a12ed427e94d99c7b6a6e59d97bfa52be0a849f43e2cc9"
    ),
}
PYTHON = (
    "docker.io/library/python:3.14-slim@sha256:"
    "22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b"
)
CORE_PGDATA = "/var/lib/postgresql/18/docker"
CORE_MOUNTS = {"catalog-db": "/var/lib/postgresql", "storage": "/data"}
ARCHIVE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
REQUIRED = {"catalog-db", "storage", "lakehouse"}
DOCKER_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,253}")
PROJECT = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
SHA256 = re.compile(r"[a-f0-9]{64}")
ARCHIVE_NAME = re.compile(r"volume-[0-9]{3}\.tar")
MAX_VOLUMES = 100
MANIFEST_BYTES = 256 * 1024


class BackupError(ValueError):
    """公開ログへ出せる、値やパスを含まないエラー。"""


def call(
    docker,
    *arguments,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.PIPE,
    timeout=30,
):
    """Dockerの標準出力だけを使い、失敗ログから非公開値を表示しない。"""
    return subprocess.run(
        [*docker, *arguments],
        stdin=stdin,
        stdout=stdout,
        stderr=subprocess.PIPE,
        check=True,
        timeout=timeout,
    ).stdout


def stopped_services(docker, project):
    """ラベルが一致する対象を一括で読み、停止済みか確認する。"""
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


def volume_command(name, binary, *arguments, image=POSTGRES):
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
        f"type=volume,source={name},target=/archive,readonly,volume-nocopy",
        "--entrypoint",
        binary,
        image,
        *arguments,
    )


def postgres_image(service):
    """採取元の固定版が既知なら、ネットワークなしで同じ補助イメージを使う。"""
    image = service["Config"]["Image"] if service else POSTGRES
    return image if image in POSTGRES_IMAGES else POSTGRES


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
        postgres_state(
            docker, matches[0], local, image=postgres_image(service)
        )


def postgres_state(docker, volume, directory, *, image=POSTGRES):
    """物理クラスタの制御ファイルで正常停止を確認する。"""
    report = call(
        docker,
        *volume_command(volume, "pg_controldata", directory, image=image),
    )
    if not any(
        line.startswith(b"Database cluster state:")
        and line.partition(b":")[2].strip() == b"shut down"
        for line in report.splitlines()
    ):
        raise BackupError("PostgreSQLの物理データは正常停止の状態が必要です。")


def image_platforms(docker, images):
    """Dockerが保持する実イメージから、OSとCPUを読む。"""
    items = json.loads(call(docker, "image", "inspect", *images))
    if len(items) != len(set(images)):
        raise BackupError("物理形式を確認できるイメージが揃っていません。")
    return {(item["Os"], item["Architecture"]) for item in items}


def supported_core_images(images):
    """台帳の任意のイメージを実行せず、固定版の組み合わせだけを受け付ける。"""
    if not isinstance(images, dict):
        return False
    database = images.get("catalog-db")
    return (
        isinstance(database, str)
        and database in POSTGRES_IMAGES
        and any(
            images == {**pinned, "catalog-db": database}
            for pinned in (CORE_IMAGES, LEGACY_CORE_IMAGES)
        )
    )


def capture_core(docker, services):
    """復元時に使う固定版を、停止したコンテナからそのまま記録する。"""
    images = {
        name: services[name]["Config"]["Image"]
        for name in CORE_IMAGES
        if name in services
    }
    if (
        not supported_core_images(images)
        or f"PGDATA={CORE_PGDATA}"
        not in services["catalog-db"]["Config"]["Env"]
    ):
        return None
    platforms = image_platforms(
        docker, [services[name]["Image"] for name in images]
    )
    if len(platforms) != 1:
        return None
    os_name, architecture = next(iter(platforms))
    if os_name != "linux" or architecture not in ("amd64", "arm64"):
        return None
    return {"architecture": architecture, "images": images}


def digest(path):
    """シンボリックリンク・FIFOを辿らず、原本とサイズをSDK外で検証する。"""
    with os.fdopen(os.open(path, ARCHIVE_FLAGS), "rb") as handle:
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


def publish(directory, manifest, filename="manifest.json"):
    """すべて確定した後に、manifestを書き込み同期して出力を確定する。"""
    body = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode()
    partial = directory / (Path(filename).stem + ".partial")
    with partial.open("xb") as output:
        output.write(body)
        output.flush()
        os.fsync(output.fileno())
    partial.rename(directory / filename)
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
    core = capture_core(docker, services)
    database = services.get("catalog-db") or services.get("tools-db")
    archive_image = postgres_image(database)
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
                    name,
                    "/bin/tar",
                    "-C",
                    "/archive",
                    "-cf",
                    "-",
                    ".",
                    image=archive_image,
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
            "format_version": 2,
            "project": project,
            "native_core": core,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "volumes": entries,
        },
    )
    return {"volumes": len(entries), "manifest_sha256": result}


def read_manifest(directory, expected):
    """公開済みの台帳を読み、承認されたハッシュと世代を確認する。"""
    path = directory / "manifest.json"
    with os.fdopen(os.open(path, ARCHIVE_FLAGS), "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise BackupError("台帳は通常のファイルを指定します。")
        body = handle.read(MANIFEST_BYTES + 1)
    checksum = sha256(body).hexdigest()
    if len(body) > MANIFEST_BYTES or (expected and expected != checksum):
        raise BackupError("台帳のサイズまたはSHA-256が一致しません。")
    document = json.loads(body)
    if (
        not isinstance(document, dict)
        or type(document.get("format_version")) is not int
        or document["format_version"] not in (1, 2)
        or not isinstance(document.get("volumes"), list)
        or not 1 <= len(document["volumes"]) <= MAX_VOLUMES
    ):
        raise BackupError("台帳の形式が不正です。")
    return document, checksum


def verify_entries(directory, document):
    """同梱台帳とすべての原本を照合する。Dockerや元の環境に接続しない。"""
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
    return len(seen)


def verify(directory, expected):
    """指定された台帳と原本を照合した件数を返す。"""
    document, checksum = read_manifest(directory, expected)
    return {
        "volumes": verify_entries(directory, document),
        "manifest_sha256": checksum,
    }


def core_archives(document, project):
    """台帳に記録したマウント先と固定イメージから、復元対象を一意に決める。"""
    core = document.get("native_core")
    if (
        document["format_version"] != 2
        or not isinstance(core, dict)
        or not supported_core_images(core.get("images"))
        or core.get("architecture") not in ("amd64", "arm64")
    ):
        raise BackupError(
            "固定イメージとCPUが記録された標準構成の台帳が必要です。"
        )
    original = document.get("project")
    if (
        not isinstance(original, str)
        or not PROJECT.fullmatch(original)
        or original == project
    ):
        raise BackupError(
            "採取元と異なるComposeのApp Nameを指定してください。"
        )
    archives = {}
    for name, mount in CORE_MOUNTS.items():
        matches = [
            entry
            for entry in document["volumes"]
            if {"service": name, "mount": mount} in entry["mounts"]
        ]
        if len(matches) != 1:
            raise BackupError(
                "標準のDBと保存先のアーカイブを一意に特定できません。"
            )
        archives[name] = matches[0]
    if len({entry["file"] for entry in archives.values()}) != len(CORE_MOUNTS):
        raise BackupError("DBと保存先に異なるアーカイブが必要です。")
    return core["architecture"], core["images"], archives


def empty_project(docker, project, *, check_volumes):
    """既存のアプリや過去の未確定復元を持つApp Nameを拒否する。"""
    if call(
        docker,
        "ps",
        "--all",
        "--quiet",
        "--filter",
        f"label=com.docker.compose.project={project}",
    ).strip():
        raise BackupError("復元先のComposeにコンテナが残っています。")
    if call(
        docker,
        "network",
        "ls",
        "--quiet",
        "--filter",
        f"label=com.docker.compose.project={project}",
    ).strip():
        raise BackupError("復元先のComposeに既存のネットワークがあります。")
    if check_volumes:
        for label in (
            "com.docker.compose.project",
            "foundation-r.restore-project",
        ):
            if call(
                docker,
                "volume",
                "ls",
                "--quiet",
                "--filter",
                f"label={label}={project}",
            ).strip():
                raise BackupError("復元先に既存のボリュームがあります。")


def create_staging_volume(docker, project, attempt, name):
    """この試行で確保したローカルのボリュームであることを確認する。"""
    labels = {
        "foundation-r.restore-project": project,
        "foundation-r.restore-id": attempt,
    }
    options = []
    for label, value in labels.items():
        options.extend(("--label", f"{label}={value}"))
    call(docker, "volume", "create", "--driver", "local", *options, name)
    (volume,) = json.loads(call(docker, "volume", "inspect", name))
    if (
        volume["Name"] != name
        or volume["Driver"] != "local"
        or volume.get("Options")
        or any(
            volume["Labels"].get(key) != value for key, value in labels.items()
        )
    ):
        raise BackupError(
            "今回の復元先として作成した空ボリュームか確認できません。"
        )


def restore_volume(docker, directory, entry, name):
    """原本を読み取り専用で開き、展開側でも内容を照合する。"""
    helper = (
        Path(__file__)
        .with_name("native_restore_tar.py")
        .read_text(encoding="utf-8")
    )
    with os.fdopen(
        os.open(directory / entry["file"], ARCHIVE_FLAGS), "rb"
    ) as archive:
        information = os.fstat(archive.fileno())
        if (
            not stat.S_ISREG(information.st_mode)
            or information.st_size != entry["size"]
        ):
            raise BackupError("復元直前のアーカイブを確認できません。")
        call(
            docker,
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            "none",
            "--read-only",
            "--user",
            "0:0",
            "--cap-drop",
            "ALL",
            "--cap-add",
            "CHOWN",
            "--cap-add",
            "DAC_OVERRIDE",
            "--cap-add",
            "FOWNER",
            "--security-opt",
            "no-new-privileges:true",
            "--interactive",
            "--mount",
            f"type=volume,source={name},target=/restore,volume-nocopy",
            "--entrypoint",
            "python",
            PYTHON,
            "-c",
            helper,
            str(entry["size"]),
            entry["sha256"],
            stdin=archive,
            timeout=1800,
        )


def restore(docker, project, directory, output, expected):
    """採取原本は保持し、隔離展開と検査が両方通った時だけ構成を確定する。"""
    directory, output = directory.absolute(), output.absolute()
    if (
        directory.resolve() != directory
        or output.resolve() != output
        or directory.is_relative_to(REPO)
        or output.is_relative_to(REPO)
        or output.is_relative_to(directory)
        or output.exists()
        or not isinstance(expected, str)
        or not SHA256.fullmatch(expected)
    ):
        raise BackupError(
            "原本と異なる非公開の新規出力先と、台帳のSHA-256を指定します。"
        )
    parent = output.parent.stat()
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or parent.st_mode & 0o022
    ):
        raise BackupError(
            "復元の出力先の親は所有者だけが書き込める必要があります。"
        )
    document, checksum = read_manifest(directory, expected)
    architecture, images, archives = core_archives(document, project)
    verify_entries(directory, document)
    for entry in archives.values():
        with os.fdopen(
            os.open(directory / entry["file"], ARCHIVE_FLAGS), "rb"
        ) as archive:
            if not stat.S_ISREG(os.fstat(archive.fileno()).st_mode):
                raise BackupError(
                    "復元の事前検査には通常のtarファイルが必要です。"
                )
            audit_archive(archive)
    if image_platforms(docker, [*images.values(), PYTHON]) != {
        ("linux", architecture)
    }:
        raise BackupError(
            "採取側と同じCPUの固定イメージを事前に取得してください。"
        )
    empty_project(docker, project, check_volumes=True)

    # Dockerの一意なコンテナ名を使い、同じApp Nameの復元を同時に確定させない。
    claim = (
        call(
            docker,
            "create",
            "--name",
            f"{project}-restore-claim",
            "--network",
            "none",
            "--pull",
            "never",
            "--entrypoint",
            "/bin/true",
            PYTHON,
        )
        .decode("ascii")
        .strip()
    )
    try:
        empty_project(docker, project, check_volumes=True)
        output.mkdir(mode=0o700)
        attempt = secrets.token_hex(12)
        volumes = {
            role: f"foundation-r-restore-{attempt}-{role}" for role in archives
        }
        publish(
            output,
            {
                "restore_project": project,
                "manifest_sha256": checksum,
                "images": images,
                "volumes": volumes,
            },
            filename="plan.json",
        )
        for role, volume in volumes.items():
            create_staging_volume(docker, project, attempt, volume)
            restore_volume(docker, directory, archives[role], volume)
        postgres_state(
            docker,
            volumes["catalog-db"],
            "/archive/18/docker",
            image=images["catalog-db"],
        )
        empty_project(docker, project, check_volumes=False)
    finally:
        call(docker, "rm", claim)
    publish(
        output,
        {
            "services": {
                role: {"image": image}
                for role, image in {
                    **images,
                    "catalog-migrate": images["catalog"],
                }.items()
            },
            "volumes": {
                role: {"external": True, "name": volume}
                for role, volume in volumes.items()
            },
        },
        filename="compose.restore.yaml",
    )
    return {"volumes": len(volumes), "manifest_sha256": checksum}


def main():
    """採取・検証・復元の報告には件数と台帳のSHA-256だけを出す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    take = commands.add_parser("capture", help="停止済みのボリュームを採取")
    take.add_argument("--project", required=True)
    take.add_argument("--sudo", action="store_true")
    take.add_argument("directory", type=Path)
    check = commands.add_parser("verify", help="原本と台帳をオフライン照合")
    check.add_argument("directory", type=Path)
    check.add_argument("--expected-manifest-sha256")
    recovery = commands.add_parser("restore", help="新規ボリュームへ隔離復元")
    recovery.add_argument("--project", required=True)
    recovery.add_argument("--output", required=True, type=Path)
    recovery.add_argument("--expected-manifest-sha256", required=True)
    recovery.add_argument("--sudo", action="store_true")
    recovery.add_argument("directory", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.action == "verify":
            result = verify(args.directory, args.expected_manifest_sha256)
        else:
            if not PROJECT.fullmatch(args.project):
                raise BackupError(
                    "Docker ComposeのApp Nameを指定してください。"
                )
            docker = ["sudo", "-n", "docker"] if args.sudo else ["docker"]
            if args.action == "capture":
                result = capture(docker, args.project, args.directory)
            else:
                result = restore(
                    docker,
                    args.project,
                    args.directory,
                    args.output,
                    args.expected_manifest_sha256,
                )
    except (BackupError, RestoreTarError) as error:
        print(error, file=sys.stderr)
        return 1
    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        subprocess.SubprocessError,
        tarfile.TarError,
    ):
        print(
            "ボリュームの採取、照合、または隔離復元に失敗しました。",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

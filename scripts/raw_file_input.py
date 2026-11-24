"""着地した原本と完了印を、保存先に依存せず読み取る。

矛盾した素材は返さない。S3、Bronze、完了台帳、Silver は呼出側の責務。
次の原本は要求されるまで読まないため、既存の表ごとの確定順も維持する。
"""

import os
import re
import stat
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

MAX_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class RawFile:
    """照合した原本 bytes、相対 path、内容 hash を同じ単位で渡す。"""

    path: Path
    relative: str
    data: bytes
    digest: str


def read_stable(path, limit, label):
    """通常ファイルを上限付きで読み、読取中の変更と差替えを拒否する。

    statとopenの間にFIFOへ差し替えられても待機しない。読んだfdと
    現在の着地パスの属性を比較し、同名差替えも失敗にする。
    """
    flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
    with os.fdopen(os.open(path, flags), "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"{label} must be a regular file.")
        if before.st_size > limit:
            raise ValueError(f"{label} exceeds the bounded import limit.")
        data = stream.read(limit + 1)
        if len(data) > limit:
            raise ValueError(f"{label} exceeds the bounded import limit.")
        after = os.fstat(stream.fileno())
        current = path.stat(follow_symlinks=False)
        if len(data) != after.st_size or not (
            identity(before) == identity(after) == identity(current)
        ):
            raise ValueError(f"{label} changed while it was read.")
        return data


def identity(info):
    """サイズ、時刻、デバイス、inodeを用いて読取りの一貫性を確認する。"""
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def completion_hash(marker):
    """通常ファイルの完了印から、改行付きSHA-256を検査して返す。"""
    content = read_stable(marker, 66, "Completion marker")
    if not re.fullmatch(rb"[a-f0-9]{64}\r?\n", content):
        raise ValueError("Invalid completion marker.")
    return content[:64].decode("ascii")


def source_bytes(path):
    """受入上限を超えずに原本を読み取る。"""
    return read_stable(path, MAX_BYTES, "Landing file")


def ready_files(directory, *, lane="all"):
    """完了印を照合する前に受付経路を分け、該当する原本を返す。"""
    if lane not in {"all", "business", "sensor"}:
        raise ValueError("Unknown landing lane.")
    for marker in sorted(directory.rglob("*.ready")):
        path = marker.with_name(marker.name[:-6])
        relative = path.relative_to(directory).as_posix()
        sensor_path = relative.startswith("sensor/")
        if lane != "all" and sensor_path != (lane == "sensor"):
            continue
        if marker.is_symlink() or path.is_symlink() or not path.is_file():
            continue
        expected = completion_hash(marker)
        data = source_bytes(path)
        digest = sha256(data).hexdigest()
        if digest != expected:
            raise ValueError(
                "Landing file does not match its completion marker."
            )
        yield RawFile(path, relative, data, digest)

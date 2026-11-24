"""物理tarを検査し、空の復元ボリュームへ通常のファイルだけを展開する。"""

import hashlib
import stat
import sys
import tarfile
from pathlib import Path, PurePosixPath

MAX_MEMBERS = 250_000


class RestoreTarError(ValueError):
    """元ファイル名を表示しない復元エラー。"""


def check_member(member, paths):
    """不正なパスやリンクを拒否し、通常の所有者・権限情報を保つ。"""
    name = member.name
    path = PurePosixPath(name)
    if (
        not name
        or name.startswith("/")
        or "\\" in name
        or "\0" in name
        or ".." in path.parts
        or (not path.parts and not member.isdir())
        or len(name) > 4096
        or len(path.parts) > 64
        or (path in paths and (paths[path] is not None or not member.isdir()))
        or not (
            member.isdir()
            or member.type in (tarfile.REGTYPE, tarfile.AREGTYPE)
        )
        or member.issparse()
        or member.size < 0
        or (member.isdir() and member.size)
        or member.mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX)
        or not all(0 <= value < 2**31 for value in (member.uid, member.gid))
    ):
        raise RestoreTarError("tarに展開できない項目があります。")
    # 親ディレクトリが通常ファイルに置き換わる場合も拒否する。
    for parent in path.parents:
        if paths.get(parent) is False:
            raise RestoreTarError("tarのディレクトリ構造が不正です。")
        paths.setdefault(parent, None)
    paths[path] = member.isdir()
    if len(paths) > MAX_MEMBERS:
        raise RestoreTarError("tarの項目数が上限を超えました。")
    return member


def audit_archive(handle, destination=None):
    """事前検査と展開で同じ項目検査を使う。リンクと特殊ファイルは通さない。"""
    seen = {}
    with tarfile.open(fileobj=handle, mode="r|", errorlevel=2) as archive:
        checked = (check_member(member, seen) for member in archive)
        if destination is None:
            for _ in checked:
                pass
        else:
            # 独自検査直後に展開し、数値UID/GIDと権限を復元する。
            # tarfileのフィルタはディレクトリに2回呼ばれるため判定を分ける。
            archive.extractall(
                destination,
                members=checked,
                numeric_owner=True,
                filter="fully_trusted",
            )
    if not seen:
        raise RestoreTarError("tarに復元する項目がありません。")


class CheckedStream:
    """Dockerに送った全バイトを、展開した側で期待値と比較する。"""

    def __init__(self, raw, expected_size):
        self.raw = raw
        self.expected_size = expected_size
        self.size = 0
        self.checksum = hashlib.sha256()

    def read(self, size):
        """期待サイズを超えて読み続けず、読取内容を記録する。"""
        data = self.raw.read(min(size, self.expected_size - self.size + 1))
        self.size += len(data)
        if self.size > self.expected_size:
            raise RestoreTarError("復元したtarのサイズが一致しません。")
        self.checksum.update(data)
        return data


def main():
    """ネットワークなしの補助コンテナ内だけで実行する。"""
    destination = Path("/restore")
    if any(destination.iterdir()):
        raise RestoreTarError("復元先ボリュームは空である必要があります。")
    size, expected = sys.argv[1:]
    stream = CheckedStream(sys.stdin.buffer, int(size))
    audit_archive(stream, destination)
    # tarの終端マーカーの後に続くパディングも、原本のハッシュに含める。
    while stream.read(1024 * 1024):
        pass
    if stream.size != int(size) or stream.checksum.hexdigest() != expected:
        raise RestoreTarError("復元したtarのSHA-256が一致しません。")


if __name__ == "__main__":
    main()

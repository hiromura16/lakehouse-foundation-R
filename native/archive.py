"""大きな検査素材を、固定した原本の版としてS3と台帳へ確定する。"""

import base64
import os
import re
from hashlib import sha256
from pathlib import Path

from scripts.raw_file_input import completion_hash, opened_regular

DIRECTORY = "large-raw"
MARKER_SUFFIX = ".sealed"
CHUNK_BYTES = 1024 * 1024
MAX_BYTES = 256 * CHUNK_BYTES
FILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,126}")


class ArchiveInputError(ValueError):
    """利用者へ返せる一定の受付エラー。原本名や内容は含めない。"""


def verify_name(name):
    """隣接完了印のファイル名を含め、予約した単一ディレクトリに限定する。"""
    if (
        not isinstance(name, str)
        or not FILE_NAME.fullmatch(name)
        or name.endswith((".sealed", ".ready"))
    ):
        raise ValueError("ARCHIVE_FILENAME_INVALID")


def original_hash(stream, size):
    """検査時のサイズ内だけを分割して読み、増加・欠損・置換を検知する。"""
    digest = sha256()
    remaining = size
    while remaining:
        chunk = stream.read(min(remaining, CHUNK_BYTES))
        if not chunk:
            raise ValueError("ARCHIVE_SIZE_CHANGED")
        digest.update(chunk)
        remaining -= len(chunk)
    if stream.read(1):
        raise ValueError("ARCHIVE_SIZE_CHANGED")
    return digest.digest()


def put_verified(s3, bucket, key, stream, size, digest):
    """S3にSHA-256を検証させ、同じ原本の搬送と上書きを避ける。"""
    from botocore.exceptions import ClientError

    checksum = base64.b64encode(digest).decode("ascii")

    def check_storage():
        """保存先が示す条件を照合し、不在のときだけ新規作成を許す。"""
        try:
            stored = s3.head_object(
                Bucket=bucket, Key=key, ChecksumMode="ENABLED"
            )
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            raise
        # 対応しないS3互換実装や、原本が異なる場合は台帳更新を拒否する。
        if (
            stored["ContentLength"] != size
            or stored.get("ChecksumSHA256") != checksum
        ):
            raise RuntimeError("ARCHIVE_STORAGE_CHECKSUM_MISMATCH")
        return True

    if check_storage():
        return False
    stream.seek(0)
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=stream,
            ContentLength=size,
            ContentType="application/octet-stream",
            IfNoneMatch="*",
            ChecksumSHA256=checksum,
        )
        created = True
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
            raise
        # 最初のHEADが不在でも、その後ほかの操作で作成された場合がある。
        created = False
    if not check_storage():
        raise RuntimeError("ARCHIVE_STORAGE_OBJECT_MISSING")
    return created


def receive(core, name, *, landing=Path("/landing")):
    """照合したバイト列を保全し、台帳に同じ版がなければ最後に確定する。"""
    from pyiceberg.expressions import EqualTo

    verify_name(name)
    path = f"{DIRECTORY}/{name}"
    bucket = core.os.environ["S3_BUCKET_RAW"]
    # ディレクトリfd基準で開くため、個別ファイルを参照する途中での
    # 親ディレクトリ差替えや末端のシンボリックリンクを辿らない。
    try:
        folder = os.open(landing / DIRECTORY, os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            expected = completion_hash(name + MARKER_SUFFIX, dir_fd=folder)
            with opened_regular(
                name, MAX_BYTES, "Archive file", dir_fd=folder
            ) as (stream, size):
                digest = original_hash(stream, size)
                if digest.hex() != expected:
                    raise ArchiveInputError("ARCHIVE_COMPLETION_HASH_MISMATCH")
                key = f"archive/{expected}/{sha256(path.encode()).hexdigest()}"
                created = put_verified(
                    core.S3, bucket, key, stream, size, digest
                )
        finally:
            os.close(folder)
    except ArchiveInputError:
        raise
    except (OSError, ValueError):
        raise ArchiveInputError("ARCHIVE_LOCAL_INPUT_INVALID") from None

    table = core.catalog().load_table("bronze.files")
    previous = (
        table.scan(
            row_filter=EqualTo("path", path),
            selected_fields=("sha256",),
            limit=1,
        )
        .to_arrow()
        .to_pylist()
    )
    registered = not previous or previous[0]["sha256"] != expected
    if registered:
        row = {
            "path": path,
            "sha256": expected,
            "s3_uri": f"s3://{bucket}/{key}",
            "mime": "application/octet-stream",
            "bytes": str(size),
        }
        core.overwrite(table, [row], "path", path)
        core.STATE["imported_files"] += 1
    return {
        "file": name,
        "bytes": size,
        "sha256": expected,
        "object_created": created,
        "registered": registered,
    }

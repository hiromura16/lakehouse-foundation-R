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
# 単一PUTを使う境界。設定した受付上限がこれより低い場合もある。
MAX_BYTES = 256 * CHUNK_BYTES
HARD_MAX_BYTES = 2048 * CHUNK_BYTES
PART_BYTES = 16 * CHUNK_BYTES
LIMIT_ENV = "LAKEHOUSE_ARCHIVE_MAX_MIB"
COMPOSITE = "COMPOSITE"
FILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,126}")


class ArchiveInputError(ValueError):
    """利用者へ返せる一定の受付エラー。原本名や内容は含めない。"""


def configured_limit(environment):
    """配備時に指定するMiB上限を検証し、未設定なら256 MiBを使う。"""
    value = environment.get(LIMIT_ENV, "256")
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[1-9][0-9]{0,3}", value)
        or int(value) * CHUNK_BYTES > HARD_MAX_BYTES
    ):
        raise ValueError("ARCHIVE_LIMIT_INVALID")
    return int(value) * CHUNK_BYTES


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


def read_part(stream, size):
    """パート境界を変えずに読み、短い読み取りは原本の変化として拒否する。"""
    body = stream.read(size)
    if len(body) != size:
        raise ValueError("ARCHIVE_SIZE_CHANGED")
    return body


def original_checksums(stream, size):
    """大容量の場合は全体SHAと、固定幅で分けた各パートSHAを採取する。"""
    if size <= MAX_BYTES:
        return original_hash(stream, size), ()
    digest = sha256()
    parts = []
    for start in range(0, size, PART_BYTES):
        body = read_part(stream, min(PART_BYTES, size - start))
        digest.update(body)
        parts.append(sha256(body).digest())
        del body
    if stream.read(1):
        raise ValueError("ARCHIVE_SIZE_CHANGED")
    return digest.digest(), tuple(parts)


def as_base64(digest):
    """S3が使用する、SHA-256のBase64表現にそろえる。"""
    return base64.b64encode(digest).decode("ascii")


def stored_matches(s3, bucket, key, size, checksum, *, multipart=False):
    """不在ならFalse、存在するなら保存長と要求したSHA方式を照合する。"""
    from botocore.exceptions import ClientError

    try:
        stored = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return False
        raise
    # 対応しない互換実装や、原本が異なる場合は台帳更新を拒否する。
    if (
        stored["ContentLength"] != size
        or stored.get("ChecksumSHA256") != checksum
        or (multipart and stored.get("ChecksumType") != COMPOSITE)
    ):
        raise RuntimeError("ARCHIVE_STORAGE_CHECKSUM_MISMATCH")
    return True


def put_verified(s3, bucket, key, stream, size, digest):
    """S3にSHA-256を検証させ、同じ原本の搬送と上書きを避ける。"""
    from botocore.exceptions import ClientError

    checksum = as_base64(digest)
    if stored_matches(s3, bucket, key, size, checksum):
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
    if not stored_matches(s3, bucket, key, size, checksum):
        raise RuntimeError("ARCHIVE_STORAGE_OBJECT_MISSING")
    return created


def abort_upload(s3, upload):
    """当該要求で取得したupload IDだけを中断し、完了済みオブジェクトを保つ。"""
    from botocore.exceptions import BotoCoreError, ClientError

    try:
        s3.abort_multipart_upload(**upload)
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchUpload":
            raise RuntimeError("ARCHIVE_MULTIPART_ABORT_FAILED") from None
    except BotoCoreError:
        raise RuntimeError("ARCHIVE_MULTIPART_ABORT_FAILED") from None


def upload_parts(s3, upload, stream, size, expected):
    """再読したパートを検査済みの原本に照合し、連番で順に送る。"""
    stream.seek(0)
    parts = []
    for number, digest in enumerate(expected, 1):
        length = min(PART_BYTES, size - (number - 1) * PART_BYTES)
        body = read_part(stream, length)
        if sha256(body).digest() != digest:
            raise ValueError("ARCHIVE_CONTENT_CHANGED")
        checksum = as_base64(digest)
        reply = s3.upload_part(
            **upload,
            PartNumber=number,
            Body=body,
            ContentLength=length,
            ChecksumSHA256=checksum,
        )
        del body
        etag = reply.get("ETag")
        if (
            not isinstance(etag, str)
            or not etag
            or reply.get("ChecksumSHA256", checksum) != checksum
        ):
            raise RuntimeError("ARCHIVE_MULTIPART_PART_NOT_VERIFIED")
        parts.append(
            {"PartNumber": number, "ETag": etag, "ChecksumSHA256": checksum}
        )
    if stream.read(1):
        raise ValueError("ARCHIVE_SIZE_CHANGED")
    return parts


def put_multipart_verified(s3, bucket, key, stream, size, expected):
    """条件付き確定と複合SHAのHEAD照合を行い、成功した場合だけ返す。"""
    from botocore.exceptions import BotoCoreError, ClientError

    checksum = (
        f"{as_base64(sha256(b''.join(expected)).digest())}-{len(expected)}"
    )
    if stored_matches(s3, bucket, key, size, checksum, multipart=True):
        return False
    try:
        upload_id = s3.create_multipart_upload(
            Bucket=bucket,
            Key=key,
            ContentType="application/octet-stream",
            ChecksumAlgorithm="SHA256",
            ChecksumType=COMPOSITE,
        ).get("UploadId")
    except (BotoCoreError, ClientError):
        raise RuntimeError("ARCHIVE_MULTIPART_CREATE_FAILED") from None
    if not isinstance(upload_id, str) or not upload_id:
        raise RuntimeError("ARCHIVE_MULTIPART_CREATE_FAILED")

    upload = {"Bucket": bucket, "Key": key, "UploadId": upload_id}
    created = False
    try:
        parts = upload_parts(s3, upload, stream, size, expected)
        try:
            s3.complete_multipart_upload(
                **upload,
                MultipartUpload={"Parts": parts},
                IfNoneMatch="*",
                ChecksumType=COMPOSITE,
            )
            created = True
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                raise
            # 自分のパートを中断した後、競合相手の原本をHEADで照合する。
    except (BotoCoreError, ClientError):
        raise RuntimeError("ARCHIVE_MULTIPART_UPLOAD_FAILED") from None
    finally:
        if not created:
            abort_upload(s3, upload)
    if not stored_matches(s3, bucket, key, size, checksum, multipart=True):
        raise RuntimeError("ARCHIVE_STORAGE_OBJECT_MISSING")
    return created


def receive(core, name, *, landing=Path("/landing")):
    """照合したバイト列を保全し、台帳に同じ版がなければ最後に確定する。"""
    from pyiceberg.expressions import EqualTo

    verify_name(name)
    path = f"{DIRECTORY}/{name}"
    bucket = core.os.environ["S3_BUCKET_RAW"]
    limit = configured_limit(core.os.environ)
    # ディレクトリfd基準で開くため、個別ファイルを参照する途中での
    # 親ディレクトリ差替えや末端のシンボリックリンクを辿らない。
    try:
        folder = os.open(landing / DIRECTORY, os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            expected = completion_hash(name + MARKER_SUFFIX, dir_fd=folder)
            with opened_regular(
                name, limit, "Archive file", dir_fd=folder
            ) as (stream, size):
                digest, parts = original_checksums(stream, size)
                if digest.hex() != expected:
                    raise ArchiveInputError("ARCHIVE_COMPLETION_HASH_MISMATCH")
                key = f"archive/{expected}/{sha256(path.encode()).hexdigest()}"
                if parts:
                    created = put_multipart_verified(
                        core.S3, bucket, key, stream, size, parts
                    )
                else:
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

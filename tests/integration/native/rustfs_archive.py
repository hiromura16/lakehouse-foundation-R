"""隔離S3へ、単一PUTまたはマルチパートで合成原本を保全する。"""

import argparse
import base64
import json
import os
from hashlib import sha256, shake_256
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

CHUNK_BYTES = 1024 * 1024
SIZE = 256 * CHUNK_BYTES
NAME = "rustfs-boundary.bin"
RELATIVE = f"large-raw/{NAME}"
MISSING = "rustfs-missing.bin"
TOO_LARGE = "rustfs-over-limit.bin"
DIGEST = "5eb0547d55cb7cf6ad667e2fa01cd07f262cb37e0c5d27e0f3003712a6d20f15"
RAW = "foundation-rustfs-raw"
WAREHOUSE = "foundation-rustfs"
MULTIPART_SIZE = 257 * CHUNK_BYTES
MULTIPART_DIGEST = (
    "4c49b1eae3da7273462af6a80cca3e15d38128bb07c12ada3b1ee975bd01ac2e"
)
MULTIPART_SUM = "7rrHz18YsY0OMEIMnEysNRXRha89Or70LE6KnikmLWo=-17"
MULTIPART_RAW = "foundation-archive-multipart-raw"
MULTIPART_WAREHOUSE = "foundation-archive-multipart"


def seed(landing, *, multipart=False):
    """合成原本を1 MiBずつ生成し、閉じたあとに隣接完了印を確定する。"""
    size = MULTIPART_SIZE if multipart else SIZE
    expected = MULTIPART_DIGEST if multipart else DIGEST
    folder = landing / "large-raw"
    folder.mkdir(exist_ok=True)
    digest = sha256()
    with (folder / NAME).open("xb") as output:
        for index in range(size // CHUNK_BYTES):
            label = f"foundation-r-rustfs-archive-{index}".encode()
            block = shake_256(label).digest(CHUNK_BYTES)
            output.write(block)
            digest.update(block)
    assert digest.hexdigest() == expected
    with (folder / (NAME + ".sealed")).open("x", encoding="ascii") as marker:
        marker.write(expected + "\n")
    # 上限越えはサイズ検査で確実に拒否されるため、穴あきファイルで再現する。
    with (folder / TOO_LARGE).open("xb") as output:
        output.truncate(size + 1)
    with (folder / (TOO_LARGE + ".sealed")).open(
        "x", encoding="ascii"
    ) as marker:
        marker.write(expected + "\n")


def api_archive(name):
    """試験コンテナ内の操作APIへ、既に設定されている書込み資格を使う。"""
    client = build_opener(ProxyHandler({}))
    request = Request(
        "http://127.0.0.1:8080/run",
        data=json.dumps({"action": "archive", "file": name}).encode(),
        headers={
            "Authorization": "Bearer " + os.environ["WORKER_TOKEN"],
            "Content-Type": "application/json",
        },
    )
    try:
        with client.open(request, timeout=120) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        with error:
            return error.code, json.load(error)


def reject_multipart_overwrite(s3, bucket, key, rejected):
    """実S3で不正SHAと条件付きCompleteの衝突を別々に再現する。"""
    from native.archive import abort_upload

    address = {"Bucket": bucket, "Key": key}
    upload = s3.create_multipart_upload(
        **address, ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE"
    )
    address["UploadId"] = upload["UploadId"]
    prefix = b"x" * (5 * CHUNK_BYTES)
    try:
        rejected(
            lambda: s3.upload_part(
                **address,
                PartNumber=1,
                Body=prefix,
                ChecksumSHA256=base64.b64encode(bytes(32)).decode("ascii"),
            ),
            400,
            s3_code="BadDigest",
        )
        parts = []
        for index, block in enumerate((prefix, b"different"), 1):
            checksum = base64.b64encode(sha256(block).digest()).decode("ascii")
            response = s3.upload_part(
                **address,
                PartNumber=index,
                Body=block,
                ChecksumSHA256=checksum,
            )
            parts.append(
                {
                    "ETag": response["ETag"],
                    "PartNumber": index,
                    "ChecksumSHA256": checksum,
                }
            )
        rejected(
            lambda: s3.complete_multipart_upload(
                **address,
                MultipartUpload={"Parts": parts},
                ChecksumType="COMPOSITE",
                IfNoneMatch="*",
            ),
            412,
        )
    finally:
        abort_upload(s3, address)


def verify_large_body(s3, bucket, key, expected_size, expected_hash):
    """HEADのメタデータと独立に、全バイトの長さとSHA-256を照合する。"""
    response = s3.get_object(Bucket=bucket, Key=key)
    digest = sha256()
    size = 0
    with response["Body"] as body:
        assert response["ContentLength"] == expected_size
        while block := body.read(CHUNK_BYTES):
            size += len(block)
            assert size <= expected_size
            digest.update(block)
    assert size == expected_size and digest.hexdigest() == expected_hash


def archive_evidence(stage, *, multipart=False):
    """条件付き作成と再送を照合し、再起動の前後で比較できる証拠を返す。"""
    from botocore.exceptions import ClientError
    from pyiceberg.expressions import EqualTo

    from native import archive, engine, storage

    bucket = MULTIPART_RAW if multipart else RAW
    warehouse = MULTIPART_WAREHOUSE if multipart else WAREHOUSE
    size = MULTIPART_SIZE if multipart else SIZE
    digest = MULTIPART_DIGEST if multipart else DIGEST
    if (
        os.environ.get("WAREHOUSE_NAME") != warehouse
        or os.environ.get("S3_BUCKET_RAW") != bucket
    ):
        raise RuntimeError("受入専用のwarehouseと原本バケットが必要です。")
    assert archive.configured_limit(os.environ) == size
    key = f"archive/{digest}/" + sha256(RELATIVE.encode()).hexdigest()

    def rejected(operation, status, *, s3_code=None):
        """S3の失敗をHTTPの応答コードで照合する。"""
        try:
            operation()
        except ClientError as error:
            actual = error.response["ResponseMetadata"]["HTTPStatusCode"]
            assert actual == status, actual
            if s3_code is not None:
                assert error.response["Error"]["Code"] == s3_code
        else:
            raise AssertionError("S3で拒否されるはずの操作が成功した。")

    catalog = storage.catalog()
    prior = engine.current_id(catalog.load_table("bronze.files"))
    creating = stage == "before"
    if creating:
        rejected(lambda: storage.S3.head_object(Bucket=bucket, Key=key), 404)
        wrong_checksum_key = "fixture/archive-invalid-checksum"
        rejected(
            lambda: storage.S3.put_object(
                Bucket=bucket,
                Key=wrong_checksum_key,
                Body=b"synthetic-bad-checksum",
                IfNoneMatch="*",
                ChecksumSHA256=base64.b64encode(bytes(32)).decode("ascii"),
            ),
            400,
            s3_code="BadDigest",
        )
        rejected(
            lambda: storage.S3.head_object(
                Bucket=bucket, Key=wrong_checksum_key
            ),
            404,
        )
        for name in (MISSING, TOO_LARGE):
            assert api_archive(name) == (
                422,
                {"error": "ARCHIVE_LOCAL_INPUT_INVALID"},
            )
        assert engine.current_id(catalog.load_table("bronze.files")) == prior

    result = api_archive(NAME)
    assert result == (
        200,
        {
            "file": NAME,
            "bytes": size,
            "sha256": digest,
            "object_created": creating,
            "registered": creating,
        },
    ), result
    if creating and multipart:
        reject_multipart_overwrite(storage.S3, bucket, key, rejected)
    elif creating:
        # 正しいキーに別の本文を送っても、作成専用条件の412で停止する。
        rejected(
            lambda: storage.S3.put_object(
                Bucket=bucket, Key=key, Body=b"wrong-revision", IfNoneMatch="*"
            ),
            412,
        )

    head = storage.S3.head_object(
        Bucket=bucket, Key=key, ChecksumMode="ENABLED"
    )
    assert head["ContentLength"] == size
    if multipart:
        assert head["ChecksumType"] == "COMPOSITE"
        assert head["ChecksumSHA256"] == MULTIPART_SUM
        verify_large_body(storage.S3, bucket, key, size, digest)
    else:
        assert head["ChecksumSHA256"] == base64.b64encode(
            bytes.fromhex(digest)
        ).decode("ascii")

    ledger = catalog.load_table("bronze.files")
    snapshot = engine.current_id(ledger)
    assert snapshot is not None
    assert (snapshot != prior) == creating
    rows = (
        ledger.scan(
            snapshot_id=snapshot, row_filter=EqualTo("path", RELATIVE), limit=2
        )
        .to_arrow()
        .to_pylist()
    )
    assert rows == [
        {
            "path": RELATIVE,
            "sha256": digest,
            "s3_uri": f"s3://{bucket}/{key}",
            "mime": "application/octet-stream",
            "bytes": str(size),
        }
    ]
    return {
        "bytes": size,
        "sha256": digest,
        "etag": head["ETag"],
        "ledger_snapshot": snapshot,
    }


def main():
    """ホスト側の合成原本生成と、コンテナ側の照合を分けて実行する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--multipart", action="store_true")
    commands = parser.add_subparsers(dest="operation", required=True)
    generate = commands.add_parser("seed")
    generate.add_argument("landing", type=Path)
    for stage in ("before", "after"):
        commands.add_parser(stage)
    args = parser.parse_args()
    if args.operation == "seed":
        seed(args.landing, multipart=args.multipart)
    else:
        evidence = archive_evidence(args.operation, multipart=args.multipart)
        print(json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()

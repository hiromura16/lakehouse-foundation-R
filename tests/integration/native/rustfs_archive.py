"""RustFSの専用受入で、256 MiB原本を操作APIから保全して再送する。"""

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


def seed(landing):
    """合成原本を1 MiBずつ生成し、閉じたあとに隣接完了印を確定する。"""
    folder = landing / "large-raw"
    folder.mkdir(exist_ok=True)
    digest = sha256()
    with (folder / NAME).open("xb") as output:
        for index in range(SIZE // CHUNK_BYTES):
            label = f"foundation-r-rustfs-archive-{index}".encode()
            block = shake_256(label).digest(CHUNK_BYTES)
            output.write(block)
            digest.update(block)
    assert digest.hexdigest() == DIGEST
    with (folder / (NAME + ".sealed")).open("x", encoding="ascii") as marker:
        marker.write(DIGEST + "\n")
    # 上限越えはサイズ検査で確実に拒否されるため、穴あきファイルで再現する。
    with (folder / TOO_LARGE).open("xb") as output:
        output.truncate(SIZE + 1)
    with (folder / (TOO_LARGE + ".sealed")).open(
        "x", encoding="ascii"
    ) as marker:
        marker.write(DIGEST + "\n")


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


def archive_evidence(stage):
    """条件付き作成と再送を照合し、再起動の前後で比較できる証拠を返す。"""
    from botocore.exceptions import ClientError
    from pyiceberg.expressions import EqualTo

    from native import archive, engine, storage

    if (
        os.environ.get("WAREHOUSE_NAME") != WAREHOUSE
        or os.environ.get("S3_BUCKET_RAW") != RAW
    ):
        raise RuntimeError(
            "RustFS専用受入のwarehouseと原本バケットが必要です。"
        )
    assert archive.MAX_BYTES == SIZE
    key = f"archive/{DIGEST}/" + sha256(RELATIVE.encode()).hexdigest()

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
        rejected(lambda: storage.S3.head_object(Bucket=RAW, Key=key), 404)
        wrong_checksum_key = "fixture/archive-invalid-checksum"
        rejected(
            lambda: storage.S3.put_object(
                Bucket=RAW,
                Key=wrong_checksum_key,
                Body=b"synthetic-bad-checksum",
                IfNoneMatch="*",
                ChecksumSHA256=base64.b64encode(bytes(32)).decode("ascii"),
            ),
            400,
            s3_code="BadDigest",
        )
        rejected(
            lambda: storage.S3.head_object(Bucket=RAW, Key=wrong_checksum_key),
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
            "bytes": SIZE,
            "sha256": DIGEST,
            "object_created": creating,
            "registered": creating,
        },
    ), result
    if creating:
        # 正しいキーに別の本文を送っても、作成専用条件の412で停止する。
        rejected(
            lambda: storage.S3.put_object(
                Bucket=RAW, Key=key, Body=b"wrong-revision", IfNoneMatch="*"
            ),
            412,
        )

    head = storage.S3.head_object(Bucket=RAW, Key=key, ChecksumMode="ENABLED")
    assert head["ContentLength"] == SIZE
    assert head["ChecksumSHA256"] == base64.b64encode(
        bytes.fromhex(DIGEST)
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
            "sha256": DIGEST,
            "s3_uri": f"s3://{RAW}/{key}",
            "mime": "application/octet-stream",
            "bytes": str(SIZE),
        }
    ]
    return {
        "bytes": SIZE,
        "sha256": DIGEST,
        "etag": head["ETag"],
        "ledger_snapshot": snapshot,
    }


def main():
    """ホスト側の合成原本生成と、コンテナ側の照合を分けて実行する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    generate = commands.add_parser("seed")
    generate.add_argument("landing", type=Path)
    for stage in ("before", "after"):
        commands.add_parser(stage)
    args = parser.parse_args()
    if args.operation == "seed":
        seed(args.landing)
    else:
        print(json.dumps(archive_evidence(args.operation), sort_keys=True))


if __name__ == "__main__":
    main()

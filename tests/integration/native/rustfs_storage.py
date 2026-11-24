"""RustFSのS3認証と原本の条件付き書込みを、専用バケットで検査する。"""

import json
import os
import secrets
from contextlib import closing
from urllib.request import ProxyHandler, build_opener

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from native import storage


def check_rejected(operation, allowed_codes):
    """期待したS3ステータスだけを正常な拒否として記録する。"""
    try:
        operation()
    except ClientError as error:
        code = error.response["ResponseMetadata"]["HTTPStatusCode"]
        assert code in allowed_codes, (code, error.response["Error"]["Code"])
        return code
    raise AssertionError("拒否が必要なS3操作が成功した。")


def main():
    """専用warehouseからだけ、合成バケットに条件付き書込みを行う。"""
    if os.environ.get("WAREHOUSE_NAME") != "foundation-rustfs":
        raise RuntimeError("RustFSの隔離試験warehouseが必要です。")
    bucket = "foundation-rustfs-preconditions"
    key = "fixture/raw-samples"
    initial = b"synthetic-A"
    updated = b"synthetic-B"
    unexpected = b"wrong-write"
    endpoint = storage.STORAGE_ORIGIN
    http = build_opener(ProxyHandler({}))
    with http.open(endpoint + "/health/ready", timeout=3) as response:
        assert response.status == 200

    storage.S3.create_bucket(Bucket=bucket)
    created = storage.S3.put_object(
        Bucket=bucket, Key=key, Body=initial, IfNoneMatch="*"
    )
    etag_a = created["ETag"]
    collision = check_rejected(
        lambda: storage.S3.put_object(
            Bucket=bucket, Key=key, Body=unexpected, IfNoneMatch="*"
        ),
        {412},
    )
    with closing(
        storage.S3.get_object(Bucket=bucket, Key=key, IfMatch=etag_a)["Body"]
    ) as body:
        assert body.read() == initial
    etag_deny = check_rejected(
        lambda: storage.S3.put_object(
            Bucket=bucket, Key=key, Body=unexpected, IfMatch='"incorrect-etag"'
        ),
        {412},
    )
    modified = storage.S3.put_object(
        Bucket=bucket, Key=key, Body=updated, IfMatch=etag_a
    )
    etag_b = modified["ETag"]
    assert etag_b != etag_a
    with closing(
        storage.S3.get_object(Bucket=bucket, Key=key, IfMatch=etag_b)["Body"]
    ) as body:
        assert body.read() == updated

    options = dict(
        endpoint_url=endpoint,
        region_name="us-east-1",
    )
    anonymous = boto3.client(
        "s3",
        **options,
        config=Config(
            signature_version=UNSIGNED,
            s3={"addressing_style": "path"},
            proxies={},
        ),
    )
    invalid = boto3.client(
        "s3",
        **options,
        config=Config(s3={"addressing_style": "path"}, proxies={}),
        aws_access_key_id=secrets.token_hex(10),
        aws_secret_access_key=secrets.token_hex(24),
    )
    anonymous_read = check_rejected(
        lambda: anonymous.get_object(Bucket=bucket, Key=key), {403}
    )
    invalid_read = check_rejected(
        lambda: invalid.get_object(Bucket=bucket, Key=key), {403}
    )
    invalid_write = check_rejected(
        lambda: invalid.put_object(Bucket=bucket, Key=key, Body=unexpected),
        {403},
    )
    with closing(
        storage.S3.get_object(Bucket=bucket, Key=key)["Body"]
    ) as body:
        assert body.read() == updated

    print(
        json.dumps(
            {
                "ready": True,
                "anonymous_read_status": anonymous_read,
                "invalid_read_status": invalid_read,
                "invalid_write_status": invalid_write,
                "if_none_match_collision": collision,
                "if_match_wrong_etag": etag_deny,
                "if_match_correct_etag_updated": True,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

"""承認済みの成果物をCloudflare R2へ作成専用で転送する。"""

import argparse
import json
import os
import re
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from scripts.raw_file_input import MAX_BYTES, read_stable, source_bytes

TOTAL_MAX_BYTES = 64 * 1024 * 1024
MIME_TYPES = {
    ".csv": "text/csv",
    ".json": "application/json",
    ".parquet": "application/vnd.apache.parquet",
}
DATASETS = {
    "factory_daily_production",
    "sales_fulfillment",
    "sensor_latest",
    "sensor_minute",
}


@dataclass(frozen=True)
class Artifact:
    """承認ファイル名、型、ハッシュと検査済みの内容を保持する。"""

    name: str
    mime: str
    digest: str
    content: bytes


def unique_fields(pairs):
    """承認された項目を一意に読み、同名の指定による曖昧な承認を拒む。"""
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("R2_MANIFEST_DUPLICATE_FIELD")
        result[name] = value
    return result


def load_bundle(manifest):
    """承認対象のパス、分類、サイズとハッシュをすべて送信前に検査する。"""
    metadata = read_stable(manifest, 32768, "Approved manifest")
    document = json.loads(metadata, object_pairs_hook=unique_fields)
    if (
        type(document["format_version"]) is not int
        or document["format_version"] != 1
        or document["classification"] != "approved_external"
        or document["dataset"] not in DATASETS
        or not re.fullmatch(r"[1-9][0-9]{0,18}", document["release"])
        or not 1 <= len(document["files"]) <= 16
    ):
        raise ValueError("R2_MANIFEST_INVALID")
    artifacts = []
    used_names = {"manifest.json"}
    total_bytes = 0
    for entry in document["files"]:
        name = entry["name"]
        size = entry["size_bytes"]
        if (
            not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name)
            or name in used_names
            or type(size) is not int
            or not 1 <= size <= MAX_BYTES
            or Path(name).suffix not in MIME_TYPES
        ):
            raise ValueError("R2_ARTIFACT_INVALID")
        total_bytes += size
        if total_bytes > TOTAL_MAX_BYTES:
            raise ValueError("R2_BUNDLE_SIZE_LIMIT")
        used_names.add(name)
        payload = source_bytes(manifest.parent / name)
        digest = sha256(payload).hexdigest()
        if len(payload) != size or digest != entry["sha256"]:
            raise ValueError("R2_ARTIFACT_HASH_MISMATCH")
        artifacts.append(
            Artifact(name, MIME_TYPES[Path(name).suffix], digest, payload)
        )
    prefix = "/".join(
        [
            "exports",
            document["dataset"],
            document["release"],
            sha256(metadata).hexdigest(),
        ]
    )
    return (
        prefix,
        artifacts,
        Artifact(
            "manifest.json",
            "application/json",
            sha256(metadata).hexdigest(),
            metadata,
        ),
    )


def create_only(client, bucket, key, artifact):
    """既存の成果物を変更せず、再送時は内容そのもののSHA-256を照合する。"""
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=artifact.content,
            ContentType=artifact.mime,
            IfNoneMatch="*",
            Metadata={"sha256": artifact.digest},
        )
    except ClientError as error:
        status = error.response["ResponseMetadata"]["HTTPStatusCode"]
        code = error.response.get("Error", {}).get("Code")
        # R2のバケットロックは、既存キーの作成条件より先に判定される。
        if status != 412 and (status, code) != (
            409,
            "ObjectLockedByBucketPolicy",
        ):
            raise
        response = client.get_object(Bucket=bucket, Key=key)
        with response["Body"] as body:
            saved = body.read(len(artifact.content) + 1)
        if (
            len(saved) != len(artifact.content)
            or sha256(saved).hexdigest() != artifact.digest
        ):
            raise ValueError("R2_EXISTING_OBJECT_MISMATCH") from None


def upload(client, bucket, prefix, artifacts, manifest):
    """成果物がすべて確定した後だけ、同じ送り状を公開する。"""
    for artifact in artifacts:
        create_only(client, bucket, f"{prefix}/{artifact.name}", artifact)
    create_only(client, bucket, f"{prefix}/{manifest.name}", manifest)


def r2_client(account):
    """明示したR2アカウント専用のSDKクライアントを生成する。"""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=f"https://{account}.r2.cloudflarestorage.com",
        region_name="auto",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ.get("R2_SESSION_TOKEN"),
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=8,
            read_timeout=45,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


def main():
    """送信先を明示した場合だけ、検証済み成果物を外部へ転送する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--account")
    parser.add_argument("--bucket")
    options = parser.parse_args()
    try:
        prefix, artifacts, manifest = load_bundle(options.manifest)
        if options.publish:
            if (
                not options.account
                or not re.fullmatch(r"[0-9a-f]{32}", options.account)
                or not options.bucket
                or not re.fullmatch(
                    r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", options.bucket
                )
            ):
                raise ValueError("R2_DESTINATION_INVALID")
            upload(
                r2_client(options.account),
                options.bucket,
                prefix,
                artifacts,
                manifest,
            )
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        BotoCoreError,
        ClientError,
    ) as error:
        parser.exit(1, f"成果物を確定できません（{type(error).__name__}）。\n")
    print(
        json.dumps(
            {
                "published": options.publish,
                "files": len(artifacts),
                "manifest_sha256": manifest.digest,
            }
        )
    )


if __name__ == "__main__":
    main()

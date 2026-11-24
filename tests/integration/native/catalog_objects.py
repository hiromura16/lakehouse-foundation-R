"""隔離した合成環境で、Icebergの原本欠落と同名異サイズを検出する。"""

import json
import os
import subprocess
import sys
from hashlib import sha256
from urllib.parse import urlsplit

from native import api, engine


def audit(*args):
    """Dokploy Terminalで使う監査CLIを動かし、終了値とJSONを返す。"""
    result = subprocess.run(
        [sys.executable, "-m", "native.catalog_audit", "--objects", *args],
        check=False,
        capture_output=True,
        timeout=45,
    )
    return result.returncode, json.loads(result.stdout), result.stdout


def main():
    """Goldの固定版を読み、不在→異サイズ→原状復帰を順に照合する。"""
    if os.environ.get("FOUNDATION_R_SYNTHETIC_OBJECT_TEST") != "1":
        raise ValueError("使い捨ての合成環境でのみ実行できます。")
    core = api.core_module()
    catalog = core.catalog()
    release = engine.latest_release(core, catalog)
    assert release["row_counts"]["gold.factory_daily_production"] > 0
    release_id = release["release_snapshot_id"]
    target = "gold.factory_daily_production"
    table = catalog.load_table(target)
    scan = table.scan(snapshot_id=release["outputs"][target])
    task = next(iter(scan.plan_files()))
    filename = task.file.file_path
    uri = urlsplit(filename)
    bucket = os.environ["S3_BUCKET_WAREHOUSE"]
    assert uri.scheme == "s3" and uri.netloc == bucket
    location = {"Bucket": bucket, "Key": uri.path.removeprefix("/")}
    original = core.S3.get_object(**location)
    with original["Body"] as stream:
        payload = stream.read(1024 * 1024 + 1)
    assert len(payload) == task.file.file_size_in_bytes <= 1024 * 1024
    expected_hash = sha256(filename.encode()).hexdigest()
    arguments = ("--release", release_id)

    code, before, _ = audit(*arguments)
    assert code == 0, before["storage"]
    assert before["storage"]["all_objects_available"]
    assert before["storage"]["object_counts"]["data"] >= 6
    assert before["storage"]["object_counts"]["manifest"] >= 6
    code, capped, _ = audit(*arguments, "--max-objects", "1")
    assert code == 1
    assert capped["storage"]["unique_objects_examined"] == 1
    assert capped["storage"]["issue_counts"] == {"OBJECT_LIMIT": 1}
    assert not capped["storage"]["enumeration_complete"]

    # このテストで作った1ファイルだけを一時的に置き換え、必ず同じ原本へ戻す。
    try:
        core.S3.delete_object(**location)
        for expected in ("NOT_FOUND", "SIZE_MISMATCH"):
            code, evidence, raw = audit(*arguments)
            assert evidence["all_metadata_available"]
            assert code == 1
            assert evidence["storage"]["enumeration_complete"]
            assert evidence["storage"]["issue_counts"] == {expected: 1}
            assert evidence["storage"]["sample_issues"] == [
                {
                    "table": target,
                    "kind": "data",
                    "code": expected,
                    "location_sha256": expected_hash,
                }
            ]
            assert filename.encode() not in raw
            if expected == "NOT_FOUND":
                core.S3.put_object(**location, Body=b"fixture")
    finally:
        core.S3.put_object(**location, Body=payload)
    code, after, _ = audit(*arguments)
    assert code == 0 and after == before
    summary = before["storage"]["unique_objects_examined"]
    print(
        f"公開世代のS3 {summary}件、欠落・異サイズ・復旧と上限を照合しました。"
    )


if __name__ == "__main__":
    main()

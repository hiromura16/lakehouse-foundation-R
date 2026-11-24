"""SQLiteの3観測を実Icebergに反映し、削除と公開世代を照合する。"""

import json
import os
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from urllib.request import ProxyHandler, Request, build_opener

from native import api

OPEN = build_opener(ProxyHandler({})).open
ORIGIN = "http://127.0.0.1:8080"


def apply(job):
    """DokployのTerminalで使えるCLIを動かし、確定IDを読む。"""
    child = subprocess.run(
        [
            sys.executable,
            "-m",
            "native.api",
            "sql-load",
            "--batch-id",
            job["batch_id"],
            "--observed-at",
            job["observed_at"],
        ],
        capture_output=True,
        timeout=75,
        check=True,
    )
    value = json.loads(child.stdout)
    assert value["completed"] and value["batch_id"] == job["batch_id"]
    return value


def gold(dataset, release=""):
    """読取用の資格情報で、SQLバッチのGoldと公開世代を取得する。"""
    suffix = f"?release={release}" if release else ""
    url = f"{ORIGIN}/v1/sql/gold/{dataset}{suffix}"
    headers = {"Authorization": "Bearer " + os.environ["READER_TOKEN"]}
    with OPEN(Request(url, headers=headers), timeout=30) as reply:
        value = json.load(reply)
    assert value["total_rows"] == 1 and not value["truncated"]
    return value


def sql_status():
    """実コンテナの端末用CLIから、読取専用の観測結果を受け取る。"""
    child = subprocess.run(
        [sys.executable, "-m", "native.sql_status"],
        capture_output=True,
        timeout=45,
        check=True,
    )
    return json.loads(child.stdout)


def main():
    """A→B→Aの業務値、同一観測の再送、原本SHAを見比べる。"""
    jobs = json.loads(Path("/landing/.sqlite-ci-jobs.json").read_text())
    assert sql_status() == {
        "last_completed": None,
        "apply": None,
        "published": None,
    }
    releases = []
    for job, (good, shipped) in zip(
        jobs, ((8, 3), (9, 0), (8, 3)), strict=True
    ):
        applied = apply(job)
        assert not applied["skipped"]
        assert len(applied["counts"]) == 6
        assert applied["counts"]["erp_shipments"] == int(shipped != 0)
        prod = gold("factory_daily_production")
        sales = gold("sales_fulfillment")
        assert prod["rows"][0]["good_qty"] == good
        order = sales["rows"][0]
        assert order["ordered_qty"] == 10 and order["shipped_qty"] == shipped
        assert sales["release_snapshot_id"] == prod["release_snapshot_id"]
        releases.append(prod["release_snapshot_id"])
        status = sql_status()
        assert status["apply"]["status"] == "idle"
        assert status["apply"]["apply_id"] == applied["apply_id"]
        assert status["last_completed"]["apply_id"] == applied["apply_id"]
        assert status["last_completed"]["counts"]["erp_shipments"] == int(
            shipped != 0
        )
        assert status["published"]["release_snapshot_id"] == releases[-1]
        assert (
            status["published"]["gold_rows"]["factory_daily_production"] == 1
        )

        replay = apply(job)
        assert replay["skipped"] and replay["apply_id"] == applied["apply_id"]
        assert gold("factory_daily_production") == prod

    assert len(set(releases)) == 3
    for release, good in zip(releases, (8, 9, 8), strict=True):
        assert (
            gold("factory_daily_production", release)["rows"][0]["good_qty"]
            == good
        )

    core = api.core_module()
    for batch_id in {job["batch_id"] for job in jobs}:
        obj = core.S3.get_object(
            Bucket=os.environ["S3_BUCKET_RAW"],
            Key=f"sql-batches/{batch_id}.json",
        )
        with obj["Body"] as body:
            assert (
                sha256(body.read(16 * 1024 * 1024 + 1)).hexdigest() == batch_id
            )
    print(
        "SQLiteの3公開世代、削除、再送、端末用の状態表示とRaw原本を照合しました。"
    )


if __name__ == "__main__":
    main()

"""実Lakehouseの内部APIでSQL Gold公開と同じ観測の再送を検査する。"""

import json
import os
from pathlib import Path
from urllib.request import ProxyHandler, Request, build_opener

ORIGIN = "http://127.0.0.1:8080"
OPEN = build_opener(ProxyHandler({})).open


def request(path, job=None):
    """書込みと読取りのトークンを、内部APIのそれぞれの操作に使う。"""
    token = os.environ["WORKER_TOKEN" if job else "READER_TOKEN"]
    headers = {"Authorization": "Bearer " + token}
    body = None
    if job is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(job).encode()
    with OPEN(
        Request(ORIGIN + path, headers=headers, data=body), timeout=60
    ) as reply:
        return json.load(reply)


def main():
    """反映時の業務結果と、再送後の公開世代の両方を確認する。"""
    job = json.loads(Path("/landing/.postgresql-ci-job.json").read_text())
    done = request("/run", job)
    assert done["completed"] and not done["skipped"]
    assert len(done["counts"]) == 6 and set(done["counts"].values()) == {1}
    assert done["batch_id"] == job["batch_id"] and done["mode"] == "snapshot"

    production_path = "/v1/sql/gold/factory_daily_production"
    production = request(production_path)
    assert production["table"] == "sql_gold.factory_daily_production"
    assert production["total_rows"] == 1 and production["release_snapshot_id"]
    assert production["rows"][0]["good_qty"] == 8

    sales = request("/v1/sql/gold/sales_fulfillment")
    assert (
        sales["table"] == "sql_gold.sales_fulfillment"
        and sales["total_rows"] == 1
    )
    order = sales["rows"][0]
    assert order["ordered_qty"] == 10 and order["shipped_qty"] == 3

    replay = request("/run", job)
    assert replay["completed"] and replay["skipped"]
    assert replay["apply_id"] == done["apply_id"]
    assert request(production_path) == production
    print(
        "実IcebergのSQL Goldを照合し、同じ観測の再送で公開世代を保持しました。"
    )


if __name__ == "__main__":
    main()

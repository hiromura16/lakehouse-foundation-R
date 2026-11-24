"""停止点の前後でRawとGoldの内容を比較する合成環境用の観測。"""

import argparse
import json
import os
from hashlib import sha256
from urllib.parse import urlsplit
from uuid import UUID

from native import api, engine


def digest(value):
    """元の行を表示せず、日付と小数の表現をそろえて内容を照合する。"""
    encoded = json.dumps(
        value, default=api.json_value, sort_keys=True, separators=(",", ":")
    )
    return sha256(encoded.encode()).hexdigest()


def evidence(*, files_only=False, physical=False):
    """値と公開世代を読み取り、復元で比較する粒度に絞って返す。"""
    core = api.core_module()
    catalog = core.catalog()
    gold = engine.gold_rows(core, "factory_daily_production", 200)
    assert gold["total_rows"] == 14
    assert not gold["truncated"]
    bronze = catalog.load_table("bronze.files").scan(limit=64)
    files = bronze.to_arrow().to_pylist()
    assert 6 <= len(files) < 64
    expected = {}
    for record in files:
        location = urlsplit(record["s3_uri"])
        result = core.S3.get_object(
            Bucket=location.netloc, Key=location.path.lstrip("/")
        )
        with result["Body"] as body:
            actual = sha256(body.read(16 * 1024 * 1024 + 1)).hexdigest()
        assert actual == record["sha256"]
        expected[record["path"]] = actual
    report = {
        "file_count": len(files),
        "raw_file_digest": digest(expected),
        "gold_release": gold["release_snapshot_id"],
        "gold_snapshot": gold["snapshot_id"],
        "gold_count": gold["total_rows"],
        "gold_digest": digest(gold["rows"]),
    }
    if physical:
        warehouses = core.request(
            "GET", core.CATALOG_ORIGIN + "/management/v1/warehouse"
        )
        (warehouse,) = [
            item
            for item in warehouses["warehouses"]
            if item.get("name", item.get("warehouse-name"))
            == os.environ["WAREHOUSE_NAME"]
        ]
        warehouse_id = UUID(warehouse.get("warehouse-id") or warehouse["id"])
        # 実ファイルが同じ場所に残り、既存の公開世代から読めることを比較する。
        products = {}
        for name in ("factory_daily_production", "sales_fulfillment"):
            product = engine.gold_rows(core, name, 200)
            assert product["total_rows"] > 0 and not product["truncated"]
            assert (
                product["release_snapshot_id"] == gold["release_snapshot_id"]
            )
            table = catalog.load_table(product["table"])
            products[name] = {
                "table_uuid": str(table.metadata.table_uuid),
                "metadata_location": table.metadata_location,
                "snapshot": product["snapshot_id"],
                "rows": product["total_rows"],
                "digest": digest(product["rows"]),
            }
        return {
            **report,
            "warehouse_id": str(warehouse_id),
            "products": products,
        }
    if files_only:
        return report
    cdc = catalog.load_table("cdc_events.events").scan(
        selected_fields=("event_id", "offset", "key_json", "envelope_json"),
        limit=10,
    )
    events = sorted(cdc.to_arrow().to_pylist(), key=lambda row: row["offset"])
    assert [int(item["offset"]) for item in events] == list(range(9))
    return {
        **report,
        "cdc_events": len(events),
        "cdc_event_digest": digest(events),
        "cdc_release": engine.latest_release(core, catalog, "cdc_")[
            "release_snapshot_id"
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files-only", action="store_true")
    parser.add_argument("--physical", action="store_true")
    args = parser.parse_args()
    report = evidence(files_only=args.files_only, physical=args.physical)
    print(json.dumps(report, sort_keys=True))

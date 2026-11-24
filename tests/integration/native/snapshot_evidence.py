"""停止点の前後でRawとGoldの内容を比較する合成環境用の観測。"""

import argparse
import json
from hashlib import sha256
from urllib.parse import urlsplit

from native import api, engine


def digest(value):
    """元の行を表示せず、日付と小数の表現をそろえて内容を照合する。"""
    encoded = json.dumps(
        value, default=api.json_value, sort_keys=True, separators=(",", ":")
    )
    return sha256(encoded.encode()).hexdigest()


def evidence(*, files_only=False):
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
    args = parser.parse_args()
    print(json.dumps(evidence(files_only=args.files_only), sort_keys=True))

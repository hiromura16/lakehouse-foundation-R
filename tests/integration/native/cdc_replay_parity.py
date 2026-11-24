"""同じ合成Kafkaの確定済みRawを隔離再生し、公開結果を照合する。"""

import argparse
import json
import sqlite3
import sys
import time
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory

from native import api, engine
from native import cdc_shadow as shadow

GOLD = {
    "gold.factory_daily_production": {
        "factory_id": "CDC-F1",
        "planned_qty": 20,
        "good_qty": 12,
        "reject_qty": 2,
        "downtime_minutes": 15.0,
    },
    "gold.sales_fulfillment": {
        "sales_order_id": "CDC-S1",
        "ordered_qty": 20,
        "shipped_qty": 7,
        "open_qty": 13,
        "late_delivered_qty": 7,
        "late_shipment_count": 1,
    },
}


def source_config(settings, mapping, work):
    """稼働設定から再生に必要なソース定義だけを分離する。"""
    config = shadow.parse(shadow.read_bounded(settings, shadow.CONFIG_LIMIT))
    source_only = work / "source.json"
    shadow.write_new(
        source_only,
        shadow.encoded(
            {
                "source": {
                    key: value
                    for key, value in config["source"].items()
                    if key in shadow.SOURCE_KEYS
                },
                "topic_has_database": config["topic_has_database"],
            }
        ),
    )
    return shadow.source_from_files(source_only, mapping), config


def positions(path, source_sha256):
    """SQLiteの確定位置を、ソース識別値を検証してから読取専用で得る。"""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        identity = db.execute(
            "SELECT value FROM metadata WHERE key='identity'"
        ).fetchone()
        assert identity == (source_sha256,), "CDCソース識別値が一致しません。"
        offsets = {}
        for topic, partition, offset in db.execute(
            "SELECT topic,partition_id,offset_id FROM offsets"
        ):
            offsets.setdefault(topic, {})[str(partition)] = offset
    return offsets


def wait_for_commit(path, source_sha256, config, expected):
    """別のグループ参加をせず、SQLiteとKafka双方の同じ確定点を待つ。"""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            **config["kafka"],
            "group.id": config["source"]["CDC_GROUP_ID"],
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = [TopicPartition(topic, 0) for topic in expected]
        # Kafkaは次に読む位置、SQLiteは反映済みの最後の位置を保存する。
        kafka_expected = {
            topic: offsets["0"] + 1 for topic, offsets in expected.items()
        }
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if positions(path, source_sha256) == expected:
                committed = consumer.committed(partitions, timeout=5)
                if {
                    item.topic: item.offset
                    for item in committed
                    if item.error is None
                } == kafka_expected:
                    return
            time.sleep(1)
    finally:
        consumer.close()
    raise AssertionError("合成CDCの最終確定位置に到達しませんでした。")


def observe(admin, source):
    """公開世代のGoldと隔離6表を、型・件数・全行の内容で比較可能にする。"""
    catalog = admin.catalog()
    release = engine.latest_release(admin, catalog, "cdc_")
    assert release, "CDCの公開世代がありません。"
    datasets = [*GOLD, *(f"quarantine.{table}" for table in source.mapping)]
    evidence = {}
    for name in datasets:
        snapshot = release["outputs"][name]
        table = engine.bounded_scan(
            catalog.load_table(f"cdc_{name}"), snapshot
        )
        rows = table.to_pylist()
        if name in GOLD:
            (row,) = rows
            assert all(row[key] == value for key, value in GOLD[name].items())
        elif name == "quarantine.erp_products":
            (row,) = rows
            assert row["product_id"] == "CDC-INVALID"
            assert row["validation_reason"] == "invalid_type"
        else:
            assert not rows, f"合成データの予期しない隔離行があります。{name}"
        # 行順を正規化し、Icebergの採番や保存パスを含めず内容を比較する。
        encoded_rows = sorted(
            json.dumps(row, default=api.json_value, sort_keys=True)
            for row in rows
        )
        evidence[name] = {
            "rows": len(rows),
            "schema": [
                [field.name, str(field.type), field.nullable]
                for field in table.schema
            ],
            "sha256": sha256(shadow.encoded(encoded_rows)).hexdigest(),
        }
    return {"release": release["release_snapshot_id"], "tables": evidence}


def capture(work):
    """試験用コンテナで、確定したRawと公開世代の内容を一度だけ採取する。"""
    source, config = source_config(
        Path("/run/cdc/settings.json"), Path("/run/cdc/table-map.json"), work
    )
    completed = {
        topic: {"0": 8 if target == "erp_products" else 0}
        for topic, target in source.topics.items()
    }
    source_sha256 = shadow.identity_digest(source.identity)
    wait_for_commit(
        Path("/state/state.sqlite3"), source_sha256, config, completed
    )
    # 合成fixtureの唯一のproducerが既に終了し、これ以降の配送はない。
    core = api.core_module()
    catalog = core.catalog()
    table = catalog.load_table(shadow.RAW)
    approval = {
        "source_sha256": source_sha256,
        "raw_table_uuid": str(table.metadata.table_uuid),
        "raw_snapshot_id": engine.current_id(table),
        "completed": completed,
    }
    bundle = work / "raw.json"
    digest = shadow.export_bundle(catalog.load_table, source, approval, bundle)
    sys.stdout.buffer.write(
        shadow.encoded(
            {
                "bundle": bundle.read_text(encoding="utf-8"),
                "bundle_sha256": digest,
                "positions": completed,
                "live": observe(core, source),
            }
        )
    )


def replay(work, stage):
    """公開先と別のSQLiteとIcebergに再生し、出力一致の証跡を返す。"""
    source, _ = source_config(
        stage / "settings.json", stage / "map.json", work
    )
    capture_result = shadow.parse(
        shadow.read_bounded(stage / "parity.json", shadow.MAX_BUNDLE)
    )
    bundle = work / "raw.json"
    shadow.write_new(bundle, capture_result["bundle"].encode())
    output = work / "replayed"
    receipt = shadow.replay_bundle(
        bundle, capture_result["bundle_sha256"], source, output
    )
    assert receipt["raw_events_appended"] == 14
    completed = positions(output / "state.sqlite3", receipt["source_sha256"])
    assert completed == capture_result["positions"], (
        "再生後の位置が異なります。"
    )
    admin = shadow.ShadowAdmin(output)
    try:
        result = observe(admin, source)
    finally:
        admin.catalog().engine.dispose()
    live = capture_result["live"]
    for name, evidence in live["tables"].items():
        assert result["tables"][name] == evidence, (
            f"再生結果が異なります。{name}"
        )
    print(
        json.dumps(
            {
                "raw_snapshot": receipt["raw_snapshot_id"],
                "raw_events": receipt["raw_events_appended"],
                "live_release": live["release"],
                "shadow_release": result["release"],
                "completed_partitions": len(completed),
                "verified_rows": {
                    name: evidence["rows"]
                    for name, evidence in result["tables"].items()
                },
            },
            sort_keys=True,
        )
    )


def main():
    """引数なしでコンテナ採取、試験ディレクトリ付きでホスト再生を行う。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path, nargs="?")
    args = parser.parse_args()
    with TemporaryDirectory() as directory:
        work = Path(directory)
        if args.stage is None:
            capture(work)
        else:
            replay(work, args.stage)


if __name__ == "__main__":
    main()

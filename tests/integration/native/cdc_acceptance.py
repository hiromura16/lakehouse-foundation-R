"""使い捨てのブローカーとトピックを使い、ネイティブCDCを検証する。

隔離したLakehouseのテストコンテナでこのファイルをPythonへ渡す。
first、deleteの順に実行し、コンシューマーを再起動してresumeを実行する。
"""

import argparse
import json
import time

from confluent_kafka import Producer

from native import api, engine

TOPIC = "foundation_r.synthetic_sales.dbo.erp_products"
DATA = {
    "product_id": "CDC-P1",
    "product_name": "Synthetic original",
    "product_family": "SYN",
    "unit": "pcs",
    "standard_cycle_seconds": "12.500",
    "updated_at": "2026-01-01T00:00:00Z",
}


def event(operation, sequence, row):
    """Foundationが解釈する形式でソースイベントを作る。"""
    return json.dumps(
        {
            "op": operation,
            "before": row if operation in ("u", "d") else None,
            "after": None if operation == "d" else row,
            "source": {
                "db": "synthetic_sales",
                "schema": "dbo",
                "table": "erp_products",
                "commit_lsn": "00000001:00000001:0001",
                "change_lsn": f"00000001:00000001:{sequence:04x}",
                "event_serial_no": sequence,
            },
        }
    )


def publish(events):
    """テスト用DockerのDNS名へイベントを発行し、配信確認を検査する。"""
    producer = Producer({"bootstrap.servers": "test-broker:9092"})
    errors = []

    def delivered(error, _):
        """イベントの原値を表示せず、配信の失敗を記録する。"""
        if error:
            errors.append(error)

    for operation, sequence, row in events:
        producer.produce(
            TOPIC,
            key=json.dumps({"product_id": row["product_id"]}),
            value=event(operation, sequence, row) if operation else None,
            on_delivery=delivered,
        )
    if producer.flush(15) or errors:
        raise RuntimeError("試験用ブローカーへの送信に失敗しました。")


def check(core, expected_count, expected_product):
    """整合した世代の公開を待ち、変更しないRawと照合する。"""
    catalog = core.catalog()
    for _ in range(90):
        if not catalog.table_exists("cdc_events.events"):
            time.sleep(1)
            continue
        raw = (
            catalog.load_table("cdc_events.events")
            .scan()
            .to_arrow()
            .to_pylist()
        )
        release = engine.latest_release(core, catalog, "cdc_")
        if len(raw) >= expected_count and release:
            names = ("silver.erp_products", "quarantine.erp_products")
            live, rejected = [
                engine.bounded_scan(
                    catalog.load_table(f"cdc_{name}"),
                    release["outputs"][name],
                ).to_pylist()
                for name in names
            ]
            product = [row["product_name"] for row in live]
            reasons = [row["validation_reason"] for row in rejected]
            if product == expected_product and reasons == ["invalid_type"]:
                assert len(raw) == expected_count
                assert len({row["event_id"] for row in raw}) == expected_count
                offsets = sorted(int(row["offset"]) for row in raw)
                assert offsets == list(range(expected_count))
                assert all(row["key_is_null"] is False for row in raw)
                deleted = [
                    int(row["offset"])
                    for row in raw
                    if row["value_is_null"] is True
                ]
                assert deleted == ([6] if expected_count >= 8 else [])
                assert all(type(row["value_is_null"]) is bool for row in raw)
                print(
                    json.dumps(
                        {
                            "raw_events": len(raw),
                            "silver_products": product,
                            "quarantine": reasons,
                            "release_snapshot_id": release[
                                "release_snapshot_id"
                            ],
                        }
                    )
                )
                return
        time.sleep(1)
    raise AssertionError("期待したCDCの世代が公開されませんでした。")


def main():
    """合成試験の1段階を実行し、期待するRaw・Silverの結果を待つ。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["first", "delete", "resume"])
    args = parser.parse_args()
    phases = {
        "first": (
            [
                ("c", 1, DATA),
                ("u", 3, {**DATA, "product_name": "Newest product"}),
                ("u", 2, {**DATA, "product_name": "Late update"}),
                ("r", 9, {**DATA, "product_name": "Delayed snapshot"}),
                (
                    "c",
                    3,
                    {
                        **DATA,
                        "product_id": "CDC-INVALID",
                        "standard_cycle_seconds": "invalid",
                    },
                ),
            ],
            5,
            ["Newest product"],
        ),
        "delete": (
            [("d", 4, DATA), (None, 0, DATA), ("c", 2, DATA)],
            8,
            [],
        ),
        "resume": (
            [("c", 5, {**DATA, "product_name": "Reinserted after restart"})],
            9,
            ["Reinserted after restart"],
        ),
    }
    events, count, expected = phases[args.phase]
    publish(events)
    check(api.core_module(), count, expected)


if __name__ == "__main__":
    main()

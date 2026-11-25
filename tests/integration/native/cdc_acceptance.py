"""使い捨てのブローカーとトピックを使い、ネイティブCDCを検証する。

隔離したLakehouseのテストコンテナでこのファイルをPythonへ渡す。
first、deleteの順に実行し、コンシューマーを再起動してresumeを実行する。
businessでは隔離再生との照合に使う工場の計画・実績・出荷を発行する。
"""

import argparse
import errno
import json
import re
import time
from hashlib import sha256

from confluent_kafka import Consumer, Producer, TopicPartition

from native import api, engine

TOPIC_PREFIX = "foundation_r.synthetic_sales.dbo"
DATA = {
    "product_id": "CDC-P1",
    "product_name": "Synthetic original",
    "product_family": "SYN",
    "unit": "pcs",
    "standard_cycle_seconds": "12.500",
    "updated_at": "2026-01-01T00:00:00Z",
}


BUSINESS = {
    ("erp_work_orders", "work_order_id"): {
        "work_order_id": "CDC-W1",
        "factory_id": "CDC-F1",
        "product_id": "CDC-P1",
        "planned_date": "2026-01-02",
        "planned_qty": "20",
    },
    ("mes_events", "event_id"): {
        "event_id": "CDC-E1",
        "work_order_id": "CDC-W1",
        "event_time": "2026-01-02T06:00:00Z",
        "good_qty": "12",
        "reject_qty": "2",
    },
    ("mes_downtime", "downtime_id"): {
        "downtime_id": "CDC-D1",
        "work_order_id": "CDC-W1",
        "start_time": "2026-01-02T06:30:00Z",
        "end_time": "2026-01-02T06:45:00Z",
        "reason_code": "PLANNED",
    },
    ("erp_sales_orders", "sales_order_id"): {
        "sales_order_id": "CDC-S1",
        "factory_id": "CDC-F1",
        "product_id": "CDC-P1",
        "order_date": "2026-01-02",
        "promised_date": "2026-01-03",
        "ordered_qty": "20",
        "unit_price": "125.50",
    },
    ("erp_shipments", "shipment_id"): {
        "shipment_id": "CDC-SH1",
        "sales_order_id": "CDC-S1",
        "shipped_at": "2026-01-04T00:00:00Z",
        "shipped_qty": "7",
    },
}


CONTINUITY_ID = "CDC-CONTINUITY"
PRIOR_PRODUCT = "Reinserted after restart"
CONTINUITY = {
    "continuity-insert": ("c", 10, "Continuity initial", 10),
    "continuity-update": ("u", 12, "Continuity updated", 11),
    "continuity-delete": ("d", 14, "Late update after delete", 12),
}
CONFIRM_PHASE = "continuity-confirm"
ORDER_LATE = "continuity-late-update"
ORDER_DUPLICATE = "continuity-duplicate"
ORDER_CONFIRM = "continuity-order-confirm"
ORDERING = (ORDER_LATE, ORDER_DUPLICATE, ORDER_CONFIRM)
PRODUCTS_TOPIC = f"{TOPIC_PREFIX}.erp_products"
LATE_OFFSET, DUPLICATE_OFFSET = 12, 13
LATE_SERIAL = 13
RESUME_SERIAL = 5
RESUME_OFFSET = 8


def event(table, operation, sequence, row, *, previous=None):
    """Foundationが解釈する形式でソースイベントを作る。"""
    return json.dumps(
        {
            "op": operation,
            "before": (
                (row if previous is None else previous)
                if operation in ("u", "d")
                else None
            ),
            "after": None if operation == "d" else row,
            "source": {
                "db": "synthetic_sales",
                "schema": "dbo",
                "table": table,
                "commit_lsn": "00000001:00000001:0001",
                "change_lsn": f"00000001:00000001:{sequence:04x}",
                "event_serial_no": sequence,
            },
        }
    )


def publish(groups, *, previous=None, expected_delivery=None):
    """テスト用DockerのDNS名へイベントを発行し、配信確認を検査する。"""
    producer = Producer(
        {
            "bootstrap.servers": "test-broker:9092",
            # ブローカーの事前準備漏れを空のトピックの自動生成で隠さない。
            "allow.auto.create.topics": False,
        }
    )
    errors = []
    deliveries = []

    def delivered(error, message):
        """イベントの原値を表示せず、配信の失敗を記録する。"""
        if error:
            errors.append(error)
        elif expected_delivery is not None:
            deliveries.append(
                (message.topic(), message.partition(), message.offset())
            )

    for table, key, events in groups:
        for operation, sequence, row in events:
            producer.produce(
                f"{TOPIC_PREFIX}.{table}",
                key=json.dumps({key: row[key]}),
                value=event(table, operation, sequence, row, previous=previous)
                if operation
                else None,
                on_delivery=delivered,
            )
    if (
        producer.flush(15)
        or errors
        or (expected_delivery is not None and deliveries != expected_delivery)
    ):
        raise RuntimeError("試験用ブローカーへの送信に失敗しました。")


def ignore_stage(_name):
    """通常の試験では診断用ステージを記録しない。"""


def check(
    core,
    expected_count,
    expected_product,
    *,
    continuity=None,
    snapshots=False,
    note_stage=ignore_stage,
    ordering=False,
):
    """整合した世代の公開を待ち、変更しないRawと照合する。"""
    note_stage("CATALOG")
    catalog = core.catalog()
    for _ in range(90):
        note_stage("RAW_READ")
        if not catalog.table_exists("cdc_events.events"):
            time.sleep(1)
            continue
        raw_table = catalog.load_table("cdc_events.events")
        raw = raw_table.scan().to_arrow().to_pylist()
        note_stage("RELEASE_READ")
        release = engine.latest_release(core, catalog, "cdc_")
        if len(raw) >= expected_count and release:
            note_stage("SILVER_READ")
            names = ("silver.erp_products", "quarantine.erp_products")
            live, rejected = [
                engine.bounded_scan(
                    catalog.load_table(f"cdc_{name}"),
                    release["outputs"][name],
                ).to_pylist()
                for name in names
            ]
            product = sorted(row["product_name"] for row in live)
            reasons = [row["validation_reason"] for row in rejected]
            note_stage("SILVER_CONTENT")
            if product == sorted(expected_product) and reasons == [
                "invalid_type"
            ]:
                note_stage("RAW_COUNT")
                assert len(raw) == expected_count
                note_stage("RAW_CONTENT")
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
                if continuity is not None:
                    prior = (
                        [
                            row
                            for row in raw
                            if int(row["offset"]) < LATE_OFFSET
                        ]
                        if ordering
                        else raw
                    )
                    verify_continuity_raw(prior, live, continuity)
                if ordering:
                    verify_ordering_raw(raw, expected_count)
                if snapshots or continuity is not None:
                    note_stage("SNAPSHOT_SHAPE")
                    raw_snapshot = engine.current_id(raw_table)
                    assert type(raw_snapshot) is int and raw_snapshot > 0
                    return {
                        "raw_snapshot_id": raw_snapshot,
                        "release_snapshot_id": release["release_snapshot_id"],
                    }
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
        else:
            note_stage(
                "RAW_COUNT" if len(raw) < expected_count else "RELEASE_MISSING"
            )
        time.sleep(1)
    raise AssertionError("期待したCDCの世代が公開されませんでした。")


def verify_continuity_raw(raw, live, sequence):
    """新規の固定IDについて、順序・本文・現在値を照合する。"""
    expected = list(CONTINUITY.values())[:sequence]
    recent = sorted(
        (
            row
            for row in raw
            if json.loads(row["key_json"]) == {"product_id": CONTINUITY_ID}
        ),
        key=lambda row: int(row["offset"]),
    )
    assert len(recent) == len(expected)
    previous = None
    products_topic = f"{TOPIC_PREFIX}.erp_products"
    for row, (operation, serial, label, count) in zip(
        recent, expected, strict=True
    ):
        offset = count - 1
        envelope = json.loads(row["envelope_json"])
        assert row["topic"] == products_topic
        assert row["partition"] == "0" and row["offset"] == str(offset)
        assert row["event_id"] == json.dumps(
            [products_topic, 0, offset], separators=(",", ":")
        )
        assert envelope["op"] == operation
        assert envelope["source"]["event_serial_no"] == serial
        before = late_images()[1] if operation == "d" else previous
        assert envelope["before"] == before
        after = (
            None
            if operation == "d"
            else {**DATA, "product_id": CONTINUITY_ID, "product_name": label}
        )
        assert envelope["after"] == after
        if operation == "u":
            assert previous is not None and after != previous
        previous = after
    target = [row for row in live if row["product_id"] == CONTINUITY_ID]
    if sequence < len(CONTINUITY):
        assert len(target) == 1
        assert target[0]["product_name"] == expected[-1][2]
    else:
        assert not target
    assert sorted(row["product_id"] for row in live) == sorted(
        [DATA["product_id"]] + ([CONTINUITY_ID] if target else [])
    )


def broker_committed(expected, *, high=None, note_stage=ignore_stage):
    """購読・commitを行わず、同じ合成groupの次の位置を照会する。"""
    note_stage("BROKER_COMMITTED")
    consumer = Consumer(
        {
            "bootstrap.servers": "test-broker:9092",
            "group.id": "lakehouse-r-native-cdc-ci",
            "enable.auto.commit": False,
        }
    )
    partitions = [TopicPartition(f"{TOPIC_PREFIX}.erp_products", 0)]
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            observed = consumer.committed(partitions, timeout=3)
            if len(observed) == 1 and observed[0].error is None:
                cursor = observed[0].offset
                assert cursor <= expected
                if cursor == expected:
                    if high is not None:
                        note_stage("BROKER_WATERMARK")
                        assert consumer.get_watermark_offsets(
                            partitions[0], timeout=3, cached=False
                        ) == (0, high)
                    return cursor
            time.sleep(1)
    finally:
        consumer.close()
    raise AssertionError("固定した合成Kafkaの確定位置に到達しませんでした。")


def run_continuity(phase):
    """各変更を1件だけ投入し、再確認では何も投入しない。"""
    if phase != CONFIRM_PHASE:
        operation, serial, label, total = CONTINUITY[phase]
        point = {**DATA, "product_id": CONTINUITY_ID, "product_name": label}
        previous = (
            {**point, "product_name": CONTINUITY["continuity-insert"][2]}
            if operation == "u"
            else None
        )
        publish(
            [("erp_products", "product_id", [(operation, serial, point)])],
            previous=previous,
        )
    else:
        operation, _, label, total = CONTINUITY["continuity-delete"]
    expected = [PRIOR_PRODUCT] + ([] if operation == "d" else [label])
    result = check(api.core_module(), total, expected, continuity=total - 9)
    result["broker_next_offset"] = broker_committed(total)
    print(json.dumps(result))


# Arrow 26.0.1のS3エラー表現とlibcurlの接続/時間切れ/送受信だけ。
# 文中の任意の断片、HTTPエラー、DNS、TLS、復号や認可は受け付けない。
RAW_TRANSPORT = re.compile(
    r"(?:When reading information for key '[^'\n]*' "
    r"in bucket '[^'\n]*': )?"
    r"AWS Error NETWORK_CONNECTION during (?:HeadObject|GetObject) "
    r"operation: curlCode: (?:7|28|55|56), "
)


def raw_transport_error(error):
    """認証付きのオブジェクト読取中の一時接続失敗だけを選別する。"""
    return error.errno in {
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.ETIMEDOUT,
    } or (error.errno is None and RAW_TRANSPORT.match(str(error)) is not None)


def raw_proof(core, expected_snapshot):
    """既知のRaw世代を認証付きで走査し、schemaと9件の内容を照合可能にする。"""
    stage = "CATALOG"
    try:
        table = core.catalog().load_table("cdc_events.events")
        stage = "SNAPSHOT"
        current = engine.current_id(table)
        if type(current) is not int or current != expected_snapshot:
            raise AssertionError
        stage = "READ"
        try:
            data = table.scan(snapshot_id=current).to_arrow()
        except OSError as error:
            if raw_transport_error(error):
                stage = "TRANSPORT"
            raise
        stage = "CONTENT"
        rows = sorted(data.to_pylist(), key=lambda row: row["event_id"])
        if (
            len(rows) != 9
            or len({row["event_id"] for row in rows}) != 9
            or sorted(int(row["offset"]) for row in rows) != list(range(9))
        ):
            raise AssertionError
        fields = [
            (field.name, str(field.type), field.nullable)
            for field in data.schema
        ]
        contents = json.dumps(
            [fields, rows],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
        print(
            json.dumps(
                {"snapshot": current, "sha256": sha256(contents).hexdigest()}
            )
        )
    except Exception:
        print(f"CDC_RAW_PROOF_{stage}", flush=True)
        raise SystemExit(1) from None


def run_storage(phase):
    """閉域S3試験で使用する合成イベントと読み取り専用の観測。"""
    if phase == "storage-old-cursor":
        print(json.dumps({"broker_next_offset": broker_committed(9, high=10)}))
        return
    if phase == "storage-publish":
        label = CONTINUITY["continuity-insert"][2]
        point = {**DATA, "product_id": CONTINUITY_ID, "product_name": label}
        topic = f"{TOPIC_PREFIX}.erp_products"
        publish(
            [("erp_products", "product_id", [("c", 10, point)])],
            expected_delivery=[(topic, 0, 9)],
        )
        print("DELIVERED")
        return
    # brokerのhigh watermarkと、既に反映・確認した件数は別物。
    total, high = {
        "storage-baseline": (9, 9),
        "storage-restored": (9, 10),
        "storage-recovered": (10, 10),
    }[phase]
    recovered = phase == "storage-recovered"
    products = [PRIOR_PRODUCT]
    if recovered:
        products.append(CONTINUITY["continuity-insert"][2])
    stage = "OBSERVATION"

    def note_stage(value):
        """対象の読み取り工程だけを保持し、実値は保存しない。"""
        nonlocal stage
        stage = value

    diagnosed = phase == "storage-restored"
    options = {"note_stage": note_stage} if diagnosed else {}
    try:
        result = check(
            api.core_module(),
            total,
            products,
            continuity=1 if recovered else None,
            snapshots=True,
            **options,
        )
        result["broker_next_offset"] = broker_committed(
            total, high=high, **options
        )
        print(json.dumps(result))
    except Exception:
        if not diagnosed:
            raise
        # hostへは実値・例外・tracebackではなく固定工程だけ返す。
        print(f"CDC_RESTORE_{stage}", flush=True)
        raise SystemExit(1) from None


def late_images():
    """版13は版14の削除より前で、版12の更新と別のイベント。"""
    base = {**DATA, "product_id": CONTINUITY_ID}
    before = {**base, "product_name": CONTINUITY["continuity-update"][2]}
    after = {**base, "product_name": CONTINUITY["continuity-delete"][2]}
    return before, after


def resume_row():
    """削除順序試験と独立した、既存商品の再INSERTをそのまま再利用。"""
    return {**DATA, "product_name": PRIOR_PRODUCT}


def verify_ordering_raw(raw, total):
    """Kafka上の配送と上流の論理イベントを混同せず、両方を照合する。"""
    by_offset = {int(row["offset"]): row for row in raw}
    assert len(by_offset) == len(raw) == total

    def delivery(offset, target, payload):
        """異なる配送位置と、非NULLの元のキー・本文を照合する。"""
        row = by_offset[offset]
        assert row["topic"] == PRODUCTS_TOPIC and row["partition"] == "0"
        assert row["event_id"] == json.dumps(
            [PRODUCTS_TOPIC, 0, offset], separators=(",", ":")
        )
        assert json.loads(row["key_json"]) == {"product_id": target}
        assert row["value_is_null"] is False
        assert row["envelope_json"] == payload

    before, after = late_images()
    assert before != after
    delivery(
        LATE_OFFSET,
        CONTINUITY_ID,
        event("erp_products", "u", LATE_SERIAL, after, previous=before),
    )
    if total == DUPLICATE_OFFSET + 1:
        # 同一の上流操作・LSN・シリアル・本文を、異なるKafka配送で保持。
        payload = event("erp_products", "c", RESUME_SERIAL, resume_row())
        for offset in (RESUME_OFFSET, DUPLICATE_OFFSET):
            delivery(offset, DATA["product_id"], payload)


def run_ordering(phase):
    """発行後はbrokerの確認を待ってから確定済み世代を読み取る。"""
    offset = LATE_OFFSET
    if phase == ORDER_LATE:
        before, after = late_images()
        publish(
            [("erp_products", "product_id", [("u", LATE_SERIAL, after)])],
            previous=before,
            expected_delivery=[(PRODUCTS_TOPIC, 0, offset)],
        )
    else:
        offset = DUPLICATE_OFFSET
        if phase == ORDER_DUPLICATE:
            publish(
                [
                    (
                        "erp_products",
                        "product_id",
                        [("c", RESUME_SERIAL, resume_row())],
                    )
                ],
                expected_delivery=[(PRODUCTS_TOPIC, 0, offset)],
            )
    expected = offset + 1
    cursor = broker_committed(expected, high=expected)
    result = check(
        api.core_module(),
        expected,
        [PRIOR_PRODUCT],
        continuity=3,
        ordering=True,
    )
    result["broker_next_offset"] = cursor
    print(json.dumps(result))


def main():
    """合成試験の1段階を実行し、期待するRaw・Silverの結果を待つ。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=[
            "first",
            "delete",
            "resume",
            "business",
            *CONTINUITY,
            CONFIRM_PHASE,
            *ORDERING,
            "storage-old-cursor",
            "storage-raw-proof",
            "storage-publish",
            "storage-baseline",
            "storage-restored",
            "storage-recovered",
        ],
    )
    parser.add_argument("--raw-snapshot", type=int)
    args = parser.parse_args()
    if args.phase == "storage-raw-proof":
        if args.raw_snapshot is None or args.raw_snapshot <= 0:
            parser.error("expected a positive Raw snapshot")
        raw_proof(api.core_module(), args.raw_snapshot)
        return
    if args.raw_snapshot is not None:
        parser.error("Raw snapshot is only accepted for the Raw proof")
    if args.phase in ORDERING:
        run_ordering(args.phase)
        return
    if args.phase.startswith("storage-"):
        run_storage(args.phase)
        return
    if args.phase in (*CONTINUITY, CONFIRM_PHASE):
        run_continuity(args.phase)
        return
    if args.phase == "business":
        publish(
            (table, key, [("c", 1, {**row, "updated_at": DATA["updated_at"]})])
            for (table, key), row in BUSINESS.items()
        )
        return
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
            [("c", RESUME_SERIAL, resume_row())],
            9,
            ["Reinserted after restart"],
        ),
    }
    events, count, expected = phases[args.phase]
    publish([("erp_products", "product_id", events)])
    check(api.core_module(), count, expected)


if __name__ == "__main__":
    main()

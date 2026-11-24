"""独立したPyIcebergプロセスで、カタログとParquetを読む。"""

import importlib.metadata
import json
import os
import re
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError


def event(row):
    """型付けされた計測行を、比較用の同じ列順にそろえる。"""
    return (
        row["event_id"],
        row["event_time"].isoformat(),
        row["measurement"],
        row["metric_value"],
    )


def wire(row):
    """Kafkaの区画・位置をキーにし、読戻したバイトを16進で採取する。"""
    return (
        (row["kafka_partition"], row["kafka_offset"]),
        row["kafka_time"].isoformat(),
        row["kafka_key"].hex() if row["kafka_key"] is not None else None,
        row["payload"].hex() if row["payload"] is not None else None,
    )


def upsert(catalog):
    """同じIceberg V2世代を、PyIcebergとDuckDBで独立して読む。"""
    import duckdb

    key = os.environ["S3_ACCESS_KEY"]
    secret = os.environ["S3_SECRET_KEY"]
    if not all(re.fullmatch(r"[0-9a-f]+", item) for item in (key, secret)):
        raise ValueError("合成S3認証の形式が不正です。")
    with duckdb.connect(
        config={
            "threads": "2",
            "memory_limit": "256MB",
            "autoload_known_extensions": "false",
            "autoinstall_known_extensions": "false",
        }
    ) as engine:
        # 拡張の暗黙の取得を無効にし、既定の署名検証付きで読み込む。
        for module in ("httpfs", "iceberg"):
            engine.execute(
                f"LOAD '/opt/upsert-extensions/{module}.duckdb_extension'"
            )
        engine.execute(f"""
CREATE SECRET local_s3 (
    TYPE s3, KEY_ID '{key}', SECRET '{secret}', ENDPOINT 'storage:8333',
    REGION 'us-east-1', URL_STYLE 'path', USE_SSL false
);
""")
        result = {
            "pyiceberg_version": importlib.metadata.version("pyiceberg"),
            "duckdb_version": engine.execute("SELECT version()").fetchone()[0],
            "tables": {
                name: upsert_table(
                    catalog.load_table(("fixture", name)), engine
                )
                for name in ("mor_bucket", "pk_position")
            },
        }
    print(json.dumps(result))


def upsert_table(table, engine):
    """読取を現行のスナップショットIDに固定し、削除ファイルも確認する。"""
    fields = ("id", "plant", "quantity", "revision")
    location = table.metadata_location
    if not re.match(r"s3://fr-iceberg-[a-z0-9]{8}-warehouse/", location):
        raise RuntimeError("専用warehouse以外のメタデータは読めません。")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Iceberg upsertのスナップショットがありません。")
    snapshot_id = snapshot.snapshot_id
    deletes = table.inspect.delete_files(snapshot_id).to_pylist()
    try:
        rows = table.scan(
            snapshot_id=snapshot_id, selected_fields=fields
        ).to_arrow()
    except ValueError as error:
        # PyIceberg 0.12.0で等値削除が実際に拒否されたときだけ識別する。
        if "does not yet support equality deletes" not in str(error):
            raise
        pyiceberg = "equality-delete-unsupported"
    else:
        pyiceberg = sorted(
            tuple(row[field] for field in fields) for row in rows.to_pylist()
        )
    return {
        "version": table.metadata.format_version,
        "partition": [str(field.transform) for field in table.spec().fields],
        "id_field": table.schema().find_field("id").field_id,
        "snapshot_id": snapshot_id,
        "sequence": snapshot.sequence_number,
        "history": sorted(item.snapshot_id for item in table.snapshots()),
        # Icebergファイルのcontent値は1が位置削除、2が等値削除。
        "position_deletes": sum(int(row["content"]) == 1 for row in deletes),
        "equality_ids": sorted(
            {
                tuple(row["equality_ids"] or ())
                for row in deletes
                if int(row["content"]) == 2
            }
        ),
        "pyiceberg": pyiceberg,
        "duckdb": engine.execute(
            "SELECT id,plant,quantity,revision FROM "
            "iceberg_scan(?, snapshot_from_id=?) ORDER BY id",
            [location, snapshot_id],
        ).fetchall(),
    }


def main() -> None:
    """同一の隔離ネットワークから、設定済みwarehouseの専用表へ接続する。"""
    catalog = load_catalog(
        "fixture",
        type="rest",
        uri="http://catalog:8181/catalog",
        warehouse="fixture-warehouse",
        **{
            "header.X-Iceberg-Access-Delegation": "client-managed",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": "http://storage:8333",
            "s3.region": "us-east-1",
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.force-virtual-addressing": "false",
        },
    )
    if sys.argv[1:] == ["namespace"]:
        try:
            catalog.create_namespace("fixture")
        except NamespaceAlreadyExistsError:
            pass
        return
    operation = sys.argv[1:]
    if operation == ["upsert"]:
        upsert(catalog)
        return
    if operation == ["read"]:
        table = catalog.load_table(("fixture", "events_v2"))
        convert = event
    elif operation == ["telemetry-wire"]:
        table = catalog.load_table(("fixture", "telemetry_wire_v2"))
        convert = wire
    else:
        raise ValueError("未対応の操作です。")

    if table.metadata.format_version != 2:
        raise RuntimeError(
            f"Iceberg形式がV2ではありません: {table.metadata.format_version}"
        )
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Icebergスナップショットがまだありません。")
    arrow = table.scan().to_arrow()
    rows = sorted(convert(row) for row in arrow.to_pylist())
    print(
        json.dumps(
            {
                "format_version": table.metadata.format_version,
                "pyiceberg": importlib.metadata.version("pyiceberg"),
                "snapshot_id": snapshot.snapshot_id,
                "snapshots": [item.snapshot_id for item in table.snapshots()],
                "rows": rows,
                "count": len(rows),
                "distinct_ids": len({row[0] for row in rows}),
            }
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error)
        for name in ("S3_ACCESS_KEY", "S3_SECRET_KEY"):
            value = os.environ.get(name)
            if value:
                message = message.replace(value, "<redacted>")
        print(message, file=sys.stderr)
        raise SystemExit(1) from None

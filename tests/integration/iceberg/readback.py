"""独立したPyIcebergプロセスで、カタログとParquetを読む。"""

import importlib.metadata
import json
import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError


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
    if sys.argv[1:] != ["read"]:
        raise ValueError("未対応の操作です。")

    table = catalog.load_table(("fixture", "events_v2"))
    if table.metadata.format_version != 2:
        raise RuntimeError(
            f"Iceberg形式がV2ではありません: {table.metadata.format_version}"
        )
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError("Icebergスナップショットがまだありません。")
    arrow = table.scan().to_arrow()
    rows = sorted(
        (
            row["event_id"],
            row["event_time"].isoformat(),
            row["measurement"],
            row["metric_value"],
        )
        for row in arrow.to_pylist()
    )
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

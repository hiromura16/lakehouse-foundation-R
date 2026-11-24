"""実DuckDBで複数Arrowバッチから、上限・並び順と全件数を照合する。"""

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa

from native import engine

SCHEMA = pa.schema(
    [("factory_id", pa.string()), ("production_date", pa.date32())]
)


class Table:
    """固定Icebergスナップショットから得るバッチの境界だけを模擬する。"""

    def scan(self, *, snapshot_id, limit, row_filter):
        """Goldを読んだときのスナップショット指定と行上限を検査する。"""
        assert snapshot_id == 11 and limit == engine.MAX_ROWS + 1
        assert row_filter is not None
        return self

    def to_arrow_batch_reader(self):
        """先頭のバッチから漏れた小さいキーも、後続バッチへ置く。"""
        return pa.RecordBatchReader.from_batches(
            SCHEMA,
            [
                pa.RecordBatch.from_arrays(
                    [
                        pa.array([factory, factory]),
                        pa.array([date(2026, 1, 2), date(2026, 1, 1)]),
                    ],
                    schema=SCHEMA,
                )
                for factory in ("F03", "F02", "F01")
            ],
        )


def main():
    """本番ライブラリで全件数6・返却2行を確認し、超過時の応答を拒む。"""
    dataset = "factory_daily_production"
    release = {
        "outputs": {f"gold.{dataset}": 11},
        "release_snapshot_id": "21",
        "released_at": "2026-01-05T12:00:00Z",
        "recipe": "synthetic",
    }
    catalog = SimpleNamespace(load_table=lambda _: Table())
    core = SimpleNamespace(
        catalog=lambda: catalog, schema=lambda prefix, layer: prefix + layer
    )
    with patch.object(engine, "latest_release", return_value=release):
        result = engine.gold_rows(core, dataset, 2)
        assert result["rows"] == [
            {"factory_id": "F01", "production_date": date(2026, 1, day)}
            for day in (1, 2)
        ]
        assert result["total_rows"] == 6 and result["truncated"]
        for limit, value in (("MAX_ROWS", 3), ("MAX_TABLE_BYTES", 1)):
            with patch.object(engine, limit, value):
                try:
                    engine.gold_rows(core, dataset, 2)
                except Exception as error:
                    assert "NATIVE_BATCH_LIMIT" in str(error)
                else:
                    raise AssertionError(
                        "走査の上限を超えた結果が返りました。"
                    )
    print("GoldのArrow複数バッチ、先頭2行、全件数と上限を確認しました。")


if __name__ == "__main__":
    main()

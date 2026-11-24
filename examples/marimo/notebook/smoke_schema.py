"""固定合成スナップショットと、入力境界・DDLの両方を検査する。"""

import hashlib
import json
import tempfile
from pathlib import Path

import duckdb
from approved_gold import load_approved, open_snapshot_db


def with_approval(directory, snapshot):
    """取り込み可能な正しいSHAを付け、SHA以外の失敗条件も試せるようにする。"""
    content = json.dumps(snapshot).encode()
    (directory / "gold.json").write_bytes(content)
    (directory / "approval.json").write_text(
        json.dumps(
            {
                "format_version": 1,
                "source": "committed_synthetic_oracle",
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ),
        encoding="utf-8",
    )


def fails(function, error_type, directory):
    """想定した段階で入力を拒否することを確認する。"""
    try:
        function(directory)
    except error_type:
        return
    raise AssertionError(f"{error_type.__name__}を期待しました。")


def main():
    """実際に合成データを読み、契約の破損を個別に再現する。"""
    source = Path("/approved")
    original = json.loads((source / "gold.json").read_bytes())
    provenance, connection = open_snapshot_db(source)
    with connection:
        result = connection.execute(
            """
            SELECT count(*), count(DISTINCT factory_id), sum(planned_qty),
                   sum(good_qty), sum(reject_qty), sum(downtime_minutes)
            FROM production
            """
        ).fetchone()
        assert result == (14, 2, 2347, 2154, 62, 185.0)
        definitions = connection.execute("DESCRIBE production").fetchall()
        assert [column[:4] for column in definitions[:3]] == [
            ("factory_id", "VARCHAR", "NO", "PRI"),
            ("production_date", "DATE", "NO", "PRI"),
            ("planned_qty", "DECIMAL(38,0)", "NO", None),
        ]
        assert connection.execute(
            "SELECT current_setting('enable_external_access')"
        ).fetchone() == (False,)
        factories = connection.execute(
            "SELECT factory_id, count(*) FROM production GROUP BY 1 ORDER BY 1"
        ).fetchall()
        assert factories == [("F01", 7), ("F02", 7)]

    with tempfile.TemporaryDirectory() as workspace:
        trial = Path(workspace)
        with_approval(trial, original)
        with (trial / "gold.json").open("ab") as handle:
            handle.write(b"\n")
        fails(load_approved, ValueError, trial)

        with_approval(trial, {**original, "truncated": True})
        fails(load_approved, ValueError, trial)

        rows = [dict(row) for row in original["rows"]]
        rows[0]["production_date"] = "2026-02-30"
        with_approval(trial, {**original, "rows": rows})
        fails(load_approved, ValueError, trial)

        rows[0] = dict(rows[1])
        with_approval(trial, {**original, "rows": rows})
        fails(open_snapshot_db, duckdb.ConstraintException, trial)

        rows[0] = {**original["rows"][0], "total_qty": 0}
        with_approval(trial, {**original, "rows": rows})
        fails(open_snapshot_db, duckdb.ConstraintException, trial)

        with_approval(
            trial, {**original, "snapshot_id": "8005438907150430096"}
        )
        fails(load_approved, ValueError, trial)

    print(
        json.dumps(
            {
                "input_rejections": 4,
                "ddl_rejections": 2,
                "factories": 2,
                "rows": 14,
                "approved_sha256": provenance["sha256"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

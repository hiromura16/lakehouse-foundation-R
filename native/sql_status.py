"""固定SQLの完了台帳、実行状態、Gold公開情報を端末から読み取る。"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta

from scripts import sql_batch_worker

COMPLETION_FIELDS = (
    "apply_id",
    "batch_id",
    "mode",
    "observed_at",
    "counts",
    "changed_counts",
    "current_counts",
    "sql_statements",
)
APPLY_FIELDS = ("apply_id", "batch_id", "mode", "observed_at", "status")


def published_summary(release, gold_names):
    """公開済みGoldから型と名前を確認し、固定の運用項目だけを返す。"""
    if release is None:
        return None
    try:
        snapshot = release["release_snapshot_id"]
        released_at = datetime.fromisoformat(release["released_at"])
    except (KeyError, TypeError, ValueError):
        raise sql_batch_worker.SqlBatchError(
            "Invalid SQL Gold release."
        ) from None
    if (
        not isinstance(snapshot, str)
        or not re.fullmatch(r"-?[0-9]{1,19}", snapshot)
        or not -(2**63) <= int(snapshot) < 2**63
        or released_at.utcoffset() != timedelta(0)
    ):
        raise sql_batch_worker.SqlBatchError("Invalid SQL Gold release.")
    counts = release.get("row_counts")
    rows = None
    if counts is not None:
        if not isinstance(counts, dict):
            raise sql_batch_worker.SqlBatchError("Invalid SQL Gold counts.")
        # 契約上のGold名だけを走査し、台帳外の表名や業務値を返さない。
        rows = {
            name: counts[f"gold.{name}"]
            for name in gold_names
            if f"gold.{name}" in counts
        }
        if any(type(count) is not int or count < 0 for count in rows.values()):
            raise sql_batch_worker.SqlBatchError("Invalid SQL Gold counts.")
    return {
        "release_snapshot_id": snapshot,
        "released_at": released_at.isoformat(),
        "gold_rows": rows,
    }


def collect(core, engine):
    """三つの観測を独立して読み、入力や処理の状態は変更しない。"""
    catalog = core.catalog()
    _, history, progress = sql_batch_worker.prior_state(catalog)
    latest = max(history, key=lambda row: row["observed_at"], default=None)
    completed = None
    if latest is not None:
        # 再送時の0件に読み替えず、完了台帳に保存した変更件数を使う。
        record = sql_batch_worker.acknowledgement(latest, skipped=False)
        if record["counts"].keys() != {row["table"] for row in core.CONTRACT}:
            raise sql_batch_worker.SqlBatchError("Invalid SQL table counts.")
        completed = {name: record.get(name) for name in COMPLETION_FIELDS}
    # source_identityは内部で検証されるが、診断結果には含めない。
    apply = (
        {name: progress[0][name] for name in APPLY_FIELDS}
        if progress
        else None
    )
    release = engine.latest_release(core, catalog, prefix="sql_")
    return {
        "last_completed": completed,
        "apply": apply,
        "published": published_summary(release, engine.GOLD),
    }


def main(argv=None):
    """既存の接続を使い、失敗時は診断コードだけを標準エラーへ返す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    try:
        from native import engine, storage

        output = json.dumps(collect(storage, engine), sort_keys=True)
    except sql_batch_worker.SqlBatchError:
        print("SQL_STATUS_INVALID", file=sys.stderr)
        return 1
    except Exception:
        print("SQL_STATUS_UNAVAILABLE", file=sys.stderr)
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

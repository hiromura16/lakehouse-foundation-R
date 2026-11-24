"""上限を設けて固定した不変のRawスナップショットからCDC状態を再構築する。

再構築ではSQL Serverへの読取やコネクターの変更を行わない。
6表すべてについて、オフセット0から欠落のないKafka配送履歴を要求する。
イベントが一度も無い空表は履歴欠落と区別できないため、基準を作らず拒否する。

各派生表は個別にコミットする。派生処理の前に旧SQLite状態へ開始印を
永続化し、中断した世代を正常と見なして再開するのを防ぐ。
ローカル状態を正常に公開した後に、呼出元がKafkaへ配送を確認する。
"""

import json
import os
import re
import tempfile
from pathlib import Path

from raw_history import (
    MAX_EVENTS,
    open_raw_snapshot,
    raw_headers,
    read_record,
)
from raw_history import (
    RebuildError as RebuildError,
)
from state import State

MAX_BYTES = 128 * 1024 * 1024


def flush_sqlite(store):
    """単一のSQLiteファイルを永続化して閉じ、不可分な差し替えに備える。"""
    store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    store.connection.execute("PRAGMA journal_mode=DELETE")
    store.connection.close()
    descriptor = os.open(store.path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def copy_diagnostics(original, candidate):
    """上限付きで永続化した異常と再構築の履歴を新世代へ引き継ぐ。"""
    for table, fields in (
        (
            "processing_errors",
            "topic,partition_id,offset_id,error_code,recorded_at",
        ),
        (
            "rebuild_history",
            "request_id,status,raw_snapshot_id,error_code,updated_at",
        ),
    ):
        # 内部で固定したSQLの表名と列名を使う。要求からは受け取らない。
        cursor = original.connection.execute(
            "SELECT " + fields + " FROM " + table
        )
        copied = 0
        with candidate.connection:
            while True:
                rows = cursor.fetchmany(128)
                if not rows:
                    break
                copied += len(rows)
                if copied > MAX_EVENTS:
                    raise RebuildError("CDC_DIAGNOSTIC_HISTORY_LIMIT")
                candidate.connection.executemany(
                    "INSERT OR REPLACE INTO " + table + " VALUES (?,?,?,?,?)",
                    rows,
                )


def rebuild(
    admin,
    original,
    mapping,
    topics,
    request_id,
    *,
    materialize,
    validate_resume=None,
    maximum=MAX_EVENTS,
    byte_limit=MAX_BYTES,
):
    """Rawを検証し、派生表を構築して、完全なローカル世代を公開する。

    呼出元はコンシューマーのディレクトリをロックし、配送を一時停止する。
    この処理ではSQL取消の保留状態を解除しない。検証失敗時は現在値と
    完了オフセットを保つ。派生処理の開始後に失敗した場合は、
    派生表の世代が異なり得るため、旧状態の処理中の印を残す。
    """
    if not original.fixed_source or not re.fullmatch(
        r"[0-9a-f]{32}", request_id
    ):
        raise RebuildError("CDC_REBUILD_NOT_SUPPORTED")
    if original.get("blocked") == "true" or admin.STATE.get("blocked"):
        raise RebuildError("CDC_QUERY_REVIEW_REQUIRED")
    table, snapshot_id = open_raw_snapshot(
        admin.catalog(), lambda: original.get("identity")
    )
    headers = raw_headers(
        table,
        snapshot_id,
        topics,
        original.completed_offsets(),
        maximum=maximum,
    )
    directory = Path(original.path).parent
    descriptor, name = tempfile.mkstemp(
        prefix=".rebuild-", suffix=".sqlite3", dir=directory
    )
    os.close(descriptor)
    temporary_path = Path(name)
    temporary = None
    published = False
    try:
        temporary = State(temporary_path, original.identity)
        total_bytes = 0
        contracts = {item["table"]: item["columns"] for item in admin.CONTRACT}
        for header in headers:
            record, size = read_record(table, snapshot_id, header)
            total_bytes += size
            if total_bytes > byte_limit:
                raise RebuildError("CDC_RAW_REBUILD_BYTE_LIMIT")
            target = topics[record["topic"]]
            temporary.stage(record, target, mapping[target], contracts[target])
        # 非公開の候補状態に位置を記録しても、配送は未確認のままにする。
        # 派生表がすべて正常に確定してから、候補を有効にする。
        temporary.complete(headers)
        temporary.set("initialized", "true")
        copy_diagnostics(original, temporary)
        if validate_resume is not None:
            validate_resume(temporary)
        original.record_rebuild(request_id, "running", snapshot_id)
        original.set(
            "rebuild_pending",
            json.dumps(
                {
                    "request_id": request_id,
                    "raw_snapshot_id": snapshot_id,
                }
            ),
        )
        materialize(admin, temporary)
        temporary.record_rebuild(request_id, "succeeded", snapshot_id)
        temporary.set("rebuild_pending", "")
        temporary.set("rebuild_generation", request_id)
        flush_sqlite(temporary)
        temporary = None
        # 旧接続を閉じ、使用中のWAL付随ファイルがない状態で差し替える。
        # os.replace 前に停止した場合、現在のファイルには処理中の印が残る。
        # 起動時に、派生表の世代が混在する状態を正常と見なすのを防ぐ。
        flush_sqlite(original)
        os.replace(temporary_path, original.path)
        published = True
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        active = State(original.path, original.identity)
        return active, snapshot_id
    except Exception:
        if temporary is not None:
            temporary.connection.close()
        # 接続終了や公開の直後にファイルシステムの異常が起きた場合は、
        # 呼出元が現行パスを開き直す。正常に公開した完了位置は保持する。
        if admin.STATE.get("blocked") and not published:
            try:
                original.set("blocked", "true")
            except Exception:
                pass
        raise
    finally:
        for suffix in ("", "-wal", "-shm"):
            Path(str(temporary_path) + suffix).unlink(missing_ok=True)

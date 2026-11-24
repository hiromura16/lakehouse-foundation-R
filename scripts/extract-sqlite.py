#!/usr/bin/env python3
"""工場・内製アプリのSQLiteバックアップを、読取専用で業務バッチにする。"""

import argparse
import json
import os
import re
import sqlite3
import stat
import sys
from contextlib import closing
from datetime import datetime, timezone
from hashlib import sha256
from operator import itemgetter
from pathlib import Path
from time import monotonic
from uuid import UUID

import sql_batch_worker as worker
import sql_snapshot
from sqlserver_batch import (
    MAX_FIELD_CHARS,
    BatchError,
    canonical_timestamp,
    safe_landing,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "medallion/sources.json"
IDENTIFIER = worker.POSTGRESQL_IDENTIFIER
UTC_SOURCE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?Z")
SQLITE_HEADER = b"SQLite format 3\x00"
MAX_ROW_BYTES = 512 * 1024
SNAPSHOT_SECONDS = 120


def fingerprint(path):
    """承認済みの単体DBを検査し、読取前後で同じ実ファイルか比較する。"""
    sidecars = [
        path.with_name(path.name + ext) for ext in ("-wal", "-shm", "-journal")
    ]
    if (
        path.resolve() != path
        or path.is_relative_to(ROOT)
        or not path.is_file()
        or any(other.exists() or other.is_symlink() for other in sidecars)
    ):
        raise BatchError(
            "checkout外の単体SQLiteバックアップを指定してください。"
        )
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(path, flags), "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise BatchError(
                "SQLiteバックアップは通常ファイルで指定してください。"
            )
        header = stream.read(100)
    # SQLiteヘッダーの書込・読取形式が両方1である単体DBを確認する。
    # 2のWAL形式や補助ファイルを伴う稼働DBは、バックアップ後に取り扱う。
    if (
        len(header) != 100
        or not header.startswith(SQLITE_HEADER)
        or header[18:20] != b"\x01\x01"
    ):
        raise BatchError(
            "SQLiteバックアップのヘッダーとjournal形式を確認してください。"
        )
    return (
        info.st_dev,
        info.st_ino,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def source_name(name):
    """SQLバッチに出す論理名を検査し、実ファイルの位置は記録しない。"""
    if not IDENTIFIER.fullmatch(name):
        raise BatchError("ソース名は英数字と下線で指定してください。")
    return name


def quoted(name):
    """固定の文字集合に合うSQLite識別子を引用する。"""
    return '"' + source_name(name) + '"'


def row_values(destinations, fields):
    """SQLiteの型付けを確認し、空文字・NULL・整数を区別する。"""
    result = {}
    for column, value in zip(destinations, fields, strict=True):
        # SQLite REALは二進浮動小数点なので価格などを正確に復元できない。
        if type(value) is int:
            value = str(value)
        if value is not None and (
            not isinstance(value, str)
            or len(value) > MAX_FIELD_CHARS
            or "\x00" in value
        ):
            raise BatchError(
                "SQLite列の型またはセルの上限を確認してください。"
            )
        result[column] = value
    updated = result["updated_at"]
    if not isinstance(updated, str) or not UTC_SOURCE.fullmatch(updated):
        raise BatchError(
            "updated_atはZ付きのUTC時刻を文字列で指定してください。"
        )
    return result, canonical_timestamp(updated)


def table_rows(db, relation, columns, remaining):
    """通常表の実列だけを読み、ビュー・仮想表・計算列を拒否する。"""
    allowed = db.execute(
        """
        SELECT type FROM pragma_table_list(?)
        WHERE schema = 'main' AND name = ?
        """,
        (relation, relation),
    ).fetchone()
    names = quoted(relation)
    if allowed != ("table",) or relation.casefold().startswith("sqlite_"):
        raise BatchError("SQLiteのmainにある通常表を使用してください。")
    defined = {
        column[1]
        for column in db.execute(f"PRAGMA main.table_xinfo({names})")
        if column[6] == 0
    }
    if set(columns) - defined:
        raise BatchError("指定したSQLiteの列が存在しないか、計算列です。")
    fields = ", ".join(quoted(column) for column in columns)
    return db.execute(
        f"SELECT {fields} FROM main.{names} LIMIT ?", (remaining + 1,)
    )


def snapshot(path, mapping, contracts, *, name, installation, max_rows):
    """一つの読取トランザクションで通常表を走査し、原本を変更しない。"""
    if not 1 <= max_rows <= worker.MAX_ROWS:
        raise BatchError("全表の行数上限は1〜100000です。")
    before = fingerprint(path)
    encode = sql_snapshot.encode
    tables = {
        contract["table"]: {
            "schema": "main",
            "table": mapping[contract["table"]][1],
            "primary_key": contract["columns"][0],
            "rows": [],
        }
        for contract in contracts
    }
    document = {
        "format_version": 1,
        "source_system": worker.SQLITE_SOURCE,
        "source_identity": sha256(
            encode((worker.SQLITE_SOURCE, name, str(installation), mapping))
        ).hexdigest(),
        "source_database": name,
        "mode": "snapshot",
        "tables": tables,
    }
    size = len(encode(document))
    remaining = max_rows
    deadline = monotonic() + SNAPSHOT_SECONDS
    with closing(
        sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    ) as db:
        db.setconfig(sqlite3.SQLITE_DBCONFIG_TRUSTED_SCHEMA, False)
        db.setconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE, True)
        db.setlimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_ROW_BYTES)
        db.set_progress_handler(lambda: monotonic() > deadline, 1000)
        db.execute("PRAGMA query_only = ON")
        db.execute("BEGIN")
        observed_at = canonical_timestamp(
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )
        for contract in contracts:
            target, destinations = contract["table"], contract["columns"]
            _, relation, columns = mapping[target]
            emitted = tables[target]["rows"]
            for fields in table_rows(
                db, relation, [columns[c] for c in destinations], remaining
            ):
                if remaining == 0 or monotonic() > deadline:
                    raise BatchError(
                        "SQLiteスナップショットが行数または時間上限です。"
                    )
                values, updated = row_values(destinations, fields)
                row = {
                    "values": values,
                    "source_pk": values[destinations[0]],
                    "source_updated_at": updated,
                    "run_id": sha256(encode(values)).hexdigest()[:12],
                }
                if (
                    not isinstance(row["source_pk"], str)
                    or not worker.BUSINESS_KEY.fullmatch(row["source_pk"])
                    or updated > observed_at
                ):
                    raise BatchError(
                        "SQLiteの業務キーか更新時刻が契約に合いません。"
                    )
                size += len(encode(row)) + bool(emitted)
                if size > worker.MAX_BYTES:
                    raise BatchError("SQLiteバッチが16 MiBを超えました。")
                emitted.append(row)
                remaining -= 1
            # 物理配置・SQLiteの暗黙の行順とバッチIDを切り離す。
            emitted.sort(key=itemgetter("source_pk"))
            if monotonic() > deadline:
                raise BatchError("SQLiteスナップショットが時間上限です。")
        db.rollback()
    if before != fingerprint(path):
        raise BatchError("SQLiteバックアップが読み取り中に変更されました。")
    return document, observed_at


def main(argv=None):
    """エラーにDBファイルのパスや行を出さず、適用コマンドの項目だけを返す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", required=True, type=Path, help="単体のSQLiteバックアップ"
    )
    parser.add_argument(
        "--mapping", required=True, type=Path, help="6表の列対応JSON"
    )
    parser.add_argument("--source-name", required=True, help="ソースの論理名")
    parser.add_argument(
        "--source-id", required=True, type=UUID, help="安定した設置先UUID"
    )
    parser.add_argument(
        "--landing", required=True, type=Path, help="受け渡しディレクトリ"
    )
    parser.add_argument("--max-rows", default=worker.MAX_ROWS, type=int)
    args = parser.parse_args(argv)
    try:
        if (
            sys.version_info < (3, 12)
            or sqlite3.sqlite_version_info < (3, 37, 0)
            or args.source_id.int == 0
        ):
            raise BatchError(
                "SQLiteのバージョンと、設置先を識別するUUIDを確認してください。"
            )
        contracts = json.loads(CONTRACT_PATH.read_text())["sources"]
        mapping = sql_snapshot.read_mapping(
            args.mapping,
            contracts,
            identifier=IDENTIFIER,
            schema_allowed=lambda schema: schema == "main",
        )
        directory = safe_landing(safe_landing(args.landing) / "sql-batches")
        document, observed_at = snapshot(
            args.db.expanduser().absolute(),
            mapping,
            contracts,
            name=source_name(args.source_name),
            installation=args.source_id,
            max_rows=args.max_rows,
        )
        result = sql_snapshot.stage_document(
            document, observed_at, contracts, directory
        )
    except (OSError, ValueError, sqlite3.Error):
        print(
            "SQLiteの抽出に失敗しました。バックアップ、列対応、型、上限を確認してください。",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

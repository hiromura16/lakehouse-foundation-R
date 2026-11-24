"""明示したSQLソース対応と、内容で識別するスナップショットの受け渡し。"""

import json
import os
import stat
from hashlib import sha256
from pathlib import Path

import sql_batch_worker as worker
from sqlserver_batch import BatchError, publish_exact, unique_json

MAX_SETTINGS_BYTES = 64 * 1024


def encode(value):
    """空白を固定したUTF-8を、サイズ検査と内容IDの共通入力にする。"""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


def read_json(path):
    """設定のサイズを読み取り前に制限し、同名キーを拒否する。"""
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    with os.fdopen(os.open(path, flags), "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise BatchError("SQLの設定は通常ファイルで指定してください。")
        raw = handle.read(MAX_SETTINGS_BYTES + 1)
    if len(raw) > MAX_SETTINGS_BYTES:
        raise BatchError("設定JSONが上限を超えました。")
    return json.loads(raw, object_pairs_hook=unique_json)


def read_mapping(path, contracts, *, identifier, schema_allowed):
    """全6表と各列の明示対応を検査する。SQL式と重複対応は拒否する。"""
    body = read_json(Path(path))
    expected = {item["table"]: item["columns"] for item in contracts}
    if (
        not isinstance(body, dict)
        or set(body) != {"tables"}
        or not isinstance(body["tables"], list)
        or len(body["tables"]) != len(expected)
    ):
        raise BatchError("6個のテーブル対応を設定してください。")
    result = {}
    relations = set()
    for entry in body["tables"]:
        if not isinstance(entry, dict) or set(entry) != {
            "target_table",
            "source_schema",
            "source_table",
            "columns",
        }:
            raise BatchError("テーブル対応の項目を確認してください。")
        name, schema, table, columns = map(
            entry.get,
            ("target_table", "source_schema", "source_table", "columns"),
        )
        if (
            not isinstance(name, str)
            or name not in expected
            or name in result
            or any(
                not isinstance(part, str) or not identifier.fullmatch(part)
                for part in (schema, table)
            )
            or not schema_allowed(schema)
            or (schema, table) in relations
            or not isinstance(columns, dict)
            or set(columns) != set(expected[name])
            or any(
                not isinstance(column, str) or not identifier.fullmatch(column)
                for column in columns.values()
            )
            or len(set(columns.values())) != len(columns)
        ):
            raise BatchError("テーブル・列の識別子と重複を確認してください。")
        relations.add((schema, table))
        result[name] = (schema, table, columns)
    return result


def stage_document(document, observed_at, contracts, directory):
    """SQLバッチを一つの内容IDで確定し、ワーカー向けの操作項目を返す。"""
    worker.validate_document(document, contracts)
    payload = encode(document)
    if len(payload) > worker.MAX_BYTES:
        raise BatchError("SQLバッチの16 MiB上限を超えました。")
    batch_id = sha256(payload).hexdigest()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    publish_exact(directory / f"{batch_id}.json", payload)
    publish_exact(
        directory / f"{batch_id}.json.ready", (batch_id + "\n").encode()
    )
    return {
        "action": "sql-load",
        "batch_id": batch_id,
        "observed_at": observed_at,
    }

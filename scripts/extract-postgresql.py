#!/usr/bin/env python3
"""読取専用のPostgreSQLスナップショットをSQLバッチとして確定する。"""

import argparse
import ipaddress
import json
import os
import re
import stat
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from operator import itemgetter
from pathlib import Path
from time import monotonic
from uuid import UUID

import sql_batch_worker as worker
from sqlserver_batch import (
    MAX_FIELD_CHARS,
    BatchError,
    canonical_timestamp,
    publish_exact,
    safe_landing,
    unique_json,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "medallion" / "sources.json"
IDENTIFIER = worker.POSTGRESQL_IDENTIFIER
DNS_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
SETTINGS_KEYS = {
    "host",
    "port",
    "database",
    "user",
    "password",
    "ca_file",
    "source_id",
}
DATE_COLUMNS = {"planned_date", "order_date", "promised_date"}
TIME_COLUMNS = {
    "shipped_at",
    "event_time",
    "start_time",
    "end_time",
    "updated_at",
}
DECIMAL_COLUMNS = {"standard_cycle_seconds", "unit_price"}
COUNT_COLUMNS = {
    "planned_qty",
    "ordered_qty",
    "shipped_qty",
    "good_qty",
    "reject_qty",
}
INTEGER_TYPES = {"int2", "int4", "int8"}
MAX_SETTINGS_BYTES = 64 * 1024
MAX_CELL_BYTES = 4 * MAX_FIELD_CHARS
FETCH_ROWS = 16
SNAPSHOT_SECONDS = 120


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
    with path.open("rb") as handle:
        raw = handle.read(MAX_SETTINGS_BYTES + 1)
    if len(raw) > MAX_SETTINGS_BYTES:
        raise BatchError("設定JSONが上限を超えました。")
    return json.loads(raw, object_pairs_hook=unique_json)


def private_input(path, *, credentials=False):
    """外部の実ファイルを検査し、秘密や信頼CAの置換を防ぐ。"""
    path = Path(path).expanduser().absolute()
    if (
        path.resolve().is_relative_to(ROOT)
        or any(part.is_symlink() for part in (path, *path.parents))
        or not path.is_file()
    ):
        raise BatchError("checkout外の通常ファイルを指定してください。")
    metadata = path.stat()
    mask = 0o077 if credentials else 0o022
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_mode & mask
        or (credentials and metadata.st_uid != os.getuid())
    ):
        raise BatchError("設定とCAの所有者または権限を確認してください。")
    return path


def valid_host(value):
    """1台のDNS名またはIPだけを認め、接続文字列やソケットを受け取らない。"""
    if not isinstance(value, str) or not 1 <= len(value) <= 253:
        return False
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return all(DNS_LABEL.fullmatch(label) for label in value.split("."))
    return "%" not in value


def read_settings(path):
    """非公開JSONから固定の接続項目を読み取る。"""
    settings = read_json(private_input(path, credentials=True))
    if (
        not isinstance(settings, dict)
        or settings.keys() != SETTINGS_KEYS
        or not valid_host(settings["host"])
        or type(settings["port"]) is not int
        or not 1 <= settings["port"] <= 65535
        or any(
            not isinstance(settings[key], str)
            or not IDENTIFIER.fullmatch(settings[key])
            for key in ("database", "user")
        )
        or settings["database"].casefold() in {"template0", "template1"}
        or not isinstance(settings["password"], str)
        or not 1 <= len(settings["password"]) <= 1024
        or "\0" in settings["password"]
        or not isinstance(settings["ca_file"], str)
        or not Path(settings["ca_file"]).is_absolute()
        or not isinstance(settings["source_id"], str)
    ):
        raise BatchError("PostgreSQL接続の設定項目を確認してください。")
    source_id = UUID(settings["source_id"])
    if source_id.int == 0:
        raise BatchError("設置先を識別するUUIDを設定してください。")
    settings["source_id"] = str(source_id)
    settings["ca_file"] = str(private_input(settings["ca_file"]))
    return settings


def read_mapping(path, contracts):
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
                not isinstance(part, str) or not IDENTIFIER.fullmatch(part)
                for part in (schema, table)
            )
            or schema.casefold().startswith("pg_")
            or schema.casefold() == "information_schema"
            or (schema, table) in relations
            or not isinstance(columns, dict)
            or set(columns) != set(expected[name])
            or any(
                not isinstance(column, str) or not IDENTIFIER.fullmatch(column)
                for column in columns.values()
            )
            or len(set(columns.values())) != len(columns)
        ):
            raise BatchError("テーブル・列の識別子と重複を確認してください。")
        relations.add((schema, table))
        result[name] = (schema, table, columns)
    return result


def source_identity(settings, mapping):
    """パスワードや証明書の更新に影響されない、固定ソースのIDを作る。"""
    return sha256(
        encode(
            (
                worker.POSTGRESQL_SOURCE,
                settings["host"].casefold(),
                settings["port"],
                settings["database"],
                settings["source_id"],
                mapping,
            )
        )
    ).hexdigest()


def required_types(column):
    """事業日、時刻、数値、文字列を取り違える列マップを拒否する。"""
    if column in DATE_COLUMNS:
        return {"date"}
    if column in TIME_COLUMNS:
        return {"timestamptz"}
    if column in COUNT_COLUMNS:
        return INTEGER_TYPES
    if column in DECIMAL_COLUMNS:
        return INTEGER_TYPES | {"numeric"}
    return {"text", "varchar"}


def check_relation(connection, schema, table, columns):
    """対象が通常表であり、読取ロールに対象への書込権限がないことを調べる。"""
    info = connection.execute(
        """
        SELECT c.oid, c.relkind, c.relrowsecurity,
               has_table_privilege(c.oid, 'SELECT'),
               has_table_privilege(c.oid, 'DELETE,TRUNCATE,TRIGGER'),
               has_any_column_privilege(c.oid, 'INSERT,UPDATE,REFERENCES'),
               pg_has_role(c.relowner, 'MEMBER')
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    ).fetchone()
    if info is None or info[1:] != ("r", False, True, False, False, False):
        raise BatchError("通常表と専用のSELECT権限を指定してください。")
    attributes = dict(
        connection.execute(
            """
            SELECT a.attname, t.typname
            FROM pg_catalog.pg_attribute AS a
            JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
            JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
            WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
              AND n.nspname = 'pg_catalog'
            """,
            (info[0],),
        ).fetchall()
    )
    if any(
        attributes.get(source) not in required_types(target)
        for target, source in columns.items()
    ):
        raise BatchError("PostgreSQL列の型が入力契約と一致しません。")


def serialize(value):
    """真のNULLと空文字を区別し、日時とNUMERICを浮動小数へ渡さない。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.utcoffset() is None:
            raise BatchError("時刻列にはtimestamptzが必要です。")
        return (
            value.astimezone(timezone.utc)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z")
        )
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise BatchError("非有限のNUMERICを取り込めません。")
        return format(value, "f")
    if type(value) is int:
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise BatchError("入力契約にないPostgreSQL型です。")


def read_rows(connection, sql, schema, table, columns, remaining):
    """名前付きカーソルで、上限を超えるセルや行を受け取る前に判別する。"""
    fields = [sql.Identifier(column) for column in columns]
    lengths = [
        sql.SQL("pg_catalog.octet_length({}::pg_catalog.text)").format(field)
        for field in fields
    ]
    cap = sql.Literal(MAX_CELL_BYTES)
    checks = [
        sql.SQL("COALESCE({} > {}, FALSE)").format(length, cap)
        for length in lengths
    ]
    capped = [
        sql.SQL("CASE WHEN {} <= {} THEN {} END").format(length, cap, field)
        for length, field in zip(lengths, fields, strict=True)
    ]
    statement = sql.SQL("SELECT {}, ({}) FROM ONLY {} LIMIT %s").format(
        sql.SQL(", ").join(capped),
        sql.SQL(" OR ").join(checks),
        sql.Identifier(schema, table),
    )
    with connection.cursor(name="foundation_source_rows") as cursor:
        cursor.execute(statement, (remaining + 1,))
        while page := cursor.fetchmany(FETCH_ROWS):
            for *values, oversized in page:
                if oversized:
                    raise BatchError("ソースのセルが読取上限を超えました。")
                yield values


def connect_source(settings, driver):
    """libpqの環境設定とGSSによる迂回を避け、CAと接続名を必ず検証する。"""
    if any(
        item.envvar and item.envvar.decode() in os.environ
        for item in driver.pq.Conninfo.get_defaults()
    ):
        raise BatchError("実行環境から継承されたlibpqの設定を外してください。")
    return driver.connect(
        host=settings["host"],
        port=settings["port"],
        dbname=settings["database"],
        user=settings["user"],
        password=settings["password"],
        sslrootcert=settings["ca_file"],
        sslmode="verify-full",
        sslcertmode="disable",
        gssencmode="disable",
        client_encoding="UTF8",
        connect_timeout=5,
        options=(
            "-c search_path=pg_catalog -c TimeZone=UTC "
            "-c statement_timeout=30000 -c lock_timeout=5000 "
            "-c idle_in_transaction_session_timeout=120000"
        ),
    )


def snapshot(settings, mapping, contracts, *, max_rows=worker.MAX_ROWS):
    """6表を一つのMVCCスナップショットに固定する。"""
    import psycopg
    from psycopg import sql

    if type(max_rows) is not int or not 1 <= max_rows <= worker.MAX_ROWS:
        raise BatchError("全表の行数上限は1〜100000です。")
    table_metadata = {
        contract["table"]: {
            "schema": mapping[contract["table"]][0],
            "table": mapping[contract["table"]][1],
            "primary_key": contract["columns"][0],
            "rows": [],
        }
        for contract in contracts
    }
    document = {
        "format_version": 1,
        "source_system": worker.POSTGRESQL_SOURCE,
        "source_identity": source_identity(settings, mapping),
        "source_database": settings["database"],
        "mode": "snapshot",
        "tables": table_metadata,
    }
    remaining = max_rows
    size = len(encode(document))
    deadline = monotonic() + SNAPSHOT_SECONDS
    try:
        with connect_source(settings, psycopg) as connection:
            connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
            connection.read_only = True
            # DMLと両立するロックで、TRUNCATEと表再編を待たせる。
            # 6表すべてをロックし終えてから、最初のSELECTで可視性を確定する。
            relations = sql.SQL(", ").join(
                sql.SQL("ONLY {}").format(sql.Identifier(schema, table))
                for schema, table in sorted(
                    (schema, table) for schema, table, _ in mapping.values()
                )
            )
            connection.execute(
                sql.SQL("LOCK TABLE {} IN ACCESS SHARE MODE").format(relations)
            )
            observed, database, privileged = connection.execute(
                """
                SELECT statement_timestamp(), current_database(),
                       rolsuper OR rolcreaterole OR rolcreatedb
                       OR rolreplication OR rolbypassrls
                FROM pg_catalog.pg_roles WHERE rolname = current_user
                """
            ).fetchone()
            if privileged or database != settings["database"]:
                raise BatchError(
                    "ソースには権限を限定した専用ロールが必要です。"
                )
            observed_at = canonical_timestamp(serialize(observed))
            for contract in contracts:
                name, destinations = contract["table"], contract["columns"]
                schema, table, columns = mapping[name]
                check_relation(connection, schema, table, columns)
                rows = table_metadata[name]["rows"]
                seen = set()
                with_source = [columns[column] for column in destinations]
                for data in read_rows(
                    connection, sql, schema, table, with_source, remaining
                ):
                    if remaining == 0 or monotonic() > deadline:
                        raise BatchError(
                            "スナップショットが行数または時間上限を超えました。"
                        )
                    values = {
                        column: serialize(value)
                        for column, value in zip(
                            destinations, data, strict=True
                        )
                    }
                    key, updated_at = (
                        values[destinations[0]],
                        values["updated_at"],
                    )
                    if (
                        key is None
                        or not worker.BUSINESS_KEY.fullmatch(key)
                        or key in seen
                        or updated_at is None
                        or any(
                            v is not None and len(v) > MAX_FIELD_CHARS
                            for v in values.values()
                        )
                    ):
                        raise BatchError(
                            "主キー、日時、空値または文字数を確認してください。"
                        )
                    row = {
                        "values": values,
                        "source_pk": key,
                        "source_updated_at": canonical_timestamp(updated_at),
                        "run_id": sha256(encode(values)).hexdigest()[:12],
                    }
                    size += len(encode(row)) + bool(rows)
                    if size > worker.MAX_BYTES:
                        raise BatchError("SQLバッチの16 MiB上限を超えました。")
                    remaining -= 1
                    seen.add(key)
                    rows.append(row)
                # PostgreSQLの照合順序や物理配置が内容IDを変えないようにする。
                rows.sort(key=itemgetter("source_pk"))
                if monotonic() > deadline:
                    raise BatchError(
                        "スナップショットが時間上限を超えました。"
                    )
    except psycopg.Error:
        raise BatchError(
            "PostgreSQLのTLS接続または読取に失敗しました。"
            "CA、接続名、認証、権限、列の型を確認してください。"
        ) from None
    return document, observed_at


def prepare(
    settings, mapping, contracts, landing, *, max_rows=worker.MAX_ROWS
):
    """全表検証後にバッチを先行、完了印を最後に公開する。"""
    directory = safe_landing(safe_landing(landing) / "sql-batches")
    document, observed_at = snapshot(
        settings, mapping, contracts, max_rows=max_rows
    )
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


def main():
    """ソース秘密を表示せず、操作APIに渡す3項目を出力する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--settings", type=Path, required=True, help="非公開接続JSON"
    )
    parser.add_argument(
        "--mapping", type=Path, required=True, help="6表の列対応JSON"
    )
    parser.add_argument(
        "--landing", type=Path, required=True, help="受け渡しディレクトリ"
    )
    parser.add_argument("--max-rows", type=int, default=worker.MAX_ROWS)
    args = parser.parse_args()
    try:
        contracts = json.loads(CONTRACT_PATH.read_text())["sources"]
        result = prepare(
            read_settings(args.settings),
            read_mapping(args.mapping, contracts),
            contracts,
            args.landing,
            max_rows=args.max_rows,
        )
    except ModuleNotFoundError:
        print(
            "PostgreSQL抽出用の固定依存を導入してください。", file=sys.stderr
        )
        return 1
    except BatchError as error:
        # 自前と既存のバッチ境界は固定文だけを返す。DB例外はここに渡さない。
        print(error, file=sys.stderr)
        return 1
    except (OSError, ValueError):
        print(
            "PostgreSQLの抽出設定または出力先を確認してください。",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

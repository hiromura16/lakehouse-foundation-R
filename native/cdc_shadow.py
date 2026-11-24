"""標準native.cdcの確定済みRawを、新規ローカルIcebergへ再生する。"""

import argparse
import json
import logging
import os
import re
import stat
import sys
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from importlib.metadata import version
from itertools import zip_longest
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from native import catalog_schema, engine

ROOT = Path(__file__).resolve().parents[1]
CDC = ROOT / "cdc" if (ROOT / "cdc").is_dir() else ROOT / "infra/cdc"
sys.path.insert(0, str(CDC))
from cdc_replay import flush_sqlite  # noqa: E402
from config import (  # noqa: E402
    mapping_document,
    reject_duplicate_keys,
    settings,
    topic_names,
)
from consumer import identity_digest, process, source_identity  # noqa: E402
from event_limits import MAX_RECORD_BYTES  # noqa: E402
from raw_writer import (  # noqa: E402
    RAW_COLUMNS,
    RawDecodeError,
    generic_null_columns,
    generic_payload,
    raw_rows,
)
from state import State  # noqa: E402
from state import version as state_version  # noqa: E402

RAW = "cdc_events.events"
LEGACY_FORMAT = "foundation-r-generic-cdc-shadow-v1"
FORMAT = "foundation-r-generic-cdc-shadow-v2"
PARTITION_FORMAT = "foundation-r-generic-cdc-shadow-v3"
MAX_PARTITIONS_PER_TOPIC = 16
MAX_PARTITIONS = 64
MAX_EVENTS = 1000
MAX_SCAN = 10000
MAX_BUNDLE = 64 * 1024 * 1024
CONFIG_LIMIT = 32 * 1024
SOURCE_KEYS = frozenset(
    {
        "CDC_SQLSERVER_DATABASE",
        "CDC_SQLSERVER_HOST",
        "CDC_SQLSERVER_PORT",
        "CDC_TOPIC_PREFIX",
    }
)
APPROVAL_KEYS = frozenset(
    {
        "source_sha256",
        "raw_table_uuid",
        "raw_snapshot_id",
        "completed",
    }
)
RECORD_FIELDS = frozenset(
    {
        "topic",
        "partition",
        "offset",
        "key",
        "value",
        "ingested_at",
    }
)
SHA256 = re.compile(r"[0-9a-f]{64}")
OFFSET = re.compile(r"(?:0|[1-9][0-9]{0,18})")


class ShadowError(ValueError):
    """元データや秘密情報を含めない固定の診断コード。"""


@dataclass(frozen=True)
class Source:
    """稼働のsource識別と表対応だけを持つ。Kafka接続資格を持たない。"""

    values: dict
    mapping: dict
    topics: dict

    @property
    def identity(self):
        """稼働のSQLiteと同じ識別規則を再使用する。"""
        return source_identity(self.values, self.mapping)


def read_bounded(path, limit):
    """読込上限を適用し、入力ファイル全体を検査する。"""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ShadowError("CDC_SHADOW_INPUT_NOT_REGULAR")
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise ShadowError("CDC_SHADOW_SIZE_LIMIT")
    return body


def parse(body):
    """重複キーと標準JSONでない浮動小数点値を拒む。"""
    return json.loads(
        body,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )


def reject_constant(_):
    """NaNと無限大の別解釈を認めない。"""
    raise ShadowError("CDC_SHADOW_JSON_CONSTANT")


def encoded(value):
    """バンドル承認用にバイト列の生成順を固定する。"""
    return (
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        + "\n"
    ).encode()


def source_from_files(source_config, mapping_path):
    """Kafka設定と秘密を持たないsource定義を、稼働時と同じ規則で検証する。"""
    source = parse(read_bounded(source_config, CONFIG_LIMIT))
    if (
        not isinstance(source, dict)
        or source.keys() != {"source", "topic_has_database"}
        or type(source["topic_has_database"]) is not bool
        or not isinstance(source["source"], dict)
        or not source["source"].keys() <= SOURCE_KEYS
        or not all(isinstance(text, str) for text in source["source"].values())
    ):
        raise ShadowError("CDC_SHADOW_SOURCE_CONFIG_INVALID")
    values = settings(source["source"], require_connector=False)
    # 検証したバイト列を再オープンせず、稼働側と共通の規則で解釈する。
    mapping = mapping_document(parse(read_bounded(mapping_path, CONFIG_LIMIT)))
    count = 4 if source["topic_has_database"] else 3
    topics = {
        topic: target
        for topic, target in topic_names(values, mapping).items()
        if len(topic.split(".")) == count
    }
    return Source(values, mapping, topics)


def check_approval(source, approval):
    """旧承認と複数区画承認を検査し、内部の配送位置の表現を統一する。"""
    if not isinstance(approval, dict) or approval.keys() != APPROVAL_KEYS:
        raise ShadowError("CDC_SHADOW_APPROVAL_INVALID")
    completed = approval["completed"]
    if (
        identity_digest(source.identity) != approval["source_sha256"]
        or type(approval["raw_snapshot_id"]) is not int
        or not 0 < approval["raw_snapshot_id"] < 2**63
        or not isinstance(completed, dict)
        or completed.keys() != source.topics.keys()
    ):
        raise ShadowError("CDC_SHADOW_APPROVAL_INVALID")
    try:
        if str(UUID(approval["raw_table_uuid"])) != approval["raw_table_uuid"]:
            raise ValueError("noncanonical UUID")
    except (TypeError, ValueError, AttributeError):
        raise ShadowError("CDC_SHADOW_APPROVAL_INVALID") from None
    # v1/v2の単一整数は区画0の位置。v3は空区画を含めて全区画を列挙する。
    bundle_format = FORMAT
    if all(type(offset) is int for offset in completed.values()):
        positions = {(topic, 0): offset for topic, offset in completed.items()}
    else:
        bundle_format = PARTITION_FORMAT
        positions = {}
        for topic, partitions in completed.items():
            if (
                not isinstance(partitions, dict)
                or not 1 <= len(partitions) <= MAX_PARTITIONS_PER_TOPIC
                or partitions.keys()
                != {str(index) for index in range(len(partitions))}
            ):
                raise ShadowError("CDC_SHADOW_APPROVAL_INVALID")
            positions.update(
                ((topic, int(partition)), offset)
                for partition, offset in partitions.items()
            )
    if len(positions) > MAX_PARTITIONS or any(
        type(offset) is not int or not -1 <= offset < MAX_EVENTS
        for offset in positions.values()
    ):
        raise ShadowError("CDC_SHADOW_APPROVAL_INVALID")
    if not 0 < sum(offset + 1 for offset in positions.values()) <= MAX_EVENTS:
        raise ShadowError("CDC_SHADOW_EVENT_LIMIT")
    return bundle_format, positions


def check_record(record):
    """Rawが消去したNULLと空の区別は推測しない。"""
    if (
        not isinstance(record, dict)
        or record.keys() != RECORD_FIELDS
        or type(record["partition"]) is not int
        or not 0 <= record["partition"] < MAX_PARTITIONS_PER_TOPIC
        or type(record["offset"]) is not int
        or not 0 <= record["offset"] < MAX_EVENTS
        or any(
            not isinstance(record[key], str)
            for key in ("topic", "ingested_at")
        )
    ):
        raise ShadowError("CDC_SHADOW_RECORD_INVALID")
    key, value = record["key"], record["value"]
    if any(
        item is not None and not isinstance(item, str) for item in (key, value)
    ):
        raise ShadowError("CDC_SHADOW_RECORD_INVALID")
    if (
        len((key or "").encode()) + len((value or "").encode())
        > MAX_RECORD_BYTES
    ):
        raise ShadowError("CDC_SHADOW_SIZE_LIMIT")
    if datetime.fromisoformat(record["ingested_at"]).utcoffset() != timedelta(
        0
    ):
        raise ShadowError("CDC_SHADOW_OBSERVED_TIME_INVALID")
    # 標準CDCはKafkaのNULL本文を業務行に反映せず、配送位置だけ確定する。
    if value is None:
        return None, None
    if not key or not value:
        raise ShadowError("CDC_SHADOW_INVALID_EVENT_BODY")
    key_doc = parse(key)
    if not isinstance(key_doc, dict):
        raise ShadowError("CDC_SHADOW_RECORD_INVALID")
    envelope = parse(value)
    if not isinstance(envelope, dict):
        raise ShadowError("CDC_SHADOW_RECORD_INVALID")
    return key_doc, envelope


def check_partition_record(source, record, key, envelope, owners):
    """業務キーの所属と、同じSQL Serverの変更版であることを検証する。"""
    if record["value"] is None:
        # キーを持たないトゥームストーンは配送位置だけを確定する。
        if not record["key"]:
            return
        try:
            key = parse(record["key"])
        except (TypeError, ValueError):
            raise ShadowError("CDC_SHADOW_PARTITION_KEY_INVALID") from None
    topic = record["topic"]
    target = source.topics[topic]
    mapping = source.mapping[target]
    contract = next(
        item for item in catalog_schema.CONTRACT if item["table"] == target
    )
    primary = contract["columns"][0]
    source_key = next(
        name
        for name, column in mapping["columns"].items()
        if column == primary
    )
    if (
        not isinstance(key, dict)
        or key.keys() != {source_key}
        or key[source_key] is None
        or str(key[source_key]) == ""
    ):
        raise ShadowError("CDC_SHADOW_PARTITION_KEY_INVALID")
    # SQLiteの現在値が使うキーと同じ文字列表現で比較する。
    identity = (target, str(key[source_key]))
    owner = (topic, record["partition"])
    if owners.setdefault(identity, owner) != owner:
        raise ShadowError("CDC_SHADOW_KEY_CHANGED_PARTITION")

    if envelope is not None:
        origin = envelope.get("source")
        if (
            not isinstance(origin, dict)
            or origin.get("db") != source.identity["database"]
            or origin.get("schema") != mapping["source_schema"]
            or origin.get("table") != mapping["source_table"]
        ):
            raise ShadowError("CDC_SHADOW_EVENT_SOURCE_MISMATCH")
        if envelope.get("op") not in ("r", "c", "u", "d"):
            raise ShadowError("CDC_SHADOW_EVENT_ORDER_INVALID")
        try:
            # 最大の版を選ぶ旧State規則を使い、区画をまたぐ到着順は推定しない。
            state_version(envelope, record["offset"])
        except (ValueError, TypeError):
            raise ShadowError("CDC_SHADOW_EVENT_ORDER_INVALID") from None


def checked_sequence(records, completed, *, source=None):
    """隔離した新規状態を作る前に、全承認位置の網羅性と順序を確認する。"""
    if not isinstance(records, list) or len(records) > MAX_EVENTS:
        raise ShadowError("CDC_SHADOW_EVENT_LIMIT")
    expected = (
        (topic, partition, offset)
        for (topic, partition), last in sorted(completed.items())
        for offset in range(last + 1)
    )
    owners = {}
    for record, position in zip_longest(records, expected):
        if record is None or position is None:
            raise ShadowError("CDC_SHADOW_HISTORY_GAP")
        key, envelope = check_record(record)
        delivery = (record["topic"], record["partition"], record["offset"])
        if delivery != position:
            raise ShadowError("CDC_SHADOW_HISTORY_GAP")
        if source is not None:
            check_partition_record(source, record, key, envelope, owners)


def decode_payload(row):
    """判定列のない歴史データと形が壊れた行を、別の理由で拒否する。"""
    try:
        return generic_payload(row)
    except RawDecodeError as error:
        if str(error) == "CDC_RAW_LEGACY_EMPTY":
            raise ShadowError("CDC_SHADOW_AMBIGUOUS_RAW") from None
        raise ShadowError("CDC_SHADOW_RAW_MISMATCH") from None


def records_from_table(table, snapshot_id, completed, *, source=None):
    """元Rawから承認位置までを取り、未確定の末尾は読み捨てる。"""
    records = []
    scanned = size = 0
    scan = table.scan(snapshot_id=snapshot_id, limit=MAX_SCAN + 1)
    # 列の追加前に作られたIceberg世代は、当時のSchema IDで投影する。
    schema = scan.projection().as_arrow()
    if not set(RAW_COLUMNS) <= set(schema.names):
        raise ShadowError("CDC_SHADOW_RAW_MISMATCH")
    markers = generic_null_columns(schema)
    with scan.select(*RAW_COLUMNS, *markers).to_arrow_batch_reader() as reader:
        for batch in reader:
            scanned += batch.num_rows
            size += batch.nbytes
            if scanned > MAX_SCAN or size > MAX_BUNDLE:
                raise ShadowError("CDC_SHADOW_SCAN_LIMIT")
            for row in batch.to_pylist():
                topic = row["topic"]
                if (
                    not isinstance(row["partition"], str)
                    or not OFFSET.fullmatch(row["partition"])
                    or not isinstance(row["offset"], str)
                    or not OFFSET.fullmatch(row["offset"])
                ):
                    raise ShadowError("CDC_SHADOW_PARTITION_UNSUPPORTED")
                partition = int(row["partition"])
                if (topic, partition) not in completed:
                    raise ShadowError("CDC_SHADOW_PARTITION_UNSUPPORTED")
                offset = int(row["offset"])
                if offset >= 2**63:
                    raise ShadowError("CDC_SHADOW_RECORD_INVALID")
                if offset > completed[(topic, partition)]:
                    continue
                record = {
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "ingested_at": row["ingested_at"],
                    **decode_payload(row),
                }
                if raw_rows([record])[0] != {
                    name: row[name] for name in RAW_COLUMNS
                }:
                    raise ShadowError("CDC_SHADOW_RECORD_INVALID")
                records.append(record)
    records.sort(
        key=lambda row: (row["topic"], row["partition"], row["offset"])
    )
    checked_sequence(records, completed, source=source)
    return records


def write_new(path, body):
    """0600の新規ファイルだけを書き、完了後に同期する。"""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())


def replay_recipe():
    """SQLと行の解釈コード、主要依存の版を同じ世代に固定する。"""
    versions = {
        name: version(name) for name in ("duckdb", "pyarrow", "pyiceberg")
    }
    modules = (
        CDC / "state.py",
        CDC / "consumer.py",
        ROOT / "native/engine.py",
        ROOT / "native/catalog_schema.py",
        ROOT / "medallion/sources.json",
    )
    recipe = engine.business_queries("cdc_")[0]
    digest = sha256(encoded({"sql": recipe, "versions": versions}))
    for module in modules:
        digest.update(module.read_bytes())
    return digest.hexdigest()


def export_bundle(load_table, source, approval, output):
    """読み取り専用カタログのload_tableを受け、承認済みRawだけを書き出す。

    呼出側はCatalog GETとObject Store HEAD/GETだけを許可する権限を使い、
    credential vending、remote signing、server-side planningを無効にする。
    approvalは稼働を停止または調停して採取し、バンドルのSHAは別経路で承認する。
    """
    bundle_format, completed = check_approval(source, approval)
    table = load_table(RAW)
    snapshot = approval["raw_snapshot_id"]
    if (
        table.metadata.format_version != 2
        or urlsplit(table.location()).scheme != "s3"
        or str(table.metadata.table_uuid) != approval["raw_table_uuid"]
        or table.snapshot_by_id(snapshot) is None
        or table.config.get("scan-planning-mode", "client") != "client"
        or table.catalog.supports_server_side_planning()
    ):
        raise ShadowError("CDC_SHADOW_RAW_MISMATCH")
    body = encoded(
        {
            "format": bundle_format,
            "approval": approval,
            "recipe": replay_recipe(),
            "records": records_from_table(
                table,
                snapshot,
                completed,
                source=source if bundle_format == PARTITION_FORMAT else None,
            ),
        }
    )
    if len(body) > MAX_BUNDLE:
        raise ShadowError("CDC_SHADOW_SIZE_LIMIT")
    write_new(output, body)
    sync_directory(output.parent)
    return sha256(body).hexdigest()


class ShadowAdmin:
    """パスに固定したカタログだけを、通常のメダリオン処理へ接続する。"""

    CONTRACT = catalog_schema.CONTRACT
    schema = staticmethod(catalog_schema.schema)

    def __init__(self, root):
        from pyiceberg.catalog.sql import SqlCatalog
        from sqlalchemy import URL

        self.STATE = {}
        self._catalog = SqlCatalog(
            "cdc-shadow",
            uri=URL.create("sqlite", database=str(root / "catalog.db")),
            warehouse=(root / "warehouse").as_uri(),
        )

    def catalog(self):
        return self._catalog

    def ensure_tables(self, prefix):
        catalog_schema.ensure_tables(self.catalog(), self.CONTRACT, prefix)

    def medallion(self, prefix):
        return engine.medallion(self, prefix)


def sync_directory(directory):
    """候補ファイル名を永続化する。"""
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def replay_bundle(bundle, manifest_sha256, source, output):
    """承認済みバイト列を、新しいローカルカタログとStateへ再生する。"""
    data = read_bounded(bundle, MAX_BUNDLE)
    if (
        not SHA256.fullmatch(manifest_sha256)
        or sha256(data).hexdigest() != manifest_sha256
    ):
        raise ShadowError("CDC_SHADOW_APPROVAL_HASH_MISMATCH")
    document = parse(data)
    if (
        not isinstance(document, dict)
        or document.keys() != {"format", "approval", "recipe", "records"}
        or document["format"] not in {FORMAT, LEGACY_FORMAT, PARTITION_FORMAT}
        or document["recipe"] != replay_recipe()
    ):
        raise ShadowError("CDC_SHADOW_BUNDLE_INVALID")
    approval = document["approval"]
    bundle_format, completed = check_approval(source, approval)
    partitioned = bundle_format == PARTITION_FORMAT
    if partitioned != (document["format"] == PARTITION_FORMAT):
        raise ShadowError("CDC_SHADOW_BUNDLE_INVALID")
    records = document["records"]
    checked_sequence(
        records, completed, source=source if partitioned else None
    )
    if document["format"] == LEGACY_FORMAT and any(
        record["key"] is None or record["value"] is None for record in records
    ):
        raise ShadowError("CDC_SHADOW_BUNDLE_INVALID")

    # 既存dirと末端symlinkを辿らず、管理者が保護した親の直下だけへ作る。
    output = output.parent.resolve(strict=True) / output.name
    output.mkdir(mode=0o700, exist_ok=False)
    candidate = output / ".state.sqlite3"
    admin = ShadowAdmin(output)
    try:
        state = State(candidate, source.identity)
        with closing(state.connection):
            # 正常系と同じRaw追記、行反映、派生表、完了位置の順序を用いる。
            result = process(
                admin,
                state,
                records,
                source.mapping,
                source.topics,
                values=source.values,
            )
            if partitioned and state.completed_offsets() != {
                position: offset
                for position, offset in completed.items()
                if offset >= 0
            }:
                raise ShadowError("CDC_SHADOW_CHECKPOINT_MISMATCH")
            state.set("initialized", "true")
            flush_sqlite(state)
        os.replace(candidate, output / "state.sqlite3")
        sync_directory(output)
        evidence = {
            "source_sha256": approval["source_sha256"],
            "raw_table_uuid": approval["raw_table_uuid"],
            "raw_snapshot_id": approval["raw_snapshot_id"],
            **result,
        }
        write_new(output / "shadow-ready.json", encoded(evidence))
        sync_directory(output)
        sync_directory(output.parent)
        return evidence
    finally:
        admin.catalog().engine.dispose()


def main():
    """外部へ接続しない隔離再生CLI。元カタログの認証情報を受け取らない。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--approval-sha256", required=True)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--mapping", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        result = replay_bundle(
            args.bundle,
            args.approval_sha256,
            source_from_files(args.source, args.mapping),
            args.output,
        )
    except ShadowError as error:
        print(error, file=sys.stderr)
        return 1
    except Exception:
        print("CDC_SHADOW_REPLAY_FAILED", file=sys.stderr)
        return 1
    print(encoded(result).decode(), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

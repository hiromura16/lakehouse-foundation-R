"""接続設定と独立して、既存Lakehouseの業務表契約を作成する。"""

import json
import re
from pathlib import Path

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads(
    (ROOT / "medallion/sources.json").read_text(encoding="utf-8")
)["sources"]


def schema(prefix, layer):
    """許可した用途のprefixと層名を結び、namespace名を返す。

    通常処理、CDC、実行ごとの隔離検証だけを受け付ける。任意prefixを
    許してしまうと、検証の後片付けが通常表へ及ぶため入力を制限する。
    """
    if prefix not in {"", "cdc_", "sql_", "sqlcdc_"} and not re.fullmatch(
        r"verify_[0-9a-f]+_", prefix
    ):
        raise ValueError("名前空間の接頭辞が許可されていません。")
    return prefix + layer


def ensure_tables(cat, contract, prefix=""):
    """選択namespaceに不足する契約表と原本台帳を作成する。

    Bronzeは業務値と由来を文字列として保存する。既存表は保持するため、
    契約の列を変更しても既存schemaが自動で移行するわけではない。
    """
    for layer in ["bronze", "silver", "gold", "quarantine"]:
        namespace = schema(prefix, layer)
        if not cat.namespace_exists(namespace):
            cat.create_namespace(namespace)
    for source in contract:
        identifier = schema(prefix, "bronze") + "." + source["table"]
        if not cat.table_exists(identifier):
            fields = source["columns"] + [
                "_source_file",
                "_source_hash",
                "_ingested_at",
            ]
            if prefix in {"sql_", "sqlcdc_"}:
                # SQL観測の由来列はSQL経路だけに追加し、
                # ファイル・汎用CDCの表契約を変えない。
                from scripts.sql_batch_worker import SQL_SOURCE_COLUMNS

                fields += list(SQL_SOURCE_COLUMNS)
            cat.create_table(
                identifier,
                schema=pa.schema([(name, pa.string()) for name in fields]),
                properties={"format-version": "2"},
            )
    for table, columns in [
        ("files", ["path", "sha256", "s3_uri", "mime", "bytes"]),
        (
            "images",
            ["path", "sha256", "s3_uri", "mime", "bytes", "width", "height"],
        ),
    ]:
        identifier = schema(prefix, "bronze") + "." + table
        if not cat.table_exists(identifier):
            cat.create_table(
                identifier,
                schema=pa.schema([(name, pa.string()) for name in columns]),
                properties={"format-version": "2"},
            )

    # 画像として解釈できなかった原本も、台帳から参照できるよう理由を保全する。
    image_issues = schema(prefix, "quarantine") + ".image_files"
    if not cat.table_exists(image_issues):
        fields = ("path", "sha256", "s3_uri", "mime", "bytes", "reason")
        cat.create_table(
            image_issues,
            schema=pa.schema([(name, pa.string()) for name in fields]),
            properties={"format-version": "2"},
        )

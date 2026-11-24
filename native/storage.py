"""Icebergカタログ、オブジェクト保存、業務テーブルの初期化。

標準ワーカーとCDCが同じカタログを使う。表の名前空間、原本のS3接続、
既存warehouseの照合はこのモジュールに集約する。
"""

import json
import os
import re
import threading
from pathlib import Path

import boto3
import pyarrow as pa
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

ROOT = Path(__file__).resolve().parents[1]
CATALOG_ORIGIN = "http://catalog:8181"
STORAGE_ORIGIN = "http://storage:8333"
HTTP = requests.Session()
# 内部カタログへの資格情報付き操作はホストのproxy設定を通さない。
HTTP.trust_env = False
S3 = boto3.client(
    "s3",
    endpoint_url=STORAGE_ORIGIN,
    region_name="us-east-1",
    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    config=Config(
        s3={"addressing_style": "path"},
        connect_timeout=5,
        read_timeout=30,
        proxies={},
    ),
)
LOCK = threading.Lock()
INGESTION_ATTEMPT = None
STATE = {
    "ready": False,
    "blocked": False,
    "successful_runs": 0,
    "failed_runs": 0,
    "partial_runs": 0,
    "last_success": 0,
    "imported_files": 0,
}
CONTRACT = json.loads((ROOT / "medallion/sources.json").read_text())["sources"]


def request(method, url, payload=None, accepted=(200, 201, 204)):
    """カタログ管理APIへJSONを送り、許可したHTTP成功時の本文を返す。

    作成系の呼出しには副作用がある。異常時はstatusだけを例外へ含め、
    資格情報を含み得る要求・応答本文は利用者へ返さない。
    """
    response = HTTP.request(
        method, url, json=payload, timeout=30, allow_redirects=False
    )
    if response.status_code not in accepted:
        raise RuntimeError(
            "Service returned HTTP " + str(response.status_code)
        )
    return response.json() if response.content else None


def catalog():
    """privateなS3資格情報で、共有のIceberg REST catalogを開く。

    表の操作先を返すだけで、ここでは表を作成しない。FileIOはclient側の
    鍵を使い、カタログ側の署名サービスへ暗黙に切り替えない。
    """
    return load_catalog(
        "lakehouse",
        type="rest",
        uri=CATALOG_ORIGIN + "/catalog",
        warehouse=os.environ["WAREHOUSE_NAME"],
        **{
            # 共通設定の鍵をFileIOへ渡し、catalog側署名へ暗黙移行しない。
            "header.X-Iceberg-Access-Delegation": "client-managed",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": STORAGE_ORIGIN,
            "s3.region": "us-east-1",
            "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
            "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
            "s3.force-virtual-addressing": "false",
        },
    )


def bootstrap():
    """不足するbucket・warehouse・空の契約表を準備する。

    既存warehouseはbucketとendpointが一致する場合に再利用する。
    既存データや鍵を再生成せず、既存表のschema移行も行わない。
    """
    for bucket in [
        os.environ["S3_BUCKET_RAW"],
        os.environ["S3_BUCKET_WAREHOUSE"],
    ]:
        try:
            S3.head_bucket(Bucket=bucket)
        except ClientError as error:
            if str(error.response["Error"]["Code"]) not in {
                "404",
                "NoSuchBucket",
                "NotFound",
            }:
                raise
            S3.create_bucket(Bucket=bucket)
    info = request("GET", CATALOG_ORIGIN + "/management/v1/info")
    if not info.get("bootstrapped"):
        request(
            "POST",
            CATALOG_ORIGIN + "/management/v1/bootstrap",
            {"accept-terms-of-use": True},
        )
    warehouses = request(
        "GET", CATALOG_ORIGIN + "/management/v1/warehouse"
    ).get("warehouses", [])
    matching = [
        r
        for r in warehouses
        if r.get("name", r.get("warehouse-name"))
        == os.environ["WAREHOUSE_NAME"]
    ]
    if not matching:
        request(
            "POST",
            CATALOG_ORIGIN + "/management/v1/warehouse",
            {
                "warehouse-name": os.environ["WAREHOUSE_NAME"],
                "project-id": "00000000-0000-0000-0000-000000000000",
                "storage-profile": {
                    "type": "s3",
                    "bucket": os.environ["S3_BUCKET_WAREHOUSE"],
                    "key-prefix": "iceberg",
                    "endpoint": STORAGE_ORIGIN,
                    "region": "us-east-1",
                    "path-style-access": True,
                    "flavor": "s3-compat",
                    "sts-enabled": False,
                },
                "storage-credential": {
                    "type": "s3",
                    "credential-type": "access-key",
                    "access-key-id": os.environ["S3_ACCESS_KEY"],
                    "secret-access-key": os.environ["S3_SECRET_KEY"],
                },
            },
        )
    else:
        profile = matching[0].get("storage-profile", {})
        if (
            profile.get("bucket") != os.environ["S3_BUCKET_WAREHOUSE"]
            or str(profile.get("endpoint", "")).rstrip("/") != STORAGE_ORIGIN
        ):
            raise RuntimeError(
                "既存warehouseのストレージ設定が一致しないため再利用できません。"
            )
    ensure_tables()


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


def ensure_tables(prefix=""):
    """選択namespaceに不足する契約表と原本台帳を作成する。

    Bronzeは業務値と由来を文字列として保存する。既存表は保持するため、
    契約の列を変更しても既存schemaが自動で移行するわけではない。
    """
    cat = catalog()
    for layer in ["bronze", "silver", "gold", "quarantine"]:
        namespace = schema(prefix, layer)
        if not cat.namespace_exists(namespace):
            cat.create_namespace(namespace)
    for source in CONTRACT:
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


def overwrite(table, rows, column, value):
    """指定列の値に一致する範囲を、渡された行集合へ置き換える。

    Arrowの型は対象Iceberg表へ合わせる。1表の置換を確定する操作であり、
    原本保存や別表の更新をまとめたtransactionではない。
    """
    arrow = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    table.overwrite(arrow, overwrite_filter=EqualTo(column, value))

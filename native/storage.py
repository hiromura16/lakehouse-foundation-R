"""Icebergカタログ、オブジェクト保存、業務テーブルの初期化。

標準ワーカーとCDCが同じカタログを使う。表の名前空間、原本のS3接続、
既存warehouseの照合はこのモジュールに集約する。
"""

import os
import threading

import boto3
import pyarrow as pa
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo

from native.catalog_schema import CONTRACT as CONTRACT
from native.catalog_schema import ensure_tables as ensure_catalog_tables
from native.catalog_schema import schema as schema

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


def ensure_tables(prefix=""):
    """稼働カタログに既存の業務表契約を準備する。"""
    return ensure_catalog_tables(catalog(), CONTRACT, prefix)


def overwrite(table, rows, column, value):
    """指定列の値に一致する範囲を、渡された行集合へ置き換える。

    Arrowの型は対象Iceberg表へ合わせる。1表の置換を確定する操作であり、
    原本保存や別表の更新をまとめたtransactionではない。
    """
    arrow = pa.Table.from_pylist(rows, schema=table.schema().as_arrow())
    table.overwrite(arrow, overwrite_filter=EqualTo(column, value))

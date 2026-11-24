"""合成入力と独立した期待値を検証用の名前空間で照合する。"""

import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlsplit

from PIL import Image

from native.landing import ingest
from native.storage import ROOT, S3, catalog, schema


def image_fixture(path, data):
    """検査用の1枚と、それに対応する完了印を更新する。"""
    path.write_bytes(data)
    digest = sha256(data).hexdigest()
    path.with_name(path.name + ".ready").write_text(digest + "\n")
    return digest


def image_raw(record):
    """検査用画像の原本を、記録された版の位置から読み取る。"""
    uri = urlsplit(record["s3_uri"])
    with S3.get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))[
        "Body"
    ] as stream:
        return stream.read()


def verify_image_replacement(cat, prefix, path, good_bytes, first_record):
    """破損版を保全し、同じ場所に正常版を再送して画像一覧を回復する。"""
    bronze = schema(prefix, "bronze") + ".images"
    quarantine = schema(prefix, "quarantine") + ".image_files"
    root = path.parent.parent
    rejected = b"invalid inspection image"
    bad_hash = image_fixture(path, rejected)
    if ingest(root, prefix)["imported"] != 1:
        raise AssertionError("破損画像の原本を保全できませんでした。")
    if cat.load_table(bronze).scan().to_arrow().num_rows:
        raise AssertionError("破損した新しい版が画像一覧に残りました。")
    (issue,) = cat.load_table(quarantine).scan().to_arrow().to_pylist()
    if (
        issue["sha256"] != bad_hash
        or issue["reason"] != "IMAGE_CORRUPT_OR_UNSUPPORTED"
        or image_raw(issue) != rejected
        or image_raw(first_record) != good_bytes
    ):
        raise AssertionError("画像の隔離理由または原本の版が一致しません。")

    image_fixture(path, good_bytes)
    if ingest(root, prefix)["imported"] != 1:
        raise AssertionError("画像の正常版を再受理できませんでした。")
    if cat.load_table(quarantine).scan().to_arrow().num_rows:
        raise AssertionError("修復した画像の隔離が解消されませんでした。")
    (restored,) = cat.load_table(bronze).scan().to_arrow().to_pylist()
    if restored != first_record:
        raise AssertionError("画像の正常版を再現できませんでした。")


def verify(*, transform, run_query):
    """専用namespaceで固定合成素材・画像・再実行の期待値を照合する。

    取込→変換→独立期待値→同一素材の再取込→再変換の順に検査する。
    結果は成功した検査名と取込件数。通常の業務表は変更対象にしない。
    後片付けも、この実行の表と原本だけに限定する。
    """
    # 実行ごとの隔離先を使い、通常業務表を検証・削除の対象にしない。
    prefix = "verify_" + secrets.token_hex(5) + "_"
    cat = catalog()
    result = {"checks": []}
    try:
        with tempfile.TemporaryDirectory(
            prefix="lakehouse-fixtures-"
        ) as directory:
            command = [
                sys.executable,
                str(ROOT / "scripts" / "generate-demo.py"),
                "--output",
                directory,
                "--seed",
                "42",
                "--orders",
                "20",
                "--days",
                "7",
                "--scenario",
                "edge",
            ]
            subprocess.run(command, check=True)
            # 業務行の期待値とは別に、画像原本のバイト列保存も検査する。
            image_path = Path(directory) / "images" / "inspection.png"
            image_path.parent.mkdir()
            Image.new("RGB", (4, 4), color="gray").save(image_path)
            image_bytes = image_path.read_bytes()
            image_hash = image_fixture(image_path, image_bytes)
            first = ingest(Path(directory), prefix)
            transform(prefix)
            image_table = cat.load_table(schema(prefix, "bronze") + ".images")
            image_record = image_table.scan().to_arrow().to_pylist()[0]
            if sha256(image_raw(image_record)).hexdigest() != image_hash:
                raise AssertionError("保存した画像と原本のSHA-256が違います。")
            verify_image_replacement(
                cat, prefix, image_path, image_bytes, image_record
            )
            result["checks"].append("画像の原本・破損版・修復版を照合")
            checks = json.loads(
                (ROOT / "medallion/expected-demo.json").read_text()
            )["checks"]
            for check in checks:
                # 版管理した検査SQLと独立期待値を使い、生成件数で代用しない。
                sql = (
                    check.get("sql")
                    or (ROOT / "medallion" / check["sql_file"]).read_text()
                )
                actual = run_query(sql, prefix)
                if actual != check["expected"]:
                    raise AssertionError(
                        "MES/ERP check failed: " + check["name"]
                    )
                result["checks"].append(check["name"])
            second = ingest(Path(directory), prefix)
            if second["imported"]:
                raise AssertionError("Unchanged inputs imported twice.")
            transform(prefix)
            for check in checks:
                # 再実行後にも同じ期待値を照合し、追記二重計上を検出する。
                sql = (
                    check.get("sql")
                    or (ROOT / "medallion" / check["sql_file"]).read_text()
                )
                if run_query(sql, prefix) != check["expected"]:
                    raise AssertionError("Medallion replay changed results.")
            result.update(
                first_import=first,
                replay_import=second,
                isolated_namespaces=True,
                ok=True,
            )
    finally:
        # 検査失敗でも自分の隔離先だけを掃除し、共有namespaceは消さない。
        cleanup_verification(cat, prefix)
    return result


def cleanup_verification(cat, prefix):
    """完了した1回の隔離検証に属する表・namespace・原本を削除する。

    専用prefixの形式を検査してからpurgeする破壊的な後片付けである。
    通常業務表や他の検証runを掃除する汎用処理として使わない。
    """
    if not re.fullmatch(r"verify_[0-9a-f]+_", prefix):
        raise ValueError("検証の一時名前空間以外は削除できません。")
    for layer in ["gold", "silver", "quarantine", "bronze"]:
        namespace = schema(prefix, layer)
        if cat.namespace_exists(namespace):
            for identifier in cat.list_tables(namespace):
                cat.purge_table(identifier)
            cat.drop_namespace(namespace)
    bucket = os.environ["S3_BUCKET_RAW"]
    for page in S3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="verify/" + prefix
    ):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            S3.delete_objects(Bucket=bucket, Delete={"Objects": objects})

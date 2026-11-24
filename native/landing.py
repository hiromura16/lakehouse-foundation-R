"""受け渡しファイルから原本とBronzeを保存する。

完了印を受け入れたファイルだけを処理する。原本、業務行、ファイル台帳の
順に確定し、台帳を再実行時の境界にする。
"""

import json
import os
import re
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from pathlib import Path

from PIL import Image
from pyiceberg.expressions import EqualTo

from native.storage import (
    CONTRACT,
    S3,
    STATE,
    catalog,
    ensure_tables,
    overwrite,
    schema,
)
from scripts.landing_contract import source_rows
from scripts.raw_file_input import ready_files

IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".webp"})
IMAGE_FORMATS = ("PNG", "JPEG", "WEBP")
IMAGE_PIXELS_LIMIT = 32_000_000


def resolve_source(relative):
    """相対pathから、固定6契約または正式なGUI増分名を照合する。

    一致した契約を返し、未知の名前はNoneを返す。未知ファイルは原本保存の
    対象にはなるが、任意の業務表として自動認識しない。
    """
    for source in CONTRACT:
        canonical = source["path"]
        if relative == canonical:
            return source
        stem, extension = canonical.rsplit(".", 1)
        pattern = (
            re.escape(stem)
            + r"__[a-f0-9]{12}_(?!0000)[0-9]{4}\."
            + re.escape(extension)
        )
        if re.fullmatch(pattern, relative):
            return source
    return None


def receipt_outcome(relative, data):
    """確定済み受領票を識別し、元の行を読まずに配送の状態を得る。"""
    if not relative.startswith("receipts/") or not relative.endswith(".json"):
        return None
    try:
        document = json.loads(data)
        sender = document["sender"]
        delivery = document["delivery"]
        outcome = document["outcome"]
        if (
            document["format_version"] == 1
            and relative == f"receipts/{sender}/{delivery}.json"
            and outcome in {"raw", "normalized", "rejected"}
        ):
            return outcome
    except (KeyError, TypeError, ValueError):
        pass
    return None


def image_details(data):
    """画像の寸法と型を検査し、失敗時は有限な理由コードを返す。"""
    try:
        with Image.open(BytesIO(data), formats=IMAGE_FORMATS) as decoded:
            width, height = decoded.size
            if width < 1 or height < 1 or width * height > IMAGE_PIXELS_LIMIT:
                return None, "IMAGE_DIMENSIONS_LIMIT"
            mime = Image.MIME[decoded.format]
            decoded.verify()
        # Pillowのverifyは形式によって画素を展開しない。
        # 先頭画像を展開できるかも確かめてから画像一覧に載せる。
        with Image.open(BytesIO(data), formats=IMAGE_FORMATS) as decoded:
            decoded.load()
    except Image.DecompressionBombError:
        return None, "IMAGE_DIMENSIONS_LIMIT"
    except (OSError, SyntaxError, ValueError):
        return None, "IMAGE_CORRUPT_OR_UNSUPPORTED"
    return {"width": str(width), "height": str(height), "mime": mime}, None


def discard_image_entry(table, relative):
    """正・隔離間を移るとき、そのpathの直前の値を退ける。"""
    filter_by_path = EqualTo("path", relative)
    if (
        table.current_snapshot() is not None
        and table.scan(
            row_filter=filter_by_path, limit=1, selected_fields=("path",)
        )
        .to_arrow()
        .num_rows
    ):
        table.delete(delete_filter=filter_by_path)


def record_image(cat, prefix, meta, detail, reason):
    """原本は共有したまま、画像として使える版と隔離理由を分ける。"""
    images = cat.load_table(f"{schema(prefix, 'bronze')}.images")
    issues = cat.load_table(f"{schema(prefix, 'quarantine')}.image_files")
    relative = meta["path"]
    if reason:
        overwrite(issues, [{**meta, "reason": reason}], "path", relative)
        discard_image_entry(images, relative)
    else:
        overwrite(images, [{**meta, **detail}], "path", relative)
        discard_image_entry(issues, relative)


def ingest(directory=Path("/landing"), prefix="", *, lane="all"):
    """ready付き原本を照合し、原本・業務文字列・完了台帳を保存する。

    directoryは読取元、prefixは通常または隔離検証のnamespaceを選ぶ。
    同じ相対pathと内容hashが完了済みならskipし、件数を辞書で返す。
    更新素材はそのpath由来のBronze行だけを置換する。原本は内容hash別に
    保存し、landingの削除を分析表のDELETEとしては扱わない。
    途中失敗では一部の原本・表が残るため、完了台帳を最後に確定する。
    """
    ensure_tables(prefix)
    cat = catalog()
    files = cat.load_table(schema(prefix, "bronze") + ".files")
    imported = skipped = 0
    receipt_counts = {"raw": 0, "normalized": 0, "rejected": 0}
    # 工程1: 完了印と原本を照合した RAW 入力だけを永続化へ渡す。
    for raw in ready_files(directory, lane=lane):
        path, relative, data, digest = (
            raw.path,
            raw.relative,
            raw.data,
            raw.digest,
        )
        previous = (
            files.scan(row_filter=EqualTo("path", relative))
            .to_arrow()
            .to_pylist()
        )
        # 工程2: 完了台帳のpath/hashが一致する時だけskipする。
        # 台帳未完了なら後段を再実行できるよう、台帳は最後に更新する。
        if previous and previous[0]["sha256"] == digest:
            skipped += 1
            continue
        bucket = os.environ["S3_BUCKET_RAW"]
        key = (
            ("verify/" + prefix if prefix else "raw/")
            + digest
            + "/"
            + sha256(relative.encode()).hexdigest()
        )
        uri = "s3://" + bucket + "/" + key
        is_image = path.suffix.lower() in IMAGE_SUFFIXES
        details, image_reason = (
            image_details(data) if is_image else (None, None)
        )
        mime = details["mime"] if details else "application/octet-stream"
        # 工程3: 内容とpathのhashで原本を識別し、同名fileの旧版もS3へ残す。
        # 業務ファイルは列検査が失敗した場合も、照合済みの原本を保つ。
        S3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=mime)
        source = resolve_source(relative)
        if source:
            # 列の妥当性はここで、型・業務ルールの妥当性はSilverで確かめる。
            rows = source_rows(path.suffix, data, source["columns"])
            stamp = datetime.now(timezone.utc).isoformat()
            rows = [
                {
                    **{
                        key: None if value is None else str(value)
                        for key, value in row.items()
                    },
                    "_source_file": relative,
                    "_source_hash": digest,
                    "_ingested_at": stamp,
                }
                for row in rows
            ]
            table = cat.load_table(
                schema(prefix, "bronze") + "." + source["table"]
            )
            overwrite(table, rows, "_source_file", relative)
        meta = {
            "path": relative,
            "sha256": digest,
            "s3_uri": uri,
            "mime": mime,
            "bytes": str(len(data)),
        }
        if is_image:
            record_image(cat, prefix, meta, details, image_reason)
        # 工程4: 業務行/画像metadataのcommit後、最後にfile完了を確定する。
        # 1表ずつはatomicでも、S3と複数表をまとめたtransactionではない。
        overwrite(files, [meta], "path", relative)
        imported += 1
        STATE["imported_files"] += 1
        outcome = receipt_outcome(relative, data)
        if outcome:
            receipt_counts[outcome] += 1
    return {
        "imported": imported,
        "skipped": skipped,
        "received_deliveries": receipt_counts,
    }

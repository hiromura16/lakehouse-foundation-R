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
        source = resolve_source(relative)
        rows = []
        if source:
            # 業務契約へ一致する素材だけを解釈し、型の妥当性はSilverへ委ねる。
            # 未知のCSV等は任意表へせず、下段の原本保存と台帳登録だけを行う。
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
        bucket = os.environ["S3_BUCKET_RAW"]
        key = (
            ("verify/" + prefix if prefix else "raw/")
            + digest
            + "/"
            + sha256(relative.encode()).hexdigest()
        )
        uri = "s3://" + bucket + "/" + key
        mime = "application/octet-stream"
        image = None
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
            # verify後のdecoderは再利用せず、寸法取得は別に開き直す。
            with Image.open(BytesIO(data)) as decoded:
                decoded.verify()
            with Image.open(BytesIO(data)) as decoded:
                image = (decoded.width, decoded.height)
                mime = Image.MIME.get(decoded.format, mime)
        # 工程3: 内容とpathのhashで原本を識別し、同名fileの旧版もS3へ残す。
        # 以降で失敗しても原本は残り得るが、完了台帳を先に進めない。
        S3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=mime)
        if source:
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
        if image:
            table = cat.load_table(schema(prefix, "bronze") + ".images")
            overwrite(
                table,
                [{**meta, "width": str(image[0]), "height": str(image[1])}],
                "path",
                relative,
            )
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

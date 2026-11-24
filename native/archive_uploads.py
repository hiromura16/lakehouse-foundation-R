"""原本の予約prefixにあるマルチパートの一覧を、識別値を表示せず確認する。"""

import argparse
import json
import os
import re
import sys
from datetime import UTC, date, datetime, time
from hashlib import sha256
from urllib.parse import unquote

PREFIX = "archive/"
PAGE_SIZE = 250
DEFAULT_PAGES = 4
MAX_PAGES = 20
MAX_KEY_BYTES = 1024
MAX_ID_LENGTH = 4096


class InventoryError(ValueError):
    """S3のキーやupload IDを含めない、固定の診断コード。"""


def parse_date(value):
    """切断点は指定された日付のUTC午前0時に限定する。"""
    try:
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            raise ValueError
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "日付を YYYY-MM-DD で指定してください。"
        ) from None


def parse_pages(value):
    """誤って大量の一覧要求を発行しない範囲に限定する。"""
    if not re.fullmatch(r"[1-9][0-9]?", value) or int(value) > MAX_PAGES:
        raise argparse.ArgumentTypeError(
            f"ページ上限は1〜{MAX_PAGES}を指定してください。"
        )
    return int(value)


def decode_identity(key, upload_id):
    """URL符号化されたキーだけを一度復号し、upload IDはそのまま扱う。"""
    if (
        not isinstance(key, str)
        or len(key) > 3 * MAX_KEY_BYTES
        or re.search(r"%(?![0-9a-fA-F]{2})", key)
        or not isinstance(upload_id, str)
        or not 1 <= len(upload_id) <= MAX_ID_LENGTH
    ):
        raise InventoryError("ARCHIVE_UPLOADS_RESPONSE_INVALID")
    try:
        decoded = unquote(key, encoding="utf-8", errors="strict")
        if not decoded.startswith(PREFIX) or (
            len(decoded.encode("utf-8")) > MAX_KEY_BYTES
        ):
            raise ValueError
    except ValueError:
        raise InventoryError("ARCHIVE_UPLOADS_RESPONSE_INVALID") from None
    return decoded, upload_id


def collect(s3, bucket, before, *, max_pages=DEFAULT_PAGES):
    """対象の一覧だけを読み、走査したページの要約と切れ目を返す。"""
    if type(max_pages) is not int or not 1 <= max_pages <= MAX_PAGES:
        raise InventoryError("ARCHIVE_UPLOADS_PAGE_LIMIT")
    cutoff = datetime.combine(before, time.min, tzinfo=UTC)
    params = {
        "Bucket": bucket,
        "Prefix": PREFIX,
        "EncodingType": "url",
        "MaxUploads": PAGE_SIZE,
    }
    entries = {}
    visited = set()
    older = 0
    for page_number in range(1, max_pages + 1):
        page = s3.list_multipart_uploads(**params)
        if not isinstance(page, dict):
            raise InventoryError("ARCHIVE_UPLOADS_RESPONSE_INVALID")
        rows = page.get("Uploads", [])
        truncated = page.get("IsTruncated")
        if (
            page.get("Bucket", bucket) != bucket
            or page.get("EncodingType") != "url"
            or type(truncated) is not bool
            or not isinstance(rows, list)
            or len(rows) > PAGE_SIZE
        ):
            raise InventoryError("ARCHIVE_UPLOADS_RESPONSE_INVALID")
        for row in rows:
            if not isinstance(row, dict):
                raise InventoryError("ARCHIVE_UPLOADS_RESPONSE_INVALID")
            identity = decode_identity(row.get("Key"), row.get("UploadId"))
            key_hash = sha256(
                json.dumps(identity, separators=(",", ":")).encode("ascii")
            ).digest()
            started = row.get("Initiated")
            if (
                not isinstance(started, datetime)
                or started.utcoffset() is None
            ):
                raise InventoryError("ARCHIVE_UPLOADS_RESPONSE_INVALID")
            if key_hash in entries:
                raise InventoryError("ARCHIVE_UPLOADS_LIST_CHANGED")
            utc_started = started.astimezone(UTC)
            entries[key_hash] = utc_started.isoformat()
            older += utc_started < cutoff
        if not truncated:
            break
        # 同じキーの別uploadを飛ばさないよう、2種類のマーカーを渡す。
        marker = decode_identity(
            page.get("NextKeyMarker"), page.get("NextUploadIdMarker")
        )
        if marker in visited:
            raise InventoryError("ARCHIVE_UPLOADS_PAGINATION_STALLED")
        visited.add(marker)
        params["KeyMarker"], params["UploadIdMarker"] = marker

    fingerprint = sha256()
    for key_hash, started in sorted(entries.items()):
        fingerprint.update(key_hash)
        fingerprint.update(started.encode("ascii") + b"\n")
    return {
        "prefix": PREFIX,
        "before_utc": before.isoformat(),
        "pages_read": page_number,
        "max_pages": max_pages,
        "page_size": PAGE_SIZE,
        "has_next_page": truncated,
        "uploads_seen": len(entries),
        "initiated_before_cutoff": older,
        "inventory_sha256": fingerprint.hexdigest(),
    }


def main(argv=None):
    """lakehouseの既存接続を使い、一覧APIの機微なエラーは表示しない。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, type=parse_date)
    parser.add_argument("--max-pages", default=DEFAULT_PAGES, type=parse_pages)
    args = parser.parse_args(argv)
    try:
        from native import storage

        result = collect(
            storage.S3,
            os.environ["S3_BUCKET_RAW"],
            args.before,
            max_pages=args.max_pages,
        )
    except InventoryError as error:
        print(str(error), file=sys.stderr)
        return 1
    except Exception:
        print("ARCHIVE_UPLOADS_LIST_FAILED", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

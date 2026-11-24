"""未完了一覧の境界、ページ継続、出力の非公開条件を検査する。"""

import argparse
import json
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, date, datetime
from hashlib import sha256
from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from native import archive_uploads as inventory

BEFORE = date(2026, 11, 20)
BUCKET = "synthetic-raw"
REQUEST = {
    "Bucket": BUCKET,
    "Prefix": "archive/",
    "EncodingType": "url",
    "MaxUploads": 250,
}


def upload(key="archive%2Fa%20%25", upload_id="fixture+id/1", started=None):
    """UTF-8のキーと不透明なupload IDを持つSDKレスポンスを表す。"""
    return {
        "Key": key,
        "UploadId": upload_id,
        "Initiated": started or datetime(2026, 11, 19, tzinfo=UTC),
    }


def page(*rows, marker=None):
    """ListMultipartUploadsの固定レスポンスを作る。"""
    response = {
        "Bucket": BUCKET,
        "EncodingType": "url",
        "IsTruncated": marker is not None,
    }
    if rows:
        response["Uploads"] = list(rows)
    if marker is not None:
        response["NextKeyMarker"], response["NextUploadIdMarker"] = marker
    return response


class ArchiveUploadInventoryTests(unittest.TestCase):
    """一覧を読むSDKだけを用い、書込みAPIが不要なことを検査する。"""

    def test_empty_inventory(self):
        client = Mock(spec=["list_multipart_uploads"])
        client.list_multipart_uploads.return_value = page()
        result = inventory.collect(client, BUCKET, BEFORE)
        client.list_multipart_uploads.assert_called_once_with(**REQUEST)
        self.assertEqual(result["pages_read"], 1)
        self.assertEqual(result["uploads_seen"], 0)
        self.assertFalse(result["has_next_page"])
        self.assertEqual(result["inventory_sha256"], sha256(b"").hexdigest())

    def test_both_markers_same_key_and_strict_utc_boundary(self):
        first = upload(
            started=datetime.fromisoformat("2026-11-20T08:59:59.999999+09:00")
        )
        boundary = upload(
            upload_id="fixture+id/2",
            started=datetime.fromisoformat("2026-11-20T09:00:00+09:00"),
        )
        marker = (first["Key"], first["UploadId"])
        client = Mock(spec=["list_multipart_uploads"])
        client.list_multipart_uploads.side_effect = [
            page(first, marker=marker),
            page(boundary),
        ]
        result = inventory.collect(client, BUCKET, BEFORE)
        self.assertEqual(result["uploads_seen"], 2)
        self.assertEqual(result["initiated_before_cutoff"], 1)
        self.assertEqual(result["before_utc"], "2026-11-20")
        self.assertFalse(result["has_next_page"])
        self.assertEqual(
            client.list_multipart_uploads.call_args_list,
            [
                call(**REQUEST),
                call(
                    **REQUEST,
                    KeyMarker="archive/a %",
                    UploadIdMarker="fixture+id/1",
                ),
            ],
        )
        serial = json.dumps(result)
        for value in ("archive/a", "archive%2F", "fixture+id"):
            self.assertNotIn(value, serial)

        # ページ分割や到着順が異なっても、同じ集合は同じhashになる。
        client.list_multipart_uploads.side_effect = None
        client.list_multipart_uploads.return_value = page(boundary, first)
        single = inventory.collect(client, BUCKET, BEFORE)
        self.assertEqual(
            single["inventory_sha256"], result["inventory_sha256"]
        )

    def test_page_limit_reports_next_without_requesting_it(self):
        first = upload()
        marker = (first["Key"], first["UploadId"])
        client = Mock(spec=["list_multipart_uploads"])
        client.list_multipart_uploads.return_value = page(first, marker=marker)
        result = inventory.collect(client, BUCKET, BEFORE, max_pages=1)
        client.list_multipart_uploads.assert_called_once_with(**REQUEST)
        self.assertEqual(result["pages_read"], 1)
        self.assertEqual(result["max_pages"], 1)
        self.assertTrue(result["has_next_page"])

    def test_invalid_keys_pages_and_timestamps_are_rejected(self):
        response_cases = (
            page(upload(key="out-of-scope%2Fa")),
            page(upload(key="archive%2F%FF")),
            page(upload(key="archive%2Fa%")),
            page(upload(upload_id="")),
            page(upload(started=datetime(2026, 11, 19))),
            page(marker=("archive%2Fa", "")),
            {**page(), "EncodingType": "bogus"},
            {**page(), "Bucket": "another-bucket"},
            {**page(), "IsTruncated": "yes"},
            page(*[upload()] * (inventory.PAGE_SIZE + 1)),
        )
        client = Mock(spec=["list_multipart_uploads"])
        for response in response_cases:
            with (
                self.subTest(case=response.keys()),
                self.assertRaisesRegex(
                    inventory.InventoryError,
                    "ARCHIVE_UPLOADS_RESPONSE_INVALID",
                ),
            ):
                client.list_multipart_uploads.return_value = response
                inventory.collect(client, BUCKET, BEFORE)

    def test_repeated_marker_and_duplicate_entries_are_rejected(self):
        first = upload()
        marker = (first["Key"], first["UploadId"])
        client = Mock(spec=["list_multipart_uploads"])
        client.list_multipart_uploads.side_effect = [
            page(first, marker=marker),
            page(marker=marker),
        ]
        with self.assertRaisesRegex(
            inventory.InventoryError, "ARCHIVE_UPLOADS_PAGINATION_STALLED"
        ):
            inventory.collect(client, BUCKET, BEFORE)
        client.list_multipart_uploads.side_effect = [
            page(first, marker=marker),
            page(first),
        ]
        with self.assertRaisesRegex(
            inventory.InventoryError, "ARCHIVE_UPLOADS_LIST_CHANGED"
        ):
            inventory.collect(client, BUCKET, BEFORE)

    def test_invalid_date_and_limits_fail_without_sdk_calls(self):
        for value in ("20261120", "2026-02-30", "2026-11-20Z", "0000-01-01"):
            with (
                self.subTest(value=value),
                self.assertRaises(argparse.ArgumentTypeError),
            ):
                inventory.parse_date(value)
        for value in ("0", "21", "01", "100", "a"):
            with (
                self.subTest(value=value),
                self.assertRaises(argparse.ArgumentTypeError),
            ):
                inventory.parse_pages(value)
        client = Mock(spec=["list_multipart_uploads"])
        for limit in (0, 21, True):
            with (
                self.subTest(limit=limit),
                self.assertRaises(inventory.InventoryError),
            ):
                inventory.collect(client, BUCKET, BEFORE, max_pages=limit)
        client.list_multipart_uploads.assert_not_called()

    def test_cli_redacts_sdk_exception_and_output_identifiers(self):
        client = Mock(spec=["list_multipart_uploads"])
        client.list_multipart_uploads.side_effect = RuntimeError(
            "archive/private-dir PRIVATE-UPLOAD PRIVATE-ENDPOINT"
        )
        storage = SimpleNamespace(S3=client)
        output, errors = StringIO(), StringIO()
        with (
            patch.dict(sys.modules, {"native.storage": storage}),
            patch.dict(os.environ, {"S3_BUCKET_RAW": BUCKET}),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            result = inventory.main(["--before", "2026-11-20"])
        self.assertEqual(result, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(errors.getvalue(), "ARCHIVE_UPLOADS_LIST_FAILED\n")


if __name__ == "__main__":
    unittest.main()

"""大容量原本の通信契約と、失敗時に台帳を確定しない条件を検査する。"""

import unittest
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

import boto3
from botocore.exceptions import ClientError, ReadTimeoutError
from botocore.stub import Stubber

from native import archive

ORIGINAL = b"abcdEFGH!"
PARTS = (b"abcd", b"EFGH", b"!")
UPLOAD = {"Bucket": "synthetic-archive", "Key": "fixture", "UploadId": "test"}
REQUEST = {"Bucket": UPLOAD["Bucket"], "Key": UPLOAD["Key"]}
HEAD = {**REQUEST, "ChecksumMode": "ENABLED"}
PART_DIGESTS = tuple(sha256(body).digest() for body in PARTS)
CHECKSUM = archive.as_base64(sha256(b"".join(PART_DIGESTS)).digest()) + "-3"
STORED = {
    "ChecksumType": "COMPOSITE",
    "ChecksumSHA256": CHECKSUM,
    "ContentLength": len(ORIGINAL),
}


def client_error(status, code):
    """合成エラーには実S3のキー・接続先を含めない。"""
    return ClientError(
        {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "fixture",
    )


class MultipartTests(unittest.TestCase):
    """SDKの固定通信と、復元先台帳の確定条件を分けて検査する。"""

    def setUp(self):
        self.enterContext(patch.object(archive, "PART_BYTES", 4))
        self.enterContext(patch.object(archive, "MAX_BYTES", 8))
        self.s3 = boto3.client(
            "s3",
            endpoint_url="http://127.0.0.1:1",
            region_name="us-east-1",
            aws_access_key_id="fixture",
            aws_secret_access_key="fixture",
        )
        self.stub = self.enterContext(Stubber(self.s3))

    def missing(self):
        """対象オブジェクトの不在を確認するHEADを準備する。"""
        self.stub.add_client_error(
            "head_object", "404", http_status_code=404, expected_params=HEAD
        )

    def create(self):
        """SHA256 COMPOSITE方式の初回要求を照合する。"""
        self.stub.add_response(
            "create_multipart_upload",
            {"UploadId": UPLOAD["UploadId"]},
            {
                **REQUEST,
                "ContentType": "application/octet-stream",
                "ChecksumAlgorithm": "SHA256",
                "ChecksumType": "COMPOSITE",
            },
        )

    def sent_parts(self):
        """順番、パートサイズ、個別のSHA、Complete用ETagを照合する。"""
        result = []
        for number, (body, digest) in enumerate(
            zip(PARTS, PART_DIGESTS, strict=True), 1
        ):
            checksum = archive.as_base64(digest)
            etag = f'"synthetic-{number}"'
            self.stub.add_response(
                "upload_part",
                {"ETag": etag, "ChecksumSHA256": checksum},
                {
                    **UPLOAD,
                    "Body": body,
                    "ContentLength": len(body),
                    "PartNumber": number,
                    "ChecksumSHA256": checksum,
                },
            )
            result.append(
                {
                    "PartNumber": number,
                    "ETag": etag,
                    "ChecksumSHA256": checksum,
                }
            )
        return {
            **UPLOAD,
            "MultipartUpload": {"Parts": result},
            "IfNoneMatch": "*",
            "ChecksumType": "COMPOSITE",
        }

    def multipart(self, body=ORIGINAL):
        """同じパート構成で中断・完了・再送を実行する。"""
        return archive.put_multipart_verified(
            self.s3,
            UPLOAD["Bucket"],
            UPLOAD["Key"],
            BytesIO(body),
            len(body),
            PART_DIGESTS,
        )

    def test_configuration_and_digest_boundaries(self):
        self.assertEqual(archive.configured_limit({}), 256 * 1024**2)
        self.assertEqual(
            archive.configured_limit({archive.LIMIT_ENV: "2048"}), 2 * 1024**3
        )
        for invalid in ("", "0", "0256", "2049", "1.5", "-1", "257\n"):
            with (
                self.subTest(invalid=invalid),
                self.assertRaisesRegex(ValueError, "ARCHIVE_LIMIT_INVALID"),
            ):
                archive.configured_limit({archive.LIMIT_ENV: invalid})
        self.assertEqual(
            archive.original_checksums(BytesIO(ORIGINAL), len(ORIGINAL)),
            (sha256(ORIGINAL).digest(), PART_DIGESTS),
        )
        self.assertEqual(
            archive.original_checksums(BytesIO(ORIGINAL[:8]), 8),
            (sha256(ORIGINAL[:8]).digest(), ()),
        )

    def test_complete_and_replay_by_head(self):
        self.missing()
        self.create()
        self.stub.add_response(
            "complete_multipart_upload", {}, self.sent_parts()
        )
        self.stub.add_response("head_object", STORED, HEAD)
        self.assertTrue(self.multipart())
        self.stub.add_response("head_object", STORED, HEAD)
        self.assertFalse(self.multipart())
        self.stub.assert_no_pending_responses()

    def test_conflict_aborts_only_own_id_and_checks_winner(self):
        self.missing()
        self.create()
        complete = self.sent_parts()
        self.stub.add_client_error(
            "complete_multipart_upload",
            "PreconditionFailed",
            http_status_code=412,
            expected_params=complete,
        )
        self.stub.add_response("abort_multipart_upload", {}, UPLOAD)
        self.stub.add_response("head_object", STORED, HEAD)
        self.assertFalse(self.multipart())
        self.stub.assert_no_pending_responses()

    def test_conflict_with_wrong_head_does_not_succeed(self):
        self.missing()
        self.create()
        complete = self.sent_parts()
        self.stub.add_client_error(
            "complete_multipart_upload",
            "PreconditionFailed",
            http_status_code=412,
            expected_params=complete,
        )
        self.stub.add_response("abort_multipart_upload", {}, UPLOAD)
        self.stub.add_response(
            "head_object", {**STORED, "ChecksumType": "FULL_OBJECT"}, HEAD
        )
        with self.assertRaisesRegex(
            RuntimeError, "ARCHIVE_STORAGE_CHECKSUM_MISMATCH"
        ):
            self.multipart()
        self.stub.assert_no_pending_responses()

    def test_mutation_is_rejected_and_abort_is_scoped(self):
        self.missing()
        self.create()
        self.stub.add_response("abort_multipart_upload", {}, UPLOAD)
        with self.assertRaisesRegex(ValueError, "ARCHIVE_CONTENT_CHANGED"):
            self.multipart(b"X" + ORIGINAL[1:])
        self.stub.assert_no_pending_responses()

    def test_wrong_part_echo_aborts_before_completion(self):
        self.missing()
        self.create()
        checksum = archive.as_base64(PART_DIGESTS[0])
        self.stub.add_response(
            "upload_part",
            {"ETag": '"synthetic"', "ChecksumSHA256": "wrong"},
            {
                **UPLOAD,
                "Body": PARTS[0],
                "PartNumber": 1,
                "ContentLength": len(PARTS[0]),
                "ChecksumSHA256": checksum,
            },
        )
        self.stub.add_response("abort_multipart_upload", {}, UPLOAD)
        with self.assertRaisesRegex(
            RuntimeError, "ARCHIVE_MULTIPART_PART_NOT_VERIFIED"
        ):
            self.multipart()
        self.stub.assert_no_pending_responses()

    def test_conditional_409_aborts_without_confirming(self):
        self.missing()
        self.create()
        complete = self.sent_parts()
        self.stub.add_client_error(
            "complete_multipart_upload",
            "ConditionalRequestConflict",
            http_status_code=409,
            expected_params=complete,
        )
        self.stub.add_response("abort_multipart_upload", {}, UPLOAD)
        with self.assertRaisesRegex(
            RuntimeError, "ARCHIVE_MULTIPART_UPLOAD_FAILED"
        ):
            self.multipart()
        self.stub.assert_no_pending_responses()

    def test_abort_failure_does_not_confirm_competing_upload(self):
        self.missing()
        self.create()
        complete = self.sent_parts()
        self.stub.add_client_error(
            "complete_multipart_upload",
            "PreconditionFailed",
            http_status_code=412,
            expected_params=complete,
        )
        self.stub.add_client_error(
            "abort_multipart_upload",
            "AccessDenied",
            http_status_code=403,
            expected_params=UPLOAD,
        )
        with self.assertRaisesRegex(
            RuntimeError, "^ARCHIVE_MULTIPART_ABORT_FAILED$"
        ):
            self.multipart()
        self.stub.assert_no_pending_responses()

    def test_lost_completion_response_leaves_ledger_for_retry(self):
        s3 = Mock()
        s3.create_multipart_upload.return_value = {
            "UploadId": UPLOAD["UploadId"]
        }
        s3.upload_part.side_effect = lambda **params: {
            "ETag": '"synthetic"',
            "ChecksumSHA256": params["ChecksumSHA256"],
        }
        s3.complete_multipart_upload.side_effect = ReadTimeoutError(
            endpoint_url="https://synthetic.invalid"
        )
        s3.head_object.side_effect = [client_error(404, "404"), STORED]
        s3.abort_multipart_upload.side_effect = client_error(
            404, "NoSuchUpload"
        )
        core = Mock()
        core.S3 = s3
        core.os = SimpleNamespace(environ={"S3_BUCKET_RAW": UPLOAD["Bucket"]})
        core.STATE = {"imported_files": 0}
        ledger = core.catalog.return_value.load_table.return_value
        scan = ledger.scan.return_value.to_arrow.return_value
        scan.to_pylist.return_value = []
        with TemporaryDirectory() as tmp:
            landing = Path(tmp)
            folder = landing / "large-raw"
            folder.mkdir()
            name = "fixture.bin"
            (folder / name).write_bytes(ORIGINAL)
            (folder / (name + ".sealed")).write_text(
                sha256(ORIGINAL).hexdigest() + "\n", encoding="ascii"
            )
            with self.assertRaisesRegex(
                RuntimeError, "ARCHIVE_MULTIPART_UPLOAD_FAILED"
            ):
                archive.receive(core, name, landing=landing)
            core.catalog.assert_not_called()
            core.overwrite.assert_not_called()
            self.assertEqual(core.STATE["imported_files"], 0)
            resumed = archive.receive(core, name, landing=landing)
            self.assertFalse(resumed["object_created"])
            self.assertTrue(resumed["registered"])
            self.assertEqual(core.STATE["imported_files"], 1)
            core.overwrite.assert_called_once()
            s3.create_multipart_upload.assert_called_once()
            s3.abort_multipart_upload.assert_called_once()


if __name__ == "__main__":
    unittest.main()

"""R2成果物の事前検証と配信順序を、通信なしで照合する。"""

import json
import tempfile
import unittest
from hashlib import sha256
from io import BytesIO
from pathlib import Path

from botocore.exceptions import ClientError

from native.edge import r2


class ConditionalStore:
    """対象バケットの作成専用書込と内容読取を最小限再現する。"""

    def __init__(self):
        self.objects = {}
        self.created = []

    def put_object(
        self, *, Bucket, Key, Body, ContentType, IfNoneMatch, Metadata
    ):
        """条件付き作成だけを受け付け、競合時に412を返す。"""
        if (
            IfNoneMatch != "*"
            or Metadata["sha256"] != sha256(Body).hexdigest()
        ):
            raise ValueError("作成専用またはハッシュが欠けています。")
        address = (Bucket, Key)
        if address in self.objects:
            raise ClientError(
                {"ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject"
            )
        self.objects[address] = Body
        self.created.append(address)

    def get_object(self, *, Bucket, Key):
        """再送のハッシュ比較用に、既に確定したバイト列を返す。"""
        return {"Body": BytesIO(self.objects[Bucket, Key])}


class R2Acceptance(unittest.TestCase):
    """承認されていない内容を送らず、再送で重複を増やさない。"""

    def setUp(self):
        """分類済みの小さいファイルと受入用manifestを準備する。"""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        self.path = root / "production.csv"
        self.payload = b"factory_id,good_qty\nF01,42\n"
        self.path.write_bytes(self.payload)
        self.manifest = root / "approved.json"
        self.manifest.write_text(
            json.dumps(
                {
                    "format_version": 1,
                    "classification": "approved_external",
                    "dataset": "factory_daily_production",
                    "release": "123",
                    "files": [
                        {
                            "name": self.path.name,
                            "size_bytes": len(self.payload),
                            "sha256": sha256(self.payload).hexdigest(),
                        }
                    ],
                }
            )
        )

    def test_manifest_last_and_retry(self):
        """全成果物の確定後にmanifestを置き、同じ送信の再試行を許す。"""
        prefix, files, manifest = r2.load_bundle(self.manifest)
        client = ConditionalStore()
        for _ in range(2):
            r2.upload(client, "private-exports", prefix, files, manifest)
        self.assertEqual(
            client.created,
            [
                ("private-exports", f"{prefix}/production.csv"),
                ("private-exports", f"{prefix}/manifest.json"),
            ],
        )

    def test_conflict_and_tampering(self):
        """異内容の既存物、ローカルの書換えでmanifestを確定させない。"""
        prefix, files, manifest = r2.load_bundle(self.manifest)
        client = ConditionalStore()
        client.objects["private-exports", f"{prefix}/production.csv"] = (
            b"wrong"
        )
        with self.assertRaisesRegex(ValueError, "R2_EXISTING_OBJECT_MISMATCH"):
            r2.upload(client, "private-exports", prefix, files, manifest)
        self.assertEqual(client.created, [])
        self.path.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "R2_ARTIFACT_HASH_MISMATCH"):
            r2.load_bundle(self.manifest)

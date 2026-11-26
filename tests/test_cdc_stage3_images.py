"""オフラインの合成OCIで実行せずに整合性エラーを再現する。"""

import gzip
import json
import sys
import tarfile
import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cdc_stage3_images as images  # noqa: E402


def fixture(arch="amd64", *, compressed=False, bad_diff=False):
    """本物のOCIと同じdescriptor関係を持つ最小の合成アーカイブ。"""
    layer_type = "application/vnd.oci.image.layer.v1.tar"
    config_type = "application/vnd.oci.image.config.v1+json"
    payload = b"synthetic layer bytes"
    layer = gzip.compress(payload, mtime=0) if compressed else payload
    if compressed:
        layer_type += "+gzip"
    diff = images.digest(b"wrong" if bad_diff else payload)

    def blob(data, mime):
        return {
            "digest": images.digest(data),
            "size": len(data),
            "mediaType": mime,
        }

    def encode(data):
        return json.dumps(data).encode()

    config = encode(
        {
            "os": "linux",
            "architecture": arch,
            "rootfs": {"type": "layers", "diff_ids": [diff]},
        }
    )
    config_desc = blob(config, config_type)
    layer_desc = blob(layer, layer_type)
    manifest = encode(
        {
            "schemaVersion": 2,
            "mediaType": images.OCI_MANIFEST,
            "config": config_desc,
            "layers": [layer_desc],
        }
    )
    image = blob(manifest, images.OCI_MANIFEST)
    image["platform"] = {"os": "linux", "architecture": "amd64"}
    index = encode(
        {
            "schemaVersion": 2,
            "mediaType": images.OCI_INDEX,
            "manifests": [image],
        }
    )
    blobs = {
        ("blobs/sha256/" + images.digest(data).split(":")[1]): data
        for data in (config, layer, manifest)
    }
    lock = {
        "index": images.digest(index),
        "manifest": image["digest"],
        "config": config_desc["digest"],
    }
    return (
        lock,
        {
            "index.json": index,
            "oci-layout": encode({"imageLayoutVersion": "1.0.0"}),
            **blobs,
        },
        manifest,
        config,
    )


def pack(path, files, additional=()):
    """安全に読み取るべき最小のtarを検証用一時領域だけへ作成する。"""
    with tarfile.open(path, "w") as archive:
        for name, data in list(files.items()) + list(additional):
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, BytesIO(data))
    return images.digest(path.read_bytes()).split(":")[1]


class RegistryTest(unittest.TestCase):
    """タグの観測と固定コンテンツの確認を分けて要求する。"""

    def test_expected_sources_and_dockerfiles(self):
        """同一タグでも可変参照だけのDockerfileを認めない。"""
        self.assertEqual(images.source_lock().keys(), images.SOURCES.keys())

    def test_matches_all_bytes_and_platform(self):
        """タグ、固定index、子manifest、configをそれぞれ照会する。"""
        lock, files, manifest, config = fixture()
        transport = Mock(
            side_effect=[
                files["index.json"],
                files["index.json"],
                manifest,
                config,
            ]
        )
        images.verify_source("connect", lock, transport)
        self.assertEqual(transport.call_count, 4)

    def test_drift_does_not_follow_new_index(self):
        """同じタグが変われば以降の取得や暗黙の更新をしない。"""
        lock, _, _, _ = fixture()
        transport = Mock(return_value=b'{"new":"index"}')
        with self.assertRaisesRegex(images.ImageError, "UPSTREAM_CHANGED"):
            images.verify_source("connect", lock, transport)
        self.assertEqual(transport.call_count, 1)

    def test_external_descriptor_rejected(self):
        """manifestの外部URLを信頼しない。"""
        with self.assertRaisesRegex(images.ImageError, "EXTERNAL_DESCRIPTOR"):
            images.descriptor(
                {"digest": images.digest(b"x"), "size": 1, "urls": []}
            )


class ArchiveTest(unittest.TestCase):
    """コンテナをロードせず全OCIバイトと検証境界を確認する。"""

    def test_archive_identity_and_blob_tree(self):
        """archiveとmanifestとconfigのハッシュは異なる実体。"""
        lock, files, _, _ = fixture()
        with TemporaryDirectory() as work:
            path = Path(work) / "image.tar"
            actual = pack(path, files)
            self.assertEqual(
                images.verify_archive(path, actual, lock["manifest"]),
                ("sha256:" + actual, lock["manifest"], lock["config"]),
            )
            with self.assertRaisesRegex(images.ImageError, "ARCHIVE_SHA256"):
                images.verify_archive(path, "0" * 64, lock["manifest"])
            with self.assertRaisesRegex(images.ImageError, "ARCHIVE_MANIFEST"):
                images.verify_archive(path, actual, lock["config"])
            with self.assertRaisesRegex(images.ImageError, "ARCHIVE_CONFIG"):
                images.verify_archive(
                    path, actual, lock["manifest"], "sha256:" + "0" * 64
                )

    def test_rejects_unsafe_or_incomplete_oci(self):
        """archive SHAを置き換えてもOCI構造破損を見逃さない。"""
        lock, files, _, _ = fixture()
        layer = next(
            key for key, value in files.items() if b"synthetic" in value
        )
        for content, additional, error in (
            (
                {k: v for k, v in files.items() if k != layer},
                (),
                "MISSING_ARCHIVE",
            ),
            ({**files, layer: b"changed"}, (), "BLOB_DIGEST"),
            (files, (("index.json", b"{}"),), "ARCHIVE_MEMBER"),
            (files, (("../path", b"x"),), "INVALID_ARCHIVE_PATH"),
        ):
            with self.subTest(error=error), TemporaryDirectory() as work:
                path = Path(work) / "image.tar"
                actual = pack(path, content, additional)
                with self.assertRaisesRegex(images.ImageError, error):
                    images.verify_archive(path, actual, lock["manifest"])

    def test_gzip_layer_matches_uncompressed_config_identity(self):
        """圧縮レイヤのdigestが一致しても展開時識別子を確認する。"""
        for bad_diff in (False, True):
            lock, files, _, _ = fixture(compressed=True, bad_diff=bad_diff)
            with self.subTest(bad_diff=bad_diff), TemporaryDirectory() as work:
                path = Path(work) / "image.tar"
                actual = pack(path, files)
                if bad_diff:
                    with self.assertRaisesRegex(
                        images.ImageError, "LAYER_DIFF"
                    ):
                        images.verify_archive(path, actual, lock["manifest"])
                else:
                    images.verify_archive(path, actual, lock["manifest"])

    def test_config_must_actually_be_amd64(self):
        """索引だけamd64と主張してもarm64 configは拒否する。"""
        lock, files, _, _ = fixture(arch="arm64")
        with TemporaryDirectory() as work:
            path = Path(work) / "image.tar"
            actual = pack(path, files)
            with self.assertRaisesRegex(images.ImageError, "PLATFORM"):
                images.verify_archive(path, actual, lock["manifest"])


if __name__ == "__main__":
    unittest.main()

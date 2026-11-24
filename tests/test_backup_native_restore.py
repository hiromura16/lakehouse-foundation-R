"""物理復元の原本、空の復元先、確定前の失敗をDockerなしで検査する。"""

import importlib.util
import io
import json
import os
import stat
import sys
import tarfile
import unittest
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
SPEC = importlib.util.spec_from_file_location(
    "backup_native", SCRIPTS / "backup-native.py"
)
backup = importlib.util.module_from_spec(SPEC)
with patch.object(sys, "path", [str(SCRIPTS), *sys.path]):
    SPEC.loader.exec_module(backup)


def archive(*entries, permissions=None):
    """ファイル名・形式・内容を指定して、入力用の非圧縮tarを作る。"""
    memory = io.BytesIO()
    with tarfile.open(mode="w", fileobj=memory) as output:
        for name, kind, data in entries:
            member = tarfile.TarInfo(name)
            member.type = kind
            member.mode = (permissions or {}).get(
                name, 0o700 if kind == tarfile.DIRTYPE else 0o600
            )
            member.uid = os.getuid()
            member.gid = os.getgid()
            member.size = len(data)
            if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                member.linkname = "../../existing"
            output.addfile(member, io.BytesIO(data))
    return memory.getvalue()


class TarTest(unittest.TestCase):
    """tarの検査と展開で同じ制約を適用する。"""

    def test_standard_contents_and_permissions(self):
        """数値所有者・権限と標準のtarの先頭項目を保持する。"""
        data = archive(
            (".", tarfile.DIRTYPE, b""), ("./a", tarfile.REGTYPE, b"1")
        )
        with TemporaryDirectory() as staging:
            backup.audit_archive(io.BytesIO(data), Path(staging))
            restored = Path(staging) / "a"
            self.assertEqual(restored.read_bytes(), b"1")
            self.assertEqual(restored.stat().st_mode & 0o777, 0o600)
            self.assertEqual(restored.stat().st_uid, os.getuid())

    def test_postgres_sticky_directory_keeps_original_mode(self):
        """PostgreSQL 18公式イメージの01777のルートを展開する。"""
        data = archive(
            (".", tarfile.DIRTYPE, b""),
            ("config", tarfile.REGTYPE, b"data"),
            permissions={".": 0o1777},
        )
        backup.audit_archive(io.BytesIO(data))
        with TemporaryDirectory() as staging:
            backup.audit_archive(io.BytesIO(data), Path(staging))
            self.assertEqual(
                stat.S_IMODE(Path(staging).stat().st_mode), 0o1777
            )

    def test_special_permissions_for_non_directories_are_rejected(self):
        """通常ファイルのsticky、setuid、setgidは引き続き拒否する。"""
        for mode in (0o1600, 0o2600, 0o4600):
            data = archive(
                ("sample", tarfile.REGTYPE, b"data"),
                permissions={"sample": mode},
            )
            with (
                self.subTest(mode=mode),
                self.assertRaises(backup.RestoreTarError),
            ):
                backup.audit_archive(io.BytesIO(data))

    def test_unsupported_paths_are_rejected(self):
        """リンク、特殊ファイル、相対パスの越境、重複、親の変更を拒否する。"""
        normal = tarfile.REGTYPE
        unsafe = [
            (("../existing", normal, b""),),
            (("/absolute", normal, b""),),
            (("linked", tarfile.SYMTYPE, b""),),
            (("linked", tarfile.LNKTYPE, b""),),
            (("pipe", tarfile.FIFOTYPE, b""),),
            (("a", normal, b""), ("./a", normal, b"")),
            (("a/b", normal, b""), ("a", normal, b"")),
        ]
        for entries in unsafe:
            with (
                self.subTest(entries=entries),
                self.assertRaises(backup.RestoreTarError),
            ):
                backup.audit_archive(io.BytesIO(archive(*entries)))


class RestoreTest(unittest.TestCase):
    """確定済みの台帳と別ディレクトリで二段階の復元を検査する。"""

    def setUp(self):
        root = TemporaryDirectory()
        self.addCleanup(root.cleanup)
        self.archive = Path(root.name) / "archive"
        self.archive.mkdir()
        self.output = Path(root.name) / "restore"
        contents = archive(("sample", tarfile.REGTYPE, b"original"))
        self.entries = []
        for number, (role, mount) in enumerate(backup.CORE_MOUNTS.items(), 1):
            name = f"volume-{number:03}.tar"
            (self.archive / name).write_bytes(contents)
            self.entries.append(
                {
                    "file": name,
                    "size": len(contents),
                    "sha256": sha256(contents).hexdigest(),
                    "mounts": [{"service": role, "mount": mount}],
                    "volume": f"original_{role}",
                }
            )
        self.manifest = {
            "format_version": 2,
            "native_core": {
                "architecture": "amd64",
                "images": backup.CORE_IMAGES,
            },
            "project": "original",
            "volumes": self.entries,
        }
        self.checksum = backup.publish(self.archive, self.manifest)
        self.docker = ["docker"]

    def run_restore(self, project="restored", checksum=None):
        """同じ採取資料からテスト用App Nameへ復元を指定する。"""
        return backup.restore(
            self.docker,
            project,
            self.archive,
            self.output,
            checksum or self.checksum,
        )

    def test_refuses_tamper_and_same_project_before_docker(self):
        """台帳ハッシュ・元プロジェクト・原本ハッシュを変更できない。"""
        with patch.object(backup, "call") as docker:
            with self.assertRaises(backup.BackupError):
                self.run_restore(checksum="a" * 64)
            with self.assertRaises(backup.BackupError):
                self.run_restore(project="original")
            path = self.archive / self.entries[-1]["file"]
            path.write_bytes(path.read_bytes()[:-1] + b"x")
            with self.assertRaises(backup.BackupError):
                self.run_restore()
            docker.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_legacy_verify_still_available(self):
        """保存先が記録されていない台帳もハッシュ照合できる。"""
        self.manifest["format_version"] = 1
        del self.manifest["native_core"]
        checksum = backup.publish(self.archive, self.manifest)
        self.assertEqual(backup.verify(self.archive, checksum)["volumes"], 2)
        with patch.object(backup, "call") as docker:
            with self.assertRaises(backup.BackupError):
                self.run_restore(checksum=checksum)
            docker.assert_not_called()

    def test_existing_project_fails_before_claim(self):
        """対象の既存Composeは停止中でも再利用しない。"""
        with (
            patch.object(
                backup, "image_platforms", return_value={("linux", "amd64")}
            ),
            patch.object(backup, "call", return_value=b"existing\n") as docker,
            self.assertRaises(backup.BackupError),
        ):
            self.run_restore()
        self.assertEqual(docker.call_count, 1)
        self.assertEqual(docker.call_args.args[1], "ps")

    def test_second_volume_failure_leaves_no_ready_overlay(self):
        """未確定のボリューム名を台帳に残し、Composeから起動できなくする。"""
        with (
            patch.object(
                backup, "image_platforms", return_value={("linux", "amd64")}
            ),
            patch.object(backup, "empty_project"),
            patch.object(backup, "call", return_value=b"claim\n"),
            patch.object(backup, "create_staging_volume") as stage,
            patch.object(
                backup,
                "restore_volume",
                side_effect=(None, backup.BackupError("test")),
            ),
            patch.object(backup, "postgres_state") as state,
            self.assertRaises(backup.BackupError),
        ):
            self.run_restore()
        self.assertEqual(stage.call_count, 2)
        state.assert_not_called()
        self.assertTrue((self.output / "plan.json").is_file())
        self.assertFalse((self.output / "compose.restore.yaml").exists())
        self.assertEqual(
            backup.verify(self.archive, self.checksum)["volumes"], 2
        )

    def test_publishes_overlay_after_both_volumes_and_check(self):
        """確定したoverlayは今回の新規ボリュームだけを参照する。"""
        with (
            patch.object(
                backup, "image_platforms", return_value={("linux", "amd64")}
            ),
            patch.object(backup, "empty_project"),
            patch.object(backup, "call", return_value=b"claim\n"),
            patch.object(backup, "create_staging_volume"),
            patch.object(backup, "restore_volume"),
            patch.object(backup, "postgres_state") as state,
        ):
            result = self.run_restore()
        overlay = json.loads(
            (self.output / "compose.restore.yaml").read_text()
        )
        plan = json.loads((self.output / "plan.json").read_text())
        self.assertEqual(result, backup.verify(self.archive, self.checksum))
        self.assertEqual(
            overlay["volumes"],
            {
                role: {"external": True, "name": volume}
                for role, volume in plan["volumes"].items()
            },
        )
        state.assert_called_once_with(
            self.docker, plan["volumes"]["catalog-db"], "/archive/18/docker"
        )
        for volume in plan["volumes"].values():
            self.assertTrue(volume.startswith("foundation-r-restore-"))
            self.assertNotIn(
                volume, [entry["volume"] for entry in self.entries]
            )

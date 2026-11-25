"""実送信せず、キューと独立した合成宛先で確定順を検証する。"""

import io
import json
import multiprocessing
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from dataclasses import asdict
from hashlib import sha256
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

try:
    from native.edge import outbox
except ModuleNotFoundError as missing:
    # 公開ソース用環境にはR2 SDKがない。外部接続はせず例外形状だけ補う。
    if missing.name != "botocore":
        raise

    class OfflineClientError(Exception):
        """R2の条件失敗を合成するための最小例外。"""

        def __init__(self, response, _operation):
            self.response = response

    sdk = ModuleType("botocore")
    sdk_errors = ModuleType("botocore.exceptions")
    sdk_errors.BotoCoreError = RuntimeError
    sdk_errors.ClientError = OfflineClientError
    with patch.dict(
        "sys.modules", {"botocore": sdk, "botocore.exceptions": sdk_errors}
    ):
        from native.edge import outbox


def bundle(directory, release="1", *, approved=True, payload=b"k,v\n1,3\n"):
    """公開可能な合成の承認ファイルだけを作る。"""
    directory.mkdir()
    (directory / "result.csv").write_bytes(payload)
    path = directory / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "format_version": 1,
                "classification": "approved_external" if approved else "local",
                "dataset": "factory_daily_production",
                "release": release,
                "files": [
                    {
                        "name": "result.csv",
                        "size_bytes": len(payload),
                        "sha256": sha256(payload).hexdigest(),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


class Destination:
    """別SQLiteをリモートに見立て、別processからも確定を観測する。"""

    def __init__(self, path, *, crash=False, callback=None):
        self.path = path
        self.crash = crash
        self.callback = callback
        self.reads = []
        with closing(sqlite3.connect(path)) as db, db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS object (
                    key TEXT PRIMARY KEY, ordinal INTEGER NOT NULL, body BLOB
                )"""
            )

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch, **_kwargs):
        """create-onlyで確定した後にのみ強制終了できる。"""
        if Bucket != outbox.DESTINATION or IfNoneMatch != "*":
            raise AssertionError("unsafe synthetic request")
        if self.callback:
            self.callback()
        with closing(sqlite3.connect(self.path)) as db, db:
            result = db.execute(
                """INSERT OR IGNORE INTO object
                   SELECT ?, count(*), ? FROM object""",
                (Key, Body),
            )
            if result.rowcount == 0:
                raise outbox.r2.ClientError(
                    {"ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject"
                )
        if self.crash and Key.endswith("/manifest.json"):
            os._exit(73)

    def get_object(self, *, Bucket, Key):
        """メタデータでなく永続バイトを返す。"""
        if Bucket != outbox.DESTINATION:
            raise AssertionError("unsafe synthetic request")
        self.reads.append(Key)
        with closing(sqlite3.connect(self.path)) as db:
            row = db.execute(
                "SELECT body FROM object WHERE key=?", (Key,)
            ).fetchone()
        if row is None:
            raise outbox.r2.ClientError(
                {"ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject"
            )
        return {"Body": io.BytesIO(row[0])}

    def objects(self):
        """実際に確定した順を返し、再実行で増えないことを確認する。"""
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute(
                "SELECT key,body FROM object ORDER BY ordinal"
            ).fetchall()


def crash_after_remote_commit(queue, remote, limits):
    """callbackが戻る前にプロセスを終了し、ローカルACKを残さない。"""
    outbox.Outbox(
        queue, outbox.Limits(**limits), clock=lambda: 100
    ).deliver_one(Destination(remote, crash=True))


class OutboxTest(unittest.TestCase):
    """固定上限、原本、確定と再開を同じ公開経路から調べる。"""

    def setUp(self):
        temp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = Path(temp)
        self.path = self.root / "queue.sqlite"
        self.remote = self.root / "remote.sqlite"
        self.now = 100
        self.limits = outbox.Limits(
            pending=2, bytes=4096, records=3, attempts=3, delay=5, max_delay=9
        )
        self.queue = self.open_queue()

    def open_queue(self):
        """接続ハンドルを共有せずに同じ台帳を開く。"""
        return outbox.Outbox(self.path, self.limits, clock=lambda: self.now)

    def test_real_process_crash_after_remote_commit_replays_frozen_bundle(
        self,
    ):
        manifest = bundle(self.root / "one")
        self.assertTrue(self.queue.enqueue(manifest))
        proc = multiprocessing.get_context("spawn").Process(
            target=crash_after_remote_commit,
            args=(self.path, self.remote, asdict(self.limits)),
        )
        proc.start()
        try:
            proc.join(8)
            self.assertEqual(proc.exitcode, 73)
        finally:
            if proc.is_alive():
                proc.terminate()
                proc.join(5)
            proc.close()
        remote = Destination(self.remote)
        before = remote.objects()
        self.assertEqual(
            [key.rsplit("/", 1)[-1] for key, _ in before],
            ["result.csv", "manifest.json"],
        )
        reopened = self.open_queue()
        status = reopened.summary()
        self.assertEqual(status["attempts"], 1)
        self.assertEqual(status["in_flight"], 1)
        self.assertIsNone(status["last_confirmed_at"])
        self.assertIsNone(status["last_ack_at"])
        self.assertEqual(reopened.deliver_one(remote), "idle")
        (manifest.parent / "result.csv").write_bytes(b"changed")
        self.now = 105
        self.assertEqual(reopened.deliver_one(remote), "acked")
        self.assertEqual(remote.objects(), before)
        self.assertEqual(
            remote.reads, [key for key, _ in before for _ in range(2)]
        )
        status = self.open_queue().summary()
        self.assertEqual(status["attempts"], 2)
        self.assertEqual(status["last_confirmed_at"], 105)
        self.assertEqual(status["last_ack_at"], 105)
        self.assertEqual(status["retained_bytes"], 0)

    def test_durable_confirmation_without_ack_needs_no_new_send(self):
        self.queue.enqueue(bundle(self.root / "one"))
        with patch.object(self.queue, "_ack", side_effect=InterruptedError):
            with self.assertRaises(InterruptedError):
                self.queue.deliver_one(Destination(self.remote))
        self.now = 101
        reopened = self.open_queue()
        status = reopened.summary()
        self.assertEqual(status["last_confirmed_at"], 100)
        self.assertIsNone(status["last_ack_at"])
        self.assertEqual(status["confirmed_without_ack"], 1)
        client = Mock()
        self.assertEqual(reopened.deliver_one(client), "acked")
        self.assertEqual(client.mock_calls, [])
        self.assertEqual(reopened.summary()["last_ack_at"], 101)

    def test_worker_guard_does_not_hold_sqlite_write_transaction(self):
        first = bundle(self.root / "one")
        second = bundle(self.root / "two", "2")
        self.queue.enqueue(first)
        another = self.open_queue()
        called = []

        def both():
            if not called:
                called.append(another.deliver_one(Mock()))
                called.append(another.enqueue(second))

        client = Destination(self.remote, callback=both)
        self.assertEqual(self.queue.deliver_one(client), "acked")
        self.assertEqual(called, ["busy", True])
        self.assertEqual(another.summary()["pending"], 1)

    def test_retry_bound_and_quarantine_retain_data_and_no_ack(self):
        first = bundle(self.root / "one")
        second = bundle(self.root / "two", "2")
        self.assertTrue(self.queue.enqueue(first))
        client = Mock()
        client.put_object.side_effect = OSError("private-message-not-stored")
        for now, expected in (
            (100, "pending"),
            (105, "pending"),
            (114, "quarantined"),
        ):
            self.now = now
            self.assertEqual(self.queue.deliver_one(client), expected)
            if expected == "pending":
                self.assertEqual(self.queue.deliver_one(client), "idle")
        status = self.open_queue().summary()
        self.assertEqual(status["attempts"], 3)
        self.assertEqual(status["quarantined"], 1)
        self.assertGreater(status["retained_bytes"], 0)
        self.assertIsNone(status["last_ack_at"])
        self.assertFalse(self.queue.enqueue(first))
        self.assertTrue(self.queue.enqueue(second))
        with self.assertRaisesRegex(outbox.OutboxError, "OUTBOX_CAPACITY"):
            self.queue.enqueue(bundle(self.root / "three", "3"))

    def test_corrupt_frozen_blob_never_leaves_queue(self):
        self.queue.enqueue(bundle(self.root / "one"))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE part SET body = X'010203' WHERE position=0")
        client = Mock()
        self.assertEqual(self.queue.deliver_one(client), "quarantined")
        self.assertEqual(client.mock_calls, [])
        self.assertIsNone(self.queue.summary()["last_ack_at"])

    def test_target_content_conflict_is_not_overwritten_or_acked(self):
        manifest = bundle(self.root / "one")
        identity, _, _ = outbox.r2.load_bundle(manifest)
        self.queue.enqueue(manifest)
        client = Destination(self.remote)
        client.put_object(
            Bucket=outbox.DESTINATION,
            Key=f"{identity}/result.csv",
            Body=b"incompatible",
            IfNoneMatch="*",
        )
        self.assertEqual(self.queue.deliver_one(client), "quarantined")
        self.assertEqual(
            [body for _, body in client.objects()], [b"incompatible"]
        )
        self.assertIsNone(self.queue.summary()["last_ack_at"])

    def test_approval_byte_and_history_bounds(self):
        bad = bundle(self.root / "bad", "4", approved=False)
        with self.assertRaisesRegex(outbox.OutboxError, "BUNDLE_NOT_APPROVED"):
            self.queue.enqueue(bad)
        large = bundle(self.root / "large", "5", payload=b"x" * 4096)
        with self.assertRaisesRegex(outbox.OutboxError, "OUTBOX_CAPACITY"):
            self.queue.enqueue(large)
        remote = Destination(self.remote)
        for index in range(3):
            path = bundle(self.root / f"b{index}", str(index + 1))
            self.assertTrue(self.queue.enqueue(path))
            self.assertEqual(self.queue.deliver_one(remote), "acked")
            self.assertFalse(self.queue.enqueue(path))
        with self.assertRaisesRegex(outbox.OutboxError, "OUTBOX_CAPACITY"):
            self.queue.enqueue(bundle(self.root / "new", "9"))
        status = self.queue.summary()
        self.assertEqual(status["records"], 3)
        self.assertEqual(status["retained_bytes"], 0)

    def test_successful_write_without_durable_objects_cannot_ack(self):
        self.queue.enqueue(bundle(self.root / "one"))
        noop = Mock(spec=["put_object", "get_object"])
        noop.get_object.side_effect = outbox.r2.ClientError(
            {"ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject"
        )
        retained = self.queue.summary()["retained_bytes"]
        self.assertEqual(self.queue.deliver_one(noop), "pending")
        self.assertEqual(noop.put_object.call_count, 1)
        self.assertEqual(noop.get_object.call_count, 1)
        reopened = self.open_queue().summary()
        self.assertEqual(reopened["retained_bytes"], retained)
        self.assertEqual(reopened["acked"], 0)
        self.assertIsNone(reopened["last_confirmed_at"])
        self.assertIsNone(reopened["last_ack_at"])

    def test_missing_artifact_never_publishes_manifest(self):
        self.queue.enqueue(bundle(self.root / "one"))
        remote = Destination(self.remote)
        put = remote.put_object

        def discard_results(**options):
            if options["Key"].endswith("/manifest.json"):
                put(**options)

        with patch.object(
            remote, "put_object", side_effect=discard_results
        ) as attempts:
            self.assertEqual(self.queue.deliver_one(remote), "pending")
        self.assertEqual(attempts.call_count, 1)
        self.assertTrue(
            attempts.call_args.kwargs["Key"].endswith("/result.csv")
        )
        self.assertEqual(remote.objects(), [])
        self.assertGreater(self.queue.summary()["retained_bytes"], 0)
        self.assertIsNone(self.queue.summary()["last_ack_at"])

    def test_partial_destination_missing_manifest_cannot_ack(self):
        self.queue.enqueue(bundle(self.root / "one"))
        remote = Destination(self.remote)
        put = remote.put_object

        def discard_manifest(**options):
            if not options["Key"].endswith("/manifest.json"):
                put(**options)

        with patch.object(remote, "put_object", side_effect=discard_manifest):
            self.assertEqual(self.queue.deliver_one(remote), "pending")
        self.assertEqual(len(remote.objects()), 1)
        self.assertEqual(
            [key.rsplit("/", 1)[-1] for key in remote.reads],
            ["result.csv", "manifest.json"],
        )
        self.assertGreater(self.queue.summary()["retained_bytes"], 0)
        self.assertIsNone(self.queue.summary()["last_ack_at"])

    def test_readback_integrity_and_transport_fail_closed(self):
        for index, (body, expected) in enumerate(
            (
                (b"short", "quarantined"),
                (b"oversize-data", "quarantined"),
                (b"bad-data", "quarantined"),
                (OSError("private-transport-message"), "pending"),
            )
        ):
            with self.subTest(index=index):
                temp = self.root / f"readback-{index}"
                temp.mkdir()
                queue = outbox.Outbox(
                    temp / "outbox.sqlite", clock=lambda: self.now
                )
                queue.enqueue(bundle(temp / "bundle"))
                client = Mock(spec=["put_object", "get_object"])
                if isinstance(body, OSError):
                    client.get_object.side_effect = body
                else:
                    client.get_object.return_value = {"Body": io.BytesIO(body)}
                self.assertEqual(queue.deliver_one(client), expected)
                result = queue.summary()
                self.assertGreater(result["retained_bytes"], 0)
                self.assertEqual(result["acked"], 0)
                self.assertIsNone(result["last_confirmed_at"])

    def test_existing_instance_rechecks_parent_permissions(self):
        self.root.chmod(0o755)
        try:
            with self.assertRaisesRegex(
                outbox.OutboxError, "PRIVATE_DIRECTORY_REQUIRED"
            ):
                self.queue.summary()
            with self.assertRaisesRegex(
                outbox.OutboxError, "PRIVATE_DIRECTORY_REQUIRED"
            ):
                self.queue.deliver_one(Mock())
        finally:
            self.root.chmod(0o700)

    def test_worker_inode_cannot_release_sqlite_cross_process_lock(self):
        script = """
import sqlite3
import sys
from contextlib import closing
with closing(sqlite3.connect(
    sys.argv[1], timeout=0.15, isolation_level=None
)) as db:
    try:
        db.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as err:
        if err.sqlite_errorcode != sqlite3.SQLITE_BUSY:
            raise
        raise SystemExit(17)
    db.rollback()
"""

        def writer_returncode():
            result = subprocess.run(
                [sys.executable, "-B", "-c", script, str(self.path)],
                capture_output=True,
                check=False,
                timeout=5,
            )
            return result.returncode

        with self.queue._transaction():
            self.assertEqual(writer_returncode(), 17)
            self.queue._prepare_database()
            with self.queue._worker_file() as fd:
                self.assertNotEqual(
                    os.fstat(fd).st_ino, self.path.stat().st_ino
                )
            self.assertEqual(writer_returncode(), 17)
        self.assertEqual(writer_returncode(), 0)

    def test_private_queue_and_stable_limits_are_required(self):
        with self.assertRaisesRegex(outbox.OutboxError, "LIMITS_MISMATCH"):
            outbox.Outbox(self.path)
        share = self.root / "others"
        share.mkdir()
        share.chmod(0o755)
        with self.assertRaisesRegex(
            outbox.OutboxError, "PRIVATE_DIRECTORY_REQUIRED"
        ):
            outbox.Outbox(share / "queue.sqlite")
        alias = self.root / "alias"
        alias.hardlink_to(self.path)
        with self.assertRaisesRegex(
            outbox.OutboxError, "PRIVATE_DATABASE_REQUIRED"
        ):
            outbox.Outbox(alias, self.limits)

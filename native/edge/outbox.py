"""承認済みGoldの永続台帳。実行入口・接続クライアントは設けない。"""

import json
import os
import re
import sqlite3
import stat
import tempfile
import time
from contextlib import closing, contextmanager
from dataclasses import dataclass
from fcntl import LOCK_EX, LOCK_NB, flock
from hashlib import sha256
from pathlib import Path

from native.edge import r2

DESTINATION = "synthetic-gold-outbox"

SCHEMA = """
CREATE TABLE IF NOT EXISTS limits (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    policy TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery (
    sequence INTEGER PRIMARY KEY,
    identity TEXT NOT NULL UNIQUE,
    size_bytes INTEGER NOT NULL CHECK(size_bytes > 0),
    queued_at INTEGER NOT NULL,
    state TEXT NOT NULL
        CHECK(state IN ('pending', 'in_flight', 'quarantined', 'acked')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    next_at INTEGER NOT NULL,
    last_attempt_at INTEGER,
    confirmed_at INTEGER,
    acked_at INTEGER,
    diagnostic TEXT
);
CREATE TABLE IF NOT EXISTS part (
    sequence INTEGER NOT NULL REFERENCES delivery(sequence),
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    mime TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    body BLOB NOT NULL,
    PRIMARY KEY (sequence, position),
    UNIQUE (sequence, name)
);
"""

TOTALS = """
SELECT count(*) AS records,
       count(*) FILTER (WHERE state <> 'acked') AS retained,
       coalesce(sum(size_bytes) FILTER (WHERE state <> 'acked'), 0)
           AS retained_bytes,
       count(*) FILTER (WHERE state IN ('pending', 'in_flight')) AS pending,
       count(*) FILTER (WHERE state = 'in_flight') AS in_flight,
       count(*) FILTER (WHERE state = 'in_flight' AND confirmed_at IS NOT NULL)
           AS confirmed_without_ack,
       count(*) FILTER (WHERE state = 'pending' AND attempts > 0)
           AS scheduled_retries,
       count(*) FILTER (WHERE state = 'quarantined') AS quarantined,
       count(*) FILTER (WHERE state = 'acked') AS acked,
       min(queued_at) FILTER (WHERE state <> 'acked') AS oldest_unacked_at,
       coalesce(sum(attempts), 0) AS attempts,
       max(last_attempt_at) AS last_attempt_at,
       max(confirmed_at) AS last_confirmed_at,
       max(acked_at) AS last_ack_at
FROM delivery
"""


class OutboxError(ValueError):
    """運用や試験で使う固定コード。"""


@dataclass(frozen=True)
class Limits:
    """未ACKデータと完了メタデータを別に制限する。"""

    pending: int = 64
    bytes: int = 128 * 1024 * 1024
    records: int = 2048
    attempts: int = 5
    delay: int = 30
    max_delay: int = 600

    def encoded(self):
        """数値以外と非現実的な上限を拒否する。"""
        policy = vars(self)
        bounds = (512, 2**31, 65536, 16, 86400, 86400)
        if (
            not all(
                type(value) is int and 1 <= value <= bound
                for value, bound in zip(policy.values(), bounds, strict=True)
            )
            or self.delay > self.max_delay
        ):
            raise OutboxError("INVALID_LIMITS")
        return json.dumps(policy, sort_keys=True)


def private(info):
    """所有者以外がアクセスできないローカル資源に限定する。"""
    return info.st_uid == os.getuid() and not info.st_mode & 0o077


def identity_for(document, digest):
    """承認済みmanifestと同じ不変の宛先キーを再構成する。"""
    return f"exports/{document['dataset']}/{document['release']}/{digest}"


class Outbox:
    """Linuxの非共有ローカルディスク上で単一配送者を強制する。"""

    def __init__(self, path, limits=Limits(), *, clock=time.time):
        target = Path(path)
        self.path = target.parent.resolve(strict=True) / target.name
        if not target.name or self.path.name.endswith(
            (".delivery.lock", "-journal", "-wal", "-shm")
        ):
            raise OutboxError("INVALID_DATABASE_NAME")
        self.lock_path = self.path.with_name(self.path.name + ".delivery.lock")
        self.limits = limits
        self.clock = clock
        policy = limits.encoded()
        self._prepare_database()
        with self._transaction() as db:
            for sql in SCHEMA.strip().split(";"):
                if sql.strip():
                    db.execute(sql)
            db.execute("INSERT OR IGNORE INTO limits VALUES (1, ?)", (policy,))
            if db.execute("SELECT policy FROM limits").fetchone()[0] != policy:
                raise OutboxError("LIMITS_MISMATCH")

    def _check_parent(self):
        """所有者が管理する固定の親ディレクトリだけを使用する。"""
        directory = self.path.parent.lstat()
        if not stat.S_ISDIR(directory.st_mode) or not private(directory):
            raise OutboxError("PRIVATE_DIRECTORY_REQUIRED")

    @staticmethod
    def _check_private_file(info):
        """単独リンクの所有者限定ファイルだけを使用する。"""
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or not private(info)
        ):
            raise OutboxError("PRIVATE_DATABASE_REQUIRED")

    def _check_database(self):
        """既存のDBは生FDを使わず、SQLiteのPOSIXロックを維持する。"""
        self._check_parent()
        self._check_private_file(self.path.lstat())

    def _prepare_database(self):
        """最初だけ閉じた専用一時inodeを、既存ファイル非置換で用意する。"""
        self._check_parent()
        if not self.path.exists() and not self.path.is_symlink():
            fd, name = tempfile.mkstemp(
                prefix=".outbox-initial-", dir=self.path.parent
            )
            pending = Path(name)
            try:
                # SQLiteがDBを開くより先に生FDを閉じる。
                os.close(fd)
                try:
                    self.path.hardlink_to(pending)
                except FileExistsError:
                    pass
            finally:
                pending.unlink()
        self._check_database()

    @contextmanager
    def _worker_file(self):
        """SQLiteと異なるinodeに配送者専用のflockを取る。"""
        self._check_database()
        try:
            self._check_private_file(self.lock_path.lstat())
        except FileNotFoundError:
            pass
        fd = os.open(
            self.lock_path,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
            0o600,
        )
        try:
            self._check_private_file(os.fstat(fd))
            yield fd
        finally:
            os.close(fd)

    @contextmanager
    def _transaction(self):
        """書込み工程ごとに短い確定を行う。配送中は開かない。"""
        self._check_database()
        with closing(
            sqlite3.connect(
                self.path.as_uri() + "?mode=rw",
                uri=True,
                timeout=5,
                isolation_level=None,
            )
        ) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def enqueue(self, manifest_path):
        """既存の承認検査を経た内容を同一transactionで台帳へ凍結する。"""
        try:
            identity, files, manifest = r2.load_bundle(Path(manifest_path))
        except (OSError, KeyError, TypeError, ValueError, RecursionError):
            raise OutboxError("BUNDLE_NOT_APPROVED") from None
        content = [*files, manifest]
        size = sum(len(artifact.content) for artifact in content)
        now = int(self.clock())
        with self._transaction() as db:
            if db.execute(
                "SELECT 1 FROM delivery WHERE identity=?", (identity,)
            ).fetchone():
                return False
            totals = db.execute(TOTALS).fetchone()
            if (
                totals["records"] >= self.limits.records
                or totals["retained"] >= self.limits.pending
                or totals["retained_bytes"] + size > self.limits.bytes
            ):
                raise OutboxError("OUTBOX_CAPACITY")
            cursor = db.execute(
                """INSERT INTO delivery
                   (identity, size_bytes, queued_at, state, next_at)
                   VALUES (?, ?, ?, 'pending', ?)""",
                (identity, size, now, now),
            )
            db.executemany(
                "INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)",
                (
                    (
                        cursor.lastrowid,
                        pos,
                        item.name,
                        item.mime,
                        item.digest,
                        item.content,
                    )
                    for pos, item in enumerate(content)
                ),
            )
        return True

    def summary(self):
        """秘密・配達先・本文・個別IDを含めない集計。"""
        with self._transaction() as db:
            return dict(db.execute(TOTALS).fetchone())

    def _claim(self):
        """開始を永続化し、有効な配送を同時に1件だけ選ぶ。"""
        now = int(self.clock())
        with self._transaction() as db:
            row = db.execute(
                """SELECT * FROM delivery
                   WHERE state IN ('pending', 'in_flight')
                     AND (next_at <= ? OR confirmed_at IS NOT NULL)
                   ORDER BY sequence LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return "idle", None, ()
            seq = row["sequence"]
            if row["confirmed_at"] is not None:
                return "confirmed", row, ()
            if row["attempts"] >= self.limits.attempts:
                db.execute(
                    """UPDATE delivery
                       SET state='quarantined', diagnostic='ATTEMPT_LIMIT'
                       WHERE sequence=?""",
                    (seq,),
                )
                return "quarantined", None, ()
            pause = min(
                self.limits.max_delay, self.limits.delay * 2 ** row["attempts"]
            )
            db.execute(
                """UPDATE delivery
                   SET state='in_flight', attempts=attempts+1,
                       last_attempt_at=?, next_at=?, diagnostic=NULL
                   WHERE sequence=?""",
                (now, now + pause, seq),
            )
            parts = db.execute(
                """SELECT name,mime,sha256,body FROM part
                   WHERE sequence=? ORDER BY position""",
                (seq,),
            ).fetchall()
            return "send", row, tuple(r2.Artifact(*item) for item in parts)

    def _failed(self, seq, *, conflict=None):
        """未知の配送結果をACKせず再試行か隔離として保持する。"""
        with self._transaction() as db:
            attempts = db.execute(
                "SELECT attempts FROM delivery WHERE sequence=?", (seq,)
            ).fetchone()[0]
            state = "pending"
            reason = "UNCONFIRMED_SEND"
            if conflict is not None or attempts >= self.limits.attempts:
                state = "quarantined"
                reason = conflict or "ATTEMPT_LIMIT"
            db.execute(
                "UPDATE delivery SET state=?,diagnostic=? WHERE sequence=?",
                (state, reason, seq),
            )
            return state

    def _confirm(self, seq):
        """宛先の成功確認のみを別commitで保存する。"""
        with self._transaction() as db:
            result = db.execute(
                """UPDATE delivery SET confirmed_at=?
                   WHERE state='in_flight' AND confirmed_at IS NULL
                     AND sequence=?""",
                (int(self.clock()), seq),
            )
            if result.rowcount != 1:
                raise OutboxError("CONFIRM_STATE_MISMATCH")

    def _ack(self, seq):
        """リモート確定がある場合だけローカルACKと本文の解放を行う。"""
        with self._transaction() as db:
            result = db.execute(
                """UPDATE delivery SET state='acked', acked_at=?
                   WHERE state='in_flight' AND confirmed_at IS NOT NULL
                     AND sequence=?""",
                (int(self.clock()), seq),
            )
            if result.rowcount != 1:
                raise OutboxError("ACK_STATE_MISMATCH")
            db.execute("DELETE FROM part WHERE sequence=?", (seq,))

    def deliver_one(self, synthetic_client):
        """注入された試験用クライアントに限定する前提の配送処理。

        接続先や通信の有無は注入側に属し、この関数は強制しない。
        """
        with self._worker_file() as fd:
            try:
                flock(fd, LOCK_EX | LOCK_NB)
            except BlockingIOError:
                return "busy"
            mode, row, parts = self._claim()
            if mode in ("idle", "quarantined"):
                return mode
            seq = row["sequence"]
            if mode == "send":
                try:
                    validate_frozen(row, parts)
                except (KeyError, TypeError, ValueError, RecursionError):
                    return self._failed(seq, conflict="FROZEN_BUNDLE_INVALID")
                try:
                    deliver_verified(synthetic_client, row["identity"], parts)
                except ValueError as error:
                    if error.args in (
                        ("R2_EXISTING_OBJECT_MISMATCH",),
                        ("DESTINATION_READBACK_MISMATCH",),
                    ):
                        return self._failed(
                            seq, conflict="DESTINATION_CONFLICT"
                        )
                    return self._failed(seq)
                except Exception:
                    # 偽宛先やSDK由来の例外内容は台帳・標準出力へ残さない。
                    return self._failed(seq)
                self._confirm(seq)
            self._ack(seq)
            return "acked"


def validate_frozen(row, parts):
    """再試行でも同じ承認済みバイト・型・順序以外は配送しない。"""
    if not 2 <= len(parts) <= 17:
        raise OutboxError("FROZEN_BUNDLE_INVALID")
    manifest = parts[-1]
    document = json.loads(manifest.content, object_pairs_hook=r2.unique_fields)
    if (
        manifest.name != "manifest.json"
        or manifest.mime != "application/json"
        or type(document["format_version"]) is not int
        or document["format_version"] != 1
        or document["classification"] != "approved_external"
        or document["dataset"] not in r2.DATASETS
        or not re.fullmatch(r"[1-9][0-9]{0,18}", document["release"])
        or len(manifest.content) > 32768
        or sum(len(item.content) for item in parts[:-1]) > r2.TOTAL_MAX_BYTES
        or row["identity"] != identity_for(document, manifest.digest)
        or sum(len(item.content) for item in parts) != row["size_bytes"]
    ):
        raise OutboxError("FROZEN_BUNDLE_INVALID")
    entries = document["files"]
    if not isinstance(entries, list) or len(entries) != len(parts) - 1:
        raise OutboxError("FROZEN_BUNDLE_INVALID")
    for item in parts:
        if sha256(item.content).hexdigest() != item.digest:
            raise OutboxError("FROZEN_BUNDLE_INVALID")
    for item, entry in zip(parts[:-1], entries, strict=True):
        if (
            item.name != entry["name"]
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", item.name)
            or type(entry["size_bytes"]) is not int
            or not 1 <= entry["size_bytes"] <= r2.MAX_BYTES
            or item.digest != entry["sha256"]
            or len(item.content) != entry["size_bytes"]
            or r2.MIME_TYPES.get(Path(item.name).suffix) != item.mime
        ):
            raise OutboxError("FROZEN_BUNDLE_INVALID")


def deliver_verified(client, identity, parts):
    """各ファイルの確定を照合してから、最後のmanifestを公開する。"""
    # 検証済みpartsは通常ファイルの後ろにmanifestを1つだけ置く。
    for part in parts:
        key = f"{identity}/{part.name}"
        r2.create_only(client, DESTINATION, key, part)
        response = client.get_object(Bucket=DESTINATION, Key=key)
        with response["Body"] as body:
            saved = body.read(len(part.content) + 1)
        if (
            not isinstance(saved, bytes)
            or len(saved) != len(part.content)
            or sha256(saved).hexdigest() != part.digest
        ):
            raise OutboxError("DESTINATION_READBACK_MISMATCH")

"""閉域CDCを止めず、SQLiteの合成宛先だけを止めるhosted受入。"""

import io
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from queue import Queue
from threading import Event, Thread

from native.edge import outbox
from scripts import cdc_local_continuity as closed
from scripts.cdc_storage_failure import OVERLAY
from scripts.influx_local_acceptance import AcceptanceError, require

DATASET = "factory_daily_production"
LIMITS = outbox.Limits(
    pending=2, bytes=8192, records=3, attempts=2, delay=5, max_delay=5
)


class Unavailable(OSError):
    """合成宛先が格納する前に返す固定のI/O失敗。"""


class Destination:
    """独立SQLiteの作成専用オブジェクトと、固定操作の観察記録。"""

    def __init__(self, path):
        """同じファイルを開き、可用性と記録だけは接続ごとに分ける。"""
        self.path = path
        self.available = True
        self.before_put = None
        self.calls = []
        with self.connect() as db, db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS object (
                    ordinal INTEGER PRIMARY KEY, key TEXT UNIQUE NOT NULL,
                    body BLOB NOT NULL
                )"""
            )

    def connect(self):
        """短命接続にして復旧時に別インスタンスでも永続状態を読む。"""
        return closing(sqlite3.connect(self.path, timeout=5))

    def ensure_available(self, method, key):
        """失敗を記録し、書込みや読取りの前に不通を返す。"""
        if not self.available:
            self.calls.append((f"{method}_failed", key))
            raise Unavailable

    def put_object(
        self, *, Bucket, Key, Body, IfNoneMatch, Metadata, ContentType
    ):
        """送信契約を守った実データだけを格納し、重複書込みは失敗。"""
        require(
            Bucket == outbox.DESTINATION
            and IfNoneMatch == "*"
            and isinstance(Body, bytes)
            and Metadata == {"sha256": sha256(Body).hexdigest()}
            and ContentType == outbox.r2.MIME_TYPES[Path(Key).suffix]
        )
        self.ensure_available("put", Key)
        if self.before_put:
            self.before_put(Key)
        with self.connect() as db, db:
            db.execute(
                "INSERT INTO object(key,body) VALUES (?,?)", (Key, Body)
            )
        self.calls.append(("put", Key))

    def get_object(self, *, Bucket, Key):
        """格納済みの正確なバイトを返し、呼出順を記録する。"""
        require(Bucket == outbox.DESTINATION)
        self.ensure_available("get", Key)
        with self.connect() as db:
            row = db.execute(
                "SELECT body FROM object WHERE key=?", (Key,)
            ).fetchone()
        require(row is not None)
        self.calls.append(("get", Key))
        return {"Body": io.BytesIO(row[0])}

    def objects(self):
        """試験観測専用。可用性を迂回する配送用APIではない。"""
        with self.connect() as db:
            return db.execute(
                "SELECT key,body FROM object ORDER BY ordinal"
            ).fetchall()


def bundle(root, release):
    """Coreのデータとは別に、承認済みと明示した合成バイトを作る。"""
    directory = root / str(release)
    directory.mkdir(mode=0o700)
    assets = [
        ("result.csv", f"key,value\nsynthetic,{release}\n".encode()),
        ("summary.json", json.dumps({"synthetic": release}).encode()),
    ]
    for name, payload in assets:
        (directory / name).write_bytes(payload)
    manifest = directory / "manifest.json"
    metadata = json.dumps(
        {
            "format_version": 1,
            "classification": "approved_external",
            "dataset": DATASET,
            "release": str(release),
            "files": [
                {
                    "name": name,
                    "size_bytes": len(payload),
                    "sha256": sha256(payload).hexdigest(),
                }
                for name, payload in assets
            ],
        }
    ).encode()
    manifest.write_bytes(metadata)
    identity = f"exports/{DATASET}/{release}/{sha256(metadata).hexdigest()}"
    return (
        manifest,
        identity,
        [
            (f"{identity}/{name}", payload)
            for name, payload in [*assets, (manifest.name, metadata)]
        ],
    )


def ledger(path):
    """集計だけでは分からない各配送IDの状態と確定時刻を読み取る。"""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        return db.execute(
            """SELECT identity,state,attempts,next_at,confirmed_at,acked_at
                 FROM delivery ORDER BY sequence"""
        ).fetchall()


def replay_while(deliver, destination, first_key, advance):
    """最初の再送が実際に止まった間にCoreの更新を観測する。"""
    entered = Event()
    release = Event()
    result = Queue(maxsize=1)

    def pause(key):
        """対象の最初のPUTだけを待ち合わせ、送信はまだしない。"""
        if key == first_key:
            entered.set()
            require(release.wait(220))

    def worker():
        """例外や応答本文をログへ持ち出さず、結果を待ち合わせる。"""
        try:
            result.put_nowait(deliver(destination))
        except Exception:
            result.put_nowait("worker_failed")

    destination.before_put = pause
    # 想定外の実装停止でも、fixtureのfinallyとowned資源削除を妨げない。
    thread = Thread(target=worker, daemon=True)
    thread.start()
    try:
        require(entered.wait(10) and thread.is_alive())
        observation = advance()
        require(thread.is_alive() and result.empty())
    finally:
        release.set()
        thread.join(12)
        if thread.is_alive():
            print("FAIL: EDGE_REPLAY_WORKER_CLEANUP", flush=True)
        destination.before_put = None
    require(not thread.is_alive() and result.get_nowait() == "acked")
    return observation


def run_scenario(root, advance, boundary):
    """成功→送信失敗→Core進行→再送と同時進行→厳密照合。"""
    queue_path = root / "queue.sqlite"
    remote_path = root / "destination.sqlite"
    phase = "WARM"

    def require_offline(key):
        """既存の成功オブジェクトにもアクセスできないことを確認する。"""
        try:
            destination.get_object(Bucket=outbox.DESTINATION, Key=key)
        except Unavailable:
            return
        raise AcceptanceError

    def clock():
        """障害と再送の間隔をsleepせずに、固定秒で前進させる。"""
        return now

    try:
        now = 100
        queue = outbox.Outbox(queue_path, LIMITS, clock=clock)
        destination = Destination(remote_path)
        a, b, c = (bundle(root, i) for i in (1, 2, 3))
        first = advance("storage-baseline", 8)
        require(queue.enqueue(a[0]) is True)
        require(queue.deliver_one(destination) == "acked")
        a_done = (a[1], "acked", 1, 105, 100, 100)
        require(ledger(queue_path) == [a_done])
        require(destination.objects() == a[2])
        require_trace(destination.calls, a[2])
        boundary()

        phase = "DESTINATION_DOWN"
        destination.available = False
        canary = a[2][-1][0]
        require_offline(canary)
        require(queue.enqueue(b[0]) is True)
        require(queue.deliver_one(destination) == "pending")
        b_wait = (b[1], "pending", 1, 105, None, None)
        require(ledger(queue_path) == [a_done, b_wait])
        failures = [("get_failed", canary), ("put_failed", b[2][0][0])]
        require(destination.calls[-2:] == failures)
        now = 104
        require(queue.deliver_one(destination) == "idle")
        require(destination.calls[-2:] == failures)
        require(ledger(queue_path) == [a_done, b_wait])
        require(destination.objects() == a[2])

        phase = "CORE_WHILE_DOWN"
        inserted = advance("continuity-insert", 9)
        require_new_snapshots(first, inserted)
        require(queue.enqueue(c[0]) is True)
        c_wait = (c[1], "pending", 0, 104, None, None)
        queue = outbox.Outbox(queue_path, LIMITS, clock=clock)
        require(ledger(queue_path) == [a_done, b_wait, c_wait])
        retained = queue.summary()
        require(
            retained["retained"] == 2 and retained["scheduled_retries"] == 1
        )
        require(
            retained["retained_bytes"]
            == sum(len(data) for _, data in b[2] + c[2])
            and destination.objects() == a[2]
        )
        require_offline(canary)
        require(destination.calls[-3:] == [*failures, ("get_failed", canary)])
        require_trace(destination.calls[:-3], a[2])
        boundary()

        phase = "DESTINATION_RESTORED"
        destination = Destination(remote_path)
        with destination.get_object(Bucket=outbox.DESTINATION, Key=canary)[
            "Body"
        ] as body:
            require(body.read() == a[2][-1][1])
        require(destination.objects() == a[2])
        now = 105

        def while_replay_waits():
            """再送が開始済み・未確定の間に別のCore更新を確定する。"""
            b_active = (b[1], "in_flight", 2, 110, None, None)
            require(ledger(queue_path) == [a_done, b_active, c_wait])
            require(destination.objects() == a[2])
            observed = advance("continuity-update", 10)
            require_new_snapshots(inserted, observed)
            require(ledger(queue_path) == [a_done, b_active, c_wait])
            require(destination.objects() == a[2])
            return observed

        phase = "CORE_DURING_REPLAY"
        updated = replay_while(
            queue.deliver_one, destination, b[2][0][0], while_replay_waits
        )
        phase = "FINAL_REPLAY"
        require(queue.deliver_one(destination) == "acked")
        all_objects = a[2] + b[2] + c[2]
        require(destination.objects() == all_objects)
        require(destination.calls[0] == ("get", canary))
        require_trace(destination.calls[1:], b[2] + c[2])
        final_rows = [
            a_done,
            (b[1], "acked", 2, 110, 105, 105),
            (c[1], "acked", 1, 110, 105, 105),
        ]
        require(ledger(queue_path) == final_rows)
        summary = queue.summary()
        require(
            summary["acked"] == 3
            and summary["attempts"] == 4
            and all(
                summary[key] == 0
                for key in (
                    "retained_bytes",
                    "pending",
                    "quarantined",
                    "confirmed_without_ack",
                )
            )
        )
        history = destination.calls.copy()
        require(all(queue.enqueue(item[0]) is False for item in (a, b, c)))
        require(queue.deliver_one(destination) == "idle")
        require(
            destination.calls == history
            and destination.objects() == all_objects
            and ledger(queue_path) == final_rows
        )

        phase = "CORE_FINAL_CONFIRM"
        deleted = advance("continuity-delete", 11)
        require_new_snapshots(updated, deleted)
        require(advance("continuity-confirm", 11) == deleted)
        boundary()
    except Exception:
        print(f"FAIL: EDGE_{phase}", flush=True)
        raise AcceptanceError from None


def require_trace(calls, objects):
    """各格納の直後に読取りがあり、manifestより先かを順で照合。"""
    require(
        calls
        == [(method, key) for key, _ in objects for method in ("put", "get")]
    )


def require_new_snapshots(previous, current):
    """Raw・Silverの両方が次世代へ進んだことを照合する。"""
    require(
        all(old != new for old, new in zip(previous, current, strict=True))
    )


def edge_case(docker, compose, project, original, checkpoint, body):
    """実コアは既存の閉域fixture、宛先はhosted上の一時SQLiteだけ。"""

    def boundary():
        """起動した同一の閉域資源以外がないことを再確認する。"""
        require(closed.runtime_boundary(docker, compose, project) == original)

    def advance(phase, offset):
        """Coreで実合成イベントと確定位置・世代を照合する。"""
        return closed.observe_workload(
            compose, checkpoint, body, phase, offset
        )

    for phase in ("first", "delete", "resume"):
        compose(
            "exec",
            "-T",
            "lakehouse",
            "python",
            "-",
            phase,
            timeout=110,
            input_text=body,
        )
    with tempfile.TemporaryDirectory(
        prefix="r-cdc-edge-outbox-", dir=os.environ["RUNNER_TEMP"]
    ) as directory:
        run_scenario(Path(directory), advance, boundary)


if __name__ == "__main__":
    raise SystemExit(
        closed.main(
            fixture=edge_case, overlay=OVERLAY, fixture_name="EDGE_OUTBOX"
        )
    )

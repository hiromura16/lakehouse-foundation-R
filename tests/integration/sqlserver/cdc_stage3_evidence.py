"""段階3隔離テストで使う副作用のない証拠検査。"""

import json
import re
from time import monotonic, sleep

DATABASE = "FixtureStageThree"
SCHEMA = "foundation_r_demo"
FIXTURE_IDS = (
    "Stage3Snapshot",
    "Stage3Stream",
    "Stage3AfterRestart",
    "Stage3Cold",
)


class FixtureError(ValueError):
    """Docker/HTTP/SQLの非公開エラー本文を出力対象から外す。"""


def check(condition):
    """想定外の成功・欠落は固定診断で停止する。"""
    if not condition:
        raise FixtureError("EVIDENCE_MISSING")


def poll(observe, accept, *, seconds):
    """観測値が条件を満たすまでのみ、上限付きで待つ。"""
    end = monotonic() + seconds
    while True:
        result = observe()
        if accept(result):
            return result
        if monotonic() >= end:
            raise FixtureError("OBSERVATION_TIMEOUT")
        sleep(1)


def patched_runtime(version):
    """Java 21系列の数値パッチ・ビルドだけを比較し、EAや他系列は止める。"""
    parts = re.fullmatch(r"21\.0\.(\d+)(?:\.(\d+))?\+(\d+)(?:-LTS)?", version)
    return bool(
        parts
        and tuple(int(item or 0) for item in parts.groups()) >= (13, 1, 2)
    )


def decode_events(payload, count):
    """明示した非Schema JSONとtombstone以外を成功扱いしない。"""
    rows = payload.splitlines()
    check(len(rows) == count)
    events = []
    for record in rows:
        key, separator, body = record.partition(b"\t")
        check(separator == b"\t" and bool(key) and bool(body))
        identity = json.loads(key)
        check(type(identity) is dict and set(identity) == {"product_id"})
        identifier = identity["product_id"]
        check(identifier in FIXTURE_IDS)
        value = json.loads(body)
        if value is None:
            events.append((identifier, "t", None, None, None))
            continue
        check(type(value) is dict)
        source = value["source"]
        check(
            source["db"] == DATABASE
            and source["schema"] == SCHEMA
            and source["table"] == "products"
        )
        op = value["op"]
        check(op in ("r", "c", "u", "d"))
        commit = source["commit_lsn"]
        check(
            (op == "r" and commit is None)
            or (
                isinstance(commit, str)
                and bool(re.fullmatch(r"[0-9a-fA-F:]+", commit))
            )
        )
        before, after = value["before"], value["after"]
        check((before is None) == (op in ("r", "c")))
        check((after is None) == (op == "d"))
        for image in (before, after):
            if image is not None:
                check(image["product_id"] == identifier)
        events.append(
            (
                identifier,
                op,
                None if before is None else before["product_name"],
                None if after is None else after["product_name"],
                commit,
            )
        )
    return events


def persisted(runtime, previous, *, commit=None):
    """LSNの初期化または進行を証明するまで、再起動しない。"""
    return poll(
        runtime.offsets,
        lambda current: (
            bool(current)
            and current != previous
            and (
                commit is None or current[0]["offset"]["commit_lsn"] == commit
            )
        ),
        seconds=45,
    )


def assert_events(runtime, expected):
    """想定した順序のイベントだけを照合し、余計な再snapshotを拒否する。"""
    count = poll(
        runtime.high_products, lambda value: value >= len(expected), seconds=90
    )
    got = runtime.events(count)
    check([event[:4] for event in got] == expected)
    return next(
        (event[4] for event in reversed(got) if event[4] is not None), None
    )


def prepare_and_start_worker(compose, preflight, attach, service):
    """broker証跡とSQLネットワークを先に確かめてからworkerを開始する。"""
    preflight()
    compose("up", "--no-start", "--no-deps", "--pull", "never", service)
    attach()
    compose("start", service)


def approved_capture_fixture(host, database, *, stage3, stage_two_targets):
    """対象はloopbackの明示した試験段階の合成DBだけ。"""
    if host != "localhost":
        return False
    return database == DATABASE if stage3 else database in stage_two_targets

"""段階3隔離テストで使う副作用のない証拠検査。"""

import json
import re
from time import monotonic, sleep

DATABASE = "FixtureStageThree"
SCHEMA = "foundation_r_demo"
CDC_CATALOGS = ("change_tables", "captured_columns", "lsn_time_mapping")
SQL_FIXTURE_IMAGE = (
    "mcr.microsoft.com/mssql/server@sha256:"
    "4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f"
)
SQL_CATALOG_FIXTURE_READER = "fixture_cdc_reader"
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


def worker_receipt(content):
    """RESTの状態と障害の大分類だけを公開し、エラー本文を返さない。"""
    allowed = {
        "RUNNING",
        "FAILED",
        "UNASSIGNED",
        "RESTARTING",
        "PAUSED",
        "STOPPED",
    }
    if not isinstance(content, dict):
        return "UNKNOWN", "UNKNOWN", "UNKNOWN"
    connector = content.get("connector")
    tasks = content.get("tasks")
    task = tasks[0] if isinstance(tasks, list) and len(tasks) == 1 else None
    connector_state = (
        connector.get("state") if isinstance(connector, dict) else None
    )
    task_state = task.get("state") if isinstance(task, dict) else None
    trace = task.get("trace") if isinstance(task, dict) else None
    if not isinstance(trace, str):
        trace = ""
    text = trace.casefold()
    if (
        "select permission was denied" in text
        or "execute permission was denied" in text
    ):
        cause = "SQL_PERMISSION"
    elif "certificate" in text and ("tls" in text or "ssl" in text):
        cause = "SQL_OR_BROKER_TLS"
    elif "authorizationexception" in text:
        cause = "KAFKA_AUTHORIZATION"
    elif "unknowntopicorpartitionexception" in text:
        cause = "KAFKA_TOPIC_UNAVAILABLE"
    elif "sqlserverexception" in text:
        cause = "SQL_DRIVER"
    elif text:
        cause = "OTHER"
    else:
        cause = "NOT_REPORTED"
    return (
        connector_state
        if isinstance(connector_state, str) and connector_state in allowed
        else "UNKNOWN",
        task_state
        if isinstance(task_state, str) and task_state in allowed
        else "UNKNOWN",
        cause,
    )


def high_receipt(value):
    """topic名と具体offsetを出さず、観測値があるかを固定文字にする。"""
    if type(value) is int and value >= 0:
        return "PRESENT" if value else "EMPTY"
    return "UNKNOWN"


def console_receipt(stderr):
    """raw stderrを公開せず、直接partition読み取りの失敗を分類する。"""
    text = stderr.lower()
    if b"timeoutexception" in text or b"timed out" in text:
        return "CDC_READ_TIMEOUT_AFTER_HIGH"
    if (
        b"unrecognized option" in text
        or b"unrecognizedoptionexception" in text
    ):
        return "CDC_READ_CLI_OPTION_REJECTED"
    if b"authorizationexception" in text:
        return "CDC_READ_AUTHORIZATION_FAILED"
    return "CDC_READ_COMMAND_FAILED"


def allows_catalog_fixture(container_evidence, binding, site, port):
    """所有ラベル・固定像・loopbackのlistenerが同じ隔離実体か照合。"""
    return (
        re.fullmatch(r"foundation-r-sqlserver-[0-9a-f]{16}", site) is not None
        and port.isdecimal()
        and 0 < int(port) <= 65535
        and container_evidence == [True, SQL_FIXTURE_IMAGE, site]
        and container_evidence[0] is True
        and binding == "127.0.0.1:" + port
    )


def catalog_fixture_grants():
    """当合成readerに必要な2つの正準object-level SELECTだけを返す。"""
    return tuple(
        f"GRANT SELECT ON OBJECT::[cdc].[{table}] "
        f"TO [{SQL_CATALOG_FIXTURE_READER}]"
        for table in CDC_CATALOGS[:2]
    )


def fixture_acl_baseline(rows):
    """直前の許容状態は、接続権のみまたは暗黙接続で直接付与なし。"""
    connect = (None, None, 0, 0, 0, "G", "CONNECT")
    if rows == []:
        return "EMPTY"
    if rows == [connect]:
        return "DATABASE_CONNECT"
    return None


def exact_fixture_catalog_acl(object_ids):
    """このDBで照合した2つの異なるobject IDを固定ACLへ写す。"""
    if (
        type(object_ids) is not tuple
        or len(object_ids) != 2
        or any(type(item) is not int or item <= 0 for item in object_ids)
        or len(set(object_ids)) != 2
    ):
        return None
    return {
        ("cdc", name, 1, oid, 0, "G", "SELECT")
        for name, oid in zip(CDC_CATALOGS[:2], object_ids, strict=True)
    }


def fixture_catalog_acl_matches(before, after, granted):
    """許可済みの2 objectだけ追加され、以前の権限が不変か照合。"""
    return (
        fixture_acl_baseline(before) is not None
        and granted is not None
        and len(after) == len(before) + 2
        and set(after) == set(before) | granted
    )

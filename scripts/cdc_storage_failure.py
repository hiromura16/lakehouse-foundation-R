"""同じ閉域資源上でRaw保存経路の失敗と再開を確認する hosted CI。"""

import ast
import json
import re
import subprocess
import time
from datetime import UTC, datetime

from scripts import cdc_local_continuity as closed
from scripts.influx_local_acceptance import AcceptanceError, require

OVERLAY = closed.FIXTURES / "compose.cdc-storage-failure.yaml"
# 接続先・portはこの試験専用のCompose内の固定DNS。Proxyや資格情報は使わない。
PROBE = """
import http.client

request = http.client.HTTPConnection("storage", 8333, timeout=2)
try:
    request.connect()
except OSError:
    print("TCP_UNAVAILABLE")
else:
    request.request("GET", "/")
    print(f"HTTP_{request.getresponse().status}")
finally:
    request.close()
"""
CONNECTIVITY_ERRORS = {
    "ArrowIOError",
    "ConnectError",
    "ConnectTimeout",
    "ConnectionError",
    "ConnectionRefusedError",
    "EndpointConnectionError",
    "OSError",
    "TimeoutError",
}


def raw_io_locations():
    """同じcheckoutのRaw scan・append呼び出し行だけを失敗証拠に使う。"""
    source = closed.ROOT / "infra/cdc/raw_writer.py"
    function = next(
        item
        for item in ast.parse(source.read_text(encoding="utf-8")).body
        if isinstance(item, ast.FunctionDef) and item.name == "append_raw"
    )
    lines = {
        call.func.end_lineno
        for call in ast.walk(function)
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr in {"to_arrow", "append"}
            and (
                call.func.attr == "to_arrow"
                or (
                    isinstance(call.func.value, ast.Name)
                    and call.func.value.id == "table"
                )
            )
        )
    }
    require(lines)
    return {f"raw_writer.py:{line}" for line in lines}


def relevant_failure(logs, locations):
    """保存先が実際に不通だった区間の型とI/O工程を厳密に照合する。"""
    # 例外メッセージ、発行値、識別子などは解釈も出力もしない。
    failures = []
    for line in logs.splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and item.get("event") == (
            "native.operation_failed"
        ):
            failures.append(item)
    if len(failures) != 1:
        return False
    item = failures[0]
    frames = item.get("locations")
    error = item.get("error_type")
    if not (
        isinstance(error, str)
        and error in CONNECTIVITY_ERRORS
        and isinstance(frames, list)
        and all(isinstance(frame, str) for frame in frames)
    ):
        return False
    seen_runtime = seen_consumer = False
    for frame in frames:
        if re.fullmatch(r"consumer_runtime\.py:[1-9][0-9]*", frame):
            seen_runtime = True
        elif seen_runtime and re.fullmatch(r"consumer\.py:[1-9][0-9]*", frame):
            seen_consumer = True
        elif seen_consumer and frame in locations:
            return True
    return False


def inspect(docker, kind, identity):
    """既知IDの所有・実行状態確認専用。"""
    return json.loads(
        docker(kind, "inspect", "--format", "{{json .}}", identity)
    )


def storage_volume(docker, storage_id, project):
    """物理volumeのID・Compose所有と意図しないbindがないことを照合。"""
    mounts = inspect(docker, "container", storage_id)["Mounts"]
    target = [value for value in mounts if value["Destination"] == "/data"]
    name = f"{project}_storage"
    require(
        len(target) == 1
        and target[0]["Type"] == "volume"
        and target[0]["Name"] == name
        and target[0]["RW"] is True
    )
    volume = inspect(docker, "volume", name)
    require(
        volume["Name"] == name
        and volume["Driver"] == "local"
        and not volume.get("Options")
        and volume["Labels"].get("com.docker.compose.project") == project
        and volume["Labels"].get("com.docker.compose.volume") == "storage"
    )
    return name, volume["CreatedAt"]


def wait_until(observe, expected, *, seconds=75):
    """同じ状態を確認できないタイムアウトを失敗にする。"""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = observe()
        if result == expected:
            return
        time.sleep(0.5)
    raise AcceptanceError


def evidence(value, expected):
    """工程間の世代・位置比較に使う値の形を制限する。"""
    item = json.loads(value)
    require(
        isinstance(item, dict)
        and set(item)
        == {"raw_snapshot_id", "release_snapshot_id", "broker_next_offset"}
        and type(item["raw_snapshot_id"]) is int
        and item["raw_snapshot_id"] > 0
        and isinstance(item["release_snapshot_id"], str)
        and re.fullmatch(r"[1-9][0-9]*", item["release_snapshot_id"])
        and type(item["broker_next_offset"]) is int
        and item["broker_next_offset"] == expected
    )
    return item


def storage_case(docker, compose, project, original, checkpoint, body):
    """1件の追加だけを使い、現在値・Rawと両方の確定位置を照合する。"""
    ids, _ = original
    storage = ids["storage"]
    consumer = ids["native-cdc"]
    broker = ids["test-broker"]

    def fixture(phase, timeout=130):
        return compose(
            "exec",
            "-T",
            "lakehouse",
            "python",
            "-",
            phase,
            timeout=timeout,
            input_text=body,
        ).strip()

    def probe():
        return compose(
            "exec",
            "-T",
            "lakehouse",
            "python",
            "-",
            timeout=6,
            input_text=PROBE,
        ).strip()

    def state(identity):
        return inspect(docker, "container", identity)["State"]

    def boundary(*inactive):
        require(
            closed.runtime_boundary(
                docker, compose, project, inactive=frozenset(inactive)
            )
            == original
        )

    stage = "BEFORE"
    try:
        config = inspect(docker, "container", consumer)
        require(config["HostConfig"]["RestartPolicy"]["Name"] == "no")
        require(config["RestartCount"] == 0)
        started = config["State"]["StartedAt"]
        broker_start = state(broker)["StartedAt"]
        owned_volume = storage_volume(docker, storage, project)
        wait_until(probe, "HTTP_403")
        for action in ("first", "delete", "resume"):
            fixture(action)
        checkpoint(8)
        before = evidence(fixture("storage-baseline"), 9)

        stage = "RAW_FAILURE"
        boundary()
        docker("stop", "--time", "20", storage, timeout=35)
        boundary("storage")
        wait_until(probe, "TCP_UNAVAILABLE")
        since = datetime.now(UTC).isoformat(timespec="microseconds")
        require(state(consumer)["Running"] is True)
        require(fixture("storage-publish", timeout=25) == "DELIVERED")
        wait_until(lambda: state(consumer)["Status"], "exited", seconds=120)
        ended = inspect(docker, "container", consumer)
        require(
            ended["State"]["ExitCode"] == 1
            and ended["State"]["OOMKilled"] is False
            and ended["State"]["StartedAt"] == started
            and ended["RestartCount"] == 0
            and state(broker)["StartedAt"] == broker_start
        )
        require(
            relevant_failure(
                docker("logs", "--since", since, "--tail", "30", consumer),
                raw_io_locations(),
            )
        )
        boundary("storage", "native-cdc")
        require(probe() == "TCP_UNAVAILABLE")
        checkpoint(8)
        require(
            json.loads(fixture("storage-old-cursor"))
            == {"broker_next_offset": 9}
        )

        stage = "RESTORE_START"
        docker("start", storage)
        stage = "RESTORE_READINESS"
        wait_until(
            lambda: state(storage)["Health"]["Status"], "healthy", seconds=150
        )
        wait_until(probe, "HTTP_403")
        stage = "RESTORE_OWNERSHIP"
        boundary("native-cdc")
        require(storage_volume(docker, storage, project) == owned_volume)
        stage = "RESTORE_PENDING_CHECKPOINT"
        checkpoint(8)
        stage = "RESTORE_PENDING_GENERATION_AND_BROKER"
        require(evidence(fixture("storage-restored"), 9) == before)

        stage = "EXACT_RECOVERY"
        docker("start", consumer)
        after = evidence(fixture("storage-recovered"), 10)
        checkpoint(9)
        require(
            after["raw_snapshot_id"] != before["raw_snapshot_id"]
            and after["release_snapshot_id"] != before["release_snapshot_id"]
        )
        require(evidence(fixture("storage-recovered"), 10) == after)
        checkpoint(9)
        boundary()
        require(
            storage_volume(docker, storage, project) == owned_volume
            and state(broker)["StartedAt"] == broker_start
            and state(consumer)["StartedAt"] != started
        )
    except (
        AcceptanceError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
        StopIteration,
        subprocess.SubprocessError,
    ):
        print(f"FAIL: STORAGE_{stage}", flush=True)
        raise AcceptanceError from None


if __name__ == "__main__":
    raise SystemExit(closed.main(fixture=storage_case, overlay=OVERLAY))

"""閉域の合成CDCで型不正の隔離と不正JSONの停止を区別する。"""

import json
import re
import subprocess
from datetime import UTC, datetime

from scripts import cdc_local_continuity as closed
from scripts.cdc_storage_failure import OVERLAY, inspect, wait_until
from scripts.influx_local_acceptance import AcceptanceError, require


def sample(value, expected):
    """識別子や位置を妥当な型・値として受理し、本文は出力しない。"""
    item = json.loads(value)
    require(
        isinstance(item, dict)
        and set(item)
        == {
            "raw_snapshot_id",
            "release_snapshot_id",
            "broker_next_offset",
            "healthy_digest",
        }
        and type(item["raw_snapshot_id"]) is int
        and item["raw_snapshot_id"] > 0
        and isinstance(item["release_snapshot_id"], str)
        and re.fullmatch(r"[1-9][0-9]*", item["release_snapshot_id"])
        and type(item["broker_next_offset"]) is int
        and item["broker_next_offset"] == expected
        and isinstance(item["healthy_digest"], str)
        and re.fullmatch(r"[a-f0-9]{64}", item["healthy_digest"])
    )
    return item


def parser_location():
    """既存generic consumerのJSON parse工程だけを失敗根拠にする。"""
    lines = (
        (closed.ROOT / "infra/cdc/consumer.py")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    needle = 'source = json.loads(record["value"]).get("source", {})'
    matching = [
        number
        for number, line in enumerate(lines, 1)
        if line.strip() == needle
    ]
    require(len(matching) == 1)
    return f"consumer.py:{matching[0]}"


def is_malformed_failure(log, site):
    """他の例外、別の位置や複数例外を、期待した停止へ読み替えない。"""
    reports = []
    for line in log.splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and item.get("event") == (
            "native.operation_failed"
        ):
            reports.append(item)
    if len(reports) != 1:
        return False
    item = reports[0]
    frames = item.get("locations")
    return (
        item.get("error_type") == "JSONDecodeError"
        and isinstance(frames, list)
        and site in frames
        and any(
            isinstance(frame, str)
            and re.fullmatch(r"consumer_runtime\.py:[1-9][0-9]*", frame)
            for frame in frames
        )
    )


def poison_case(docker, compose, project, original, checkpoint, body):
    """良品→型不正→次の良品→不正JSONを、別々に観測する。"""
    consumer = original[0]["native-cdc"]

    def fixture(phase, *, timeout=130):
        """閉域にある既存試験コンテナだけで固定工程を実行する。"""
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

    def process_value():
        """既に所有を検証したconsumerの客観的な実行状態を読む。"""
        return inspect(docker, "container", consumer)

    def boundary(*, exited=False):
        """閉域のID・network等は同じで、指定時だけconsumer停止を認める。"""
        require(
            closed.runtime_boundary(
                docker,
                compose,
                project,
                inactive=frozenset({"native-cdc"}) if exited else frozenset(),
            )
            == original
        )

    phase = "BASELINE"
    try:
        initial = process_value()
        started = initial["State"]["StartedAt"]
        require(
            initial["HostConfig"]["RestartPolicy"]["Name"] == "no"
            and initial["RestartCount"] == 0
            and isinstance(started, str)
            and started
        )
        before = sample(fixture("poison-baseline"), 1)
        checkpoint(0)
        boundary()

        phase = "TYPED_QUARANTINE"
        typed = sample(fixture("poison-typed"), 2)
        checkpoint(1)
        require_new(before, typed)
        boundary()

        phase = "FOLLOWUP"
        followup = sample(fixture("poison-followup"), 3)
        checkpoint(2)
        require_new(typed, followup)
        require(sample(fixture("poison-confirm"), 3) == followup)
        checkpoint(2)
        boundary()
        require(process_value()["State"]["StartedAt"] == started)

        phase = "MALFORMED_STOP"
        since = datetime.now(UTC).isoformat(timespec="microseconds")
        require(fixture("poison-malformed-publish", timeout=25) == "DELIVERED")
        wait_until(
            lambda: process_value()["State"]["Status"], "exited", seconds=60
        )
        after = process_value()
        require(
            after["State"]["ExitCode"] == 1
            and after["State"]["OOMKilled"] is False
            and after["State"]["StartedAt"] == started
            and after["RestartCount"] == 0
            and is_malformed_failure(
                docker("logs", "--since", since, "--tail", "30", consumer),
                parser_location(),
            )
        )
        phase = "MALFORMED_EVIDENCE"
        boundary(exited=True)
        checkpoint(2)
        result = sample(fixture("poison-malformed-observe"), 3)
        require(
            result["raw_snapshot_id"] != followup["raw_snapshot_id"]
            and result["release_snapshot_id"]
            == followup["release_snapshot_id"]
            and result["healthy_digest"] == followup["healthy_digest"]
        )
        checkpoint(2)
        boundary(exited=True)
    except (
        AcceptanceError,
        OSError,
        ValueError,
        KeyError,
        TypeError,
        AttributeError,
        subprocess.SubprocessError,
    ):
        print(f"FAIL: CDC_POISON_{phase}", flush=True)
        raise AcceptanceError from None


def require_new(before, after):
    """正常行の全公開列を維持し、新しい世代と両方の確定を待つ。"""
    require(
        before["healthy_digest"] == after["healthy_digest"]
        and before["raw_snapshot_id"] != after["raw_snapshot_id"]
        and before["release_snapshot_id"] != after["release_snapshot_id"]
    )


if __name__ == "__main__":
    raise SystemExit(
        closed.main(
            fixture=poison_case, overlay=OVERLAY, fixture_name="POISON"
        )
    )

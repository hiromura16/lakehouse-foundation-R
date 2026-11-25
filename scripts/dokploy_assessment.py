#!/usr/bin/env python3
"""手元で用意した数値・衝突件数だけを読み、配備余力を比較する。"""

import argparse
import json
import sys

MAX_BYTES = 32_768
MAX_VALUE = 10**12
DIMS = ("cpu_milli", "memory_mib", "disk_mib")
RESERVES = ("os", "docker", "dokploy", "traefik", "existing_poc", "headroom")
COLLISIONS = ("project", "container", "network", "volume", "port")
MODES = (
    "core-local",
    "local-operations",
    "standard",
    "evaluation",
    "legacy-gold-read",
    "zero-trust-access",
    "data-egress",
    "all-lab",
)
NOT_IMPLEMENTED_MODES = {"zero-trust-access", "data-egress", "all-lab"}
THRESHOLDS = {
    "memory_margin_pct": 25,
    "cpu_warn_pct": 80,
    "sample_minutes": 15,
    "disk_warn_pct": 70,
    "disk_critical_pct": 85,
    "disk_stop_pct": 95,
}
MEASUREMENTS = {
    "cpu_busy_pct": 100,
    "cpu_window_minutes": MAX_VALUE,
    "memory_available_mib": MAX_VALUE,
    "disk_free_mib": MAX_VALUE,
    "swap_in_delta_pages": MAX_VALUE,
    "swap_out_delta_pages": MAX_VALUE,
    "swap_window_minutes": MAX_VALUE,
}


class SnapshotError(ValueError):
    """構造エラーとして扱う。入力値やパスは返さない。"""


def exact_keys(value, keys):
    """未知フィールドと不足した必須フィールドを拒否する。"""
    if not isinstance(value, dict) or set(value) != set(keys):
        raise SnapshotError


def integer(value, *, lower=0, upper=MAX_VALUE, nullable=False):
    """bool、float、数値文字列は計測上の整数として扱わない。"""
    if value is None and nullable:
        return
    if type(value) is not int or not lower <= value <= upper:
        raise SnapshotError


def unique_object(fields):
    """同名キーによる上書きを入力段階で拒否する。"""
    result = {}
    for key, value in fields:
        if key in result:
            raise SnapshotError
        result[key] = value
    return result


def bad_constant(_value):
    """JSON外のNaNやInfinityを受け入れない。"""
    raise SnapshotError


def parse_snapshot(source):
    """一度だけ上限付きで読む。機器・ファイルを探索しない。"""
    raw = source.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise SnapshotError
    record = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=unique_object,
        parse_constant=bad_constant,
    )
    exact_keys(
        record,
        (
            "version",
            "mode",
            "capacity",
            "workload",
            "reserves",
            "measured",
            "collisions",
            "thresholds",
        ),
    )
    integer(record["version"], lower=1, upper=1)
    if record["mode"] not in MODES:
        raise SnapshotError
    for label in ("capacity", "workload"):
        exact_keys(record[label], DIMS)
    for name in DIMS:
        integer(record["capacity"][name], lower=1, nullable=True)
        item = record["workload"][name]
        exact_keys(item, ("known_floor", "complete"))
        integer(item["known_floor"], nullable=True)
        if type(item["complete"]) is not bool:
            raise SnapshotError
        if item["complete"] and item["known_floor"] is None:
            raise SnapshotError
    exact_keys(record["reserves"], RESERVES)
    for reserve in record["reserves"].values():
        exact_keys(reserve, DIMS)
        for value in reserve.values():
            integer(value, nullable=True)
    exact_keys(record["measured"], MEASUREMENTS)
    for name, upper in MEASUREMENTS.items():
        integer(record["measured"][name], upper=upper, nullable=True)
    for dim, name in (
        ("memory_mib", "memory_available_mib"),
        ("disk_mib", "disk_free_mib"),
    ):
        value, size = record["measured"][name], record["capacity"][dim]
        if value is not None and size is not None and value > size:
            raise SnapshotError
    exact_keys(record["collisions"], COLLISIONS)
    for item in record["collisions"].values():
        exact_keys(item, ("conflicts", "complete"))
        integer(item["conflicts"], nullable=True)
        if type(item["complete"]) is not bool:
            raise SnapshotError
        if item["complete"] and item["conflicts"] is None:
            raise SnapshotError
    threshold = record["thresholds"]
    if not isinstance(threshold, dict) or not set(threshold) <= set(
        THRESHOLDS
    ):
        raise SnapshotError
    for key, value in threshold.items():
        if key == "sample_minutes":
            integer(value, lower=5, upper=1440)
        elif key == "memory_margin_pct":
            integer(value, lower=20, upper=30)
        else:
            integer(value, lower=1, upper=99)
    threshold = THRESHOLDS | threshold
    if not (
        threshold["disk_warn_pct"]
        < threshold["disk_critical_pct"]
        < threshold["disk_stop_pct"]
    ):
        raise SnapshotError
    record["thresholds"] = threshold
    return record


def ceil_ratio(numerator, denominator):
    """整数演算で上方向の最小量を計算する。"""
    return (numerator + denominator - 1) // denominator


def sizing(record):
    """数値が欠けても既知部分の不足だけは確定できる。"""
    result = {}
    findings = []
    memory_pct = record["thresholds"]["memory_margin_pct"]
    for dimension in DIMS:
        capacity = record["capacity"][dimension]
        workload = record["workload"][dimension]
        floor = workload["known_floor"]
        defined = {
            name: values[dimension]
            for name, values in record["reserves"].items()
        }
        missing = [name for name, value in defined.items() if value is None]
        before_margin = (floor or 0) + sum(
            value for value in defined.values() if value is not None
        )
        minimum = (
            ceil_ratio(before_margin * 100, 100 - memory_pct)
            if dimension == "memory_mib"
            else before_margin
        )
        expansion = None if capacity is None else max(0, minimum - capacity)
        remaining = (
            None
            if capacity is None
            else capacity
            - sum(value for value in defined.values() if value is not None)
        )
        incomplete = (
            capacity is None or not workload["complete"] or bool(missing)
        )
        if expansion:
            findings.append((dimension, "INSUFFICIENT", "KNOWN_SHORTFALL"))
        if incomplete:
            findings.append(
                (dimension, "UNKNOWN", "INCOMPLETE_CAPACITY_EVIDENCE")
            )
        if not expansion and not incomplete:
            findings.append(
                (dimension, "PASS", "AGGREGATE_BOUND_WITHIN_CAPACITY")
            )
        result[dimension] = {
            "known_lower_bound_capacity": minimum,
            "known_lower_bound_expansion": expansion,
            "remaining_after_known_reserves": remaining,
            "incomplete_bound": incomplete,
            "missing_reserves": missing,
        }
    return result, findings


def disk_level(free, size, threshold):
    """現在値と予測値に同じ閾値を用いる。"""
    occupied = (size - free) * 100
    if occupied >= size * threshold["disk_stop_pct"]:
        return "INSUFFICIENT", "DISK_STOP"
    if occupied >= size * threshold["disk_critical_pct"]:
        return "WARN", "DISK_CRITICAL"
    if occupied >= size * threshold["disk_warn_pct"]:
        return "WARN", "DISK_WARN"
    return "PASS", "DISK_BELOW_WARN"


def measured_findings(record):
    """現在の余裕・持続負荷と予算上限を別に判定する。"""
    measured = record["measured"]
    threshold = record["thresholds"]
    window = threshold["sample_minutes"]
    findings = []
    cpu = measured["cpu_busy_pct"]
    minutes = measured["cpu_window_minutes"]
    if cpu is None or minutes is None or minutes < window:
        findings.append(("cpu_load", "UNKNOWN", "CPU_WINDOW_INCOMPLETE"))
    elif cpu >= threshold["cpu_warn_pct"]:
        findings.append(("cpu_load", "WARN", "SUSTAINED_CPU_WARN"))
    else:
        findings.append(("cpu_load", "PASS", "SUSTAINED_CPU_BELOW_WARN"))
    memory = measured["memory_available_mib"]
    memory_size = record["capacity"]["memory_mib"]
    if memory is None or memory_size is None:
        findings.append(
            ("memory_available", "UNKNOWN", "MEMORY_AVAILABLE_UNKNOWN")
        )
    elif memory * 100 < memory_size * threshold["memory_margin_pct"]:
        findings.append(
            ("memory_available", "WARN", "MEMORY_AVAILABLE_BELOW_MARGIN")
        )
    else:
        findings.append(("memory_available", "PASS", "MEMORY_MARGIN_OBSERVED"))
    disk = measured["disk_free_mib"]
    disk_size = record["capacity"]["disk_mib"]
    if disk is None or disk_size is None:
        findings.append(("disk_pressure", "UNKNOWN", "DISK_FREE_UNKNOWN"))
    else:
        status, reason = disk_level(disk, disk_size, threshold)
        findings.append(("disk_pressure", status, reason))
    swap_in = measured["swap_in_delta_pages"]
    swap_out = measured["swap_out_delta_pages"]
    minutes = measured["swap_window_minutes"]
    if (
        swap_in is None
        or swap_out is None
        or minutes is None
        or minutes < window
    ):
        findings.append(("swap_activity", "UNKNOWN", "SWAP_WINDOW_INCOMPLETE"))
    elif swap_in or swap_out:
        findings.append(("swap_activity", "WARN", "SWAP_ACTIVITY_OBSERVED"))
    else:
        findings.append(("swap_activity", "PASS", "NO_SWAP_ACTIVITY_OBSERVED"))
    return findings


def projected_findings(record):
    """実測空きから追加分の下限だけを引き、上限となる残余を判定する。"""
    findings = []
    remaining = {}
    threshold = record["thresholds"]
    for dimension, name, label in (
        ("memory_mib", "memory_available_mib", "projected_memory"),
        ("disk_mib", "disk_free_mib", "projected_disk"),
    ):
        free = record["measured"][name]
        size = record["capacity"][dimension]
        workload = record["workload"][dimension]
        floor = workload["known_floor"]
        after = (
            free - floor if free is not None and floor is not None else None
        )
        remaining[dimension] = after
        if after is not None and after < 0:
            findings.append(
                (dimension, "INSUFFICIENT", "OBSERVED_FREE_SHORTFALL")
            )
        elif after is None or size is None:
            findings.append((label, "UNKNOWN", "PROJECTED_CAPACITY_UNKNOWN"))
        else:
            if dimension == "disk_mib":
                status, reason = disk_level(after, size, threshold)
            elif after * 100 < size * threshold["memory_margin_pct"]:
                status, reason = "WARN", "MEMORY_AVAILABLE_BELOW_MARGIN"
            else:
                status, reason = "PASS", "MEMORY_MARGIN_OBSERVED"
            if status == "PASS" and not workload["complete"]:
                status, reason = "UNKNOWN", "WORKLOAD_NOT_BOUNDED"
            findings.append((label, status, f"PROJECTED_{reason}"))
    return remaining, findings


def collision_findings(record):
    """識別子を受け取らず、確認済みの衝突だけを報告する。"""
    findings = []
    for kind in COLLISIONS:
        item = record["collisions"][kind]
        if item["conflicts"]:
            findings.append((kind, "INSUFFICIENT", "EXISTING_POC_COLLISION"))
        if not item["complete"]:
            findings.append((kind, "UNKNOWN", "COLLISION_COMPARE_INCOMPLETE"))
        elif not item["conflicts"]:
            findings.append((kind, "PASS", "NO_COLLISION_REPORTED"))
    return findings


def assess(record):
    """計測結果を起動証拠やサイト全体の認可へ読み替えない。"""
    output = {
        "source": "operator-aggregate",
        "mode": record["mode"],
        "startup_verified": False,
        "deployment_approved": False,
    }
    if record["mode"] in NOT_IMPLEMENTED_MODES:
        output.update(status="NOT_IMPLEMENTED", findings=[])
        return output
    resources, findings = sizing(record)
    findings += measured_findings(record)
    remaining, projected = projected_findings(record)
    findings += projected
    findings += collision_findings(record)
    codes = {status for _, status, _ in findings}
    output.update(
        status=next(
            label
            for label in ("INSUFFICIENT", "UNKNOWN", "WARN", "PASS")
            if label in codes
        ),
        resources=resources,
        reported_aggregates=record["measured"],
        projected_remaining_upper_bound_mib=remaining,
        findings=[
            dict(check=check, status=status, reason=reason)
            for check, status, reason in findings
        ],
    )
    return output


class Parser(argparse.ArgumentParser):
    """入力値を誤って引数に渡しても表示しない。"""

    def error(self, _message):
        self.exit(6, '{"error":"INVALID_USAGE"}\n')


def main(argv=None, source=None):
    """計測しない。標準入力の集計スナップショットだけを判定する。"""
    Parser(description=__doc__).parse_args(argv)
    try:
        result = assess(
            parse_snapshot(sys.stdin.buffer if source is None else source)
        )
    except (OSError, ValueError, TypeError, RecursionError):
        print('{"error":"INVALID_SNAPSHOT"}', file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return {
        "PASS": 0,
        "WARN": 2,
        "INSUFFICIENT": 3,
        "UNKNOWN": 4,
        "NOT_IMPLEMENTED": 5,
    }[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())

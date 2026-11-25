"""外部経路と送信待ちを集計する純粋関数。通信や配線は行わない。"""

from dataclasses import dataclass
from enum import Enum

MAX_TIMESTAMP = 2**63 - 1
MAX_DRAIN_AGE_SECONDS = 30
MIN_DRAIN_WINDOW_SECONDS = 60
MAX_DRAIN_WINDOW_SECONDS = 3600
PATHS = {
    "human_access": ("dns", "tunnel", "access"),
    "delivery": ("r2",),
    "notification": ("notification",),
}
COUNT_FIELDS = {
    "records",
    "retained",
    "retained_bytes",
    "pending",
    "in_flight",
    "confirmed_without_ack",
    "scheduled_retries",
    "quarantined",
    "acked",
    "attempts",
}
TIME_FIELDS = {
    "oldest_unacked_at",
    "last_attempt_at",
    "last_confirmed_at",
    "last_ack_at",
}


class StatusError(ValueError):
    """入力の原値をログへ返さない固定理由。"""


class Channel(Enum):
    """評価対象に許可する外部の観測名。"""

    DNS = "dns"
    TUNNEL = "tunnel"
    ACCESS = "access"
    R2 = "r2"
    NOTIFICATION = "notification"


class Result(Enum):
    """観測した結果だけを指定し、未観測は項目を渡さない。"""

    HEALTHY = "healthy"
    FAILED = "failed"


@dataclass(frozen=True)
class Observation:
    """既に呼出側が得た結果とUTCのUnix秒。"""

    channel: Channel
    result: Result
    observed_at: int


@dataclass(frozen=True)
class QueueObservation:
    """同一の台帳と計測期間に呼出側が結び付けた集計サンプル。"""

    epoch: object
    observed_at: int
    summary: dict


def timestamp(value, maximum=MAX_TIMESTAMP):
    """bool、負値、範囲外を時刻・件数として扱わない。"""
    return type(value) is int and 0 <= value <= maximum


def group_status(states):
    """既知の障害を優先し、未確認だけで正常とは判定しない。"""
    if "failed" in states:
        return "cloud-degraded"
    if all(state == "healthy" for state in states):
        return "healthy"
    return "unknown"


def cloud_status(local_core_ready, observations, *, now, max_age_seconds):
    """ローカルのreadinessを変えずに外部経路を個別に評価する。"""
    if (
        type(local_core_ready) is not bool
        or type(observations) is not tuple
        or not timestamp(now)
        or not timestamp(max_age_seconds, 86400)
        or not max_age_seconds
    ):
        raise StatusError("INVALID_STATUS_INPUT")
    channels = {item.value: "unknown" for item in Channel}
    seen = set()
    for item in observations:
        if (
            type(item) is not Observation
            or type(item.channel) is not Channel
            or type(item.result) is not Result
            or not timestamp(item.observed_at, now)
            or item.channel in seen
        ):
            raise StatusError("INVALID_CHANNEL_OBSERVATION")
        seen.add(item.channel)
        channels[item.channel.value] = (
            item.result.value
            if now - item.observed_at <= max_age_seconds
            else "stale"
        )
    return {
        "local_core_ready": local_core_ready,
        "cloud_status": group_status(tuple(channels.values())),
        "channels": channels,
        "paths": {
            path: group_status(tuple(channels[item] for item in members))
            for path, members in PATHS.items()
        },
    }


def queue_stats(sample, *, now):
    """固定のOutbox集計形状だけを受け、数と時刻を先に検証する。"""
    if (
        type(sample) is not QueueObservation
        or type(sample.epoch) is not object
        or not timestamp(sample.observed_at, now)
        or type(sample.summary) is not dict
        or set(sample.summary) != COUNT_FIELDS | TIME_FIELDS
    ):
        raise StatusError("INVALID_QUEUE_OBSERVATION")
    data = sample.summary.copy()
    if any(not timestamp(data[key]) for key in COUNT_FIELDS) or any(
        data[key] is not None and not timestamp(data[key], sample.observed_at)
        for key in TIME_FIELDS
    ):
        raise StatusError("INVALID_QUEUE_COUNTER")
    if (
        data["retained"] != data["pending"] + data["quarantined"]
        or data["retained"] + data["acked"] != data["records"]
        or not data["pending"]
        >= data["in_flight"]
        >= data["confirmed_without_ack"]
        or data["scheduled_retries"] > data["pending"] - data["in_flight"]
        or data["retained_bytes"] < data["retained"]
        or bool(data["retained_bytes"]) != bool(data["retained"])
        or data["attempts"]
        < (data["acked"] + data["in_flight"] + data["scheduled_retries"])
        or (data["oldest_unacked_at"] is not None) != bool(data["retained"])
        or (data["last_attempt_at"] is not None) != bool(data["attempts"])
        or (data["last_ack_at"] is not None) != bool(data["acked"])
        or (data["last_confirmed_at"] is not None)
        != bool(data["acked"] + data["confirmed_without_ack"])
    ):
        raise StatusError("INCONSISTENT_QUEUE_OBSERVATION")
    return data


def drain_projection(current, previous, after, before, now):
    """同一台帳の実測差を照合し、不明な速度を仮定しない。"""
    elapsed = current.observed_at - previous.observed_at
    enqueued = after["records"] - before["records"]
    local_acks = after["acked"] - before["acked"]
    drained = before["pending"] - after["pending"]
    if (
        current.epoch is not previous.epoch
        or not MIN_DRAIN_WINDOW_SECONDS <= elapsed <= MAX_DRAIN_WINDOW_SECONDS
        or now - current.observed_at > MAX_DRAIN_AGE_SECONDS
        or before["quarantined"]
        or after["quarantined"]
        or not after["pending"]
        or enqueued < 0
        or drained <= 0
        or after["attempts"] < before["attempts"]
        or local_acks - enqueued != drained
        or after["last_ack_at"] is None
        or not previous.observed_at < after["last_ack_at"]
    ):
        return None
    return {
        "window_seconds": elapsed,
        "new_queued_bundles": enqueued,
        "local_acks": local_acks,
        "net_drained_bundles": drained,
        "estimated_remaining_seconds": (
            after["pending"] * elapsed + drained - 1
        )
        // drained,
    }


def backlog_metrics(current, *, now, previous=None):
    """本文・接続先・個別IDなしで未ACKと確認/ACKの境界を返す。"""
    if not timestamp(now):
        raise StatusError("INVALID_STATUS_INPUT")
    data = queue_stats(current, now=now)
    estimate = None
    if previous is not None:
        baseline = queue_stats(previous, now=now)
        estimate = drain_projection(current, previous, data, baseline, now)
    oldest = data["oldest_unacked_at"]
    return {
        "observed_at_unix_seconds": current.observed_at,
        "sample_age_seconds": now - current.observed_at,
        "pending_count": data["pending"],
        "unacked_count": data["retained"],
        "unacked_bytes": data["retained_bytes"],
        "oldest_unacked_age_seconds": (
            None if oldest is None else current.observed_at - oldest
        ),
        "in_flight_count": data["in_flight"],
        "confirmed_without_ack_count": data["confirmed_without_ack"],
        "scheduled_retry_count": data["scheduled_retries"],
        "quarantine_count": data["quarantined"],
        "total_attempt_count": data["attempts"],
        "last_remote_confirmed_at_unix_seconds": data["last_confirmed_at"],
        "last_local_ack_at_unix_seconds": data["last_ack_at"],
        "drain_projection": estimate,
    }

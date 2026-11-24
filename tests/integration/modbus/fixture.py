"""Telegrafによる読取専用Modbus収集と品質・鮮度を照合する。"""

import json
import math
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROJECT = "fr-modbus-" + ROOT.name.split(".")[-1].lower()
if not re.fullmatch(r"fr-modbus-[a-z0-9]{8}", PROJECT):
    raise RuntimeError("合成試験の専用プロジェクト名が不正です。")
STALE_NS = 4_000_000_000


@dataclass(frozen=True)
class Sample:
    """この合成試験だけに現れる数値Line Protocolの値とタイムスタンプ。"""

    measurement: str
    tags: dict[str, str]
    fields: dict[str, float | int]
    observed_at_ns: int


def compose(*args: str, mode: str = "normal") -> str:
    """試験プロジェクトだけを対象にし、任意の接続先を受け付けない。"""
    if mode not in {"normal", "invalid", "recovered"}:
        raise ValueError("合成装置のモードが不正です。")
    result = subprocess.run(
        [
            *(
                ("sudo", "-n")
                if os.environ.get("FIXTURE_DOCKER_SUDO") == "1"
                else ()
            ),
            "env",
            f"FOUNDATION_MODBUS_FIXTURE_MODE={mode}",
            "docker",
            "compose",
            "-p",
            PROJECT,
            "-f",
            str(ROOT / "compose.yaml"),
            *args,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    return result.stdout


def parse_samples(log: str) -> list[Sample]:
    """固定フィールド名だけからなる、この試験のLine Protocolを復元する。"""
    samples = []
    for line in log.splitlines():
        if not line.startswith(
            (
                "foundation_modbus,",
                "foundation_modbus_bytes,",
                "internal_gather,",
            )
        ):
            continue
        series, metrics, stamp = line.split(" ")
        name, *raw_tags = series.split(",")
        tags = dict(item.split("=", 1) for item in raw_tags)
        fields = {}
        for component in metrics.split(","):
            key, value = component.split("=", 1)
            fields[key] = (
                int(value[:-1]) if value.endswith(("i", "u")) else float(value)
            )
        samples.append(Sample(name, tags, fields, int(stamp)))
    return samples


def sample_state(sample: Sample | None, now_ns: int) -> dict:
    """計測のない区間、鮮度切れ、欠測コード、利用できる値を区別する。"""
    if sample is None:
        return {"state": "unknown", "value": None}
    stamp = sample.observed_at_ns
    age_ns = now_ns - stamp
    if age_ns < 0:
        return {"state": "unknown", "value": None, "observed_at_ns": stamp}
    if age_ns > STALE_NS:
        return {"state": "stale", "value": None, "observed_at_ns": stamp}
    value = sample.fields.get("temperature_celsius")
    if (
        not sample.fields.get("temperature_valid")
        or value is None
        or not math.isfinite(value)
    ):
        return {"state": "invalid", "value": None, "observed_at_ns": stamp}
    return {"state": "fresh", "value": value, "observed_at_ns": stamp}


def snapshot() -> tuple[Sample, Sample, int]:
    """最後の計測と、Modbus入力固有の収集エラーカウンターを得る。"""
    metrics = parse_samples(compose("logs", "--no-log-prefix", "telegraf"))

    def latest(name: str, **tags: str) -> Sample:
        return max(
            (
                sample
                for sample in metrics
                if sample.measurement == name
                and all(
                    sample.tags.get(key) == value
                    for key, value in tags.items()
                )
            ),
            key=lambda sample: sample.observed_at_ns,
        )

    return (
        latest("foundation_modbus"),
        latest("foundation_modbus_bytes"),
        int(latest("internal_gather", input="modbus").fields["gather_errors"]),
    )


def wait_for(operation, label: str, seconds: int = 25):
    """停止や再開を決め打ちした待ち時間に依存せず、期限内に照合する。"""
    deadline = time.monotonic() + seconds
    while True:
        try:
            return operation()
        except (ValueError, KeyError, AssertionError) as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"{label}で期待値に到達しませんでした: {error}"
                ) from None
            time.sleep(0.3)


def wire_requests(mode: str = "normal") -> list[dict]:
    """合成装置が実際に受信した要求だけを列挙する。"""
    lines = compose(
        "logs", "--no-log-prefix", "simulator", mode=mode
    ).splitlines()
    return [
        json.loads(line)
        for line in lines
        if line.startswith('{"kind": "request"')
    ]


def assert_read_only_wire(mode: str = "normal") -> dict:
    """許可範囲を実際に読んだことと、書込要求が存在しないことを確認する。"""
    requests = wire_requests(mode)
    assert requests, "合成装置への要求がありません。"
    covered = set()
    ranges = set()
    for request in requests:
        assert request["unit"] == 7
        assert request["function"] == 3
        address, length = request["address"], request["count"]
        assert address >= 0 and length > 0 and address + length <= 6
        covered.update(range(address, address + length))
        ranges.add((address, length))
    assert covered == set(range(6))
    return {"unit": 7, "function_codes": [3], "request_ranges": sorted(ranges)}


def fresh(
    temperature: float, *, after_ns: int = 0
) -> tuple[Sample, Sample, int]:
    """工学値、順序変換、観測時刻、有効フラグをまとめて照合する。"""
    sample, byte_sample, errors = snapshot()
    status = sample_state(sample, time.time_ns())
    assert status["state"] == "fresh"
    assert sample.observed_at_ns > after_ns
    assert math.isclose(status["value"], temperature)
    assert sample.fields["temperature_valid"] == 1
    assert sample.fields["word_pattern"] == 0x12345678
    assert math.isclose(sample.fields["vibration_rms"], 23.5)
    assert byte_sample.fields["byte_pattern"] == 0x1234
    assert byte_sample.observed_at_ns == sample.observed_at_ns
    return sample, byte_sample, errors


def main() -> None:
    """通常、拒否、停止、欠測、正常復帰を順番に確認する。"""
    version = compose(
        "exec", "-T", "telegraf", "telegraf", "--version"
    ).strip()
    assert "1.40.3" in version
    baseline, byte_order, initial_errors = wait_for(
        lambda: fresh(-12.3), "正常時の計測"
    )
    assert baseline.fields["temperature_raw"] == -123
    baseline_status = sample_state(baseline, time.time_ns())
    assert baseline_status["state"] == "fresh"
    print(
        json.dumps(
            {
                "phase": "baseline",
                "telegraf": version,
                "fields": baseline.fields | byte_order.fields,
                **baseline_status,
                **assert_read_only_wire(),
            }
        )
    )

    print(
        compose(
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "python",
            "simulator",
            "/opt/fixture/probe.py",
        ).strip()
    )
    # 能動的な負試験を分離し、直前の読取監査を収集器だけの証拠とする。
    requests = wire_requests()
    assert {6, 16} <= {request["function"] for request in requests}

    compose("stop", "simulator")
    cutoff = time.time_ns()

    def stale() -> dict:
        sample, _, errors = snapshot()
        status = sample_state(sample, time.time_ns())
        assert status["state"] == "stale"
        assert status["value"] is None
        assert sample.observed_at_ns <= cutoff
        assert errors > initial_errors
        return status | {"gather_errors_delta": errors - initial_errors}

    print(
        json.dumps(
            {"phase": "disconnect", **wait_for(stale, "通信断の鮮度切れ")}
        )
    )
    compose("up", "-d", "--force-recreate", "simulator", mode="invalid")

    def invalid() -> dict:
        sample, _, _ = snapshot()
        status = sample_state(sample, time.time_ns())
        assert sample.observed_at_ns > cutoff
        assert sample.fields["temperature_raw"] == -32768
        assert sample.fields["temperature_valid"] == 0
        assert "temperature_celsius" not in sample.fields
        assert status["state"] == "invalid"
        return status | {"raw": sample.fields["temperature_raw"]}

    observed = wait_for(invalid, "欠測コードの除外")
    print(
        json.dumps(
            {
                "phase": "invalid",
                **observed,
                **assert_read_only_wire(mode="invalid"),
            }
        )
    )
    compose("up", "-d", "--force-recreate", "simulator", mode="recovered")
    restored, _, _ = wait_for(
        lambda: fresh(24.3, after_ns=observed["observed_at_ns"]),
        "再接続後の正常値",
    )
    assert restored.fields["temperature_raw"] == 243
    print(
        json.dumps(
            {
                "phase": "recovered",
                **sample_state(restored, time.time_ns()),
                **assert_read_only_wire(mode="recovered"),
            }
        )
    )


if __name__ == "__main__":
    main()

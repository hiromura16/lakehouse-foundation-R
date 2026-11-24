"""ネットワークなしのPTYで、Modbus RTUの読取りと復旧を実測する。"""

import json
import math
import os
import pty
import re
import select
import struct
import subprocess
import tempfile
import termios
import time
import tomllib
import tty
from collections import deque
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

BASE = Path(__file__).with_name("telegraf-reference.toml")
# tests/integration/modbus/simulator.pyの6点と同じ合成レジスタ。
VALUES = (0xFF85, 0x3412, 0x5678, 0x1234, 0x0000, 0x41BC)
TEMPERATURE = {"normal": 0xFF85, "invalid": 0x8000, "recovered": 0x00F3}
UNIT = 7
READ_HOLDING = 3
ILLEGAL_ADDRESS = 2
MAX_FRAME = 256
MAX_REQUESTS = 180
STALE_NS = 4_000_000_000
METRIC = "foundation_modbus"
BYTE_METRIC = "foundation_modbus_bytes"


def emit(phase, **fields):
    """試験の合否を、端末に依存しないJSONとして記録する。"""
    print(json.dumps({"phase": phase, **fields}, ensure_ascii=False))


def crc16(data):
    """Modbus RTUのCRC-16を、低位バイト先頭の送信形式へ使う。"""
    result = 0xFFFF
    for byte in data:
        result ^= byte
        for _ in range(8):
            result = (result >> 1) ^ (0xA001 if result & 1 else 0)
    return result


def with_crc(data):
    """合成ADUにCRCを付ける。"""
    return data + crc16(data).to_bytes(2, "little")


@dataclass(frozen=True)
class Request:
    """実際にPTYへ届いた要求の固定メタデータ。"""

    unit: int
    function: int
    address: int
    length_or_value: int


class Bus:
    """自分で開いたPTYだけを持ち、固定の番地を読み取り専用で提供する。"""

    def __init__(self):
        self.master, self.slave = pty.openpty()
        self.device = Path(os.ttyname(self.slave))
        if not re.fullmatch(r"/dev/pts/[0-9]+", str(self.device)):
            self.close()
            raise ValueError("FIXTURE_PTY_DEVICE_REQUIRED")
        tty.setraw(self.slave)
        self.buffer = bytearray()
        self.requests = []
        self.mode = "normal"
        self.serial = None
        self.bad_crc_requests = 0
        self.bad_crc_responses = 0
        self.ignored_units = 0

    def disconnect(self):
        """PTYのリンクを切る。番号の再利用を避けるためslaveは後で閉じる。"""
        if self.master is not None:
            os.close(self.master)
            self.master = None

    def close(self):
        """試験が開いたディスクリプターを解放する。"""
        self.disconnect()
        os.close(self.slave)

    def sample_serial(self):
        """カーネルが受理した速度と制御ビットを、実際の要求時に読み取る。"""
        chars = termios.tcgetattr(self.slave)
        flags = chars[2]
        self.serial = {
            "baud_19200": chars[4] == chars[5] == termios.B19200,
            "data_bits_8": (flags & termios.CSIZE) == termios.CS8,
            "parity_enabled": bool(flags & termios.PARENB),
            "two_stop_bits": bool(flags & termios.CSTOPB),
        }

    def service(self):
        """短い分割読み取りをつなぎ、既知の合成要求だけを解釈する。"""
        self.buffer.extend(os.read(self.master, MAX_FRAME + 1))
        if len(self.buffer) > MAX_FRAME:
            raise ValueError("FIXTURE_RTU_BUFFER_LIMIT")
        while len(self.buffer) >= 8:
            length = 8
            if self.buffer[1] == 16:
                # 試験で扱う複数点書込は2ワードの固定負例だけ。
                if self.buffer[6] != 4:
                    raise ValueError("FIXTURE_RTU_UNSUPPORTED_REQUEST_LENGTH")
                length = 13
            if len(self.buffer) < length:
                return
            frame = bytes(self.buffer[:length])
            del self.buffer[:length]
            self.respond(frame)

    def respond(self, frame):
        """局番・CRC・範囲を検証し、書込み系を例外で拒否する。"""
        if crc16(frame):
            self.bad_crc_requests += 1
            return
        request = Request(*struct.unpack(">BBHH", frame[:6]))
        if len(self.requests) >= MAX_REQUESTS:
            raise ValueError("FIXTURE_RTU_REQUEST_LIMIT")
        self.requests.append(request)
        if request.unit != UNIT:
            self.ignored_units += 1
            return
        self.sample_serial()
        if self.mode == "silent":
            return
        function, address, length = (
            request.function,
            request.address,
            request.length_or_value,
        )
        end = address + length
        if function != READ_HOLDING or not 0 <= address < end <= len(VALUES):
            # TCP試験にもある読み取り専用レジスタの拒否に合わせる。
            result = bytes((UNIT, function | 0x80, ILLEGAL_ADDRESS))
        else:
            registers = (
                TEMPERATURE.get(self.mode, TEMPERATURE["normal"]),
                *VALUES[1:],
            )
            result = bytes((UNIT, READ_HOLDING, length * 2)) + struct.pack(
                ">" + "H" * length, *registers[address:end]
            )
        reply = with_crc(result)
        if self.mode == "bad_crc":
            reply = reply[:-1] + bytes((reply[-1] ^ 1,))
            self.bad_crc_responses += 1
        assert os.write(self.master, reply) == len(reply)


def make_bus(resources):
    """生成した仮想装置の後片付けを、呼出元と共有する。"""
    bus = Bus()
    resources.callback(bus.close)
    return bus


def collector_config(link, *, parity, stops):
    """既存TCPの番地・換算は維持し、シリアルの通信設定だけを差し替える。"""
    base = BASE.read_text(encoding="utf-8")
    replacements = {
        'controller = "tcp://simulator:1502"': f'controller = "file://{link}"',
        'transmission_mode = "TCP"': (
            'transmission_mode = "RTU"\n'
            f'  baud_rate = 19200\n  data_bits = 8\n  parity = "{parity}"\n'
            f"  stop_bits = {stops}"
        ),
        "[[inputs.internal]]": (
            "[inputs.modbus.workarounds]\n"
            '  pause_between_requests = "5ms"\n'
            "  close_connection_after_gather = true\n\n"
            "[[inputs.internal]]"
        ),
    }
    for original, replacement in replacements.items():
        if base.count(original) != 1:
            raise ValueError("FIXTURE_TCP_REFERENCE_CHANGED")
        base = base.replace(original, replacement)
    config = tomllib.loads(base)
    (modbus,) = config["inputs"]["modbus"]
    assert modbus["controller"] == f"file://{link}"
    assert modbus["timeout"] == "300ms" and config["agent"]["interval"] == "1s"
    assert [metric["slave_id"] for metric in modbus["metric"]] == [UNIT] * 2
    return base


@dataclass(frozen=True)
class Sample:
    """既存の合成フィールドを、固定のLine Protocolから読み戻す。"""

    at_ns: int
    tags: dict
    fields: dict


class Collector:
    """一つのTelegrafプロセスとPTYを並べて、期限付きで監視する。"""

    def __init__(self, resources, directory, bus, *, parity, stops):
        self.bus = bus
        self.link = directory / "virtual-serial"
        self.link.symlink_to(bus.device)
        self.latest = {}
        self.errors = 0
        self.last_error = ""
        self.log = deque(maxlen=50)
        self.stream = bytearray()
        config = directory / "telegraf.toml"
        config.write_text(
            collector_config(self.link, parity=parity, stops=stops),
            encoding="utf-8",
        )
        self.process = subprocess.Popen(
            ("telegraf", "--config", config),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        resources.callback(self.close)

    def close(self):
        """Telegrafだけを終了し、停止の待ち時間に上限を設ける。"""
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.communicate(timeout=2)
        self.process.stdout.close()

    def capture(self):
        """パイプを短い塊で読み、行の長さと保持ログ数を制限する。"""
        self.stream.extend(os.read(self.process.stdout.fileno(), 4096))
        while b"\n" in self.stream:
            line, _, tail = self.stream.partition(b"\n")
            self.stream = bytearray(tail)
            self.record(line.decode("utf-8", errors="replace"))
        if len(self.stream) > 8192:
            raise ValueError("FIXTURE_COLLECTOR_LINE_LIMIT")

    def record(self, line):
        """計測と入力固有のエラー数だけを抜き出す。"""
        allowed = (METRIC + ",", BYTE_METRIC + ",", "internal_gather,")
        if not line.startswith(allowed):
            self.log.append(line[:400])
            if "E! [inputs.modbus::fixture_modbus]" in line:
                self.errors += 1
                self.last_error = line[:400]
            return
        series, metrics, stamp = line.split(" ")
        name, *tags = series.split(",")
        tags = dict(tag.split("=", 1) for tag in tags)
        fields = {}
        for item in metrics.split(","):
            key, value = item.split("=", 1)
            fields[key] = (
                int(value[:-1]) if value.endswith(("i", "u")) else float(value)
            )
        if name in {METRIC, BYTE_METRIC}:
            self.latest[name] = Sample(int(stamp), tags, fields)

    def until(self, condition, name, *, seconds=10):
        """一つの監視ループで通信とTelegrafの出力を進める。"""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            output = self.process.stdout.fileno()
            handles = [output]
            if self.bus.master is not None:
                handles.append(self.bus.master)
            ready = select.select(handles, [], [], 0.05)[0]
            if self.bus.master in ready:
                self.bus.service()
            if output in ready:
                self.capture()
            if self.process.poll() is not None:
                break
            matched = condition()
            if matched:
                return matched
        raise RuntimeError(f"{name}: " + " | ".join(list(self.log)[-5:]))

    def sample(self, raw, *, after_ns=0):
        """一回分の取得で、局番、タイムスタンプ、換算値を全部照合する。"""
        main = self.latest.get(METRIC)
        alternate = self.latest.get(BYTE_METRIC)
        if (
            not main
            or not alternate
            or main.at_ns <= after_ns
            or main.at_ns != alternate.at_ns
            or main.fields["temperature_raw"] != raw
        ):
            return None
        assert main.tags["slave_id"] == alternate.tags["slave_id"] == str(UNIT)
        assert main.fields["word_pattern"] == 0x12345678
        assert math.isclose(main.fields["vibration_rms"], 23.5)
        assert alternate.fields["byte_pattern"] == 0x1234
        if raw == -32768:
            assert main.fields["temperature_valid"] == 0
            assert "temperature_celsius" not in main.fields
        else:
            assert main.fields["temperature_valid"] == 1
            assert math.isclose(main.fields["temperature_celsius"], raw * 0.1)
        return Sample(main.at_ns, main.tags, main.fields | alternate.fields)

    def relink(self, bus):
        """同じ収集器の設定を保ち、私有シンボリックリンクだけを切り替える。"""
        next_link = self.link.with_name("virtual-next")
        next_link.symlink_to(bus.device)
        next_link.replace(self.link)
        self.bus = bus


def check_wire(bus):
    """収集器由来の全フレームが、単一局番と読取機能に収まるか確認する。"""
    assert bus.requests and not bus.bad_crc_requests
    covered = set()
    unique = set()
    for req in bus.requests:
        assert req.unit == UNIT and req.function == READ_HOLDING
        end = req.address + req.length_or_value
        assert 0 <= req.address < end <= 6
        covered.update(range(req.address, end))
        unique.add((req.address, req.length_or_value))
    assert covered == set(range(6))
    return {"requests": len(bus.requests), "ranges": sorted(unique)}


def probe(bus, packet, *, expected_bytes):
    """実機指定を持たない固定の負例を、自分のPTY内だけで往復させる。"""
    assert os.write(bus.slave, packet) == len(packet)
    ready = select.select([bus.master], [], [], 0.25)[0]
    if not ready:
        raise ValueError("FIXTURE_SYNTHETIC_REQUEST_TIMEOUT")
    bus.service()
    readable = select.select([bus.slave], [], [], 0.15)[0]
    if expected_bytes == 0:
        assert not readable
        return b""
    assert readable
    reply = os.read(bus.slave, MAX_FRAME)
    assert len(reply) == expected_bytes and crc16(reply) == 0
    return reply


def negative_probe():
    """局番違い・CRC違い・書込拒否を、収集器と異なるPTYで確認する。"""
    with ExitStack() as resources:
        bus = make_bus(resources)
        # 固定の6点読取り。任意機器や任意番地への送信を受け付けない。
        read = with_crc(bytes.fromhex("07 03 00 00 00 06"))
        baseline = probe(bus, read, expected_bytes=17)
        for operation, data in (
            (3, "07 03 00 06 00 01"),
            (6, "07 06 00 00 00 2A"),
            (16, "07 10 00 02 00 02 04 00 2A 00 2A"),
        ):
            reply = probe(bus, with_crc(bytes.fromhex(data)), expected_bytes=5)
            assert reply[:3] == bytes(
                (UNIT, operation | 0x80, ILLEGAL_ADDRESS)
            )
        probe(
            bus, with_crc(bytes.fromhex("08 03 00 00 00 06")), expected_bytes=0
        )
        probe(bus, read[:-1] + bytes((read[-1] ^ 1,)), expected_bytes=0)
        assert bus.ignored_units == bus.bad_crc_requests == 1
        assert probe(bus, read, expected_bytes=17) == baseline
        emit(
            "negative_probe",
            function_codes=[3, 6, 16],
            exception_code=2,
            wrong_unit_ignored=True,
            bad_crc_ignored=True,
            registers_unchanged=True,
        )


def parity_probe():
    """偶数パリティ要求と、PTYが保持できる情報を区別して測る。"""
    with ExitStack() as resources:
        bus = make_bus(resources)
        root = Path(resources.enter_context(tempfile.TemporaryDirectory()))
        collector = Collector(resources, root, bus, parity="E", stops=1)
        collector.until(lambda: collector.sample(-123), "FIXTURE_8E1_READ")
        assert bus.serial["baud_19200"] and bus.serial["data_bits_8"]
        assert (
            not bus.serial["parity_enabled"]
            and not bus.serial["two_stop_bits"]
        )
        emit("parity_request", configured="19200 8E1", observed=bus.serial)


def full_acceptance():
    """欠測、CRC不一致、デバイス取り替えを、同じ収集プロセスで検証する。"""
    with ExitStack() as resources:
        original = make_bus(resources)
        directory = Path(
            resources.enter_context(tempfile.TemporaryDirectory())
        )
        collector = Collector(
            resources, directory, original, parity="N", stops=2
        )
        pid = collector.process.pid
        baseline = collector.until(
            lambda: collector.sample(-123), "FIXTURE_BASELINE"
        )
        assert original.serial == {
            "baud_19200": True,
            "data_bits_8": True,
            "parity_enabled": False,
            "two_stop_bits": True,
        }
        emit(
            "baseline",
            configuration="19200 8N2",
            serial=original.serial,
            unit=UNIT,
            fields=baseline.fields,
        )

        original.mode = "bad_crc"
        bad_window = time.time_ns()
        before_errors = collector.errors
        collector.until(
            lambda: (
                collector.errors > before_errors and original.bad_crc_responses
            ),
            "FIXTURE_BAD_CRC",
        )
        assert collector.latest[METRIC].at_ns < bad_window
        assert "response crc" in collector.last_error
        emit(
            "bad_crc",
            dropped_responses=original.bad_crc_responses,
            collector_reason="response_crc_mismatch",
        )

        original.mode = "silent"
        start = time.time_ns()
        before_errors = collector.errors
        before_requests = len(original.requests)
        collector.until(
            lambda: (
                collector.errors > before_errors
                and len(original.requests) > before_requests
                and time.time_ns() - collector.latest[METRIC].at_ns > STALE_NS
            ),
            "FIXTURE_SILENT_TIMEOUT",
            seconds=9,
        )
        assert collector.latest[METRIC].at_ns < start
        assert "timeout" in collector.last_error.lower()
        emit(
            "timeout",
            state="stale",
            value=None,
            collector_error_logs_increased=True,
            new_value_published=False,
        )

        original.mode = "invalid"
        invalid = collector.until(
            lambda: collector.sample(-32768, after_ns=start),
            "FIXTURE_INVALID_CODE",
        )
        emit("invalid", state="invalid", fields=invalid.fields, value=None)

        collector.link.unlink()
        original.disconnect()
        before_errors = collector.errors
        collector.until(
            lambda: collector.errors > before_errors,
            "FIXTURE_DISCONNECTED_PORT",
        )
        assert collector.latest[METRIC].at_ns == invalid.at_ns
        assert "no such file" in collector.last_error.lower()
        replacement = make_bus(resources)
        replacement.mode = "recovered"
        assert original.device != replacement.device
        collector.relink(replacement)
        latest = collector.until(
            lambda: collector.sample(243, after_ns=invalid.at_ns),
            "FIXTURE_RECONNECT",
        )
        assert collector.process.pid == pid
        assert replacement.serial == original.serial
        emit(
            "reconnect",
            same_process=True,
            distinct_pty=True,
            disconnected_device_error=True,
            state="fresh",
            fields=latest.fields,
            old_wire=check_wire(original),
            replacement_wire=check_wire(replacement),
        )


if __name__ == "__main__":
    assert crc16(b"123456789") == 0x4B37
    version = subprocess.check_output(
        ("telegraf", "--version"), text=True, timeout=5
    )
    assert re.search(r"Telegraf 1\.40\.3\b", version)
    emit("version", telegraf="1.40.3")
    parity_probe()
    full_acceptance()
    negative_probe()

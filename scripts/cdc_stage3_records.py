"""Kafkaの状態recordを値非公開のpartition/offset枠と生バイトで照合する。"""

import json
from hashlib import sha256

FIELD = "\x1f"
RECORD = "\x1e"
LIMIT = 64


class RecordProofError(ValueError):
    """Kafkaの値やoffsetをエラー本文に含めない。"""


def read_arguments(topic, first, count):
    """Kafka 4.3.2のformatterでJSON内の改行とrecord境界を分離する。"""
    if (
        type(first) is not int
        or first < 0
        or type(count) is not int
        or not 1 <= count <= LIMIT
    ):
        raise RecordProofError("CDC_FRAMED_RANGE_INVALID")
    return [
        "--topic",
        topic,
        "--partition",
        "0",
        "--offset",
        str(first),
        "--max-messages",
        str(count),
        "--timeout-ms",
        "15000",
        "--command-property",
        "enable.auto.commit=false",
        "--command-property",
        "auto.offset.reset=none",
        "--formatter-property",
        "print.partition=true",
        "--formatter-property",
        "print.offset=true",
        "--formatter-property",
        "print.key=true",
        "--formatter-property",
        "print.value=true",
        "--formatter-property",
        "key.separator=" + FIELD,
        "--formatter-property",
        "line.separator=" + RECORD,
        "--formatter-property",
        "null.literal=null",
    ]


def reject_constant(_):
    """非標準JSON定数を許容しない。"""
    raise ValueError


def read_frame_json(raw):
    """許可したKafkaのUTF-8 JSONだけを認め、例外の値を隠す。"""
    try:
        return json.loads(raw.decode("utf-8"), parse_constant=reject_constant)
    except (RecursionError, ValueError):
        raise RecordProofError("CDC_FRAMED_VALUE_INVALID") from None


def framed_digest(raw, *, first, count, kind):
    """対象partitionの全offsetと各recordの存在を検証し、生バイトをhash化。"""
    if (
        type(raw) is not bytes
        or type(first) is not int
        or first < 0
        or type(count) is not int
        or not 1 <= count <= LIMIT
        or kind not in {"history", "offset"}
    ):
        raise RecordProofError("CDC_FRAMED_RANGE_INVALID")
    frames = raw.split(RECORD.encode())
    if len(frames) != count + 1 or frames[-1]:
        raise RecordProofError("CDC_FRAMED_RECORD_COUNT_MISMATCH")
    for expected, frame in enumerate(frames[:-1], first):
        fields = frame.split(FIELD.encode())
        if (
            len(fields) != 4
            or fields[0] != b"Partition:0"
            or fields[1] != b"Offset:" + str(expected).encode("ascii")
        ):
            raise RecordProofError("CDC_FRAMED_PARTITION_OFFSET_MISMATCH")
        key = read_frame_json(fields[2])
        value = read_frame_json(fields[3])
        if type(value) is not dict:
            raise RecordProofError("CDC_FRAMED_VALUE_INVALID")
        if kind == "history":
            if (
                key is not None
                or type(value.get("source")) is not dict
                or type(value.get("position")) is not dict
            ):
                raise RecordProofError("CDC_FRAMED_HISTORY_INVALID")
        elif (
            type(key) is not list
            or len(key) != 2
            or not isinstance(key[0], str)
            or not key[0]
            or type(key[1]) is not dict
        ):
            raise RecordProofError("CDC_FRAMED_OFFSET_KEY_INVALID")
    return sha256(raw).hexdigest()

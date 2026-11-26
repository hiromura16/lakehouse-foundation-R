"""段階3のKafka状態証跡を改行を含む合成recordで検査する。"""

import sys
import unittest
from hashlib import sha256
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from cdc_stage3_records import (  # noqa: E402
    FIELD,
    LIMIT,
    RECORD,
    RecordProofError,
    framed_digest,
    read_arguments,
)

HISTORY = (
    b'{\r\n "source": {"server": "fixture"},\n "position": {"id": 1}\r\n}'
)
OFFSET_KEY = b'["fixture",{"server":"fixture"}]'
OFFSET = b'{"snapshot":false,"id":2}'


def record(offset, *, partition=0, key=b"null", value=HISTORY):
    """Kafka formatterの公開仕様に従い合成frameを作る。"""
    return (
        FIELD.encode().join(
            (
                f"Partition:{partition}".encode(),
                f"Offset:{offset}".encode(),
                key,
                value,
            )
        )
        + RECORD.encode()
    )


class Stage3KafkaRecords(unittest.TestCase):
    """値を出さず、実offsetと生バイトを照合する。"""

    def test_read_only_exact_range(self):
        args = read_arguments("fixture", 7, 2)
        self.assertEqual(
            args[:8],
            [
                "--topic",
                "fixture",
                "--partition",
                "0",
                "--offset",
                "7",
                "--max-messages",
                "2",
            ],
        )
        for option in (
            "enable.auto.commit=false",
            "auto.offset.reset=none",
            "print.partition=true",
            "print.offset=true",
            "print.key=true",
            "print.value=true",
            "key.separator=\x1f",
            "line.separator=\x1e",
        ):
            self.assertIn(option, args)
        for first, count in ((-1, 1), (0, 0), (0, LIMIT + 1), (0, True)):
            with self.subTest(first=first, count=count):
                with self.assertRaisesRegex(
                    RecordProofError, "^CDC_FRAMED_RANGE_INVALID$"
                ):
                    read_arguments("fixture", first, count)

    def test_pretty_json_does_not_split_record_and_hashes_raw_bytes(self):
        raw = record(0) + record(1)
        self.assertNotEqual(len(raw.splitlines()), 2)
        digest = framed_digest(raw, first=0, count=2, kind="history")
        self.assertEqual(digest, sha256(raw).hexdigest())
        changed = raw.replace(b"\r\n", b"\n")
        self.assertNotEqual(
            framed_digest(changed, first=0, count=2, kind="history"), digest
        )

    def test_missing_extra_and_truncated_records_fail(self):
        samples = (
            record(0),
            record(0) + record(1) + record(2),
            record(0) + record(1)[:-1],
        )
        for raw in samples:
            with self.assertRaisesRegex(
                RecordProofError, "^CDC_FRAMED_RECORD_COUNT_MISMATCH$"
            ):
                framed_digest(raw, first=0, count=2, kind="history")

    def test_wrong_partition_and_missing_or_duplicate_physical_offset_fail(
        self,
    ):
        for second in (record(1, partition=1), record(2), record(0)):
            with self.assertRaisesRegex(
                RecordProofError, "^CDC_FRAMED_PARTITION_OFFSET_MISMATCH$"
            ):
                framed_digest(
                    record(0) + second, first=0, count=2, kind="history"
                )

    def test_malformed_and_tombstoned_history_fail_without_values(self):
        cases = (
            (record(0, value=b"not-json"), "CDC_FRAMED_VALUE_INVALID"),
            (record(0, value=b"null"), "CDC_FRAMED_VALUE_INVALID"),
            (record(0, value=b"{}"), "CDC_FRAMED_HISTORY_INVALID"),
            (record(0, key=b'"not-null"'), "CDC_FRAMED_HISTORY_INVALID"),
            (
                record(0, value=b'{ "x": "\x1f" }'),
                "CDC_FRAMED_PARTITION_OFFSET_MISMATCH",
            ),
        )
        for raw, canonical in cases:
            with self.assertRaisesRegex(RecordProofError, f"^{canonical}$"):
                framed_digest(raw, first=0, count=1, kind="history")

    def test_only_latest_exact_non_null_connect_record_is_evidence(self):
        raw = record(9, key=OFFSET_KEY, value=OFFSET)
        self.assertEqual(
            framed_digest(raw, first=9, count=1, kind="offset"),
            sha256(raw).hexdigest(),
        )
        for raw, canonical in (
            (
                record(8, key=OFFSET_KEY, value=OFFSET),
                "CDC_FRAMED_PARTITION_OFFSET_MISMATCH",
            ),
            (
                record(9, key=OFFSET_KEY, value=b"null"),
                "CDC_FRAMED_VALUE_INVALID",
            ),
            (record(9, value=OFFSET), "CDC_FRAMED_OFFSET_KEY_INVALID"),
        ):
            with self.assertRaisesRegex(RecordProofError, f"^{canonical}$"):
                framed_digest(raw, first=9, count=1, kind="offset")

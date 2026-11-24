"""工場暦と固定世代の照合で起きやすい境界条件を合成入力で固定する。"""

import json
import tempfile
import unittest
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from zoneinfo import ZoneInfo

from native.shift_report import (
    SelectionError,
    local_bound,
    read_calendar,
    reconcile,
)


class ShiftReportTest(unittest.TestCase):
    """現地のシフト日、UTC瞬間、報告区間の扱いを確認する。"""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "calendar.json"

    def calendar(self, entries, **settings):
        """合成暦だけを専用の一時パスに保存する。"""
        calendar = {
            "schema_version": 1,
            "factory_id": "F01",
            "timezone": "Asia/Tokyo",
            "shifts": [
                {"shift_id": name, "start": start, "end": end}
                for name, start, end in entries
            ],
            **settings,
        }
        self.path.write_text(json.dumps(calendar), encoding="utf-8")
        return calendar

    def read(self, start="2026-01-05", until="2026-01-07"):
        """日本の合成期間と固定工場で暦を選択する。"""
        return read_calendar(self.path, "F01", start, until)

    @staticmethod
    def event(instant, good, reject, order="A"):
        """Silverの型に相当する合成生産実績を作る。"""
        return {
            "work_order_id": order,
            "event_time": datetime.fromisoformat(instant),
            "good_qty": good,
            "reject_qty": reject,
        }

    @staticmethod
    def stop(start, end, order="A"):
        """Silverの型に相当する合成停止報告を作る。"""
        return {
            "work_order_id": order,
            "start_time": datetime.fromisoformat(start),
            "end_time": datetime.fromisoformat(end),
        }

    def test_overnight_crossing_and_duplicate_time(self):
        self.calendar(
            [
                ("night", "2026-01-05T22:00", "2026-01-06T06:00"),
                ("day", "2026-01-06T06:00", "2026-01-06T14:00"),
            ]
        )
        shifts, zone, fingerprint = self.read()
        self.assertEqual(zone, "Asia/Tokyo")
        self.assertEqual(
            fingerprint, sha256(self.path.read_bytes()).hexdigest()
        )
        events = [
            self.event("2026-01-05T21:00:00+00:00", 0, 1),
            self.event("2026-01-05T21:00:00+00:00", 42, 0, order="OTHER"),
        ]
        stops = [
            self.stop(
                "2026-01-05T20:40:00+00:00", "2026-01-05T21:20:00+00:00"
            ),
            self.stop(
                "2026-01-05T20:50:00+00:00", "2026-01-05T21:10:00+00:00"
            ),
            self.stop(
                "2026-01-05T20:00:00+00:00",
                "2026-01-05T22:00:00+00:00",
                order="OTHER",
            ),
        ]
        result, gap = reconcile(shifts, {"A"}, events, stops)
        night, day = result
        self.assertEqual(night["shift_date"], "2026-01-05")
        self.assertEqual(night["start_utc"], "2026-01-05T13:00:00+00:00")
        self.assertEqual(
            (night["observed_event_count"], night["good_qty"]), (0, None)
        )
        self.assertEqual(day["shift_date"], "2026-01-06")
        self.assertEqual(
            (day["observed_event_count"], day["good_qty"]), (1, 0)
        )
        self.assertEqual(day["reject_qty"], 1)
        for row in result:
            self.assertEqual(row["observed_stop_count"], 2)
            self.assertEqual(row["reported_stop_microseconds"], 1_800_000_000)
            self.assertEqual(
                row["any_reported_stop_microseconds"], 1_200_000_000
            )
        self.assertEqual(gap["reported_stop_microseconds"], 0)

    def test_gap_empty_and_explicit_zero(self):
        self.calendar(
            [
                ("first", "2026-01-05T00:00", "2026-01-05T02:00"),
                ("last", "2026-01-05T03:00", "2026-01-05T04:00"),
            ],
            timezone="UTC",
        )
        shifts, _, _ = self.read()
        events = [self.event("2026-01-05T02:00:00Z", 10, 0)]
        stops = [
            self.stop("2026-01-05T02:00:00Z", "2026-01-05T02:20:00Z"),
            self.stop("2026-01-05T03:00:00Z", "2026-01-05T03:00:00Z"),
        ]
        (first, last), gap = reconcile(shifts, {"A"}, events, stops)
        self.assertEqual(first["observed_stop_count"], 0)
        self.assertIsNone(first["any_reported_stop_microseconds"])
        self.assertEqual(last["observed_stop_count"], 1)
        self.assertEqual(last["any_reported_stop_microseconds"], 0)
        self.assertEqual(last["reported_stop_microseconds"], 0)
        self.assertEqual(gap["event_count"], 1)
        self.assertEqual(gap["reported_stop_microseconds"], 1_200_000_000)

    def test_shift_start_date_controls_selection(self):
        self.calendar(
            [
                ("previous", "2026-01-05T22:00", "2026-01-06T06:00"),
                ("current", "2026-01-06T22:00", "2026-01-07T06:00"),
            ]
        )
        shifts, _, _ = self.read("2026-01-06", "2026-01-07")
        (shift,) = shifts
        self.assertEqual(shift.name, "current")
        self.assertEqual(shift.end.isoformat(), "2026-01-06T21:00:00+00:00")

    def test_dst_elapsed_hours_and_invalid_civil_time(self):
        zone = ZoneInfo("America/New_York")
        for day, hours in (("2026-03-08", 7), ("2026-11-01", 9)):
            with self.subTest(day=day):
                self.calendar(
                    [("night", day + "T00:00", day + "T08:00")],
                    timezone=zone.key,
                )
                end = "2026-03-09" if day.endswith("03-08") else "2026-11-02"
                (shift,), _, _ = self.read(day, end)
                self.assertEqual(
                    (shift.end - shift.start).total_seconds(), hours * 3600
                )
        with self.assertRaisesRegex(SelectionError, "存在しない"):
            local_bound("2026-03-08T02:30", None, zone)
        with self.assertRaisesRegex(SelectionError, "重複"):
            local_bound("2026-11-01T01:30", None, zone)
        _, first = local_bound("2026-11-01T01:30", 0, zone)
        _, later = local_bound("2026-11-01T01:30", 1, zone)
        self.assertEqual(first.isoformat(), "2026-11-01T05:30:00+00:00")
        self.assertEqual(later.isoformat(), "2026-11-01T06:30:00+00:00")

    def test_two_folds_can_bound_same_civil_time(self):
        self.calendar(
            [],
            timezone="America/New_York",
            shifts=[
                {
                    "shift_id": "clock-change",
                    "start": "2026-11-01T01:30",
                    "start_fold": 0,
                    "end": "2026-11-01T01:30",
                    "end_fold": 1,
                }
            ],
        )
        shifts, _, _ = self.read("2026-11-01", "2026-11-02")
        events = [
            self.event("2026-11-01T05:30:00Z", 2, 0),
            self.event("2026-11-01T06:29:59.999999Z", 3, 0),
        ]
        stops = [
            self.stop(
                "2026-11-01T05:29:59.999999Z", "2026-11-01T05:30:00.000001Z"
            ),
        ]
        (row,), gap = reconcile(shifts, {"A"}, events, stops)
        self.assertEqual(row["observed_event_count"], 2)
        self.assertEqual(row["good_qty"], 5)
        self.assertEqual(row["any_reported_stop_microseconds"], 1)
        self.assertEqual(gap["reported_stop_microseconds"], 0)

    def test_calendar_rejects_overlaps_ambiguous_schema_and_limits(self):
        self.calendar(
            [
                ("first", "2026-01-05T00:00", "2026-01-05T04:00"),
                ("last", "2026-01-05T03:00", "2026-01-05T04:00"),
            ]
        )
        with self.assertRaisesRegex(SelectionError, "重なって"):
            self.read()
        with self.assertRaisesRegex(SelectionError, "1〜31"):
            self.read(until="2026-03-01")
        with self.assertRaisesRegex(SelectionError, "YYYY-MM-DD"):
            self.read(start="20260105")
        calendar = self.calendar(
            [("day", "2026-01-05T06:00", "2026-01-05T14:00")],
            schema_version=True,
        )
        with self.assertRaisesRegex(SelectionError, "工場ID"):
            self.read()
        calendar["schema_version"] = 1
        text = json.dumps(calendar).replace(
            '"factory_id":', '"factory_id":"X","factory_id":'
        )
        self.path.write_text(text, encoding="utf-8")
        with self.assertRaisesRegex(SelectionError, "重複キー"):
            self.read()
        calendar["timezone"] = "localtime"
        self.path.write_text(json.dumps(calendar), encoding="utf-8")
        with self.assertRaisesRegex(SelectionError, "IANA"):
            self.read()

    def test_union_is_capped_to_shift_but_sum_is_not(self):
        self.calendar(
            [("day", "2026-01-05T06:00", "2026-01-05T07:00")],
            timezone="UTC",
        )
        shifts, _, _ = self.read()
        stops = [
            self.stop("2026-01-05T05:55:00Z", "2026-01-05T07:05:00Z"),
            self.stop("2026-01-05T05:58:00Z", "2026-01-05T07:02:00Z"),
            self.stop("2026-01-05T05:30:00Z", "2026-01-05T06:00:00Z"),
            self.stop("2026-01-05T07:00:00Z", "2026-01-05T07:05:00Z"),
        ]
        (row,), _ = reconcile(shifts, {"A"}, [], stops)
        self.assertEqual(row["observed_stop_count"], 2)
        self.assertEqual(
            row["any_reported_stop_microseconds"], row["duration_microseconds"]
        )
        self.assertEqual(
            row["reported_stop_microseconds"], row["duration_microseconds"] * 2
        )

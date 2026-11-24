"""公開世代のSilverを工場のシフト暦と照合し、根拠付きJSONを標準出力へ返す。"""

import argparse
import json
import re
import sys
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from native.lineage import CHANNELS, SelectionError, select_release

UTC = timezone.utc
MICROSECOND = timedelta(microseconds=1)
MAX_CALENDAR_BYTES = 1024 * 1024
MAX_DAYS = 31
MAX_SHIFTS = 128
DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
LOCAL = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}")
TABLES = ("erp_work_orders", "mes_events", "mes_downtime")


@dataclass(frozen=True)
class Shift:
    """現地の開始日と、DST解決済みのUTC半開区間を持つ。"""

    name: str
    day: date
    start: datetime
    end: datetime


def unique_object(pairs):
    """二重指定で意図せず上書きされる暦のキーを拒否する。"""
    result = dict(pairs)
    if len(result) != len(pairs):
        raise SelectionError("シフト暦のJSONに重複キーがあります。")
    return result


def local_bound(value, fold, zone):
    """現地時刻を検証し、その時刻に対応する一意なUTC境界を返す。"""
    if not isinstance(value, str) or not LOCAL.fullmatch(value):
        raise SelectionError(
            "境界はオフセットなしの YYYY-MM-DDTHH:MM 形式です。"
        )
    if fold is not None and (type(fold) is not int or fold not in (0, 1)):
        raise SelectionError("foldは整数の0または1です。")
    try:
        local = datetime.fromisoformat(value)
        candidates = {}
        for option in (0, 1):
            instant = local.replace(tzinfo=zone, fold=option).astimezone(UTC)
            # UTCからの往復で春の欠けた時刻を除く。同一瞬間は一つにまとめる。
            if instant.astimezone(zone).replace(tzinfo=None) == local:
                candidates.setdefault(instant, option)
    except (ValueError, OverflowError):
        raise SelectionError("境界の日付または時刻が無効です。") from None
    if not candidates:
        raise SelectionError(
            "存在しない現地時刻です。シフト暦を修正してください。"
        )
    if len(candidates) == 2 and fold is None:
        raise SelectionError("重複する現地境界にはfoldを指定してください。")
    if fold is not None and fold not in candidates.values():
        raise SelectionError("この現地境界のfold指定は無効です。")
    return local.date(), next(
        instant
        for instant, option in candidates.items()
        if fold is None or option == fold
    )


def parse_shift(entry, zone):
    """1件の暦定義を検証し、比較可能な区間へ変換する。"""
    required = {"shift_id", "start", "end"}
    allowed = required | {"start_fold", "end_fold"}
    if not isinstance(entry, dict) or not required <= entry.keys() <= allowed:
        raise SelectionError(
            "シフトにはshift_id、start、endを指定してください。"
        )
    name = entry["shift_id"]
    if not isinstance(name, str) or not name.strip() or len(name) > 64:
        raise SelectionError("shift_idは1〜64文字の名前です。")
    for field in ("start_fold", "end_fold"):
        if field in entry and entry[field] is None:
            raise SelectionError("foldを指定する場合は整数の0または1です。")
    day, start = local_bound(entry["start"], entry.get("start_fold"), zone)
    _, end = local_bound(entry["end"], entry.get("end_fold"), zone)
    if start >= end or end - start > timedelta(days=2):
        raise SelectionError(
            "シフトには正の長さ（実時間で2日以内）が必要です。"
        )
    return Shift(name, day, start, end)


def read_calendar(path, factory, start, until):
    """会社の暦を読み、選択した期間の区間と暦のSHA-256を返す。"""
    if not DATE.fullmatch(start) or not DATE.fullmatch(until):
        raise SelectionError("照合日は YYYY-MM-DD 形式です。")
    try:
        first, last = date.fromisoformat(start), date.fromisoformat(until)
    except ValueError:
        raise SelectionError("照合日が無効です。") from None
    if not 0 < (last - first).days <= MAX_DAYS:
        raise SelectionError(f"照合期間は1〜{MAX_DAYS}日です。")
    if path == "-":
        raw = sys.stdin.buffer.read(MAX_CALENDAR_BYTES + 1)
    else:
        try:
            with Path(path).open("rb") as source:
                raw = source.read(MAX_CALENDAR_BYTES + 1)
        except OSError:
            raise SelectionError("シフト暦ファイルを読めません。") from None
    if len(raw) > MAX_CALENDAR_BYTES:
        raise SelectionError("シフト暦は1 MiB以内です。")
    try:
        calendar = json.loads(
            raw.decode("utf-8"), object_pairs_hook=unique_object
        )
    except SelectionError:
        raise
    except (ValueError, RecursionError):
        raise SelectionError(
            "シフト暦はUTF-8のJSONで指定してください。"
        ) from None
    if (
        not isinstance(calendar, dict)
        or calendar.keys()
        != {"schema_version", "factory_id", "timezone", "shifts"}
        or type(calendar["schema_version"]) is not int
        or calendar["schema_version"] != 1
        or not isinstance(calendar["shifts"], list)
        or not factory.strip()
        or calendar["factory_id"] != factory
    ):
        raise SelectionError("シフト暦の版、工場ID、形式を確認してください。")
    zone_name = calendar["timezone"]
    if not isinstance(zone_name, str):
        raise SelectionError("シフト暦にはIANAタイムゾーン名が必要です。")
    # ホストに依存する別名や、うるう秒などの特別な派生区分を除く。
    if zone_name in {"localtime", "posixrules"} or zone_name.startswith(
        ("posix/", "right/")
    ):
        raise SelectionError("シフト暦にはIANAタイムゾーン名が必要です。")
    try:
        zone = ZoneInfo(zone_name)
    except (ValueError, ZoneInfoNotFoundError):
        raise SelectionError(
            "シフト暦には有効なIANAタイムゾーンが必要です。"
        ) from None
    windows = sorted(
        (parse_shift(entry, zone) for entry in calendar["shifts"]),
        key=lambda shift: shift.start,
    )
    if len({(shift.day, shift.name) for shift in windows}) != len(windows):
        raise SelectionError("同じシフト日に重複するshift_idがあります。")
    if any(
        left.end > right.start for left, right in zip(windows, windows[1:])
    ):
        raise SelectionError("シフト暦の境界が重なっています。")
    selected = [shift for shift in windows if first <= shift.day < last]
    if not 0 < len(selected) <= MAX_SHIFTS:
        raise SelectionError(f"照合対象のシフト数は1〜{MAX_SHIFTS}件です。")
    return selected, zone.key, sha256(raw).hexdigest()


def window_index(shifts, starts, instant):
    """瞬間を受け持つシフトの添字。暦の空白では-1を返す。"""
    index = bisect_right(starts, instant) - 1
    if index >= 0 and instant < shifts[index].end:
        return index
    return -1


def stop_durations(shifts, stops):
    """境界を順に通過し、停止報告の延べ時間と和集合の時間を積算する。"""
    edges = sorted(
        (instant, change)
        for start, end in stops
        for instant, change in ((start, 1), (end, -1))
    )
    # 同じ瞬間に始まる報告と終わる報告の間は0秒なので、順序に依存しない。
    index = active = reported = covered = 0
    cursor = shifts[0].start
    totals = {}
    bounds = sorted({t for shift in shifts for t in (shift.start, shift.end)})
    for boundary in bounds:
        while index < len(edges) and edges[index][0] <= boundary:
            instant, change = edges[index]
            span = (instant - cursor) // MICROSECOND
            reported += active * span
            covered += span if active else 0
            cursor = instant
            active += change
            index += 1
        span = (boundary - cursor) // MICROSECOND
        totals[boundary] = (
            reported + active * span,
            covered + (span if active else 0),
        )
    return totals


def reconcile(shifts, order_ids, events, downtimes):
    """指図から工場を照合し、実績と停止を独立して集計する。"""
    starts = [shift.start for shift in shifts]
    event_count = Counter()
    good = Counter()
    reject = Counter()
    for row in events:
        if row["work_order_id"] in order_ids:
            index = window_index(shifts, starts, row["event_time"])
            event_count[index] += 1
            good[index] += row["good_qty"]
            reject[index] += row["reject_qty"]

    zero_length = Counter()
    stops = []
    for row in downtimes:
        if row["work_order_id"] not in order_ids:
            continue
        start, end = row["start_time"], row["end_time"]
        if start == end:
            zero_length[window_index(shifts, starts, start)] += 1
        else:
            # 両端を選択期間の包絡に切り詰め、跨いでいる停止も計上する。
            start, end = max(start, shifts[0].start), min(end, shifts[-1].end)
            if start < end:
                stops.append((start, end))
    stop_starts = sorted(start for start, _ in stops)
    stop_ends = sorted(end for _, end in stops)
    durations = stop_durations(shifts, stops)
    rows = []
    for index, shift in enumerate(shifts):
        count = (
            bisect_left(stop_starts, shift.end)
            - bisect_right(stop_ends, shift.start)
            + zero_length[index]
        )
        start_reported, start_covered = durations[shift.start]
        end_reported, end_covered = durations[shift.end]
        rows.append(
            {
                "shift_id": shift.name,
                "shift_date": shift.day.isoformat(),
                "start_utc": shift.start.isoformat(),
                "end_utc": shift.end.isoformat(),
                "duration_microseconds": (shift.end - shift.start)
                // MICROSECOND,
                "observed_event_count": event_count[index],
                "good_qty": good[index] if event_count[index] else None,
                "reject_qty": reject[index] if event_count[index] else None,
                "observed_stop_count": count,
                "reported_stop_microseconds": (
                    end_reported - start_reported if count else None
                ),
                "any_reported_stop_microseconds": (
                    end_covered - start_covered if count else None
                ),
            }
        )
    # 暦の空白にあった実績と停止時間は、隣のシフトへ付け替えず別に示す。
    unscheduled = {
        "event_count": event_count[-1],
        "zero_length_stop_count": zero_length[-1],
        "reported_stop_microseconds": durations[shifts[-1].end][0]
        - sum(row["reported_stop_microseconds"] or 0 for row in rows),
    }
    return rows, unscheduled


def released_rows(table, snapshot, row_filter):
    """Arrowをバッチごとに読み、既存の行数・バイト上限を適用する。"""
    from native import engine

    with engine.snapshot_reader(table, snapshot, row_filter) as reader:
        for batch in engine.ScanProgress().batches(reader):
            for offset in range(0, batch.num_rows, 4096):
                yield from batch.slice(offset, 4096).to_pylist()


def run(core, args):
    """公開マニフェストとSilverの3表を固定してから照合する。"""
    from pyiceberg.expressions import (
        And,
        EqualTo,
        GreaterThanOrEqual,
        LessThan,
    )

    shifts, zone, calendar_hash = read_calendar(
        args.calendar, args.factory, args.start, args.until
    )
    catalog, release = select_release(core, args.channel, args.release)
    namespace = core.schema(CHANNELS[args.channel], "silver")
    outputs = release.get("outputs", {})
    references = {}
    tables = {}
    for name in TABLES:
        reference = f"silver.{name}"
        identifier = f"{namespace}.{name}"
        if reference not in outputs or not catalog.table_exists(identifier):
            raise SelectionError(
                "公開世代に必要なSilver表の参照がありません。"
            )
        table, snapshot = catalog.load_table(identifier), outputs[reference]
        if snapshot is not None and (
            type(snapshot) is not int or table.snapshot_by_id(snapshot) is None
        ):
            raise SelectionError(
                "公開世代のSilverスナップショットを読めません。"
            )
        references[identifier] = (
            str(snapshot) if snapshot is not None else None
        )
        tables[name] = table, snapshot

    first, last = shifts[0].start, shifts[-1].end
    order_ids = {
        row["work_order_id"]
        for row in released_rows(
            *tables["erp_work_orders"], EqualTo("factory_id", args.factory)
        )
    }
    events = released_rows(
        *tables["mes_events"],
        And(
            GreaterThanOrEqual("event_time", first),
            LessThan("event_time", last),
        ),
    )
    # 先頭境界の0秒記録を残す。境界の手前で終わる正の区間は集計時に除く。
    stops = released_rows(
        *tables["mes_downtime"],
        And(
            GreaterThanOrEqual("end_time", first), LessThan("start_time", last)
        ),
    )
    # 固定世代に対象工場の指図がなければ、全工場の明細は読み始めない。
    rows, unscheduled = reconcile(
        shifts,
        order_ids,
        events if order_ids else (),
        stops if order_ids else (),
    )
    return {
        "schema_version": 1,
        "factory_id": args.factory,
        "timezone": zone,
        "channel": args.channel,
        "release_snapshot_id": release["release_snapshot_id"],
        "silver_snapshot_ids": references,
        "calendar_sha256": calendar_hash,
        "shift_dates": {"from": args.start, "until": args.until},
        "coverage": "unknown",
        "shifts": rows,
        "unscheduled_in_window": unscheduled,
    }


def main():
    """照合結果だけを標準出力に書き、失敗時は集計を返さない。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factory", required=True, help="工場ID")
    parser.add_argument(
        "--calendar", required=True, help="工場暦JSONのパス。-は標準入力"
    )
    parser.add_argument(
        "--from", dest="start", required=True, help="最初のシフト日"
    )
    parser.add_argument("--until", required=True, help="最後のシフト日の翌日")
    parser.add_argument("--channel", choices=CHANNELS, default="files")
    parser.add_argument("--release", help="公開世代ID。省略で実行開始時の最新")
    args = parser.parse_args()
    from native.api import core_module, report_error

    try:
        result = run(core_module(), args)
    except SelectionError as error:
        parser.error(str(error))
    except Exception as error:
        report_error(error, file=sys.stderr)
        raise SystemExit(1) from None
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()

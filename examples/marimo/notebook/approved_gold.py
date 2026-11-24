"""承認済みの日次生産Goldを、閉じたDuckDB分析用データに変換する。"""

import argparse
import hashlib
import hmac
import json
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

TABLE = "gold.factory_daily_production"
MAX_BYTES = 256 * 1024
MAX_ROWS = 200
COLUMNS = (
    "factory_id",
    "production_date",
    "planned_qty",
    "good_qty",
    "reject_qty",
    "total_qty",
    "downtime_minutes",
    "yield_pct",
    "plan_adherence_pct",
)
PROFILES = {"foundation_gold_api", "committed_synthetic_oracle"}


def unique_object(pairs):
    """重複キーのあるJSONは上書き順によって解釈が変わるため拒否する。"""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("JSONに重複キーがあります。")
        result[key] = value
    return result


def reject_json_constant(_value):
    """標準JSON外のNaNやInfinityをスナップショットに混入させない。"""
    raise ValueError("JSONに有限でない数値があります。")


def file_bytes(path, limit):
    """サイズ上限を超えたファイルをメモリーに展開しない。"""
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if not 1 <= len(data) <= limit:
        raise ValueError("承認ファイルが空か、サイズ上限を超えています。")
    return data


def parse_json(payload):
    """数量の精度を保ち、曖昧なJSONを拒否して読み込む。"""
    return json.loads(
        payload,
        object_pairs_hook=unique_object,
        parse_float=Decimal,
        parse_constant=reject_json_constant,
    )


def check_id(value):
    """Iceberg識別子は浮動小数点へ変換せず、64ビット以内の文字列で保持する。"""
    return (
        isinstance(value, str)
        and re.fullmatch(r"[1-9][0-9]{0,18}", value) is not None
        and int(value) <= 2**63 - 1
    )


def quantity(value):
    """Python整数かAPIのDECIMAL文字列だけを整数数量として扱う。"""
    if type(value) is int or (
        isinstance(value, str) and re.fullmatch(r"[0-9]{1,38}", value)
    ):
        quantity_value = int(value)
        if 0 <= quantity_value < 10**38:
            return Decimal(quantity_value)
    raise ValueError("数量はDECIMAL(38,0)で表せる非負整数にしてください。")


def metric(value, nullable=False):
    """有限なJSON数値を解析し、値が未定義の比率のみnullを許可する。"""
    if value is None and nullable:
        return None
    if not isinstance(value, (int, Decimal)) or isinstance(value, bool):
        raise ValueError("時間と比率にはJSON数値を使います。")
    converted = float(value)
    if not 0 <= converted < float("inf"):
        raise ValueError("時間と比率は有限の非負数にしてください。")
    return converted


def pack_row(row):
    """行の形と業務日付を検証し、DuckDBに渡す順序と型を固定する。"""
    if not isinstance(row, dict) or row.keys() != set(COLUMNS):
        raise ValueError("Goldの日次生産スキーマが一致しません。")
    factory = row["factory_id"]
    if not isinstance(factory, str) or not re.fullmatch(
        r"[^\s\x00-\x1f]{1,128}", factory
    ):
        raise ValueError("工場IDが不正です。")
    stamp = row["production_date"]
    if not isinstance(stamp, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}", stamp
    ):
        raise ValueError("生産日はYYYY-MM-DDで指定してください。")
    return (
        factory,
        date.fromisoformat(stamp),
        *(quantity(row[key]) for key in COLUMNS[2:6]),
        metric(row["downtime_minutes"]),
        metric(row["yield_pct"], nullable=True),
        metric(row["plan_adherence_pct"], nullable=True),
    )


def load_approved(directory):
    """承認記録の指紋と完全一致した日次Goldだけを受け入れる。"""
    approval = parse_json(file_bytes(directory / "approval.json", 4096))
    if (
        not isinstance(approval, dict)
        or approval.keys() != {"format_version", "source", "sha256"}
        or type(approval["format_version"]) is not int
        or approval["format_version"] != 1
        or not isinstance(approval["source"], str)
        or approval["source"] not in PROFILES
        or not isinstance(approval["sha256"], str)
        or re.fullmatch(r"[0-9a-f]{64}", approval["sha256"]) is None
    ):
        raise ValueError("承認記録が形式バージョン1に一致しません。")
    content = file_bytes(directory / "gold.json", MAX_BYTES)
    if not hmac.compare_digest(
        hashlib.sha256(content).hexdigest(), approval["sha256"]
    ):
        raise ValueError("GoldのSHA-256が承認記録と一致しません。")
    gold = parse_json(content)
    if (
        not isinstance(gold, dict)
        or gold.get("table") != TABLE
        or gold.get("truncated") is not False
        or type(gold.get("total_rows")) is not int
        or not isinstance(gold.get("rows"), list)
        or not 0 < gold["total_rows"] == len(gold["rows"]) <= MAX_ROWS
    ):
        raise ValueError("対象の日次Goldを省略せずに出力してください。")
    if approval["source"] == "foundation_gold_api":
        if not all(
            check_id(gold.get(key))
            for key in ("release_snapshot_id", "snapshot_id")
        ):
            raise ValueError("APIの公開世代と表スナップショットが必要です。")
    elif (
        gold.get("release_snapshot_id") is not None
        or gold.get("snapshot_id") is not None
    ):
        raise ValueError("合成期待値は実際のIceberg世代を名乗れません。")
    rows = [pack_row(row) for row in gold["rows"]]
    provenance = {
        "source": approval["source"],
        "sha256": approval["sha256"],
        "release_snapshot_id": gold.get("release_snapshot_id"),
        "snapshot_id": gold.get("snapshot_id"),
    }
    return provenance, rows


def open_snapshot_db(directory):
    """外部アクセスを無効にしたセッション内DBへ、パラメーターで行を登録する。"""
    import duckdb

    provenance, rows = load_approved(directory)
    connection = duckdb.connect(
        ":memory:",
        config={"enable_external_access": "false", "threads": "1"},
    )
    try:
        connection.execute(
            Path(__file__).with_name("daily_production.sql").read_text()
        )
        connection.executemany(
            "INSERT INTO production VALUES (?,?,?,?,?,?,?,?,?)", rows
        )
    except Exception:
        connection.close()
        raise
    return provenance, connection


def main():
    """ホストや隔離コンテナから受入検証できる件数・来歴だけを返す。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("approved_dir", type=Path)
    args = parser.parse_args()
    provenance, database = open_snapshot_db(args.approved_dir)
    with database:
        count, factories = database.execute(
            "SELECT count(*), count(DISTINCT factory_id) FROM production"
        ).fetchone()
    print(json.dumps({**provenance, "rows": count, "factories": factories}))


if __name__ == "__main__":
    main()

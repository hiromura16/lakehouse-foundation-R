"""契約で決めたCSV・JSONLの列、文字コード、件数を検証する。"""

import csv
import json
from io import StringIO
from itertools import islice

MAX_FILE_ROWS = 100_000


class LandingContractError(ValueError):
    """ファイルのヘッダー、行の形、上限が契約に合わない場合のエラー。"""


def source_rows(suffix, payload, columns):
    """0行のCSVでもヘッダーを照合し、列の不足や重複を検出する。"""
    expected = set(columns)
    text = payload.decode("utf-8-sig")
    if suffix == ".csv":
        reader = csv.DictReader(StringIO(text), strict=True)
        fields = reader.fieldnames or []
        if len(fields) != len(columns) or set(fields) != expected:
            raise LandingContractError("SOURCE_CSV_HEADER")
        rows = list(islice(reader, MAX_FILE_ROWS + 1))
        if any(None in row or None in row.values() for row in rows):
            raise LandingContractError("SOURCE_CSV_ROW_WIDTH")
    else:
        lines = (line for line in text.splitlines() if line.strip())
        rows = [json.loads(line) for line in islice(lines, MAX_FILE_ROWS + 1)]
    if len(rows) > MAX_FILE_ROWS or any(
        not isinstance(row, dict) or set(row) != expected for row in rows
    ):
        raise LandingContractError("SOURCE_COLUMNS_OR_ROW_LIMIT")
    return rows

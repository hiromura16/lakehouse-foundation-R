"""承認済みGoldをRillの読み取り専用DBとして保存する。"""

import os
import tempfile
from pathlib import Path

import duckdb

# 同じ承認チェックを使い、Rillの実行コンテナに準備コードを渡さない。
from approved_gold import load_approved


def main():
    """DDLを通過したDBだけを新規ファイルとして配置する。"""
    provenance, rows = load_approved(Path("/approved"))
    output = Path("/output")
    with tempfile.TemporaryDirectory(dir=output) as workspace:
        staging = Path(workspace) / "gold.duckdb"
        connection = duckdb.connect(str(staging))
        with connection:
            connection.execute("SET enable_external_access=false")
            connection.execute(
                Path("/adapter/daily_production.sql").read_text()
            )
            connection.executemany(
                "INSERT INTO production VALUES (?,?,?,?,?,?,?,?,?)", rows
            )
            connection.execute(
                """
                CREATE TABLE provenance (
                    source VARCHAR NOT NULL,
                    sha256 VARCHAR NOT NULL,
                    release_snapshot_id VARCHAR,
                    snapshot_id VARCHAR
                )
                """
            )
            connection.execute(
                "INSERT INTO provenance VALUES (?,?,?,?)",
                list(provenance.values()),
            )
            connection.execute("CHECKPOINT")
        staging.chmod(0o444)
        os.link(staging, output / "gold.duckdb")


if __name__ == "__main__":
    main()

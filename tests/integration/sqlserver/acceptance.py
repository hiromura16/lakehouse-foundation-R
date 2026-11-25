"""合成SQL Serverで六表の読取、接続時TLS、完了印を照合する。"""

import csv
import json
import subprocess
import sys
import time
from contextlib import contextmanager
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from secrets import token_hex
from unittest.mock import patch

import pymssql
from writer_acceptance import verify_fixed_writer

ROOT = Path(__file__).resolve().parents[3]
EXTRACT = ROOT / "scripts/extract-sqlserver.py"
SPEC = spec_from_file_location("sqlserver_extractor", EXTRACT)
SOURCE = module_from_spec(SPEC)
SPEC.loader.exec_module(SOURCE)
CONNECT = pymssql.connect
DATABASE = "FixtureWarehouse"
READER = "fixture_reader"
TYPES = {
    "updated_at": "datetime2(6)",
    "planned_date": "date",
    "planned_qty": "int",
    "unit_price": "decimal(12,2)",
}
VALUES = {
    "product_name": "組立部品",
    "updated_at": "2026-01-05T09:01:02.345678",
    "planned_date": "2026-01-05",
    "planned_qty": 17,
    "unit_price": "12.30",
    "reason_code": None,
}


@contextmanager
def connect(settings, **changes):
    """抽出CLIと同じCA設定で、用途に応じた合成セッションを開く。"""
    settings = settings | changes
    with SOURCE.tls_configuration(settings):
        connection = CONNECT(
            server="lh_source_target",
            port=settings["SOURCE_SQLSERVER_PORT"],
            database=settings["SOURCE_SQLSERVER_DATABASE"],
            user=settings["SOURCE_SQLSERVER_USER"],
            password=settings["SOURCE_SQLSERVER_PASSWORD"],
            encryption="require",
            autocommit=True,
            login_timeout=5,
            timeout=10,
            charset="UTF-8",
        )
        try:
            yield connection.cursor()
        finally:
            connection.close()


def prepare(admin, password, contracts):
    """使い捨てDBに型付きの1行ずつと、SELECT専用のログインを作る。"""
    deadline = time.monotonic() + 90
    while True:
        try:
            with connect(admin, SOURCE_SQLSERVER_DATABASE="master") as cursor:
                cursor.execute("SELECT 1")
                assert cursor.fetchone() == (1,)
            break
        except pymssql.Error:
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "合成SQL Serverの起動を確認できません。"
                ) from None
            time.sleep(2)

    with connect(admin, SOURCE_SQLSERVER_DATABASE="master") as cursor:
        cursor.execute(f"CREATE DATABASE [{DATABASE}]")
        cursor.execute(
            f"ALTER DATABASE [{DATABASE}] SET ALLOW_SNAPSHOT_ISOLATION ON"
        )
        # パスワードはこの試験で生成した英数字と固定記号だけを使う。
        cursor.execute(f"CREATE LOGIN [{READER}] WITH PASSWORD = '{password}'")
    with connect(admin) as cursor:
        cursor.execute(f"CREATE USER [{READER}] FOR LOGIN [{READER}]")
        for source in contracts:
            relation = "dbo." + SOURCE.quoted(source["table"])
            columns = source["columns"]
            cursor.execute(
                f"CREATE TABLE {relation} ("
                + ", ".join(
                    SOURCE.quoted(column)
                    + " "
                    + TYPES.get(column, "nvarchar(80)")
                    for column in columns
                )
                + ")"
            )
            cursor.execute(
                f"INSERT INTO {relation} VALUES ("
                + ", ".join(["%s"] * len(columns))
                + ")",
                tuple(VALUES.get(column, "fixture") for column in columns),
            )
            cursor.execute(f"GRANT SELECT ON {relation} TO [{READER}]")


class ChangeDuringSecondSelect:
    """二番目のSELECTの前に別セッションを確定し、参照の時点を検査する。"""

    def __init__(self, cursor, admin):
        self.cursor = cursor
        self.admin = admin
        self.selects = 0

    def execute(self, statement):
        """全表取得の途中で、先頭二表を1回の書込で変更する。"""
        if statement.startswith("SELECT TOP ("):
            self.selects += 1
            if self.selects == 2:
                with connect(self.admin) as writer:
                    writer.execute("SET LOCK_TIMEOUT 5000")
                    writer.execute("BEGIN TRANSACTION")
                    writer.execute(
                        "UPDATE dbo.erp_products SET product_name = %s",
                        ("組立部品・訂正",),
                    )
                    writer.execute(
                        "UPDATE dbo.erp_work_orders SET planned_qty = %s",
                        (29,),
                    )
                    writer.execute("COMMIT TRANSACTION")
        self.cursor.execute(statement)

    def fetchmany(self, size):
        """実ドライバーの型付きの取得結果をそのまま返す。"""
        return self.cursor.fetchmany(size)


class ObservedConnection:
    """実接続に試験イベントだけを差し込む。"""

    def __init__(self, connection, admin):
        self.connection = connection
        self.admin = admin

    def cursor(self):
        """二番目のSELECTに同期点を設ける。"""
        return ChangeDuringSecondSelect(self.connection.cursor(), self.admin)

    def close(self):
        """実接続の終了処理を使う。"""
        self.connection.close()


def assert_rejection(args, failure):
    """CLIの失敗と、完了印が一件も出力されていないことを照合する。"""
    check = subprocess.run(
        [sys.executable, str(EXTRACT), *args],
        check=False,
        capture_output=True,
        timeout=35,
    )
    assert check.returncode == 1, failure
    assert b"Source extraction failed" in check.stderr, failure
    assert not Path(args[args.index("--output") + 1]).exists(), failure


def verify(stage, port):
    """TLS・権限・同時更新・型・出力のハッシュをそれぞれ照合する。"""
    password = "Aa9!" + token_hex(24)
    admin = {
        "SOURCE_SQLSERVER_HOST": "localhost",
        "SOURCE_SQLSERVER_PORT": port,
        "SOURCE_SQLSERVER_DATABASE": DATABASE,
        "SOURCE_SQLSERVER_USER": "sa",
        "SOURCE_SQLSERVER_PASSWORD": (stage / "fixture.env")
        .read_text()
        .splitlines()[0]
        .split("=", 1)[1],
        "SOURCE_SQLSERVER_CA_FILE": str(stage / "ca.pem"),
        "SOURCE_SQLSERVER_TIMEZONE": "Asia/Tokyo",
    }
    reader = admin | {
        "SOURCE_SQLSERVER_USER": READER,
        "SOURCE_SQLSERVER_PASSWORD": password,
    }
    contracts = SOURCE.source_io().contracts()
    prepare(admin, password, contracts)

    map_file = stage / "tables.json"
    table_map = json.loads(
        (ROOT / "infra/source/sqlserver-tables.example.json").read_text()
    )
    for table in table_map["tables"]:
        table["source_schema"] = "dbo"
    map_file.write_text(json.dumps(table_map), encoding="utf-8")
    map_file.chmod(0o600)
    mapping = SOURCE.read_mapping(map_file, contracts)
    with connect(reader) as cursor:
        try:
            cursor.execute(
                "UPDATE dbo.erp_products SET product_name = N'拒否'"
            )
        except pymssql.Error as error:
            assert error.args[0] == 229, "書込権限に由来するエラーが必要です。"
        else:
            raise AssertionError(
                "ソース読取用のログインでUPDATEが成功しました。"
            )

    def observed_connect(**kwargs):
        """抽出の実接続だけを監視し、別接続で変更できる時点を作る。"""
        return ObservedConnection(CONNECT(**kwargs), admin)

    with patch.object(pymssql, "connect", observed_connect):
        before = SOURCE.extract_tables(
            reader, mapping, contracts, 10, snapshot=True
        )
    assert before["erp_products"][0]["product_name"] == "組立部品"
    assert before["erp_work_orders"][0]["planned_qty"] == "17"
    assert before["erp_sales_orders"][0]["unit_price"] == "12.30"
    assert before["erp_sales_orders"][0]["updated_at"] == (
        "2026-01-05T00:01:02.345678Z"
    )

    def launch_args(label, **changes):
        """接続設定を非公開に保持し、各試行で未使用の出力先を渡す。"""
        file = stage / (label + ".env")
        file.write_text(
            "".join(
                f"{key}={value}\n" for key, value in (reader | changes).items()
            ),
            encoding="utf-8",
        )
        file.chmod(0o600)
        return [
            "--env-file",
            str(file),
            "--table-map",
            str(map_file),
            "--output",
            str(stage / label),
            "--snapshot",
        ]

    subprocess.run(
        [sys.executable, str(EXTRACT), *launch_args("verified")],
        check=True,
        timeout=35,
    )
    landing = stage / "verified"
    manifest = json.loads((landing / "source-extract.json").read_text())
    assert manifest["read_consistency"] == "snapshot"
    assert len(manifest["sources"]) == len(contracts) == 6
    for item in manifest["sources"]:
        path = landing / item["path"]
        digest = sha256(path.read_bytes()).hexdigest()
        assert item["rows"] == 1
        assert item["sha256"] == digest
        assert (
            path.with_name(path.name + ".ready").read_text() == digest + "\n"
        )
    with (landing / "csv/erp_products.csv").open(
        encoding="utf-8", newline=""
    ) as stream:
        assert next(csv.DictReader(stream))["product_name"] == "組立部品・訂正"
    with (landing / "csv/erp_work_orders.csv").open(
        encoding="utf-8", newline=""
    ) as stream:
        assert next(csv.DictReader(stream))["planned_qty"] == "29"

    assert_rejection(
        launch_args(
            "wrong-ca", SOURCE_SQLSERVER_CA_FILE=str(stage / "wrong-ca.pem")
        ),
        "誤ったCAで抽出が成功しました。",
    )
    assert_rejection(
        launch_args("wrong-name", SOURCE_SQLSERVER_HOST="127.0.0.1"),
        "証明書に含めていないIPで抽出が成功しました。",
    )
    with connect(admin, SOURCE_SQLSERVER_DATABASE="master") as cursor:
        cursor.execute(
            f"ALTER DATABASE [{DATABASE}] SET ALLOW_SNAPSHOT_ISOLATION OFF"
        )
    assert_rejection(launch_args("disabled"), "同時点読取が無効でした。")
    verify_fixed_writer(connect, admin)
    print(
        "六表の同一時点・並行更新、型、完了印、TLSと権限の拒否を照合しました。"
    )


if __name__ == "__main__":
    verify(Path(sys.argv[1]), sys.argv[2])

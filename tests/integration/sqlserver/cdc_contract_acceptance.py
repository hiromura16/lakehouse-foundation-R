"""一時CAで検証した隔離SQL Serverで専用CDC契約を照合する。"""

import importlib.util
import re
import sys
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from secrets import token_hex
from subprocess import run
from time import monotonic, sleep
from unittest.mock import patch

import pymssql
from acceptance import SOURCE

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "foundation_r_cdc_contract", ROOT / "scripts/cdc_sql_contract.py"
)
CONTRACT = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CONTRACT
SPEC.loader.exec_module(CONTRACT)

DATABASE = "FixtureCdcContract"
COLLISION_DB = "FixtureCdcCollision"
READER = "fixture_cdc_reader"
WRITER = "fixture_cdc_writer"
SIBLING = "fixture_sibling_v1"
SIBLING_ROLE = "fixture_sibling_reader"
EMPTY_LSN = bytes(10)


@contextmanager
def connect(settings, *, transactional=False):
    """読み取り側の既存一時CA設定で、合成ループバックだけに接続する。"""
    with SOURCE.tls_configuration(settings):
        session = pymssql.connect(
            server="lh_source_target",
            port=settings["SOURCE_SQLSERVER_PORT"],
            database=settings["SOURCE_SQLSERVER_DATABASE"],
            user=settings["SOURCE_SQLSERVER_USER"],
            password=settings["SOURCE_SQLSERVER_PASSWORD"],
            login_timeout=4,
            timeout=12,
            encryption="require",
            autocommit=not transactional,
            charset="UTF-8",
        )
        try:
            yield session
        finally:
            session.close()


def execute(settings, statement, arguments=()):
    """接続を使い回さず、実際の確定済み結果だけを受け取る。"""
    with connect(settings) as session:
        cursor = session.cursor()
        cursor.execute(statement, arguments)
        return cursor.fetchall() if cursor.description else []


def database_settings(settings, name):
    """隔離サーバー内で、呼出し先の合成DBだけを切り替える。"""
    return settings | {"SOURCE_SQLSERVER_DATABASE": name}


def configure(settings, name, database_guid, installation, *, apply):
    """本体と同じトランザクション可能な接続で検証する。"""
    with connect(settings, transactional=True) as session:
        return CONTRACT.configure(
            session,
            name,
            installation,
            expected_database_guid=database_guid,
            apply=apply,
        )


def require_contract_denial(settings, name, guid, installation, reason):
    """安全前提を満たさない場合、成功扱いを許さない。"""
    try:
        configure(settings, name, guid, installation, apply=True)
    except CONTRACT.ContractError as error:
        assert str(error) == reason
        return
    raise AssertionError("Expected contract denial")


def require_sql_denial(settings, statement, numbers, arguments=()):
    """SQL Serverが特定の権限不足として拒否したことを確認する。"""
    try:
        execute(settings, statement, arguments)
    except pymssql.Error as error:
        assert error.args[0] in numbers
        return
    raise AssertionError("Expected SQL permission denial")


def database_guid(settings):
    """現在接続している専用DBのService Broker識別子を受け取る。"""
    ((value,),) = execute(
        settings,
        "SELECT CONVERT(varchar(36),service_broker_guid) FROM sys.databases "
        "WHERE database_id=DB_ID()",
    )
    assert re.fullmatch(r"[0-9A-Fa-f-]{36}", value)
    return value


def database_baseline(settings):
    """変化してはいけない既存DBオプションを値出力せず照合する。"""
    rows = execute(
        settings,
        "SELECT is_cdc_enabled, recovery_model, compatibility_level, "
        "snapshot_isolation_state, is_read_committed_snapshot_on, "
        "is_broker_enabled FROM sys.databases WHERE database_id=DB_ID()",
    )
    assert len(rows) == 1 and rows[0][0] == 1
    return rows[0]


def verify_readonly_cli(stage, settings, guid, installation, expected):
    """運用入口も合成認証・既定の読取モードで検査する。"""
    values = settings | {
        "CDC_EXPECTED_DATABASE": settings["SOURCE_SQLSERVER_DATABASE"],
        "CDC_EXPECTED_BROKER_GUID": guid,
        "CDC_INSTALLATION_ID": installation,
    }
    private = stage / "cdc-contract-cli.env"
    private.write_text(
        "".join(key + "=" + value + "\n" for key, value in values.items()),
        encoding="utf-8",
    )
    private.chmod(0o600)
    try:
        result = run(
            [
                sys.executable,
                str(ROOT / "scripts/cdc_sql_contract.py"),
                "--settings",
                str(private),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0 and result.stdout.strip() == expected
    finally:
        private.unlink()


def jobs(settings):
    """共有CDCジョブの識別子と実効設定を値出力なしで保全する。"""
    rows = execute(settings, "EXEC sys.sp_cdc_help_jobs")
    assert len(rows) == 2
    by_kind = {row[1]: row for row in rows}
    assert set(by_kind) == {"capture", "cleanup"}
    assert all(row[0] for row in rows)
    return by_kind


def seed_sibling(settings):
    """DBA事前準備を隔離系だけで再現し、既存captureの番兵を残す。"""
    execute(settings, "CREATE ROLE [fixture_sibling_reader] AUTHORIZATION dbo")
    execute(
        settings,
        "CREATE TABLE dbo.fixture_sibling "
        "(id int NOT NULL PRIMARY KEY, note nvarchar(40) NOT NULL)",
    )
    execute(
        settings,
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo',"
        "@source_name=N'fixture_sibling',@capture_instance=%s,"
        "@role_name=%s,@supports_net_changes=0",
        (SIBLING, SIBLING_ROLE),
    )
    execute(settings, "INSERT dbo.fixture_sibling VALUES (1,N'既存番兵')")


def wait_for_live_sibling(settings):
    """共有captureの実在だけでなく、初期化済みの開始位置を待つ。"""
    deadline = monotonic() + 90
    while monotonic() < deadline:
        rows = execute(
            settings,
            "SELECT x.start_lsn FROM cdc.change_tables x JOIN sys.tables t "
            "ON t.object_id=x.source_object_id WHERE x.capture_instance=%s "
            "AND t.is_tracked_by_cdc=1 AND x.has_drop_pending=0",
            (SIBLING,),
        )
        if len(rows) == 1 and rows[0][0] not in (None, EMPTY_LSN):
            return
        sleep(1)
    raise AssertionError("Incumbent capture was not initialized")


def sibling_state(settings):
    """Installerの前後で、別系統のcapture定義と表の行を比較する。"""
    row = execute(
        settings,
        "SELECT capture_instance,source_object_id,object_id,role_name,"
        "supports_net_changes,create_date FROM cdc.change_tables "
        "WHERE capture_instance=%s",
        (SIBLING,),
    )
    assert len(row) == 1
    assert execute(settings, "SELECT id,note FROM dbo.fixture_sibling") == [
        (1, "既存番兵")
    ]
    return row


def ensure_absent(settings):
    """拒否と読取確認の直後に、専用オブジェクトができていないと示す。"""
    assert execute(
        settings,
        "SELECT SCHEMA_ID(%s),DATABASE_PRINCIPAL_ID(%s),"
        "DATABASE_PRINCIPAL_ID(%s)",
        (CONTRACT.SCHEMA, CONTRACT.READER_ROLE, CONTRACT.WRITER_ROLE),
    ) == [(None, None, None)]


def put_accounts(master, admin):
    """固定合成ログインを作り、明示した専用ロールだけに所属させる。"""
    users = {}
    for login, role in (
        (READER, CONTRACT.READER_ROLE),
        (WRITER, CONTRACT.WRITER_ROLE),
    ):
        password = "Aa9!" + token_hex(24)
        execute(master, f"CREATE LOGIN [{login}] WITH PASSWORD = '{password}'")
        execute(admin, f"CREATE USER [{login}] FOR LOGIN [{login}]")
        execute(admin, f"ALTER ROLE [{role}] ADD MEMBER [{login}]")
        users[login] = admin | {
            "SOURCE_SQLSERVER_USER": login,
            "SOURCE_SQLSERVER_PASSWORD": password,
        }
    return users[READER], users[WRITER]


def catalog_state(settings):
    """6表のCDC実体を独立照合し、再適用で置換されないことを記録する。"""
    rows = execute(
        settings,
        "SELECT t.name,t.object_id,c.capture_instance,c.object_id,c.role_name,"
        "c.supports_net_changes,c.create_date FROM sys.tables t "
        "JOIN sys.schemas s ON s.schema_id=t.schema_id "
        "LEFT JOIN cdc.change_tables c ON c.source_object_id=t.object_id "
        "WHERE s.name=%s AND t.is_tracked_by_cdc=1 ORDER BY t.name",
        (CONTRACT.SCHEMA,),
    )
    assert [row[0] for row in rows] == sorted(CONTRACT.TABLES)
    for name, _, capture, capture_id, role, supports_net, _ in rows:
        assert capture == CONTRACT.capture_name(name)
        assert capture_id and role == CONTRACT.READER_ROLE and not supports_net
        columns = execute(
            settings,
            "SELECT column_name FROM cdc.captured_columns "
            "WHERE object_id=%s ORDER BY column_ordinal",
            (capture_id,),
        )
        assert columns == [(column.name,) for column in CONTRACT.TABLES[name]]
    return rows


def typed_fixture(column, *, revised=False):
    """捕捉される列型ごとに、再現可能な合成値を生成する。"""
    sql_type = column.sql_type.lower()
    if sql_type.startswith(("nvarchar", "varchar", "char", "nchar")):
        return "検証更新" if revised else "検証投入"
    if sql_type.startswith("datetime2"):
        return datetime(2026, 1, 2, 3, 4, 5, 123456)
    if sql_type == "date":
        return date(2026, 1, 2)
    if sql_type.startswith(("decimal", "numeric")):
        return Decimal("-1.25")
    if sql_type == "bit":
        return bool(revised)
    if sql_type in {"int", "bigint", "smallint", "tinyint"}:
        return 2 if revised else 1
    raise AssertionError("Unexpected fixture column type")


def lsn_range(settings, capture):
    """有効な範囲が準備できるまで、LSNをログに出さず待つ。"""
    ((minimum, maximum),) = execute(
        settings,
        "SELECT sys.fn_cdc_get_min_lsn(%s),sys.fn_cdc_get_max_lsn()",
        (capture,),
    )
    return minimum, maximum


def await_rows(settings, capture, statement, predicate):
    """正しい内容が現れるまでだけ待機し、115秒後に閉じて失敗する。"""
    deadline = monotonic() + 115
    while True:
        minimum, maximum = lsn_range(settings, capture)
        if minimum and minimum != EMPTY_LSN and maximum and minimum <= maximum:
            rows = execute(settings, statement, (minimum, maximum))
            if predicate(rows):
                return minimum, maximum
        if monotonic() >= deadline:
            raise AssertionError(
                "Expected committed CDC rows were not visible"
            )
        sleep(1)


def prove_stream(admin, reader, writer):
    """専用writerの実DML、限定readerの関数、別表への拒否を照合する。"""
    table = "products"
    columns = CONTRACT.TABLES[table]
    key, changed = columns[:2]
    target = f"[{CONTRACT.SCHEMA}].[{table}]"
    initial = tuple(typed_fixture(column) for column in columns)
    revised = typed_fixture(changed, revised=True)
    assert revised != initial[1]
    execute(
        writer,
        f"INSERT {target} ("
        + ",".join(f"[{column.name}]" for column in columns)
        + ") VALUES ("
        + ",".join("%s" for _ in columns)
        + ")",
        initial,
    )
    actual = execute(
        reader,
        f"SELECT [{key.name}],[{changed.name}] FROM {target}",
    )
    assert actual == [initial[:2]]
    execute(
        writer,
        f"UPDATE {target} SET [{changed.name}]=%s WHERE [{key.name}]=%s",
        (revised, initial[0]),
    )
    execute(writer, f"DELETE {target} WHERE [{key.name}]=%s", (initial[0],))
    assert execute(reader, f"SELECT COUNT_BIG(*) FROM {target}") == [(0,)]
    capture = CONTRACT.capture_name(table)
    statement = (
        f"SELECT [__$operation],[{key.name}],[{changed.name}] "
        f"FROM [cdc].[fn_cdc_get_all_changes_{capture}]"
        "(%s,%s,N'all update old') "
        "ORDER BY [__$start_lsn],[__$seqval],[__$operation]"
    )
    approved_range = await_rows(
        reader,
        capture,
        statement,
        lambda rows: (
            rows
            == [
                (2, initial[0], initial[1]),
                (3, initial[0], initial[1]),
                (4, initial[0], revised),
                (1, initial[0], revised),
            ]
        ),
    )
    sibling_statement = (
        f"SELECT id,note FROM [cdc].[fn_cdc_get_all_changes_{SIBLING}]"
        "(%s,%s,N'all')"
    )
    sibling_range = await_rows(
        admin,
        SIBLING,
        sibling_statement,
        lambda rows: rows == [(1, "既存番兵")],
    )
    for principal in (reader, writer):
        require_sql_denial(
            principal, "SELECT * FROM dbo.fixture_sibling", {229}
        )
        require_sql_denial(
            principal,
            "UPDATE dbo.fixture_sibling SET note=N'変更しない'",
            {229},
        )
        require_sql_denial(principal, sibling_statement, {229}, sibling_range)
        require_sql_denial(
            principal, "CREATE TABLE dbo.fixture_forbidden (id int)", {262}
        )
        require_sql_denial(
            principal,
            "EXEC sys.sp_cdc_enable_table @source_schema=%s,"
            "@source_name=%s,@capture_instance=N'fixture_forbidden_v1',"
            "@role_name=%s,@supports_net_changes=0",
            {229, 22904},
            (CONTRACT.SCHEMA, table, CONTRACT.READER_ROLE),
        )
    assert execute(
        admin,
        "SELECT COUNT_BIG(*) FROM cdc.change_tables "
        "WHERE capture_instance=N'fixture_forbidden_v1'",
    ) == [(0,)]
    require_sql_denial(reader, f"DELETE FROM {target}", {229})
    require_sql_denial(writer, statement, {229}, approved_range)


def verify(stage, port):
    """所有する合成DB内だけで、前提拒否・適用・非干渉を照合する。"""
    master = {
        "SOURCE_SQLSERVER_HOST": "localhost",
        "SOURCE_SQLSERVER_PORT": port,
        "SOURCE_SQLSERVER_DATABASE": "master",
        "SOURCE_SQLSERVER_USER": "sa",
        "SOURCE_SQLSERVER_PASSWORD": (stage / "fixture.env")
        .read_text()
        .splitlines()[0]
        .split("=", 1)[1],
        "SOURCE_SQLSERVER_CA_FILE": str(stage / "ca.pem"),
    }
    admin = database_settings(master, DATABASE)
    collision = database_settings(master, COLLISION_DB)
    installation = token_hex(16)
    phase = "CDC_FIXTURE_TLS_START"
    try:
        deadline = monotonic() + 90
        while True:
            try:
                assert execute(master, "SELECT 1") == [(1,)]
                break
            except pymssql.Error:
                if monotonic() >= deadline:
                    raise AssertionError
                sleep(2)

        phase = "CDC_FIXTURE_OWN_DATABASE"
        execute(master, f"CREATE DATABASE [{DATABASE}]")
        guid = database_guid(admin)
        phase = "CDC_DB_DISABLED_DENIED"
        require_contract_denial(
            admin, DATABASE, guid, installation, "DATABASE_PREREQUISITES"
        )
        ensure_absent(admin)

        phase = "CDC_MISSING_JOBS_DENIED"
        execute(admin, "EXEC sys.sp_cdc_enable_db")
        require_contract_denial(
            admin,
            DATABASE,
            guid,
            installation,
            "PREEXISTING_CDC_JOBS_REQUIRED",
        )
        ensure_absent(admin)

        phase = "CDC_SHARED_ANCHOR_PREPARED"
        seed_sibling(admin)
        wait_for_live_sibling(admin)
        initial_jobs = jobs(admin)
        initial_sibling = sibling_state(admin)
        initial_options = database_baseline(admin)

        phase = "CDC_WRONG_DATABASE_AND_GUID_DENIED"
        require_contract_denial(
            admin, COLLISION_DB, guid, installation, "DATABASE_MISMATCH"
        )
        wrong_guid = ("a" if guid[0].lower() != "a" else "b") + guid[1:]
        require_contract_denial(
            admin, DATABASE, wrong_guid, installation, "DATABASE_MISMATCH"
        )
        ensure_absent(admin)
        assert jobs(admin) == initial_jobs

        phase = "CDC_PLAN_READ_ONLY"
        verify_readonly_cli(stage, admin, guid, installation, "ABSENT")
        assert configure(admin, DATABASE, guid, installation, apply=False) == (
            "ABSENT"
        )
        ensure_absent(admin)

        phase = "CDC_POST_DDL_ROLLBACK"
        inspect = CONTRACT.inspect
        observed = []

        def fail_after_actual_ddl(cursor, installation_id):
            """本物のDDLを確認してから、テスト内だけ後置条件を失敗させる。"""
            observed.append(inspect(cursor, installation_id))
            if observed[-1] == "VERIFIED":
                raise CONTRACT.ContractError(
                    "FIXTURE_FORCED_POST_DDL_ROLLBACK"
                )
            return observed[-1]

        with patch.object(CONTRACT, "inspect", fail_after_actual_ddl):
            require_contract_denial(
                admin,
                DATABASE,
                guid,
                installation,
                "FIXTURE_FORCED_POST_DDL_ROLLBACK",
            )
        assert observed == ["ABSENT", "VERIFIED"]
        ensure_absent(admin)
        assert execute(
            admin,
            "SELECT COUNT_BIG(*) FROM cdc.change_tables WHERE "
            "capture_instance LIKE N'foundation[_]r[_]demo[_]%'",
        ) == [(0,)]
        assert jobs(admin) == initial_jobs
        assert sibling_state(admin) == initial_sibling
        assert database_baseline(admin) == initial_options

        phase = "CDC_INITIAL_APPLY"
        assert configure(admin, DATABASE, guid, installation, apply=True) == (
            "CREATED"
        )
        expected = catalog_state(admin)
        assert database_baseline(admin) == initial_options
        assert jobs(admin) == initial_jobs
        assert sibling_state(admin) == initial_sibling

        phase = "CDC_WRONG_INSTALLATION_DENIED"
        wrong_install = ("a" if installation[0] != "a" else "b") + (
            installation[1:]
        )
        require_contract_denial(
            admin, DATABASE, guid, wrong_install, "INSTALLATION_MISMATCH"
        )
        assert catalog_state(admin) == expected

        phase = "CDC_IDEMPOTENT_NO_WRITE"
        verify_readonly_cli(stage, admin, guid, installation, "VERIFIED")
        for apply in (False, True):
            assert (
                configure(admin, DATABASE, guid, installation, apply=apply)
                == "VERIFIED"
            )
            assert catalog_state(admin) == expected
            assert jobs(admin) == initial_jobs
            assert sibling_state(admin) == initial_sibling

        phase = "CDC_LIMITED_READER_WRITER_AND_SIBLING"
        reader, writer = put_accounts(master, admin)
        prove_stream(admin, reader, writer)
        assert jobs(admin) == initial_jobs
        assert sibling_state(admin) == initial_sibling

        phase = "CDC_EXISTING_DEFINITION_DRIFT_DENIED"
        execute(
            admin,
            f"ALTER TABLE [{CONTRACT.SCHEMA}].[products] "
            "ADD [fixture_drift] int NULL",
        )
        require_contract_denial(
            admin, DATABASE, guid, installation, "TABLE_SHAPE_MISMATCH"
        )
        assert execute(
            admin,
            "SELECT COL_LENGTH(%s,N'fixture_drift')",
            (f"{CONTRACT.SCHEMA}.products",),
        ) == [(4,)]
        assert catalog_state(admin) == expected
        assert jobs(admin) == initial_jobs
        assert sibling_state(admin) == initial_sibling
        assert database_baseline(admin) == initial_options

        phase = "CDC_UNOWNED_COLLISION_PRESERVED"
        execute(master, f"CREATE DATABASE [{COLLISION_DB}]")
        execute(collision, "EXEC sys.sp_cdc_enable_db")
        seed_sibling(collision)
        wait_for_live_sibling(collision)
        collision_jobs = jobs(collision)
        collision_sibling = sibling_state(collision)
        execute(
            collision, f"CREATE SCHEMA [{CONTRACT.SCHEMA}] AUTHORIZATION dbo"
        )
        execute(
            collision,
            f"CREATE TABLE [{CONTRACT.SCHEMA}].[collision] (id int NOT NULL)",
        )
        execute(
            collision, f"INSERT [{CONTRACT.SCHEMA}].[collision] VALUES(86)"
        )
        require_contract_denial(
            collision,
            COLLISION_DB,
            database_guid(collision),
            installation,
            "NAMESPACE_COLLISION",
        )
        assert execute(
            collision,
            "SELECT t.name FROM sys.tables t JOIN sys.schemas s "
            "ON s.schema_id=t.schema_id WHERE s.name=%s",
            (CONTRACT.SCHEMA,),
        ) == [("collision",)]
        assert execute(
            collision, f"SELECT id FROM [{CONTRACT.SCHEMA}].[collision]"
        ) == [(86,)]
        assert jobs(collision) == collision_jobs
        assert sibling_state(collision) == collision_sibling
    except Exception:
        raise AssertionError(
            f"SQL CDC contract fixture failed: {phase}"
        ) from None
    print("SQL CDC contract and limited principals verified.")


if __name__ == "__main__":
    verify(Path(sys.argv[1]), sys.argv[2])

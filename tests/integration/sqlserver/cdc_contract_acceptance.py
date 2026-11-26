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
from cdc_stage3_evidence import approved_capture_fixture

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
EXPECTED_MAJOR = 16
# 本体が返す静的な理由だけを出す。SQLの本文・名前・値を通さない。
PUBLIC_CONTRACT_ERRORS = {
    "CAPTURE_ARTIFACT_SET_MISMATCH",
    "CAPTURE_CHANGE_TABLE_MISMATCH",
    "CAPTURE_COLUMNS_MISMATCH",
    "CAPTURE_ENABLE_FAILED",
    "CAPTURE_FUNCTION_DEFINITION_UNVERIFIED",
    "CAPTURE_FUNCTION_IDENTITY_MISMATCH",
    "CAPTURE_IDENTITY_MISMATCH",
    "CAPTURE_SET_MISMATCH",
    "CAPTURE_SOURCE_IDENTITY_MISMATCH",
    "DATABASE_MISMATCH",
    "DATABASE_PREREQUISITES",
    "DATABASE_UNVERIFIED",
    "DBA_REQUIRED",
    "DEDICATED_TRANSACTION_REQUIRED",
    "EXPLICIT_TLS_ENDPOINT_REQUIRED",
    "FIXTURE_FORCED_POST_DDL_ROLLBACK",
    "INSTALLATION_MISMATCH",
    "INVALID_EXPECTED_IDENTITY",
    "INVALID_TABLE",
    "LIVE_INCUMBENT_CAPTURE_REQUIRED",
    "NAMESPACE_COLLISION",
    "PARTIAL_INSTALLATION",
    "POSTCONDITION_MISMATCH",
    "PREEXISTING_CDC_JOBS_REQUIRED",
    "ROLE_COLLISION",
    "ROLE_REQUIRED_FUNCTION_PERMISSIONS_MISSING",
    "ROLE_REQUIRED_TABLE_PERMISSIONS_MISSING",
    "ROLE_SCOPE_OR_INHERITANCE_MISMATCH",
    "ROLE_UNEXPECTED_OBJECT_PERMISSIONS",
    "ROLLBACK_NOT_CONFIRMED",
    "SETUP_LOCK_UNAVAILABLE",
    "SQL_OPERATION_FAILED",
    "TABLE_DEPENDENCY_MISMATCH",
    "TABLE_EXTERNAL_MODE_MISMATCH",
    "TABLE_KEY_MISMATCH",
    "TABLE_MEMORY_MODE_MISMATCH",
    "TABLE_OBJECT_TYPE_MISMATCH",
    "TABLE_OWNER_MISMATCH",
    "TABLE_SHAPE_MISMATCH",
    "TABLE_TEMPORAL_MISMATCH",
    "TABLE_TRACKING_MISMATCH",
}


def numeric_code(value):
    """SQLエラーで出力可能なのは固定範囲内の整数だけ。"""
    return type(value) is int and 0 <= value <= 999999


class CaptureNotReady(AssertionError):
    """DB名・ジョブ名・LSNを出さず、診断を定型化する。"""

    def __init__(self, probe, scanner_present, scanner_error=None):
        super().__init__()
        self.probe = probe
        self.scanner_present = scanner_present
        self.scanner_error = scanner_error


class AgentNotReady(AssertionError):
    """Agentの新規セッション未確認と、限定したTLSシグナルを区別する。"""

    def __init__(self, tls_observation):
        super().__init__()
        self.tls_observation = tls_observation


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
        if str(error) != reason:
            raise
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


def safe_failure(error):
    """診断は固定の種類・範囲内のエラー番号だけ。SQL例外本文は返さない。"""
    if isinstance(error, pymssql.Error):
        code = error.args[0] if error.args else None
        if numeric_code(code):
            return f"SQL_DRIVER_{code}"
        return "SQL_DRIVER_FAILURE"
    if isinstance(error, CONTRACT.ContractError):
        reason = str(error)
        return (
            reason if reason in PUBLIC_CONTRACT_ERRORS else "CONTRACT_FAILURE"
        )
    if isinstance(error, AgentNotReady):
        if error.tls_observation is True:
            return "AGENT_TLS_CERTIFICATE_ERROR"
        if error.tls_observation is False:
            return "AGENT_SESSION_MISSING_NO_CERTIFICATE_ERROR"
        return "AGENT_SESSION_MISSING_LOG_UNAVAILABLE"
    if isinstance(error, CaptureNotReady):
        scanner = (
            "SCAN_SESSION" if error.scanner_present else "NO_SCAN_SESSION"
        )
        if numeric_code(error.scanner_error):
            scanner += f"_ERROR_{error.scanner_error}"
        else:
            scanner += "_NO_ERROR"
        return sibling_probe_summary(error.probe) + "/" + scanner
    if isinstance(error, AssertionError):
        return "ASSERTION_FAILED"
    return "UNEXPECTED_FAILURE"


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


def agent_certificate_error(settings):
    """fixtureのAgentログから証明書失敗の有無だけを読み、本文は秘匿する。"""
    try:
        rows = execute(settings, "EXEC sys.sp_readerrorlog 0,2,N'certificate'")
    except pymssql.Error:
        return None
    phrases = (
        "certificate verify failed",
        "certificate chain was issued by an authority that is not trusted",
        "unable to get local issuer certificate",
        "self-signed certificate",
        "hostname mismatch",
    )
    return any(
        isinstance(row[-1], str)
        and any(phrase in row[-1].casefold() for phrase in phrases)
        for row in rows
        if row
    )


def wait_for_agent_session(settings):
    """新規コンテナのAgentが現在のmsdbセッションを記録するまで待つ。"""
    deadline = monotonic() + 30
    while monotonic() < deadline:
        ((session_id,),) = execute(
            settings, "SELECT MAX(session_id) FROM msdb.dbo.syssessions"
        )
        if type(session_id) is int and session_id > 0:
            return
        sleep(1)
    raise AgentNotReady(agent_certificate_error(settings))


def capture_job_active(settings, job_id):
    """対象DB・既知の捕捉ジョブ・現在のAgentセッションだけを観測する。"""
    rows = execute(
        settings,
        "SELECT j.enabled,CASE WHEN a.start_execution_date IS NOT NULL "
        "AND a.stop_execution_date IS NULL THEN 1 ELSE 0 END "
        "FROM msdb.dbo.cdc_jobs c JOIN msdb.dbo.sysjobs j "
        "ON c.job_id=j.job_id "
        "LEFT JOIN msdb.dbo.sysjobactivity a ON a.job_id=j.job_id "
        "AND a.session_id=(SELECT MAX(session_id) FROM msdb.dbo.syssessions) "
        "WHERE c.database_id=DB_ID() AND c.job_type=N'capture' "
        "AND j.job_id=CONVERT(uniqueidentifier,%s)",
        (str(job_id),),
    )
    assert len(rows) == 1 and rows[0][0] == 1
    return rows[0][1] == 1


def ensure_fixture_capture_started(
    settings, job_id, owned_database_guid, *, stage3=False
):
    """対象の隔離DBに生成した既知の捕捉ジョブだけを一度起動する。"""
    expected = settings["SOURCE_SQLSERVER_DATABASE"]
    assert approved_capture_fixture(
        settings["SOURCE_SQLSERVER_HOST"],
        expected,
        stage3=stage3,
        stage_two_targets={DATABASE, COLLISION_DB},
    )
    assert execute(settings, "SELECT DB_NAME()") == [(expected,)]
    assert database_guid(settings) == owned_database_guid
    deadline = monotonic() + 5
    while monotonic() < deadline:
        if capture_job_active(settings, job_id):
            return
        sleep(1)
    try:
        execute(settings, "EXEC sys.sp_cdc_start_job @job_type=N'capture'")
    except pymssql.Error as error:
        # 競合による二重起動は、対象ジョブが稼働中と読めた場合だけ許容。
        if error.args[:1] != (22022,) or not capture_job_active(
            settings, job_id
        ):
            raise
        return
    deadline = monotonic() + 15
    while monotonic() < deadline:
        if capture_job_active(settings, job_id):
            return
        sleep(1)
    raise AssertionError


def lsn_status(name):
    """固定SQL式のLSNを、生の値を返さない0/1/NULLに正規化する。"""
    if name not in {"x.start_lsn", "f.minimum_lsn"}:
        raise AssertionError
    return (
        f"CASE WHEN {name} IS NULL THEN NULL "
        f"WHEN DATALENGTH({name})=10 AND "
        f"{name}<>0x00000000000000000000 THEN 1 ELSE 0 END"
    )


def sibling_probe(settings):
    """兄弟captureの行を先に絞り込み、失敗条件を隠さず観測する。"""
    rows = execute(
        settings,
        "SELECT CASE WHEN t.object_id=OBJECT_ID(N'dbo.fixture_sibling',N'U') "
        "THEN 1 ELSE 0 END,"
        "CASE WHEN ct.object_id=OBJECT_ID(%s,N'U') THEN 1 ELSE 0 END,"
        "t.is_tracked_by_cdc,x.has_drop_pending,"
        + lsn_status("x.start_lsn")
        + ","
        + lsn_status("f.minimum_lsn")
        + ",CASE WHEN x.start_lsn=f.minimum_lsn THEN 1 ELSE 0 END "
        "FROM cdc.change_tables x "
        "LEFT JOIN sys.tables t ON t.object_id=x.source_object_id "
        "LEFT JOIN sys.tables ct ON ct.object_id=x.object_id "
        "CROSS APPLY (VALUES (sys.fn_cdc_get_min_lsn(x.capture_instance))) "
        "f(minimum_lsn) WHERE x.capture_instance COLLATE "
        "Latin1_General_100_BIN2=%s",
        ("[cdc].[" + SIBLING + "_CT]", SIBLING),
    )
    if len(rows) != 1:
        return ()
    observation = rows[0]
    if len(observation) != 7:
        return ()
    sentinel = False
    if observation[:2] == (1, 1):
        # 名前・型・IDを照合できた隔離fixtureの変更表だけを読む。
        sentinel = execute(
            settings,
            f"SELECT COUNT_BIG(*) FROM [cdc].[{SIBLING}_CT] "
            "WHERE id=1 AND note=N'既存番兵' AND [__$operation]=2",
        ) == [(1,)]
    return (*observation, sentinel)


def sibling_probe_summary(probe):
    """生のLSNや識別子を含まない、固定の状態語だけに変換する。"""
    if len(probe) != 8:
        return "CAPTURE_CATALOG_NOT_SINGLE"
    src, ct, tracked, pending, catalog, minimum, same, sentinel = probe
    pending_state = (
        "DROP_NONE"
        if pending == 0
        else "DROP_PENDING"
        if pending == 1
        else "DROP_UNKNOWN"
    )

    def init_state(value):
        return "READY" if value == 1 else "NULL" if value is None else "ZERO"

    return "+".join(
        (
            "SOURCE_OK" if src == 1 else "SOURCE_MISMATCH",
            "CT_OK" if ct == 1 else "CT_MISMATCH",
            "TRACKED" if tracked == 1 else "NOT_TRACKED",
            pending_state,
            "CATALOG_" + init_state(catalog),
            "FN_" + init_state(minimum),
            "LSNS_MATCH" if same == 1 else "LSNS_NOT_MATCHED",
            "SENTINEL_CAPTURED"
            if sentinel is True
            else "SENTINEL_NOT_CAPTURED",
        )
    )


def sibling_probe_ready(probe):
    """物理照合・実LSN・番兵を伴う場合だけ、未記録のdropを許容する。"""
    return probe in (
        (1, 1, 1, 0, 1, 1, 1, True),
        (1, 1, 1, None, 1, 1, 1, True),
    )


def wait_for_live_sibling(settings):
    """最大90秒で、元表・変更表・有効LSNと実際の番兵を確認する。"""
    deadline = monotonic() + 90
    probe = ()
    while monotonic() < deadline:
        probe = sibling_probe(settings)
        if sibling_probe_ready(probe):
            return
        sleep(1)
    errors = execute(
        settings,
        "SELECT TOP (1) error_number FROM sys.dm_cdc_errors "
        "ORDER BY entry_time DESC",
    )
    scanner_present = execute(
        settings,
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.dm_cdc_log_scan_sessions "
        "WHERE session_id>0) THEN 1 ELSE 0 END",
    ) == [(1,)]
    raise CaptureNotReady(
        probe, scanner_present, errors[0][0] if len(errors) == 1 else None
    )


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

        phase = "CDC_FIXTURE_SQL2022_VERSION"
        assert execute(
            master,
            "SELECT CONVERT(int,SERVERPROPERTY('ProductMajorVersion'))",
        ) == [(EXPECTED_MAJOR,)]

        phase = "CDC_FIXTURE_OWN_DATABASE"
        execute(master, f"CREATE DATABASE [{DATABASE}]")
        guid = database_guid(admin)
        phase = "CDC_DB_DISABLED_DENIED"
        require_contract_denial(
            admin, DATABASE, guid, installation, "DATABASE_PREREQUISITES"
        )
        ensure_absent(admin)

        phase = "CDC_ENABLE_FIXTURE_DATABASE"
        execute(admin, "EXEC sys.sp_cdc_enable_db")
        phase = "CDC_MISSING_JOBS_DENIED"
        require_contract_denial(
            admin,
            DATABASE,
            guid,
            installation,
            "PREEXISTING_CDC_JOBS_REQUIRED",
        )
        ensure_absent(admin)

        phase = "CDC_FIXTURE_AGENT_SESSION"
        wait_for_agent_session(master)
        phase = "CDC_SHARED_ANCHOR_CREATE"
        seed_sibling(admin)
        phase = "CDC_CAPTURE_AND_CLEANUP_JOBS"
        initial_jobs = jobs(admin)
        phase = "CDC_CAPTURE_JOB_ACTIVITY"
        ensure_fixture_capture_started(admin, initial_jobs["capture"][0], guid)
        phase = "CDC_SIBLING_CAPTURE_LSN"
        wait_for_live_sibling(admin)
        phase = "CDC_SIBLING_BASELINE"
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
        collision_guid = database_guid(collision)
        execute(collision, "EXEC sys.sp_cdc_enable_db")
        seed_sibling(collision)
        phase = "CDC_COLLISION_JOBS"
        collision_jobs = jobs(collision)
        phase = "CDC_COLLISION_JOB_ACTIVITY"
        ensure_fixture_capture_started(
            collision, collision_jobs["capture"][0], collision_guid
        )
        phase = "CDC_COLLISION_CAPTURE_LSN"
        wait_for_live_sibling(collision)
        phase = "CDC_COLLISION_NAMESPACE"
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
            collision_guid,
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
    except Exception as error:
        raise AssertionError(
            f"SQL CDC contract fixture failed: {phase} / {safe_failure(error)}"
        ) from None
    print("SQL CDC contract and limited principals verified.")


if __name__ == "__main__":
    verify(Path(sys.argv[1]), sys.argv[2])

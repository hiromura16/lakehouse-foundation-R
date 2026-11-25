#!/usr/bin/env python3
"""明示したDBに限り、Foundation R専用のSQL CDC契約を固定する。

通常は読取だけで、接続先・マーカー・実効メタデータに異常があれば停止する。
合成試験を除き、DB全体のCDCやAgentジョブはこの入口の管理対象外。
"""

from argparse import ArgumentParser
from dataclasses import dataclass
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from re import fullmatch
from uuid import UUID

SCHEMA = "foundation_r_demo"
READER_ROLE = SCHEMA + "_cdc_reader"
WRITER_ROLE = SCHEMA + "_cdc_writer"
MARKER = "installation"
COLLATION = "Latin1_General_100_BIN2"
SYSTEM = {"master", "model", "msdb", "tempdb"}
VERBS = ("SELECT", "INSERT", "UPDATE", "DELETE")


class ContractError(ValueError):
    """非公開のSQL例外や契約値を含めない固定の失敗理由。"""


@dataclass(frozen=True)
class Column:
    """型・NULL・期待するSQL Serverカタログ値を固定する。"""

    name: str
    sql_type: str
    nullable: bool
    catalog: tuple


def text(name, length, nullable=True):
    """バイナリ照合順序を使うUnicode列を宣言する。"""
    return Column(
        name,
        f"nvarchar({length}) COLLATE {COLLATION}",
        nullable,
        ("nvarchar", length * 2, 0, COLLATION),
    )


def simple(name, sql_type, size, *, scale=0, nullable=True):
    """数値または時刻のSQL型と精度を宣言する。"""
    return Column(
        name, sql_type, nullable, (sql_type.split("(")[0], size, scale, None)
    )


def utc(name):
    """UTCとして扱うdatetime2(7)列を宣言する。"""
    return simple(name, "datetime2(7)", 8, scale=7)


def quantity(name):
    """64ビット数量を宣言する。"""
    return simple(name, "bigint", 8)


def key(name):
    """主キーは固定長上限のある非NULLのUnicodeとする。"""
    return text(name, 160, nullable=False)


TABLES = {
    "products": (
        key("product_id"),
        text("product_name", 256),
        text("product_family", 128),
        text("unit", 32),
        simple("standard_cycle_seconds", "decimal(18,3)", 9, scale=3),
        utc("updated_at"),
    ),
    "work_orders": (
        key("work_order_id"),
        text("factory_id", 64),
        text("product_id", 160),
        simple("planned_date", "date", 3),
        quantity("planned_qty"),
        utc("updated_at"),
    ),
    "sales_orders": (
        key("sales_order_id"),
        text("factory_id", 64),
        text("product_id", 160),
        simple("order_date", "date", 3),
        simple("promised_date", "date", 3),
        quantity("ordered_qty"),
        simple("unit_price", "decimal(18,2)", 9, scale=2),
        utc("updated_at"),
    ),
    "shipments": (
        key("shipment_id"),
        text("sales_order_id", 160),
        utc("shipped_at"),
        quantity("shipped_qty"),
        utc("updated_at"),
    ),
    "equipment_events": (
        key("event_id"),
        text("work_order_id", 160),
        utc("event_time"),
        quantity("good_qty"),
        quantity("reject_qty"),
        utc("updated_at"),
    ),
    "equipment_stops": (
        key("downtime_id"),
        text("work_order_id", 160),
        utc("start_time"),
        utc("end_time"),
        text("reason_code", 64),
        utc("updated_at"),
    ),
}
MARKER_COLUMNS = (
    simple("marker_id", "int", 4, nullable=False),
    simple("schema_version", "int", 4, nullable=False),
    Column(
        "installation_id",
        f"char(32) COLLATE {COLLATION}",
        False,
        ("char", 32, 0, COLLATION),
    ),
    simple("layout_fingerprint", "binary(32)", 32, nullable=False),
)


def capture_name(table):
    """既定の六表以外から捕捉名を組み立てない。"""
    if table not in TABLES:
        raise ContractError("INVALID_TABLE")
    return f"{SCHEMA}_{table}_v1"


def query(cursor, statement, args=()):
    """固定クエリの行を取得し、欠落を暗黙の成功として扱わない。"""
    cursor.execute(statement, args)
    return tuple(cursor.fetchall())


def db_gate(cursor, expected_database, expected_database_guid):
    """明示したDB名、DB識別、SQL Server版、DBAと変更条件を確認する。"""
    info = query(
        cursor,
        "SELECT DB_NAME(),CONVERT(varchar(36),service_broker_guid),"
        "is_cdc_enabled,is_read_only,IS_MEMBER(N'db_owner'),"
        "ISNULL(IS_SRVROLEMEMBER(N'sysadmin'),0),"
        "CONVERT(int,SERVERPROPERTY('ProductMajorVersion')),is_published,"
        "is_merge_published,is_subscribed,@@TRANCOUNT "
        "FROM sys.databases WHERE database_id=DB_ID()",
    )
    if len(info) != 1 or len(info[0]) != 11:
        raise ContractError("DATABASE_UNVERIFIED")
    name, guid, cdc, readonly, dba, admin, major, pub, merge, sub, txn = info[
        0
    ]
    if (
        name != expected_database
        or not guid
        or guid.lower() != expected_database_guid
    ):
        raise ContractError("DATABASE_MISMATCH")
    if dba != 1 and admin != 1:
        raise ContractError("DBA_REQUIRED")
    if (
        cdc != 1
        or readonly != 0
        or major != 16
        or (pub, merge, sub) != (0, 0, 0)
    ):
        raise ContractError("DATABASE_PREREQUISITES")
    if txn != 1:
        raise ContractError("DEDICATED_TRANSACTION_REQUIRED")


def jobs(cursor):
    """既存CDCジョブの識別子と設定を取得し、初回作成経路を閉じる。"""
    cursor.execute("EXEC sys.sp_cdc_help_jobs")
    names = tuple(item[0].lower() for item in (cursor.description or ()))
    rows = tuple(cursor.fetchall()) if names else ()
    if not {"job_id", "job_type"}.issubset(names) or len(rows) != 2:
        raise ContractError("PREEXISTING_CDC_JOBS_REQUIRED")
    tag, identifier = names.index("job_type"), names.index("job_id")
    indexed = {row[tag]: row for row in rows}
    if (
        set(indexed) != {"capture", "cleanup"}
        or any(not row[identifier] for row in rows)
        or rows[0][identifier] == rows[1][identifier]
    ):
        raise ContractError("PREEXISTING_CDC_JOBS_REQUIRED")
    return names, indexed


def incumbents(cursor):
    """別系統の実在・有効・初期化済みcaptureを共有ジョブの前提にする。"""
    # SQL Server 2022の有効captureでNULLを観測した。NULL単独では
    # 稼働の証拠にせず、追跡済み実表・変更表・LSNの全条件を要求する。
    rows = query(
        cursor,
        "SELECT x.source_object_id,x.object_id,x.capture_instance,"
        "x.create_date,src.schema_id,src.name,ct.name "
        "FROM cdc.change_tables x WITH (HOLDLOCK) "
        "JOIN sys.tables src ON src.object_id=x.source_object_id "
        "JOIN sys.tables ct ON ct.object_id=x.object_id "
        "WHERE src.is_tracked_by_cdc=1 AND ct.schema_id=SCHEMA_ID(N'cdc') "
        "AND ct.name COLLATE Latin1_General_100_BIN2="
        "(x.capture_instance+N'_CT') COLLATE Latin1_General_100_BIN2 "
        "AND (x.has_drop_pending=0 OR x.has_drop_pending IS NULL) "
        "AND DATALENGTH(x.start_lsn)=10 "
        "AND x.start_lsn<>0x00000000000000000000 "
        "AND x.start_lsn=sys.fn_cdc_get_min_lsn(x.capture_instance) "
        "AND src.schema_id<>ISNULL(SCHEMA_ID(%s),-1) "
        "AND x.capture_instance COLLATE Latin1_General_100_CI_AI "
        "NOT LIKE N'foundation[_]r[_]demo[_]%%' ORDER BY x.source_object_id,"
        "x.object_id",
        (SCHEMA,),
    )
    if (
        not rows
        or len({row[1] for row in rows}) != len(rows)
        or len({row[2] for row in rows}) != len(rows)
    ):
        raise ContractError("LIVE_INCUMBENT_CAPTURE_REQUIRED")
    return rows


def namespace(cursor):
    """専用スキーマ・ロール名と対象外の同名捕捉を、変更前に照合する。"""
    schema = query(
        cursor,
        "SELECT schema_id,name,CASE WHEN principal_id="
        "DATABASE_PRINCIPAL_ID(N'dbo') THEN 1 ELSE 0 END "
        "FROM sys.schemas WHERE name COLLATE Latin1_General_100_CI_AI=%s",
        (SCHEMA,),
    )
    roles = query(
        cursor,
        "SELECT principal_id,name,type,is_fixed_role,"
        "CASE WHEN owning_principal_id=DATABASE_PRINCIPAL_ID(N'dbo') "
        "THEN 1 ELSE 0 END,create_date "
        "FROM sys.database_principals WHERE name COLLATE "
        "Latin1_General_100_CI_AI IN (%s,%s)",
        (READER_ROLE, WRITER_ROLE),
    )
    captures = query(
        cursor,
        "SELECT x.source_object_id,x.object_id,x.capture_instance,x.role_name,"
        "x.index_name,x.supports_net_changes,COALESCE(x.has_drop_pending,0),"
        "x.create_date "
        "FROM cdc.change_tables x LEFT JOIN sys.objects o "
        "ON o.object_id=x.source_object_id "
        "WHERE o.schema_id=SCHEMA_ID(%s) OR "
        "x.capture_instance COLLATE Latin1_General_100_CI_AI "
        "LIKE N'foundation[_]r[_]demo[_]%%'",
        (SCHEMA,),
    )
    artifacts = query(
        cursor,
        "SELECT o.object_id,o.name,o.type,o.create_date,"
        "HASHBYTES('SHA2_256',CONVERT(varbinary(max),m.definition)) "
        "FROM sys.objects o LEFT JOIN sys.all_sql_modules m "
        "ON o.object_id=m.object_id "
        "WHERE o.schema_id=SCHEMA_ID(N'cdc') "
        "AND (o.name COLLATE Latin1_General_100_CI_AI LIKE "
        "N'foundation[_]r[_]demo[_]%%' OR "
        "o.name COLLATE Latin1_General_100_CI_AI LIKE "
        "N'fn[_]cdc[_]get[_]%%[_]foundation[_]r[_]demo[_]%%') "
        "AND o.parent_object_id=0",
    )
    return schema, roles, captures, artifacts


def complete_definition_digest(value):
    """エンジン側で全文を計算できた固定長SHA-256だけを採用する。"""
    return isinstance(value, bytes) and len(value) == 32


def fingerprint(schema, roles, captures, artifacts):
    """同じ名前の置き換えを再取込せず、実体の識別子と作成日時を固定する。"""
    evidence = (
        schema,
        tuple(sorted(roles)),
        tuple(sorted(captures)),
        tuple(sorted(artifacts)),
    )
    return sha256(repr(evidence).encode("utf-8")).digest()


def pk_name(table):
    """専用表の固定主キー名を返す。"""
    return f"pk_{table}"


def inspect_table(cursor, table, object_id, columns):
    """列順、型、照合順序、NULL、PK、所有、危険な追加物を照合する。"""
    actual = query(
        cursor,
        "SELECT c.name,TYPE_NAME(c.user_type_id),c.max_length,c.scale,"
        "c.collation_name,c.is_nullable,c.is_identity,c.is_computed,"
        "c.is_hidden,c.default_object_id,c.rule_object_id,c.precision "
        "FROM sys.columns c WHERE c.object_id=%s ORDER BY c.column_id",
        (object_id,),
    )
    expected = tuple(
        (c.name, *c.catalog, int(c.nullable), 0, 0, 0, 0, 0) for c in columns
    )
    if tuple(row[:-1] for row in actual) != expected or any(
        column.catalog[0] == "decimal" and row[-1] != 18
        for column, row in zip(columns, actual, strict=True)
    ):
        raise ContractError("TABLE_SHAPE_MISMATCH")
    key = query(
        cursor,
        "SELECT i.name,c.name,i.is_primary_key,i.is_unique,i.is_disabled,"
        "k.is_descending_key FROM sys.indexes i "
        "JOIN sys.index_columns k ON k.object_id=i.object_id "
        "AND k.index_id=i.index_id "
        "JOIN sys.columns c ON c.object_id=k.object_id "
        "AND c.column_id=k.column_id "
        "WHERE i.object_id=%s AND k.key_ordinal>0 "
        "ORDER BY i.index_id,k.key_ordinal",
        (object_id,),
    )
    if key != ((pk_name(table), columns[0].name, 1, 1, 0, 0),):
        raise ContractError("TABLE_KEY_MISMATCH")


def owned_capture_matches(row, expected_source, table):
    """所有済みcaptureはLSN未初期化でも構成の一致が必要。"""
    return row[0] == expected_source and row[3:7] == (
        READER_ROLE,
        pk_name(table),
        0,
        0,
    )


def inspect(cursor, installation_id):
    """空、完全一致のいずれかを返し、半端な既存配置を採用しない。"""
    schema, roles, captures, artifacts = namespace(cursor)
    if not (schema or roles or captures or artifacts):
        return "ABSENT"
    if len(schema) != 1 or schema[0][1:] != (SCHEMA, 1) or len(roles) != 2:
        raise ContractError("NAMESPACE_COLLISION")
    if {row[1:5] for row in roles} != {
        (READER_ROLE, "R", 0, 1),
        (WRITER_ROLE, "R", 0, 1),
    }:
        raise ContractError("ROLE_COLLISION")
    schema_id = schema[0][0]
    owned = query(
        cursor,
        "SELECT o.object_id,o.name,o.type,CASE WHEN "
        "COALESCE(o.principal_id,s.principal_id)="
        "DATABASE_PRINCIPAL_ID(N'dbo') "
        "THEN 1 ELSE 0 END,t.temporal_type,t.is_memory_optimized,"
        "t.is_external,t.is_tracked_by_cdc "
        "FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id "
        "LEFT JOIN sys.tables t ON t.object_id=o.object_id "
        "WHERE o.schema_id=%s AND o.parent_object_id=0",
        (schema_id,),
    )
    expected = {**TABLES, MARKER: MARKER_COLUMNS}
    if len(owned) != len(expected) or {row[1] for row in owned} != set(
        expected
    ):
        raise ContractError("PARTIAL_INSTALLATION")
    for (
        object_id,
        name,
        kind,
        owner,
        temporal,
        memory,
        external,
        tracked,
    ) in owned:
        if (kind, owner, temporal, memory, external, tracked) != (
            "U",
            1,
            0,
            0,
            0,
            int(name != MARKER),
        ):
            raise ContractError("TABLE_IDENTITY_MISMATCH")
        inspect_table(cursor, name, object_id, expected[name])
    unexpected = query(
        cursor,
        "SELECT TOP (1) o.object_id FROM sys.objects o "
        "WHERE o.schema_id=%s AND o.parent_object_id<>0 AND o.type<>N'PK' "
        "UNION ALL SELECT fk.object_id FROM sys.foreign_keys fk "
        "JOIN sys.objects o ON o.object_id=fk.referenced_object_id "
        "WHERE o.schema_id=%s",
        (schema_id, schema_id),
    )
    if unexpected:
        raise ContractError("TABLE_DEPENDENCY_MISMATCH")
    marker = query(
        cursor,
        "SELECT TOP (2) marker_id,schema_version,installation_id,"
        "layout_fingerprint "
        "FROM [foundation_r_demo].[installation]",
    )
    if marker != (
        (
            1,
            1,
            installation_id,
            fingerprint(schema, roles, captures, artifacts),
        ),
    ):
        raise ContractError("INSTALLATION_MISMATCH")
    expected_objects = {row[1]: row[0] for row in owned}
    if len(captures) != len(TABLES):
        raise ContractError("CAPTURE_SET_MISMATCH")
    by_name = {row[2]: row for row in captures}
    if set(by_name) != {capture_name(table) for table in TABLES}:
        raise ContractError("CAPTURE_SET_MISMATCH")
    expected_artifact_names = set()
    functions = set()
    indexed_artifacts = {
        name: (object_id, kind, definition)
        for object_id, name, kind, _, definition in artifacts
    }
    for table, columns in TABLES.items():
        capture = capture_name(table)
        row = by_name[capture]
        if not owned_capture_matches(row, expected_objects[table], table):
            raise ContractError("CAPTURE_IDENTITY_MISMATCH")
        actual_columns = query(
            cursor,
            "SELECT column_name,column_ordinal FROM cdc.captured_columns "
            "WHERE object_id=%s ORDER BY column_ordinal",
            (row[1],),
        )
        if actual_columns != tuple(
            (c.name, i) for i, c in enumerate(columns, 1)
        ):
            raise ContractError("CAPTURE_COLUMNS_MISMATCH")
        change, function = capture + "_CT", "fn_cdc_get_all_changes_" + capture
        expected_artifact_names.update((change, function))
        if indexed_artifacts.get(change) != (row[1], "U", None):
            raise ContractError("CAPTURE_ARTIFACT_MISMATCH")
        function_info = indexed_artifacts.get(function)
        if (
            not function_info
            or function_info[1] not in {"IF", "TF"}
            or not complete_definition_digest(function_info[2])
        ):
            raise ContractError("CAPTURE_ARTIFACT_MISMATCH")
        functions.add(function_info[0])
    if len(artifacts) != len(expected_artifact_names) or (
        set(indexed_artifacts) != expected_artifact_names
    ):
        raise ContractError("CAPTURE_ARTIFACT_MISMATCH")
    protected = [*expected_objects.values(), *(row[1] for row in captures)]
    rls = query(
        cursor,
        "SELECT TOP (1) target_object_id FROM sys.security_predicates "
        "WHERE target_object_id IN ("
        + ",".join("%s" for _ in protected)
        + ")",
        tuple(protected),
    )
    if rls:
        raise ContractError("TABLE_DEPENDENCY_MISMATCH")
    check_roles(cursor, roles, expected_objects, functions)
    return "VERIFIED"


def check_roles(cursor, roles, tables, functions):
    """作成したロールの権限とロール間の継承を固定する。"""
    role_ids = {row[1]: row[0] for row in roles}
    reader, writer = role_ids[READER_ROLE], role_ids[WRITER_ROLE]
    allowed = {
        (role, tables[table], 0, "G", operation)
        for table in TABLES
        for role, operations in ((reader, ("SELECT",)), (writer, VERBS))
        for operation in operations
    }
    # SQL Server自身が生成する取得関数への正確なSELECTだけを含める。
    allowed.update((reader, fn, 0, "G", "SELECT") for fn in functions)
    permissions = query(
        cursor,
        "SELECT grantee_principal_id,major_id,minor_id,state,permission_name "
        "FROM sys.database_permissions WHERE grantee_principal_id IN (%s,%s) "
        "AND class=1",
        (reader, writer),
    )
    other = query(
        cursor,
        "SELECT TOP (1) grantee_principal_id FROM sys.database_permissions "
        "WHERE grantee_principal_id IN (%s,%s) AND class<>1 "
        "UNION ALL SELECT member_principal_id FROM sys.database_role_members "
        "WHERE member_principal_id IN (%s,%s)",
        (reader, writer, reader, writer),
    )
    if (
        other
        or set(permissions) != allowed
        or len(permissions) != len(allowed)
    ):
        raise ContractError("ROLE_PERMISSIONS_MISMATCH")


def create(cursor, installation_id):
    """DBA承認の専用オブジェクトだけを追加する。"""
    cursor.execute(f"CREATE SCHEMA [{SCHEMA}] AUTHORIZATION [dbo]")
    for role in (READER_ROLE, WRITER_ROLE):
        cursor.execute(f"CREATE ROLE [{role}] AUTHORIZATION [dbo]")
    for table, columns in {**TABLES, MARKER: MARKER_COLUMNS}.items():
        declarations = [
            f"[{c.name}] {c.sql_type} "
            + ("NULL" if c.nullable else "NOT NULL")
            for c in columns
        ]
        declarations.append(
            f"CONSTRAINT [{pk_name(table)}] PRIMARY KEY ([{columns[0].name}])"
        )
        cursor.execute(
            f"CREATE TABLE [{SCHEMA}].[{table}] ("
            + ",".join(declarations)
            + ")"
        )
    for table, columns in TABLES.items():
        cursor.execute(
            "DECLARE @rc int; EXEC @rc=sys.sp_cdc_enable_table "
            "@source_schema=%s,@source_name=%s,@capture_instance=%s,"
            "@role_name=%s,@supports_net_changes=0,@index_name=%s,"
            "@captured_column_list=%s,@allow_partition_switch=0; "
            "SELECT @rc",
            (
                SCHEMA,
                table,
                capture_name(table),
                READER_ROLE,
                pk_name(table),
                ",".join(c.name for c in columns),
            ),
        )
        if cursor.fetchone() != (0,):
            raise ContractError("CAPTURE_ENABLE_FAILED")
        cursor.execute(
            f"GRANT SELECT ON [{SCHEMA}].[{table}] TO [{READER_ROLE}]"
        )
        cursor.execute(
            f"GRANT SELECT,INSERT,UPDATE,DELETE ON [{SCHEMA}].[{table}] "
            f"TO [{WRITER_ROLE}]"
        )
    schema, roles, captures, artifacts = namespace(cursor)
    cursor.execute(
        "INSERT INTO [foundation_r_demo].[installation] "
        "(marker_id,schema_version,installation_id,layout_fingerprint) "
        "VALUES (1,1,%s,%s)",
        (installation_id, fingerprint(schema, roles, captures, artifacts)),
    )


def expected_identity(
    expected_database, installation_id, expected_database_guid
):
    """必須の設置識別子を接続より前に形式検査する。"""
    if (
        not isinstance(expected_database, str)
        or not fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", expected_database)
        or expected_database.casefold() in SYSTEM
        or not isinstance(installation_id, str)
        or not fullmatch(r"[0-9a-f]{32}", installation_id)
    ):
        raise ContractError("INVALID_EXPECTED_IDENTITY")
    try:
        expected_guid = str(UUID(expected_database_guid))
        if expected_guid == str(UUID(int=0)):
            raise ValueError
    except (TypeError, ValueError, AttributeError):
        raise ContractError("INVALID_EXPECTED_IDENTITY") from None
    return expected_guid


def configure(
    connection,
    expected_database,
    installation_id,
    *,
    expected_database_guid,
    apply=False,
):
    """未作成なら検査だけ。明示承認時だけ専用スキーマを追加する。"""
    if type(apply) is not bool:
        raise ContractError("INVALID_EXPECTED_IDENTITY")
    expected_guid = expected_identity(
        expected_database, installation_id, expected_database_guid
    )
    try:
        cursor = connection.cursor()
        db_gate(cursor, expected_database, expected_guid)
        cursor.execute(
            "SET NOCOUNT ON; SET XACT_ABORT ON; SET LOCK_TIMEOUT 5000"
        )
        if apply:
            lock = query(
                cursor,
                "DECLARE @rc int; EXEC @rc=sys.sp_getapplock "
                "@Resource=N'foundation_r_demo_cdc_v1',@LockMode=N'Exclusive',"
                "@LockOwner=N'Transaction',@LockTimeout=5000; SELECT @rc",
            )
            if len(lock) != 1 or lock[0][0] not in (0, 1):
                raise ContractError("SETUP_LOCK_UNAVAILABLE")
        before_jobs = jobs(cursor)
        incumbent_before = incumbents(cursor) if apply else None
        state = inspect(cursor, installation_id)
        if state == "VERIFIED" or not apply:
            connection.rollback()
            return state
        create(cursor, installation_id)
        if (
            inspect(cursor, installation_id) != "VERIFIED"
            or jobs(cursor) != before_jobs
            or incumbents(cursor) != incumbent_before
        ):
            raise ContractError("POSTCONDITION_MISMATCH")
        connection.commit()
        return "CREATED"
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            raise ContractError("ROLLBACK_NOT_CONFIRMED") from None
        if isinstance(exc, ContractError):
            raise
        raise ContractError("SQL_OPERATION_FAILED") from None


def connection_settings(path):
    """既存の所有者専用設定ファイルとTLSの読み方を再利用する。"""
    spec = spec_from_file_location(
        "fixed_cdc_connection",
        Path(__file__).with_name("extract-sqlserver.py"),
    )
    source = module_from_spec(spec)
    spec.loader.exec_module(source)
    selected = source.private_file(path)
    explicit = {
        line.partition("=")[0]
        for line in selected.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    }
    if not {"SOURCE_SQLSERVER_PORT", "SOURCE_SQLSERVER_CA_FILE"} <= explicit:
        raise ContractError("EXPLICIT_TLS_ENDPOINT_REQUIRED")
    values = source.read_settings(selected)
    return source, values


def main(argv=None):
    """明示ファイルでDBA用の読取検査または加算的セットアップを開く。"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--settings", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    try:
        source, values = connection_settings(args.settings)
        expected = values["CDC_EXPECTED_DATABASE"]
        guid = values["CDC_EXPECTED_BROKER_GUID"]
        installation = values["CDC_INSTALLATION_ID"]
        if expected != values["SOURCE_SQLSERVER_DATABASE"]:
            raise ContractError("DATABASE_MISMATCH")
        expected_identity(expected, installation, guid)
        import pymssql

        with source.tls_configuration(values):
            connection = pymssql.connect(
                server="lh_source_target",
                port=values["SOURCE_SQLSERVER_PORT"],
                database=expected,
                user=values["SOURCE_SQLSERVER_USER"],
                password=values["SOURCE_SQLSERVER_PASSWORD"],
                encryption="require",
                autocommit=False,
                login_timeout=10,
                timeout=60,
            )
            try:
                result = configure(
                    connection,
                    expected,
                    installation,
                    expected_database_guid=guid,
                    apply=args.apply,
                )
            finally:
                connection.close()
    except ContractError as error:
        print(f"FAIL: {error}")
        return 1
    except Exception:
        print("FAIL: DBA_SESSION_UNAVAILABLE")
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

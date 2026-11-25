"""隔離SQL Serverで、固定writerの実効権限とI/U/Dを検査する。"""

from decimal import Decimal
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from json import loads
from pathlib import Path
from secrets import token_hex

ROOT = Path(__file__).resolve().parents[3]
SPEC = spec_from_file_location(
    "sqlserver_fixed_writer", ROOT / "scripts/sqlserver_demo.py"
)
WRITER = module_from_spec(SPEC)
SPEC.loader.exec_module(WRITER)
LOGIN = "fixture_fixed_writer"
ROLE = "lh_demo_writer"
OTHER_PRINCIPAL = "fixture_reader"
OTHER_OBJECT = "dbo.fixture_writer_unrelated"
EXTRA_OBJECT = "dbo.fixture_writer_not_approved"
LEGACY_ROLE = "fixture_legacy_limited"
LEGACY_PARENT = "fixture_legacy_parent"
NEIGHBOR_RUN = "0123456789ab"
NEIGHBOR_PRODUCT = "CDC-" + NEIGHBOR_RUN
NEIGHBOR_ROW = (NEIGHBOR_PRODUCT, NEIGHBOR_RUN, "fixture neighbor", None)
SENTINEL = (1, "fixture legacy sentinel")


def install(connect, admin, author):
    """使い捨てDB内に固定6表と制御3表だけを準備する。"""
    password = author["DEMO_SQLSERVER_PASSWORD"]
    with connect(admin, SOURCE_SQLSERVER_DATABASE="master") as cursor:
        cursor.execute(f"CREATE LOGIN [{LOGIN}] WITH PASSWORD = '{password}'")
    with connect(admin) as cursor:
        for schema in WRITER.SCHEMAS.values():
            cursor.execute(f"CREATE SCHEMA [{schema}] AUTHORIZATION [dbo]")
        cursor.execute(f"CREATE USER [{LOGIN}] FOR LOGIN [{LOGIN}]")
        cursor.execute(f"CREATE ROLE [{ROLE}] AUTHORIZATION [dbo]")
        cursor.execute(f"ALTER ROLE [{ROLE}] ADD MEMBER [{LOGIN}]")
        for name, item in WRITER.TABLES.items():
            target = WRITER.table_name(name)
            columns = [
                f"[{col}] nvarchar(160) PRIMARY KEY"
                if col == item["pk"]
                else "[updated_at] datetime2(7) DEFAULT SYSUTCDATETIME()"
                if col == "updated_at"
                else f"[{col}] nvarchar(160)"
                for col in item["columns"]
            ]
            cursor.execute(
                f"CREATE TABLE {target} ("
                + ", ".join(columns)
                + ",run_id char(12),original_values nvarchar(max))"
            )
            cursor.execute(
                f"GRANT SELECT,INSERT,UPDATE,DELETE ON {target} TO [{ROLE}]"
            )
        cursor.execute(
            "CREATE TABLE lh_demo_ctl.installation "
            "(singleton_id int PRIMARY KEY, schema_version int NOT NULL,"
            " installation_id char(32) NOT NULL, "
            "ownership_hash char(64) NOT NULL)"
        )
        cursor.execute(
            "INSERT INTO lh_demo_ctl.installation VALUES (1, %s, %s, %s)",
            (
                WRITER.SCHEMA_VERSION,
                author["DEMO_SQLSERVER_INSTALLATION_ID"],
                sha256(
                    author["DEMO_SQLSERVER_OWNERSHIP_TOKEN"].encode()
                ).hexdigest(),
            ),
        )
        cursor.execute(
            "CREATE TABLE lh_demo_ctl.demo_runs "
            "(run_id char(12) PRIMARY KEY, seed int, "
            "quality_mode nvarchar(32),"
            " record_count int, period_days int, target_mode nvarchar(64),"
            " status nvarchar(32), completed_at datetime2(7))"
        )
        cursor.execute(
            "CREATE TABLE lh_demo_ctl.batch_watermarks (id int PRIMARY KEY)"
        )
        cursor.execute("SET ANSI_NULLS ON")
        cursor.execute("SET QUOTED_IDENTIFIER ON")
        cursor.execute(WRITER.SAFETY_DEFINITION)
        for permission, target in (
            ("SELECT", "lh_demo_ctl.installation"),
            ("SELECT,INSERT,UPDATE", "lh_demo_ctl.demo_runs"),
            ("EXECUTE,VIEW DEFINITION", WRITER.SAFETY_CHECKER),
        ):
            cursor.execute(f"GRANT {permission} ON {target} TO [{ROLE}]")
        cursor.execute(
            "INSERT INTO lh_demo_erp.products "
            "(product_id,run_id,product_name,original_values) "
            "VALUES (%s,%s,%s,%s)",
            NEIGHBOR_ROW,
        )
        cursor.execute(
            f"CREATE TABLE {OTHER_OBJECT} "
            "(id int PRIMARY KEY, note nvarchar(64) NOT NULL)"
        )
        cursor.execute(
            f"INSERT INTO {OTHER_OBJECT} (id,note) VALUES (%s,%s)",
            SENTINEL,
        )
        cursor.execute(
            f"CREATE TABLE {EXTRA_OBJECT} "
            "(id int PRIMARY KEY, note nvarchar(64))"
        )
        cursor.execute(
            f"GRANT VIEW DEFINITION ON OBJECT::{OTHER_OBJECT} TO [{ROLE}]"
        )
        cursor.execute(
            f"GRANT VIEW DEFINITION ON USER::[{OTHER_PRINCIPAL}] TO [{ROLE}]"
        )


def assert_committed_product(cursor, run_id, action):
    """別接続で対象行のID・値・更新時の同一性・削除を確認する。"""
    key = "CDC-" + run_id
    cursor.execute(
        "SELECT product_id,run_id,product_name,product_family,unit,"
        "standard_cycle_seconds,original_values "
        "FROM lh_demo_erp.products WHERE product_id=%s OR run_id=%s",
        (key, run_id),
    )
    rows = cursor.fetchall()
    if action == "delete":
        assert not rows
        return
    name = {"insert": "合成CDC投入", "update": "合成CDC更新"}[action]
    assert len(rows) == 1
    (row,) = rows
    assert row[:5] == (key, run_id, name, "SYNTHETIC", "piece")
    assert Decimal(row[5]) == Decimal("30.000")
    payload = loads(row[6])
    assert isinstance(payload.pop("updated_at"), str)
    assert payload == {
        "product_id": key,
        "product_name": name,
        "product_family": "SYNTHETIC",
        "unit": "piece",
        "standard_cycle_seconds": "30.000",
    }


def assert_untouched_neighbors(cursor):
    """対象外のテーブルと固定表内の別runがそのままであることを確認する。"""
    cursor.execute(f"SELECT id,note FROM {OTHER_OBJECT}")
    assert cursor.fetchall() == [SENTINEL]
    cursor.execute(
        "SELECT product_id,run_id,product_name,original_values "
        "FROM lh_demo_erp.products WHERE product_id=%s OR run_id=%s",
        (NEIGHBOR_PRODUCT, NEIGHBOR_RUN),
    )
    assert cursor.fetchall() == [NEIGHBOR_ROW]


def install_policy(connect, admin, author):
    """管理接続だけで、実効権限に一致する合成の承認4件を設ける。"""
    with connect(admin) as cursor:
        for name in ("policy", "delegation"):
            cursor.execute(
                (ROOT / "scripts" / f"sqlserver_writer_legacy_{name}.sql")
                .read_text(encoding="utf-8")
                .rstrip("\n")
            )
        cursor.execute(
            f"INSERT INTO {WRITER.WRITER_POLICY} "
            "SELECT %s,p.sid,p.principal_id,o.object_id,o.schema_id,s.name,"
            "o.name,o.create_date,v.verb FROM sys.objects o "
            "JOIN sys.schemas s ON s.schema_id=o.schema_id "
            "CROSS JOIN sys.database_principals p "
            "CROSS JOIN (VALUES ('SELECT'),('INSERT'),('UPDATE'),('DELETE')) "
            "AS v(verb) "
            "WHERE p.name=%s AND o.object_id=OBJECT_ID(%s,N'U')",
            (author["DEMO_SQLSERVER_INSTALLATION_ID"], LOGIN, OTHER_OBJECT),
        )
        cursor.execute(
            f"SELECT operation FROM {WRITER.WRITER_POLICY} "
            "WHERE installation_id=%s",
            (author["DEMO_SQLSERVER_INSTALLATION_ID"],),
        )
        assert set(cursor.fetchall()) == {
            ("SELECT",),
            ("INSERT",),
            ("UPDATE",),
            ("DELETE",),
        }
        cursor.execute(f"GRANT SELECT ON {WRITER.WRITER_POLICY} TO [{ROLE}]")
        cursor.execute(
            f"GRANT EXECUTE,VIEW DEFINITION ON {WRITER.WRITER_DELEGATION} "
            f"TO [{ROLE}]"
        )


def assert_fixture_denied(connect, admin, author):
    """拒否が確実にDML前であり、別runと外部センチネルを維持する。"""
    denied_run = "fedcba543210"
    try:
        WRITER.mutate(author, "insert", run_id=denied_run, consent=True)
    except WRITER.SqlServerError as error:
        assert "permissions exceed the allowed boundary" in str(error)
    else:
        raise AssertionError
    with connect(admin) as cursor:
        assert_committed_product(cursor, denied_run, "delete")
        assert_untouched_neighbors(cursor)
        cursor.execute(
            "SELECT COUNT_BIG(*) FROM lh_demo_ctl.demo_runs WHERE run_id=%s",
            (denied_run,),
        )
        assert cursor.fetchone() == (0,)


def verify_fixed_writer(connect, admin):
    """権限のない名前は許容し、実際の境界外実効UPDATEは拒否する。"""
    author = {
        "DEMO_SQLSERVER_HOST": "localhost",
        "DEMO_SQLSERVER_PORT": admin["SOURCE_SQLSERVER_PORT"],
        "DEMO_SQLSERVER_DATABASE": admin["SOURCE_SQLSERVER_DATABASE"],
        "DEMO_SQLSERVER_CA_FILE": admin["SOURCE_SQLSERVER_CA_FILE"],
        "DEMO_SQLSERVER_USER": LOGIN,
        "DEMO_SQLSERVER_PASSWORD": "Aa9!" + token_hex(24),
        "DEMO_SQLSERVER_INSTALLATION_ID": token_hex(16),
        "DEMO_SQLSERVER_OWNERSHIP_TOKEN": token_hex(32),
    }
    phase = "INSTALL"
    try:
        install(connect, admin, author)
        phase = "WRITER_SETTINGS_VALIDATION"
        WRITER.validate_settings(author)
        phase = "WRITER_TLS_AND_LOGIN"
        with WRITER.connection(author) as session:
            cursor = session.cursor()
            phase = "WRITER_SESSION_IDENTITY"
            cursor.execute("SELECT DB_NAME(), USER_NAME()")
            assert cursor.fetchone() == (
                author["DEMO_SQLSERVER_DATABASE"],
                LOGIN,
            )
            phase = "METADATA_ONLY_LEGACY"
            cursor.execute(
                "SELECT OBJECT_ID(%s,N'U'), DATABASE_PRINCIPAL_ID(%s), "
                "HAS_PERMS_BY_NAME(%s,N'OBJECT',N'UPDATE')",
                (OTHER_OBJECT, OTHER_PRINCIPAL, OTHER_OBJECT),
            )
            visible, principal, write = cursor.fetchone()
            assert visible and principal and write == 0
            for verb in ("INSERT", "UPDATE", "DELETE"):
                cursor.execute(
                    "SELECT HAS_PERMS_BY_NAME(%s,N'OBJECT',%s)",
                    (OTHER_OBJECT, verb),
                )
                assert cursor.fetchone() == (0,)
            cursor.execute(
                "SELECT permission_name "
                "FROM sys.fn_my_permissions(%s,N'OBJECT')",
                (OTHER_OBJECT,),
            )
            assert cursor.fetchall() == [("VIEW DEFINITION",)]
            assert WRITER.permissions_safe(
                cursor,
                installation_id=author["DEMO_SQLSERVER_INSTALLATION_ID"],
            )

        phase = "VERIFY_EMPTY_TVF_PRIMITIVE_NOT_VISIBLE_CASE"
        with WRITER.connection(author) as session:
            cursor = session.cursor()
            for join, expected in (("OUTER", [(None,)]), ("CROSS", [])):
                cursor.execute(
                    "SELECT p.permission_name FROM (VALUES (1)) as seed(id) "
                    + join
                    + " APPLY (SELECT CAST(NULL as nvarchar(128)) "
                    "AS permission_name WHERE seed.id=0) p"
                )
                assert cursor.fetchall() == expected

        phase = "FIXED_DML"
        before = WRITER.diagnose(author)
        assert before["connected"] and before["ownership"]
        assert before["permissions_safe"]
        assert all(
            row["exists"] and row["capture_instance"] is None
            for row in before["tables"].values()
        )
        run_id = "abcdef012345"
        with connect(admin) as cursor:
            assert_committed_product(cursor, run_id, "delete")
            assert_untouched_neighbors(cursor)
        for action in ("insert", "update", "delete"):
            phase = "FIXED_DML_" + action.upper()
            result = WRITER.mutate(author, action, run_id=run_id, consent=True)
            assert result["changed_rows"] == 1 and result["source_only"]
            with connect(admin) as cursor:
                assert_committed_product(cursor, run_id, action)
                assert_untouched_neighbors(cursor)

        phase = "CREATE_BOUNDED_NESTED_ROLE"
        with connect(admin) as cursor:
            cursor.execute(f"CREATE ROLE [{LEGACY_ROLE}] AUTHORIZATION dbo")
            cursor.execute(f"CREATE ROLE [{LEGACY_PARENT}] AUTHORIZATION dbo")
            cursor.execute(
                f"ALTER ROLE [{LEGACY_PARENT}] ADD MEMBER [{LOGIN}]"
            )
            cursor.execute(
                f"ALTER ROLE [{LEGACY_ROLE}] ADD MEMBER [{LEGACY_PARENT}]"
            )
        phase = "SECOND_LIMITED_ROLE_MEMBERSHIP_ALONE"
        with WRITER.connection(author) as session:
            cursor = session.cursor()
            cursor.execute("SELECT IS_MEMBER(%s)", (LEGACY_PARENT,))
            assert cursor.fetchone() == (1,)
            assert WRITER.permissions_safe(
                cursor,
                installation_id=author["DEMO_SQLSERVER_INSTALLATION_ID"],
            )
        phase = "SAME_APPROVED_VERBS_EFFECTIVE_BEFORE_POLICY"
        with connect(admin) as cursor:
            cursor.execute(
                f"GRANT SELECT,INSERT,UPDATE,DELETE ON {OTHER_OBJECT} "
                f"TO [{LEGACY_ROLE}]"
            )
        with WRITER.connection(author) as session:
            cursor = session.cursor()
            for verb in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                cursor.execute(
                    "SELECT HAS_PERMS_BY_NAME(%s,N'OBJECT',%s)",
                    (OTHER_OBJECT, verb),
                )
                assert cursor.fetchone() == (1,)
            cursor.execute(
                "SELECT permission_name FROM sys.fn_my_permissions("
                "%s,N'OBJECT')",
                (OTHER_OBJECT,),
            )
            assert {
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
            } <= {row[0] for row in cursor.fetchall()}
        phase = "OLD_POLICY_REJECTS_SAME_EFFECTIVE_GRANTS"
        assert_fixture_denied(connect, admin, author)
        phase = "INSTALL_DBA_POLICY"
        install_policy(connect, admin, author)
        phase = "APPROVED_NESTED_ROLE_AND_FIXED_DML"
        legacy_run = "789012abcdef"
        before = WRITER.diagnose(author)
        assert before["ownership"] and before["permissions_safe"]
        for action in ("insert", "update", "delete"):
            result = WRITER.mutate(
                author, action, run_id=legacy_run, consent=True
            )
            assert result["changed_rows"] == 1 and result["source_only"]
            with connect(admin) as cursor:
                assert_committed_product(cursor, legacy_run, action)
                assert_untouched_neighbors(cursor)
        phase = "REFUSE_OUTSIDE_POLICY"
        with connect(admin) as cursor:
            cursor.execute(
                f"GRANT UPDATE ON {EXTRA_OBJECT} TO [{LEGACY_ROLE}]"
            )
        assert_fixture_denied(connect, admin, author)
        with connect(admin) as cursor:
            cursor.execute(
                f"REVOKE UPDATE ON {EXTRA_OBJECT} FROM [{LEGACY_ROLE}]"
            )
        phase = "REFUSE_BROAD_SCHEMA_UPDATE"
        with connect(admin) as cursor:
            cursor.execute(f"GRANT UPDATE ON SCHEMA::dbo TO [{LEGACY_ROLE}]")
        with WRITER.connection(author) as session:
            cursor = session.cursor()
            cursor.execute(
                "SELECT HAS_PERMS_BY_NAME(N'dbo',N'SCHEMA',N'UPDATE')"
            )
            assert cursor.fetchone() == (1,)
        assert_fixture_denied(connect, admin, author)
        with connect(admin) as cursor:
            cursor.execute(
                f"REVOKE UPDATE ON SCHEMA::dbo FROM [{LEGACY_ROLE}]"
            )
        phase = "REFUSE_UNAPPROVED_ALTER"
        with connect(admin) as cursor:
            cursor.execute(f"GRANT ALTER ON {OTHER_OBJECT} TO [{LEGACY_ROLE}]")
        assert_fixture_denied(connect, admin, author)
        with connect(admin) as cursor:
            cursor.execute(
                f"REVOKE ALTER ON {OTHER_OBJECT} FROM [{LEGACY_ROLE}]"
            )
        phase = "REFUSE_EXPLICIT_LEGACY_COLUMN_GRANT"
        with connect(admin) as cursor:
            # 表の権限と列の権限は、実際のwriterが継承する別ロールに付く。
            cursor.execute(
                f"GRANT UPDATE ON OBJECT::{OTHER_OBJECT} (note) "
                f"TO [{LEGACY_PARENT}]"
            )
            cursor.execute(
                "SELECT COUNT_BIG(*) FROM sys.database_permissions "
                "WHERE class=1 AND major_id=OBJECT_ID(%s,N'U') "
                "AND minor_id=COLUMNPROPERTY(major_id,N'note',N'ColumnId') "
                "AND minor_id>0 AND permission_name=N'UPDATE' AND state=N'G' "
                "AND grantee_principal_id=DATABASE_PRINCIPAL_ID(%s)",
                (OTHER_OBJECT, LEGACY_PARENT),
            )
            assert cursor.fetchone() == (1,)
        with WRITER.connection(author) as session:
            cursor = session.cursor()
            cursor.execute(
                "SELECT HAS_PERMS_BY_NAME(%s,N'OBJECT',N'UPDATE')",
                (OTHER_OBJECT,),
            )
            assert cursor.fetchone() == (1,)
        assert_fixture_denied(connect, admin, author)
        with connect(admin) as cursor:
            cursor.execute(
                f"REVOKE UPDATE ON OBJECT::{OTHER_OBJECT} (note) "
                f"FROM [{LEGACY_PARENT}]"
            )
        assert WRITER.diagnose(author)["permissions_safe"]
        for grantee, mode in ((LOGIN, "DIRECT"), (LEGACY_ROLE, "NESTED")):
            phase = "REFUSE_WRITER_DELEGATION_" + mode
            with connect(admin) as cursor:
                cursor.execute(
                    f"GRANT UPDATE ON {OTHER_OBJECT} TO [{grantee}] "
                    "WITH GRANT OPTION"
                )
                cursor.execute(
                    "SELECT COUNT_BIG(*) FROM sys.database_permissions "
                    "WHERE class=1 AND major_id=OBJECT_ID(%s,N'U') "
                    "AND minor_id=0 AND permission_name=N'UPDATE' "
                    "AND state=N'W' "
                    "AND grantee_principal_id=DATABASE_PRINCIPAL_ID(%s)",
                    (OTHER_OBJECT, grantee),
                )
                assert cursor.fetchone() == (1,)
            assert_fixture_denied(connect, admin, author)
            with connect(admin) as cursor:
                cursor.execute(
                    f"REVOKE UPDATE ON {OTHER_OBJECT} FROM [{grantee}] CASCADE"
                )
                if grantee == LEGACY_ROLE:
                    cursor.execute(
                        f"GRANT UPDATE ON {OTHER_OBJECT} TO [{LEGACY_ROLE}]"
                    )
            assert WRITER.diagnose(author)["permissions_safe"]
        phase = "REFUSE_POLICY_COLUMN_MUTATION"
        with connect(admin) as cursor:
            cursor.execute(
                f"GRANT UPDATE ON OBJECT::{WRITER.WRITER_POLICY} "
                f"(operation) TO [{LOGIN}]"
            )
        assert_fixture_denied(connect, admin, author)
        with connect(admin) as cursor:
            cursor.execute(
                f"REVOKE UPDATE ON OBJECT::{WRITER.WRITER_POLICY} "
                f"(operation) FROM [{LOGIN}]"
            )
        phase = "REFUSE_DANGEROUS_ROLES"
        for role in ("db_owner", "db_ddladmin", "sysadmin"):
            server = role == "sysadmin"
            kind = "SERVER ROLE" if server else "ROLE"
            database = (
                "master" if server else admin["SOURCE_SQLSERVER_DATABASE"]
            )
            phase = "REFUSE_ROLE_" + role.upper()
            with connect(admin, SOURCE_SQLSERVER_DATABASE=database) as cursor:
                cursor.execute(f"ALTER {kind} [{role}] ADD MEMBER [{LOGIN}]")
            with WRITER.connection(author) as session:
                cursor = session.cursor()
                function = "IS_SRVROLEMEMBER" if server else "IS_MEMBER"
                cursor.execute(f"SELECT {function}(%s)", (role,))
                assert cursor.fetchone() == (1,)
            assert_fixture_denied(connect, admin, author)
            with connect(admin, SOURCE_SQLSERVER_DATABASE=database) as cursor:
                cursor.execute(f"ALTER {kind} [{role}] DROP MEMBER [{LOGIN}]")
            assert WRITER.diagnose(author)["permissions_safe"]
        for verb, target in (
            ("UPDATE", "lh_demo_erp.products"),
            ("INSERT", "lh_demo_ctl.demo_runs"),
        ):
            phase = "REFUSE_MISSING_FIXED_" + verb
            with connect(admin) as cursor:
                cursor.execute(f"REVOKE {verb} ON {target} FROM [{ROLE}]")
            with WRITER.connection(author) as session:
                cursor = session.cursor()
                cursor.execute(
                    "SELECT HAS_PERMS_BY_NAME(%s,N'OBJECT',%s)",
                    (target, verb),
                )
                assert cursor.fetchone() == (0,)
            assert_fixture_denied(connect, admin, author)
            with connect(admin) as cursor:
                cursor.execute(f"GRANT {verb} ON {target} TO [{ROLE}]")
            assert WRITER.diagnose(author)["permissions_safe"]
        phase = "REFUSE_STALE_POLICY_OBJECT_IDENTITY"
        with connect(admin) as cursor:
            cursor.execute(
                f"UPDATE {WRITER.WRITER_POLICY} SET object_created_at="
                "DATEADD(day,-1,object_created_at)"
            )
        assert_fixture_denied(connect, admin, author)
    except Exception:
        raise AssertionError(f"Fixed writer fixture failed: {phase}") from None
    print(
        "Metadata-only visibility, bounded legacy grants, fixed I/U/D and "
        "permissions outside the approved policy verified."
    )

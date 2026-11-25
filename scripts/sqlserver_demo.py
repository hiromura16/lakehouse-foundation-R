"""実行時のDDL権限を付与せず、固定構成の合成SQL Serverデータソースを管理する。

6つの業務表はLandingの契約に対応する。SQLの updated_at はサーバー側の
UTCウォーターマークであり、元の業務上の更新版は original_values に残す。
キー以外の値がSQLの型で表せない場合は型付きNULLにし、元のJSON全体を
Bronzeと隔離処理で使えるようにする。安定した主キーを安全に補えないため、
主キーが欠落または長さを超過した行は拒否する。

オブジェクトの作成と権限の付与はDBA用SQLだけが行う。実行時の書込元は
DMLの前に、設定、所有権、実効権限、明示的な同意を確認する。
ドライバーの例外は機密情報を含まない固定メッセージに置き換える。
"""

import hmac
import json
import math
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
SAFETY_CHECKER = "lh_demo_ctl.check_write_safety_v1"
WRITER_POLICY = "lh_demo_ctl.writer_legacy_policy_v2"
WRITER_DELEGATION = "lh_demo_ctl.check_writer_legacy_delegation_v2"
POLICY_COLLATION = "Latin1_General_100_BIN2"
POLICY_COLUMNS = (
    ("installation_id", "char", 32, POLICY_COLLATION),
    ("writer_sid", "varbinary", 85, None),
    ("writer_principal_id", "int", 4, None),
    ("object_id", "int", 4, None),
    ("schema_id", "int", 4, None),
    ("schema_name", "sysname", 256, POLICY_COLLATION),
    ("table_name", "sysname", 256, POLICY_COLLATION),
    ("object_created_at", "datetime", 8, None),
    ("operation", "varchar", 6, POLICY_COLLATION),
)
POLICY_KEY = ("installation_id", "writer_sid", "object_id", "operation")
POLICY_LIMIT = 64
POLICY_OPERATIONS = frozenset(("SELECT", "INSERT", "UPDATE", "DELETE"))
SAFETY_DEFINITION = (
    Path(__file__)
    .with_name("sqlserver_safety.sql")
    .read_text(encoding="utf-8")
    .rstrip("\n")
)
SCHEMAS = {
    "ERP": "lh_demo_erp",
    "MES": "lh_demo_mes",
    "CONTROL": "lh_demo_ctl",
}
CONTRACTS = json.loads(
    (ROOT / "medallion/sources.json").read_text(encoding="utf-8")
)["sources"]
TABLES = {
    source["table"]: {
        "schema": SCHEMAS[
            "ERP" if source["table"].startswith("erp_") else "MES"
        ],
        "table": source["table"].split("_", 1)[1],
        "columns": source["columns"],
        "pk": source["columns"][0],
    }
    for source in CONTRACTS
}
CAPTURE_INSTANCES = {
    name: item["schema"] + "_" + item["table"] + "_v1"
    for name, item in TABLES.items()
}
SYSTEM_DATABASES = {"master", "model", "msdb", "tempdb"}
# スキーマ内のオブジェクトに対する権限監査の対象型を列挙する。
# 各型で実際に有効な権限は fn_my_permissions で取得する。
# この取得結果にはCLRルーチンとシノニムに対する権限も含まれる。
PERMISSION_OBJECT_TYPES = (
    "U",
    "V",
    "P",
    "PC",
    "X",
    "FN",
    "FS",
    "AF",
    "IF",
    "TF",
    "FT",
    "SN",
    "ET",
    "SO",
    "SQ",
    "R",
    "D",
)
# 子制約、DMLトリガー、内部メタデータには独立したOBJECT権限を付与できない。
# 従来形式の独立した既定値（Dかつ parent=0）は監査対象に含める。
# 未知のユーザーオブジェクト型は拒否し、手動監査を求める。
NON_GRANTABLE_CHILD_TYPES = (
    "C",
    "D",
    "F",
    "PK",
    "UQ",
    "EC",
    "TR",
    "TA",
    "IT",
    "ST",
)
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
RUN_ID = re.compile(r"[0-9a-f]{12}")
TLS_LOCK = threading.RLock()
INTEGER_COLUMNS = {
    "planned_qty",
    "ordered_qty",
    "shipped_qty",
    "good_qty",
    "reject_qty",
}
DECIMAL_COLUMNS = {"standard_cycle_seconds": (18, 3), "unit_price": (18, 2)}
DATE_COLUMNS = {"planned_date", "order_date", "promised_date"}
TIME_COLUMNS = {"event_time", "shipped_at", "start_time", "end_time"}
TEXT_LENGTHS = {
    "product_name": 256,
    "product_family": 128,
    "unit": 32,
    "factory_id": 64,
    "reason_code": 64,
}


class SqlServerError(ValueError):
    """アプリが定義した機密値を含まない失敗メッセージだけを保持する。"""


def endpoint(values, prefix):
    """内部比較用のタプルを返す。APIの応答には含めない。"""
    return (
        values.get(prefix + "_HOST", ""),
        values.get(prefix + "_PORT", "1433"),
        values.get(prefix + "_DATABASE", ""),
    )


def validate_settings(values, *, consent=False, role="demo"):
    """接続と環境設定のコード評価を行わず、固定ソースの境界を検証する。

    各書込操作でも同意を確認する。読取専用の診断ではSQL行の変更に
    同意する前でも設定を調べられる。ソース読取元と制御情報の書込元は
    接続先を共有し、認証情報にはそれぞれ別のものを使う。
    """
    if role not in {"demo", "source", "control"} or type(consent) is not bool:
        raise SqlServerError("Invalid fixed SQL operation role or consent.")
    selected = dict(values)
    for key, schema in SCHEMAS.items():
        setting = "DEMO_SQLSERVER_" + key + "_SCHEMA"
        selected.setdefault(setting, schema)
        if selected[setting] != schema:
            raise SqlServerError(
                "Only the three fixed demo schemas are allowed."
            )
    for key, size in (("INSTALLATION_ID", 32), ("OWNERSHIP_TOKEN", 64)):
        value = selected.get("DEMO_SQLSERVER_" + key, "")
        if not isinstance(value, str) or not re.fullmatch(
            r"[0-9a-f]{" + str(size) + "}", value
        ):
            raise SqlServerError(
                "Configure the fixed demo installation identity."
            )
    shared = selected.get(
        "DEMO_SQLSERVER_ALLOW_SHARED_SOURCE_DATABASE", "false"
    )
    if shared not in {"true", "false"}:
        raise SqlServerError("Shared database flag must be true or false.")
    prefix = "DEMO_SQLSERVER" if role == "demo" else "SOURCE_SQLSERVER"
    for suffix in ("HOST", "DATABASE", "USER", "PASSWORD"):
        key = prefix + "_" + suffix
        if role == "control" and suffix in {"USER", "PASSWORD"}:
            key = "SOURCE_SQLSERVER_CONTROL_" + suffix
        value = selected.get(key, "")
        if (
            not isinstance(value, str)
            or not value
            or any(c in value for c in "\r\n\x00")
        ):
            raise SqlServerError(
                "Complete the selected fixed SQL credentials."
            )
    if role == "control":
        # 比較用の識別情報が欠けると、別のプリンシパルか確認できない。
        # 比較にはユーザー名だけを使う。DEMOの認証情報は、SettingsStoreが渡す
        # バッチ読取元と制御情報の設定から引き続き除外する。
        principals = [
            selected.get(key, "")
            for key in (
                "SOURCE_SQLSERVER_CONTROL_USER",
                "SOURCE_SQLSERVER_USER",
                "DEMO_SQLSERVER_USER",
            )
        ]
        if any(not isinstance(user, str) or not user for user in principals):
            raise SqlServerError(
                "Configure the three distinct fixed SQL principal identities."
            )
        if len({user.casefold() for user in principals}) != 3:
            raise SqlServerError(
                "The control principal must differ from source and demo users."
            )
    host = selected[prefix + "_HOST"]
    database = selected[prefix + "_DATABASE"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,253}", host):
        raise SqlServerError("Invalid fixed SQL host setting.")
    if (
        not IDENTIFIER.fullmatch(database)
        or database.casefold() in SYSTEM_DATABASES
    ):
        raise SqlServerError("Select an existing non-system demo database.")
    port = selected.setdefault(prefix + "_PORT", "1433")
    if (
        not isinstance(port, str)
        or not port.isdigit()
        or not 1 <= int(port) <= 65535
    ):
        raise SqlServerError("Invalid fixed SQL port setting.")
    if (
        role == "demo"
        and database.casefold()
        == selected.get("SOURCE_SQLSERVER_DATABASE", "").casefold()
    ):
        if shared != "true" or endpoint(
            selected, "DEMO_SQLSERVER"
        ) != endpoint(selected, "SOURCE_SQLSERVER"):
            raise SqlServerError(
                "Shared source database is denied without an exact approved "
                "endpoint."
            )
    if selected.get(prefix + "_ENCRYPT", "true") != "true":
        raise SqlServerError("Fixed SQL connections require encryption.")
    trust = selected.get(prefix + "_TRUST_SERVER_CERTIFICATE", "false")
    if trust not in {"true", "false"}:
        raise SqlServerError(
            "SQL certificate trust flag must be true or false."
        )
    certificate = selected.get(prefix + "_CA_FILE", "") or "system"
    if certificate != "system" and (
        not Path(certificate).is_file()
        or any(c in certificate for c in "\r\n[]")
    ):
        raise SqlServerError(
            "SQL CA must be system trust or a regular PEM file."
        )
    selected[prefix + "_CA_FILE"] = certificate
    hostname = selected.get(prefix + "_HOSTNAME_IN_CERTIFICATE", "")
    if hostname and not re.fullmatch(r"[A-Za-z0-9_.-]{1,253}", hostname):
        raise SqlServerError("Invalid expected SQL certificate hostname.")
    return selected


@contextmanager
def connection(values, role="demo"):
    """プロセス内だけで有効なFreeTDS暗号化設定と役割別の認証情報を使う。

    既定では証明書の信頼性を検証する。非公開の概念実証で
    trust=true を明示すると、暗号化は維持したまま証明書チェーンと
    名前の検証を省略する。接続の失敗からこの指定を推定しない。
    ソース読取元は常に read_only=True を指定する。
    変更は呼出元が明示的に確定するまで未コミットのままにする。
    """
    selected = validate_settings(values, role=role)
    prefix = "DEMO_SQLSERVER" if role == "demo" else "SOURCE_SQLSERVER"
    credentials = prefix if role != "control" else "SOURCE_SQLSERVER_CONTROL"
    with (
        TLS_LOCK,
        tempfile.TemporaryDirectory(prefix="fixed-sql-tls-") as name,
    ):
        path = Path(name) / "freetds.conf"
        trust = selected.get(prefix + "_TRUST_SERVER_CERTIFICATE", "false")
        lines = [
            "[global]",
            "    tds version = 7.4",
            "    encryption = require",
        ]
        if trust == "false":
            lines += [
                "    ca file = " + selected[prefix + "_CA_FILE"],
                "    check certificate hostname = yes",
            ]
            hostname = selected.get(prefix + "_HOSTNAME_IN_CERTIFICATE", "")
            if hostname:
                lines.append("    certificate hostname = " + hostname)
        lines += [
            "[lh_fixed_demo]",
            "    host = " + selected[prefix + "_HOST"],
            "    port = " + selected[prefix + "_PORT"],
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        path.chmod(0o600)
        previous = {
            key: os.environ.get(key) for key in ("FREETDS", "FREETDSCONF")
        }
        os.environ.update(FREETDS=str(path), FREETDSCONF=str(path))
        handle = None
        try:
            import pymssql

            handle = pymssql.connect(
                server="lh_fixed_demo",
                port=selected[prefix + "_PORT"],
                database=selected[prefix + "_DATABASE"],
                user=selected[credentials + "_USER"],
                password=selected[credentials + "_PASSWORD"],
                encryption="require",
                read_only=role == "source",
                autocommit=False,
                charset="UTF-8",
                login_timeout=15,
                timeout=60,
                appname="Lakehouse fixed synthetic source",
            )
            yield handle
        except SqlServerError:
            if handle is not None:
                try:
                    handle.rollback()
                except Exception:
                    pass
            raise
        except Exception:
            if handle is not None:
                try:
                    handle.rollback()
                except Exception:
                    pass
            raise SqlServerError(
                "Fixed SQL operation failed; private details withheld."
            ) from None
        finally:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    # 接続を閉じる際の例外で、安全な失敗メッセージが
                    # ドライバーの例外文に置き換わるのを防ぐ。
                    # プロセス内のTLS設定の復元は続ける。
                    pass
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def table_name(contract):
    """管理対象の表名だけを引用する。

    利用者が指定するスキーマ名・表名・SQLは受け取らない。
    """
    if contract not in TABLES:
        raise SqlServerError("Only the six fixed source tables are allowed.")
    item = TABLES[contract]
    return "[" + item["schema"] + "].[" + item["table"] + "]"


def ownership(cursor, values, role="demo"):
    """接続先データベースと変更不可の識別印を一定時間で比較する。"""
    prefix = "DEMO_SQLSERVER" if role == "demo" else "SOURCE_SQLSERVER"
    cursor.execute("SELECT DB_NAME()")
    row = cursor.fetchone()
    if not row or row[0] != values[prefix + "_DATABASE"]:
        raise SqlServerError(
            "The connected database did not match fixed settings."
        )
    cursor.execute("SELECT OBJECT_ID(N'lh_demo_ctl.installation', N'U')")
    if not cursor.fetchone()[0]:
        raise SqlServerError("The fixed demo installation marker is missing.")
    cursor.execute(
        "SELECT schema_version, installation_id, ownership_hash "
        "FROM [lh_demo_ctl].[installation] WHERE singleton_id = 1"
    )
    row = cursor.fetchone()
    expected_hash = sha256(
        values["DEMO_SQLSERVER_OWNERSHIP_TOKEN"].encode("ascii")
    ).hexdigest()
    if (
        not row
        or row[0] != SCHEMA_VERSION
        or not hmac.compare_digest(
            str(row[1]), values["DEMO_SQLSERVER_INSTALLATION_ID"]
        )
        or not hmac.compare_digest(str(row[2]), expected_hash)
    ):
        raise SqlServerError(
            "Fixed demo ownership or schema version did not match."
        )
    return SCHEMA_VERSION


def side_effects_safe(cursor, role="demo"):
    """DBAが設置した所定の読取専用dboメタデータ確認手続きだけを信頼する。

    呼出元のメタデータからは、既存の表からこの表を参照する外部キーが
    見えない場合がある。所定の手続きが未設置、変更済み、または失敗の場合に、
    呼出元のカタログビューへ切り替えることはしない。
    """
    if role == "source":
        return True
    cursor.execute(
        "SELECT m.definition, "
        "CASE WHEN m.execute_as_principal_id=DATABASE_PRINCIPAL_ID(N'dbo') "
        "AND COALESCE(o.principal_id,s.principal_id)="
        "DATABASE_PRINCIPAL_ID(N'dbo') AND m.uses_ansi_nulls=1 "
        "AND m.uses_quoted_identifier=1 THEN 1 ELSE 0 END "
        "FROM sys.sql_modules m JOIN sys.objects o "
        "ON m.object_id=o.object_id JOIN sys.schemas s "
        "ON o.schema_id=s.schema_id "
        "WHERE o.object_id=OBJECT_ID(%s,N'P') AND o.is_ms_shipped=0",
        (SAFETY_CHECKER,),
    )
    row = cursor.fetchone()
    if (
        not row
        or len(row) != 2
        or row[1] != 1
        or not isinstance(row[0], str)
        or row[0].replace("\r\n", "\n") != SAFETY_DEFINITION
    ):
        return False
    cursor.execute("EXEC [lh_demo_ctl].[check_write_safety_v1]", ())
    return cursor.fetchone() == (1,) and cursor.fetchone() is None


def source_permissions_safe(cursor):
    """所定の読取元に許可された境界だけを確認する。

    呼出元はデータベースやサーバーの広範なロールと権限をすでに拒否している。
    データベース全体の最小権限の確認は、DBAによる別の監査で行う。
    """
    cursor.execute("SELECT IS_MEMBER(N'lh_demo_reader')")
    if cursor.fetchone() != (1,):
        return False
    for schema in SCHEMAS.values():
        for operation in ("ALTER", "CONTROL", "TAKE OWNERSHIP"):
            cursor.execute(
                "SELECT HAS_PERMS_BY_NAME(%s,N'SCHEMA',%s)",
                (schema, operation),
            )
            if cursor.fetchone() != (0,):
                return False
    objects = ["lh_demo_ctl.installation"] + [
        item["schema"] + "." + item["table"] for item in TABLES.values()
    ]
    for name in objects:
        cursor.execute(
            "SELECT HAS_PERMS_BY_NAME(%s,N'OBJECT',N'SELECT')", (name,)
        )
        if cursor.fetchone() != (1,):
            return False
        # 実効権限の一覧には、列単位の付与とロールから継承した権限を含む。
        cursor.execute(
            "SELECT permission_name FROM sys.fn_my_permissions(%s,N'OBJECT')",
            (name,),
        )
        permissions = cursor.fetchall()
        if not permissions or any(
            row[0] is None
            or row[0]
            in {
                "INSERT",
                "UPDATE",
                "DELETE",
                "ALTER",
                "CONTROL",
                "TAKE OWNERSHIP",
            }
            for row in permissions
        ):
            return False
        # TOP (0) はデータを開示せず、表と列のSELECT権限を確認する。
        qualified = ".".join("[" + part + "]" for part in name.split("."))
        cursor.execute("SELECT TOP (0) * FROM " + qualified)
        cursor.fetchall()
    return True


def writer_delegation_checker(cursor):
    """権限を委任できないことをdboの完全なカタログから確認する。"""
    definition = (
        Path(__file__)
        .with_name("sqlserver_writer_legacy_delegation.sql")
        .read_text(encoding="utf-8")
        .rstrip("\n")
    )
    cursor.execute(
        "SELECT o.object_id,m.definition FROM sys.sql_modules m "
        "JOIN sys.objects o ON m.object_id=o.object_id "
        "JOIN sys.schemas s ON o.schema_id=s.schema_id "
        "WHERE o.object_id=OBJECT_ID(%s,N'P') AND o.is_ms_shipped=0 "
        "AND m.execute_as_principal_id=DATABASE_PRINCIPAL_ID(N'dbo') "
        "AND s.principal_id=DATABASE_PRINCIPAL_ID(N'dbo') "
        "AND COALESCE(o.principal_id,s.principal_id)="
        "DATABASE_PRINCIPAL_ID(N'dbo') AND m.uses_ansi_nulls=1 "
        "AND m.uses_quoted_identifier=1",
        (WRITER_DELEGATION,),
    )
    row = cursor.fetchone()
    if (
        not row
        or len(row) != 2
        or type(row[0]) is not int
        or row[0] <= 0
        or not isinstance(row[1], str)
        or row[1].replace("\r\n", "\n") != definition
    ):
        return None
    cursor.execute("EXEC [lh_demo_ctl].[check_writer_legacy_delegation_v2]")
    if cursor.fetchone() != (1,) or cursor.fetchone() is not None:
        return None
    return row[0]


def writer_policy(cursor, installation_id):
    """DBAだけが変更できる所定の表から、所定の狭い例外を取得する。

    未設置・不可視なら例外なし。見えるが同一性またはACLが不明なら拒否する。
    設定や利用者の入力から、例外となる対象名や操作を受け取らない。
    """
    cursor.execute("SELECT OBJECT_ID(%s)", (WRITER_POLICY,))
    found = cursor.fetchone()
    if not found or len(found) != 1:
        return None
    policy_id = found[0]
    if policy_id is None:
        return {}
    if type(policy_id) is not int or policy_id <= 0:
        return None
    cursor.execute(
        "SELECT COUNT_BIG(*) FROM sys.tables t JOIN sys.schemas s "
        "ON t.schema_id=s.schema_id WHERE t.object_id=%s "
        "AND t.is_ms_shipped=0 AND t.temporal_type=0 AND t.is_external=0 "
        "AND t.is_memory_optimized=0 "
        "AND s.principal_id=DATABASE_PRINCIPAL_ID(N'dbo') "
        "AND COALESCE(t.principal_id,s.principal_id)="
        "DATABASE_PRINCIPAL_ID(N'dbo')",
        (policy_id,),
    )
    if cursor.fetchone() != (1,):
        return None
    cursor.execute(
        "SELECT c.name,TYPE_NAME(c.user_type_id),c.max_length,"
        "c.collation_name,"
        "c.is_computed,c.is_hidden,c.is_nullable "
        "FROM sys.columns c WHERE c.object_id=%s ORDER BY c.column_id",
        (policy_id,),
    )
    if tuple(cursor.fetchall()) != tuple(
        (*column, 0, 0, 0) for column in POLICY_COLUMNS
    ):
        return None
    cursor.execute(
        "SELECT c.name FROM sys.indexes i JOIN sys.index_columns k "
        "ON i.object_id=k.object_id AND i.index_id=k.index_id "
        "JOIN sys.columns c ON c.object_id=k.object_id "
        "AND c.column_id=k.column_id "
        "WHERE i.object_id=%s AND i.is_primary_key=1 AND i.is_unique=1 "
        "AND i.is_disabled=0 AND k.key_ordinal>0 ORDER BY k.key_ordinal",
        (policy_id,),
    )
    if tuple(row[0] for row in cursor.fetchall()) != POLICY_KEY:
        return None
    cursor.execute(
        "SELECT p.permission_name FROM sys.fn_my_permissions(%s,N'OBJECT') p",
        (WRITER_POLICY,),
    )
    permissions = {row[0] for row in cursor.fetchall()}
    if "SELECT" not in permissions or not permissions <= {
        "SELECT",
        "VIEW DEFINITION",
    }:
        return None
    cursor.execute(
        "SELECT COUNT_BIG(*) FROM sys.columns c WHERE c.object_id=%s AND "
        "(COALESCE(HAS_PERMS_BY_NAME(%s,N'OBJECT',N'UPDATE',"
        "c.name,N'COLUMN'),1)<>0 OR "
        "COALESCE(HAS_PERMS_BY_NAME(%s,N'OBJECT',N'SELECT',"
        "c.name,N'COLUMN'),0)<>1)",
        (policy_id, WRITER_POLICY, WRITER_POLICY),
    )
    if cursor.fetchone() != (0,):
        return None
    cursor.execute(
        "SELECT sid,principal_id FROM sys.database_principals "
        "WHERE principal_id=DATABASE_PRINCIPAL_ID() "
        "AND principal_id>4 AND type='S' AND authentication_type=1 "
        "AND sid=SUSER_SID(ORIGINAL_LOGIN())"
    )
    principal = cursor.fetchone()
    if (
        not principal
        or len(principal) != 2
        or not isinstance(principal[0], bytes)
        or not principal[0]
        or type(principal[1]) is not int
        or principal[1] <= 4
    ):
        return None
    delegation_id = writer_delegation_checker(cursor)
    if delegation_id is None:
        return None
    cursor.execute(
        "SELECT TOP (65) p.installation_id,p.writer_sid,"
        "p.writer_principal_id,p.object_id,"
        "p.schema_id,p.schema_name,p.table_name,"
        "p.object_created_at,p.operation,"
        "t.schema_id,s.name,t.name,t.create_date "
        "FROM [lh_demo_ctl].[writer_legacy_policy_v2] p "
        "LEFT JOIN sys.tables t ON p.object_id=t.object_id "
        "AND t.is_ms_shipped=0 AND t.is_external=0 "
        "AND t.temporal_type=0 AND t.is_memory_optimized=0 "
        "LEFT JOIN sys.schemas s ON t.schema_id=s.schema_id"
    )
    rows = cursor.fetchall()
    if len(rows) > POLICY_LIMIT:
        return None
    admitted = {"SELECT": [policy_id], "EXECUTE": [delegation_id]}
    for row in rows:
        (
            installation,
            sid,
            writer_id,
            target_id,
            schema_id,
            schema,
            target,
            created,
            operation,
            actual_schema_id,
            actual_schema,
            actual_target,
            actual_created,
        ) = row
        if (
            installation != installation_id
            or sid != principal[0]
            or writer_id != principal[1]
            or type(target_id) is not int
            or target_id <= 0
            or type(schema_id) is not int
            or schema_id <= 0
            or schema_id != actual_schema_id
            or not isinstance(schema, str)
            or not IDENTIFIER.fullmatch(schema)
            or schema.casefold()
            in {
                *(name.casefold() for name in SCHEMAS.values()),
                "sys",
                "information_schema",
            }
            or not isinstance(target, str)
            or not IDENTIFIER.fullmatch(target)
            or schema != actual_schema
            or target != actual_target
            or not isinstance(created, datetime)
            or created != actual_created
            or not isinstance(operation, str)
            or operation not in POLICY_OPERATIONS
            or target_id in admitted.get(operation, ())
        ):
            return None
        # 許可するのは明示した表全体の実効操作だけ。列単位の付与は別扱い。
        cursor.execute(
            "SELECT HAS_PERMS_BY_NAME(%s,N'OBJECT',%s)",
            ("[" + schema + "].[" + target + "]", operation),
        )
        if cursor.fetchone() != (1,):
            return None
        admitted.setdefault(operation, []).append(target_id)
    return admitted


def permissions_safe(
    cursor, role="demo", *, full_audit=False, installation_id=None
):
    """広範なロール、危険な権限、管理対象外へのアクセスを検出したら拒否する。

    書込元に以前から付与されている権限は調べるだけで、取り消さない。
    別の制御用プリンシパルには、ウォーターマークのDMLと
    設置情報のSELECTだけを許可する。
    ソース読取元は full_audit の指定時を除き、所定の境界だけを確認する。
    全件監査はDBA向けの個別診断として厳格に実施し、起動時は適用しない。
    メタデータの可視性だけでは、書込元がその対象を操作できるとは限らない。
    """
    writer_runtime = role == "demo" and not full_audit
    roles = [
        "db_owner",
        "db_ddladmin",
        "db_datawriter",
        "db_securityadmin",
        "db_accessadmin",
    ]
    if role != "source":
        roles += ["db_datareader"]
    server_roles = [
        "sysadmin",
        "serveradmin",
        "securityadmin",
        "processadmin",
        "setupadmin",
        "bulkadmin",
        "diskadmin",
        "dbcreator",
    ]
    query = (
        "SELECT "
        + ", ".join("IS_MEMBER(N'" + name + "')" for name in roles)
        + ", "
        + ", ".join(
            "IS_SRVROLEMEMBER(N'" + name + "')" for name in server_roles
        )
    )
    cursor.execute(query)
    result = cursor.fetchone()
    if (
        not result
        or len(result) != len(roles) + len(server_roles)
        or any(value != 0 for value in result)
    ):
        return False
    for scope, dangerous in (
        (
            "DATABASE",
            (
                "CONTROL",
                "ALTER",
                "CREATE TABLE",
                "CREATE SCHEMA",
                "ALTER ANY USER",
                "ALTER ANY ROLE",
                "IMPERSONATE ANY USER",
                "TAKE OWNERSHIP",
            ),
        ),
        (
            "SERVER",
            (
                "CONTROL SERVER",
                "ALTER ANY LOGIN",
                "ALTER ANY SERVER ROLE",
                "IMPERSONATE ANY LOGIN",
            ),
        ),
    ):
        if writer_runtime and scope == "DATABASE":
            dangerous += (
                "ALTER ANY SCHEMA",
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "EXECUTE",
            )
        cursor.execute(
            "SELECT permission_name FROM fn_my_permissions(NULL, %s) "
            "WHERE permission_name IN ("
            + ",".join("%s" for _ in dangerous)
            + ")",
            (scope, *dangerous),
        )
        if cursor.fetchall():
            return False
    if role == "source" and not full_audit:
        return source_permissions_safe(cursor)
    if writer_runtime:
        cursor.execute("SELECT IS_MEMBER(N'lh_demo_writer')")
        if cursor.fetchone() != (1,):
            return False
    admitted = {}
    if writer_runtime and installation_id is not None:
        admitted = writer_policy(cursor, installation_id)
        if admitted is None:
            return False
    business = [
        item["schema"] + "." + item["table"] for item in TABLES.values()
    ]
    selectable = ["lh_demo_ctl.installation"]
    writable = []
    deletable = []
    if role == "demo":
        selectable += [*business, "lh_demo_ctl.demo_runs"]
        writable = [*business, "lh_demo_ctl.demo_runs"]
        deletable = business
    elif role == "control":
        selectable += ["lh_demo_ctl.batch_watermarks"]
        writable = ["lh_demo_ctl.batch_watermarks"]
    allowed_by_operation = {
        "SELECT": selectable,
        "INSERT": writable,
        "UPDATE": writable,
        "DELETE": deletable,
        "EXECUTE": [SAFETY_CHECKER] if role != "source" else [],
        "RECEIVE": [],
        "ALTER": [],
        "CONTROL": [],
        "TAKE OWNERSHIP": [],
    }
    if role == "source":
        allowed_by_operation.pop("SELECT")
    known_types = ",".join(
        "'" + kind + "'" for kind in PERMISSION_OBJECT_TYPES
    )
    child_types = ",".join(
        "'" + kind + "'" for kind in NON_GRANTABLE_CHILD_TYPES
    )
    eligible = (
        "o.is_ms_shipped=0 AND NOT (COALESCE(o.parent_object_id,0)>0 "
        "AND COALESCE(o.type,'') IN (" + child_types + "))"
    )
    # 未対応の型を権限確認済みとみなさず、全監査と同じく拒否する。
    cursor.execute(
        "SELECT TOP (1) o.object_id FROM sys.objects o WHERE "
        + eligible
        + " AND (o.type IS NULL OR o.parent_object_id IS NULL "
        "OR o.type NOT IN (" + known_types + "))"
    )
    if cursor.fetchone():
        return False
    for operation, allowed in allowed_by_operation.items():
        # 権限とオブジェクト型の組合せを推測して HAS_PERMS_BY_NAME を呼ぶと、
        # 未対応の組合せでNULLを返したり、不完全な型一覧からCLRやシノニムの
        # 権限が漏れたりする。列の権限を含めた実効権限を監査する。
        # 不明な実効権限は、承認済み表であっても拒否する。
        clause = "p.permission_name IS NULL OR (p.permission_name=%s"
        arguments = [operation]
        if allowed:
            clause += (
                " AND s.name+N'.'+o.name NOT IN ("
                + ",".join("%s" for _ in allowed)
                + ")"
            )
            arguments += allowed
        if operation in admitted:
            ids = admitted[operation]
            clause += (
                " AND o.object_id NOT IN (" + ",".join("%s" for _ in ids) + ")"
            )
            arguments += ids
        clause += ")"
        cursor.execute(
            "SELECT TOP (1) o.object_id FROM sys.objects o JOIN "
            "sys.schemas s ON o.schema_id=s.schema_id "
            "OUTER APPLY sys.fn_my_permissions(QUOTENAME(s.name)+N'.'+"
            "QUOTENAME(o.name),N'OBJECT') p WHERE "
            + eligible
            + " AND o.type IN ("
            + known_types
            + ")"
            + " AND ("
            + clause
            + ")",
            tuple(arguments),
        )
        if cursor.fetchone():
            return False
    # オブジェクト単位の確認では、列だけに付与された権限を見落とす。
    # ロールから継承した権限や、オブジェクトのDENYに優先する列のGRANTも含む。
    # カタログに直接記録された付与とあわせて、継承を含む実効権限を評価する。
    for operation in ("SELECT", "UPDATE"):
        if operation not in allowed_by_operation:
            continue
        allowed = allowed_by_operation[operation]
        check = (
            "HAS_PERMS_BY_NAME(QUOTENAME(s.name)+N'.'+QUOTENAME(o.name),"
            " N'OBJECT', %s, c.name, N'COLUMN')"
        )
        clause = check + " IS NULL OR (" + check + " = 1"
        arguments = [operation, operation]
        if allowed:
            clause += (
                " AND s.name+N'.'+o.name NOT IN ("
                + ",".join("%s" for _ in allowed)
                + ")"
            )
            arguments += allowed
        if operation in admitted:
            ids = admitted[operation]
            clause += (
                " AND o.object_id NOT IN (" + ",".join("%s" for _ in ids) + ")"
            )
            arguments += ids
        clause += ")"
        cursor.execute(
            "SELECT TOP (1) o.object_id, c.column_id FROM sys.columns c "
            "JOIN sys.objects o ON c.object_id=o.object_id "
            "JOIN sys.schemas s ON o.schema_id=s.schema_id WHERE "
            "o.is_ms_shipped=0 AND o.type IN "
            + ("('U','V','IF','TF')" if operation == "SELECT" else "('U','V')")
            + " AND ("
            + clause
            + ")",
            tuple(arguments),
        )
        if cursor.fetchone():
            return False
    # スキーマ全体への実効DDL権限は、空のスキーマでも許容しない。
    schema_operations = ("ALTER", "CONTROL", "TAKE OWNERSHIP")
    if writer_runtime:
        # 将来追加される表への権限も、固定表別の付与には含まれない。
        schema_operations += (
            "INSERT",
            "UPDATE",
            "DELETE",
            "SELECT",
            "EXECUTE",
        )
    cursor.execute(
        "SELECT TOP (1) s.schema_id FROM sys.schemas s WHERE s.name "
        "NOT IN (N'sys',N'INFORMATION_SCHEMA') AND "
        "("
        + " OR ".join(
            "HAS_PERMS_BY_NAME(s.name,N'SCHEMA',N'" + operation + "')=1"
            for operation in schema_operations
        )
        + ")"
    )
    if cursor.fetchone():
        return False
    cursor.execute(
        "SELECT TOP (1) principal_id FROM sys.database_principals "
        "WHERE principal_id<>DATABASE_PRINCIPAL_ID() "
        "AND type IN ('S','U','G','E','X') AND "
        "HAS_PERMS_BY_NAME(QUOTENAME(name),N'USER',N'IMPERSONATE')=1"
    )
    if cursor.fetchone():
        return False
    required = [("lh_demo_ctl.installation", "SELECT")]
    if role in {"demo", "source"}:
        for item in TABLES.values():
            name = item["schema"] + "." + item["table"]
            required += [
                (name, operation)
                for operation in (
                    ("SELECT", "INSERT", "UPDATE", "DELETE")
                    if role == "demo"
                    else ("SELECT",)
                )
            ]
    if role == "demo":
        required += [
            ("lh_demo_ctl.demo_runs", operation)
            for operation in ("SELECT", "INSERT", "UPDATE")
        ]
    if role == "control":
        required += [
            ("lh_demo_ctl.batch_watermarks", operation)
            for operation in ("SELECT", "INSERT", "UPDATE")
        ]
    for name, operation in required:
        cursor.execute(
            "SELECT HAS_PERMS_BY_NAME(%s, N'OBJECT', %s)", (name, operation)
        )
        if cursor.fetchone() != (1,):
            return False
    if writer_runtime and not writer_columns_safe(cursor):
        return False
    return side_effects_safe(cursor, role)


def writer_columns_safe(cursor):
    """固定DMLが使う列のSELECT/UPDATEは、実効1を必須にする。"""
    required = {
        "lh_demo_ctl.installation": {
            "SELECT": (
                "singleton_id",
                "schema_version",
                "installation_id",
                "ownership_hash",
            ),
        },
        "lh_demo_ctl.demo_runs": {
            "SELECT": ("run_id",),
            "UPDATE": (
                "record_count",
                "completed_at",
                "status",
                "target_mode",
            ),
        },
    }
    for item in TABLES.values():
        required[item["schema"] + "." + item["table"]] = {
            "SELECT": (item["pk"], "run_id", "original_values", "updated_at"),
            "UPDATE": tuple(
                column for column in item["columns"] if column != item["pk"]
            )
            + ("original_values",),
        }
    for name, operations in required.items():
        for operation, columns in operations.items():
            # 1文で複数列のDENY/不可視を検査する。
            cursor.execute(
                "SELECT "
                + ", ".join(
                    "HAS_PERMS_BY_NAME(%s,N'OBJECT',%s,%s,N'COLUMN')"
                    for _ in columns
                ),
                tuple(
                    value
                    for column in columns
                    for value in (name, operation, column)
                ),
            )
            if cursor.fetchone() != (1,) * len(columns):
                return False
    return True


def diagnose(values):
    """許可リストにある状態だけを返し、接続名やドライバーの例外文は返さない。"""
    result = {
        "connected": False,
        "ownership": False,
        "schema_version": 0,
        "permissions_safe": False,
        "tables": {
            name: {
                "exists": False,
                "rows": None,
                "last_updated_at": None,
                "cdc_enabled": False,
                "capture_instance": None,
            }
            for name in TABLES
        },
    }
    try:
        selected = validate_settings(values)
        with connection(selected) as handle:
            result["connected"] = True
            cursor = handle.cursor()
            try:
                result["schema_version"] = ownership(cursor, selected)
                result["ownership"] = True
            except SqlServerError:
                return result
            result["permissions_safe"] = permissions_safe(
                cursor,
                installation_id=selected["DEMO_SQLSERVER_INSTALLATION_ID"],
            )
            for name in TABLES:
                target = table_name(name)
                cursor.execute("SELECT OBJECT_ID(%s, N'U')", (target,))
                exists = bool(cursor.fetchone()[0])
                result["tables"][name]["exists"] = exists
                if not exists:
                    continue
                cursor.execute(
                    "SELECT COUNT_BIG(*), MAX(CONVERT(varchar(33), updated_at,"
                    " 126)) FROM " + target
                )
                count, stamp = cursor.fetchone()
                result["tables"][name].update(
                    rows=int(count),
                    last_updated_at=(stamp + "Z") if stamp else None,
                )
                # どちらのCDCメタデータも書込許可や他の表の診断とは独立。
                try:
                    cursor.execute(
                        "SELECT is_tracked_by_cdc FROM sys.tables WHERE "
                        "object_id=OBJECT_ID(%s)",
                        (target,),
                    )
                    status = cursor.fetchone()
                    if status and status[0] is not None:
                        result["tables"][name]["cdc_enabled"] = bool(status[0])
                except Exception:
                    pass
                try:
                    cursor.execute(
                        "SELECT capture_instance FROM cdc.change_tables WHERE "
                        "source_object_id=OBJECT_ID(%s)",
                        (target,),
                    )
                    captures = {row[0] for row in cursor.fetchall()}
                    if CAPTURE_INSTANCES[name] in captures:
                        result["tables"][name]["capture_instance"] = (
                            CAPTURE_INSTANCES[name]
                        )
                except Exception:
                    pass
    except Exception:
        # 確認できない診断項目はFalseまたはNoneのままにする。
        return result
    return result


def timestamp(value):
    """ISO形式の時刻をUTCへ正規化する際に、小数秒7桁を保つ。"""
    if not isinstance(value, str):
        raise ValueError("Timestamp must be an ISO string.")
    matched = re.fullmatch(
        "(\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d)(?:\\.(\\d{1,"
        "7}))?(Z|[+-]\\d\\d:\\d\\d)",
        value,
    )
    if not matched:
        raise ValueError("Timestamp needs an explicit UTC offset.")
    base, fraction, offset = matched.groups()
    parsed = datetime.fromisoformat(
        base + ("+00:00" if offset == "Z" else offset)
    )
    utc = parsed.astimezone(timezone.utc)
    return (
        utc.strftime("%Y-%m-%dT%H:%M:%S")
        + "."
        + (fraction or "").ljust(7, "0")
    )


def normalized_value(column, value):
    """SQL互換のスカラー値を1つ返し、元の値は別に保持する。"""
    if value is None:
        return None
    try:
        if column in INTEGER_COLUMNS:
            if isinstance(value, bool) or not re.fullmatch(
                r"[+-]?\d+", str(value)
            ):
                return None
            number = int(value)
            return number if -(2**63) <= number < 2**63 else None
        if column in DECIMAL_COLUMNS:
            if isinstance(value, bool):
                return None
            precision, scale = DECIMAL_COLUMNS[column]
            number = Decimal(str(value))
            if not number.is_finite() or abs(number) >= Decimal(10) ** (
                precision - scale
            ):
                return None
            quantum = Decimal(1).scaleb(-scale)
            quantized = number.quantize(quantum)
            return quantized if quantized == number else None
        if column in DATE_COLUMNS:
            return date.fromisoformat(value).isoformat()
        if column in TIME_COLUMNS:
            return timestamp(value)
        text = str(value)
        limit = TEXT_LENGTHS.get(column, 160)
        # SQLのnvarcharの長さはUTF-16コード単位で数える。
        # Pythonのコードポイント数とは異なる。
        return text if len(text.encode("utf-16-le")) // 2 <= limit else None
    except (ValueError, TypeError, InvalidOperation, OverflowError):
        return None


def original_json(row):
    """NULLと小数を保持し、元の値を毎回同じ形式に直列化する。"""

    def scalar(value):
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, float) and not math.isfinite(value):
            raise SqlServerError(
                "Non-finite fixed SQL source values are rejected."
            )
        if value is not None and not isinstance(
            value, (str, int, float, bool)
        ):
            raise SqlServerError("Unsupported fixed SQL source scalar.")
        return value

    return json.dumps(
        {key: scalar(value) for key, value in row.items()},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def prepare_data(data):
    """6つの契約を検証し、同じ主キーの改訂を一定の規則で集約する。"""
    if not isinstance(data, dict) or set(data) != set(TABLES):
        raise SqlServerError(
            "Supply all six fixed source contracts exactly once."
        )
    prepared = {}
    duplicates = normalized = 0
    for name, item in TABLES.items():
        rows = data[name]
        if not isinstance(rows, list) or len(rows) > 100000:
            raise SqlServerError("Fixed source rows must be a bounded list.")
        latest = {}
        for row in rows:
            if not isinstance(row, dict) or set(row) != set(item["columns"]):
                raise SqlServerError(
                    "Fixed source columns did not match their contract."
                )
            key = row[item["pk"]]
            if (
                not isinstance(key, str)
                or not key
                or normalized_value(item["pk"], key) is None
            ):
                raise SqlServerError(
                    "Fixed source needs a nonempty stable primary key."
                )
            payload = original_json(row)
            try:
                revision = timestamp(row["updated_at"])
            except (ValueError, TypeError):
                revision = ""
            candidate = (revision, payload, row)
            if key not in latest or candidate[:2] > latest[key][:2]:
                latest[key] = candidate
        duplicates += len(rows) - len(latest)
        output = []
        for key in sorted(latest):
            _, payload, original = latest[key]
            columns = [
                column for column in item["columns"] if column != "updated_at"
            ]
            typed = {
                column: normalized_value(column, original[column])
                for column in columns
            }
            normalized += sum(
                original[column] is not None and typed[column] is None
                for column in columns
            )
            output.append((key, typed, payload))
        prepared[name] = output
    return prepared, duplicates, normalized


def check_writer(cursor, values):
    """DMLを行う前に、所有権と限定された実効権限を確認する。"""
    ownership(cursor, values)
    if not permissions_safe(
        cursor, installation_id=values["DEMO_SQLSERVER_INSTALLATION_ID"]
    ):
        raise SqlServerError(
            "Fixed demo writer permissions exceed the allowed boundary "
            "or are incomplete."
        )
    cursor.execute("SET XACT_ABORT ON")


def record_run(cursor, run_id, seed, scenario, days, count, target):
    """ソースへの書込と同じトランザクション内で完了を記録する。"""
    cursor.execute(
        "IF NOT EXISTS (SELECT 1 FROM [lh_demo_ctl].[demo_runs] "
        "WITH (UPDLOCK, HOLDLOCK) WHERE run_id=%s) INSERT INTO "
        "[lh_demo_ctl].[demo_runs] (run_id, seed, quality_mode, "
        "record_count, period_days, target_mode, status) VALUES (%s,"
        "%s,%s,%s,%s,%s,N'succeeded'); ELSE UPDATE "
        "[lh_demo_ctl].[demo_runs] SET record_count=%s, "
        "completed_at=SYSUTCDATETIME(), status=N'succeeded', "
        "target_mode=%s WHERE run_id=%s",
        (
            run_id,
            run_id,
            seed,
            scenario,
            count,
            days,
            target,
            count,
            target,
            run_id,
        ),
    )


def write_data(
    data, values, *, run_id, seed=42, scenario="clean", days=7, consent=False
):
    """管理対象の合成行を確定し、再実行で内容が同じ行は処理を省く。"""
    if consent is not True:
        raise SqlServerError(
            "Explicit GUI/API consent is required for fixed SQL writes."
        )
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise SqlServerError(
            "Fixed demo run ID must contain 12 lowercase hex digits."
        )
    if (
        type(seed) is not int
        or not 0 <= seed <= 2147483647
        or scenario not in {"clean", "edge"}
        or type(days) is not int
        or not 1 <= days <= 366
    ):
        raise SqlServerError("Invalid fixed demo generation options.")
    selected = validate_settings(values, consent=consent)
    prepared, duplicates, normalized = prepare_data(data)
    result = {
        "run_id": run_id,
        "source_only": True,
        "counts": {name: len(rows) for name, rows in prepared.items()},
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "pk_deduplicated": duplicates,
        "normalized_values": normalized,
    }
    with connection(selected) as handle:
        cursor = handle.cursor()
        check_writer(cursor, selected)
        for name, rows in prepared.items():
            item = TABLES[name]
            target = table_name(name)
            primary = "[" + item["pk"] + "]"
            for key, typed, payload in rows:
                cursor.execute(
                    "SELECT "
                    + primary
                    + ", run_id, original_values FROM "
                    + target
                    + " WITH (UPDLOCK,HOLDLOCK) WHERE "
                    + primary
                    + "=%s",
                    (key,),
                )
                prior = cursor.fetchone()
                # SQLの照合は、大文字小文字、アクセント、末尾の空白が
                # 異なる値を同じと判定する場合がある。
                # 値が異なる物理主キーの行は更新せず、original_values に
                # 別のキーが記録されるのを防ぐ。
                if prior and prior[0] != key:
                    raise SqlServerError(
                        "A SQL-collation key collision was refused; existing "
                        "data was preserved."
                    )
                if prior and prior[1] != run_id:
                    raise SqlServerError(
                        "A fixed source key belongs to another run; existing "
                        "data was preserved."
                    )
                if prior and prior[2] == payload:
                    result["skipped"] += 1
                    continue
                if prior:
                    changed = {
                        column: value
                        for column, value in typed.items()
                        if column != item["pk"]
                    }
                    cursor.execute(
                        "UPDATE "
                        + target
                        + " SET "
                        + ",".join("[" + column + "]=%s" for column in changed)
                        + ",original_values=%s,updated_at=SYSUTCDATETIME() "
                        "WHERE " + primary + "=%s AND run_id=%s",
                        (*changed.values(), payload, key, run_id),
                    )
                    result["updated"] += 1
                else:
                    columns = [*typed, "run_id", "original_values"]
                    cursor.execute(
                        "INSERT INTO "
                        + target
                        + " ("
                        + ",".join("[" + column + "]" for column in columns)
                        + ") VALUES ("
                        + ",".join("%s" for _ in columns)
                        + ")",
                        (*typed.values(), run_id, payload),
                    )
                    result["inserted"] += 1
        record_run(
            cursor,
            run_id,
            seed,
            scenario,
            days,
            sum(result["counts"].values()),
            "fixed-sqlserver",
        )
        handle.commit()
    return result


def mutate(values, action, *, run_id, consent=False):
    """この実行のために決まるCDC検証用の商品だけにI/U/Dを適用する。"""
    if (
        consent is not True
        or not isinstance(action, str)
        or action not in {"insert", "update", "delete"}
    ):
        raise SqlServerError(
            "A fixed CDC action and explicit consent are required."
        )
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise SqlServerError(
            "Fixed CDC run ID must contain 12 lowercase hex digits."
        )
    selected = validate_settings(values, consent=consent)
    key = "CDC-" + run_id
    target = table_name("erp_products")
    with connection(selected) as handle:
        cursor = handle.cursor()
        check_writer(cursor, selected)
        cursor.execute(
            "SELECT product_id, run_id, original_values FROM "
            + target
            + " WITH (UPDLOCK,HOLDLOCK) WHERE product_id=%s",
            (key,),
        )
        previous = cursor.fetchone()
        if previous and previous[0] != key:
            raise SqlServerError(
                "A SQL-collation CDC key collision was refused; existing "
                "data was preserved."
            )
        if previous and previous[1] != run_id:
            raise SqlServerError(
                "The CDC test product is not owned by this run."
            )
        changed = 0
        if action == "delete":
            if previous:
                cursor.execute(
                    "DELETE FROM "
                    + target
                    + " WHERE product_id=%s AND run_id=%s",
                    (key, run_id),
                )
                changed = 1
        else:
            if action == "update" and not previous:
                raise SqlServerError(
                    "Insert the owned CDC test product before updating it."
                )
            if action == "insert" and previous:
                return {
                    "action": action,
                    "changed_rows": 0,
                    "run_id": run_id,
                    "source_only": True,
                }
            row = {
                "product_id": key,
                "product_name": "合成CDC"
                + ("更新" if action == "update" else "投入"),
                "product_family": "SYNTHETIC",
                "unit": "piece",
                "standard_cycle_seconds": "30.000",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            payload = original_json(row)
            if previous:
                cursor.execute(
                    "UPDATE "
                    + target
                    + " SET product_name=%s, original_values=%s, "
                    "updated_at=SYSUTCDATETIME() WHERE product_id=%s AND "
                    "run_id=%s",
                    (row["product_name"], payload, key, run_id),
                )
            else:
                cursor.execute(
                    "INSERT INTO "
                    + target
                    + " (product_id,product_name,product_family,unit,"
                    "standard_cycle_seconds,run_id,original_values) VALUES "
                    "(%s,%s,%s,%s,%s,%s,%s)",
                    (
                        key,
                        row["product_name"],
                        row["product_family"],
                        row["unit"],
                        Decimal("30.000"),
                        run_id,
                        payload,
                    ),
                )
            changed = 1
        record_run(
            cursor, run_id, 0, "clean", 1, changed, "fixed-cdc-" + action
        )
        handle.commit()
    return {
        "action": action,
        "changed_rows": changed,
        "run_id": run_id,
        "source_only": True,
    }

"""固定CDCの対象を、管理対象6表と読取専用のプリンシパルに限定する。

登録前の確認で実行するのは、メタデータを取得するSELECTだけ。
CDCの有効化、権限変更、SQLオブジェクトの作成、完了位置の記録は行わない。
SQL ServerのDDLはDBAが別途内容を確認したうえで実行する。
"""

import importlib
import re
import sys
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if not (ROOT / "medallion").is_dir():
    ROOT = Path(__file__).resolve().parents[1]
FIXED_PREFIX = "lakehouse-demo-sqlserver"
FIXED_GROUP = FIXED_PREFIX + "-consumer"
EXTRA_COLUMNS = ("original_values", "run_id", "created_at")


@lru_cache(maxsize=1)
def source_adapter():
    """共通の所有権と読取権限の確認を、別のドライバーを使わずに読み込む。"""
    scripts = str(ROOT / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    return importlib.import_module("sqlserver_demo")


def select_settings(values):
    """SOURCE設定からCDC内部の名前を作る。書込用の認証情報は使わない。

    非公開の環境設定で指定する項目は引き続き SOURCE_SQLSERVER_* とする。
    CDC内部の名前により、既存の汎用コンシューマーでも同じソース識別情報を
    比較できる。コネクターの認証情報にはSOURCE環境変数への参照を維持する。
    """
    certificate = values.get("SOURCE_SQLSERVER_CA_FILE", "")
    if certificate == "/run/source-certs/source-ca.pem" and (
        not Path(certificate).is_file()
        or Path(certificate).stat().st_size == 0
    ):
        raise ValueError(
            "Mount the approved SOURCE PEM with a private Compose override; "
            "the empty CDC placeholder cannot provide certificate trust."
        )
    if (
        certificate not in {"", "system"}
        and values.get("SOURCE_SQLSERVER_TRUST_SERVER_CERTIFICATE", "false")
        == "false"
        and not values.get("CDC_SQLSERVER_TRUSTSTORE")
    ):
        raise ValueError(
            "Custom SOURCE PEM does not configure Java trust; also select "
            "the approved CDC SQL Server truststore."
        )
    selected = source_adapter().validate_settings(values, role="source")
    if selected.get("SOURCE_SQLSERVER_TIMEZONE", "UTC") != "UTC":
        raise ValueError(
            "Fixed CDC requires the approved UTC source contract."
        )
    selected.update(
        CDC_TOPIC_PREFIX=FIXED_PREFIX,
        CDC_CONNECTOR_NAME=FIXED_PREFIX,
        CDC_GROUP_ID=FIXED_GROUP,
        CDC_TEST_TRUST_SERVER_CERTIFICATE="false",
    )
    for suffix in ("HOST", "PORT", "DATABASE", "USER", "PASSWORD"):
        selected["CDC_SQLSERVER_" + suffix] = selected.get(
            "SOURCE_SQLSERVER_" + suffix, "1433" if suffix == "PORT" else ""
        )
    return selected


def table_mapping():
    """要求や非公開の対応表を入力にせず、固定の対応表を返す。"""
    return {
        name: {
            "target_table": name,
            "source_schema": item["schema"],
            "source_table": item["table"],
            "columns": {column: column for column in item["columns"]},
        }
        for name, item in source_adapter().TABLES.items()
    }


def validate_mapping(mapping):
    """内部呼出しでも、表の差し替えや追加を拒否する。"""
    if mapping != table_mapping():
        raise ValueError("Fixed CDC accepts only the six owned demo tables.")


def preflight(values, mapping, *, adapter=None):
    """接続先、読取権限の境界、CDCキャプチャー定義の一致を確認する。

    成功はこの接続で確認した結果を示す。Kafka、SQL Server Agent、または
    レイクハウス全体でイベントが処理されたかは別途確認する。
    接続したプリンシパルに必要なメタデータが見えない場合は拒否する。
    """
    adapter = source_adapter() if adapter is None else adapter
    validate_mapping(mapping)
    selected = adapter.validate_settings(values, role="source")
    try:
        with adapter.connection(selected, role="source") as handle:
            cursor = handle.cursor()
            adapter.ownership(cursor, selected, role="source")
            if not adapter.permissions_safe(cursor, role="source"):
                raise ValueError("Fixed CDC reader permissions are unsafe.")
            cursor.execute(
                "SELECT is_cdc_enabled FROM sys.databases "
                "WHERE database_id=DB_ID()"
            )
            if cursor.fetchone() != (True,):
                raise ValueError("Fixed source database CDC is not ready.")
            cursor.execute("SELECT IS_MEMBER(N'lh_demo_cdc_reader')")
            if cursor.fetchone() != (1,):
                raise ValueError("Fixed CDC reader role is missing.")
            cursor.execute(
                "SELECT HAS_PERMS_BY_NAME(DB_NAME(),N'DATABASE',"
                "N'VIEW DATABASE STATE')"
            )
            if cursor.fetchone() != (1,):
                raise ValueError("Fixed CDC database state is unavailable.")
            for metadata in (
                "change_tables",
                "captured_columns",
                "lsn_time_mapping",
            ):
                cursor.execute(
                    "SELECT TOP (0) * FROM [cdc].[" + metadata + "]"
                )
                cursor.fetchall()
            for name, item in adapter.TABLES.items():
                capture = adapter.CAPTURE_INSTANCES[name]
                qualified = item["schema"] + "." + item["table"]
                for object_name in (
                    capture + "_CT",
                    "fn_cdc_get_all_changes_" + capture,
                ):
                    cursor.execute(
                        "SELECT HAS_PERMS_BY_NAME(%s,N'OBJECT',N'SELECT')",
                        ("cdc." + object_name,),
                    )
                    if cursor.fetchone() != (1,):
                        raise ValueError(
                            "Fixed CDC read permission is missing."
                        )
                cursor.execute(
                    "SELECT TOP (0) * FROM [cdc].[" + capture + "_CT]"
                )
                cursor.fetchall()
                cursor.execute(
                    "SELECT TOP (0) * FROM [cdc].[fn_cdc_get_all_changes_"
                    + capture
                    + "](NULL,NULL,N'all')"
                )
                cursor.fetchall()
                cursor.execute(
                    "SELECT ct.capture_instance,ct.role_name "
                    "FROM cdc.change_tables ct JOIN sys.tables t ON "
                    "ct.source_object_id=t.object_id WHERE "
                    "t.object_id=OBJECT_ID(%s) AND t.is_tracked_by_cdc=1",
                    (qualified,),
                )
                if cursor.fetchall() != [(capture, "lh_demo_cdc_reader")]:
                    raise ValueError("Fixed CDC capture ownership differs.")
                cursor.execute(
                    "SELECT cc.column_name FROM cdc.captured_columns cc "
                    "JOIN cdc.change_tables ct ON cc.object_id=ct.object_id "
                    "WHERE ct.capture_instance=%s",
                    (capture,),
                )
                captured = [row[0] for row in cursor.fetchall()]
                expected = [*item["columns"], *EXTRA_COLUMNS]
                if len(captured) != len(expected) or set(captured) != set(
                    expected
                ):
                    raise ValueError("Fixed CDC captured columns differ.")
                # JsonConverterではスキーマを無効にしている。
                # 型付きタイムスタンプは、消費側でナノ秒単位と解釈する。
                cursor.execute(
                    "SELECT c.name,ty.name,c.scale FROM sys.columns c "
                    "JOIN sys.types ty ON c.user_type_id=ty.user_type_id "
                    "WHERE c.object_id=OBJECT_ID(%s)",
                    (qualified,),
                )
                types = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
                timestamp_columns = {"updated_at", "created_at"} | (
                    set(item["columns"]) & adapter.TIME_COLUMNS
                )
                if any(
                    types.get(column) != ("datetime2", 7)
                    for column in timestamp_columns
                ):
                    raise ValueError("Fixed CDC timestamp precision differs.")
                cursor.execute("SELECT IS_MEMBER(N'lh_demo_cdc_reader')")
                if cursor.fetchone() != (1,):
                    raise ValueError("Fixed CDC reader role is missing.")
    except Exception:
        # ドライバーの例外には、SQLの名前、認証情報、応答文が含まれ得る。
        raise ValueError(
            "Fixed CDC preflight failed; review ownership, read-only "
            "permissions and the six captures privately."
        ) from None
    return {
        "checked": True,
        "tables": 6,
        "schema_version": 1,
        "agent_jobs_checked": False,
    }


def validate_state_volume(value):
    """確認済みのボリューム名を任意で許可し、パスとしては扱わない。"""
    if value and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise ValueError("Invalid CDC state volume name.")

"""Debezium形式の合成CDCをIceberg V2で照合し、障害復旧を試験する。"""

import json
from datetime import UTC, datetime
from subprocess import TimeoutExpired

from fixture import ROOT, bootstrap, compose, sql, wait_for

TOPIC = "fixture-cdc-debezium-v2"
FIELDS = ("id", "plant", "quantity", "revision")
COLUMNS = ", ".join(FIELDS)
SINKS = {
    "mor_bucket": "primary_key='id', partition_by='bucket(4,id)',",
    # PK-indexは上流の主キーを引き継ぐ。primary_keyの指定は拒否される。
    "pk_position": "enable_pk_index='true',",
}
INSERTED = [[1, "north", 12, 1], [2, "south", 20, 1], [3, "north", 30, 1]]
CHANGED = [[1, "west", 15, 2], INSERTED[2]]
RECOVERED = [[3, "north", 44, 2]]


def initialize():
    """同じKafka変更系列を、専用warehouseの別々のV2表へ接続する。"""
    bootstrap()
    wait_for(lambda: sql("SELECT 1;", timeout=5), "RisingWave SQL")
    compose(
        "exec",
        "-T",
        "broker",
        "rpk",
        "cluster",
        "health",
        "--exit-when-healthy",
    )
    compose(
        "exec",
        "-T",
        "broker",
        "rpk",
        "topic",
        "create",
        TOPIC,
        "--partitions",
        "1",
        "--replicas",
        "1",
    )
    sql(f"""
CREATE CONNECTION cdc_rest WITH (
    type='iceberg', catalog.type='rest_rust',
    catalog.uri='http://catalog:8181/catalog',
    warehouse.path='fixture-warehouse', s3.endpoint='http://storage:8333',
    s3.region='us-east-1', s3.path.style.access='true'
);
CREATE TABLE cdc_input (
    id INTEGER PRIMARY KEY, plant VARCHAR, quantity INTEGER, revision INTEGER
) WITH (
    connector='kafka', topic='{TOPIC}',
    properties.bootstrap.server='broker:9092', scan.startup.mode='earliest'
) FORMAT DEBEZIUM ENCODE JSON;
""")
    for target, options in SINKS.items():
        # SETは接続単位なので、シンク作成のSQLと同じセッションで設定する。
        sql(f"""
SET sink_decouple = true;
CREATE SINK {target} AS SELECT {COLUMNS} FROM cdc_input
WITH (
    connector='iceberg', type='upsert', connection=cdc_rest,
    database.name='fixture', table.name='{target}',
    write_mode='merge-on-read', {options}
    create_table_if_not_exists='true', format_version='2',
    commit_checkpoint_interval='1', is_exactly_once='true',
    enable_snapshot_expiration='false'
);
""")
    names = dict(
        row.split("|", 1)
        for row in sql("SELECT id,name FROM rw_catalog.rw_sinks;").splitlines()
    )
    decouple = {
        names[key]: value == "t"
        for key, value in (
            row.split("|", 1)
            for row in sql(
                "SELECT sink_id,is_decouple FROM rw_sink_decouple;"
            ).splitlines()
        )
    }
    assert decouple == {"mor_bucket": True, "pk_position": False}, decouple
    print(
        json.dumps({"phase": "settings", "rw_sink_decouple": decouple}),
        flush=True,
    )


def publish(changes):
    """更新前後を含むDebeziumメッセージをキー付きで送る。"""
    messages = []
    for before, after in changes:
        key = {"id": (before or after)[0]}
        envelope = {
            "op": "c" if before is None else ("d" if after is None else "u"),
            "before": (
                dict(zip(FIELDS, before, strict=True)) if before else None
            ),
            "after": dict(zip(FIELDS, after, strict=True)) if after else None,
            "source": {
                "version": "fixture",
                "connector": "postgresql",
                "db": "fixture",
                "schema": "public",
                "table": "changes",
                "lsn": 1,
            },
            "ts_ms": 1770000000000,
        }
        messages.append(f"{json.dumps(key)}\t{json.dumps(envelope)}\n")
    compose(
        "exec",
        "-T",
        "broker",
        "rpk",
        "topic",
        "produce",
        TOPIC,
        "--format",
        r"%k\t%v\n",
        input_text="".join(messages),
    )


def snapshots():
    """独立読取プロセスでIcebergの行、削除方式、世代を取得する。"""
    return json.loads(
        compose(
            "run",
            "--no-deps",
            "--rm",
            "-T",
            "--volume",
            f"{ROOT / 'extensions'}:/opt/upsert-extensions:ro",
            "reader",
            "upsert",
            timeout=45,
        )
    )


def verify(phase, expected, *, has_deletes):
    """固定した読取版の成功と既知の非対応を、それぞれ期待値と照合する。"""
    expected_sql = ["|".join(map(str, row)) for row in expected]

    def read_committed():
        """再起動直後の一時エラーとIcebergの確定待ちを許す。"""
        sql("FLUSH;", timeout=10)
        state = snapshots()
        assert state["pyiceberg_version"] == "0.12.0", state
        assert state["duckdb_version"] == "v2.0.1", state
        assert state["tables"].keys() == SINKS.keys(), state
        for name, observation in state["tables"].items():
            assert observation["version"] == 2, observation
            assert observation["duckdb"] == expected, observation
            if name == "mor_bucket":
                assert observation["partition"] == ["bucket[4]"], observation
            if has_deletes and name == "mor_bucket":
                expected_ids = [[observation["id_field"]]]
                assert observation["equality_ids"] == expected_ids, observation
                assert not observation["position_deletes"], observation
                assert (
                    observation["pyiceberg"] == "equality-delete-unsupported"
                ), observation
            else:
                assert not observation["equality_ids"], observation
                assert observation["pyiceberg"] == expected, observation
                assert bool(observation["position_deletes"]) == has_deletes
            # Rust読取はIceberg外部表を使う。取込元の同じ行で代用しない。
            source = f"iceberg_read_{name}"
            sql(f"""
CREATE SOURCE IF NOT EXISTS {source}
WITH (
    connector='iceberg', connection=cdc_rest,
    database.name='fixture', table.name='{name}'
);
""")
            result = sql(f"SELECT {COLUMNS} FROM {source} ORDER BY id;")
            assert result.splitlines() == expected_sql, (source, result)
        return state["tables"]

    state = wait_for(read_committed, f"Iceberg upsert {phase}", seconds=100)
    print(json.dumps({"phase": phase, "tables": state}), flush=True)
    return state


def catalog_failure(previous):
    """RESTに接続できない期間を挟み、同じチェックポイントで再開する。"""
    start = datetime.now(UTC).isoformat()
    compose("stop", "-t", "2", "catalog")
    publish([(CHANGED[0], None), (CHANGED[1], RECOVERED[0])])

    def failed_checkpoint():
        """RisingWaveが実際にRESTカタログ由来の失敗を観測するまで待つ。"""
        try:
            sql("FLUSH;", timeout=10)
        except (RuntimeError, TimeoutExpired):
            # 復旧中はFLUSHが一時エラーを返す。原因は下のログで確認する。
            pass
        logs = compose("logs", "--no-color", "--since", start, "risingwave")
        assert any(
            "http://catalog:8181/" in line
            and "iceberg" in line.lower()
            and (
                "Connection refused" in line
                or "failed to lookup address information" in line
            )
            for line in logs.splitlines()
        ), "Iceberg RESTへの接続失敗がまだ観測されていません。"

    wait_for(failed_checkpoint, "Lakekeeper停止時の障害観測", seconds=45)
    print(json.dumps({"phase": "catalog-unreachable"}), flush=True)
    compose("kill", "-s", "SIGKILL", "risingwave")
    compose("start", "catalog")
    unchanged = wait_for(snapshots, "Lakekeeper単独の復帰")["tables"]
    for name, before in previous.items():
        assert unchanged[name] == before, (name, before, unchanged[name])
    print(json.dumps({"phase": "catalog-head-unchanged"}), flush=True)

    compose("start", "risingwave")
    replayed = verify("catalog-recovered", RECOVERED, has_deletes=True)
    for name, after in replayed.items():
        before = previous[name]
        assert after["sequence"] > before["sequence"], (name, after)
        assert set(before["history"]) < set(after["history"]), (name, after)


def main():
    """一回の隔離実行で初期投入・更新削除・停止からの復旧を確認する。"""
    initialize()
    publish([(None, row) for row in INSERTED])
    verify("insert", INSERTED, has_deletes=False)
    publish([(INSERTED[0], CHANGED[0]), (INSERTED[1], None)])
    changed = verify("update-delete", CHANGED, has_deletes=True)
    catalog_failure(changed)


if __name__ == "__main__":
    main()

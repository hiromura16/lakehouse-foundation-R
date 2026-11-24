-- 専用Kafkaの非NULL計測メッセージを、解釈前のバイト列のまま保管する。
-- 評価環境のfixture_restへ接続する。資格情報は接続設定側で管理する。
CREATE SOURCE IF NOT EXISTS fixture_telemetry_wire (payload BYTEA)
INCLUDE key AS kafka_key
INCLUDE partition AS source_partition
INCLUDE offset AS source_offset
INCLUDE timestamp AS kafka_time
WITH (
    connector = 'kafka',
    topic = 'fixture-telemetry-wire-v1',
    properties.bootstrap.server = 'broker:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE BYTES;

-- Kafkaの記録時刻と、ペイロードに含まれる設備の実測時刻を区別する。
SET sink_decouple = true;
CREATE SINK IF NOT EXISTS fixture_telemetry_archive AS
SELECT source_partition::INTEGER AS kafka_partition,
       source_offset::BIGINT AS kafka_offset,
       kafka_time, kafka_key, payload
FROM fixture_telemetry_wire
WITH (
    connector = 'iceberg',
    type = 'append-only',
    connection = fixture_rest,
    database.name = 'fixture',
    table.name = 'telemetry_wire_v2',
    write_mode = 'merge-on-read',
    create_table_if_not_exists = 'true',
    format_version = '2',
    commit_checkpoint_interval = '1',
    is_exactly_once = 'true',
    enable_snapshot_expiration = 'false'
);

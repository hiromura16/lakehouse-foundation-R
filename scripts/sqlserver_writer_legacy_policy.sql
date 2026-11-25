-- DBAによる任意の導入。運用CLIはこれを実行しない。
-- 既存の dbo 所有 lh_demo_ctl スキーマと固定インストールを先に確認する。
-- このファイルは表の作成だけを行う。権限付与・具体的な許可は含めない。
CREATE TABLE [lh_demo_ctl].[writer_legacy_policy_v2] (
    installation_id char(32) COLLATE Latin1_General_100_BIN2 NOT NULL,
    writer_sid varbinary(85) NOT NULL,
    writer_principal_id int NOT NULL,
    object_id int NOT NULL,
    schema_id int NOT NULL,
    schema_name sysname COLLATE Latin1_General_100_BIN2 NOT NULL,
    table_name sysname COLLATE Latin1_General_100_BIN2 NOT NULL,
    object_created_at datetime NOT NULL,
    operation varchar(6) COLLATE Latin1_General_100_BIN2 NOT NULL,
    CONSTRAINT PK_writer_legacy_policy_v2
        PRIMARY KEY (installation_id, writer_sid, object_id, operation),
    CONSTRAINT CK_writer_legacy_policy_v2_verb
        CHECK (operation IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')),
    CONSTRAINT CK_writer_legacy_policy_v2_ids
        CHECK (writer_principal_id>4 AND object_id>0 AND schema_id>0)
);

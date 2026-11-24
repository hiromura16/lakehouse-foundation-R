# ファイル・SQL Server・Kafkaの入力契約

<!-- doc-purpose:start -->
> **役割:** ファイル・SQLバッチ・Kafka・設備の入力契約を確認します。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 外部ソースを接続するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [認証付きAPIとLangChain](<API.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## ファイル受け渡し

1. 投入側で一時名へ書き込み、書き終わったら同じファイルシステム内で確定名へ移動します。
2. 確定ファイルのSHA-256を計算し、隣の `<ファイル名>.ready` へ64桁の小文字と改行を保存します。完了印を最後に確定します。
3. `python -m native.api medallion` をLakehouseコンテナ内で実行します。原本と印は取込み完了まで保持します。

6ソースのパスと列は [sources.json](../../medallion/sources.json) にあります。 `csv/erp_products__<12桁の小文字hex>_<4桁の連番>.csv` のような増分名も認識します。見覚えのない拡張子や名前はファイル原本として保存し、任意の業務表には割り当てません。大文字JSONキーも含め列名は契約と一致させます。同じパスとhashは取込済みとして扱います。

送信アプリの実装や配備は送信側で管理します。Foundation Rが受け取るのは、型と列を定めたファイルと隣接する `.ready` です。JSONの完了マニフェストを使う送信側では、受け渡しアダプターで原本と整合性を検証してから、この形式の完了印を出力します。

## 設備時系列JSONL

`sensor/<名前>.jsonl` にUTF-8のJSONを1行1点で置き、同じSHA-256完了印を付けます。 [設備契約](../../contracts/native/sensor-event-v1.json) に規則があります。

```json
{"factory_id":"F01","line_id":"L01","equipment_id":"M01","sensor_id":"T01","measurement":"temperature","unit":"C","timestamp":"2026-01-05T12:00:00Z","sequence_no":1,"value":24.5,"quality_code":"GOOD"}
```

原本はS3、型付けした履歴は `silver.sensor_history`、最新値とUTCの1分集計は `gold.sensor_latest` と `gold.sensor_minute` に保存します。系列キーは工場・ライン・設備・センサー・計測種別・単位です。同じ系列と時刻・sequenceの完全一致は1点にまとめ、値や品質が衝突するとそのキーを隔離します。1分集計では全点数とGOOD数を持ち、最小・最大・平均はGOODだけを対象にします。異常値や衝突は `quarantine.sensor_events` に原本hashと行番号を残します。

## SQL Serverからファイルを抽出する

会社のSQL Serverに接続できるホストで、対象6表のSELECTを許可された専用IDを用意します。マッピングは [表・列の対応例](../../infra/source/sqlserver-tables.example.json) をcheckout外へコピーし、実際の表・列へ置き換えます。資格情報はcheckout外の0600の設定ファイルへ保存します。主要項目は `SOURCE_SQLSERVER_HOST`、`SOURCE_SQLSERVER_DATABASE`、`SOURCE_SQLSERVER_USER`、`SOURCE_SQLSERVER_PASSWORD`、`SOURCE_SQLSERVER_PORT`、`SOURCE_SQLSERVER_CA_FILE`、`SOURCE_SQLSERVER_TIMEZONE` です。

**Bash / Zsh共通**

```bash
python3 -m venv "$HOME/.config/lakehouse-foundation-r/sql-source-venv"
"$HOME/.config/lakehouse-foundation-r/sql-source-venv/bin/pip" install -r requirements-demo.txt
source_env="$HOME/.config/lakehouse-foundation-r/sql-source.env"
source_map="$HOME/.config/lakehouse-foundation-r/sql-tables.json"
landing="$HOME/.config/lakehouse-foundation-r/landing"
site="$HOME/.config/lakehouse-foundation-r/site.env"
"$HOME/.config/lakehouse-foundation-r/sql-source-venv/bin/python" \
  scripts/extract-sqlserver.py --env-file "$source_env" --table-map "$source_map" \
  --output "$landing"
docker compose --env-file "$site" exec -T lakehouse python -m native.api medallion
```

抽出は固定列を読むSELECTだけを実行し、landing直下の `csv/`・`json/` と完了印を確定します。6表を同一時点で取得するトランザクションは使いません。会社の証明書検証を有効にし、出力済みの同じパスを置き換える場合は `--force` の対象を確認します。[抽出CLI](../../scripts/extract-sqlserver.py)

## 固定SQLバッチをAPIで受け取る

上流の固定SQL観測が生成した `<batch_id>.json` と完了印を `sql-batches/` に置く経路です。 `POST /run` に操作トークンと `batch_id`、`observed_at` を送ります。ファイル抽出CLIの出力は上記の通常取込に渡します。[操作APIとSQLバッチ例](API.md)

業務出力は `sql_bronze`、`sql_silver`、`sql_quarantine`、`sql_gold` に分かれます。確認するには `GET /v1/sql/gold/sales_fulfillment` を使用します。取り込み前に実DBの権限・SQL型・水位の意味を確認してください。

## Kafka互換ブローカーでCDCを取り込む

外部のDebeziumがSQL Server CDCのJSONを発行している環境向けの任意構成です。コネクターは外部のKafka Connectで管理します。標準RのComposeとconsumerにはJavaが不要です。Debezium Kafka Connectの実行環境にはJVMが必要です。

1. Kafka管理者が専用consumer groupのACLと対象6 topicへの読取権限を用意します。初回に必要な履歴を確保し、topicの保持期間と鍵ごとのpartition整合性を確認します。
2. [設定例](../../native/kafka-settings.example.json) をcheckout外のprivateファイルへコピーして、broker、DB、topic prefix、`lakehouse-r-` から始まる専用groupを入力します。実接続ではTLSまたはSASL_SSLを使い、brokerが広告する全アドレスへ直接接続できるようにします。OSの信頼ストアを使う場合は設定例のCAパスを維持し、broker専用のCAを使う場合は `site.env` に `CDC_BROKER_CA_FILE` の絶対パスを、Kafka設定の `ssl.ca.location` に `/run/cdc/broker-ca.pem` を入力します。
3. Sourceと契約の対応を `{"tables":[...]}` で記述します。項目は `source_schema`、`source_table`、`target_table`、`columns` です。 `columns` はsource列名をkey、契約列をvalueにし、6表・全列を一対一対応させます。 `topic_has_database` は `prefix.database.schema.table` 形式ならtrueです。
4. 専用空ディレクトリを同じUIDで作り、`site.env` に絶対パスの `CDC_STATE_DIR`、`CDC_SETTINGS_FILE`、`CDC_TABLE_MAP_FILE` を追加します。configファイルは0600、stateディレクトリは0700を使用します。
5. 既存Rと同じproject名で次を実行します。Dokployの場合は同じ `-f` をRun Commandへ追加します。

**Bash / Zsh共通**

```bash
docker compose --env-file "$site" -f compose.yaml -f compose.cdc.yaml \
  up -d --build --wait --wait-timeout 180
docker compose --env-file "$site" -f compose.yaml -f compose.cdc.yaml \
  logs --tail 60 native-cdc
```

まず `cdc_events.events` に配送IDと原本を保存し、LSNで現在値を選びます。派生テーブルの公開に成功した後にローカルcheckpointとKafkaの確認位置を進めます。 `/v1/cdc/gold/<dataset>` はCDCの公開世代を参照します。SQLiteは現在値と配送位置を持つため、Kafka・Iceberg・state directoryを揃えて復旧してください。履歴が切れた場合はconsumerを停止して原因を確認します。手動で状態を入れ替える前に、Kafkaの保持範囲と保存済みRawの照合を実施します。Rでの実SQL Server、社内Kafka TLS、長期保持からの復元は会社環境の受入対象です。 [Debezium SQL Server公式](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)

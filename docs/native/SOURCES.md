# SQL Server・Kafka・データ投入アプリとの接続

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

別アプリ [factory-file-generator](https://github.com/hiromura16/factory-file-generator) は、同一ホスト・権限で専用landingを共有し、製品プリセットの `lakehouse-products-local` を選ぶと製品CSVと隣接完了印を作れます。既定のマニフェスト方式は `.ready.json` を作るため、利用するにはファイル内容を検証してから該当のFoundation完了印を生成するアダプターが必要です。試験では独立アプリを隔離起動し、`examples/lakehouse-products-local.json` をAPIで実行しました。

## 設備時系列JSONL

`sensor/<名前>.jsonl` にUTF-8のJSONを1行1点で置き、同じSHA-256完了印を付けます。 [設備契約](../../contracts/native/sensor-event-v1.json) に規則があります。

```json
{"factory_id":"F01","line_id":"L01","equipment_id":"M01","sensor_id":"T01","measurement":"temperature","unit":"C","timestamp":"2026-01-05T12:00:00Z","sequence_no":1,"value":24.5,"quality_code":"GOOD"}
```

原本はS3、型付けした履歴は `silver.sensor_history`、最新値とUTCの1分集計は `gold.sensor_latest` と `gold.sensor_minute` に保存します。系列キーは工場・ライン・設備・センサー・計測種別・単位です。同じ系列と時刻・sequenceの完全一致は1点にまとめ、値や品質が衝突するとそのキーを隔離します。1分集計では全点数とGOOD数を持ち、最小・最大・平均はGOODだけを対象にします。異常値や衝突は `quarantine.sensor_events` に原本hashと行番号を残します。

## SQL Serverのバッチを反映する

SQL Serverの接続・抽出は [SQL抽出ガイド](../SOURCE-SQLSERVER.md) に従います。既設DBで読取専用principalを使用して固定6表のフル観測またはwatermark差分を作ります。SQL抽出先はR専用landingの `sql-batches/` です。 `POST /run` に操作トークンと、抽出側が返す `batch_id` と `observed_at` を送ります。 [API](API.md)

業務出力は `sql_bronze`、`sql_silver`、`sql_quarantine`、`sql_gold` に分かれます。確認するには `GET /v1/sql/gold/sales_fulfillment` を使用します。取り込み前に実DBの権限・SQL型・水位の意味を確認してください。

## Kafka互換ブローカーでCDCを取り込む

外部のDebeziumがSQL Server CDCのJSONを発行している環境向けの任意構成です。source connectorの登録や設定は [既存のCDCガイド](../CDC.md) にあります。標準RのComposeとconsumerにはJavaが不要です。Debezium Kafka Connectの実行環境にはJVMが必要です。

1. Kafka管理者が専用consumer groupのACLと対象6 topicへの読取権限を用意します。初回に必要な履歴を確保し、topicの保持期間と鍵ごとのpartition整合性を確認します。
2. [設定例](../../native/kafka-settings.example.json) をcheckout外のprivateファイルへコピーして、broker、DB、topic prefix、`lakehouse-r-` から始まる専用groupを入力します。実接続ではTLSまたはSASL_SSLを使い、brokerが広告する全アドレスへ直接接続できるようにします。 `ssl.ca.location` はコンテナ内パスです。
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

まず `cdc_events.events` に配送IDと原本を保存し、LSNで現在値を選びます。派生テーブルの公開に成功した後にローカルcheckpointとKafkaの確認位置を進めます。 `/v1/cdc/gold/<dataset>` はCDCの公開世代を参照します。SQLiteは現在値と配送位置を持つため、Kafka・Iceberg・state directoryを揃えて復旧してください。履歴が切れた場合はconsumerを停止して原因を確認します。通常経路のRaw再構築機能は互換構成の固定CDCにあります。Rでの実SQL Server、社内Kafka TLS、長期保持からの復元は会社環境の受入対象です。 [Debezium SQL Server公式](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)

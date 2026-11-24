# アーキテクチャと業務契約

<!-- doc-purpose:start -->
> **役割:** コンポーネントの役割と採用理由、業務契約、公開世代、元のFoundationとの違いを確認します。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 実装を理解・変更するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [レイクハウスの設計思想](<DESIGN.md>) / [SQL Server・Kafka・データ投入アプリとの接続](<SOURCES.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

```mermaid
flowchart LR
  F[外部ファイル生成・SQL抽出] --> Landing[landingとSHA-256完了印]
  Landing --> I[ファイル取込]
  I --> R[SeaweedFS S3 原本]
  I --> B[Iceberg Bronzeと台帳]
  K[外部Kafka / Debezium] --> C[任意のnative CDC consumer]
  C --> CR[Iceberg CDC Raw]
  C --> CB[Iceberg CDC Bronze]
  B --> D[DuckDBの業務変換]
  CB --> D
  R --> T[設備時系列の処理]
  T --> G
  D --> Q[SilverとQuarantine]
  D --> G[Goldと公開世代]
  G --> A[読取専用API]
  A --> L[LangChain]
  DB[PostgreSQL] --> LK[Lakekeeper REST Catalog]
  LK --> B
  LK --> G
  P[PrometheusとBlackbox] --> O[Grafana 稼働監視]
```

## 何を使い、なぜ選んだか

「受注の一部が出荷された」という行を例にすると、原本をSeaweedFSに保管し、Icebergで表の版を管理し、DuckDBで受注と出荷を結合します。利用者はGoldの集計をWeb画面やAPIで読みます。これらの役割を分けると、同じ原本から集計を作り直したり、計算方式を変えてもテーブルの保存形式を維持したりできます。[設計思想と運用への当てはめ](DESIGN.md)

| コンポーネント | Foundation Rでの役割 | 採用理由 |
| --- | --- | --- |
| DuckDB | 受注・出荷の結合、工場別日次集計、設備の1分窓など、Pythonワーカーの中でSQLを実行 | 分析処理を1台に収め、別のSQLサーバーやJVMの管理を省ける。既存業務SQLの結果を合成データで照合できる。[DuckDBの設計](https://duckdb.org/why_duckdb) |
| Apache Arrow / PyArrow | DuckDBとPyIcebergの間で、列ごとの型を持つデータを受け渡す | 一時CSVを作る工程を省ける。日時や数値型を明示して渡せる。[Arrowの仕様](https://arrow.apache.org/docs/format/Columnar.html) |
| Parquet / Apache Iceberg | Parquetが行の実体を保存し、Icebergがその表の列、ファイル一覧、履歴を管理 | 開いた仕様で保存し、スナップショットIDで参照する表の版を特定できる。[Parquet](https://parquet.apache.org/docs/) · [Iceberg仕様](https://iceberg.apache.org/spec/) |
| PyIceberg | PythonでIceberg表を読み書きし、表のスナップショットを確定 | ArrowとRESTカタログを使って、書き込みまでPythonワーカーで完結できる。[PyIceberg](https://py.iceberg.apache.org/) |
| Lakekeeper | 表の所在と名前空間をIceberg REST Catalogとして提供 | Rust製のカタログで、JVMなしの標準構成からIcebergのRESTインターフェースを使える。[Lakekeeper](https://github.com/lakekeeper/lakekeeper) |
| PostgreSQL | Lakekeeperの表登録や倉庫設定など、カタログ状態を永続化 | Lakekeeperが採用する永続バックエンドをそのまま使える。移行ジョブを通常の起動から切り分けられる。[Lakekeeperの構成](https://docs.lakekeeper.io/) |
| SeaweedFS | S3互換の入口で原本とIcebergのデータ・メタデータファイルを保管 | Pythonとカタログから共通のS3インターフェースで使え、試験用の単一VMに配置できる。[SeaweedFS](https://github.com/seaweedfs/seaweedfs) |
| Pythonのワーカー、HTTP API、CLI | 取込、業務変換、公開、Gold読取を定義済みの操作として提供 | 各操作をテストやCLIから同じ入口で実行できる。処理の実行権限と閲覧権限を分けられる。[APIの利用方法](API.md) |
| Prometheus / Blackbox Exporter / Grafana | Prometheusがメトリクスを貯め、BlackboxがHTTP・TCPを確認し、Grafanaが表示 | アプリへの追加実装が少ない疎通確認と、業務ジョブのメトリクスを併用できる。監視対象と画面をファイルで管理できる。[監視設定](MONITORING.md) |
| Streamlit / NGINX | 任意構成でGoldの表をブラウザー表示し、NGINXが閲覧画面と運用画面の認証付き窓口になる | Pythonで合成データの結果を確認でき、読取APIと認証境界を共有できる。[データ閲覧](GUI.md) |
| Kafka互換ブローカー / confluent-kafka | 任意のCDCで外部ブローカーからDBの変更イベントを受信 | 既存イベント契約を利用し、必須のComposeサービスを増やさずにファイル取込と併用できる。[入力経路](SOURCES.md) |
| LangChain Core | AIワークフローから、固定されたGoldデータセットを読取ツールとして利用 | データセット名や行数を制約し、表の版と公開世代を回答の根拠として渡せる。[LangChainのTools](https://docs.langchain.com/oss/python/langchain/tools) |
| Docker Compose / Dokploy | 同じサービス定義をCLIとDokployから配置 | セットアップ、ヘルスチェック、追加GUIを共通化し、配置先ごとのプロキシやCAを外部設定に置ける。[会社VMでの配置](COMPANY.md) |

1台のVMで業務SQLを実行する用途では、Sparkの代わりにDuckDBを使います。Icebergへの保存と表の版管理はPyIcebergが担当します。単一ホスト構成の処理上限は下記を参照してください。複数ホストでの分散実行や既存PySparkジョブには互換構成のSparkが使えます。[採用バージョンと公式リファレンス](REFERENCES.md)

## UNIXの責務分離とDDD

投入アプリは原本を作ります。入力アダプターは完了印とバイト列を照合して保管します。DuckDBはバッチごとにUTCのインメモリ接続を作り、承認されたSQLだけをArrowへ実行します。永続化の責務はLakekeeper、PostgreSQLとS3にあります。APIは固定された処理名とGoldデータセットを提供します。CLIとCIは同じAPIで確認できます。モデルは読取専用の利用者です。

| 文脈 | 言葉の意味 |
| --- | --- |
| 受け渡し | Rawは原本です。ファイルのSHA-256と相対パスで識別します。landing上でファイルを削除しても業務上のDELETEにはしません。 |
| 製造計画・実績 | 指図、実績、停止は別粒度です。Goldでは工場×UTC日へそれぞれ集計してから結合します。 |
| 受注・出荷 | 出荷を受注キーへ集約します。未出荷数、遅延出荷、充足率の意味は既存SQLに従います。 |
| 品質 | Silverの候補をキーごとに先に選び、代表エラーをQuarantineへ出します。最新候補が不正でも旧版へ戻しません。 |
| CDC | SQL Serverのストリーミング変更はsnapshot読取より後とし、commit/change LSNとserialで順序を決めます。業務DELETEは現在値を取り消し、Kafka tombstoneは別の生配送記録です。 |
| 時系列 | 設備の品質コードを残し、良品判定とは分離します。センサーの1分窓はUTC基準で、GOODだけを統計に使います。 |

`medallion/sources.json` と既存SQLを正本とし、同期元commitとSHA-256は [契約マニフェスト](../../contracts/native/foundation-source.json) に記録します。 `python3 native/check_contracts.py` で一致を確認できます。業務上の「訂正」「取消」「生産日」「品質」を変更する場合は、業務オーナーと意味を決めてからSQL・独立期待値を更新します。構想書のSail、RisingWave、GreptimeDB/InfluxDB、DataFusion、Ballista、Cometは評価候補です。標準構成はDuckDBとPyIcebergで、時系列はIcebergバッチです。ミリ秒級のオンラインTSDB性能、分散処理、候補製品間の比較結果は現時点で未計測です。

## 公開と失敗時の一貫性

Icebergは表ごとにcommitします。14本のMES/ERP SQLと、該当する設備時系列をすべて保存できた時点で、最後に `gold._release` に出力スナップショットを登録します。APIはその世代だけを参照します。途中で失敗すると直接Icebergを読む利用者には各表の最新状態が見えるため、一貫した世代が必要な場合は公開マニフェストを参照してください。read APIから古い公開世代も指定できます。参照する表スナップショットも一緒に保持する必要があります。

1つのCompose projectで各namespaceのwriterは1つです。複数レプリカや他製品からの並行書き込みは運用範囲外です。CPU、メモリ、原本サイズ、テーブル走査の行数とサイズには上限を設けています。単体100,000行・業務表64 MiBの走査上限を超えた場合は、別のdataset分割や分散構成を検証してから適用します。

### ファイル受け渡しから公開まで

```mermaid
sequenceDiagram
  participant F as データ投入アプリ
  participant W as Lakehouse ワーカー
  participant S as SeaweedFS S3
  participant I as Iceberg表・Lakekeeper
  participant D as DuckDB
  participant R as 読取API・画面
  F->>W: 原本とSHA-256完了印を受け渡す
  W->>S: 照合した原本を保存する
  W->>I: Bronzeとファイル台帳を確定する
  I-->>W: 固定したBronzeのスナップショットを返す
  W->>D: Arrowを登録して業務SQLを実行する
  D-->>W: Silver・Quarantine・Goldを返す
  W->>I: 派生表のスナップショットを確定する
  alt 全ての表を保存できた
    W->>I: gold._releaseに公開世代を記録する
    R->>I: 公開世代が指す表の版を読む
  else 保存中に失敗した
    R->>I: 前回の公開世代を読む
  end
```

## 元のFoundationとの比較

比較元は [既存Foundationのmain](https://github.com/hiromura16/lakehouse-foundation/tree/8adaef6d28a4fa01f2e409fd01a568cd6770e071) です。既存の6ソース契約と合成データの期待値を両構成で共有します。

| 観点 | 元のFoundation | Foundation R |
| --- | --- | --- |
| 標準の変換・参照 | Trinoで業務SQLを実行。Sparkクラスタを追加可能 | 組み込みのDuckDBとPyIcebergで単一ホスト実行。スナップショットを固定したAPIを提供 |
| 取り込み | ファイル、SQL Server、既存Debezium/Kafka用の配置と管理 | 同じファイル・SQLバッチ契約。外部Kafka互換ブローカー向けのネイティブCDCと設備JSONLを追加 |
| 業務・運用GUI | 操作ポータル、Superset、Kafka UI、n8n、監視などが豊富 | Grafana、Gold閲覧、Lakekeeper、Prometheusを使う軽量な入口。既存の各画面は互換構成で起動可能 |
| 監視・オーケストレーション | Prometheus、Grafana、Loki、Vector、Airflow、n8nなど | 設定ファイルで標準アプリの監視を配備。ログ基盤とオーケストレーションは互換構成 |
| 新しいデータ利用 | Gold API、MCP、LangChainのサンプル | 設備の最新値・1分集計と全表の公開世代。Gold閲覧画面とLangChainで表の版を確認可能 |
| 処理規模 | Spark分散構成・外部分析ツールを追加可能 | 標準は単一ホスト。複数ノードと既存Sparkジョブには互換構成を使用 |

標準構成の機能数は既存Foundationの全プロファイルより少なく、JVMなしで配備できる変換・閲覧の経路と設備時系列の扱いが増えています。

## 既存機能の入口

| 機能 | 起動する構成 |
| --- | --- |
| S3、Iceberg、メダリオン、SQLバッチ、設備時系列、稼働監視、Gold API、LangChain | RのルートCompose |
| KafkaイベントのRawとCurrent State | ルートComposeと `compose.cdc.yaml` |
| Trino、Spark、Superset、Jupyter、Airflow、n8n、Qdrant、Marquez、MCP、従来GUI、Samba、詳細ログ監視、Sail比較 | [Foundation互換構成](../../FOUNDATION.md) |
| SQL ServerのDebezium登録、固定CDCのRaw再構築 | [FoundationのCDC](../CDC.md)。Kafka Connect / DebeziumはJVMで稼働 |

互換構成は元と同じ入口を保ち、JVMサービスを必要に応じて別プロジェクトで起動できます。互換構成の既存手順のclone先は元のFoundationです。同じコードをRのcheckoutで動かす場合は、専用のprivate設定とプロジェクト名を使います。Rから元の本番namespace・topic・offsetへ切り替えるマイグレーションは実施していません。

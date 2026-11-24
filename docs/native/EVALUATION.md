# ネイティブ処理系の比較試験

<!-- doc-purpose:start -->
> **役割:** Sail、RisingWave、GreptimeDB、DataFusion、Ballista、Cometを合成データで起動・検証します。
>
> **対象読者:** 開発・設計担当 / 評価・報告担当
>
> **読むタイミング:** ネイティブ製品を分けて実行し、結果を確認するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [構想と実装の対応](<ROADMAP.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

比較試験は専用のDockerプロジェクトと合成入力だけで実施します。標準のDokploy配備は [START](START.md) を、実際に提供する機能の範囲は [構想と実装の対応](ROADMAP.md) を参照してください。

| プロファイル | 構成 | 確認できる内容 |
| --- | --- | --- |
| `sail` | PySail 0.7.3、Spark Connectクライアント | 同じ8入力で再送、遅延、取消、型不正を処理 |
| `stream` | RisingWave 3.1.2、隔離Redpanda | Kafka上のDebezium形式の作成・訂正・削除から現在値を作成 |
| `timeseries` | GreptimeDB 1.3.0 | 3測定点の時系列SQLと最新値 |
| `distributed` | Ballista 55.0.0のScheduler・Executor | 別プロセスに登録し、共通の集計値を取得 |
| `accelerator` | Spark 4.1.3とComet 1.2.0 | 合計5,050とCometの物理実行プランを確認 |
| 単体CLI | DataFusion 55.0.0 | 8入力の現在値、CSV・JSONLからParquetへの変換 |

配備するVMはプロファイルを一つずつ試せる空き容量を用意してください。接続先はループバックか専用ネットワークに限定します。CometはSparkとJVMを含むため、明示的に選んで動かす比較ジョブです。

## SailとDataFusionで同じ入力を照合する

Python 3.12とuvを使います。プロキシ環境ではuvが利用する経路にも会社のCAを設定します。SailのDockerビルドへ企業CAを渡す場合は `CORPORATE_CA_FILE` に配備先の絶対パスを設定します。

**Bash / Zsh共通**

```bash
uv venv --python 3.12 .venv-evaluation
uv pip sync --python .venv-evaluation/bin/python \
  --require-hashes native/evaluation/compute.lock
docker compose -f compose.evaluation.yaml --profile sail up -d --build --wait
.venv-evaluation/bin/python -m native.evaluation.compute sail
.venv-evaluation/bin/python -m native.evaluation.compute datafusion
```

同じSQLを初回1回、温まったセッションで3回実行します。P1の14.25を採用し、削除されたP2を除き、P3の型不正を報告します。DataFusionは別の合成入力をParquetにし、件数3と平均23.5を照合します。RSSはクライアントプロセス、時間はその実行に含まれるクエリを測っています。サーバーの起動時間や同一CPU制限の比較としては扱いません。[Sailの導入](https://github.com/lakehq/sail/blob/main/docs/introduction/getting-started/index.md) · [DataFusion PythonのI/O](https://datafusion.apache.org/python/user-guide/data-sources.html)

## RisingWaveでKafkaの現在値を確認する

新規の合成試験プロジェクトで実行します。PostgreSQLクライアント `psql` が必要です。起動後に `psql` の接続が確立してからDDLを適用します。

**Bash / Zsh共通**

```bash
docker compose -f compose.evaluation.yaml --profile stream up -d
docker compose -f compose.evaluation.yaml exec -T broker \
  rpk topic create foundation-r-evaluation-products --partitions 1
psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 18602 -U root -d dev \
  -f native/evaluation/stream.sql
docker compose -f compose.evaluation.yaml exec -T broker \
  rpk topic produce foundation-r-evaluation-products \
  < native/evaluation/stream-events.jsonl
psql -X -h 127.0.0.1 -p 18602 -U root -d dev -Atc \
  'SELECT product_id, revision, cycle FROM evaluation_current ORDER BY product_id'
```

反映後の結果は `P1|20|14.25` です。標準CDCと同じ業務契約の全列照合、Icebergへの書き出しとその復旧は別の試験です。[RisingWaveのKafka構成](https://github.com/risingwavelabs/risingwave-docs/blob/main/integrations/sources/kafka.mdx)

## GreptimeDBの時系列を確認する

**Bash / Zsh共通**

```bash
docker compose -f compose.evaluation.yaml --profile timeseries up -d
python3 -m native.evaluation.greptime
```

合成3点で平均23.5、最新24.5を返します。InfluxDB側は [運用ツールの手順](TOOLS.md) で同じ測定値を確認できます。保持・再送・破損時復旧と負荷の比較は専用の連続試験で測ります。[GreptimeDBのスタンドアロン構成](https://docs.greptime.com/getting-started/installation/greptimedb-standalone/)

## 分散SQLを確認する

**Bash / Zsh共通**

```bash
docker compose -f compose.evaluation.yaml --profile distributed up -d
docker compose -f compose.evaluation.yaml --profile distributed-check \
  run --rm --build ballista-check
```

クライアントがSchedulerにSQLを送信し、Executorを経由した合計10を返します。複数ホスト、途中停止、分散時の性能は別の受入試験です。[BallistaのDocker構成](https://datafusion.apache.org/ballista/user-guide/deployment/docker.html)

## SparkとCometの実行プランを確認する

**Bash / Zsh共通**

```bash
docker compose -f compose.evaluation.yaml --profile accelerator \
  run --rm --build comet
```

Parquetを読み、合計5,050と実行プランのComet演算子を確認します。ビルドでは固定したJARを取得するため、Maven Centralへの到達経路が必要です。[Cometのインストールと対応版](https://datafusion.apache.org/comet/user-guide/)

## 試験の記録と停止

入力バイト数、行数、製品の版、実行ホスト、CPUとメモリ上限、初回・再実行の別、処理時間、出力の一致を同じ記録に残します。分散時は障害で失った作業の範囲と復旧までの操作も加えます。会社データの投入は [受入条件](ACCEPTANCE.md) に沿って設置先で行います。

**Bash / Zsh共通**

```bash
docker compose -f compose.evaluation.yaml --profile '*' down
```

この停止では永続volumeを保持します。

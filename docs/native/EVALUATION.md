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
| S3比較 | RustFS 1.0.6、Apache-2.0 | 隔離したLakekeeperからIcebergを公開し、認証・条件付き書込・再起動を確認 |
| 単体CLI | DataFusion 55.0.0 | 8入力の現在値、CSV・JSONLからParquetへの変換 |

配備するVMはプロファイルを一つずつ試せる空き容量を用意してください。接続先はループバックか専用ネットワークに限定します。CometはSparkとJVMを含むため、明示的に選んで動かす比較ジョブです。

## RustFSを別のS3保存先として試す

RustFS 1.0.6はRust製のS3互換ストレージです。専用ネットワークからS3を提供し、
APIやConsoleをホストへ公開しません。標準構成と同じLakekeeper・PyIceberg・DuckDB
を使い、新規のwarehouseと名前付きvolumeへ合成入力だけを保存します。
Linux amd64、Compose 2.24.4以上を使用します。
[公式Docker手順](https://docs.rustfs.com/en/installation/container/docker)
· [Composeファイルの上書き仕様](https://docs.docker.com/reference/compose-file/merge/#replace-value)

次のコマンドはcheckout直下から実行します。接続のある間にイメージを用意し、
内部専用ネットワークに切り替えてから起動します。必ずここで作る新しい
project名と設定を使ってください。既存のwarehouseへの上書きや移行には使用しません。

**Bash / Zsh共通**

```bash
stage=$(mktemp -d)
project="foundation-r-rustfs-$(openssl rand -hex 4)"
export RUSTFS_TEST_LAKEHOUSE_IMAGE="$project-lakehouse"
site="$stage/site.env"
landing="$stage/landing"
python3 native/setup.py --output "$site" --landing "$landing"
docker compose --env-file "$site" -p "$project" build lakehouse
docker compose --env-file "$site" -p "$project" \
  -f compose.yaml -f compose.storage.rustfs.yaml \
  pull storage catalog catalog-migrate catalog-db
docker compose --env-file "$site" -p "$project" \
  -f compose.yaml -f compose.storage.rustfs.yaml \
  up -d --no-build --pull never --wait lakehouse
docker compose --env-file "$site" -p "$project" \
  -f compose.yaml -f compose.storage.rustfs.yaml \
  exec -T lakehouse python - < tests/integration/native/rustfs_storage.py
docker compose --env-file "$site" -p "$project" \
  -f compose.yaml -f compose.storage.rustfs.yaml \
  exec -T lakehouse python -m native.api verify
```

S3の匿名・誤認証は403、原本の条件付き書込の条件不一致は412。正常な条件のみ
更新を許可します。合成の受入11件に加え、6業務入力から得たGold14行とRaw6件の
SHA-256、Iceberg公開世代は、同じvolumeで再起動後も一致しました。
単一ノード、単一ディスクの試験です。企業利用ではIAM・TLS、監視、
複数ノードの障害復旧、データ移行を別の環境で確認します。
[公式S3互換性の範囲](https://docs.rustfs.com/en/reference/s3-compatibility)

検証を終了するだけなら同じファイル指定で `docker compose down` を実行します。
`down --volumes` は試験データも削除するため、証跡と保管の要否を確認してから実行します。

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

## RisingWaveの追記をIceberg V2に確定する

Kafkaの合成トピック、RisingWave、Lakekeeper、SeaweedFS、読取用
PyIcebergを、標準とは別のComposeプロジェクトに起動します。
RisingWave 3.1.2の `rest_rust` を使用し、カタログURIは
`http://catalog:8181/catalog` と指定します。採用版のRustクライアントは
この後ろにAPIパスを連結するため、末尾スラッシュは付けません。
S3の合成認証はRisingWaveのプロセスへ渡し、DDL本文から分けます。

**Bash / Zsh共通**

```bash
uv venv --python 3.14 "$HOME/.cache/lakehouse-r-iceberg-evaluation"
uv pip sync --python "$HOME/.cache/lakehouse-r-iceberg-evaluation/bin/python" \
  --require-hashes tests/integration/iceberg/requirements-runner.lock
"$HOME/.cache/lakehouse-r-iceberg-evaluation/bin/python" \
  tests/integration/iceberg/acceptance.py
```

Dockerをsudoで実行する環境では試験プログラムに `--sudo` を付けます。
試験は新規環境の作成、各段階の照合、当該ボリュームの片付けを自動で行います。

| 試験 | Bakuの合成環境で確認した結果 |
| --- | --- |
| 最初の3点とRisingWave再起動 | 一意ID 3件、再起動の前後で同じIceberg V2スナップショット |
| 再開後の2点 | 新しいスナップショットで5件。別コンテナのPyIcebergで発生元UTC時刻のマイクロ秒、計測種別、値を照合 |
| カタログ停止中に2点、RisingWave再起動とカタログ復帰 | RisingWaveの確定を待った後に再送。Icebergでは全7件、重複0。さらに再起動しても同じスナップショット |
| S3認証 | 誤った認証でHTTP 403、検証した6サービスのログに生成した認証値の混入なし |

この試験は単一ノードで追記する経路です。CDCの更新・削除や並行書込、
OIDCと権限、コミット応答だけが失われるケースは別受入です。
同じ業務IDを別のKafkaイベントとして送った場合は、業務の再送契約が必要です。
[RisingWaveのIceberg確定と再送](https://docs.risingwave.com/iceberg/deliver-to-iceberg) · [採用版のRust REST選択](https://github.com/risingwavelabs/risingwave/blob/v3.1.2/src/connector/src/connector_common/iceberg/mod.rs) · [受入プログラム](../../tests/integration/iceberg/acceptance.py)

## Kafkaの設備原本をIcebergへ保管する

同じ相互運用試験では、型付けした計測点とは別に、2区画の専用Kafka
トピックを読み、RisingWaveの `FORMAT PLAIN ENCODE BYTES` と
Iceberg V2へ追記するシンクを使用します。`fixture.telemetry_wire_v2` に
Kafkaの区画・位置・記録時刻・キー・本文を保存します。本文内の設備時刻は
原本バイトの一部として残り、Kafkaの時刻へ置き換わりません。

最初の3件にはLine Protocolに加えて非UTF-8、NUL、改行のある原本が
含まれます。別プロセスのPyIcebergでキーと本文を全バイト照合し、
RisingWaveを再起動して入力なしで同じスナップショットを維持した後、
別区画の1件を追加して全4件と配送位置が一致することを確認しました。

このSQLの用途は、非NULLの設備原本を隔離環境で追記する試験です。
トピックの再作成は別の原本系列として管理し、運用時はKafka保持期限に
達する前に区画・位置の照合を行います。CDCの削除、Kafkaヘッダーの保存、
長期の保持や高負荷、標準の設備Goldへの正規化は別のデータ契約です。
[再現用SQL](../../native/evaluation/telemetry-wire.sql)
· [RisingWaveのKafkaバイト列とメタデータ](https://docs.risingwave.com/ingestion/sources/kafka#extract-metadata-from-kafka-sources)
· [Icebergシンク](https://docs.risingwave.com/iceberg/deliver-to-iceberg)

## RisingWaveの更新・削除をIceberg V2で読む

追記試験と同じ隔離構成に、合成Debezium JSONのINSERT・UPDATE・DELETEを
送ります。RisingWave 3.1.2の `rest_rust` から同じwarehouseの2表へ
書き、同梱の読取コンテナとRisingWaveの外部Iceberg読取で全行を照合します。
Lakekeeper 0.14.3、SeaweedFS 4.53、Redpanda 26.3.1のイメージと、
PyIceberg 0.12.0、DuckDB 2.0.1を固定しています。

| Iceberg V2へ書く方式 | 削除ファイル | 固定版での論理行の読取 |
| --- | --- | --- |
| 通常のMoR。`primary_key='id'`、`partition_by='bucket(4,id)'` | 等価削除。`content=2`、等価削除キーは`id`のIcebergフィールドID | DuckDBとRisingWave外部読取で一致。PyIcebergは初回INSERTを読めるが、等価削除を読取計画に含めるとエラー |
| PK-indexのMoR。`enable_pk_index='true'`、上流の主キーを使用、非パーティション表 | 位置削除。`content=1`。対象データファイルと行位置を指定 | PyIceberg、DuckDB、RisingWave外部読取で一致 |

PK-indexには `primary_key` を指定せず、上流の一意なキーを維持します。
両方に `SET sink_decouple=true` と `is_exactly_once='true'` を指定した
合成試験では、`rw_sink_decouple` の実効値は通常MoRが `true`、
PK-indexが `false` でした。公式文書が記載するsink decouplingを前提とした
exactly-onceの条件は、設定文字列だけでは確認できません。
[RisingWaveの配信設定](https://docs.risingwave.com/iceberg/deliver-to-iceberg) · [PK-indexのV2位置削除](https://github.com/risingwavelabs/risingwave/pull/26116) · [Iceberg仕様の削除適用](https://iceberg.apache.org/spec/#scan-planning) · [PyIcebergの読取計画](https://py.iceberg.apache.org/reference/pyiceberg/table/)

前節と同じ方法で仮想環境を作り、Linux amd64で次を実行します。
プロキシ経由の場合は事前に環境変数を設定します。実行ホストから
`extensions.duckdb.org` のHTTPSを許可してください。読取コンテナで
使う拡張は展開後のSHA-256とDuckDBの署名検証を使って固定します。

**Bash / Zsh共通**

```bash
"$HOME/.cache/lakehouse-r-iceberg-evaluation/bin/python" \
  tests/integration/iceberg/acceptance.py --upsert
```

Lakekeeper停止中に変更を送って接続失敗を観測し、RisingWaveを強制停止
します。カタログだけ復帰させると両表の現行世代と履歴は停止前のままです。
次に同じ永続ボリュームでRisingWaveを再開し、新しい世代で削除済みIDが
消え、残ったIDの数量と改訂番号が一致することを照合します。
試験の範囲は単一writer、Kafka 1区画、完全な更新前後の行、カタログに
到達できない故障です。コミット応答だけの消失と並行writerは別受入です。

固定イメージの隔離試験ではライセンスキーを渡さずにPK-indexが動作しました。
上流 `main` にはPK-indexの利用条件を変更する実装が入っており、更新時は
対象バージョンの配布・ライセンス条件を確認します。Copy-on-writeは
評価時に専用compactorがない環境で `ingestion` が進んでも `main` が
進まなかったため、標準設定に加えていません。
[ライセンス条件の上流変更](https://github.com/risingwavelabs/risingwave/pull/27970) · [compactorと書込方式](https://docs.risingwave.com/iceberg/write-modes) · [再現用プログラム](../../tests/integration/iceberg/upsert.py)

## 1万・10万・100万行のParquetを測る

DuckDB 2.0.1とDataFusion 55.0.0で、合成設備4列の同じParquetに対して
フィルター・設備×日集計・並べ替えを実行します。LinuxのCPU割当てを
同じ2つの論理CPUに固定し、各規模で実行順を入れ替えます。
各セッションは新しいプロセスの初回1回、同じセッションでの反復3回です。
測定中にOSページキャッシュを消していません。RSSはエンジン、Python、
Arrow、結果検証を含むプロセス全体の最大値です。

**Bash / Zsh共通**

```bash
uv venv --python 3.12 "$HOME/.cache/lakehouse-r-benchmark"
uv pip sync --python "$HOME/.cache/lakehouse-r-benchmark/bin/python" \
  --require-hashes native/evaluation/parquet_benchmark.lock
bench_result=$(mktemp -d)/parquet
"$HOME/.cache/lakehouse-r-benchmark/bin/python" \
  native/evaluation/parquet_benchmark.py --output "$bench_result" --threads 2
```

結果の `report.json` に入力ファイルの行数・サイズ・SHA-256、SQL、
出力スキーマ、独立集計の結果SHA-256、時間とRSSを記録します。
Intel Core i9-13900HのLinux x86_64環境で実施し、別ディレクトリへの
再生成を含めて96クエリで期待値に一致しました。

| 入力行 | 実Parquet合計 | DuckDBの初回／反復中央値（ms） | DataFusionの初回／反復中央値（ms） |
| ---: | ---: | ---: | ---: |
| 10,000 | 33,601 B | 8.11–118.59／8.06–14.00 | 8.77–19.10／5.09–10.64 |
| 100,000 | 286,964 B | 10.57–18.67／8.02–9.62 | 17.36–35.88／9.24–11.09 |
| 1,000,000 | 2,812,054 B | 27.96–30.37／18.60–18.85 | 45.04–47.92／20.55–26.70 |

100万行の初回計測では、最大RSSがDuckDB 99,528–100,824 KiB、
DataFusion 105,788–109,212 KiBでした。計測対象はローカルの列指向
フィルター集計です。少ない入力では初回のばらつきが大きく、エンジン選定時は
実際の列数、圧縮率、並列数、データ分布とネットワークを合わせて再測定します。
[2回の測定JSON](../../native/evaluation/parquet_benchmark_reference.json) · [Linuxの最大RSSの定義](https://man7.org/linux/man-pages/man2/getrusage.2.html) · [DataFusionの設定](https://datafusion.apache.org/python/user-guide/configuration.html)

## Gold読取のメモリと応答を照合する

BakuのローカルIcebergに合成95,000行、Parquet 25件を作り、固定した
公開世代から最大200行を参照しました。記録に日本語文字列、小数型と
NULLを含め、公開を更新せずに物理表へ追記した1行が応答へ混入しないことを
確認しました。全体、工場で絞った9,500行、該当なしの3パターンで、件数、
型、順序とレスポンスのSHA-256は前後で一致しています。

2論理CPU、PyIcebergの読取並列数2、各条件と実装を別プロセスで4回、
起動順を交互にして記録しました。OSページキャッシュは維持しています。
全体の中央値はArrow表を先に作る経路201.96 ms、バッチ読取175.06 ms。
プロセス最大RSSは約252〜277 MiBから約228〜237 MiBでした。工場で
絞った例のRSSには重なりがあり、測定値はネットワーク越しの性能や
同時アクセスの処理能力を示しません。
[PyIcebergの並列数](https://py.iceberg.apache.org/configuration/#concurrency)
· [DuckDBのArrowデータ取込](https://duckdb.org/docs/2.0/clients/python/data_ingestion)

## 試験の記録と停止

入力バイト数、行数、製品の版、実行ホスト、CPUとメモリ上限、初回・再実行の別、処理時間、出力の一致を同じ記録に残します。分散時は障害で失った作業の範囲と復旧までの操作も加えます。会社データの投入は [受入条件](ACCEPTANCE.md) に沿って設置先で行います。

**Bash / Zsh共通**

```bash
docker compose -f compose.evaluation.yaml --profile '*' down
```

この停止では永続volumeを保持します。

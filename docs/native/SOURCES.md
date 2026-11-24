# ファイル・SQL・Kafka・設備の入力契約

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

## 工場ファイルをローカルで確定する

SMB、SFTP、手動コピーで届いた原本は、配備先VMのローカルファイルシステムへ受け渡します。コピーと書込の終了を送信側で確認してから、原本のパス、送信元、配送IDを指定します。別送信元が同じ名前で送っても保管先を分離します。

**Bash / Zsh共通**

```bash
landing="$HOME/.config/lakehouse-foundation-r/landing"
python3 -m native.intake.spool --landing "$landing" \
  --sender factory-a --delivery shift-001 --dataset erp_products \
  --encoding cp932 /path/to/copied-products.csv
```

登録済みの6データセットはUTF-8、CP932、BOM付きUTF-16を受け入れます。原本と変換結果のSHA-256を受入台帳に記録します。完了印が付いたUTF-8の変換結果だけを既存の業務取込へ渡します。任意形式のファイルを保全する場合は `--dataset raw` を指定します。対象の上限は1ファイル16 MiBです。

| 終了値 | 受入の状態 |
| --- | --- |
| 0 | 原本を保全済み。登録済み形式は列検査を通して正規化済み |
| 1 | 原本の読取、権限、同じ配送IDの内容不一致などで確定に失敗 |
| 2 | 原本と拒否理由を保全済み。列や文字コードの不一致で業務表への配置を拒否 |

再送には同じ送信元と配送IDを使います。訂正版は新しい配送IDを発行し、後続の業務ルールで更新時刻や業務キーを照合します。複数の配送が同じファイルを同時に上書きする形でマウントせず、工場側で原本を閉じてから送ってください。復旧時は `receipts/<送信元>/<配送ID>.json` と完了印を確認します。正規化の拒否は取込境界の結果として保全されます。型や業務ルールによる行の隔離は業務処理で行います。

## ファイル受け渡し

1. 投入側で一時名へ書き込み、書き終わったら同じファイルシステム内で確定名へ移動します。
2. 確定ファイルのSHA-256を計算し、隣の `<ファイル名>.ready` へ64桁の小文字と改行を保存します。完了印を最後に確定します。
3. `python -m native.api medallion` をLakehouseコンテナ内で実行します。原本と印は取込み完了まで保持します。

6ソースのパスと列は [sources.json](../../medallion/sources.json) にあります。 `csv/erp_products__<12桁の小文字hex>_<4桁の連番>.csv` のような増分名も認識します。見覚えのない拡張子や名前はファイル原本として保存し、任意の業務表には割り当てません。大文字JSONキーも含め列名は契約と一致させます。同じパスとhashは取込済みとして扱います。

送信アプリの実装や配備は送信側で管理します。Foundation Rが受け取るのは、型と列を定めたファイルと隣接する `.ready` です。JSONの完了マニフェストを使う送信側では、受け渡しアダプターで原本と整合性を検証してから、この形式の完了印を出力します。

## 検査画像と原本の保全

完了印が一致した原本をS3へ保存します。業務CSVやJSONLが列・文字コードの検査で
失敗した場合も原本を保全し、公開済みの業務集計を維持します。訂正された素材を
受け取ってから再実行します。

PNG、JPEG、WebPは32メガピクセル以下で先頭画像を展開できる場合に、型と寸法を
`bronze.images` に記録します。破損・対象外・上限超過の画像はS3にそのまま保全し、
`quarantine.image_files` に原本SHA-256、位置、理由コードを記録します。業務の取込は
継続します。同じパスの正常版を受け取ると、現在の画像一覧が回復します。
検査用画像の実データは公開APIから読めません。原本を扱う担当者を分け、
製品番号や作業者が画像に含まれる場合は社内データとして管理します。

## 大容量の原本を保全する

通常取込の上限を超える検査素材、ログ圧縮ファイル、CADの書き出しなどは、
`large-raw/` で別に受け取れます。既定の上限は1ファイル256 MiBです。ファイル名は
英数字で始まる英数字・ピリオド・下線・ハイフン127文字以内にし、接尾辞に
`.sealed` や `.ready` を使用しません。

1. 送信側で原本を閉じ、配備先の受け渡し先の `large-raw/` に用意します。
   原本を書き終わってから最終名へ移し、隣にSHA-256の小文字64桁と改行だけの
   `<ファイル名>.sealed` を最後に確定します。
2. Dokployの `lakehouse` のTerminalから明示的に保全します。ジョブは
   [操作API](API.md) からも実行できます。

**Bash / Zsh共通**

```bash
python -m native.api archive inspection-0001.bin
```

既定設定では原本を最大1 MiBずつ読み、完了印と一致するSHA-256を検証します。
S3の条件付き作成で既存原本を保護し、保存先が報告するSHA-256とサイズを照合
してから `bronze.files` に登録します。応答の `object_created` はS3への新規作成、
`registered` は台帳上の現在版の更新です。再送では両方false、更新版ではSHA-256
別の原本を保全します。対応する `.sealed` の付いた入力を、通常の `medallion` は
自動処理しません。業務表への変換、圧縮の展開、外部公開には別の承認を使います。
[条件付き書込](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
· [S3のオブジェクト照合](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

Dokployの環境変数またはローカルComposeの `site.env` で
`LAKEHOUSE_ARCHIVE_MAX_MIB` を1〜2048の整数に設定できます。値はMiB単位で、
変更後に `lakehouse` を再作成します。256 MiB以下の原本には単一の `PutObject`
を使い、256 MiBを超える原本は明示した上限以内で固定16 MiBのS3パートへ分けます。
末尾のパートだけは短くなります。原本全体のSHA-256を完了印と照合し、各パートは
送信時に再ハッシュします。完了時は `If-None-Match: *` を付け、保存後のHEADで
サイズとS3の複合SHA-256を照合してから、同じ `bronze.files` の台帳に登録します。

中断や競合では、その要求で取得したupload IDのパートだけを中断します。競合の
412では、既に存在する原本のサイズと複合SHA-256が一致した場合だけ再送として
扱います。応答を失った場合は、稼働中のジョブが終わってから同じ原本を再送します。
保存済みならHEAD照合後に台帳を整え、未完了で見えなければ新しく送信します。
プロセスの強制終了や初回応答の消失時には、中断対象のIDを取得できない場合が
あります。保存先の対応を確認し、未完了マルチパートを期限付きで回収します。
支給するS3権限には対象バケットでの条件付き作成・HEAD・当該upload IDの中断が
必要です。複合SHA-256を返さない保存先では台帳を確定しません。2 GiBは設定の上限で、
隔離CIの入力は257 MiBです。採用する最大サイズで転送時間と
プロキシ・クライアントの待機時間を照合してください。コンテナ内の操作CLIは
最長600秒待機します。CLIが先に時間切れになっても、処理が完了している場合が
あります。ヘルスAPIの `busy` がfalseになってから、同じ完了印と原本で再実行します。
停止・再配備では先に新規投入を止め、実行中の保全ジョブが終了したことを
確認してください。Composeの停止猶予は2分です。
[S3のチェックサム方式](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html) · [マルチパートの確定](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html) · [マルチパートの中断](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html) · [257 MiBの隔離受入](../../.github/workflows/archive-multipart.yml)

## 設備時系列JSONL

`sensor/<名前>.jsonl` にUTF-8のJSONを1行1点で置き、同じSHA-256完了印を付けます。 [設備契約](../../contracts/native/sensor-event-v1.json) に規則があります。

```json
{"factory_id":"F01","line_id":"L01","equipment_id":"M01","sensor_id":"T01","measurement":"temperature","unit":"C","timestamp":"2026-01-05T12:00:00Z","sequence_no":1,"value":24.5,"quality_code":"GOOD"}
```

原本はS3、型付けした履歴は `silver.sensor_history`、最新値とUTCの1分集計は `gold.sensor_latest` と `gold.sensor_minute` に保存します。系列キーは工場・ライン・設備・センサー・計測種別・単位です。同じ系列と時刻・sequenceの完全一致は1点にまとめ、値や品質が衝突するとそのキーを隔離します。1分集計では全点数とGOOD数を持ち、最小・最大・平均はGOODだけを対象にします。異常値や衝突は `quarantine.sensor_events` に原本hashと行番号を残します。

設備の完了印と変換は製造・出荷の入力から分けて実行します。
`medallion` で設備側の原本照合や変換に失敗した場合、既存の設備表を保持
しながら業務の確定版を公開します。閲覧画面の設備集計に更新保留が表示され、
取込状態APIに前回の設備確定時刻と失敗段階が残ります。現行の設備ファイル
再集計枠は256パス、計32MiB、計10万物理行です。連続計測の収集・保持には
[専用テレメトリ経路](TELEMETRY.md) を使い、配置先で保持容量を設定します。

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
  --output "$landing" --snapshot
docker compose --env-file "$site" exec -T lakehouse python -m native.api medallion
```

`--snapshot` は、DBAが `ALLOW_SNAPSHOT_ISOLATION ON` を設定済みのソースで使います。
1つのSQL Server接続でトランザクションを開き、固定6表を同じ時点で読みます。
抽出CLIはDBの設定を変更しません。設定が未対応ならファイルを一切公開せず、
終了値1を返します。従来の表単位の読取を選ぶ場合は `--snapshot` を省きます。
どちらの読み方かは `source-extract.json` の `read_consistency` で確認できます。
長時間の読取とソース側の行バージョン領域は、DBAと確認してください。

抽出は全表の読取後に `csv/`・`json/` と完了印を1件ずつ確定します。
読取の同一時点性と、着地ファイルの公開は異なる段階です。
同じ着地先に定期ジョブがある場合は、別の着地先に抽出して全6表の
マニフェストを照合し、定期ジョブを止めた状態で受け渡してください。
出力済みのパスを置き換える場合は `--force` の対象を確認します。

合成SQL Server 2025 CU11で、読取中に別接続が二表を更新した際も、
抽出は両方の旧値を返し、次の抽出で両方の新値を返すことを照合しています。
日本語、十進数、時刻、6ファイルのSHA-256、誤ったCA・接続名、書込権限の拒否も
試験します。[受入ワークフロー](../../.github/workflows/sqlserver.yml) ·
[抽出CLI](../../scripts/extract-sqlserver.py) ·
[Microsoftの分離レベル](https://learn.microsoft.com/sql/t-sql/statements/set-transaction-isolation-level-transact-sql) ·
[FreeTDSのCA・接続名検証](https://www.freetds.org/userguide/freetdsconf.html)

## PostgreSQLの6表を同じ時点で取得する

会社のPostgreSQLからMES/ERPの全件を取得する場合は、別の抽出プロセスに
Psycopg 3を導入します。抽出プロセスだけがソースDBへ接続し、アプリには
完了印を付けたJSONと操作用の3項目を渡します。実証対象はPostgreSQL 18.6、
CPython 3.14、Psycopg 3.3.9です。

1. DBAに専用のログインロールを用意してもらいます。対象DBの接続権、
   対象スキーマのUSAGE、対象6表のSELECTだけを付けます。RLSのない通常表
   を指定します。ビュー、外部表、パーティション親表、表所有者と書込権を
   持つロールはこの構成の対象外です。DBのパスワード認証を使用するため、
   TLSサーバー証明書のDNS名またはIPと信頼するCAを入手してください。
2. [列マップの例](../../infra/source/postgresql-tables.example.json) を
   checkout外へコピーし、6表すべてのスキーマ名、表名、列名を登録します。
   名前はASCIIの英数字とアンダースコアで最大63文字、先頭は英字か
   アンダースコアです。任意のSQL式は
   入力できません。事業日は `date`、時刻は `timestamptz`、金額と標準秒数は
   `numeric` または整数型、数量は整数型、その他は `text` または `varchar`
   を使います。
   主キーと更新時刻は必須です。
3. checkout外の自分だけが読める設定ファイルに次の情報を記録します。
   `source_id` はその設置先を表すUUIDを一度発行して保管します。
   `ca_file` は信頼CAの絶対パスを使います。設定とCAのパスに
   シンボリックリンクを含めないでください。

```json
{
  "host": "postgres.example.invalid",
  "port": 5432,
  "database": "factory",
  "user": "lakehouse_reader",
  "password": "実際のパスワードに置換",
  "ca_file": "/absolute/private/company-ca.pem",
  "source_id": "00000000-0000-4000-8000-000000000001"
}
```

checkoutのルートから次を実行します。登録済みの `landing` を指定します。
設定ファイルは作成時から0600にし、接続用の `PG*` 環境設定がある場合は、
抽出専用のシェルから実行します。

**Bash / Zsh共通**

```bash
config="$HOME/.config/lakehouse-postgresql"
landing="$HOME/.config/lakehouse-foundation-r/landing"
umask 077
mkdir -p "$config"
python3 -m venv "$config/venv"
"$config/venv/bin/python" -m pip install --require-hashes \
  -r infra/source/requirements-postgresql.lock
"$config/venv/bin/python" scripts/extract-postgresql.py \
  --settings "$config/source.json" \
  --mapping "$config/tables.json" \
  --landing "$landing" > "$config/job.json"
```

`job.json` の `action`、`batch_id`、`observed_at` を
[操作API](API.md) の `POST /run` へ送ります。`sql-batches/` のJSONと
`.json.ready` が揃った後にこの3項目を出力します。`observed_at` は
PostgreSQLで読取の可視性を確定したクエリの開始時刻です。同じ入力は同じ内容IDに
なり、再取得時刻は別に記録されます。同じ観測を再送する場合は最初の
`job.json` をそのまま送ります。

TLSは `verify-full` でCAと接続名を検証します。6表を同じ
`REPEATABLE READ READ ONLY` トランザクションで読みます。
最初のSELECTの前に6表へ ACCESS SHARE を取得し、読取中に未読の表が
TRUNCATEや表の書換えで空に見えることを防ぎます。行の挿入・更新・削除は
並行して実行できます。全表合計
100,000行、JSON 16 MiB、読取期限を設け、超過や障害では完了印を公開
しません。`--max-rows` で小さい上限を設定できます。原本では数値の小数末尾桁を
10進文字列として残し、時刻はUTCのマイクロ秒を保持します。Silver以降は
業務契約に従う精度で集計します。
`NULL` と空文字は別の値です。レコードの欠落が次の全件取得で確定すると、
SQL Goldの現行データへ反映されます。

SQLバッチの設置先は1プロジェクトにつき1件です。異なるソースを
SQLバッチで使う場合は、別プロジェクトと別カタログ・保存領域を
用意します。接続先、データベース、列マップを変える場合も新規の
隔離先で受け入れます。パスワードやCAの更新で設置IDは変わりません。
この抽出は全件スナップショット専用で、増分取得とCDCは別の経路です。
[PostgreSQLのトランザクション分離](https://www.postgresql.org/docs/18/transaction-iso.html) · [PostgreSQLのMVCCに関する注意](https://www.postgresql.org/docs/18/mvcc-caveats.html) · [libpqの証明書検証](https://www.postgresql.org/docs/18/libpq-ssl.html) · [Psycopgの型変換](https://www.psycopg.org/psycopg3/docs/basic/adapt.html)

[PostgreSQL専用のGitHub Actions](../../.github/workflows/postgresql.yml) は、
一時証明書と合成6表でTLSの受入・拒否、実IcebergのGoldと同一観測の再送を
確認します。ローカルで再現する場合は、使い捨て環境でDockerとComposeを用意し、
上で固定依存を入れたPythonを指定して、リポジトリのルートから実行します。
試験用のPostgreSQLはループバックの自動割当ポートだけを公開します。

**Bash / Zsh共通**

```bash
bash tests/integration/postgresql/run.sh "$config/venv/bin/python"
```

## 内製アプリのSQLiteバックアップを取り込む

工場PCや設備の内製アプリがSQLiteを使う場合は、アプリのバックアップ機能、
またはSQLiteの `.backup` で作ったDBを基盤へ渡します。書込中のSQLiteファイルを
OSのコピーで持ち出すと、WALにある確定済みのデータがコピーに含まれないことが
あります。アプリから承認済みのバックアップを取得してください。
[SQLite Backup API](https://sqlite.org/backup.html) · [WALファイルの扱い](https://sqlite.org/wal.html#the_wal_file)

Python 3.12以上とSQLite 3.37以上を用意します。SQLiteの通常表を6個用意し、
[列対応の例](../../infra/source/sqlite-tables.example.json) をcheckout外へコピーして
実際の表名・列名を指定します。 `source_schema` は全表で `main` を使います。
`source-name` は保存する設置先の論理名、`source-id` は設備導入時に一度発行したUUIDです。
同じ設置先を継続するときは、ファイル名が変わってもこの2項目と列対応を維持します。

バックアップは補助ジャーナルファイルのない単体のDBを使います。WAL形式の
バックアップを受け取った場合は、元の稼働DBから分離したコピーに対して
`PRAGMA journal_mode=DELETE` を実行し、閉じた状態で送ります。
DBの内容は読取専用トランザクションで取得し、抽出は受領したDBを更新しません。
業務値はTEXT、INTEGERまたはNULLを使い、十進数の精度を保持する列はTEXTで
保存します。 `updated_at` は `2026-01-05T03:00:00.1234567Z` の形でUTCを指定します。
全6表合計10万行とJSON 16 MiBが上限です。
[SQLite URIのmode=ro](https://sqlite.org/uri.html) · [SQLiteのデータ型](https://sqlite.org/datatype3.html)

**Bash / Zsh共通**

```bash
sqlite_config="$HOME/.config/lakehouse-sqlite"
landing="$HOME/.config/lakehouse-foundation-r/landing"
umask 077
mkdir -p "$sqlite_config"
python3 scripts/extract-sqlite.py --db "$sqlite_config/source.sqlite3" \
  --mapping "$sqlite_config/tables.json" --source-name factory_console \
  --source-id "$(cat "$sqlite_config/installation-id")" \
  --landing "$landing" > "$sqlite_config/job.json"
```

抽出が成功すると、内容に対応したSQLバッチと完了印を作り、`job.json` に
内容IDとUTCの抽出時刻を出します。 `job.json` の2項目を使い、下の手順で反映します。
抽出先のホストと基盤ホストが別なら、バッチ本体を先にコピーしてSHA-256を確認し、
最後に `.ready` を置きます。元のSQLite DBを基盤ホストへ同期する必要はありません。
SQLiteの全件取込から消えた業務行はSQLの公開世代に反映されます。
[SQLite専用のGitHub Actions](../../.github/workflows/sqlite.yml) は、同じ内容を再取得した時点、
削除、3世代のGold読取、同じ観測の再送、RawのSHA-256を照合します。

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

確定位置までのRawを隔離したIcebergに再生して照合する手順があります。
復元する履歴の範囲と、旧世代に含まれるNULL判定の条件を確認してください。
[CDCの確定履歴を隔離復元する](CDC-SHADOW.md)

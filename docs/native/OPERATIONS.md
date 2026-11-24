# 運用、バックアップ、受入試験

<!-- doc-purpose:start -->
> **役割:** 障害対応、状態の保全と合成データでの受入を行います。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 導入先で検証・運用するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [会社VMの受入手順と合格条件](<ACCEPTANCE.md>) / [公式リファレンスと採用バージョン](<REFERENCES.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## 毎日の確認

1. Dokployの対象Composeと **Docker → Containers** で状態を確認し、Grafanaで標準7件、閲覧構成を追加した場合は9件の監視先を確認します。
2. `catalog-migrate` の終了コード0と、Lakehouseのhealthyを確認します。
3. 取り込み後は件数とGold公開世代を控えます。 `JOB_FAILED` のときは Dokployの対象コンテナの **View Logs** で例外型・行番号を確認し、完了印、列、空き領域、監視状態を照合します。
4. 入力修正後に同じジョブを再実行し、アプリが示す公開世代が更新されたことを確認します。古い世代を読む利用者がいる間は、そのIceberg表のスナップショットを失効させないでください。

| 症状 | 確認すること |
| --- | --- |
| GitHub cloneが403 | 同じHTTPS URLで `git ls-remote` を試し、会社のproxy・許可リストを確認。DokployでGitHub Appを使用している場合は連携対象も確認 |
| pull/buildがTLSエラー | ホスト、Docker daemon、BuildKitのどの層で失敗したか。 [企業設定](COMPANY.md) |
| bind mountのpermission denied | landing作成者と環境設定のUID/GID、ディレクトリ階層の権限 |
| Grafanaが開けるがダッシュボードがない | provisioningのread権限とGrafanaログ。 [設定ファイル](MONITORING.md) |
| Goldが404 | `medallion` の成功、ファイル・SQL・CDCに合うAPIパス、指定世代の保持を確認。閲覧画面では「最新版を読み込む」で再確認 |
| CDCが停止 | broker接続、保有topic、stateの所有者、保持期限がcheckpointを超えたか |

CLIで運用する場合は、Dokployに表示される実際の `appName` を `docker compose -p` に指定し、配備と同じComposeファイルおよび非公開設定を指定します。

## Dokployで定期取込を実行する

配備先の管理画面で対象Composeの **Schedules** を開き、**Create
Schedule** を選びます。会社で使うDokployは修正済みの版を確認し、
試験時はv0.31.4の入力欄を基準にします。[Dokployの定期ジョブ](https://docs.dokploy.com/docs/core/schedule-jobs)
· [Dokploy v0.31.4](https://github.com/Dokploy/dokploy/releases/tag/v0.31.4)

| 入力欄 | 例 |
| --- | --- |
| **Service Name** | `lakehouse` |
| **Name** | `定期取込と業務集計` |
| **Cron Expression** | `7,22,37,52 * * * *` |
| **Timezone** | `Asia/Tokyo` |
| **Command** | `python -m native.api medallion --strict` |

最初に同じコマンドを `lakehouse` のTerminalで試します。**Run Manual
Schedule** で1回実行し、ジョブのログとGrafana、取込状態APIを照合します。
App Nameは配備時に決まるため、`COMPOSE_PROJECT_NAME` を別の名前へ
上書きすると対象コンテナを特定できなくなります。

正常完了と変更なしは終了0です。業務が公開され設備経路だけが更新保留に
なった場合は、結果JSONを出力して終了2を返します。別ジョブの実行中は
APIが409を返し、CLIは終了1になります。入力修正後に再実行します。
運用担当者は、投入側が完了印を確定する時刻と、実測した最長の処理時間を
基に実行間隔を選びます。トークンはコンテナの既存環境から渡され、Command
には書きません。[取込状態と認証](API.md#取込状態と公開中のデータ)

## バックアップと復元

保全対象はカタログDB、S3のSeaweedFSデータと設定、landing原本、private `site.env` と暗号鍵、Grafana volumeと設定ファイル、必要に応じてPrometheusデータ、CDC SQLiteのディレクトリ全体とその運用設定です。追加構成ではn8n専用DBと暗号鍵、OpenObserve、InfluxDB、Explorerのデータと資格情報も対象にします。DokployのVolume Backupsを使う場合、名前付きvolumeに加えてホストのlandingとCAを別途保全します。

1. 投入・変換・CDCを停止し、ジョブの完了を確認します。Dokployは対象のComposeだけをStopします。
   ワーカーはSIGTERMを受けると新規接続を止め、受付済みの要求が完了するのを待ちます。
   Composeの猶予は2分です。大きな原本や長い変換は先に完了を確認し、
   強制停止が発生した場合は公開世代と原本の台帳を照合してから再開します。
2. カタログとオブジェクトの同じ停止時点を、ホストのストレージスナップショットまたは運用で承認されたバックアップ手段で保全します。秘密は暗号化された保管先へ保存します。
3. 別VM・別projectに同時点の両方とprivate設定を復元します。会社側の実接続を作る前に、隔離状態で同じ公開世代とGold件数、S3原本hash、Grafana設定を照合します。
4. 更新の試験で問題が起きた場合は、採用版のGit commitとバックアップを揃えて隔離環境で復旧を検証します。DBのschema移行を行った後にコンテナタグだけ戻すと、DBが旧版に対応しない場合があります。

### PostgreSQL 18.7への更新

標準のcatalog-dbと任意構成のtools-dbは、PostgreSQL 18.7の
Alpine 3.24版をダイジェストで固定します。18.6からの通常の更新は、
同じメジャー版のデータを使い、ダンプ・再ロードや `pg_upgrade` を
必要としません。PostgreSQLの公式リリースノートには、一部のGIN・GiST
およびTOASTなどに関わる索引破損の修正と、該当する索引を再構築する
条件が掲載されています。DBAと利用中の拡張・索引を確認します。
[PostgreSQL 18.7の移行とREINDEX](https://www.postgresql.org/docs/18/release-18-7.html)

1. 更新前に依存するアプリと定期書込みを停止し、カタログと保存先を
   同じ停止時点で保全します。運用ツールを使う場合は、n8nのDBと
   暗号鍵も保全します。採取時のCompose、固定イメージと台帳SHA-256を
   合わせて保持します。
2. 隔離した復元先で採取時の固定版を起動し、復元できることを確認します。
   同じ隔離先を18.7へ更新し、DBの照合プロバイダーと記録・実際の
   照合バージョン、Gold公開世代と原本のSHA-256を更新前後で照合します。
   バージョンが違う場合やリリースノートに該当する場合は、必要な索引を
   調べ、再構築を検証してから公開を再開します。
3. 本番では利用者へ停止時間を案内し、会社の手順を使って同じCompose
   プロジェクトとボリュームを更新します。DBとカタログのヘルス、
   運用ツールの読取、採取前と同じGold世代・原本を確認します。

2026-11-24にLinux amd64の固定イメージを比較した結果、旧版と新版とも
Alpine 3.24.4、musl 1.2.6-r3、ICU 78.1-r0、tzdata 2026f-r0でした。
修正版があるOSパッケージのHIGH検出は20件から1件に減り、残りは
libxml2のCVE-2026-86144です。gosuのGo 1.24.6由来の
CVE-2025-68121も両方で検出されました。このCVEはTLSセッション再開時の
特定の条件を対象にするため、バージョン検出と実行時の利用可否を分けて
評価します。
[PostgreSQLの公式イメージ](https://hub.docker.com/_/postgres)
· [Goの該当アドバイザリ](https://pkg.go.dev/vuln/GO-2026-4337)
· [隔離復元と更新のCI](../../.github/workflows/recovery.yml)

### 停止済みのDockerボリュームを採取する

[保全コマンド](../../scripts/backup-native.py) は停止済みのFoundation Rと
追加サービスをDockerのプロジェクトラベルから見つけ、アタッチされていた
名前付き・匿名ボリュームを読み取り専用で採取します。PostgreSQL 18の
`pg_controldata` が正常停止を示すこと、稼働中のコンテナが同じvolumeを
使っていないことを先に確認します。収集中は他の管理者やAutoDeployに
起動されないよう運用を調整します。

例では標準構成の起動時と同じ `site` と `project` を使い、新規のバックアップ
出力をGit管理外に指定します。GUIなどを追加した配置では、停止時と再起動時に
配備時と同じComposeファイル・プロファイルをすべて指定してください。
出力先の親ディレクトリは所有者限定にし、保全に必要な空き容量を確認します。

**Bash / Zsh共通**

```bash
backup="$HOME/.config/lakehouse-foundation-r/backup-20261125"
docker compose --env-file "$site" -p "$project" stop
python3 scripts/backup-native.py capture --project "$project" "$backup"
python3 scripts/backup-native.py verify "$backup"
docker compose --env-file "$site" -p "$project" up -d --wait --wait-timeout 180
```

Dockerをsudoで利用する場合は採取コマンドに `--sudo` を付けます。
`capture` はサービスを停止・起動する操作を実行しません。エラーになった
採取先は `manifest.json` の有無を確認し、新しいディレクトリでやり直します。
`verify` は別のVMでも実行でき、配備元への接続が不要です。保全時の
`manifest_sha256` を別経路で保管しておくと、`--expected-manifest-sha256`
で台帳も照合できます。landing、CDCの `state`、非公開設定とCAは
配備先の管理領域から別に保全します。保全物は暗号化保管し、会社の
バックアップ権限と保持期間を適用してください。

[Docker公式のボリューム保全](https://docs.docker.com/engine/storage/volumes/#back-up-restore-or-migrate-data-volumes)
· [PostgreSQL 18のpg_controldata](https://www.postgresql.org/docs/18/app-pgcontroldata.html)

### 標準のDBと保存先を新しいボリュームへ復元する

`restore` は標準構成の `catalog-db` とSeaweedFSの `storage` を
空のローカルDockerボリュームへ展開します。対応する台帳は、固定イメージ
3種とCPUを含む形式2です。採取元の固定版が18.6・18.7のどちらでも、
台帳のイメージを補助処理と `compose.restore.yaml` に使います。
形式1の台帳やRustFSを使った採取物も
`verify` で照合できます。代替保存先、CDCのSQLite、監視データ、
ツール群、landingと非公開設定は個別手順で復元します。

1. 復元専用VMまたは外部の投入経路から切り離したDockerホストに、
   採取時のコミットと同版の設定を用意します。DBパスワード、カタログ暗号鍵、
   S3資格情報は、採取時に保全した非公開設定を引き継ぎます。
   `LANDING_DIR` は空の試験用ディレクトリを指定します。
2. 採取側と同じCPUの固定イメージをキャッシュに揃えます。バックアップ
   の `manifest.json` にある `native_core.images` の `catalog-db`、
   `catalog`、`storage` と、
   `native/Dockerfile` の先頭に指定されたPythonベースイメージを
   事前に取得します。復元処理はネットワークを使わず、
   足りないイメージを自動取得しません。
3. 暗号化保管先から原本を読み取り専用で参照できるようにし、保存台帳とは
   別経路で保全した `manifest_sha256` を用意します。
   `recovery` にはアプリ・ネットワーク・ボリュームのない別のApp Nameを使い、
   `restore_output` には所有者限定の親にある未作成のパスを使います。

**Bash / Zsh共通**

```bash
python3 scripts/backup-native.py restore --project "$recovery" \
  --output "$restore_output" --expected-manifest-sha256 "$manifest_sha256" "$backup"

restore_compose=(
  docker compose --env-file "$restore_site" -p "$recovery"
  -f compose.yaml -f "$restore_output/compose.restore.yaml"
)
"${restore_compose[@]}" config --quiet
"${restore_compose[@]}" up -d --no-deps --no-build --pull never \
  --wait --wait-timeout 160 catalog-db storage
"${restore_compose[@]}" up -d --no-deps --no-build --pull never \
  --wait --wait-timeout 160 catalog
```

復元先のDBが正常停止していることを確認し、2つの展開が完了した場合に
`compose.restore.yaml` が生成されます。`plan.json` のみが残る場合は
部分復元です。作成済みの外部ボリュームは保持されます。失敗原因と
`plan.json` の名前、ボリュームの `foundation-r.restore-project` ラベル、
他コンテナから参照されていないことを確認してから処置を決めます。
`docker compose down --volumes` で外部ボリュームは削除されません。
同じ出力先へ上書きせず、再試行には空の別App Nameと新しい出力先を使います。
標準の通常ファイルとディレクトリのみが対象で、リンク、特殊ファイル、
別マウントのPostgreSQL表領域を使う保全物は事前検査で中止します。

公開再開やDokployへの接続は、採取側で記録した公開世代、2つの業務Gold表の
行数とハッシュ、warehouse ID、Icebergの表IDとメタデータ位置、
Rawの個別SHA-256を
隔離側の読み取り結果と照合した後に行います。カタログ移行や再投入を
照合の前に実行すると、物理復元で元の版を読めたか判断できなくなります。
[合成データによる隔離復元のCI](../../.github/workflows/recovery.yml)
· [Dockerの外部ボリューム](https://docs.docker.com/reference/compose-file/volumes/#external)

### 停止時点をそろえる復旧リハーサル

採用したGitコミット、Composeの追加ファイル、プロファイル、Dokployの
`App Name`、イメージのダイジェストを、復元用の管理記録へ残します。
物理バックアップは元と同じPostgreSQLのメジャーバージョンとアーキテクチャ
の組み合わせに戻し、正常に読めることを確認してからバージョンを変更します。
[PostgreSQL 18のファイルシステムバックアップ](https://www.postgresql.org/docs/18/backup-file.html)

| 保全対象 | 復旧時に必要なもの |
| --- | --- |
| `catalog-db` と `storage` | 同一停止時点のPostgreSQL全体とSeaweedFSデータ。片方だけを別時点から戻すとカタログ参照と実体が食い違います。 |
| CDCの `state` と接続先の保持範囲 | SQLiteファイルのディレクトリ全体、確定offset、購読先のtopic・partitionごとの開始可能位置。`state` はCDC追加構成で保存します。 |
| `landing` とprivate設定 | 未処理原本・完了印・受付票、操作と読取の資格情報、追加機能の鍵・企業CA。アクセス制限と暗号化を適用します。 |
| 追加ツールの状態 | Grafanaの手作業で追加した設定、n8nの暗号化鍵と永続DB、時系列DB等。構成ファイルで再生成できるものを棚卸しして保全対象に決めます。 |

停止したコンテナのマウント名をDockerのラベルや `volume inspect` で
照合し、指定したボリュームだけを読取専用で採取します。PostgreSQLは
`pg_controldata` のクラスタ状態が `shut down` の時点を選びます。
S3互換ストレージ・カタログ・状態DBのアーカイブには個別のSHA-256を
記録します。アーカイブを接続のない別プロジェクトへ展開する際は、
書込みコンテナの起動前に展開先に既存のファイル・リンクがないことを
調べます。所有者と権限は保存元に合わせます。
[Docker公式のボリュームのバックアップと復元](https://docs.docker.com/engine/storage/volumes/#back-up-restore-or-migrate-data-volumes)

アプリ・カタログ・保存先を先に起動し、外部の投入、CDC、Cloudflareへの
接続は止めた状態で、対象表の公開世代、行数、原本ハッシュを照合します。
続けてSQLiteの `PRAGMA integrity_check` と確定offsetを照合します。
復元先が同じKafka consumer groupを使う場合、元と復元先を同時に接続すると
partitionの割り当てが変わるため、切り替え時に元を停止します。

#### 合成データの復元結果

2026-11-24に、独立した2つのComposeプロジェクトで、3領域の停止時点
バックアップと別ボリュームへの復元を行いました。公開世代の生成や
再取込を挟まず、復元した状態を読み取りました。

| 照合対象 | 復元前後の結果 |
| --- | --- |
| 合成ファイルの原本 | S3互換保存先から11件を再取得し、個別SHA-256と台帳が一致 |
| 標準ファイル経路 | 14行のハッシュ、公開世代 `8005438907150430096`、表スナップショット `2810786987315192321` が一致 |
| CDC | offset 0–8の9イベントのハッシュ、公開世代 `6265858029840293004`、SQLite確定offset 8が一致。整合性検査は `ok` |
| 失敗時の扱い | 復元先のSeaweedFSが作る空ディレクトリを認識し、既存のファイルまたはシンボリックリンクが見つかる場合に展開を中断 |

固定合成データに対する読み取り照合は [検証スクリプト](../../tests/integration/native/snapshot_evidence.py)
で再現できます。同じ入力で起動した両方の `lakehouse` コンテナへ
Python標準入力で流し、出力したJSONを比較します。外部Kafkaブローカーの
障害復旧、バックアップ間隔、復旧所要時間は導入先で測定してください。

## 公開世代の来歴と保持状態を確認する

対象の `lakehouse` コンテナ端末で、次のコマンドを実行します。
設置先IDは英小文字で始まる63文字以内の英小文字・数字・ハイフンにし、
同じ来歴システムへ出力する設置先同士で重複を避けます。

**Bash / Zsh共通**

```bash
python -m native.lineage --site factory-test
python -m native.catalog_audit
```

省略時はファイル経路の最新公開を確認します。両方とも
`--channel cdc` または `--channel sql`、および
`--release <APIで確認した公開世代ID>` を受け付けます。
64ビットのスナップショットIDは、JSON数値の丸めを避けて文字列で維持します。

- 来歴のJSONLには、その公開世代が採用した派生表の版と、生成した公開マニフェストの版が記録されます。部分公開で設備の旧版を保持した場合も、実際に採用した版を出力します。同じ世代の再出力は同じRun IDと時刻になります。開始時刻の記録がない公開世代は表の版を示す `DatasetEvent` になります。出力先を指定するまで標準出力だけを使います。 [OpenLineageのイベント](https://openlineage.io/docs/spec/object-model/) · [DatasetVersionの仕様](https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json)
- 保持状態のJSONには、指定世代から参照するBronzeと派生表のメタデータが残っているかを出力します。失われた参照があると終了コード1です。データファイルまで照合する際は、復元環境でのハッシュ比較とGold読取も実施します。
- 複数世代を保持する場合は、業務・設備の公開記録に残る世代ごとに確認します。このコマンドは表やデータファイルを変更しません。外部システムに来歴を保存する場合は、送信先の権限と設置先ID・表名の持ち出し可否を運用者が確認します。

### 公開世代が参照するS3ファイルも調べる

物理復元の照合後やIcebergの履歴整理を検討するときは、
`--objects` で公開世代の参照先を追加で調べます。
選択した経路・世代の公開表、入力表、派生表を対象に、S3 HEADで
Icebergの表メタデータ、マニフェスト一覧、マニフェスト、有効な
データファイルと削除ファイルの存在を確認します。マニフェストにサイズが
記録されている項目は、その値とS3のサイズも比較します。

**Bash / Zsh共通**

```bash
python -m native.catalog_audit --objects --channel files   --release "$release_snapshot_id" --max-objects 2000
```

`storage.all_objects_available` がtrueで終了値0なら、この照合に成功しました。
`issue_counts` の `NOT_FOUND` は存在しないと応答された件数、
`SIZE_MISMATCH` はサイズ不一致です。 `HEAD_FAILED` は権限・疎通を先に
確認します。上限に達した場合や、32 MiBを超えるマニフェストを検出した場合は、
`enumeration_complete` がfalseになり、終了値1を返します。出力は表名、数値と
位置のSHA-256を含みます。原因を調べるときは、管理権限で参照先の位置を同じ
方法でハッシュ化して照合します。

対象は選択したIceberg世代の参照先です。Raw原本のSHA-256、同じサイズの内容の
変化、業務集計の正しさは復元手順とGoldの照合で確認します。履歴整理では
保持する世代をそれぞれ確認し、削除・期限変更の前に別の場所への保全を済ませます。
[Apache Icebergのテーブル仕様](https://iceberg.apache.org/spec/)
· [S3 HEADと権限](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)

## イメージと依存のセキュリティ確認

上流イメージのタグとdigestは両方固定し、Pythonの依存は版とhashを
固定しています。配備先で審査に提出するときは、対象コミット、イメージID、
CPUアーキテクチャ、スキャナー版、脆弱性データベースの更新時刻を記録します。
タグが同じであってもビルド時の基点と実行イメージを区別します。

1. 配備したComposeのサービス名とイメージIDは、Dokployの対象サーバーの
   **Docker → Containers** または配備先の `docker compose images` で確認します。
2. 担当者が導入したTrivyで各イメージを調べます。 `image` の値を確認した
   イメージIDへ置き換えます。レジストリへ取得に行かず、Dockerに存在する
   イメージだけを使用します。初回はTrivyの脆弱性DBを取得する通信が必要です。
   社内でDBを配布する場合は [TrivyのDB設定](https://trivy.dev/docs/latest/guide/configuration/db/) を参照します。
3. HIGH以上は、対象のパッケージ、修正の有無、入力やネットワークから
   到達する経路、配備した組織の判断を記録します。上流が発行した修正版は
   digestを更新し、隔離配備、原本・公開世代の照合、復元を行ってから切り替えます。

**Bash / Zsh共通**

```bash
image=REPLACE_WITH_LOCAL_IMAGE_ID
umask 077
trivy image --image-src docker --scanners vuln --severity HIGH,CRITICAL \
  --format json --output image-vulnerabilities.json "$image"
```

2026-11-25 JST、x86-64で16の実行イメージをTrivy 0.76.2で検査しました。
同じデータベース（2026-11-24 13:06 UTC更新）で更新候補も比較しました。
HIGH／CRITICALはパッケージ検出件数です。

| 対象 | 検出件数の比較と判断 |
| --- | --- |
| ワーカー・閲覧画面 | 各59→53 HIGH。実行イメージから依存のインストールに使わないpipを取り除き、公式Pythonベースを更新したところ、修正版ありの6件が各イメージで解消。残る53件にスキャン時点の修正版の記載なし |
| Grafana OSS | 13.3.0→13.3.2で54→15 HIGH、CRITICALは両方0 |
| PostgreSQLイメージの `gosu` | TrivyがGo標準ライブラリのCRITICALを検出。Goのバイナリ解析から該当TLS関数の到達性は確定できず、継続確認 |
| InfluxDB Explorer | Debian BookwormのzlibでCRITICALを1件検出。Debianは該当MiniZipをBookwormのzlibバイナリに含めていないと記載。同梱のOSパッケージ一覧も確認。組織で利用条件・更新時期を確認 |

別に、リポジトリのPythonロック19ファイルに含まれる148種類の版を
PyPIの版別JSONで確認し、掲載された脆弱性情報は0件でした。この照合結果には
OSや個別イメージの組み込み依存は含まれないため、配備時は両方を確認します。
[Trivyの検出範囲](https://trivy.dev/docs/latest/guide/scanner/vulnerability/)
· [PyPIのJSON API](https://docs.pypi.org/api/json/)
· [Goの脆弱性情報](https://pkg.go.dev/vuln/GO-2026-4337)
· [DebianのMiniZip判定](https://security-tracker.debian.org/tracker/CVE-2023-45853)

## 受入の手順

- `python3 native/check_contracts.py` で業務SQLと契約のhashを照合します。
- Dokployの対象コンテナの **Terminal** から `python -m native.api verify` で固定合成データの11検査を行います。
- 専用の空の検証projectで `tests/integration/native/acceptance.py` を実行し、センサー、公開世代の固定、読み書きトークンの分離、Grafanaのプロビジョニングを確認します。このテストは指定landingへ合成ファイルを書き込みます。
- 会社の受入では、Git取得と認証、デーモンpull、BuildKitの依存取得、配置後の接続、実データと運用上限、バックアップからの復元をそれぞれ確認します。

監視設定の検査は [Grafanaの設定変更手順](MONITORING.md) にあります。契約やソース変更をレビューする場合は、 [品質検査規約](../../CONTRIBUTING.md) とCIの `Foundation R 標準構成の受入検査` および既存の `Validate public source` を確認します。

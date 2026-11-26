# Foundation R 段階3: 隔離CDCインフラ

<!-- doc-purpose:start -->
> **役割:** Kafka、Connect、実行イメージの検証と復旧の境界を整理する。
>
> **対象読者:** 基盤構築担当 / 開発・設計担当 / 運用担当
>
> **読むタイミング:** 段階3の隔離環境と配布物をレビューするとき。
>
> **前提となる資料:** [Foundation R内包CDCの現状と設計差分](<cdc-self-contained-design.md>)
>
> **次に読む資料:** [内包CDCの受入と配布物の復旧](<cdc-self-contained-acceptance.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

この構成はApache Kafka 4.3.2のJVM版 (KRaft) とDebezium SQL Server
3.7.1.FinalのKafka Connect workerを評価するものです。既定のCore、既設CDC、
従来のRedpanda受入、実データの取り込み先、配備設定は変更しません。
本手順では社内サーバー、既設のDokploy App、registry、SQL Serverへ接続しません。

## 起動前に止める条件

* 段階2の[専用SQL契約](cdc-self-contained-design.md)を同じDBと設置IDで読み取り
  照合する。DBAが独立に承認しない限りSQL作成、捕捉再作成、新しいGRANTは行わない。
  旧経路のSQLアカウント、topic、offset、schema history、volumeは流用しない。
* Kafka 4.3.2には標準イメージのJavaだけでは使えない既知の問題がある。
  brokerとConnectの**実際に起動した**JVMそれぞれでJava 21.0.13.1+2以上の
  承認済みJava 21を確認する。バージョン表示だけを攻撃の再現で代用しない。
* Debezium 3.7はKafka Connect 4.3.1でビルドされ、上流のbroker検証も
  4.3.1。4.3.2との互換性および限定SQLロールでのsnapshot/streamは、
  ここで合成環境の実測が通るまで未確認。
* 固定した上流tagの参照先が変わった場合は `UPSTREAM_CHANGED` として
  新規の承認・昇格を停止する。別digest、raw tag、別の配布場所へ自動で切り替えない。

## 分離と容量

Dokployの構成候補は専用の `compose.cdc-stage3.yaml` 一つを、Coreとは異なる
`foundation-r-cdc-stage3` プロジェクト／専用Appで評価します。
`stage3` プロファイルを意図して選んだ場合のみ対象となります。
既設のComposeファイルとは重ねず、通常のCore Appには追加しません。
管理APIやKafkaポートをホスト、プロキシ、Public DNSに公開しません。
broker内部ネットワークとConnectのSQL用経路を分離し、SQL宛通信の許可は
運用側で接続先一つに制限する前提です。Dockerのブリッジだけでは
宛先制限を証明できません。Coreのbroker接続、信頼境界、外向き経路を伴う
実サイト組み込みは別の設計・承認です。

初期の単一broker／単一workerは障害切替や本番容量を保証しません。
レプリケーション係数は評価用に1、全topicを評価用に1 partitionとします。
CDCデータは72時間保持の `delete`、config・offset・statusは `compact` のみ、
configは必ず1 partition。schema historyも1 partitionで `delete` のみ、
時間もbytesも無期限です。保持契約のbytes無制限はディスク無制限を意味
しません。最大流入量・1レコード最大サイズ、segment/compaction用余裕、
72時間データと履歴の増分、volumeのクォータ、停止基準を実測して承認します。
平常時のRAM、CPU、ディスク増加、lag、再起動後の追い付き時間を別に記録し、
証拠が揃うまで運用容量として案内しません。

## 配布物の証跡と復旧

上流の参照は「tagの到達先」「OCI index」「linux/amd64 manifest」
「そのconfig」の四つを区別し、ビルドした実行イメージも同様に識別します。
保存したOCI tarのSHA-256はこれらとは別です。tarに含まれるlayerと
configもすべて読んで照合し、最終的に使い捨て環境で読み込んだ
linux/amd64 manifestと実ユーザー・JVMを照合します。Dockerのcontainerd
では対象platformのIdはmanifest digestであり、config digestではありません。
OCI検証でmanifestからconfigとlayerのバイトを別途追跡し、起動後に
コンテナ自身が選んだplatform manifestも再照合します。metadata確認のみを
Docker実行、あるいは空の環境からの復旧の証拠と見なしません。

承認済みの実行イメージは、入力のdigest/JVM/作成方法/出力manifest/
config/アーカイブSHA-256とともに、承認済みの管理registryの不変参照、
またはVMとは異なる保管先の検証済みOCIアーカイブとして保持します。
CIは検証・一時アーカイブ作成まで。管理registryへの登録も実サイトへの
復旧も実行しません。配布先の権限、消去保護、保管期間、所有者は別に承認します。

開始前と再起動前に、対象cluster ID、固定topic識別、設定、既知のoffset／
履歴を照合します。offsetとschema historyは**イメージのアーカイブとは
別の状態**です。実サイトの復旧手順では、停止時点の専用broker状態と
保護された登録設定、台帳を一組として保存します。不一致、欠落、読取不能、
旧stateとの混在時は停止します。別の新規topicへ同じ名前を流用したり、
既設stateを移動、リセット、削除したりしません。実運用で新旧ランタイムを
切り替える操作やバックアップからの状態復元は、本合成受入とは別承認です。

## 検証入口

[空欄の非秘密テンプレート](../../infra/cdc-stage3/site.env.example)を参考に、
実設定はアクセス制限した別ディレクトリの0600ファイルへ写します。
SQLユーザー・パスワードとtruststoreのパスワードは、テンプレートや環境変数の値
ではなく、専用UID 1001だけが読める別のpropertiesファイルを参照します。

* 監査済みの管理registry参照を明示するか、明示的に `--oci-receipt` を選びます。
  OCI receiptは承認を済ませた別保管のJSONで、トップレベルは `kafka` と
  `connect` の二件。各項目のキーは `path`（tarの絶対パス）、
  `archive_sha256`、`manifest`、`config`（それぞれsha256形式）だけです。
  OCI各tarとreceiptは所有者のみ読めることを要求します。
  `plan` は保管tarを照合し、コンテナやregistryには触れません。
  オペレーターが照合済みtarを隔離daemonへ読み込み、承認済みconfigの全64桁を
  含む `foundation-r-cdc-stage3-kafka:oci-<config>` と
  `foundation-r-cdc-stage3-connect:oci-<config>` に命名したうえで
  `verify-local` で別名のLinux/amd64 manifestを照合します。検証する操作ごとに双方の
  アーカイブ検査を繰り返します。不一致への自動fallbackはありません。
* 意図的に新規構築する場合だけ `topics --initialize` と
  `register --initialize` を段階ごとに用います。初回の検査用変更データと
  確定LSN・schema historyを確かめてから `capture-state --state-receipt`
  で私有receiptを新規保存します。同じ名前のファイルは上書きしません。
  復旧ではbrokerだけを起動し、`check-broker --state-receipt` で
  同一クラスタと全topic ID、最新の確定offset recordと保持schema historyの
  完全なハッシュ一致を求めます。Stage 3の試験上限を超える64件超の履歴も
  不完全な読取りで許可せず停止します。Connectを起動するのはこの後です。
  起動後は `check --state-receipt` でRESTの確定LSN、非空で逆行しない
  offsetとschema historyを別途検査します。同じDBの別系統の設定は触りません。
  復旧点が監査できないときは初期化へ戻らず停止します。
* `write-map --topic-map` は、DB名を含む六つの完全topic名をキーとする
  非公開ファイルを作ります。各項目は段階2の全列と既存Landing先の一致を要求
  し、実brokerのデータtopicもこの集合と一致しなければ停止します。
  Coreの型変換・購読・書込を接続する操作ではありません。

## 合成受入の合否

1. 使い捨てSQL Serverで段階2の既存コードを使用。専用schemaと六つの捕捉
   を確認し、生成した試験アカウントと一時CAを使う。
2. 実brokerのtopic契約と実Connect workerの版、SQL Server plugin、
   Connectorとtask双方のRUNNINGを確認する。同一登録は無変更、未知設定は停止。
3. 対象の六表に合成行を一件ずつ入れ、六つの実topicごとにキー、全列、
   source DB・schema・table・opを照合する。品目表ではコミットした
   INSERT・UPDATE・DELETEとtombstoneを観測する。初回のみのsnapshotを
   再起動後に再実行しない。
4. 同じ専用状態でConnectのみ、次にbrokerのみを再起動し、相手側の
   コンテナと開始時刻が変わらないこと、既存状態と次のコミットの到達を検査する。
5. コンテナを停止し、同じ専用stateを保持したまま、承認済みOCIから実行
   イメージを冷間ロードする。故意に別の合成イメージをローカル別名に付けた
   場合、起動前に拒否します。その後、合成条件で異常終了コードを返す
   workerの実起動失敗を観測し、その一個だけを除去します。SQLへ接続する
   前にbrokerを再照合して、検証済みの元の構成で新しいコミットの到達を
   確かめます。これは初回リリースの**起動失敗からの構成復旧**であり、異版への
   ダウングレードや、異常なランタイムが状態を書き換えた後の巻き戻しは未検証。オフセットはリセットせず、システム全体の
   exactly-once、障害切替、実サイトのGold到達の保証とは区別する。

この文書の設計記述は実施済みの配備、互換性、復旧、容量の証明ではありません。
実行していない段階、診断された互換性・権限要件の差は未完了として記録します。
生のConnector設定、trace、payload、秘密、SQL識別子をCIログへ出力しません。

参照: [Apache Kafka 4.3.2リリース](https://kafka.apache.org/blog/2026/10/23/apache-kafka-4.3.2-release-announcement/) /
[Apache KafkaのJDK案内](https://kafka.apache.org/blog/2026/11/19/mitigating-cve-2026-114938-java4shell-in-apache-kafka-deployments/) /
[Debezium 3.7リリースノート](https://debezium.io/releases/3.7/release-notes.html) /
[Debezium 3.7.1.Final SQL Server接続の実装](https://github.com/debezium/debezium/blob/v3.7.1.Final/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerConnection.java)。

# Foundation R内包CDCの現状と設計差分

<!-- doc-purpose:start -->
> **役割:** 現行の証拠と未実装範囲を区別し、追加する契約と配置を確認する。
>
> **対象読者:** 基盤構築担当 / 開発・設計担当 / 運用担当
>
> **読むタイミング:** SQL、topic、配備の後続差分を作る前。
>
> **前提となる資料:** [ADR: Foundation Rに閉じたCDC取り込みを追加する](<cdc-self-contained-adr.md>)
>
> **次に読む資料:** [内包CDCの権限境界と否定試験](<cdc-self-contained-threat-model.md>) / [内包CDCの受入と配布物の復旧](<cdc-self-contained-acceptance.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

SQLの専用契約を段階2で実装します。独立BrokerとConnectの段階3の構成・
実SQLの隔離受入は[段階3の設計と配布物](cdc-stage3-infrastructure.md)を参照してください。
Coreの連結・実サイトの配置はここでは扱いません。以下は設計用の合成例です。

## 現行の証拠と不足

| 追跡箇所 | 何が確認できるか | 何を証明しないか |
| --- | --- | --- |
| [CDC Compose](../../compose.cdc.yaml)、[Native CDC入口](../../native/cdc.py) | 既存ブローカーにつなぐConsumerをCoreに追加する | ブローカー、Connect、SQL Serverの配備 |
| [CDC CI](../../.github/workflows/cdc.yml)、[合成イベント準備](../../tests/integration/native/prepare_cdc.py) | 隔離ブローカーと合成イベントを使う現行の受入経路 | 実SQLと実Debeziumの接続、限定readerの権限 |
| [SQL Server CI](../../.github/workflows/sqlserver.yml)、[既存SQLデモ](../../scripts/sqlserver_demo.py) | SQL側入力や抽出の別試験。既存の安全拒否は維持する | 1環境でSQLから非空Goldまで届くこと |

継続性、失敗後の再開、毒イベントの隔離といった既存CDC回帰は基準にしますが、新経路の連結合格に数えません。既存のConnector補助の環境参照・比較は再利用を検討し、既存の接続値・登録先・登録処理は持ち込みません。型付きアダプターは明示的に選んだ内包モードだけの入力境界です。従来モードの設定、既存入力の解釈、確定・再開の挙動に波及させません。

## 入力契約とプロセス

合成DBを `demo_r`、専用スキーマを `foundation_r_demo` とした設計例です。四つの業務表と二つの設備表を対象にし、一つの許可契約にSQL表、捕捉列、型付き主キー、topic、格納先を定義します。

| 用途 | 合成の入力例 | 契約上の照合先 |
| --- | --- | --- |
| 品目・作業指図 | `products` / `work_orders` | 既存の `erp_products` / `erp_work_orders` |
| 受注・出荷 | `sales_orders` / `shipments` | 既存の `erp_sales_orders` / `erp_shipments` |
| 設備実績・停止 | `equipment_events` / `equipment_stops` | 既存の `mes_events` / `mes_downtime` |

捕捉インスタンスは専用接頭辞、一意、100文字以内とし、照合順序・大文字小文字・所有者も調べます。未作成なら加算的に作成、完全一致なら維持、不一致なら停止です。DB照合前には変更しません。既存SQLデモの共有DBに対する既定拒否と、厳密な接続先一致を要求する例外を緩めず、新モードには専用セットアップを別段階で作ります。DB全体削除、CDC無効化、共有ジョブ再作成は通常手順に含めません。

Topic名は候補版の実出力と照合し、`demo_r_cdc.demo_r.foundation_r_demo.products` のような接頭辞・DB・スキーマ・表の4部分を許可契約に記述します。専用Consumerは `topic_has_database=true`、既存validatorに合う `lakehouse-r-native-cdc-v1` を候補にします。文字列の区切りをエスケープした全体一致の選択と実イベントの `source.db`、schema、table、型付きkeyの再照合を併用し、二重購読しません。

* JSON converterのschema有無、Decimal表現、精度と丸めの扱いを固定します。NULLと空文字、bitの真偽値、Unicode、Decimalの桁、datetime2の精度と時刻解釈を別に検証します。既存の汎用取り込みでのNULL・bool・数値時刻の扱いを専用モードの保証に流用しません。時刻列と更新版の順序、UTCへの写像と精度上限も受入試験で固定します。
* snapshotの読み取り（r）、追加（c）、更新（u）、削除（d）ごとにbefore/afterとキーを照合します。値のない削除補助イベント（tombstone）は、許可topicと型付きkeyで判定し、削除そのものと区別します。認める加算DDLと拒否する破壊的変更は別テストです。
* 再開前には、Broker cluster IDに加えてtopic ID（取得不能なら作成時に明示更新する世代ID）、partition集合、ソース・捕捉の識別、契約hash、確定先と状態DBの由来を照合します。同じクラスタ内でtopicを作り直した場合も同一扱いにせず、証拠不足なら停止します。自動offset resetは行いません。

## 段階2: DBA用SQLの専用契約

[DBA用入口](../../scripts/cdc_sql_contract.py) は接続先を省略できず、通常は
読取のみです。既存のSQLデモと異なる専用スキーマ、設置マーカー、読取専用ロール、
合成投入専用ロールを使用します。6表の列名、順序、型・桁・NULL、固定の主キー、
専用capture名とcapture列を照合します。既存のLanding契約は変更しません。

1. DBAが対象DBを別経路で照合し、SQL Server 2022、対象DBのCDCが有効、既存の
   capture・cleanup両ジョブがあること、別系統に稼働可能な既存captureがあること、
   変更作業の排他を確認します。DDLが見えるだけでは足りず、実表と変更表の両方、
   追跡有効、削除待ちでないことと有効な開始LSNを検査します。SQL Server 2022の
   [隔離CIでの観測](https://github.com/hiromura16/lakehouse-foundation-R/actions/runs/45585080982)では、
   実際に番兵のINSERTを捕捉し、カタログと取得関数の有効な開始LSNが
   一致していても、削除待ちフラグが未記録（NULL）でした。NULL自体は稼働の
   証拠にせず、実表・正規の変更表・追跡・非ゼロの開始LSNと取得関数の一致を
   すべて要求します。明示された削除待ちは拒否します。新設直後の専用6表は
   LSN初期化を要求せず、同じ名前・実体・追跡・列契約と、明示された削除待ちがないことを確認します。レプリケーションと併用するDBは今回対象外です。
   アプリはDBのCDC、Agent、初回captureを設定・起動せず、不足なら停止します。
   DBAはサーバー／DBで有効なDDLのフック（トリガー等）が対象の操作に
   別系統への副作用を起こさないことを確認します。SQL ServerがCDC管理用に
   用意した内部トリガーを一律に無効化する手順ではありません。
2. DBAの接続設定はリポジトリ外の所有者専用ファイルに置きます。既存の認証・TLS
   項目 `SOURCE_SQLSERVER_HOST`、`SOURCE_SQLSERVER_PORT`、
   `SOURCE_SQLSERVER_DATABASE`、`SOURCE_SQLSERVER_USER`、
   `SOURCE_SQLSERVER_PASSWORD`、`SOURCE_SQLSERVER_CA_FILE` と、設置担当が
   別途照合した `CDC_EXPECTED_DATABASE`、`CDC_EXPECTED_BROKER_GUID`、
   `CDC_INSTALLATION_ID`（小文字hex 32桁）を明示します。DB名とService Broker
   GUIDは同じ対象を指すことを接続後に確認します。GUIDが複製されたDBの区別は
   この値だけではできないので、DBAがTLSの接続先と復元履歴も照合します。
3. `python scripts/cdc_sql_contract.py --settings /path/to/private.env` で検査し、
   空なら `ABSENT`、期待する設置と完全一致なら `VERIFIED` です。承認後に
   `--apply` を追加した場合に限り、新規の専用範囲を作成します。途中の拒否、
   名前や形の衝突、別の設置ID、同名の実体への置き換えを自動修復しません。
   「変更なし」は設置の一致であり、Agentの稼働やConnectorの接続は証明しません。
4. 実行主体へのロール追加は別途DBAが審査します。readerには6表とSQL Serverが
   作成する限定CDC取得関数へのSELECTだけ、writerには6表のI/U/DとSELECTだけを
   設定します。対象関数の識別子・作成日時・SQL側で計算した実装全文のハッシュを
   設置時に固定し、想定外の
   行レベルセキュリティ（RLS）が付いた場合も停止します。この照合は専用ロールの
   直接付与と所属先が対象です。実アカウントが他ロール・public・既存の
   所有権チェーン等から継承する権限は別途DBAが現地で検査します。
   非公開ログインを作成しません。本段階で確認するのは限定readerの
   SQL取得関数であり、Debeziumのdirect/functionの選定やstream動作の保証では
   ありません。隔離CI以外でDBに変更は加えません。

公開するのは合成契約と手順だけです。既存データ・他capture・共有ジョブの削除、
DB全体のCDCや設定の変更、対象の破棄や巻き戻しはこの入口に実装しません。
復旧が必要な場合は、既存系と保持位置を保存し、DBAが個別に承認してください。

## Topicの保持・起動契約

Connectには専用コネクター名 `foundation-r-sqlserver-cdc` と専用worker groupを割り当てます。内部状態とデータを同じ保持設定にしません。

| Topicクラス | Partition | Cleanup | 時間 / bytes の契約 |
| --- | --- | --- | --- |
| 変更データ | 合成受入は各topic 1。増設時はキー順序と遅延を再検証 | delete | 72時間を初期評価値とし、bytesはpartitionごとの実レートから設定する。容量制限が想定保持を先に削らないことを測る |
| Connect config | 1 | compactのみ | 時間・容量の削除上限は無制限。生きた設定キーを削除させない |
| Connect offset | 採用負荷に対して固定。再起動で数を変えない | compactのみ | 時間・容量の削除上限は無制限。復帰に必要な位置を失わない |
| Connect status | 採用負荷に対して固定。再起動で数を変えない | compactのみ | 時間・容量の削除上限は無制限。稼働観測の状態を保持し、確定位置には使わない |
| Debezium schema history | 1 | deleteのみ。compaction禁止 | 時間・容量とも無期限。リセットせず順序付きで読み直せることを検証 |

本表は受入契約であり配備済み設定ではありません。無制限はディスク無制限の意味ではなく、compactの削除マーカー保持や掃除の作業容量を別に検証します。offset/statusの個数、データbytes、内部容量は実測と承認後に固定します。採用した個数・cleanup・時間とbytesを明示し、readinessで実効値に完全一致することを確認します。履歴が必要な再起動では台帳上のtopicと履歴を読み戻せること、関連offsetを保持することを確認し、欠損・不一致・読取不能を空の新規状態として扱いません。

## 配置と準備確認

分離配置ではCDCインフラAppとCore Appは別に管理し、所有が確認済みの専用ブリッジでCoreからBrokerだけへ接続します。Connectから承認済みSQL Serverへの経路だけを許可し、管理REST、SQL、ホストポートは既定で公開しません。ローカル複合ではComposeの定義を重ねず、合成結果を静的に確認します。将来の配備プランは `cdc-infrastructure`、`cdc-full-local`、`standard-with-cdc` を区別します。

DokployのApp間起動順に頼らず、DB前提、Brokerとtopic契約、Connect worker、同一設定での登録、Connectorとtask双方のRUNNING、合成イベントの観測、Core確定の順に期限付きで確認します。実効デフォルトを含めて設定を照合し、機密項目は参照同士だけを比較します。一致なら再実行は無変更、差分なら停止。RESTの設定取得内容、差分値、traceを出力せず、上書き・削除・自動offset resetをしません。

参考: [候補版のSQL Serverイベント試験](https://github.com/debezium/debezium/blob/v3.7.1.Final/debezium-connector-sqlserver/src/test/java/io/debezium/connector/sqlserver/SqlServerConnectorIT.java#L1044-L1056) / [Debeziumのtopic要件](https://debezium.io/documentation/reference/3.7/install-kafka-connect.html#_configuring_debezium_topics) / [Dokploy Composeの機能](https://docs.dokploy.com/docs/core/docker-compose/utilities)

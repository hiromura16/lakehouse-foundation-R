# ADR: Foundation Rに閉じたCDC取り込みを追加する

<!-- doc-purpose:start -->
> **役割:** 内包CDCを任意構成として追加する理由、代案、取り消し条件を整理する。
>
> **対象読者:** 基盤構築担当 / 開発・設計担当 / 運用担当
>
> **読むタイミング:** 追加構成の採否と責務を設計するとき。
>
> **前提となる資料:** [アーキテクチャと業務契約](<../native/ARCHITECTURE.md>)
>
> **次に読む資料:** [Foundation R内包CDCの現状と設計差分](<cdc-self-contained-design.md>) / [内包CDCの権限境界と否定試験](<cdc-self-contained-threat-model.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

状態: **設計採択候補。実装・配備・受入はいずれも未実施。**

## 決定

変更データ捕捉（CDC）は、SQL Serverの変更を、取り込み側が定期的に表全体を読み直すことなく受け取る方式です。本設計では、独立運用が必要なときだけ、次の経路を既存の基盤に追加します。

**SQL Server → Debezium（Kafka Connect）→ Kafka → Native CDC → Raw → Silver → Gold**

既存の外部ブローカー利用と標準構成を維持します。CDCを内包する構成は明示的に選んだときだけ有効にし、GUIや外部配送はデータ確定の前提にしません。標準取込と既存の外部ブローカー用Consumerは、設定・入力契約・確定の挙動を維持します。内包モードだけに厳密な型と出所の照合を行う入力アダプターを追加でき、検証後は既存Coreの確定境界へ渡します。KafkaとConnectは選択したCDCインフラ区画に限ってJVMを使います。

| 選択肢 | 判断 |
| --- | --- |
| 外部の既存ConnectとKafkaに共用する | 現行モードとして有用ですが、障害や設定変更、履歴、資格情報の責務が別環境にまたがります。独立構成の既定にはしません。 |
| 既存CIのRedpandaを配備標準にする | 合成イベントを使う回帰治具としては維持します。新構成の互換・復旧条件はApache Kafka KRaft（ZooKeeperを使わない構成）で検証します。 |
| 単一VMに独立インフラを置く | 独立した設定・停止・復旧ができる一方、VMと物理ディスクの障害を共有します。RF1・minISR1は隔離実証用であり、冗長構成ではありません。 |

Kafkaの比較基準は4.3.1、修正版の評価候補は4.3.2、SQL Server Connectorの評価候補はDebezium 3.7.1.Finalです。公開版の存在と互換性の証明は別です。上流タグ付きソースと実際の配布イメージのConnectバージョンを混同せず、合成SQLからGoldまでの同じ試験で、稼働中のBroker 4.3.2、実Connectランタイムの版、Connector 3.7.1.Finalの版とロード済みプラグインを採用台帳に照合してから確定します。Connectの版を上流タグから推測せず、契約と異なる版・読めない履歴・再開不能なら準備完了にしません。配布イメージの同梱物は各本体のライセンスとは別に審査します。

## 所有と取り消し

ブローカー、Connect、登録確認処理をCDCインフラ側の所有物とし、Native CDCと状態・公開世代はCore側の所有物とします。ソースDBを共有するケースでも、許可されたDBの接続先以外の識別子・サービス・権限・永続領域を持ち回りません。共有DBのログとCDCジョブは共通障害点なので、別アプリにするだけで共有影響がなくなるとはみなしません。

次の段階に進めないときは対象の新規モードだけを停止し、既存モードは維持します。SQL CDC、KafkaとConnectの履歴・オフセット、Raw・Silver・状態DBを個別に捨てて再開しません。イベント形式、履歴、DDLの互換性が未確認なら既存volumeを旧版につなぎ直さず、保全して判断を待ちます。データ削除、既存DBの設定変更、本配備はいずれも別承認です。

参考: [Apache Kafka 4.3.2リリース](https://kafka.apache.org/blog/2026/10/23/apache-kafka-4.3.2-release-announcement/) / [Debezium SQL Server Connector](https://debezium.io/documentation/reference/3.7/connectors/sqlserver.html)

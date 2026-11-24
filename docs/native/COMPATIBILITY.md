# 旧Foundationとの互換性と移行

<!-- doc-purpose:start -->
> **役割:** Foundation Rと旧構成の使い分け、引き継ぐ契約と切替時の確認事項を調べます。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 配置前の確認と受入時。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [アーキテクチャと業務契約](<ARCHITECTURE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## どの構成を使うか

| 用途 | 起動する構成 | 注意点 |
| --- | --- | --- |
| 1台のVMでファイル取込、業務集計、AI読取、稼働監視 | Foundation Rの `compose.yaml` | DuckDBとPyIcebergを使う。新規のカタログ・オブジェクト保存と独立したlandingを用意 |
| Goldの閲覧、カタログ、Prometheusの運用画面 | Foundation Rに `compose.gui.yaml` を追加 | 認証ファイルと端末からの接続経路を用意 |
| 外部KafkaからSQL Server由来の変更を取得 | Foundation Rに `compose.cdc.yaml` を追加 | 外部にKafka互換brokerとDebeziumの発行環境が必要。consumer groupと状態を専用にする |
| 既存のSpark分散ジョブ、Trino、Superset、Airflow、MCP、Debeziumの登録、従来の全機能 | 既存のLakehouse Foundationを別checkoutで配置 | 既存の配置手順、profile、設定、データの保持手順に従う |

既存構成を使うときは [互換構成専用ガイド](../compatibility/README.md) を参照します。元の構成を固定版で再現する場合は、別の作業ディレクトリに [既存Foundationの固定版](https://github.com/hiromura16/lakehouse-foundation/tree/8adaef6d28a4fa01f2e409fd01a568cd6770e071) を取得します。同じホストに置く場合はCompose project名、ポート、landing、ボリューム、カタログ、Kafka consumer groupを分けます。

## 引き継ぐもの

受注・製造・出荷など固定6ソースの入力契約、業務SQL、独立期待値とCDCのRaw・状態契約を保持しています。コピー元のコミットとファイルのSHA-256は [契約マニフェスト](../../contracts/native/foundation-source.json) から確認できます。

Foundation Rは表のスナップショットを組にした公開世代を返し、設備JSONLから最新値と1分集計を提供します。単一VMの標準構成で扱うデータ量と書込元の制約を確認して使います。[構成と制約](ARCHITECTURE.md) · [実際の受入手順](ACCEPTANCE.md)

## 切替前に照合する

1. 元のプロジェクトの設定・データを保持し、Foundation Rを新しい試験用projectに起動します。
2. 自分の権限で利用できる同じ入力を両方に渡し、SilverとQuarantineの採否、Goldのキーと集計値を比較します。
3. ファイル数・行数・処理時間・停止後の再開を同じ条件で記録し、利用条件を超えた場合は分散構成を評価します。
4. アプリの読取先をRのGold APIに変更する前に、認証、公開世代ID、件数の上限、障害時に戻す接続先を確認します。

旧構成のIceberg warehouseやKafkaのoffsetをそのまま引き継ぐ移行は、設置先で別途検証します。処理エンジン間の性能優劣は、実際の負荷・同じ条件の測定で判断します。

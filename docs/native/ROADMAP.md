# 構想と実装の対応

<!-- doc-purpose:start -->
> **役割:** 設計上の目的と稼働する機能、評価候補を確認します。
>
> **対象読者:** 開発・設計担当 / 評価・報告担当
>
> **読むタイミング:** 技術選定や追加構成を検討するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [公式リファレンスと採用バージョン](<REFERENCES.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

実際に起動した機能と、今後比較する候補が分かるよう、構想の目的ごとに整理しています。

| 構想上の目的 | 現在使える機能 | 試験する条件 |
| --- | --- | --- |
| JVMを使わない小規模変換 | DuckDBのSQLとPyIcebergで1台のVMに配置。Apache DataFusionを使う製品としてInfluxDB 3 Coreを追加 | 単独のDataFusionやSailを比較する場合は、固定入力、同じ業務SQLの期待値、CPU・メモリ上限を揃える |
| Kafkaから現在値を構築 | PythonのCDCでRaw、更新順、DELETE、再開位置を確認 | RisingWaveのIceberg書込、取消、再開を同じイベントで比較する |
| 設備時系列の調査 | Icebergに業務向けの最新値・1分窓。InfluxDB 3 CoreとExplorerで測定点のSQL検索 | GreptimeDBやTelegrafの転送を比較する場合は、保持・遅延、再送、バックアップを測定する |
| 分散SQLと高度な互換 | 単一VMの処理上限を明示 | Ballistaは単一VMの制約に達した場合に評価。SailはSpark Connect互換、CometはSpark実行環境を前提に評価する |
| GUI監視と運用自動化 | Grafana、OpenObserve、n8n、運用ポータル | 接続先ごとのTLSと権限、アラート通知、復元を実環境で検証する |

データ量や起動時間を比較するときは、同一ホスト・同一入力・同じ制限で測ります。合成データの短時間テストと会社の実データ・負荷試験は、結果を別々に記録します。[受入手順](ACCEPTANCE.md) · [追加ツール](TOOLS.md)

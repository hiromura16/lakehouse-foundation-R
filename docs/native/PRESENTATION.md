# 説明スライド

<!-- doc-purpose:start -->
> **役割:** 概要、データの流れ、構成と配備を説明する資料です。
>
> **対象読者:** 評価・報告担当
>
> **読むタイミング:** チームへ導入概要を説明するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [アーキテクチャと業務契約](<ARCHITECTURE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

工場のシステム担当者、データ利用者、基盤運用者へ、導入時の分担と判断材料を説明します。導入編20枚に、試験条件・結果を示す技術付録10枚を加えています。PowerPointでは見出し、表、構成図のパーツを編集できます。全体構成図は16:9でそのままスライドに貼れます。

| 枚 | 主な内容 |
| --- | --- |
| 1〜5 | 対象業務、全体の構造、データの流れと契約 |
| 6〜10 | 取込の経路、設備テレメトリ、Iceberg公開、調査と閲覧、監視 |
| 11〜15 | 自動化とAIの境界、Dokploy、企業プロキシ、Cloudflare、障害復旧 |
| 16〜20 | 計算エンジンの比較、検証結果、初日からの運用、コンポーネントの対応 |
| 21〜26 | VMの事前診断、SQLの同一観測、設備の停止復帰、Icebergの計算・削除、復元、GoldとR2 |
| 27〜30 | 2 GiB原本の実測、現地シフトの照合、PostgreSQL更新、公開ポートの診断 |

- [編集用PowerPoint（30枚）](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25-v3/factory-lakehouse-briefing.pptx)
- [閲覧・配布用PDF（30ページ）](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25-v3/factory-lakehouse-briefing.pdf)
- [全体構造PNG（2400×1350）](images/foundation-r-structure.png)
- [オンプレと外部連携の配置PNG（2400×1350）](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25-v3/factory-lakehouse-topology.png)
- [全30枚のスクリーンショット（PNGのZIP）](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25-v3/factory-lakehouse-preview-images.zip) · [編集・再生成用のソース](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25-v3/factory-lakehouse-source.zip) · [ファイルのSHA-256](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25-v3/SHA256SUMS)
- [説明会向けHTML（30枚）](../presentation/index.html) · [技術付録の最初のページ](../presentation/index.html#vm-preflight) · [2 GiB原本・工場のシフト・DB更新・ポート診断](../presentation/index.html#two-gib-archives) · [配備と運用の補助ツール](TOOLS.md)

HTML版は運用ポータルの **説明スライド**、またはチェックアウト内の `docs/presentation/index.html` をブラウザーで開きます。目次と前後ボタンで移動できます。左右キーとPageUp・PageDownは前後、Home・Endは最初と最後への移動です。目次から選んだスライドのURLは共有できます。スマートフォンでは1列に並びます。

**印刷・PDF** では、表示中の位置にかかわらず全30枚を16:9で出力します。ブラウザーでPDFに保存する場合は、ページのヘッダーとフッターを無効にしてください。JavaScriptを無効にしていても、全スライドを読み、ブラウザーの印刷機能で出力できます。画像やスタイルはチェックアウトまたは運用ポータルから読み込みます。[PowerPoint 11枚の短縮版](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/presentation-2026-11-24/lakehouse-foundation-r.pptx)

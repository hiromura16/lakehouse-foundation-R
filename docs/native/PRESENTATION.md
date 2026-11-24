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

工場のシステム担当者、データ利用者、基盤運用者へ、導入時の分担と判断材料を説明する20枚の資料です。PowerPointでは見出し、表、構成図のパーツを編集できます。全体構成図は16:9でそのままスライドに貼れます。

| 枚 | 主な内容 |
| --- | --- |
| 1〜5 | 対象業務、全体の構造、データの流れと契約 |
| 6〜10 | 取込の経路、設備テレメトリ、Iceberg公開、調査と閲覧、監視 |
| 11〜15 | 自動化とAIの境界、Dokploy、企業プロキシ、Cloudflare、障害復旧 |
| 16〜20 | 計算エンジンの比較、検証結果、初日からの運用、コンポーネントの対応 |

- [編集用PowerPoint（20枚）](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25/factory-lakehouse-briefing.pptx)
- [閲覧・配布用PDF（20ページ）](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25/factory-lakehouse-briefing.pdf)
- [全体構造PNG（2400×1350）](images/foundation-r-structure.png)
- [オンプレと外部連携の配置PNG（2400×1350）](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/briefing-2026-11-25/factory-lakehouse-topology.png)
- [説明会向けHTML（10枚）](../presentation/index.html) · [配備と運用の補助ツール](TOOLS.md)

HTML版は前後ボタンまたは左右キーで進められます。任意の運用ポータルからも開けます。[PowerPoint 11枚の短縮版](https://github.com/hiromura16/lakehouse-foundation-R/releases/download/presentation-2026-11-24/lakehouse-foundation-r.pptx)

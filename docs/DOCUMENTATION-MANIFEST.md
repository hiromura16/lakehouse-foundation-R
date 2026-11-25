# 目的・読者から探す文書マニフェスト

<!-- doc-purpose:start -->
> **役割:** 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。
>
> **対象読者:** 初めて使う人 / 基盤構築担当 / データ利用担当 / 開発・設計担当 / 運用担当 / 評価・報告担当
>
> **読むタイミング:** 必要な文書を探すときや、文書の追加・改名時に登録を更新するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** 目的別一覧から必要な資料を選びます。
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

この一覧は公開Markdown全件の道案内です。「前提」は先に確認する文書、「次」は読み終えた後の候補です。
実際の操作はリンク先で成功条件まで確認します。templateは社内へコピーして記入する雛形、referenceは照合資料です。

## 目的から探す

### 01 入口・学習

| 文書 | 何のために読むか | 読むタイミング | 区分 |
| --- | --- | --- | --- |
| [Lakehouse Foundation R](<../README.md>) | JVMを使わない標準構成と会社VMでのDokploy配備の入口を確認します。 | 導入の方法を選ぶとき。 | active |
| [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>) | 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。 | 必要な文書を探すときや、文書の追加・改名時に登録を更新するとき。 | reference |
| [会社VMの受入手順と合格条件](<native/ACCEPTANCE.md>) | 導入後の検査を観測できる合格条件と照合します。 | 配置前の確認と受入時。 | active |
| [認証付きAPIとLangChain](<native/API.md>) | 固定API操作とスナップショットを根拠にしたLangChain利用を確認します。 | 分析やAIのツールを実装するとき。 | active |
| [アーキテクチャと業務契約](<native/ARCHITECTURE.md>) | コンポーネントの役割と採用理由、業務契約と公開世代を確認します。 | 実装を理解・変更するとき。 | active |
| [Dokployと企業ネットワーク](<native/COMPANY.md>) | Git・Docker・BuildKitの通信を準備し、Dokployで配備します。 | 会社環境の準備と配置時。 | active |
| [レイクハウスの設計思想](<native/DESIGN.md>) | データの保存、品質、所有、AI利用の考え方をFoundation Rの実装と照らし合わせます。 | 設計判断を理解するときや、用途に合わせて構成を変更するとき。 | active |
| [Dokploy配備モードの合成プラン](<native/DOKPLOY-MODES.md>) | 既存Composeを合成値だけで展開し、配備モードごとの差を確認する。 | Dokployの配備構成を選ぶ前、またはレシピ変更をレビューするとき。 | active |
| [データ閲覧と管理画面を追加する](<native/GUI.md>) | Gold・Lakekeeper・Prometheusの認証付き閲覧画面を用意します。 | ブラウザーで実データと運用状態を確認するとき。 | active |
| [Grafanaの稼働監視を設定ファイルで管理する](<native/MONITORING.md>) | PrometheusとGrafanaの監視対象・パネルを変更します。 | 起動確認や監視対象の変更時。 | active |
| [運用、バックアップ、受入試験](<native/OPERATIONS.md>) | 障害対応、状態の保全と合成データでの受入を行います。 | 導入先で検証・運用するとき。 | active |
| [説明スライド](<native/PRESENTATION.md>) | 概要、データの流れ、構成と配備を説明する資料です。 | チームへ導入概要を説明するとき。 | active |
| [公式リファレンスと採用バージョン](<native/REFERENCES.md>) | 固定した版と公式仕様への参照を調べます。 | 採用版の理由や更新差分を調べるとき。 | active |
| [ファイル・SQL・Kafka・設備の入力契約](<native/SOURCES.md>) | ファイル・SQLバッチ・Kafka・設備の入力契約を確認します。 | 外部ソースを接続するとき。 | active |
| [Foundation Rの初回導入と操作](<native/START.md>) | 会社VMにDokployで配備し、合成データから業務集計までを確認します。 | 初めて起動するとき。 | active |

### 02 設計・仕様

| 文書 | 何のために読むか | 読むタイミング | 区分 |
| --- | --- | --- | --- |
| [構想と実装の対応](<native/ROADMAP.md>) | 設計上の目的と稼働する機能、評価候補を確認します。 | 技術選定や追加構成を検討するとき。 | active |

### 03 データ・操作

| 文書 | 何のために読むか | 読むタイミング | 区分 |
| --- | --- | --- | --- |
| [工場のシフト暦で公開結果を照合する](<native/SHIFT-REPORT.md>) | 公開世代のSilverを工場の現地シフト暦と照合し、実績・停止報告の根拠を保存します。 | 夜勤・日勤の集計を、同じ公開世代のまま再照合するとき。 | active |
| [工場テレメトリの接続と受入](<native/TELEMETRY.md>) | 設備のMQTTを集め、Kafka経由の再送とGreptimeDBの時系列検索を検証します。 | 合成の設備テレメトリを隔離環境で検証するとき。 | active |
| [日次生産Goldをmarimoで再現する](<../examples/marimo/README.md>) | 承認済みのGoldを別環境で確認し、工場別の日次生産と歩留まりを再集計します。 | 入力のハッシュと公開世代を固定し、調査を他の担当者と再現するとき。 | active |
| [Rillで工場別の日次生産を探索する](<../examples/rill/README.md>) | 承認した日次生産Goldから、工場と期間を切り替えてKPIを確認します。 | 計画、実績、歩留まりを工場と日付で探索するとき。 | active |
| [合成入力を最初に作る](<../samples/README.md>) | 実データを使わずに最初の6契約素材を生成し、出力と後続処理の境界を確認する。 | 空の検証用landingに小さい固定条件の合成入力を初めて用意するとき。 | active |

### 04 配備・接続

| 文書 | 何のために読むか | 読むタイミング | 区分 |
| --- | --- | --- | --- |
| [オンプレとCloudflareをつなぐ](<native/CLOUDFLARE.md>) | Cloudflare TunnelとAccessで承認済みGoldを読み取る経路を配備します。 | オンプレの業務集計を拠点外の承認済みアプリへ提供するとき。 | active |

### 05 運用・検証

| 文書 | 何のために読むか | 読むタイミング | 区分 |
| --- | --- | --- | --- |
| [標準CDCのRawから隔離復元する](<native/CDC-SHADOW.md>) | 汎用CDCの確定済みRawを読み出し、元環境から切り離したSQLiteとIcebergへ再現する。 | 固定したRawスナップショットからの復元を、稼働中の保存先に影響を与えず検証するとき。 | active |
| [ネイティブ処理系の比較試験](<native/EVALUATION.md>) | Sail、RisingWave、GreptimeDB、DataFusion、Ballista、Cometを合成データで起動・検証します。 | ネイティブ製品を分けて実行し、結果を確認するとき。 | active |
| [運用ポータルと追加ツール](<native/TOOLS.md>) | n8n、OpenObserve、InfluxDBの設置と初回操作を確認します。 | 運用自動化、監視調査、設備時系列を追加するとき。 | active |

### 06 開発・レビュー

| 文書 | 何のために読むか | 読むタイミング | 区分 |
| --- | --- | --- | --- |
| [変更する人のための読みやすさと検証規約](<../CONTRIBUTING.md>) | 読みやすいコード・コメント・CLI説明と、専用uv環境での品質検査規約を揃える。 | コードや文書を変更する前と、差分をレビューに出す前。 | active |
| [アクセス行列: 通常利用・ローカル業務継続・緊急管理](<reviews/zero-trust-dokploy-access-matrix.md>) | アプリ別の認証、認可、ネットワーク、障害時の境界を定める。 | Foundation Rの配備とアクセス分離を設計・レビューするとき。 | active |
| [ADR: ローカル優先と配備・アクセス境界の分離](<reviews/zero-trust-dokploy-adr.md>) | ローカル優先の設計選択、代案、および取り消せる条件を記録する。 | Foundation Rの配備とアクセス分離を設計・レビューするとき。 | active |
| [Foundation Rのゼロトラスト・Dokploy・WAN断耐性の差分レビュー](<reviews/zero-trust-dokploy-gap-analysis.md>) | 追跡済み構成を棚卸しし、オンプレの自律運転を守る設計と段階的な受入条件を決める。 | Dokployの配備モードや人アクセス、WAN断耐性の変更を設計・レビューするとき。 | active |
| [宣言台帳: Compose資源・イメージ・供給元](<reviews/zero-trust-dokploy-inventory.md>) | 追跡済みComposeの資源、上限と外部供給の前提を確認する。 | Foundation Rの配備とアクセス分離を設計・レビューするとき。 | active |
| [脅威と受入試験の対応表](<reviews/zero-trust-dokploy-threat-test.md>) | 障害・認証・データ保全上の脅威から合格証拠までを追跡する。 | Foundation Rの配備とアクセス分離を設計・レビューするとき。 | active |
| [WAN断の継続・容量・再送設計](<reviews/zero-trust-dokploy-wan-recovery.md>) | WAN断中のローカル処理、容量の仮定、復旧時の再送と検証を設計する。 | Foundation Rの配備とアクセス分離を設計・レビューするとき。 | active |

## 読者から探す

### データ利用担当

| 文書 | 目的 |
| --- | --- |
| [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>) | 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。 |
| [工場のシフト暦で公開結果を照合する](<native/SHIFT-REPORT.md>) | 公開世代のSilverを工場の現地シフト暦と照合し、実績・停止報告の根拠を保存します。 |
| [日次生産Goldをmarimoで再現する](<../examples/marimo/README.md>) | 承認済みのGoldを別環境で確認し、工場別の日次生産と歩留まりを再集計します。 |
| [Rillで工場別の日次生産を探索する](<../examples/rill/README.md>) | 承認した日次生産Goldから、工場と期間を切り替えてKPIを確認します。 |
| [合成入力を最初に作る](<../samples/README.md>) | 実データを使わずに最初の6契約素材を生成し、出力と後続処理の境界を確認する。 |

### 初めて使う人

| 文書 | 目的 |
| --- | --- |
| [Lakehouse Foundation R](<../README.md>) | JVMを使わない標準構成と会社VMでのDokploy配備の入口を確認します。 |
| [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>) | 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。 |
| [会社VMの受入手順と合格条件](<native/ACCEPTANCE.md>) | 導入後の検査を観測できる合格条件と照合します。 |
| [認証付きAPIとLangChain](<native/API.md>) | 固定API操作とスナップショットを根拠にしたLangChain利用を確認します。 |
| [アーキテクチャと業務契約](<native/ARCHITECTURE.md>) | コンポーネントの役割と採用理由、業務契約と公開世代を確認します。 |
| [Dokployと企業ネットワーク](<native/COMPANY.md>) | Git・Docker・BuildKitの通信を準備し、Dokployで配備します。 |
| [レイクハウスの設計思想](<native/DESIGN.md>) | データの保存、品質、所有、AI利用の考え方をFoundation Rの実装と照らし合わせます。 |
| [Dokploy配備モードの合成プラン](<native/DOKPLOY-MODES.md>) | 既存Composeを合成値だけで展開し、配備モードごとの差を確認する。 |
| [データ閲覧と管理画面を追加する](<native/GUI.md>) | Gold・Lakekeeper・Prometheusの認証付き閲覧画面を用意します。 |
| [Grafanaの稼働監視を設定ファイルで管理する](<native/MONITORING.md>) | PrometheusとGrafanaの監視対象・パネルを変更します。 |
| [運用、バックアップ、受入試験](<native/OPERATIONS.md>) | 障害対応、状態の保全と合成データでの受入を行います。 |
| [公式リファレンスと採用バージョン](<native/REFERENCES.md>) | 固定した版と公式仕様への参照を調べます。 |
| [ファイル・SQL・Kafka・設備の入力契約](<native/SOURCES.md>) | ファイル・SQLバッチ・Kafka・設備の入力契約を確認します。 |
| [Foundation Rの初回導入と操作](<native/START.md>) | 会社VMにDokployで配備し、合成データから業務集計までを確認します。 |
| [合成入力を最初に作る](<../samples/README.md>) | 実データを使わずに最初の6契約素材を生成し、出力と後続処理の境界を確認する。 |

### 基盤構築担当

| 文書 | 目的 |
| --- | --- |
| [Lakehouse Foundation R](<../README.md>) | JVMを使わない標準構成と会社VMでのDokploy配備の入口を確認します。 |
| [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>) | 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。 |
| [会社VMの受入手順と合格条件](<native/ACCEPTANCE.md>) | 導入後の検査を観測できる合格条件と照合します。 |
| [認証付きAPIとLangChain](<native/API.md>) | 固定API操作とスナップショットを根拠にしたLangChain利用を確認します。 |
| [アーキテクチャと業務契約](<native/ARCHITECTURE.md>) | コンポーネントの役割と採用理由、業務契約と公開世代を確認します。 |
| [標準CDCのRawから隔離復元する](<native/CDC-SHADOW.md>) | 汎用CDCの確定済みRawを読み出し、元環境から切り離したSQLiteとIcebergへ再現する。 |
| [オンプレとCloudflareをつなぐ](<native/CLOUDFLARE.md>) | Cloudflare TunnelとAccessで承認済みGoldを読み取る経路を配備します。 |
| [Dokployと企業ネットワーク](<native/COMPANY.md>) | Git・Docker・BuildKitの通信を準備し、Dokployで配備します。 |
| [レイクハウスの設計思想](<native/DESIGN.md>) | データの保存、品質、所有、AI利用の考え方をFoundation Rの実装と照らし合わせます。 |
| [Dokploy配備モードの合成プラン](<native/DOKPLOY-MODES.md>) | 既存Composeを合成値だけで展開し、配備モードごとの差を確認する。 |
| [データ閲覧と管理画面を追加する](<native/GUI.md>) | Gold・Lakekeeper・Prometheusの認証付き閲覧画面を用意します。 |
| [Grafanaの稼働監視を設定ファイルで管理する](<native/MONITORING.md>) | PrometheusとGrafanaの監視対象・パネルを変更します。 |
| [運用、バックアップ、受入試験](<native/OPERATIONS.md>) | 障害対応、状態の保全と合成データでの受入を行います。 |
| [公式リファレンスと採用バージョン](<native/REFERENCES.md>) | 固定した版と公式仕様への参照を調べます。 |
| [ファイル・SQL・Kafka・設備の入力契約](<native/SOURCES.md>) | ファイル・SQLバッチ・Kafka・設備の入力契約を確認します。 |
| [Foundation Rの初回導入と操作](<native/START.md>) | 会社VMにDokployで配備し、合成データから業務集計までを確認します。 |
| [工場テレメトリの接続と受入](<native/TELEMETRY.md>) | 設備のMQTTを集め、Kafka経由の再送とGreptimeDBの時系列検索を検証します。 |
| [運用ポータルと追加ツール](<native/TOOLS.md>) | n8n、OpenObserve、InfluxDBの設置と初回操作を確認します。 |
| [アクセス行列: 通常利用・ローカル業務継続・緊急管理](<reviews/zero-trust-dokploy-access-matrix.md>) | アプリ別の認証、認可、ネットワーク、障害時の境界を定める。 |
| [ADR: ローカル優先と配備・アクセス境界の分離](<reviews/zero-trust-dokploy-adr.md>) | ローカル優先の設計選択、代案、および取り消せる条件を記録する。 |
| [Foundation Rのゼロトラスト・Dokploy・WAN断耐性の差分レビュー](<reviews/zero-trust-dokploy-gap-analysis.md>) | 追跡済み構成を棚卸しし、オンプレの自律運転を守る設計と段階的な受入条件を決める。 |
| [宣言台帳: Compose資源・イメージ・供給元](<reviews/zero-trust-dokploy-inventory.md>) | 追跡済みComposeの資源、上限と外部供給の前提を確認する。 |
| [脅威と受入試験の対応表](<reviews/zero-trust-dokploy-threat-test.md>) | 障害・認証・データ保全上の脅威から合格証拠までを追跡する。 |
| [WAN断の継続・容量・再送設計](<reviews/zero-trust-dokploy-wan-recovery.md>) | WAN断中のローカル処理、容量の仮定、復旧時の再送と検証を設計する。 |
| [日次生産Goldをmarimoで再現する](<../examples/marimo/README.md>) | 承認済みのGoldを別環境で確認し、工場別の日次生産と歩留まりを再集計します。 |
| [Rillで工場別の日次生産を探索する](<../examples/rill/README.md>) | 承認した日次生産Goldから、工場と期間を切り替えてKPIを確認します。 |

### 評価・報告担当

| 文書 | 目的 |
| --- | --- |
| [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>) | 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。 |
| [ネイティブ処理系の比較試験](<native/EVALUATION.md>) | Sail、RisingWave、GreptimeDB、DataFusion、Ballista、Cometを合成データで起動・検証します。 |
| [説明スライド](<native/PRESENTATION.md>) | 概要、データの流れ、構成と配備を説明する資料です。 |
| [構想と実装の対応](<native/ROADMAP.md>) | 設計上の目的と稼働する機能、評価候補を確認します。 |

### 運用担当

| 文書 | 目的 |
| --- | --- |
| [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>) | 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。 |
| [標準CDCのRawから隔離復元する](<native/CDC-SHADOW.md>) | 汎用CDCの確定済みRawを読み出し、元環境から切り離したSQLiteとIcebergへ再現する。 |
| [オンプレとCloudflareをつなぐ](<native/CLOUDFLARE.md>) | Cloudflare TunnelとAccessで承認済みGoldを読み取る経路を配備します。 |
| [工場のシフト暦で公開結果を照合する](<native/SHIFT-REPORT.md>) | 公開世代のSilverを工場の現地シフト暦と照合し、実績・停止報告の根拠を保存します。 |
| [工場テレメトリの接続と受入](<native/TELEMETRY.md>) | 設備のMQTTを集め、Kafka経由の再送とGreptimeDBの時系列検索を検証します。 |
| [運用ポータルと追加ツール](<native/TOOLS.md>) | n8n、OpenObserve、InfluxDBの設置と初回操作を確認します。 |
| [アクセス行列: 通常利用・ローカル業務継続・緊急管理](<reviews/zero-trust-dokploy-access-matrix.md>) | アプリ別の認証、認可、ネットワーク、障害時の境界を定める。 |
| [ADR: ローカル優先と配備・アクセス境界の分離](<reviews/zero-trust-dokploy-adr.md>) | ローカル優先の設計選択、代案、および取り消せる条件を記録する。 |
| [Foundation Rのゼロトラスト・Dokploy・WAN断耐性の差分レビュー](<reviews/zero-trust-dokploy-gap-analysis.md>) | 追跡済み構成を棚卸しし、オンプレの自律運転を守る設計と段階的な受入条件を決める。 |
| [宣言台帳: Compose資源・イメージ・供給元](<reviews/zero-trust-dokploy-inventory.md>) | 追跡済みComposeの資源、上限と外部供給の前提を確認する。 |
| [脅威と受入試験の対応表](<reviews/zero-trust-dokploy-threat-test.md>) | 障害・認証・データ保全上の脅威から合格証拠までを追跡する。 |
| [WAN断の継続・容量・再送設計](<reviews/zero-trust-dokploy-wan-recovery.md>) | WAN断中のローカル処理、容量の仮定、復旧時の再送と検証を設計する。 |

### 開発・設計担当

| 文書 | 目的 |
| --- | --- |
| [変更する人のための読みやすさと検証規約](<../CONTRIBUTING.md>) | 読みやすいコード・コメント・CLI説明と、専用uv環境での品質検査規約を揃える。 |
| [目的・読者から探す文書マニフェスト](<DOCUMENTATION-MANIFEST.md>) | 公開Markdown全件の固有の目的・読者・読む時点を、目的別と読者別に引く。 |
| [ネイティブ処理系の比較試験](<native/EVALUATION.md>) | Sail、RisingWave、GreptimeDB、DataFusion、Ballista、Cometを合成データで起動・検証します。 |
| [構想と実装の対応](<native/ROADMAP.md>) | 設計上の目的と稼働する機能、評価候補を確認します。 |
| [アクセス行列: 通常利用・ローカル業務継続・緊急管理](<reviews/zero-trust-dokploy-access-matrix.md>) | アプリ別の認証、認可、ネットワーク、障害時の境界を定める。 |
| [ADR: ローカル優先と配備・アクセス境界の分離](<reviews/zero-trust-dokploy-adr.md>) | ローカル優先の設計選択、代案、および取り消せる条件を記録する。 |
| [Foundation Rのゼロトラスト・Dokploy・WAN断耐性の差分レビュー](<reviews/zero-trust-dokploy-gap-analysis.md>) | 追跡済み構成を棚卸しし、オンプレの自律運転を守る設計と段階的な受入条件を決める。 |
| [宣言台帳: Compose資源・イメージ・供給元](<reviews/zero-trust-dokploy-inventory.md>) | 追跡済みComposeの資源、上限と外部供給の前提を確認する。 |
| [脅威と受入試験の対応表](<reviews/zero-trust-dokploy-threat-test.md>) | 障害・認証・データ保全上の脅威から合格証拠までを追跡する。 |
| [WAN断の継続・容量・再送設計](<reviews/zero-trust-dokploy-wan-recovery.md>) | WAN断中のローカル処理、容量の仮定、復旧時の再送と検証を設計する。 |

## 文書を追加・変更するとき

1. 本文の初H1と役割を決め、[JSON正本](documentation-manifest.json)に1項目を追加・更新します。
2. purpose・audience・when_to_useはその文書固有の説明にします。prerequisitesとnextには登録済みMarkdownの相対pathだけを指定します。
3. リポジトリの端末で `python3 scripts/documentation.py` を実行し、全目的欄と本一覧を生成します。マーカーの外側の本文は保持されます。
4. `python3 scripts/documentation.py --check` で登録・生成差分・ローカルリンク・見出しを検査します。外部URLの到達性は別確認です。
5. 図の生成キャプションは共通のpurpose_block APIを使います。本文生成器の出力とこの検査を両方確認します。

削除・改名時は相互参照と本文リンクも更新します。実環境の設定・証跡・記入済み報告書は登録せずGit外に保管します。
Gitのignore対象は列挙されません。既に追跡済みのファイルはignore設定だけでは対象外になりません。
削除・改名は `git rm` / `git mv`、または対象を確認した `git add -u` でindexにも反映してから生成します。追跡済みの未stage削除は欠落として検出されます。

## リンク検査の範囲と限界

inline/reference形式のMarkdownリンクと画像、HTMLのhref/src、見出しIDと明示id/nameを検査します。コードフェンス内の例は対象外です。参照先の存在、重複見出しの連番、repository外への逸脱を確認します。
外部URLへ通信しないため到達性は保証しません。独立した軽量検査であり、全CommonMark/GFM構文やGitHubの表示処理への完全適合を保証するものではありません。複雑な記法を追加した場合はGitHub上の表示とリンクも確認します。

記法の一次資料: [GFMのコードフェンス](https://github.github.com/gfm/#fenced-code-blocks)、[GitHubの見出しリンク](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax#section-links)、[GitHubの相対リンク](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax#relative-links)。

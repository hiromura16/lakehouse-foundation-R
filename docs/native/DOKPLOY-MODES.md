# Dokploy配備モードの合成プラン

<!-- doc-purpose:start -->
> **役割:** 既存Composeを合成値だけで展開し、配備モードごとの差を確認する。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** Dokployの配備構成を選ぶ前、またはレシピ変更をレビューするとき。
>
> **前提となる資料:** [Dokployと企業ネットワーク](<COMPANY.md>)
>
> **次に読む資料:** [会社VMの受入手順と合格条件](<ACCEPTANCE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

このプランは、既存Composeの合成順・有効サービス・論理リソースを**合成入力だけ**で比較するための確認手順です。Dokployの代替配備手順ではありません。既存の `compose.yaml`、Environment、`scripts/preflight.py` に変更はなく、実際の導入は[初回導入](START.md)、ホスト確認は[企業ネットワークと配備](COMPANY.md)を参照します。

## 対応範囲

| コマンドのモード | 展開する構成 | この段階で確認しないこと |
| --- | --- | --- |
| `core-local` | 現行のベースのみ。必要に応じて既存の `--cdc`／`--alerting` を追加。 | 外部人入口・Gold搬出なし。既存のループバック公開は維持。実際のWAN断・ローカル設備時系列は未検証。 |
| `local-operations` | ベース＋既存GUI。 `--cdc`／`--alerting` の明示選択も可。 | 既存認証ファイル・ローカル運用経路を展開する形だけ。承認済みネットワーク・認証の確立は別段階。 |
| `standard` | ベース＋既存GUI・Toolsセット。必要に応じ `--tools-metrics`、`--cdc`／`--alerting`。 | Toolsの各アプリを個別選択する仕組みは未追加。通常人経路・Gold搬出を自動追加しない。 |
| `legacy-gold-read` | ベース＋既存Cloudflare Gold読取用overlay。 | 人向けAccessの設置や、永続キューによる新規外部搬出を実装したものではない。 |
| `evaluation` | ベース＋評価overlay、`--profile` で1候補のみ。 | 比較候補の同時起動・予算実測・物理障害試験はしない。 |
| `catalog` | Dockerを使わず、実装したモード・未実装モード・評価プロファイルを列挙。 | `all-lab` は全候補の個別比較として扱い、一括合成しない。 |

`zero-trust-access`、`data-egress` は設計上の名前で、実装がないため展開コマンドは失敗します。ローカル設備時系列は[本案件のWAN受入](../reviews/zero-trust-dokploy-wan-recovery.md)で必須です。既存の汎用 `core-local` はInfluxDBを起動せず、`standard` にそのサービスがあることだけでもWAN断中の書込み・クエリは証明しません。後続のローカル選択と隔離試験を要します。

## 合成値だけで確認

管理者が信頼するこのリポジトリのチェックアウト、Python 3.12、標準ディレクトリにインストールしたDocker CLIと、`config --no-env-resolution --format json` に対応するComposeを使います。未レビューで変更したComposeを実行するための隔離機構ではありません。この手順が明示するComposeファイル以外の入力は受け取りません。

**Bash / Zsh共通**

```bash
python3 scripts/dokploy_plan.py catalog
python3 scripts/dokploy_plan.py core-local
python3 scripts/dokploy_plan.py local-operations --cdc --alerting
python3 scripts/dokploy_plan.py standard --tools-metrics
python3 scripts/dokploy_plan.py evaluation --profile stream
```

既存の環境変数・ローカル `.env`、ユーザーのDocker設定・context・daemonは使いません。作業用の一時ディレクトリに空の環境ファイル、専用HOME/設定先、実在しないUnixソケットを割り当てます。現在の追跡済みoverlayには外部Composeを取り込む記述はありません。他のComposeコマンドやpull、build、サービス起動、ネットワーク設定変更は実行しません。

JSONの理論値は、順序付き `compose_files` と `profiles`、有効 `services`、論理 `networks`・`volumes`・`configs`・`secrets`、合成時の `ports`（プロトコル、公開・コンテナ側番号、bindの分類）です。IP文字列、実環境の値・パス・生Compose出力・エラー原文は出しません。未対応の組合せや構造は固定理由で失敗します。Docker/Dokployで実際に見えるネットワークやポート、ボリューム所有権との照合には実機ゲートが別途必要です。

## 本番Environmentの記入項目（値の例ではない）

次は追加構成を選ぶ際に見直す**キーの目録**です。文中の行を貼り付けて配備しないでください。未指定の既定値とプロキシ、ポート、TLS、実際のパス・必要権限は[配備手順](COMPANY.md)と[宣言台帳](../reviews/zero-trust-dokploy-inventory.md)で照合します。

| 構成 | 確認する既存の主なキー |
| --- | --- |
| ベース | `LANDING_DIR`・`LANDING_UID`・`LANDING_GID`、`CATALOG_DB_PASSWORD`・`CATALOG_ENCRYPTION_KEY`、`S3_ACCESS_KEY`・`S3_SECRET_KEY`、`WORKER_TOKEN`・`READER_TOKEN`、`GRAFANA_ADMIN_PASSWORD` |
| GUI | `LAKEHOUSE_OPS_HTPASSWD` |
| CDC | `CDC_STATE_DIR`・`CDC_SETTINGS_FILE`・`CDC_TABLE_MAP_FILE`。サイトのKafka、既存のoffset/connectorと分離された新規試験契約を確認。 |
| Tools | `TOOLS_DB_PASSWORD`・`TOOLS_N8N_KEY`・`TOOLS_OBSERVE_USER`・`TOOLS_OBSERVE_PASSWORD`・`TOOLS_EXPLORER_KEY`・`TOOLS_HTPASSWD`・`TOOLS_INFLUX_TOKEN_FILE`・`TOOLS_OBSERVE_PASSCODE_FILE` |
| 既存Gold読取 | `CF_TUNNEL_TOKEN_FILE`・`CF_EDGE_POLICY_FILE`・`CF_EDGE_READER_FILE`。人の認証とは別の用途。 |

合成成功後も、対象バージョンのDokploy **Preview** で、追加されたrouter／network・実効Environment・mount・永続資源を照合し、既存preflightを配備先で実行します。切り戻しは従来の配備指定を維持し、既存環境のデータ・offset・volumeは変更しません。

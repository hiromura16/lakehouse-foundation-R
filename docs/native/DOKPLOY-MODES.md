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

## サービス別の棚卸しと暫定容量

既存の `dokploy_plan.py` は変更しません。追加の `dokploy_capacity.py` は、[開発用ロック](../../requirements-dev.lock)にあるPyYAMLを利用して追跡済みのCompose宣言を読み、同じ合成用Composeで有効になったサービスの上限だけを集計します。GitやVMから実際のEnvironment／実機容量を取りません。

**Bash / Zsh共通**

```bash
python3 scripts/dokploy_capacity.py catalog
python3 scripts/dokploy_capacity.py all-lab
python3 scripts/dokploy_capacity.py standard
python3 scripts/dokploy_capacity.py evaluation --profile stream
```

- `catalog` は設計上の7モードを表示します。 `zero-trust-access` と `data-egress` は計画のみ。互換用 `legacy-gold-read` は別扱いです。 `all-lab` は全機能群の比較目録であり、同時に起動する定義ではありません。既存レシピで単独比較できるものと、別プロジェクト・プロファイルの依存・排他が残る候補を区別します。Nisshiと既定broker、RustFSとSeaweedFSを重複計上せず、Rill/marimoは個別プロジェクトとして扱います。 `distributed-check` の静的展開は動作に必要なdistributed側の前提を満たしません。
- サービスには上書き順の論理Composeファイル、プロファイル、名前だけのEnvironment参照、CPU/memoryを表示します。参照名は宣言・使用しているconfig/secretを含む**上限集合**で、上書きで消えた参照やinactiveな定義を別に含み得ます。 `required_nonempty`・`required_present` は参照の構文で、条件分岐や使用先によって実際に必須になるかは変わります。 `has_default` でも既定値そのものは表示しません。外部サイト設定やSDKの暗黙の環境変数までは揃ったと判定しません。
- 上限は正規化した稼働サービスを重複加算しません。単発サービスも除外しません。 `unbounded` は宣言がなく、`unknown` は相違する上限、未対応の配備modeや単一でないレプリカ、異常な数値などから解釈できないものです。CPUはミリCPU、memoryはMiBに切り上げ、`known_total` は既知上限のみの合計です。たとえば現行coreの2,000ミリCPU／5,088 MiBには、未設定9 CPU・1 memoryが含まれず、VMをそのサイズで足りると判定できません。

容量の数値は**試算用の入力**としてのみ渡します。例は架空の16 vCPU／32 GiBであり、現行VM、採用値、推奨構成の表明ではありません。OS・Docker・Dokploy・Traefik・既存PoCと運用余裕を資源ごとに分けます。未記入は未知で、0と推定しません。明示の0は「別帳票で計上済み」または「対象外」を確認した場合に限ります。

**Bash / Zsh共通**

```bash
python3 scripts/dokploy_capacity.py standard --synthetic-capacity 16000:32768 \
  --reserve-os 1000:2048 --reserve-docker 500:1024 \
  --reserve-dokploy 1000:2048 --reserve-traefik 500:512 \
  --reserve-existing-poc 2000:4096 --reserve-headroom 2000:4096
```

予算指定時、既知分だけでも枠を越えると `deny`（終了3）で、CPUとmemoryの不足を別々に表示します。不足が見つからなくても、上限未設定・未検証・予約未記入、明示された外部前提が残れば `indeterminate`（終了4）です。全て既知で枠内のときだけ `fits_static_envelope`（終了0）を返します。比較候補は未評価で、機能同等や適合を示しません。不足に応じ、候補モードを別見積にするか、既知最小容量から拡張を検討し、未知があれば計測または上限設定を確認します。

これは配備許可ではありません。実機で永続ディスク、既存利用量、並列・CPU/I/O余裕、Dokploy Preview、対象型を確認し、別途承認してから使用します。

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

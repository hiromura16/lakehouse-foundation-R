# 運用ポータルと追加ツール

<!-- doc-purpose:start -->
> **役割:** n8n、OpenObserve、InfluxDBの設置と初回操作を確認します。
>
> **対象読者:** 基盤構築担当 / 運用担当
>
> **読むタイミング:** 運用自動化、監視調査、設備時系列を追加するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [運用、バックアップ、受入試験](<OPERATIONS.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

[標準構成](START.md) の運用を始めた後、同じComposeプロジェクトに追加できます。管理画面は運用者向けです。会社VMではTLS終端またはSSH転送を用意し、Composeの公開ポートはループバックへ限定します。

| アプリ | 何に使うか | 初期ポート |
| --- | --- | --- |
| 運用ポータル | 疎通状況、作業別の入口、説明スライドを開く | 18506 |
| n8n | 手動実行やイベントから定型手順をつなぐ。ワークフローをJSONで管理 | 18507 |
| OpenObserve | Rust製の観測基盤。取り込んだPrometheusの監視履歴をPromQLで調査 | 18508 |
| InfluxDB 3 Explorer | 保存した測定点をSQLで検索し、表やチャートで確認 | 18509 |
| InfluxDB 3 Core | Rust製の時系列DB。Line Protocolで受信し、DataFusionによるSQLを提供 | 18510 |

n8nの永続状態は専用PostgreSQLに分離します。InfluxDBの測定点は時系列の調査用データです。業務集計の公開範囲と版はIcebergのGoldで管理します。n8nの配布条件は商用再提供の扱いが異なるため、導入形態に応じて公式ライセンスを確認します。[n8n Sustainable Use License](https://docs.n8n.io/sustainable-use-license/) · [InfluxDB 3 Core](https://docs.influxdata.com/influxdb3/core/)

![9サービスの応答と作業別ナビゲーションを示す運用ポータルの実画面](images/operations-portal.png)

## 起動する

DockerとOpenSSLを使います。標準構成で使った `site.env` を指定し、追加ツールの秘密ファイルは初回だけ生成します。閲覧UIなしでも起動できます。閲覧UIを併用する場合は [GUIの認証を準備](GUI.md) した上で `-f compose.gui.yaml` を `-f compose.tools.yaml` の手前に加えます。以降の起動・停止でも同じComposeファイルを指定します。

**Bash / Zsh共通**

```bash
site="$HOME/.config/lakehouse-foundation-r/site.env"
tools="$HOME/.config/lakehouse-foundation-r/tools"
python3 native/tools/setup.py --directory "$tools"
chmod a+r native/tools/targets.yml native/tools/health-workflow.json
docker compose --env-file "$site" --env-file "$tools/tools.env" \
  -f compose.yaml -f compose.tools.yaml config --quiet
docker compose --env-file "$site" --env-file "$tools/tools.env" \
  -f compose.yaml -f compose.tools.yaml up -d --build --wait --wait-timeout 240
```

ポータルは `http://localhost:18506/` です。`operator.json` に記録された専用アカウントを使います。n8nとExplorerの入口でも同じ認証が必要です。n8nの初回画面で管理者を登録します。OpenObserveは `tools.env` の `TOOLS_OBSERVE_USER` と `TOOLS_OBSERVE_PASSWORD` を使います。

ホストのポートは `TOOLS_PORT`、`TOOLS_N8N_PORT`、`TOOLS_OBSERVE_PORT`、`TOOLS_EXPLORER_PORT`、`TOOLS_INFLUX_PORT` で変更できます。ポータルの接続先URLは、開いているホスト名と構成で指定したポートを使います。n8nをHTTPSで使う場合は `TOOLS_N8N_URL` を実際のHTTPS URLへ設定し、`TOOLS_SECURE_COOKIES=true` にします。リバースプロキシの段数は `TOOLS_N8N_PROXY_HOPS` で指定します。[Dokployの配置手順](COMPANY.md) · [n8nのリバースプロキシ](https://docs.n8n.io/hosting/configuration/configuration-examples/webhook-url/)

## ワークフローを手動実行する

1. n8nの画面でワークフローを作り、メニューの **Import from File** から [疎通確認のJSON](../../native/tools/health-workflow.json) を選択します。
2. **Execute workflow** を押し、最後のステップで `ready: true` と `blocked: false` を確認します。フローには内部APIのヘルス確認だけが含まれます。
3. 日程実行や書き込みを追加する場合は、用途別の認証と権限を決め、試験環境で同じ処理が再実行される場合を確認します。

## OpenObserveに監視メトリクスを送る

初回のprovisionは転送用パスコードを `observe-passcode` に0600で保存し、[4パネルのダッシュボード](../../native/tools/openobserve-dashboard.json) を登録します。次に転送サービスを起動します。既存の同名ボードは維持します。変更する場合は画面から編集するか、設定JSONの内容を **Dashboards → Import** からインポートします。

**Bash / Zsh共通**

```bash
python3 -m native.tools.observe provision --directory "$tools"
docker compose --env-file "$site" --env-file "$tools/tools.env" \
  -f compose.yaml -f compose.tools.yaml --profile tools-metrics up -d --wait
```

1分ほど経ってから確認します。ローカルのPrometheusが疎通とジョブ数値を取得し、転送サービスが監視系列を選んでOpenObserveへ送ります。ダッシュボードの「サービスの応答」は `probe_success` です。1は応答あり、0は取得失敗を示し、系列がなければ対象登録と転送を確認します。

**Bash / Zsh共通**

```bash
python3 -m native.tools.observe verify --directory "$tools"
```

採取対象は [targets.yml](../../native/tools/targets.yml) を編集します。Grafana側の編集は [監視設定](MONITORING.md) を参照してください。[Prometheus remote write](https://openobserve.ai/docs/ingestion/metrics/prometheus/) · [OpenObserveダッシュボード](https://openobserve.ai/docs/user-guide/dashboards/)

## 設備の合成測定点を確認する

**Bash / Zsh共通**

```bash
python3 -m native.tools.demo --token-file "$tools/influx-admin.json"
```

結果に `foundation_demo` と `points: 3` が表示されます。Explorerは運用者がポート18509を開き、**Add Server** でDocker内部URL `http://influxdb3:8181` と `influx-admin.json` の `token` を登録します。データベース `foundation_demo` を選び、次のSQLで値を確認します。

```sql
SELECT time, device, celsius
FROM temperature
ORDER BY time DESC
LIMIT 3;
```

Explorerはクエリモードで動かします。使用するInfluxDB Coreのトークンには管理権限があります。接続登録と閲覧を許可された運用者の作業環境として運用してください。会社の設備データを送る前に、保持期間・バックアップとリストア・API認証を受け入れます。[InfluxDB Explorerの接続手順](https://docs.influxdata.com/influxdb3/explorer/connect-to-server/) · [管理トークン](https://docs.influxdata.com/influxdb3/core/admin/tokens/)

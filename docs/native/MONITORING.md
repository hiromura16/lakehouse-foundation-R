# Grafanaの稼働監視を設定ファイルで管理する

<!-- doc-purpose:start -->
> **役割:** PrometheusとGrafanaの監視対象・パネルを変更します。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 起動確認や監視対象の変更時。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [運用、バックアップ、受入試験](<OPERATIONS.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

標準起動で `Foundation R サービス監視` がGrafanaに自動作成されます。サーバー自身から `http://localhost:18501/d/lakehouse-r-runtime` を開き、ユーザー名 `admin` とprivate `site.env` の `GRAFANA_ADMIN_PASSWORD` で入ります。初回パスワードはGrafanaデータvolumeが空のときに適用されます。

会社がSSHポート転送を許可している場合は、手元の端末で次を実行します。`operator@vm.example.invalid` は、会社から案内されたSSHユーザーと配備先VMに置き換えます。SSH接続を開いたまま、手元のブラウザーで同じGrafana URLを開きます。別の画面は [閲覧URL](GUI.md) と [運用ツールの入口](TOOLS.md) から必要なポートを選びます。

**Bash / Zsh共通**

```bash
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:18501:127.0.0.1:18501 operator@vm.example.invalid
```

[OpenSSHのローカル転送](https://man.openbsd.org/ssh#L)

![合成環境で各サービスの稼働状態と履歴を表示したGrafana](images/runtime-demo.png)

画像は [閲覧画面を追加](GUI.md) した構成です。標準構成は7対象、閲覧画面付きは9対象、運用ツール付きは12対象、両方追加した構成は14対象です。標準の常駐コンテナは、監視対象の7個に内部の `read-api` と `health-relay` を加えた9個です。内部の2個はDockerのヘルスチェックで確認します。「サービスの状態」は緑の✓が正常、赤の×が異常です。履歴は `app` ごとに1行です。対象が応答できない点は赤になり、Prometheusが停止して取得できなかった区間は空白になります。履歴のバーにカーソルを合わせるとその区間の判定が表示されます。「取込・変換ジョブ」は成功と失敗を別の系列で表示します。「最終ジョブ成功からの経過時間」はワーカーの起動後に観測した成功を表示します。成功前は「この起動では未実行」と表示し、メトリクスが取得できない状態と区別します。会社の実行間隔に応じたアラートは別途設定します。

「Goldの同時参照件数」は受理済みのクエリと上限を表示します。
「Goldの混雑による応答」は上限に達してHTTP 503と再試行時間を返した累計です。
ワーカーの再起動時に累計は0へ戻ります。既定は同時2件です。
メモリとCPUを増やした配備で負荷を確認したうえで、調整する場合は
`LAKEHOUSE_GOLD_MAX_CONCURRENT` を1〜8の範囲で指定します。

| ファイル | 変更する内容 |
| --- | --- |
| [core.yml](../../native/monitoring/targets/core.yml) | PostgreSQL、SeaweedFS、Lakekeeper、Lakehouse、Prometheus、Grafana、Blackboxの監視先 |
| [gui.yml](../../native/monitoring/gui-targets/gui.yml) | 閲覧構成を追加した場合のStreamlitと閲覧ゲートウェイ |
| [blackbox.yml](../../native/monitoring/blackbox.yml) | HTTPステータスまたはTCP疎通の判定、タイムアウト |
| [health-relay.conf](../../native/network/health-relay.conf) | Grafanaと任意アプリの固定ヘルス経路 |
| [prometheus.yml](../../native/monitoring/prometheus.yml) | scrape間隔、直接メトリクス、監視対象ファイルの場所 |
| [alerts.yml](../../native/monitoring/alerts.yml) | ダウンとジョブ失敗のPrometheusルール |
| [runtime.json](../../native/monitoring/dashboards/runtime.json) | パネル、配置、しきい値、PromQL |
| [lakehouse.yml](../../native/monitoring/provisioning/dashboards/lakehouse.yml) | Grafanaのダッシュボードの読み込み方 |
| [prometheus.yml](../../native/monitoring/provisioning/datasources/prometheus.yml) | Grafanaが読むPrometheusの接続先とUID |

## Dockerネットワークの容量

起動するComposeプロジェクトごとに、標準構成は6個、閲覧付きは7個、運用ツール付きは9個、両方付きは10個のDockerネットワークを使います。CDCは1個、Cloudflareは2個を追加するため、同じプロジェクトでこれらを有効にすると13個です。通知用のAlertmanagerを加えるとさらに1個を使います。隔離したRustFS受入環境を同じホストに追加すると、さらに6個を使います。

Dockerが `all predefined address pools have been fully subnetted` を返す場合は、ホスト上の各Composeプロジェクトと停止済みの検証環境が使うネットワークを管理者と確認します。会社やVPNの経路と重複しないアドレス範囲を確保したうえで、ホスト側のDocker自動割当プールを設計します。停止だけではComposeネットワークが残るため、不要になった検証環境は、その環境のプロジェクト名とComposeファイルを指定して `docker compose down` で片付けます。[Dockerのサブネット割当](https://docs.docker.com/engine/network/#subnet-allocation)

## 監視先を追加する

Blackbox Exporterは書込側の疎通と内部の監視専用ネットワークに置きます。任意アプリのヘルスは固定経路のリレーを経由します。リレーは受信した認証情報やクエリ文字列を上流へ渡さず、設定したHTTP GETだけを実行します。

任意アプリの確認を増やす場合は、リレーに固定のヘルスURLを加えてから、`targets` にそのリレー経路、`app` に重複しない表示名、`module` に `http_ready` を指定します。TCPだけの監視では、専用ネットワーク内の `hostname:port` と `tcp_connect` を使います。別VMや別プロジェクトの監視は、その範囲だけに到達できるプローブを用意します。 [Prometheusのセキュリティモデル](https://prometheus.io/docs/operating/security/) · [Composeのネットワーク](https://docs.docker.com/compose/how-tos/networking/)

```yaml
- targets: [http://health-relay:8120/injector]
  labels: {app: Factory File Generator, module: http_ready}
```

任意のnative CDCを起動した環境では、Composeの `native-cdc` の状態とログ、Kafkaのコンシューマーグループの遅延、Icebergに保存された結果を照合します。[CDCの運用手順](SOURCES.md)を参照してください。緑の✓は監視先への疎通確認です。DB/S3のTCPチェックはポート到達性、HTTPは該当ヘルスの2xxを示します。DB移行の成功はLakekeeperの起動条件です。TLS経由の社外利用体験、実データ品質、バックアップ成功はそれぞれの検査で確認します。

## 反映する

対象定義、ダッシュボードJSON、各サービスの設定は、ビルド時にイメージへ組み込みます。作業ディレクトリでファイルを編集した後は、該当コンテナを `--build --force-recreate` で更新します。Prometheusのfile discoveryとGrafanaのポーリングは、コンテナ内に配置された設定を読みます。

**Bash / Zsh共通**

```bash
project="APP_NAME_FROM_HEADER"
docker compose -p "$project" --env-file "$site" up -d --build --force-recreate --no-deps blackbox prometheus grafana health-relay
docker compose -p "$project" --env-file "$site" exec -T prometheus \
  promtool check config /etc/prometheus/prometheus.yml
docker compose -p "$project" --env-file "$site" exec -T blackbox \
  /bin/blackbox_exporter --config.file=/etc/blackbox/blackbox.yml --config.check
```

例は標準構成です。`project` は起動時のプロジェクト名、`site` は環境ファイルのパスです。閲覧や運用ツールを有効にしている場合は、起動時と同じ順番で追加の `-f`、環境ファイル、プロファイルを指定します。Dokploy管理の構成で設定変更を反映するには、次の段落に従います。

Dokployでは変更をGitへ保存し、対象Composeを再配備します。公開設定はビルド時に読み取り権限を固定してイメージへ組み込みます。Dokployの標準Deployはビルドも実行します。[Advanced → Run Command](COMPANY.md) でカスタムコマンドを使う場合も `--build` を含め、Grafana・Prometheus・Blackboxがhealthyになったことを確認します。GrafanaのUIで試作したパネルはClassic JSONへexportし、`runtime.json` を更新します。配布したダッシュボードは `uid` を維持し、起動時にファイルからプロビジョニングします。GrafanaのUIからの永続上書きは無効です。配信が必要な場合は、次の任意構成を選びます。

[Grafana公式Provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) · [Prometheus file discovery](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config) · [Blackbox Exporterの設定パターン](https://prometheus.io/docs/guides/multi-target-exporter/)

## オンプレミスの通知を追加する

Prometheusが異常を検知し、Alertmanagerが通知のグループ化、抑制、復旧通知を担当します。
[Alertmanager v0.35.0](https://github.com/prometheus/alertmanager/releases/tag/v0.35.0) は
Go製で、[Apache-2.0](https://github.com/prometheus/alertmanager/blob/v0.35.0/LICENSE) です。

1. Dokployで対象Composeの **Advanced → Run Command → Command** を開きます。
   `-f compose.yaml` の後ろ、`up` の前に `-f compose.alerting.yaml` を追加します。
   閲覧や運用ツールを有効にしているときは、使っている追加ファイルも残します。
2. 同じComposeを再配備し、`prometheus` と `alertmanager` がhealthyになることを
   **Docker → Containers** から確認します。標準構成の他サービスは同じComposeに残ります。
3. Grafanaの **Explore** で `up{job="alertmanager"}` が1であることを確認します。
   `prometheus_notifications_errors_total` はPrometheusからAlertmanagerへの
   送信エラー、`alertmanager_notifications_failed_total` は受信先への送信エラーです。
   宛先の設定後に、両方の指標と試験通知を確認します。

標準構成だけに通知を追加する場合のDokploy用例です。`APP_NAME_FROM_HEADER` は
対象サービスの実際のApp Nameに置き換えます。

```text
compose -p APP_NAME_FROM_HEADER --env-file .env -f compose.yaml -f compose.alerting.yaml up -d --build --wait --wait-timeout 180 --remove-orphans
```

既定の [alertmanager.yml](../../native/monitoring/alertmanager.yml) は通知先なしで
異常の状態を集約します。配信を始める際は、運用承認済みの受信サービスを内部の
`alert_delivery` ネットワークで到達できるようにし、同じファイルのルートと受信先を
登録します。例えば、同じComposeに置いた社内受信サービスが `onprem-receiver` の
名前で待機する場合、受信先の例は次のとおりです。ルートの `receiver` も
`onprem-webhook` へ変更します。

```yaml
receivers:
  - name: onprem-webhook
    webhook_configs:
      - url: http://onprem-receiver:8080/receive
        send_resolved: true
```

`alert_delivery` は内部ネットワークです。受信サービスをこのネットワークへ参加させる
場合は、同ネットワークから両監視サービスへ到達できることを前提にします。他ネットワーク
へは、配備先で承認された専用の中継サービスを通します。URLに埋め込む認証値や
送信用の秘密は、Gitやビルド済みイメージへ保存せず、実行時ファイルとして管理します。
受信先の障害などで再送される場合があります。受信側ではWebhookのグループキー、
アラートの指紋・開始時刻・状態を使って、同じ発生の再送を照合します。
単一障害の検査では、発報と復旧の両方が届くことを配備先の試験受信先で確認します。

設定をGitで更新して再配備します。Alertmanagerのポートはホストに公開せず、
コンテナ端末から次を実行できます。名前付きボリューム `alertmanager` は
サイレンスと通知状態を保持します。廃止するときは追加Composeファイルを外し、
同じApp Nameの標準コマンドで `--build --remove-orphans` を付けて再配備します。
履歴のボリュームを削除する場合は、保全の要否を確認します。

**Bash / Zsh共通**

```bash
amtool check-config /etc/alertmanager/alertmanager.yml
amtool --alertmanager.url=http://127.0.0.1:9093 alert
```

[専用のGitHub Actions](../../.github/workflows/alerting.yml) では、Prometheus、
Blackbox、Alertmanager、合成受信サービスだけで、宛先なし、発報、HTTP 503からの再送、
復旧通知を検査します。手元で再現する場合は使い捨てのDocker環境に移り、
リポジトリのルートで `python3 tests/integration/alerting/acceptance.py` を実行します。

[Alertmanager公式の設定](https://prometheus.io/docs/alerting/latest/configuration/) ·
[Prometheusからの送信先設定](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#alertmanager_config)

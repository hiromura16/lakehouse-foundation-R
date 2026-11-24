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

標準起動で `Lakehouse R 稼働監視` がGrafanaに自動作成されます。サーバー自身から `http://localhost:18501/d/lakehouse-r-runtime` を開き、ユーザー名 `admin` とprivate `site.env` の `GRAFANA_ADMIN_PASSWORD` で入ります。初回パスワードはGrafanaデータvolumeが空のときに適用されます。

![合成環境で各サービスの稼働状態と履歴を表示したGrafana](images/runtime-demo.png)

画像は [閲覧画面を追加](GUI.md) した構成です。標準構成は7対象、閲覧画面を追加すると9対象、さらに運用ツールを追加すると14対象が表示されます。標準の常駐コンテナは、監視対象の7個に内部の `read-api` と `health-relay` を加えた9個です。内部の2個はDockerのヘルスチェックで確認します。「サービスの状態」は緑の✓が正常、赤の×が異常です。履歴のバーにカーソルを合わせるとその区間の判定が表示されます。「取込・変換ジョブ」は成功と失敗を別の系列で表示します。「最終ジョブ成功からの経過時間」は調査の目安で、会社の実行間隔に応じたアラートは別途設定します。

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

起動するComposeプロジェクトごとに、標準構成は6個、閲覧を追加すると7個、閲覧と運用ツールを追加すると10個のDockerネットワークを使います。CDCは1個、Cloudflareは2個を追加するため、すべてを同じプロジェクトで有効にすると13個です。隔離したRustFS受入環境を同じホストに追加すると、さらに6個を使います。

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
docker compose --env-file "$site" up -d --build --force-recreate --no-deps blackbox prometheus grafana health-relay
docker compose --env-file "$site" exec -T prometheus \
  promtool check config /etc/prometheus/prometheus.yml
docker compose --env-file "$site" exec -T blackbox \
  /bin/blackbox_exporter --config.file=/etc/blackbox/blackbox.yml --config.check
```

例は標準構成です。閲覧や運用ツールを有効にしている場合は、最初の起動と同じプロジェクト名、追加の `-f`、環境ファイルを指定します。

Dokployでは変更をGitへ保存し、対象Composeを再配備します。公開設定はビルド時に読み取り権限を固定してイメージへ組み込みます。Dokployの標準Deployはビルドも実行します。[Advanced → Run Command](COMPANY.md) でカスタムコマンドを使う場合も `--build` を含め、Grafana・Prometheus・Blackboxがhealthyになったことを確認します。GrafanaのUIで試作したパネルはClassic JSONへexportし、`runtime.json` を更新します。配布したダッシュボードは `uid` を維持し、起動時にファイルからプロビジョニングします。GrafanaのUIからの永続上書きは無効です。通知を配信する場合は、Prometheusのルールに接続するAlertmanagerを導入先で設定します。

[Grafana公式Provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) · [Prometheus file discovery](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#file_sd_config) · [Blackbox Exporterの設定パターン](https://prometheus.io/docs/guides/multi-target-exporter/)

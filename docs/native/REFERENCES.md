# 公式リファレンスと採用バージョン

<!-- doc-purpose:start -->
> **役割:** 固定した版と公式仕様への参照を調べます。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 採用版の理由や更新差分を調べるとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [運用、バックアップ、受入試験](<OPERATIONS.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

2026-11-24（JST）確認。サービスの上流イメージはComposeまたはDockerfileのタグとdigestで固定し、Python実行依存は `native/requirements.lock` の版とhashで固定します。公式ページの最新版とここで検証する版は別に確認します。

| 対象 | 採用版 | 参照した仕様 |
| --- | --- | --- |
| Docker Compose / BuildKit | Compose 2.23.1以上が前提。x86-64の実試験はCompose v5.7.1 | [Configのcontent](https://docs.docker.com/reference/compose-file/configs/) · [Build仕様](https://docs.docker.com/reference/compose-file/build/) · [Build secrets](https://docs.docker.com/build/building/secrets/) |
| Docker proxy・CA | ホスト依存 | [Daemon proxy](https://docs.docker.com/engine/daemon/proxy/) · [CLIとビルドproxy](https://docs.docker.com/engine/cli/proxy/) · [カスタムCA](https://docs.docker.com/engine/network/ca-certs/) |
| Dokploy | Composeサービスの公式手順 | [Docker Compose](https://docs.dokploy.com/docs/core/docker-compose) · [公開リポジトリのHTTPS取得](https://docs.dokploy.com/docs/core/providers) · [GitHub App](https://docs.dokploy.com/docs/core/github) · [Domains](https://docs.dokploy.com/docs/core/docker-compose/domains) |
| DuckDB | 2.0.1 | [Python API](https://duckdb.org/docs/2.0/clients/python/overview) · [Arrowの変換](https://duckdb.org/docs/2.0/clients/python/conversion) |
| Apache Iceberg / PyIceberg | PyIceberg 0.12.0 | [Iceberg仕様](https://iceberg.apache.org/spec/) · [RESTとFileIO](https://py.iceberg.apache.org/configuration/) · [テーブル操作](https://py.iceberg.apache.org/api/) |
| Lakekeeper | 0.14.3 | [Release](https://github.com/lakekeeper/lakekeeper/releases/tag/v0.14.3) · [bootstrapとwarehouse](https://docs.lakekeeper.io/getting-started/) · [S3](https://docs.lakekeeper.io/docs/latest/storage-s3/) |
| SeaweedFS、PostgreSQL | 4.53、18.6 | [SeaweedFS release](https://github.com/seaweedfs/seaweedfs/releases/tag/4.53) · [公式PostgreSQL Dockerイメージ](https://hub.docker.com/_/postgres) |
| Prometheus、Blackbox Exporter、Grafana | 3.16.0、0.29.1、13.3.0 | [Prometheus設定](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) · [Blackbox](https://github.com/prometheus/blackbox_exporter/releases/tag/v0.29.1) · [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) |
| Debezium、Redpanda | 外部Debeziumは設置先の採用版。Redpanda 26.3.1は隔離試験のみ | [SQL Server connector](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html) · [Redpanda単一broker](https://docs.redpanda.com/labs/docker-compose/single-broker/) · [Redpanda 26.3のライセンス](https://docs.redpanda.com/streaming/current/get-started/licensing/overview/) |
| Streamlit、NGINX | 1.67.0、1.30.8 | [Streamlitの表](https://docs.streamlit.io/develop/api-reference/data/st.dataframe) · [NGINXのBasic認証](https://nginx.org/en/docs/http/ngx_http_auth_basic_module.html) · [WebSocket中継](https://nginx.org/en/docs/http/websocket.html) |
| LangChain Core | 1.7.6 | [Tools](https://docs.langchain.com/oss/python/langchain/tools) |
| n8n | 2.49.5 | [Docker Compose](https://docs.n8n.io/deploy/host-n8n/install-options/use-a-cloud-provider/use-docker-compose) · [SSRF保護](https://docs.n8n.io/deploy/host-n8n/configure-n8n/security/enable-ssrf-protection) · [Sustainable Use License](https://docs.n8n.io/n8n-community-license) · [2.49.5の変更](https://github.com/n8n-io/n8n/releases/tag/n8n%402.49.5) |
| OpenObserve | 1.1.2 | [Prometheus remote write](https://openobserve.ai/docs/ingestion/metrics/prometheus/) · [ダッシュボード](https://openobserve.ai/docs/user-guide/analytics/dashboards/) |
| InfluxDB 3 Core、Explorer | 3 Coreはdigest固定、Explorer 1.11.2 | [管理用トークン](https://docs.influxdata.com/influxdb3/core/admin/tokens/admin/preconfigured/) · [SQL API](https://docs.influxdata.com/influxdb3/core/query-data/execute-queries/) · [Explorer](https://docs.influxdata.com/influxdb3/explorer/) |

## 追加機能の配布条件

| コンポーネント | 主な実装 | 採用版の配布条件と確認先 |
| --- | --- | --- |
| Lakekeeper | Rust | Apache-2.0。 [公式ソース](https://github.com/lakekeeper/lakekeeper) |
| Grafana OSS | Go、TypeScript | AGPL-3.0。 [公式ライセンス案内](https://grafana.com/licensing/) |
| OpenObserve OSS | Rust、TypeScript | AGPL-3.0。メトリクスを別の調査画面でも検索する用途で追加できます。 [公式ソース](https://github.com/openobserve/openobserve#license) |
| InfluxDB 3 Core | Rust | MITまたはApache-2.0。 [公式ソース](https://github.com/influxdata/influxdb#license) |
| InfluxDB 3 Explorer 1.11.2 | タイムシリーズのWeb画面 | 採用したDockerイメージの `/app-root/license.txt` に専用Software License Agreementを同梱。利用前に組織で契約条件を確認します。 [Explorerの公式配布手順](https://docs.influxdata.com/influxdb3/explorer/install/) |
| n8n Community | Node.js、TypeScript | ソース公開のSustainable Use License。自組織内の利用、提供や再配布の条件を公式文書で確認します。 [ライセンス本文](https://github.com/n8n-io/n8n/blob/master/LICENSE.md) · [用途別案内](https://docs.n8n.io/n8n-community-license) |
| RustFS 1.0.6 | Rust | Apache-2.0。 [公式リリース](https://github.com/rustfs/rustfs/releases/tag/1.0.6) · [S3互換性](https://docs.rustfs.com/en/reference/s3-compatibility) |
| Nisshi 0.8.2 | Rust | Apache-2.0。 [公式リリース](https://github.com/nisshi-io/solace-nisshi/releases/tag/v0.8.2) · [Kafka互換性](https://docs.nisshi.io/docs/compatibility) |
| Redpanda 26.3.1 Community | C++ | 提供元の専用ライセンス。26.3.1以降のブローカー本体はソース非公開で、無償枠は組織全体で合計50コアです。同梱の隔離試験は固定版イメージを各環境で取得します。 [変更の公式発表](https://www.redpanda.com/blog/r2-the-engine-of-engines) · [契約本文](https://docs.redpanda.com/streaming/current/get-started/licensing/redpanda-community-edition-license/) |

このリポジトリの公開設定と、上流コンポーネントごとの配布条件は個別に確認します。

更新時は依存の版とhash、イメージdigest、入力契約のハッシュ、受入試験結果を同じ差分で確認します。

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

2026-11-24（JST）確認。サービスのイメージは `compose.yaml` のタグとdigestで固定し、Python実行依存は `native/requirements.lock` の版とhashで固定します。公式ページの最新版とここで検証する版は別に確認します。

| 対象 | 採用版 | 参照した仕様 |
| --- | --- | --- |
| Docker Compose / BuildKit | Compose 2.23.1以上が前提。x86-64の実試験はCompose v5.7.1 | [Configのcontent](https://docs.docker.com/reference/compose-file/configs/) · [Build仕様](https://docs.docker.com/reference/compose-file/build/) · [Build secrets](https://docs.docker.com/build/building/secrets/) |
| Docker proxy・CA | ホスト依存 | [Daemon proxy](https://docs.docker.com/engine/daemon/proxy/) · [CLIとビルドproxy](https://docs.docker.com/engine/cli/proxy/) · [カスタムCA](https://docs.docker.com/engine/network/ca-certs/) |
| Dokploy | Composeサービスの公式手順 | [Docker Compose](https://docs.dokploy.com/docs/core/docker-compose) · [GitHub App](https://docs.dokploy.com/docs/core/github) · [Domains](https://docs.dokploy.com/docs/core/docker-compose/domains) |
| DuckDB | 2.0.1 | [Python API](https://duckdb.org/docs/2.0/clients/python/overview) · [Arrowの変換](https://duckdb.org/docs/2.0/clients/python/conversion) |
| PyIceberg | 0.12.0 | [RESTとFileIO](https://py.iceberg.apache.org/configuration/) · [テーブル操作](https://py.iceberg.apache.org/api/) |
| Lakekeeper | 0.14.3 | [Release](https://github.com/lakekeeper/lakekeeper/releases/tag/v0.14.3) · [bootstrapとwarehouse](https://docs.lakekeeper.io/getting-started/) · [S3](https://docs.lakekeeper.io/docs/latest/storage-s3/) |
| SeaweedFS、PostgreSQL | 4.53、18.6 | [SeaweedFS release](https://github.com/seaweedfs/seaweedfs/releases/tag/4.53) · [公式PostgreSQL Dockerイメージ](https://hub.docker.com/_/postgres) |
| Prometheus、Blackbox Exporter、Grafana | 3.16.0、0.29.1、13.3.0 | [Prometheus設定](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) · [Blackbox](https://github.com/prometheus/blackbox_exporter/releases/tag/v0.29.1) · [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) |
| Debezium、Redpanda | 外部Debeziumは設置先の採用版。Redpanda 26.3.1は隔離試験のみ | [SQL Server connector](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html) · [Redpanda単一broker](https://docs.redpanda.com/labs/docker-compose/single-broker/) · [Redpandaのライセンス](https://github.com/redpanda-data/redpanda/blob/dev/licenses/bsl.md) |
| LangChain Core | 1.7.6 | [Tools](https://docs.langchain.com/oss/python/langchain/tools) |

既存互換構成の採用版・仕様は [Foundationの公式リファレンス一覧](../REFERENCES.md) にあります。更新時は依存の版とhash、イメージdigest、入力契約の参照commit、受入試験結果を同じ差分で確認します。

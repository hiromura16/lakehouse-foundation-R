# 宣言台帳: Compose資源・イメージ・供給元

<!-- doc-purpose:start -->
> **役割:** 追跡済みComposeの資源、上限と外部供給の前提を確認する。
>
> **対象読者:** 基盤構築担当 / 開発・設計担当 / 運用担当
>
> **読むタイミング:** Foundation Rの配備とアクセス分離を設計・レビューするとき。
>
> **前提となる資料:** [Foundation Rのゼロトラスト・Dokploy・WAN断耐性の差分レビュー](<zero-trust-dokploy-gap-analysis.md>)
>
> **次に読む資料:** [会社VMの受入手順と合格条件](<../native/ACCEPTANCE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

追跡済みの宣言を表にした静的基準です。値を解決せず、秘密ファイルを読みません。Docker Engineの配備状態・配布物の利用可否・ホストの実使用量は未確認です。CI専用の合成fixtureは運用サービスに含めません。

## 全サービスの宣言

「基底継承」は追記／上書きのみの行です。空のnetwork/port列はこのファイルでの追加がない意味で、基底からの削除や隔離を意味しません。networkがない新規サービスの既定はComposeのdefaultです。mountの `変数` は配置側で確定し、会社環境の解決値は記録しません。メモリはMiB/サービス、CPUはvCPU相当の宣言上限であり、0や実測値ではありません。

### [compose.yaml](../../compose.yaml)

- network定義: `api_origin`, `catalog_db`, `catalog_view`, `metrics_view`, `probes`, `writer`
- volume定義: `catalog-db`, `grafana`, `prometheus`, `storage`
- config定義: `filer`, `storage-auth`
- secret定義: `corporate-ca`, `python-index-auth`, `python-index-config`
- 参照するEnvironment変数名: `CATALOG_DB_PASSWORD`, `CATALOG_ENCRYPTION_KEY`, `COMPOSE_PROJECT_NAME`, `CORPORATE_CA_FILE`, `CORPORATE_CA_SHA256`, `CORPORATE_PYPI_NETRC_FILE`, `CORPORATE_UV_CONFIG_FILE`, `GRAFANA_ADMIN_PASSWORD`, `GRAFANA_COOKIE_SECURE`, `GRAFANA_ROOT_URL`, `HTTPS_PROXY`, `HTTP_PROXY`, `LAKEHOUSE_API_PORT`, `LAKEHOUSE_ARCHIVE_MAX_MIB`, `LAKEHOUSE_GOLD_MAX_CONCURRENT`, `LAKEHOUSE_GRAFANA_PORT`, `LANDING_DIR`, `LANDING_GID`, `LANDING_UID`, `NO_PROXY`, `READER_TOKEN`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `WORKER_TOKEN`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `catalog-db` | 未設定 / 512 | `catalog_db` | なし | `catalog-db:/var/lib/postgresql` | `postgres:18.7-alpine3.24@sha256:0115267ab5a509022df9319c77be365cef335adb71556c75f60f5360ee247e71` |
| `catalog-migrate` | 未設定 / 未設定 | `catalog_db` | なし | なし | `quay.io/lakekeeper/catalog:v0.15.1@sha256:22dcb6044c106a962c242b70051bd226bd50c9134c763121d8713c37fe9a70ec` |
| `catalog` | 未設定 / 512 | `catalog_db`, `writer`, `catalog_view` | なし | なし | `quay.io/lakekeeper/catalog:v0.15.1@sha256:22dcb6044c106a962c242b70051bd226bd50c9134c763121d8713c37fe9a70ec` |
| `storage` | 未設定 / 768 | `writer` | なし | `storage:/data` | `chrislusf/seaweedfs:4.55@sha256:7ca46aeae2f223b37e5d91593c17ab37738eec6df6ab69b48a0487f7643342b9` |
| `lakehouse` | 2 / 2048 | `writer`, `api_origin` | `loopback ${LAKEHOUSE_API_PORT}:8080` | `${LANDING_DIR} → /landing (ro)` | `${COMPOSE_PROJECT_NAME}-lakehouse` / `build:native/Dockerfile` |
| `blackbox` | 未設定 / 96 | `catalog_db`, `writer`, `probes` | なし | なし | `${COMPOSE_PROJECT_NAME}-blackbox` / `build:native/Dockerfile.configured:blackbox` |
| `prometheus` | 未設定 / 512 | `probes`, `api_origin`, `metrics_view` | なし | `prometheus:/prometheus` | `${COMPOSE_PROJECT_NAME}-prometheus` / `build:native/Dockerfile.configured:prometheus` |
| `grafana` | 未設定 / 512 | `metrics_view` | `loopback ${LAKEHOUSE_GRAFANA_PORT}:3000` | `grafana:/var/lib/grafana` | `${COMPOSE_PROJECT_NAME}-grafana` / `build:native/Dockerfile.configured:grafana` |
| `read-api` | 未設定 / 64 | `api_origin`, `probes` | なし | なし | `${COMPOSE_PROJECT_NAME}-read-api` / `build:native/Dockerfile.configured:read-api` |
| `health-relay` | 未設定 / 64 | `probes`, `catalog_view`, `metrics_view` | なし | なし | `${COMPOSE_PROJECT_NAME}-health-relay` / `build:native/Dockerfile.configured:health-relay` |

### [compose.alerting.yaml](../../compose.alerting.yaml)

- network定義: `alert_delivery`
- volume定義: `alertmanager`
- 参照するEnvironment変数名: `COMPOSE_PROJECT_NAME`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `prometheus` | 基底継承 / 基底継承 | `alert_delivery` | なし | なし | 基底継承 / `build:native/Dockerfile.configured:prometheus-alerting` |
| `alertmanager` | 0.25 / 128 | `alert_delivery` | なし | `alertmanager:/alertmanager` | `${COMPOSE_PROJECT_NAME}-alertmanager` / `build:native/Dockerfile.configured:alertmanager` |

### [compose.cdc.yaml](../../compose.cdc.yaml)

- network定義: `cdc_egress`
- 参照するEnvironment変数名: `CDC_ALLOW_PLAINTEXT`, `CDC_BROKER_CA_FILE`, `CDC_SETTINGS_FILE`, `CDC_STATE_DIR`, `CDC_TABLE_MAP_FILE`, `COMPOSE_PROJECT_NAME`, `LANDING_GID`, `LANDING_UID`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `WORKER_TOKEN`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `native-cdc` | 2 / 2048 | `writer`, `cdc_egress` | なし | `${CDC_STATE_DIR} → /state`, `${CDC_SETTINGS_FILE} → /run/cdc/settings.json (ro)`, `${CDC_TABLE_MAP_FILE} → /run/cdc/table-map.json (ro)`, `${CDC_BROKER_CA_FILE} → /run/cdc/broker-ca.pem (ro)` | `${COMPOSE_PROJECT_NAME}-lakehouse` |

### [compose.cloudflare.yaml](../../compose.cloudflare.yaml)

- network定義: `edge`, `edge_read`
- secret定義: `edge-policy`, `edge-reader-token`, `tunnel-token`
- 参照するEnvironment変数名: `CF_EDGE_HTTPS_PROXY`, `CF_EDGE_POLICY_FILE`, `CF_EDGE_READER_FILE`, `CF_TUNNEL_PROTOCOL`, `CF_TUNNEL_TOKEN_FILE`, `CORPORATE_CA_SHA256`, `HTTPS_PROXY`, `HTTP_PROXY`, `LANDING_GID`, `LANDING_UID`, `NO_PROXY`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `edge-gold` | 未設定 / 128 | `edge_read`, `edge` | なし | `secret:edge-policy`, `secret:edge-reader-token` | ローカル生成 / `build:native/edge/Dockerfile` |
| `read-api` | 基底継承 / 基底継承 | `edge_read` | なし | なし | 基底継承 |
| `cloudflared` | 未設定 / 128 | `edge` | なし | `secret:tunnel-token` | `docker.io/cloudflare/cloudflared:2026.11.2@sha256:55c7e3e8d89d5e8d20b75a82f3602cbf98e06db406e73e282073cfdc5a9a3af6` |

### [compose.evaluation.yaml](../../compose.evaluation.yaml)

- volume定義: `broker`, `greptime`, `risingwave`
- config定義: `risingwave-settings`
- secret定義: `corporate-ca`, `python-index-auth`, `python-index-config`
- 参照するEnvironment変数名: `CORPORATE_CA_FILE`, `CORPORATE_CA_SHA256`, `CORPORATE_PYPI_NETRC_FILE`, `CORPORATE_UV_CONFIG_FILE`, `EVAL_GREPTIME_PORT`, `EVAL_RISINGWAVE_PORT`, `EVAL_SAIL_PORT`, `HTTPS_PROXY`, `HTTP_PROXY`, `NO_PROXY`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `sail` / sail | 2 / 1024 | なし | `loopback ${EVAL_SAIL_PORT}:50051` | なし | ローカル生成 / `build:native/evaluation/sail.Dockerfile` |
| `broker` / stream | 未設定 / 768 | なし | なし | `broker:/var/lib/redpanda/data` | `docker.redpanda.com/redpandadata/redpanda:v26.3.1@sha256:260532f70fd840bf2159106bd4ef900f199c1f5ca2a23cb362642320513b90f5` |
| `risingwave` / stream | 2 / 4096 | なし | `loopback ${EVAL_RISINGWAVE_PORT}:4566` | `risingwave:/state` | `risingwavelabs/risingwave:v3.1.2@sha256:7efe24b9006baf4b40cd9623557ebc1c344d3740b81f0b16dac7605aed082d38` |
| `greptime` / timeseries | 2 / 1024 | なし | `loopback ${EVAL_GREPTIME_PORT}:4000` | `greptime:/greptimedb_data` | `greptime/greptimedb:v1.3.1@sha256:a10181273b9400a30fbad6f5043b3d08e388c7d4ee30a7570e5f0241496e9ee1` |
| `ballista-scheduler` / distributed | 1 / 512 | なし | なし | なし | `ghcr.io/apache/datafusion-ballista-standalone:55.0.0@sha256:b531543d6933d785a54bd46c174246681adfa12d046e42ae77c831e44d183b66` |
| `ballista-executor` / distributed | 2 / 1024 | なし | なし | なし | `ghcr.io/apache/datafusion-ballista-standalone:55.0.0@sha256:b531543d6933d785a54bd46c174246681adfa12d046e42ae77c831e44d183b66` |
| `ballista-check` / distributed-check | 未設定 / 512 | なし | なし | なし | ローカル生成 / `build:native/evaluation/ballista.Dockerfile` |
| `comet` / accelerator | 2 / 2048 | なし | なし | なし | ローカル生成 / `build:native/evaluation/comet.Dockerfile` |

### [compose.gui.yaml](../../compose.gui.yaml)

- network定義: `gui_apps`
- 参照するEnvironment変数名: `COMPOSE_PROJECT_NAME`, `CORPORATE_CA_SHA256`, `HTTPS_PROXY`, `HTTP_PROXY`, `LAKEHOUSE_BROWSER_HOST`, `LAKEHOUSE_BROWSER_PORT`, `LAKEHOUSE_CATALOG_PORT`, `LAKEHOUSE_CATALOG_URL`, `LAKEHOUSE_OPS_HTPASSWD`, `LAKEHOUSE_PROMETHEUS_PORT`, `LANDING_GID`, `LANDING_UID`, `READER_TOKEN`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `catalog` | 基底継承 / 基底継承 | なし | なし | なし | 基底継承 |
| `data-browser` | 1 / 512 | `gui_apps` | なし | なし | ローカル生成 / `build:native/gui/Dockerfile` |
| `ops-proxy` | 未設定 / 96 | `gui_apps`, `catalog_view`, `metrics_view` | `loopback ${LAKEHOUSE_BROWSER_PORT}:8501`, `loopback ${LAKEHOUSE_CATALOG_PORT}:8181`, `loopback ${LAKEHOUSE_PROMETHEUS_PORT}:9090` | `${LAKEHOUSE_OPS_HTPASSWD} → /run/lakehouse-auth/htpasswd (ro)` | `${COMPOSE_PROJECT_NAME}-ops-proxy` / `build:native/Dockerfile.configured:ops-proxy` |
| `read-api` | 基底継承 / 基底継承 | `gui_apps` | なし | なし | 基底継承 |
| `health-relay` | 基底継承 / 基底継承 | `gui_apps` | なし | なし | 基底継承 |
| `prometheus` | 基底継承 / 基底継承 | なし | なし | なし | 基底継承 / `build:native/Dockerfile.configured:prometheus` |

### [compose.storage.rustfs.yaml](../../compose.storage.rustfs.yaml)

- network定義: `api_origin`, `catalog_db`, `catalog_view`, `default`, `metrics_view`, `probes`, `writer`
- volume定義: `rustfs-data`
- 参照するEnvironment変数名: `RUSTFS_TEST_LAKEHOUSE_IMAGE`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `storage` | 基底継承 / 768 | `writer` | なし | `rustfs-data:/data` | `docker.io/rustfs/rustfs:1.0.6@sha256:ee717072290bec9b11abfc0365f8310b00df1cd9b3639943edae47bdef05044a` |
| `lakehouse` | 基底継承 / 基底継承 | なし | なし | なし | `${RUSTFS_TEST_LAKEHOUSE_IMAGE}` |

### [compose.telemetry.nisshi.yaml](../../compose.telemetry.nisshi.yaml)

- 参照するEnvironment変数名: `NISSHI_GID`, `NISSHI_STATE_DIR`, `NISSHI_UID`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `broker` | 1.0 / 384 | なし | なし | `${NISSHI_STATE_DIR} → /data` | `ghcr.io/nisshi-io/nisshi:v0.8.2@sha256:885a32be1d49a20f1b70eb302bf40e1d0139caeec917821deacc456400d342a0` |

### [compose.telemetry.yaml](../../compose.telemetry.yaml)

- volume定義: `mqtt-state`, `vector-sensor-state`
- config定義: `mqtt-acl`, `mqtt-settings`, `telegraf-settings`, `vector-settings`
- secret定義: `mqtt-passwords`
- 参照するEnvironment変数名: `MQTT_COLLECTOR_PASSWORD`, `MQTT_PASSWORD_HASH_FILE`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `mosquitto` / telemetry | 未設定 / 128 | なし | なし | `mqtt-state:/mosquitto/data`, `secret:mqtt-passwords` | `docker.io/library/eclipse-mosquitto:2.0.23@sha256:6a332cc7a8dbe011cbc076180cbb76b37c6f0f55809682ea08e7980ef0112ad9` |
| `telegraf` / telemetry | 1.0 / 384 | なし | なし | なし | `docker.io/library/telegraf:1.40.3@sha256:695045186c69152ff71827ec0c4f40216fb10491597af71c00195d98be8c39f2` |
| `vector` / telemetry | 未設定 / 384 | なし | なし | `vector-sensor-state:/var/lib/vector` | `docker.io/timberio/vector:0.62.0-debian@sha256:6266b1d35c844918ff1c48ea2ec4f57c2b0664c46f3e17fc2418d5dda1692068` |

### [compose.tools.yaml](../../compose.tools.yaml)

- network定義: `automation_apps`, `automation_db`, `tools_apps`
- volume定義: `tools-db`, `tools-explorer`, `tools-influx`, `tools-n8n`, `tools-observe`
- config定義: `observe-forward`, `tools-ports`
- secret定義: `influx-admin`, `observe-passcode`, `tools-htpasswd`
- 参照するEnvironment変数名: `COMPOSE_PROJECT_NAME`, `LAKEHOUSE_API_PORT`, `LAKEHOUSE_BROWSER_PORT`, `LAKEHOUSE_CATALOG_PORT`, `LAKEHOUSE_GRAFANA_PORT`, `LAKEHOUSE_PROMETHEUS_PORT`, `LANDING_GID`, `LANDING_UID`, `N8N_IMAGE_REPO`, `OPENOBSERVE_IMAGE_PATH`, `TOOLS_DB_PASSWORD`, `TOOLS_EXPLORER_KEY`, `TOOLS_EXPLORER_PORT`, `TOOLS_HTPASSWD`, `TOOLS_INFLUX_PORT`, `TOOLS_INFLUX_TOKEN_FILE`, `TOOLS_N8N_KEY`, `TOOLS_N8N_PORT`, `TOOLS_N8N_PROXY_HOPS`, `TOOLS_N8N_URL`, `TOOLS_OBSERVE_PASSCODE_FILE`, `TOOLS_OBSERVE_PASSWORD`, `TOOLS_OBSERVE_PORT`, `TOOLS_OBSERVE_USER`, `TOOLS_PORT`, `TOOLS_SECURE_COOKIES`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `tools-db` | 未設定 / 256 | `automation_db` | なし | `tools-db:/var/lib/postgresql` | `postgres:18.7-alpine3.24@sha256:0115267ab5a509022df9319c77be365cef335adb71556c75f60f5360ee247e71` |
| `n8n` | 未設定 / 1024 | `automation_db`, `automation_apps` | なし | `tools-n8n:/home/node/.n8n` | `${COMPOSE_PROJECT_NAME}-n8n` / `build:native/Dockerfile.configured:n8n` |
| `openobserve` | 未設定 / 2048 | `tools_apps` | `loopback ${TOOLS_OBSERVE_PORT}:5080` | `tools-observe:/data` | ローカル生成 / `build:infra/openobserve/Dockerfile` |
| `observe-forwarder` / tools-metrics | 未設定 / 256 | `tools_apps`, `metrics_view` | なし | `secret:observe-passcode` | `prom/prometheus:v3.16.0@sha256:89c1a644817e9ad70fa2af76c84898e327c1de7063735f962e8313b1e28672c3` |
| `influxdb3` | 未設定 / 1536 | `tools_apps` | `loopback ${TOOLS_INFLUX_PORT}:8181` | `tools-influx:/var/lib/influxdb3/data`, `secret:influx-admin` | ローカル生成 / `build:native/tools/influx.Dockerfile` |
| `influx-explorer` | 未設定 / 512 | `tools_apps` | なし | `tools-explorer:/db` | `docker.io/influxdata/influxdb3-ui:1.12.0@sha256:01a29e2e547574dbd112412af0ea81992f05457b0ce887d30453606b864d20e4` |
| `tools-portal` | 未設定 / 96 | `tools_apps`, `automation_apps` | `loopback ${TOOLS_PORT}:8060`, `loopback ${TOOLS_N8N_PORT}:8061`, `loopback ${TOOLS_EXPLORER_PORT}:8062` | `secret:tools-htpasswd` | `${COMPOSE_PROJECT_NAME}-tools-portal` / `build:native/Dockerfile.configured:tools-portal` |
| `read-api` | 基底継承 / 基底継承 | `automation_apps` | なし | なし | 基底継承 |
| `health-relay` | 基底継承 / 基底継承 | `tools_apps`, `automation_apps` | なし | なし | 基底継承 |
| `prometheus` | 基底継承 / 基底継承 | なし | なし | なし | 基底継承 / `build:native/Dockerfile.configured:prometheus` |

### [examples/marimo/compose.yaml](../../examples/marimo/compose.yaml)

- network定義: `frontend`, `offline`
- 参照するEnvironment変数名: `MARIMO_APPROVED_DIR`, `MARIMO_GID`, `MARIMO_UID`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `notebook` | 1.0 / 768 | `offline` | なし | `./notebook → /work (ro)`, `${MARIMO_APPROVED_DIR} → /approved (ro)` | `ghcr.io/marimo-team/marimo:0.25.2-sql@sha256:f154a40fba17cda7581edd3a3ef6c371d46c69a0e45dcd825293554e5d62e4c6` |
| `gateway` | 未設定 / 48 | `frontend`, `offline` | `loopback dynamic → 8080/tcp` | `./nginx.conf → /etc/nginx/nginx.conf (ro)` | `docker.io/library/nginx:stable-alpine@sha256:8eb2b5540eae779603cc3db84b3986a15ecb5308b54303146eee9837c137b51a` |

### [examples/rill/compose.yaml](../../examples/rill/compose.yaml)

- network定義: `frontend`, `offline`
- 参照するEnvironment変数名: `RILL_GATEWAY_GID`, `RILL_GATEWAY_UID`, `RILL_PASSWORD_FILE`

| service / profile | CPU・メモリ | network | host port | mount・secret名 | イメージまたはbuild |
| --- | --- | --- | --- | --- | --- |
| `rill` | 1.0 / 1024 | `offline` | なし | `./project:/project:ro`, `./analytics.yaml:/home/rill/.rill/config.yaml:ro` | `docker.io/rilldata/rill:v0.92.1@sha256:4a9b7c8729ab9fa4fff640c4023739d31690e2b77323b48dfbe95b018983afc6` |
| `gateway` | 未設定 / 48 | `frontend`, `offline` | `loopback dynamic → 8080/tcp` | `./nginx.conf → /etc/nginx/nginx.conf (ro)`, `${RILL_PASSWORD_FILE} → /run/operator.htpasswd (ro)` | `docker.io/library/nginx:stable-alpine@sha256:8eb2b5540eae779603cc3db84b3986a15ecb5308b54303146eee9837c137b51a` |

## 合計と配備判定

複数ファイルに現れる同名serviceは追加台数として数えません。各表の値は宣言のあるサービスだけを加算。どちらかの上限が未設定なら、合計は有限上限ではなく**既知分のみ**です。CPU未設定の例をメモリ上限で代用しません。CPU/メモリ外にホスト・Dokploy・Traefik・Docker・既存PoC、WAL/queue/監視のディスクと余裕を別枠で確認します。

| 選択 | 内訳のサービス数 | 既知CPU | 既知メモリMiB | 未設定・排他 |
| --- | ---: | ---: | ---: | --- |
| core | 10 | 2 | 5088 | CPU未設定9。メモリ未設定は単発catalog-migrate |
| GUI差分 | 2 | 1 | 608 | ops-proxyのCPU未設定。4つの既存serviceへの追記は重複除外 |
| tools差分（tools-metrics発効なし） | 6 | なし | 5472 | CPUはいずれも未設定。既存serviceへの追記は重複除外 |
| tools-metrics差分 | 1 | なし | 256 | observe-forwarderのCPU未設定 |
| CDC差分 | 1 | 2 | 2048 | 外部ソース・brokerの資源は含まない |
| Cloudflare差分 | 2 | なし | 256 | CPU未設定2。read-api追記を除外 |
| alerting差分 | 1 | 0.25 | 128 | Prometheusのbuild差分のみは基底を継承 |
| telemetry差分 | 3 | 1 | 896 | broker/GreptimeDBは別宣言のため重複加算しない。単体選択可とは判定しない |
| sail | 1 | 2 | 1024 | 評価のSail |
| stream | 2 | 2 | 4864 | brokerのCPU未設定。RisingWaveと既定broker |
| timeseries | 1 | 2 | 1024 | GreptimeDB |
| distributed | 2 | 3 | 1536 | Ballista scheduler/executor |
| distributed-check | 1 | なし | 512 | 検証コンテナのCPU未設定。distributedの稼働が別途必要 |
| accelerator | 1 | 2 | 2048 | Spark/Comet |
| evaluation全profileの追加分 | 8 | 11 | 11008 | brokerとcheckのCPU未設定。全候補の運用可能性は未証明 |
| Nisshi評価差分 | 置換1 | ＋1（置換後のbroker） | 384（置換後のbroker） | 既定brokerの768 MiBを置換するためstreamは4480 MiB/3 CPU。両方を合算しない |
| RustFS差分 | 置換1 | 未設定 | 768（置換後storage） | SeaweedFSを置換。両方を合算しない。lakehouseの差分は基底の資源制限を引き継ぐ前提を合成で確認 |
| marimo単独例 | 2 | 1 | 816 | gatewayのCPU未設定、標準・評価の全profileには混ぜない |
| Rill単独例 | 2 | 1 | 1072 | gatewayのCPU未設定、別Compose例 |

候補 `standard`／`local-operations` は確定していないため、定義のない合計を出しません。採用時はPR2で**Compose合成後の**差分と上限を計算し、PR9で容量予算とアラート、現行PoCとの併存余裕を判定します。

## ビルド入力とライセンス

上表の外部imageと次のベース/COPY入力は追跡宣言。タグの存在・供給・署名・プラットフォーム、ローカルビルドの最終digestやSBOM、社内キャッシュからのコールド起動は未確認です。Dockerfileローカルstage参照は外部imageではありません。

- [native/Dockerfile](../../native/Dockerfile): `docker.io/library/python:3.14-slim@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b`, `ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490`
- [native/Dockerfile.configured](../../native/Dockerfile.configured): `prom/prometheus:v3.16.0@sha256:89c1a644817e9ad70fa2af76c84898e327c1de7063735f962e8313b1e28672c3`, `prom/blackbox-exporter:v0.29.1@sha256:ea1c7aa3ef3f307cd03e85f34abb604478ccb90b567fc690da0cd9776da76b13`, `grafana/grafana:13.3.2@sha256:81556409ba48b2b922e0adb69a2511337d8ba7578f11962eefa6149a2d815101`, `docker.io/library/nginx:stable-alpine@sha256:8eb2b5540eae779603cc3db84b3986a15ecb5308b54303146eee9837c137b51a`, `${N8N_IMAGE_REPO}:2.49.5@sha256:c349b24d6e242b4ea14d75e6f1a90c50e348cf4b1c4512b4a0a50219ead6533d`, `docker.io/prom/alertmanager:v0.35.0@sha256:e638797febd034838eca8b98cdbd46f951e9bf7f178777aa8669167406a9946c`
- [native/edge/Dockerfile](../../native/edge/Dockerfile): `docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341`, `ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490`
- [native/gui/Dockerfile](../../native/gui/Dockerfile): `docker.io/library/python:3.14-slim@sha256:22eafb370920ad8b429e0aec3fff433e0669968519f5d60469d83729c138b28b`, `ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490`
- [native/tools/influx.Dockerfile](../../native/tools/influx.Dockerfile): `docker.io/library/influxdb:3-core@sha256:9eb55d1873ea629e497179cc465d1d345a78f4bce5c5bc318d3d3ac0b8647022`
- [infra/openobserve/Dockerfile](../../infra/openobserve/Dockerfile): `docker.io/library/busybox:1.37.0-uclibc@sha256:7aa3d3c53cc1ed823c6a5967ebaac87700a5b0766af45ff599dbca3506c8b072`, `${OPENOBSERVE_IMAGE_PATH}:v1.2.0@sha256:896361d533cac56ef346d940b1ddd6664e04d5f9606bd80b4a2e28069a33020e`
- [native/evaluation/sail.Dockerfile](../../native/evaluation/sail.Dockerfile): `docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341`, `ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490`
- [native/evaluation/ballista.Dockerfile](../../native/evaluation/ballista.Dockerfile): `docker.io/library/python:3.14-slim@sha256:ac25e5afbfdb4a94ff99cc24b0345719210133144481480bcba6085efa36a341`, `ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490`
- [native/evaluation/comet.Dockerfile](../../native/evaluation/comet.Dockerfile): `docker.io/apache/spark:4.2.0-scala2.13-java17-python3-ubuntu@sha256:84a4eedb1abcf36a90808d5a1310e3e910b78c85d85aaa1599e31af5f862ed59`

| 配布物 | 上流の開示・ライセンス（要確認の区分） | 扱い |
| --- | --- | --- |
| PostgreSQL、SeaweedFS、Lakekeeper | [PostgreSQL License](https://www.postgresql.org/about/licence/)、[Apache-2.0](https://github.com/seaweedfs/seaweedfs)、[Apache-2.0](https://github.com/lakekeeper/lakekeeper/blob/main/LICENSE) | core。利用するbundleの追加依存・NOTICEは配布物ごとに照合 |
| DuckDB、PyIceberg、Python/uv | [DuckDB MIT](https://github.com/duckdb/duckdb/blob/main/LICENSE)、[PyIceberg Apache-2.0](https://github.com/apache/iceberg-python/blob/main/LICENSE)、Pythonは[PSF](https://docs.python.org/3/license.html)、uvは[MIT/Apache-2.0](https://github.com/astral-sh/uv) | local buildの内部依存。lockを完全SBOMとは呼ばない |
| Prometheus、Blackbox Exporter、Alertmanager、NGINX | Prometheus系[Apache-2.0](https://github.com/prometheus/prometheus/blob/main/LICENSE)、[NGINXのBSD系](https://github.com/nginx/nginx/blob/master/LICENSE) | 局所監視・固定境界。コンテナ内の追加部品は別 |
| Grafana OSS、OpenObserve OSS | [Grafana AGPL-3.0系](https://github.com/grafana/grafana/blob/main/LICENSING.md)、[OpenObserve AGPL-3.0](https://github.com/openobserve/openobserve#license) | 商用配布物／プラグインの条件を一括判断しない |
| n8n、InfluxDB 3 Core、Explorer | [n8n Sustainable Use License](https://github.com/n8n-io/n8n/blob/master/LICENSE.md)、[InfluxDB Core MIT/Apache-2.0](https://github.com/influxdata/influxdb#license)、Explorer採用版は[公式配布手順](https://docs.influxdata.com/influxdb3/explorer/install/)と同梱条項を参照 | n8nとExplorerは無条件のOSS扱いにしない。アプリごとに組織の利用・改変・配布条件を確認 |
| MQTT収集、Telegraf、Vector、Nisshi | [Mosquitto EPL/EDL](https://mosquitto.org/)を配布版で照合、[Telegraf MIT](https://github.com/influxdata/telegraf/blob/master/LICENSE)、[Vector MPL系](https://github.com/vectordotdev/vector/blob/master/LICENSE)、[Nisshi v0.8.2](https://github.com/nisshi-io/solace-nisshi/releases/tag/v0.8.2)は既存台帳でApache-2.0 | Nisshiは別broker評価。装置側の使用許諾・通信許可も別 |
| Redpanda、RisingWave、GreptimeDB、RustFS | [Redpandaの26.3.1](https://docs.redpanda.com/streaming/current/reference/releases/redpanda/)は現行engine/提供条件を確認し旧BSLの一律評価不可。[RisingWave Apache-2.0](https://github.com/risingwavelabs/risingwave/blob/main/LICENSE)、[GreptimeDB core Apache-2.0 / Enterprise別](https://github.com/GreptimeTeam/greptimedb/blob/main/README.md#license)、[RustFS 1.0.6](https://github.com/rustfs/rustfs/releases/tag/1.0.6)は既存台帳でApache-2.0 | 評価専用。option・配布バイナリの付属条項、正確なタグを採用前に確認 |
| Sail、DataFusion/Ballista、Spark/Comet、marimo、Rill | [Sail Apache-2.0](https://github.com/lakehq/sail/blob/main/Cargo.toml)、[Ballista Apache-2.0](https://github.com/apache/datafusion-ballista)、[Comet Apache-2.0](https://github.com/apache/datafusion-comet/)、[marimo Apache-2.0](https://github.com/marimo-team/marimo/blob/main/LICENSE)、[Rill Apache-2.0](https://github.com/rilldata/rill) | 評価・個別例。採用前に固定版のLICENSE/NOTICEとイメージの追加依存確認 |
| Cloudflare、Dokploy、Traefik、Entra ID | [cloudflared](https://github.com/cloudflare/cloudflared)の実装と、Cloudflare Access/Tunnel/R2のサービス条件は別。[Dokploy](https://github.com/Dokploy/dokploy)、[Traefik](https://github.com/traefik/traefik)は対象環境の実際の版・配布物を照合。Entraは外部の認証サービス | 利用プラン・保守条件・認証継続性は未確認。ローカルcoreの依存にしない |

この表は技術棚卸しであり、版ごとの法務判断や再配布の承認ではない。[既存の版・ライセンス参照](../native/REFERENCES.md)も確認する。未確定の版・追加featureは未確認のまま扱う。

## 外部到達の要否

| 経路 | 依存と扱い |
| --- | --- |
| core・固定監視 | source、Kafka、DNS、object/catalogと監視宛先をオンプレで提供する前提。既定のローカル起動順はcloudflared/R2なし。通常稼働と停止後の再起動に必要なイメージ/CA/DNS、遠隔ソースの有無は別試験 |
| build/更新 | image registry、パッケージ索引、評価用artifact、Gitが必要になり得る。siteのproxyやCAを実環境記録へ限定。実際の配布の到達性、版対応、local cache／mirrorのオフライン動作は未確認 |
| 人向けAccess（提案） | ブラウザーとAccess/IdPの到達、cloudflaredの外向き接続と鍵・名前解決。オリジンは固定Traefik入口以外に転送しない。端末やサービスが変わっても信頼済みと見なさない |
| 既存Gold参照 | edge-goldのAccess鍵配布と検証、cloudflaredの外向き接続。許可されるデータとは別のhealthを保ち、失敗してもcore readinessを下げない |
| R2搬出 | 現状は承認manifestを使う専用CLI。バケットに限定した別資格と外部HTTPSが必要。配送台帳・定期送信は将来。それまで単に到達不能をもって欠損と判定しない |
| 定期処理・通知・任意tools | n8nのworkflow、webhook、メール／HTTP通知、設備プロトコルは選ぶ構成で異なる。宣言から現場の宛先や無通信を保証しない。ローカル監視と外部通知は分離 |
| 評価・代替storage | プロファイル定義上は別候補。外部の配布/診断・運用上の到達先を審査し、標準採用が決まるまで通常のネットワーク・資源と混ぜない |

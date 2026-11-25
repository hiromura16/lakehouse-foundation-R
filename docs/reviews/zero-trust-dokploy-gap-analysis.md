# Foundation Rのゼロトラスト・Dokploy・WAN断耐性の差分レビュー

<!-- doc-purpose:start -->
> **役割:** 追跡済み構成を棚卸しし、オンプレの自律運転を守る設計と段階的な受入条件を決める。
>
> **対象読者:** 基盤構築担当 / 開発・設計担当 / 運用担当
>
> **読むタイミング:** Dokployの配備モードや人アクセス、WAN断耐性の変更を設計・レビューするとき。
>
> **前提となる資料:** [アーキテクチャと業務契約](<../native/ARCHITECTURE.md>) / [Dokployと企業ネットワーク](<../native/COMPANY.md>)
>
> **次に読む資料:** [オンプレとCloudflareをつなぐ](<../native/CLOUDFLARE.md>) / [会社VMの受入手順と合格条件](<../native/ACCEPTANCE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

対象: Foundation R の `main`（調査時点の先頭: `8eda78b82bac77c14374e8fa4ba843cbdf2faaf8`）。既存Foundation/PoCは対象外。リポジトリの宣言・実装・既存テストに基づく静的レビューであり、配備先・認証基盤の実態や本差分の実機受入を確認したものではない。以下の「現状」は追跡済みソースで確認した事実、「提案」は実装前の判断案、「未確認」はマージ判定に必要な証拠を表す。

## 1. 判定と設計の優先順位

**オンプレの処理と監視を守ることを先に確定し、その境界の外側にアクセスとデータ搬出を追加する。** 通常時にWeb入口を統一しても、WAN断中に取込・CDC・原本保存・Iceberg・Silver/Gold・ローカル監視を止めない。WAN復旧後は永続保存済みの位置から続け、欠損と有害な二重適用を結果表と台帳で検査する。既存PoCとの共存は各段階の必須条件であり、最後にまとめて判断しない。

- **再利用する:** 既存の分割Compose、専用認証と用途別ネットワーク、有限の読取ゲートウェイ、明示的なR2搬出、固定CDCのcheckpoint・shadow/replay、既存preflightと復旧試験。
- **まだない:** Dokployの実効構成を単一の配備モード計画として照合する仕組み、利用者用Traefik/Accessの経路、意味の異なる2種類の障害時アクセス、永続配送状態とWANをまたぐ検証済み再送契約。
- **ここで決めない:** テナントのグループ・端末条件、承認データ、外部宛先、回線断想定時間・最大滞留量・復旧目標。要件の参照先にある別冊のWAN要件は確認できていない。具体閾値は仮定で補完しない。

## 2. リポジトリの現物

- [アクセス行列](zero-trust-dokploy-access-matrix.md): 入口、利用者、オフライン時の許可条件。
- [ADR](zero-trust-dokploy-adr.md): 選択と代案、撤回条件。
- [脅威・試験表](zero-trust-dokploy-threat-test.md): リスクと試験方法、未実施のゲート。
- [宣言台帳](zero-trust-dokploy-inventory.md): service、port、network、volume、キー名、image/digest、上限、供給元と外部通信。

### サービス、ネットワーク、入口

| 宣言 / グループ | 現状（宣言上） | 設計差分 |
| --- | --- | --- |
| [core Compose](../../compose.yaml) | `catalog-db`、`catalog-migrate`、`catalog`、`storage`、`lakehouse`。PostgreSQL、Lakekeeper、SeaweedFS、DuckDB/PyIcebergによるローカル経路。DBは `catalog_db`、書込・保管は `writer`。双方とも内部。 | cloudflared/R2/外部DNS/IdPを起動依存とローカルliveness/readinessから除外することを合成モデルと障害試験で保証する。DB/storageを公開対象にしない。Lakekeeperの内部認可設定はネットワーク保護の代わりにならない。 |
| 同ファイルのAPI・監視 | `read-api` は `api_origin`＋`probes`、`health-relay` は `probes`＋`catalog_view`＋`metrics_view` に接続する。 `blackbox`、`prometheus`、`grafana` で局所監視。 `lakehouse` API とGrafanaはloopbackホストportあり。 | APIホストportの利用者（管理・定期実行・ローカル試験）を確認してから分離。Grafana閲覧と管理はアプリ内roleを保持。Prometheus直公開は不採用。 |
| [GUI Compose](../../compose.gui.yaml) | `data-browser`、`ops-proxy`、Lakekeeper UI設定のoverride、既存API・probeの `gui_apps` への接続。 `ops-proxy` はブラウザー、カタログ、Prometheus向けのloopback入口を持つ。 | 新しい通常WAN経路をそのまま旧プロキシへ重ねない。Lakekeeperの限定参照境界を保持し、公開対象のUIと内部APIを別々に検証する。 |
| [Tools Compose](../../compose.tools.yaml) | `tools-db`、`n8n`、`openobserve`、任意の `observe-forwarder`、`influxdb3`、`influx-explorer`、`tools-portal`。DB専用 `automation_db`、 `automation_apps`、 `tools_apps`。OpenObserve、InfluxDB、tools-portalの複数loopback portあり。 | n8nの編集UI・手動操作と受信webhookは別の対象。診断UI・管理UI・APIの一括公開はしない。ブラウザー向けbase URLと内部サービス接続先を分離。 |
| [CDC Compose](../../compose.cdc.yaml) | `native-cdc` は `lakehouse` 準備を待つ。 `writer` と `cdc_egress` に接続し、ソース設定は外部の読取用ファイル。単体でKafka/Connectを起動する定義ではない。 | WAN断でも続く範囲はサイト内broker、ソース、DNS、資格と保持時間を実測するまで保証不可。比較用brokerを自動で標準に昇格させない。 |
| [Cloudflare Compose](../../compose.cloudflare.yaml) | `edge-gold` は `edge_read` と `edge` に接続し、限定 `read-api` を読む。 `cloudflared` は同じ `edge` のみに接続して `edge-gold` を待つ。一般利用者のTraefikまでを収容するファイルではない。 | 既存の承認済みGold APIを人向けトンネルと扱わない。人向けコネクタは別ネットワーク・資格・policy、データpull型とR2 push型も別の契約として管理。 |
| [通知](../../compose.alerting.yaml)・[計測](../../compose.telemetry.yaml)・[設備差分](../../compose.telemetry.nisshi.yaml) | `alertmanager` と `prometheus` の配送用接続、 `mosquitto`、 `telegraf`、 `vector`、broker差分。送信先への到達とローカルの監視対象は同義ではない。 | ローカル保存・監視を継続し、外部通知不能は別状態として記録する。設備側の運用契約なしにportを追加しない。 |
| [評価Compose](../../compose.evaluation.yaml)・[RustFS差分](../../compose.storage.rustfs.yaml) | `sail`、 `broker`、 `risingwave`、 `greptime`、 `ballista-scheduler`／`ballista-executor`／`ballista-check`、 `comet`。RustFSは `storage` を置換する別構成。 | RustFSは既存SeaweedFSと同じprojectやvolumeを使わない。評価は候補別に起動する。Rill・marimoは個別例であり、標準の常駐GUIには含めない。 |

基点のルートComposeにはDokploy向けの有効な公開Traefikルーターや `dokploy-network` 接続は見当たらない。一方、[配置文書](../native/COMPANY.md)にはDomainsでGrafanaをHTTPS化する手順がある。リポジトリ外の実際のDomains、ネットワーク、listen、firewallの有無を否定する根拠にはならない。loopback宣言も、別コンテナ・同一ホスト・既存ポート転送経由からの到達不可を証明しない。

### ボリューム、設定、資格情報（キー名のみ）

| 境界 | 追跡済み設定・資源の例 | 受入での扱い |
| --- | --- | --- |
| core | 名前付き `catalog-db`、`storage`、`prometheus`、`grafana`。 `LANDING_DIR` は外部・読取専用bind、 `LANDING_UID`／`LANDING_GID` を固定。 `CATALOG_DB_PASSWORD`、`CATALOG_ENCRYPTION_KEY`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`、`WORKER_TOKEN`、`READER_TOKEN`、`GRAFANA_ADMIN_PASSWORD`。 | 取込権限と閲覧権限を分離。project切り替えで空のvolumeや別landingへ向いても成功扱いしない。旧PoCと名前・所有・永続データを共有しない。 |
| 上書きと監視・評価 | tools DB/n8n/観測/Influx/Explorerの各専用volume、通知・MQTT・sensor state、評価broker/RisingWave/GreptimeDB、RustFS専用data。CA・パッケージ索引資格はビルド用の別境界。 | ホストの名前付きvolume、bind実体、所有UID/GID、権限、バックアップと復元を配備VMで確認。snapshotとリトライキューの保持はルートfsに任せない。 |
| 人アクセスとデータ | 既存データpullの `CF_TUNNEL_TOKEN_FILE`、`CF_EDGE_POLICY_FILE`、`CF_EDGE_READER_FILE` は別の役割。R2 CLIは承認manifestとバケットに限定した別資格で処理。 | 人のTunnel/Accessアプリと共有しない。マスクされた画面を写真に撮る代わりに、key存在・読取可能性・policy種別だけを記録。JSON化したCompose全体もログやartifactに出さない。 |

基点の宣言で扱うその他の機密入力は、GUIの `LAKEHOUSE_OPS_HTPASSWD`、toolsの `TOOLS_DB_PASSWORD`、`TOOLS_N8N_KEY`、`TOOLS_OBSERVE_PASSWORD`、`TOOLS_EXPLORER_KEY`、`TOOLS_HTPASSWD` とtoken/passcode file、計測の `MQTT_COLLECTOR_PASSWORD`／`MQTT_PASSWORD_HASH_FILE`、ビルドの索引用設定・認証fileです。種類ごとに所有者と読取主体を限定します。 `CDC_SETTINGS_FILE`／`CDC_TABLE_MAP_FILE`／`CDC_BROKER_CA_FILE`／`CDC_STATE_DIR` は読取・状態の別境界です。設定名は値・パス・実行時の権限の証拠ではありません。

[設定生成](../../native/setup.py)は一回限りのファイル作成と権限管理を行う。管理者が配備時に既存資格情報を再生成することは、この段階の条件ではない。

### NGINXを一括で削除しない

- [GUIのNGINX](../../native/gui/nginx.conf)はBasic認証、ブラウザーへのプロキシ、カタログの専用ビュー、Prometheus閲覧を収容する。特にカタログのURLを分け、管理用の特定経路を制限している。変換・保存の依存ではないが、置換の際に認可境界・URL・アップグレードを型別に再現する必要がある。
- [ToolsのNGINX](../../native/tools/nginx.conf)はポータルの静的配信と固定状態リレー、n8n・Explorerの専用入口を担当する。通常経路から取り除く場合もこの機能の担当を決める。n8nの外部URLや信頼ヘッダーを盲目的に維持しない。
- [core Compose](../../compose.yaml)の `read-api`／`health-relay` もNGINXを使う。公開UIプロキシと違い、内部の固定参照・監視境界であるため**削除候補に含めない**。Rill・marimoの例も独立に判断する。

## 3. 目標モードと拒否基準

| 提案する運用区分 | 起動条件 | 障害時の扱い |
| --- | --- | --- |
| `core-local` | core、認められたオンプレ入力・CDC、ローカル監視。必要なイメージと設定はローカルに保持できること。 | 外部DNS、Access、IdP、Tunnel、R2はreadinessに含めない。温存中の継続と、WAN断中のコールド再起動を分けて確認する。 |
| `local-operations` | 承認済みの管理／業務ネットワーク上で、普段の操作に必要なローカルの設定、認証、RBAC。 | WAN断に備えて設ける業務継続経路であり、匿名や広範囲へ開かない。緊急管理者向けのbreak-glassとは別。 |
| `standard` | 上記に通常の利用者入口と選択したtoolsを追加。人向けAccessは明示設定。 | 外部ログインと搬出の障害は別の `cloud-degraded` とし、coreをnot-readyにしない。社内の一般利用者に直接ホストportを配らない。 |
| `data-egress` | 対象Gold、公開世代、送信先、転送用資格、滞留上限を別で承認。 | 外部の障害を観測・記録し、承認済みキューを再送。データ基盤や認証基盤と一緒に再起動しない。 |
| `evaluation` / `all-lab` | 選択プロファイルまたは比較用セットを明示し、CPU・メモリ・ディスク予算を表示。 | OOM・枯渇の影響を既存PoCと分離。全候補表示は通常採用でも同一データを共有する意味でもない。 |

Dokployを配備の正本とし、**再現可能なCompose合成計画と、選択UIだけを登録するDokploy Domains**を第一候補とする。リポジトリの手書きTraefikルーターと二重管理しない。理由は既存配置手順と[DokployのCompose Domains](https://docs.dokploy.com/docs/core/docker-compose/domains)に整合するため。最終決定は対象版のPreview、実効ネットワーク、Domainsの追加内容、既存アプリとの共存を確認した後。Docker Composeの `include`、統合生成、アプリ分割は、ビルドcontext・相対パス・秘密ファイル・ロールバックが揃わない場合の代案。

**通常の人アクセス:** 標準IdPは Microsoft Entra ID。本人確認とMFA/グループ/端末条件 → Accessで到達認可 → Tunnelで転送 → Dokploy Traefikの公開許可個別UI → アプリ内RBAC。Tunnelの設置だけではアクセス認可にならない。公開ルート作成前に対象Accessを用意し、オリジンの別hostname・生IP・意図しないTraefik entrypointからバイパス不可を検査する。Tunnelからは明示したTraefik入口だけを使い、他サービスや管理listenerへ転送しない。署名・想定issuer/audience等で検証していないIDヘッダーは、アプリ認可に渡さない。根拠: [Cloudflareの境界設計](https://developers.cloudflare.com/reference-architecture/architectures/identity-gated-edge/)、[TraefikのDocker provider](https://doc.traefik.io/traefik/reference/install-configuration/providers/docker/)。

| 対象の分類 | 通常入口の提案 | 検証する固有条件 |
| --- | --- | --- |
| Portal、Grafana閲覧、選択した分析UI | 明示許可されたhostのみAccess、閲覧グループをアプリ側にも対応させる。 | 不許可グループ、role昇格、別hostname、WebSocket・リンク・ログアウト。 |
| n8n、OpenObserve、Influx Explorer、Lakekeeper UI、編集系 | private側を優先し、操作グループ＋MFA＋管理端末。一般Web UIと同じ公開集合にはしない。 | アプリ独自RBAC、n8n webhookと編集・callbackの分離、内部API/managementの否定試験。 |
| Dokploy、SSH、DB管理、storage管理、backup/restore | 別の管理経路、鍵・アクセス記録・限定者。 | WAN断でも諸権限を維持。break-glassは限定・時間制御・監査・復旧記録付き。 |
| DB、storage、内部API、Prometheus | 公開ルーター・共有Traefik側networkなし。 | ホストport、network membership・route、および実到達を別々に確認。 |

## 4. WAN断・復旧を「動いた」とする条件

1. **障害範囲を先に定義。** オンプレ内のソースとbroker/DNSが生き、外部DNS・IdP・Tunnel・R2への到達だけを個別に止める条件と、遠隔ソース自身が不達になる条件は別。後者で届いていないデータの処理継続を約束しない。既存PoCや会社網の設定を変えず、隔離環境で依存先を模擬する。
2. **断前・断中・復旧を同じrunで追跡。** 一意IDを持つ合成ファイルとCDCイベントを投入し、accept/skip/quarantine、各ソース位置、Raw、Silver/Goldの件数とhashを記録。API・DB・storage・ローカルの監視が継続することと、人のログインや外部送信のNGを両方記録する。
3. **送信と確定を分ける。** [R2搬出](../../native/edge/r2.py)の内容照合付き作成とmanifest最後の発行は再利用候補であり、永続キューや自動追いつきを実装済みとは言えない。承認した公開世代＋content hashを配送IDにし、送信成功と確認済みcheckpointを別々に記録。送信プロセス再起動、相手への格納成功後かつローカルACK前に応答を失った場合を含め、同一バイトの既送達と異内容競合を区別する。繰返し失敗・不正payloadは隔離し、保管期限と手動再試行を決める。
4. **観測の正本を指定。** [CDCのshadow/replay](../../native/cdc_shadow.py)と既存checkpointテストはオンプレのローカル処理順の根拠であり、WAN配送台帳ではない。送信待ち件数／最古経過／容量と空き、再送・重複抑止、隔離、最終成功を独立したメトリクスにする。欠損は一意ID集合差、重複は件数よりも同じsnapshotへの有害な再適用0で判定。
5. **限界と回復を明示。** 必要なキュー容量はピーク入力率×受理する最長断時間に、再送間隔・圧縮・安全余裕を加えて決める。上限到達前に警告、到達時は安全停止または既に承認されたbackpressure。一時停止中に未確認のデータを削除・既読化しない。brokerまたはキューの保持期間を越えた場合は、欠損区間と開始位置を明示し、完全に回復できたと表示しない。試験閾値と復旧目標は運用側承認待ち。

[edge試験](../../tests/integration/edge/acceptance.py)、[CDC checkpoint試験](../../tests/integration/native/cdc_checkpoint.py)、[復旧試験](../../tests/integration/recovery/run.sh)がある。固定署名・対象外経路・鍵配布失敗やローカル経路等の個別試験に使える。現実のAccess/IdP/管理端末、回線復旧後の全経路、又は同居環境の無影響を証明するものではない。

## 5. 改訂PR順と完了ゲート

各PRは最新main・重複の確認 → 合格条件の固定 → 実装 → 独立レビューと修正再確認 → 必須検査 → 変更範囲に必要な人手確認 → 通常マージ → 反映確認。BLOCKER/MAJOR、CI不成功、必須の実機証拠がない場合はマージしない。前段階の未解決事項を次のPRで隠さない。

| 順番 | 変更単位 | 受入条件／切り戻し |
| --- | --- | --- |
| 1 | 調査・設計確定と優先順位 | 本レビュー、アクセス行列と脅威、ADRと台帳の論点を独立確認。まず方針合意。変更は文書のみ。 |
| 2 | Dokploy配備モード、Environment例、Compose計画、preflight | 全候補のサービス、ポート、資源名、必要keyを合成値のみで照合。 `core-local` はクラウドサービスなしで解決。現行Compose入口を維持して戻せる。 |
| 3 | Traefik境界、local port分離 | 選択UIだけ検出。内部公開とhost portの迂回がない。明示local overrideは本番合成に含めない。新規routeだけ個別に戻せる。 |
| 4 | 人向けAccessとGold連携を分離 | separate token・network・policy・運用監視。人経路の無認証拒否、既存Gold連携の許可世代・分離。外側の経路を先に無効化できる。 |
| 5 | GUIのURL、WebSocket等の整合、ops-proxy任意化 | Portal/Grafana/n8n等のアプリ内認可を維持。標準とlocal fallbackで実挙動を確認。内部read-api/health-relayは維持。 |
| 6 | ローカル自律運転、WANに依存しないhealth | core-local/local-operationsで取込～監視を通し、WAN停止中の受入を確認。準備済みイメージでの再起動と初回導入を分ける。 |
| 7 | WAN復旧時の永続バッファ、checkpoint、再送、監視 | 合成IDの欠損0、有害な二重適用0、異常隔離と容量警告。新配送を止めてもアプリとcoreは稼働し、キューは保持。 |
| 8 | break-glass、バックアップ、監査、切り戻し | 一般利用者は到達不能、緊急管理者のみ復旧・記録・失効できる。旧PoCの資源を変更しない。 |
| 9 | 評価プロファイル、予算、all-lab | 通常系と実験系を分け、同居時の予算を示す。RustFSなど置換型は専用プロジェクトで同一volumeへ接続しない。 |
| 10 | 隔離実機E2Eと移行ガイド | 拒否・許可・RBAC・バイパス否定・WAN断/復旧・バックアップ回復・旧PoC正常継続。維持できない移行手順は未承認。 |

## 6. 今回の証拠と未確認事項

- 確認済み: リポジトリのCompose宣言、GUI/Toolsのプロキシの責務、既存preflightの読み取り検査、WANに関係する公開コードと試験の範囲。既存のstorage/catalog先行履歴はmainに含まれる。将来storage、GUIポート、物理復元を変える候補では、既存hardeningとの重複と受入基準を再確認する。
- [preflight](../../scripts/preflight.py)はCompose正規化、ホストのmount・接続先・ネットワーク/port競合等の検査を行う。実機ゲートと、値を出さずに合成設定で走らせる静的計画検査の違いを維持する。 `--sudo`、ホスト情報、実設定ファイルが必要な検査は今回実行していない。
- 対象Dokployの版とPreview/実効Domains、トンネルの外向き通信・グループ同期、各アプリRBAC、正規ローカルDNS/認証、関係者と閾値、同居するポート・ネットワーク・永続資源、実際のWAN断と追いつきはいずれも未確認。実在の値を共有文書に保存せず、管理者が別の作業票で確認する。
- 次に進む条件: この資料の独立レビューと計画確定。構成変更、認証・ネットワーク設定変更、既存PoCの停止、デプロイ、実通信は別の明示段階でのみ判断する。

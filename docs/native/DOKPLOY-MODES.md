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

`zero-trust-access`、`data-egress` は設計上の名前で、実装がないため展開コマンドは失敗します。ローカル設備時系列は[本案件のWAN受入](../reviews/zero-trust-dokploy-wan-recovery.md)で必須です。汎用の `core-local` のデフォルトはInfluxDBなしです。下記の明示的ローカル候補は構成の比較までで、WAN断中の書込み・クエリは隔離試験を要します。

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

## WAN受入に向けたローカル時系列の候補

`core-local --local-influx` は既存のcoreにInfluxDB 3と専用の固定監視先だけを追加します。通常の `core-local`、Tools、既存デプロイは変えません。InfluxDBは既存の[Tools版](../../compose.tools.yaml)と同じビルド、起動設定、権限、コンテナ内の保管先、管理token形式を使います。全Tools overlayを参照するとn8nやOpenObserve等で使う鍵まで要求されるため、[ローカル専用overlay](../../compose.influx.local.yaml)は必要な定義だけを持ち、合成結果がTools版と一致するかを回帰検査します。これはTools、legacy Gold、評価MQTTとの併用や置き換えを認めるものではありません。

**Bash / Zsh共通**

```bash
python3 scripts/dokploy_plan.py core-local --local-influx
python3 scripts/dokploy_plan.py core-local --local-influx --cdc --alerting
python3 scripts/dokploy_capacity.py core-local --local-influx
```

- **隔離した名前と保管。** 専用サービス `influxdb-local`、volume `influx-local-data`、Compose secret `influx-local-admin` を使い、既存Toolsの名前を使いません。コンテナ内のマウント先は維持します。 `LOCAL_INFLUX_INSTANCE` は新規隔離環境の名前を明示します。Composeの `-p` や `COMPOSE_PROJECT_NAME`、Dokploy側の指定はoverlayの名前に優先し得ます。実機では有効project名が `foundation-r-influx-local-` から始まる承認済みの新規名で、旧PoC、旧volume、ネットワークやcontainerに重ならないことを確認できなければ使用しません。合成プランは追跡済みoverlayの名前を展開して照合するだけで、手動指定の誤りや実機衝突を防ぐガードではありません。
- **専用資格。** `LOCAL_INFLUX_TOKEN_FILE` は隔離した新規プロジェクト専用の、Git外・所有者限定の管理tokenファイルを明示します。旧ToolsやPoCのtokenへの既定値・フォールバックはありません。既存tokenを自動読取・発行したり、保管先を移動したりしません。管理tokenを一般設備や監視へ配りません。実機では書込・照会の最小権限と保持・バックアップ・復元を別途受け入れます。
- **到達境界。** この候補のInfluxDBは内部networkだけに接続し、ホストport・LAN公開・Traefik・Tunnelは設定しません。内部networkのみのコンテナでは、ホストloopbackへの公開指定がComposeで受理されても有効になるとは限りません（[Docker Engineの説明](https://github.com/moby/moby/discussions/53256)）。隔離runnerは対象コンテナの中から合成データの書込み・SQL照合だけを実行します。[Tools版のホスト経由](TOOLS.md#設備の合成測定点を確認する)は従来どおり別構成です。設備を接続する前に、承認済みの同一プロジェクト内の送信元、最小権限と実際の書込検証を用意します。
- **監視。** 専用内部networkに入るのはInfluxDBと既存health-relayのみ。新サービスの既存DNS名は専用networkのaliasに限り、監視の記録名は新サービスを区別します。既存の固定 `GET /influxdb` が無認証healthだけを調べ、Prometheusにはこの候補専用のfile_sd設定を追加します。DBへの管理token・問い合わせ・データ書込を中継せず、Grafanaの既存設定は変えません。実機で初めてhealthと、書込・SQL照合を別々に検証します。
- **CDCは別の入力。** `--cdc` は既存consumerを追加するだけで、SQL Server、connector、サイトKafka、topicを生成・発見・変更しません。隔離で確認したサイト内の接続、専用CA／資格、6表の対応、別のconsumer group・topic・checkpointと所有権が揃うまでは実行しません。評価用MQTT→Kafka→GreptimeDBを選び直さず、InfluxDBによる設備時系列とCDCをごちゃ混ぜにしません。 `--alerting` は既存の明示設定を継承し、WAN受入では外部通知の失敗をlocal healthの失敗にしません。

**受入は未完了。** 必要イメージが保持された隔離環境で、WAN断中の合成書込・SQL照合、再起動後の保持、サイト内CDC・ローカル監視と相互影響、ログとバックアップの保全、既存PoC無影響を実測します。[WAN設計](../reviews/zero-trust-dokploy-wan-recovery.md)にある必須条件は、静的なサービス存在だけでは満たしません。切り戻しはこの候補の追加を外し、隔離データと記録を保持します。既存環境の変更やvolume削除は行いません。

**隔離runnerの証拠。** 追加のGitHub-hosted PRジョブは、自分で作った固有プロジェクトと合成資格だけで、書込み・SQL応答、同じInfluxサービスの再起動前後の読取一致、固定リレーとPrometheus監視、ホストportがないことと後片付けを検査します。InfluxDB・監視サーバー類にはホストportを割り当てず、再起動の前に採った成功サンプルを再起動後の証拠に使いません。他の標準workflowは変えません。実行結果が出るまでは、この文書はその合格を主張しません。このジョブでも実際のWAN断、外部依存のないコールド起動、実環境のDokploy・CDC・機器・既存PoCは確認しません。

## サービス別の棚卸しと暫定容量

合成プランとは別に、 `dokploy_capacity.py` は、[開発用ロック](../../requirements-dev.lock)にあるPyYAMLを利用して追跡済みのCompose宣言を読み、同じ合成用Composeで有効になったサービスの上限だけを集計します。GitやVMから実際のEnvironment／実機容量を取りません。

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

## 実機の観測を使うオフライン判定

合成検査と実機の証拠は別です。 `dokploy_plan.py` はCompose合成、`dokploy_capacity.py` は合成構成の宣言上限・架空予算の比較、`dokploy_assessment.py` は運用者が別に確認した**集計値だけ**を判定します。後者は標準入力からJSONを1件読むだけで、端末・Docker・VM・Dokploy・他サービス・ネットワーク・秘密ファイルには接続しません。どちらの判定も起動成功、テナント認可や配備承認の証明ではありません。

**Bash / Zsh共通**

```bash
python3 scripts/dokploy_assessment.py < examples/dokploy/operator-assessment.synthetic.json
```

[合成スナップショット例](../../examples/dokploy/operator-assessment.synthetic.json)には架空の割当だけを記載し、実測・既存PoC調査が未完のため `UNKNOWN` になります。実データはアクセス制限された端末上でのみ別に保管し、会社固有の数値・識別子をソース、PR、ログや添付へ入れません。未知は `null`、未完は `complete: false` とし、未計測を0としません。集計を作れない場合はUNKNOWNのままにします。

| 判定 | 範囲・次の行動 |
| --- | --- |
| `PASS` | 合成側ならレシピ・静的比較が成功、実機集計側なら提示した集計が全項目を満たす。実際の起動や再起動、Preview、受入は未確認。 |
| `WARN` | 容量不足の確証はないが、計測上の余裕が小さい。閾値を超えた項目を別表示し、再計測や負荷分離を行う。 |
| `INSUFFICIENT` | 既知の比較だけで足りない、衝突が確定、または停止閾値に達した。下限の拡張・別レシピ・容量または割当の見直しを要する。起動失敗の実測ではない。 |
| `UNKNOWN` | 上限・予約・観測・衝突調査のどれかが不足。合格ではない。不足が既に確定している場合も未確認の詳細を残す。 |
| `NOT_IMPLEMENTED` | 人用Access、永続搬出、またはall-lab一括運転の対象レシピがない。比較目録の表示は可能。 |

実機集計コマンドの終了値はPASS 0、WARN 2、INSUFFICIENT 3、UNKNOWN 4、NOT_IMPLEMENTED 5、不正スナップショット1、不正な引数6です。合成側の既存終了値は維持し、JSONの `static_status` と `startup_verified: false` で意味を表示します。

### 集計値を作る条件

- **同じ対象・計測窓:** CPUはミリCPU、メモリ・ディスクはMiB。予約と実際の余裕で対象レシピ、データ保管先、既存PoCを揃えます。ディスクはVMの総割当でなく、対象写し・保存・再送を配置するファイルシステムの容量・空きです。複数ファイルシステムの空きを無条件に合計せず、対象ごとに別途評価します。CPU平均利用率は代表負荷中の持続窓、メモリはOSの利用可能量を使います。swapの確保・使用残量は単独で悪化を意味しないため、入出力カウンタの**同じ窓の差分ページ数**を使います。
- **同時に必要な既知分:** `workload.known_floor` は選んだ試算方針で同時に必要と確認した追加分のみ。静的な上限の合計を使うなら、非オーバーコミットで予約する保守的な基準であることを別に明示します。上限やディスク増分が不明なら `complete: false` です。OS／Docker／Dokploy／Traefik／既存PoC／余裕を重複なく別々に予約します。既存消費分を既に含む実測空きから、追加 workload の既知下限だけを引きます。予約と既存PoCは二重に引きません。現在と追加後の両方に同じRAM・ディスク閾値を適用し、空き不足または追加後のディスク停止閾値は `INSUFFICIENT` とします。追加後の残余は既知下限から求めた**上限**で、追加量が未確定なら閾値に達しない試算だけで合格としません。
- **下限と余裕:** 既知追加分＋既知予約分を最小の比較基準とし、RAMは容量の25%を空ける前提で `ceil(既知分 × 100 / (100 − 25))` を使います。20〜30%で調整でき、別の明示予約を加えるならその理由を記録します。出力は既知下限と不足下限を別に示し、未確認が残る限り最終所要量は不明です。
- **暫定警戒値:** CPU持続80%、ディスク70/85/95%（注意・重大・停止）、CPUとswapの最小観測15分が既定。急な増加やI/O競合から既存PoCを守るよう前倒しで警告します。 `thresholds` の許可キーを変更するときは代表負荷・容量・運用の基準を確認します。この値が実機で十分であることは未検証です。
- **既存PoCとの衝突:** 対象予定と既存占有の両方を、承認されたローカル手段で、project、container、network、volume、portごとに照合します。ポートはbind先・プロトコル・番号、network/volumeは論理名だけでなく実効名・外部共有も確認します。ここには件数と対象範囲の確認済みフラグのみを渡し、名前、アドレス、占有一覧、コマンド出力は入力しません。欠けた比較で衝突0とは扱わず、確定した衝突は既存サービスを変更せず別割当で解消します。

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

# 脅威と受入試験の対応表

<!-- doc-purpose:start -->
> **役割:** 障害・認証・データ保全上の脅威から合格証拠までを追跡する。
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

これは試験台帳であり、試験済み宣言ではない。追跡目的に、試験対象commit、構成の版、合成run ID、判定、取得時刻のみを記録する。秘密、アドレス、実payload、認証traceを公開ログ・CI artifactへ入れない。

| ID / 脅威 | テストと期待値 | 証拠／段階 |
| --- | --- | --- |
| T01 無認証や無関係グループで閲覧 | 無認証、別グループ、誤／期限切れaudienceを拒否し業務値は返らない。正規閲覧のみ通る。 | 既存Gold fixtureは一部。実人経路はPR4/10で認証担当と隔離受入 |
| T02 VM/別hostname/未管理port経由で認証を素通り | 合成Composeでhost portsとrouter許可集合を比較。正規入口以外から接続不可、TLS/routeが別パスに抜けない。 | PR3の静的／PR10の承認済みネットワーク否定試験。現行ネットは変更しない |
| T03 コンテナ間の境界を取り違える | Tunnelから意図したTraefik以外、DB/storage/internal APIを利用できない。Traefik対象を選択UIに限定。 | PR3/4で宣言と実効network両面。ネットワーク名の一致だけを証拠にしない |
| T04 Accessを通ると全権限になる | Viewerは編集、更新、管理・任意SQL不可。Operatorは担当scopeだけ。WebSocket、logout、許可されたcallbackも照合。 | PR5/10でアプリ別。正規ログイン成功だけでは合格しない |
| T05 webhookに人用認可や過剰権限を適用 | editor/callback/webhookの対象と認証を分け、無許可リクエストを拒否。 | PR5/10で各URL・同一origin・redirectも確認 |
| T06 外部依存がcoreを停止 | Run開始後、DNS、IdP、Cloudflare、R2を個別に不達にする。ローカルファイル・設備・SQL Server CDC/Kafka、Raw/Bronze・Silver/Gold・Iceberg、ローカル分析・時系列・Prometheus・ログ/バックアップを継続。人Gateと外部Goldの障害を個別のcloud-degradedとして評価。 | PR2–6で独立静的ゲート、PR6/10で実際の合成障害テスト。warm継続とoffline再起動を分ける |
| T07 ACK喪失で重複／取りこぼし | 断前・断中・復旧の順序で記録。相手確定直後、ローカルACK直前に停止して同一payloadを再試行。 | PR7でcount/ID集合差と結果hash、重複適用・欠損0を別々に測定。R2 CLIのcreate-onlyだけで合格しない |
| T08 retentionや容量を超えた障害 | 外部spool枯渇だけではオンプレ取込を止めない。物理空き下限は別障害として明示backpressureへ。警告、保持越えgap、最古とETA、隔離、復旧時にWAN/CPU/I/Oを制限。未送達データを確定・消去しない。 | PR7で件数・最古時刻・容量・gap区間・poison quarantine、閾値は承認後 |
| T09 資格や本番設定が漏れる | dummy Compose、値なし診断、Git・履歴・ログ・artifact検査。SQL/Storageの別権限、token回転と失効を確認。 | 全PRの独立レビュー。人とデータで資格を共有しない |
| T10 emergencyが恒久バイパスになる | 一般LANと利用者資格は失敗。承認された管理者の緊急経路だけ操作でき、利用後閉鎖・監査が残る。業務向けローカル経路は別判定。 | PR8/10の隔離復旧、バックアップから同一対象の復元と再発行しないことを照合 |
| T11 既存PoC停止・データ消失 | 別project・volume・bind・port・connector/topic・offsetで共存。旧正常系は試験前後に不変。 | 各PRの合成衝突検査、PR10で所有者合意の下に確認。削除や強制停止は対象外 |
| T12 評価モードで資源枯渇 | バリアント選択・排他、CPU/メモリ/ディスク上限、停止と復旧。限界不明は通常系へ導入しない。 | PR9で資源計画と隔離実測。上限未定義を0で合算しない |

[WAN設計](zero-trust-dokploy-wan-recovery.md)に合成の容量・復旧目標、閾値と観測方法を定義した。実機向けの採用値ではない。

実環境の試験は、変更の所有者・対象・条件・記録先と戻し方が事前に確定していることが前提。異常時は新規routeまたは配送だけを止め、オンプレ処理と既存PoCを止めない。検証のために会社ネットワーク、現行Composeや実データを変更しない。

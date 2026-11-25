# アクセス行列: 通常利用・ローカル業務継続・緊急管理

<!-- doc-purpose:start -->
> **役割:** アプリ別の認証、認可、ネットワーク、障害時の境界を定める。
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

**候補であり、現行の権限・IdPグループを表さない。** 閲覧、業務操作、配置と管理は別グループとし、割当先は承認済みの運用票で管理。実在のグループ名・アドレス・値をこの文書に置かない。MFAは人向けに要求し、操作・管理は短いセッションと管理端末条件を追加する。時間は組織承認まで未確定。Accessは入口認可であり、アプリ内認証・RBACやデータ制限は保持する。

| 対象 | WANありの候補 | WANなしの候補 | 残す認可・拒否 |
| --- | --- | --- | --- |
| Operations Portal / data-browser | 業務閲覧グループ、専用Access hostname | 許可済みローカル業務入口 | 有界な公開済み世代のみ。任意実行、内部書込経路なし |
| Grafana | 閲覧と運用を別グループとして認可 | ローカル監視。編集は担当者のみ | Viewer / Editor / Adminをアプリで強制、匿名無効 |
| Rill | 分析閲覧、採用決定まで原則評価環境 | ローカル許可対象・評価時のみ | 現在の例には組織RBACを保証する証拠なし。外部公開は保留 |
| marimo | 管理された編集者のprivateのみ | 承認した独立のNotebook環境のみ | コードが実行できるため閲覧ゲートと兼用しない。分離未確認なら非公開 |
| n8n editor / execution | private運用グループ | ローカル運用ユーザー、内部フローだけを継続 | n8n固有アカウント・workflow権限。編集者ブラウザーと機械webhook/callbackの認可は別 |
| n8n webhook | 必要な機械経路だけ個別合意 | ローカル機械入力は承認した宛先だけ | browser MFA policyを継承させず、専用認証・署名等を受入で確認。先に公開しない |
| OpenObserve | private運用グループ | ローカル監視・探索のみ | app RBAC、取り込み用資格と閲覧・管理を分離 |
| InfluxDB Explorer | private運用グループ | ローカル運用入口 | ExplorerのセッションとInfluxDB API tokenを分ける。DB APIはUIと一緒に公開しない |
| Lakekeeper UI | private、限定管理・閲覧 | 限定的な診断と承認済み管理のみ | 現状の専用閲覧境界を保つ。カタログREST、管理API、広い内部ネットを公開しない |
| Dokploy / SSH / PostgreSQL・SeaweedFS管理 / backup | 閲覧入口とは別の運用管理経路 | 事前承認した管理経路。緊急SSH break-glassは時限・個別承認・監査を適用。 | 一般利用者不可、操作記録と閉鎖・失効を確認。公開host portへの切替はしない |
| Prometheus、worker、内部read-api、S3/Kafka・MQTT | ルート公開しない。必要な設備契約は別 | 専用ネットワークの機械・内部監視だけ | networkもportも最小化。Traefik用共有ネットに直結しない |
| 承認済みGold連携 | 人向けとは別の機械認証／R2専用資格 | coreを継続。外部送信は隔離された配送状態に記録 | 人用cookie/グループの転用不可。許可する世代・列・行・宛先・保持の承認が必要 |

公開候補のDNS・routeを作る前に、対象名ごとにAccess policy・originの識別・同名衝突と既存の例外を承認する。パス共有や別hostnameへの迂回、unauth、別認証のrole昇格は[否定試験](zero-trust-dokploy-threat-test.md)の合格が必要。privateには対応するルート・DNS・端末経路が必要で、Accessアプリを作っただけでは到達できない。根拠は[Cloudflareのprivate application](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/non-http/self-hosted-private-app/)と[認可境界設計](https://developers.cloudflare.com/reference-architecture/architectures/identity-gated-edge/)。

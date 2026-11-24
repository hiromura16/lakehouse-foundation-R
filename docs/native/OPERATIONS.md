# 運用、バックアップ、受入試験

<!-- doc-purpose:start -->
> **役割:** 障害対応、状態の保全と合成データでの受入を行います。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 導入先で検証・運用するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [会社VMの受入手順と合格条件](<ACCEPTANCE.md>) / [公式リファレンスと採用バージョン](<REFERENCES.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## 毎日の確認

1. Dokployの対象Composeと **Docker → Containers** で状態を確認し、Grafanaで標準7件、閲覧構成を追加した場合は9件の監視先を確認します。
2. `catalog-migrate` の終了コード0と、Lakehouseのhealthyを確認します。
3. 取り込み後は件数とGold公開世代を控えます。 `JOB_FAILED` のときは Dokployの対象コンテナの **View Logs** で例外型・行番号を確認し、完了印、列、空き領域、監視状態を照合します。
4. 入力修正後に同じジョブを再実行し、アプリが示す公開世代が更新されたことを確認します。古い世代を読む利用者がいる間は、そのIceberg表のスナップショットを失効させないでください。

| 症状 | 確認すること |
| --- | --- |
| GitHub cloneが403 | 同じHTTPS URLで `git ls-remote` を試し、会社のproxy・許可リストを確認。DokployでGitHub Appを使用している場合は連携対象も確認 |
| pull/buildがTLSエラー | ホスト、Docker daemon、BuildKitのどの層で失敗したか。 [企業設定](COMPANY.md) |
| bind mountのpermission denied | landing作成者と環境設定のUID/GID、ディレクトリ階層の権限 |
| Grafanaが開けるがダッシュボードがない | provisioningのread権限とGrafanaログ。 [設定ファイル](MONITORING.md) |
| Goldが404 | `medallion` の成功、ファイル・SQL・CDCに合うAPIパス、指定世代の保持を確認。閲覧画面では「最新版を読み込む」で再確認 |
| CDCが停止 | broker接続、保有topic、stateの所有者、保持期限がcheckpointを超えたか |

CLIで運用する場合は、Dokployに表示される実際の `appName` を `docker compose -p` に指定し、配備と同じComposeファイルおよび非公開設定を指定します。

## バックアップと復元

保全対象はカタログDB、S3のSeaweedFSデータと設定、landing原本、private `site.env` と暗号鍵、Grafana volumeと設定ファイル、必要に応じてPrometheusデータ、CDC SQLiteのディレクトリ全体とその運用設定です。追加構成ではn8n専用DBと暗号鍵、OpenObserve、InfluxDB、Explorerのデータと資格情報も対象にします。DokployのVolume Backupsを使う場合、名前付きvolumeに加えてホストのlandingとCAを別途保全します。

1. 投入・変換・CDCを停止し、ジョブの完了を確認します。Dokployは対象のComposeだけをStopします。
2. カタログとオブジェクトの同じ停止時点を、ホストのストレージスナップショットまたは運用で承認されたバックアップ手段で保全します。秘密は暗号化された保管先へ保存します。
3. 別VM・別projectに同時点の両方とprivate設定を復元します。会社側の実接続を作る前に、隔離状態で同じ公開世代とGold件数、S3原本hash、Grafana設定を照合します。
4. 更新の試験で問題が起きた場合は、採用版のGit commitとバックアップを揃えて隔離環境で復旧を検証します。DBのschema移行を行った後にコンテナタグだけ戻すと、DBが旧版に対応しない場合があります。

### 停止時点をそろえる復旧リハーサル

採用したGitコミット、Composeの追加ファイル、プロファイル、Dokployの
`App Name`、イメージのダイジェストを、復元用の管理記録へ残します。
物理バックアップは元と同じPostgreSQLのメジャーバージョンとアーキテクチャ
の組み合わせに戻し、正常に読めることを確認してからバージョンを変更します。
[PostgreSQL 18のファイルシステムバックアップ](https://www.postgresql.org/docs/18/backup-file.html)

| 保全対象 | 復旧時に必要なもの |
| --- | --- |
| `catalog-db` と `storage` | 同一停止時点のPostgreSQL全体とSeaweedFSデータ。片方だけを別時点から戻すとカタログ参照と実体が食い違います。 |
| CDCの `state` と接続先の保持範囲 | SQLiteファイルのディレクトリ全体、確定offset、購読先のtopic・partitionごとの開始可能位置。`state` はCDC追加構成で保存します。 |
| `landing` とprivate設定 | 未処理原本・完了印・受付票、操作と読取の資格情報、追加機能の鍵・企業CA。アクセス制限と暗号化を適用します。 |
| 追加ツールの状態 | Grafanaの手作業で追加した設定、n8nの暗号化鍵と永続DB、時系列DB等。構成ファイルで再生成できるものを棚卸しして保全対象に決めます。 |

停止したコンテナのマウント名をDockerのラベルや `volume inspect` で
照合し、指定したボリュームだけを読取専用で採取します。PostgreSQLは
`pg_controldata` のクラスタ状態が `shut down` の時点を選びます。
S3互換ストレージ・カタログ・状態DBのアーカイブには個別のSHA-256を
記録します。アーカイブを接続のない別プロジェクトへ展開する際は、
書込みコンテナの起動前に展開先に既存のファイル・リンクがないことを
調べます。所有者と権限は保存元に合わせます。
[Docker公式のボリュームのバックアップと復元](https://docs.docker.com/engine/storage/volumes/#back-up-restore-or-migrate-data-volumes)

アプリ・カタログ・保存先を先に起動し、外部の投入、CDC、Cloudflareへの
接続は止めた状態で、対象表の公開世代、行数、原本ハッシュを照合します。
続けてSQLiteの `PRAGMA integrity_check` と確定offsetを照合します。
復元先が同じKafka consumer groupを使う場合、元と復元先を同時に接続すると
partitionの割り当てが変わるため、切り替え時に元を停止します。

#### 合成データの復元結果

2026-11-24に、独立した2つのComposeプロジェクトで、3領域の停止時点
バックアップと別ボリュームへの復元を行いました。公開世代の生成や
再取込を挟まず、復元した状態を読み取りました。

| 照合対象 | 復元前後の結果 |
| --- | --- |
| 合成ファイルの原本 | S3互換保存先から11件を再取得し、個別SHA-256と台帳が一致 |
| 標準ファイル経路 | 14行のハッシュ、公開世代 `8005438907150430096`、表スナップショット `2810786987315192321` が一致 |
| CDC | offset 0–8の9イベントのハッシュ、公開世代 `6265858029840293004`、SQLite確定offset 8が一致。整合性検査は `ok` |
| 失敗時の扱い | 復元先のSeaweedFSが作る空ディレクトリを認識し、既存のファイルまたはシンボリックリンクが見つかる場合に展開を中断 |

固定合成データに対する読み取り照合は [検証スクリプト](../../tests/integration/native/snapshot_evidence.py)
で再現できます。同じ入力で起動した両方の `lakehouse` コンテナへ
Python標準入力で流し、出力したJSONを比較します。外部Kafkaブローカーの
障害復旧、バックアップ間隔、復旧所要時間は導入先で測定してください。

## 受入の手順

- `python3 native/check_contracts.py` で業務SQLと契約のhashを照合します。
- Dokployの対象コンテナの **Terminal** から `python -m native.api verify` で固定合成データの11検査を行います。
- 専用の空の検証projectで `tests/integration/native/acceptance.py` を実行し、センサー、公開世代の固定、読み書きトークンの分離、Grafanaのプロビジョニングを確認します。このテストは指定landingへ合成ファイルを書き込みます。
- 会社の受入では、Git取得と認証、デーモンpull、BuildKitの依存取得、配置後の接続、実データと運用上限、バックアップからの復元をそれぞれ確認します。

監視設定の検査は [Grafanaの設定変更手順](MONITORING.md) にあります。契約やソース変更をレビューする場合は、 [品質検査規約](../../CONTRIBUTING.md) とCIの `Foundation R 標準構成の受入検査` および既存の `Validate public source` を確認します。

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

## 受入の手順

- `python3 native/check_contracts.py` で業務SQLと契約のhashを照合します。
- Dokployの対象コンテナの **Terminal** から `python -m native.api verify` で固定合成データの11検査を行います。
- 専用の空の検証projectで `tests/integration/native/acceptance.py` を実行し、センサー、公開世代の固定、読み書きトークンの分離、Grafanaのプロビジョニングを確認します。このテストは指定landingへ合成ファイルを書き込みます。
- 会社の受入では、Git取得と認証、デーモンpull、BuildKitの依存取得、配置後の接続、実データと運用上限、バックアップからの復元をそれぞれ確認します。

監視設定の検査は [Grafanaの設定変更手順](MONITORING.md) にあります。契約やソース変更をレビューする場合は、 [品質検査規約](../../CONTRIBUTING.md) とCIの `Foundation R 標準構成の受入検査` および既存の `Validate public source` を確認します。

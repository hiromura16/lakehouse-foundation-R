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
> **次に読む資料:** [公式リファレンスと採用バージョン](<REFERENCES.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## 毎日の確認

1. `docker compose --env-file "$site" ps -a` で状態を確認し、Grafanaに7つの監視先が表示されるか見ます。
2. `catalog-migrate` の終了コード0と、Lakehouseのhealthyを確認します。
3. 取り込み後は件数とGold公開世代を控えます。 `JOB_FAILED` のときは `docker compose --env-file "$site" logs --tail 80 lakehouse` で例外型・行番号を確認し、完了印、列、空き領域、監視状態を照合します。
4. 入力修正後に同じジョブを再実行し、アプリが示す公開世代が更新されたことを確認します。古い世代を読む利用者がいる間は、そのIceberg表のスナップショットを失効させないでください。

| 症状 | 確認すること |
| --- | --- |
| GitHub cloneが403 | GitHub Appまたは端末credentialに対象private repoの読取権限があるか |
| pull/buildがTLSエラー | ホスト、Docker daemon、BuildKitのどの層で失敗したか。 [企業設定](COMPANY.md) |
| bind mountのpermission denied | landing作成者と環境設定のUID/GID、ディレクトリ階層の権限 |
| Grafanaが開けるがダッシュボードがない | provisioningのread権限とGrafanaログ。 [設定ファイル](MONITORING.md) |
| Goldが404 | `medallion` の成功、ソース別のパス、指定releaseが保持されているか |
| CDCが停止 | broker接続、保有topic、stateの所有者、保持期限がcheckpointを超えたか |

## バックアップと復元

保全対象はカタログDB、S3のSeaweedFSデータと設定、landing原本、private `site.env` と暗号鍵、Grafana volumeと設定ファイル、必要に応じてPrometheusデータ、CDC SQLiteとその運用設定です。

1. 投入・変換・CDCを停止し、ジョブの完了を確認します。Dokployは対象のComposeだけをStopします。
2. カタログとオブジェクトの同じ停止時点を、ホストのストレージスナップショットまたは運用で承認されたバックアップ手段で保全します。秘密は暗号化された保管先へ保存します。
3. 別VM・別projectに同時点の両方とprivate設定を復元します。会社側の実接続を作る前に、隔離状態で同じ公開世代とGold件数、S3原本hash、Grafana設定を照合します。
4. 更新の試験で問題が起きた場合は、採用版のGit commitとバックアップを揃えて隔離環境で復旧を検証します。DBのschema移行を行った後にコンテナタグだけ戻すと、DBが旧版に対応しない場合があります。

## 受入の手順

- `python3 native/check_contracts.py` で元のSQLと契約のhashを照合します。
- `docker compose --env-file "$site" exec -T lakehouse python -m native.api verify` で固定合成データの11検査を行います。
- 専用の空の検証projectで `tests/integration/native/acceptance.py` を実行し、センサー、公開世代の固定、読み書きトークンの分離、Grafanaのプロビジョニングを確認します。このテストは指定landingへ合成ファイルを書き込みます。
- 会社の受入では、Git取得と認証、デーモンpull、BuildKitの依存取得、配置後の接続、実データと運用上限、バックアップからの復元をそれぞれ確認します。

監視設定の検査は [Grafanaの設定変更手順](MONITORING.md) にあります。契約やソース変更をレビューする場合は、 [品質検査規約](../../CONTRIBUTING.md) とCIの `Native Lakehouse acceptance` および既存の `Validate public source` を確認します。

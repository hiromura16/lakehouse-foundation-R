# Foundation Rの初回導入と操作

<!-- doc-purpose:start -->
> **役割:** Composeで起動し、合成データの結果を確認します。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 初めて起動するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [Dokployと企業ネットワーク](<COMPANY.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## 1. 用意するもの

Linux x86-64、Docker EngineとComposeプラグイン、Python 3.12以上、Gitを使います。Composeは `configs.content` とBuildKitを使うため2.23.1以上が必要です。初回は4 vCPU、8 GiB RAMと30 GiB以上の空き領域を目安に、合成データ専用VMで試してください。これは初期の配置目安であり、負荷試験の結果ではありません。

**Bash / Zsh共通**

```bash
git --version
python3 --version
docker version
docker compose version
```

Docker Engineに接続できない場合は、管理者に接続方法を確認します。Dockerデーモンへアクセスできるユーザーはホストで強い権限を持ちます。社内プロキシを使う場合は [会社ネットワークの準備](COMPANY.md) が先です。

## 2. Cloneして秘密設定を作る

**Bash / Zsh共通**

```bash
git clone https://github.com/hiromura16/lakehouse-foundation-R.git
cd lakehouse-foundation-R
python3 native/setup.py
site="$HOME/.config/lakehouse-foundation-r/site.env"
```

`site.env` は権限0600で作成されます。初回以外は既存ファイルを使います。既存のPostgreSQLとLakekeeperの鍵が異なる設定では、再接続や復元ができなくなります。landingは入力ファイルの受け渡し場所です。標準では同じ設定ディレクトリ内の `landing` を使い、コンテナは読取専用でマウントします。作業ユーザーと異なるUIDでファイルを生成する場合は、サイト管理者が読取権限を調整します。

## 3. 起動して確認する

**Bash / Zsh共通**

```bash
chmod -R a+rX native/monitoring
docker compose --env-file "$site" config --quiet
docker compose --env-file "$site" up -d --build --wait --wait-timeout 180
docker compose --env-file "$site" ps -a
docker compose --env-file "$site" exec -T lakehouse python -m native.api verify
```

`catalog-migrate` は終了コード0で完了する初期化ジョブです。ほかの7サービスが起動します。検証の期待値は `ok: true`、11件の検査、1回目の取込7ファイル、2回目の新規取込0ファイルです。通常データのGoldは次の操作で作ります。

## 4. 合成データを入れてGoldを読む

**Bash / Zsh共通**

```bash
landing="$HOME/.config/lakehouse-foundation-r/landing"
python3 scripts/generate-demo.py --output "$landing" \
  --seed 42 --orders 20 --days 7 --scenario edge
docker compose --env-file "$site" exec -T lakehouse python -m native.api medallion
docker compose --env-file "$site" exec -T lakehouse \
  python -m native.api gold factory_daily_production --limit 5
```

`imported` は今回保存した入力ファイル数です。再実行で `imported: 0` と `unchanged: true` が返れば、入力のスナップショットと変換版が一致しています。Goldの `snapshot_id` はその表の版、`release_snapshot_id` は公開世代の版です。画面からの確認は [監視](MONITORING.md)、別アプリからの取込は [入力経路](SOURCES.md) を参照します。

## 5. 停止と再開

**Bash / Zsh共通**

```bash
docker compose --env-file "$site" stop
docker compose --env-file "$site" up -d --wait --wait-timeout 180
```

同じプロジェクト名と同じ設定を使うと保存済みのデータを開きます。[障害時とバックアップの手順](OPERATIONS.md) を参照してください。

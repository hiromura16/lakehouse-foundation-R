# Foundation Rの初回導入と操作

<!-- doc-purpose:start -->
> **役割:** 会社VMにDokployで配備し、合成データから業務集計までを確認します。
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

会社VMへの配備はDokployで行います。Docker ComposeはDokployがGitHubから取得して実行する構成ファイルです。初回の端末操作は、会社VMの通信確認、非公開設定の生成、合成データの配置に使います。

## 1. 配備先を確認する

- Linux x86-64の配備先にDocker Engine、Compose 2.23.1以上、Git、Python 3.12以上を用意します。最初の検証用VMの初期目安は4 vCPU、8 GiB RAM、空き30 GiBです。
- Dokployの管理画面で、配備先のサーバーとプロジェクトを選べることを確認します。Docker Composeサービスを作成できる権限が必要です。
- 社内プロキシを使う場合は、VMのGit、DokployのGit取得、Docker EngineとBuildKitの経路を準備します。[プロキシ・CAの確認](COMPANY.md)

## 2. 配備先VMで非公開設定を作る

Dokployがコンテナを起動するVMにSSH接続し、ファイルの受け渡しを担当する一般ユーザーで実行します。ここでcloneするコピーは設定生成と合成入力作成に使います。Dokployは配備時に管理用のコピーを取得します。

**Bash / Zsh共通**

```bash
git clone https://github.com/hiromura16/lakehouse-foundation-R.git
cd lakehouse-foundation-R
python3 native/setup.py
site="$HOME/.config/lakehouse-foundation-r/site.env"
landing="$HOME/.config/lakehouse-foundation-r/landing"
```

`site.env` は権限0600で生成されます。会社VMで安全に開き、次の手順で同じDokployサービスのEnvironmentへ内容を登録します。再配備でも同じ設定を使います。landingは入力の受け渡し先で、設定を生成したユーザーが書き込みます。

## 3. Dokployでサービスを作成する

1. **Projects** から対象プロジェクトと環境を開き、**Create Service → Compose** を選びます。**Compose Type** は **Docker Compose** にします。
2. **General** でProviderを **Git** にし、下表の値を設定して **Save** します。
3. **Environment** を開いて `site.env` の内容を登録します。**Create Environment File**（`.env` を作成） を有効にして **Save** します。プロジェクトと環境の共通設定に同じ名前がある場合は、対象サービスの値を確認します。企業CAやビルド用のプロキシが必要な場合は、[企業ネットワークの設定](COMPANY.md) もここで登録します。
4. **General → Deploy** を選び、**Deployments** の最新ログを開きます。初回はイメージと依存のダウンロードに時間がかかります。
5. **Docker → Containers** で対象サーバーを選びます。対象Composeの `catalog-migrate` は終了コード0、内部の `read-api` と `health-relay` を含む常駐9サービスはhealthyが合格です。Grafanaは標準で7対象を表示します。監視設定はGitの設定ファイルからイメージに組み込まれます。

| Generalの項目 | 入力する値 |
| --- | --- |
| Provider | Git |
| Repository URL | `https://github.com/hiromura16/lakehouse-foundation-R.git` |
| Branch | `main` |
| Compose Path | `compose.yaml` |

[DokployのDocker Compose手順](https://docs.dokploy.com/docs/core/docker-compose) · [公開リポジトリのHTTPS設定](https://docs.dokploy.com/docs/core/providers)

## 4. Dokployのコンテナ端末で受入検査を行う

**Docker → Containers** で、新しく配備したComposeに属する `lakehouse` コンテナを選び、アクションから **Terminal** を開きます。権限がない場合はDokploy運用者にコンテナ端末の操作を依頼します。

**Bash / Zsh共通**

```bash
python -m native.api verify
```

合格時は `ok: true` と11項目、初回の取込7ファイル、再取込0ファイルが返ります。[コンテナ端末の公式手順](https://docs.dokploy.com/docs/core/docker-dashboard) · [異常時の切り分け](OPERATIONS.md)

## 5. 合成データで業務集計を読む

設定を作成したVM側のcloneで、合成データをlandingへ生成します。

**Bash / Zsh共通**

```bash
python3 scripts/generate-demo.py --output "$landing" \
  --seed 42 --orders 20 --days 7 --scenario edge
```

Dokployの同じ `lakehouse` コンテナ端末で次を実行します。

**Bash / Zsh共通**

```bash
python -m native.api medallion
python -m native.api gold factory_daily_production --limit 5
```

空のlandingでの期待値は `imported: 6`、`sql_statements: 14` です。日次生産14行のうち先頭5行を返します。同じ入力でもう一度実行し、新規0、`unchanged: true` と同じ公開世代を確認します。[会社VMの合格条件](ACCEPTANCE.md)

ブラウザーから見る場合は、まず [Grafana監視](MONITORING.md) へ接続します。[業務集計の画面](GUI.md) や [運用ポータル・n8nなど](TOOLS.md) を使う場合も、Dokployの同じComposeサービスに追加します。[Dokployの追加構成とアクセス方法](COMPANY.md)

## 6. 次回の配備

Dokployの同じComposeサービスで **Deploy** を実行します。停止や再開も同じサービスから操作します。設定生成は初回だけです。ログ、アプリの状態、公開世代を確認します。[運用・バックアップ](OPERATIONS.md)

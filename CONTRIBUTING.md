# 変更する人のための読みやすさと検証規約

<!-- doc-purpose:start -->
> **役割:** 読みやすいコード・コメント・CLI説明と、専用uv環境での品質検査規約を揃える。
>
> **対象読者:** 開発・設計担当
>
> **読むタイミング:** コードや文書を変更する前と、差分をレビューに出す前。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** 目的別一覧から必要な資料を選びます。
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<docs/DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## 変更する場所

| 変更内容 | 対象 |
| --- | --- |
| 入力の検査、Goldの公開、API | `native/` と `tests/integration/native/` |
| 業務ルール、列、集計の期待値 | `medallion/` と `contracts/native/` |
| 監視するサービス、グラフ | `native/monitoring/` と `docs/native/MONITORING.md` |
| 運用ポータル、ワークフロー、時系列 | `native/tools/` と `compose.tools.yaml` |
| 発表資料と手順 | `docs/presentation/` と `docs/documentation-manifest.json` |

[構成と採用理由](docs/native/ARCHITECTURE.md) で変更対象の責務を確認します。準備、取り込み、集計、公開、閲覧の役割ごとに入力と出力を定義します。エラーの処理は、原因を把握できる呼出元の境界で行います。

## コードとコメント

- 関数名と引数名には操作と対象を含めます。公開するPython関数のdocstringは、日本語で目的、必須の前提、返す値、重要な例外を書きます。
- コメントでは、再実行でどの状態を使うか、公開位置をいつ進めるか、権限をどこで絞るかなど、ソースだけでは判断しにくい設計理由を説明します。
- Pythonの依存は隔離環境に置きます。CLIは終了コードを返し、通常結果とエラーを分けます。シェルでは変数を引用し、対象が限定されたコマンドを使います。
- SQLは読み手が行の粒度、結合キー、NULLの意味を把握できる順序で組み立てます。更新順、欠損、遅延到着、結合による行数増加の期待値を用意します。
- 会社の接続先、秘密、CA、入力ファイル、書籍PDFはチェックアウト外で管理します。画像には合成データを使い、トークン、URLの秘密値、ユーザー名が写っていないことを確認します。

## 参考書籍とレビューの観点

| 対象 | レビューで確かめること | 参考書籍 |
| --- | --- | --- |
| Python | 名前、入力と返り値、失敗時の挙動を読み取れるか。依存を固定し、同じ条件で検査を繰り返せるか | [リーダブルコード](https://www.oreilly.co.jp/books/9784873115658/) · [ハイパーモダンPython](https://www.oreilly.co.jp/books/9784814400928/) |
| API | 読取とジョブ実行を用途別に認可できるか。不正な入力と想定外の失敗を検査し、ログへの資格情報の混入を防げるか | [マスタリングAPIアーキテクチャ](https://www.oreilly.co.jp/books/9784814400898/) · [責任あるソフトウェアエンジニアリング](https://www.oreilly.co.jp/books/9784814401581/) |
| シェル | コマンドの役割、引用、ファイル権限、終了コード、BashとZshの動作を確認できるか | [Efficient Linuxコマンドライン](https://www.oreilly.co.jp/books/9784814400485/) · [入門 モダンLinux](https://www.oreilly.co.jp/books/9784814400218/) · [マスタリングLinuxシェルスクリプト 第2版](https://www.oreilly.co.jp/books/9784814400119/) |
| SQL | CTEごとの粒度、結合の多重度、NULLの意味、集計の母数を読み取れるか。部分出荷、遅延、訂正を検査できるか | [SQLクックブック 第2版](https://www.oreilly.co.jp/books/9784873119779/) · [初めてのSQL 第3版](https://www.oreilly.co.jp/books/9784873119588/) |

[日次生産のGold SQL](medallion/03_gold.sql) は、指図・実績・停止を工場×日に集計してから結合します。変更時は [結合による数量増加の検査](medallion/checks/04-factory-daily-measures-avoid-join-multiplication.sql) と [部分・遅延出荷の検査](medallion/checks/05-sales-partial-and-late-deliveries.sql) で期待値を照合します。

## 変更を確認する

Python 3.12、uv、Docker Compose、ShellCheck、Gitleaks、Bash、Zshを用意します。単体・静的検査はユーザーのソース環境を変更しないよう専用の仮想環境で実行します。

**Bash / Zsh共通**

```bash
uv venv --python 3.12 .venv
uv pip sync --python .venv/bin/python --require-hashes requirements-dev.lock
export PATH="$PWD/.venv/bin:$PATH"
ruff check .
ruff format --check .
pycodestyle --max-line-length=79 native scripts tests infra
python scripts/check_ci_sources.py
python -m unittest discover -s tests
python native/check_contracts.py
python scripts/documentation.py --check
python scripts/shell_examples.py --check
python scripts/check-source.py --history
gitleaks git --redact=100 --no-banner --log-opts=HEAD .
gitleaks dir --redact=100 --no-banner .
```

業務SQLと公開APIは [合成データでの受入手順](docs/native/ACCEPTANCE.md) も実施します。入力契約を変更するときはサンプル、SQL、期待する出力、契約ハッシュを一緒に確認します。障害を修正したときは再現条件と確認結果を記録します。

## Renovateによる依存更新

このリポジトリにインストールした [Mend RenovateのGitHub App](https://github.com/apps/renovate) が、`main` の `renovate.json` を読み取って更新PRを作ります。対象はDocker Compose・Dockerfileの外部イメージ、GitHub Actions、`requirements-demo.txt` です。更新候補は公開日時が確認できてから3日以上経過したものを対象とします。SHAで固定済みの `actions/checkout` と `actions/setup-python` は、GitHub Releasesの公開日時を基準に、CIがすべて合格したパッチ更新PRをRenovateがsquashマージします。その他の更新PRはCIとレビューを経て手動でマージします。

GitHub Actionsの **Renovate** は、設定や検証ワークフローの変更時に、固定イメージに含まれる公式バリデーターを実行します。マージ後は **Actions → Renovate → Run workflow** から同じ検証を手動で実行できます。

初回の `Configure Renovate` PRは、既定ブランチに設定がない場合にAppが提案します。このリポジトリの設定が `main` に入った後は、その設定で初期化が進みます。依存更新候補はAppが作成するDependency Dashboardでも確認できます。 [Renovateの導入と初期化](https://docs.renovatebot.com/getting-started/installing-onboarding/)

ハッシュ付きのPythonロックと生成元、CDC Stage 3の検証済みイメージ台帳、バックアップの対応イメージ（PostgreSQL・Lakekeeper・SeaweedFS）は複数ファイルをまとめて変更するため、更新時に手動で整合を確認します。Pythonコンテナの自動提案は3.14系列です。 [Renovateの設定項目](https://docs.renovatebot.com/configuration-options/)

## 文書と図を更新する

Markdownを追加したら `docs/documentation-manifest.json` へ目的と対象読者を登録し、`python3 scripts/documentation.py` で索引を更新します。CLIの例には実行できるシェルを明示します。説明スライドは [HTMLの編集元](docs/presentation/index.html) を開き、各ページと印刷を確認します。製品の仕様は [公式リファレンス](docs/native/REFERENCES.md) を採用版と照合します。

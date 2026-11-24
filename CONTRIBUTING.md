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

## 文書と図を更新する

Markdownを追加したら `docs/documentation-manifest.json` へ目的と対象読者を登録し、`python3 scripts/documentation.py` で索引を更新します。CLIの例には実行できるシェルを明示します。説明スライドは [HTMLの編集元](docs/presentation/index.html) を開き、各ページと印刷を確認します。製品の仕様は [公式リファレンス](docs/native/REFERENCES.md) を採用版と照合します。

# 認証付きAPIとLangChain

<!-- doc-purpose:start -->
> **役割:** 固定API操作とスナップショットを根拠にしたLangChain利用を確認します。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 分析やAIのツールを実装するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [アーキテクチャと業務契約](<ARCHITECTURE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

標準ではホストのloopbackで `http://127.0.0.1:18502` を待ち受けます。操作トークンと読取トークンは別々に生成されます。

| メソッドとパス | 認証 / 意味 |
| --- | --- |
| GET /health、GET /metrics | ヘルスと集計カウンタ。業務データを返しません。 |
| POST /run | WORKER_TOKEN。 `{"action":"medallion"}`、`ingest`、`verify` またはSQLバッチを実行 |
| GET /v1/releases/latest | READER_TOKEN。標準ソースの最新公開世代とその表スナップショット |
| GET /v1/gold/{dataset} | READER_TOKEN。 `limit=1..200` と任意の `release=<ID>` |
| GET /v1/cdc/gold/{dataset}、GET /v1/sql/gold/{dataset} | READER_TOKEN。該当する任意ソースの公開済みGold |

Goldのデータセット名は `factory_daily_production`、`sales_fulfillment`、`sensor_latest`、`sensor_minute` です。センサーの2表は標準ファイル経路にあります。 `release_snapshot_id` と `snapshot_id` は64-bit IDを改変しないよう文字列で返します。金額などのDECIMALは文字列、日付時刻はISO形式です。 `total_rows` と `truncated` で参照件数の限界を確認できます。未公開は404、認証エラーは401、処理中の別操作は409です。

SQLバッチを反映するときのJSONは次の形です。抽出結果のhousekeeping情報から実値を設定します。

```json
{"action":"sql-load","batch_id":"<抽出結果のSHA-256>","observed_at":"2026-01-05T12:00:00.0000000Z"}
```

## LangChainから参照する

Python 3.12以上とuvで、付属の例を専用virtualenvに入れます。既存例と同じhash付き依存セットを使います。APIの読取トークンはツールの実行環境だけに渡します。

**Bash / Zsh共通**

```bash
uv venv --python 3.12 "$HOME/.cache/lakehouse-r-ai"
uv pip sync --python "$HOME/.cache/lakehouse-r-ai/bin/python" \
  --require-hashes examples/ai/requirements.lock
```

ここで入力するのは `READER_TOKEN` です。

**Bash**

```bash
read -rs LAKEHOUSE_READER_TOKEN
export LAKEHOUSE_READER_TOKEN
export LAKEHOUSE_API_URL=http://127.0.0.1:18502
export LANGSMITH_TRACING=false
"$HOME/.cache/lakehouse-r-ai/bin/python" \
  examples/ai/native_tools.py factory_daily_production --limit 5
unset LAKEHOUSE_READER_TOKEN
```

[Pythonサンプル](../../examples/ai/native_tools.py) の `lakehouse_gold` をLangChainのtoolsへ登録できます。最初の応答の `release_snapshot_id` を次の呼出しの `release` へ渡すと、異なる表を同じ公開世代で比較できます。結果の記述には表名、表スナップショット、公開世代を残し、切り詰められた結果から全体を推定しないようにします。 `LAKEHOUSE_API_URL` はloopback HTTPまたはHTTPS originです。モデル・トレーシング・社外送信の実行先は利用者が明示して構成します。

[LangChain公式 Tools](https://docs.langchain.com/oss/python/langchain/tools)

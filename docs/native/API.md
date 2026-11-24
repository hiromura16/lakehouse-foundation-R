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
| GET /openapi.json | OpenAPI 3.2.1の機械可読仕様。業務データを返しません。 |
| GET /health、GET /metrics | ヘルスと集計カウンタ。業務データを返しません。 |
| POST /run | WORKER_TOKEN。 `{"action":"medallion"}`、`ingest`、`verify` またはSQLバッチを実行 |
| GET /v1/ingestion/status | READER_TOKEN。直近のファイル取込と標準経路の公開中世代の件数 |
| GET /v1/releases/latest | READER_TOKEN。標準ソースの最新公開世代とその表スナップショット |
| GET /v1/gold/{dataset} | READER_TOKEN。 `limit=1..200` と任意の `release=<ID>` |
| GET /v1/cdc/gold/{dataset}、GET /v1/sql/gold/{dataset} | READER_TOKEN。該当する任意ソースの公開済みGold |

Goldのデータセット名は `factory_daily_production`、`sales_fulfillment`、`sensor_latest`、`sensor_minute` です。センサーの2表は標準ファイル経路にあります。 `release_snapshot_id` と `snapshot_id` は64-bit IDを改変しないよう文字列で返します。
公開一覧の `inputs` と `outputs` に含まれる表スナップショットIDも文字列です。
スナップショットがまだない表は `null` です。金額などのDECIMALは文字列、日付時刻はISO形式です。 `total_rows` と `truncated` で参照件数の限界を確認できます。未公開は404、認証エラーは401、処理中の別操作は409です。

### 固定した世代から必要な行を読む

Goldの各読取操作は、指定された制限の前にIcebergへ絞り込みを渡します。
`factory_id` は日次生産と設備、`equipment_id` は設備、
`sales_order_id` は受注・出荷の完全一致です。IDは大文字・小文字を区別し、
1〜128文字の表示可能な文字列です。

日付は `YYYY-MM-DD` 形式です。`date_from` を含み、`date_to` より前が
対象です。日次生産では生産日、受注・出荷では受注日、設備では計測・集計の
UTC日付を使います。絞り込んだ場合は `filters` を返し、`total_rows` は
その条件の行数、`truncated` は表示上限で切ったかどうかです。
複数の集計を同じ世代で読むときは `release` と必要な条件を併用します。

### 取込状態と公開中のデータ

取込状態の `last_attempt` は、APIプロセス上で最後に受理された
`ingest` または `medallion` です。起動直後は `null` です。
`imported_files` は当該試行でBronzeの完了台帳に追加したファイル数です。
`received_deliveries` は同じ試行で新しく保存した受付票を、原本のみ
（`raw`）、正規化済み（`normalized`）、形式不備（`rejected`）で数えます。
取込が途中で失敗し、受付票の集計を返すところまで進まなかった場合は
`received_deliveries` に `null` が返ります。

業務が確定して設備だけに失敗した場合、ジョブはHTTP 200と `sensor` を返し、
取込状態の `last_attempt.status` は `partial` になります。
`sensor.status` が `retained` の場合、設備の4表は前回の正常版です。
`unavailable` は設備の初回確定前を示します。設備Goldを世代指定で読むと、
その世代の `sensor` と、計測結果を作った変換の `recipe` を取得できます。
運用では設備の `built_at` とエラー種別を確認します。

公開マニフェストに保存される `published` の件数は直近の成功世代です。
取込試行が失敗しても、公開世代は最後の成功時点に維持されます。
未公開の場合は `published` 自体が `null` になり、件数を保存する前に
公開された世代は `gold_rows` と `quarantine_rows` が `null` になります。

[API定義JSON](../../native/openapi.json) · [OpenAPI 3.2.1公式仕様](https://spec.openapis.org/oas/v3.2.1.html)

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


## ローカルMCPホストから参照する

MCPを経由すると、対応クライアントから「どの世代が公開中か」
「指定世代の登録済みGoldにどんな行があるか」を問い合わせられます。
`lakehouse_latest_release` で取得した世代文字列を、後続の
`lakehouse_read_gold` の `release` に指定します。返された
`truncated` が真なら、取得した行の範囲で解釈してください。

Python 3.12とuvで、LangChainの環境と独立したMCP環境を作ります。

**Bash / Zsh共通**

```bash
uv venv --python 3.12 "$HOME/.cache/lakehouse-r-mcp"
uv pip sync --python "$HOME/.cache/lakehouse-r-mcp/bin/python" \
  --require-hashes --only-binary :all: examples/ai/requirements-mcp.lock
```

利用するMCPホストでローカル標準入出力のサーバーを追加します。
実行ファイルには上で作成した `bin/python` の絶対パス、引数には
[サーバースクリプト](../../examples/ai/mcp_server.py) の絶対パスを指定します。
MCPプロセスの環境に `LAKEHOUSE_READER_TOKEN` を秘密として渡します。
`LAKEHOUSE_API_URL` の初期値は `http://127.0.0.1:18502` です。
同じVM上から接続するか、別PCのホストで許可されたSSHトンネルを使い、
ローカルの接続ポートに合わせます。

MCPホスト自身が会話やツール結果をクラウドに送信する構成なら、
ホスト側の送信先と業務データの持出し許可を利用前に確認してください。
このアダプターは、ループバックHTTPを通じて登録済みの4表を各回最大200行
返します。書込操作・任意SQL・Raw・モデル呼出しは提供対象外です。

[Python MCP SDKのツール定義](https://py.sdk.modelcontextprotocol.io/servers/tools/) · [MCPのツール仕様](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)

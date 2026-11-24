# オンプレとCloudflareをつなぐ

<!-- doc-purpose:start -->
> **役割:** Cloudflare TunnelとAccessで承認済みGoldを読み取る経路を配備します。
>
> **対象読者:** 基盤構築担当 / 運用担当
>
> **読むタイミング:** オンプレの業務集計を拠点外の承認済みアプリへ提供するとき。
>
> **前提となる資料:** [Foundation Rの初回導入と操作](<START.md>)
>
> **次に読む資料:** [会社VMの受入手順と合格条件](<ACCEPTANCE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

工場内の入力、SQL処理、Icebergの保存はオンプレで実行します。拠点外からの読取はCloudflare Accessを通し、承認したGoldの固定世代・列だけを返す連携サービスを使います。管理画面、工場のDB、Kafka、SMBは工場側のネットワークと権限で運用します。

トンネルは既存の `edge-gold` へ接続します。連携サービスは専用の読取ネットワークでGold APIのゲートウェイを参照します。接続先で見える `lakehouse:8080` は読取GET専用で、書込ワーカー、ストレージ、カタログ、PostgreSQLは工場側のネットワークに分かれます。

```mermaid
flowchart LR
    subgraph Factory["工場オンプレ"]
        source["MES・ERP・設備"] --> lake["Foundation R・Iceberg"]
        lake --> edge["Gold読取ゲートウェイ"]
        connector["cloudflared"] --> edge
    end
    consumer["承認済み連携アプリ"] --> access["Cloudflare Access"]
    access --> connector
```

## 公開するデータを選ぶ

1. 通常のオンプレ配備でGoldを生成し、承認した表、公開世代、列、最大行数を記録します。
2. [ポリシーの例](../../native/edge/policy.example.json) を配備先VMの非公開領域へコピーします。例のチーム、AUD、公開世代と列を実際の承認内容に合わせます。
3. 読取トークン、ポリシー、下記のトンネルトークンを、配備先VMの運用者だけが読めるファイルへ格納します。

Goldの応答が行数上限を超えて切り詰められた場合や、指定世代が失効した場合、ゲートウェイはデータを返しません。公開経路は `GET /gold/<許可したデータセット名>` のみで、問い合わせ文字列は受け付けません。アプリは共有キャッシュを無効にするレスポンスを返し、同時に上流を読む処理を4件までに制限します。

## Accessアプリとトンネルを設定する

1. Cloudflare Oneで連携アプリ専用のHTTPSホスト名に対応するセルフホストAccessアプリを作ります。連携アプリ用の **Service Auth** ポリシーで、承認されたサービス資格情報に限定します。連携はその資格情報を使用します。
2. **Networking → Tunnels** で専用のリモート管理トンネルを作成し、トンネルトークンを取得します。運用者だけ読めるファイルに保存します。
3. トンネルの公開アプリケーション経路で専用ホスト名をサービス `http://edge-gold:8080` に関連づけます。**Protect with Access** を有効にし、チーム名と対象AccessアプリのAUDを設定します。ゲートウェイ側にも同じ値を登録します。
4. Cloudflareに到達する経路を配備先で確認します。Composeの既定はHTTP/2で、TCP 7844の外向き接続を使います。QUICを選ぶ場合はUDP 7844も必要です。ゲートウェイは署名鍵をチームのHTTPSエンドポイントから取得します。会社でHTTPSプロキシが必要な場合は `CF_EDGE_HTTPS_PROXY` を使い、TLS検査用のCAは [会社VMのビルド手順](COMPANY.md) に従います。

[セルフホストAccessアプリ](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/) · [サービス資格情報](https://developers.cloudflare.com/cloudflare-one/access-controls/service-credentials/service-tokens/) · [トンネルのOrigin設定](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/origin-parameters/) · [接続事前確認](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/troubleshoot-tunnels/connectivity-prechecks/)

## DokployのComposeに追加する

対象サービスのEnvironmentへ以下を追加します。値は配備先で管理している実際の絶対パスに置換してください。ファイルは `LANDING_UID` のユーザーが読み取れるよう配置し、他の通常ユーザーが読み取れないことを確認します。

```dotenv
CF_TUNNEL_TOKEN_FILE=/path/to/private/tunnel-token
CF_EDGE_POLICY_FILE=/path/to/private/policy.json
CF_EDGE_READER_FILE=/path/to/private/lakehouse-reader-token
CF_TUNNEL_PROTOCOL=http2
```

[DokployのAdvanced → Command](COMPANY.md) で、対象サービス名と配備時のComposeファイルは維持し、`-f compose.cloudflare.yaml` を追加します。配備先ではトンネルと読取ゲートウェイのホストポートを開ける操作が不要です。

`CF_EDGE_READER_FILE` の内容は同じサイトの `READER_TOKEN` の値1行です。ゲートウェイはこの値で内部APIへ問い合わせます。トンネル認証のトークン、Accessのサービス資格情報、基盤の読取トークンは別々に管理してください。[トンネルのtoken-file](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/run-parameters/)

## 受入と切り戻し

| 試験 | 合格条件 |
| --- | --- |
| 正しい認証と公開範囲 | 対象のサービス資格情報で許可表を取得し、承認世代・列・行数と一致する |
| 拒否 | 未認証、期限切れ・別AUD・改ざん・重複ヘッダー、未許可経路、書込メソッドで業務値が返らない |
| 障害 | トンネル停止時もオンプレの取込とGold生成が継続し、復旧後に承認した世代を読める |
| 切り戻し | 先にトンネル経路を無効にし、外から読めないことを確認する。対象サービスからだけ設定を外して再配備し、専用資格情報を無効化する |

リポジトリの受入試験では、隔離したComposeでIcebergの業務集計を生成し、独立した合成CAで証明したHTTPSの鍵配布先を参照します。有効な署名で固定世代の14行と許可3列が返ること、別AUD・期限切れ・書込・未許可経路が拒否されることを照合します。鍵配布先を停止してゲートウェイのキャッシュを空にすると認証に失敗します。この間もオンプレAPIが同じGoldを返し、再取込で公開世代が変わらず、鍵配布の復旧後に連携も再開できることを確認します。

この試験にCloudflareアカウントは不要です。実テナントでは拠点外からの経路、認証ポリシー、企業CA、WAN断と復旧、アクセスログの記録を合成データで確認してください。[AccessのJWT検証](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/validating-json/)

## 承認した成果物を非公開R2へ持ち出す

閲覧トンネルと成果物の送信は別の経路です。転送したい場合は、R2で公開アクセスを無効にした専用バケットを作り、そのバケットのObject Read & Writeに権限を絞った資格情報を発行します。再送時は既存物のバイト列を読み直すため、読取権限も使用します。このCLIは標準の `https://<ACCOUNT_ID>.r2.cloudflarestorage.com` に `auto` リージョンで接続します。EU、FedRAMP、USのjurisdiction付きバケットは、該当する専用エンドポイントが必要です。転送CLIはR2を使わない場合のオンプレ配備に含める必要がありません。[R2認証とエンドポイント](https://developers.cloudflare.com/r2/api/tokens/) · [boto3の設定](https://developers.cloudflare.com/r2/examples/aws/boto3/)

承認済みのCSV、JSON、Parquetと、その分類・公開世代・SHA-256を記したmanifestを同じ非公開フォルダに置きます。1ファイル16 MiB、全体64 MiB以下です。行数が途中で切られた応答や未承認の原本・隔離データを、成果物として承認しないでください。

承認担当者は、搬出内容・アカウント・バケットを照合したうえでmanifestを確定します。CLIは分類とファイルのサイズ・ハッシュを検査し、同名のJSONフィールドや整数以外の形式バージョンを拒否します。manifestの発行と保存は、担当者だけが変更できる権限で管理します。

```json
{
  "format_version": 1,
  "classification": "approved_external",
  "dataset": "factory_daily_production",
  "release": "123",
  "files": [
    {
      "name": "production.csv",
      "size_bytes": 26,
      "sha256": "承認時に計算した64桁のSHA-256"
    }
  ]
}
```

Python 3.14とuvを使い、まずネットワーク転送なしでファイルを照合します。

**Bash / Zsh共通**

```bash
uv venv --python 3.14 .venv-r2
uv pip sync --python .venv-r2/bin/python \
  --require-hashes native/edge/requirements-r2.lock
.venv-r2/bin/python -m native.edge.r2 /path/to/private/approved.json
```

承認した担当者がGit外の環境で `R2_ACCESS_KEY_ID` と `R2_SECRET_ACCESS_KEY` を設定します。短期資格情報の場合は `R2_SESSION_TOKEN` も設定します。承認先のアカウントIDとバケット名を明示して実行します。

**Bash / Zsh共通**

```bash
.venv-r2/bin/python -m native.edge.r2 /path/to/private/approved.json \
  --publish --account "$r2_account_id" --bucket "$r2_bucket"
```

成果物は `If-None-Match: *` で、キーが存在しない場合に作成します。最後にmanifestを確定するため、通信が切れると先に作成した成果物だけが残る場合があります。利用側はmanifestのある世代を開き、各オブジェクトのハッシュを照合します。復旧時は手元の同じ承認済みmanifestで再試行してください。既存キーのHTTP 412、またはバケットロックのHTTP 409 `ObjectLockedByBucketPolicy` を受けた場合は、既存物を読み、内容が一致すれば次へ進みます。[条件付き書き込み](https://developers.cloudflare.com/r2/api/s3/conditional-operations/) · [R2の整合性](https://developers.cloudflare.com/r2/reference/consistency/)

CIでは合成HTTPSに対し、固定版boto3の署名を検証しながら、承認不成立時の通信0件、条件付き作成、503からの再送、既存物の照合、誤ったCAの拒否を確認します。導入先では対象テナントの権限、企業CA、ネットワーク断からの回復、他アカウントからのアクセス拒否を合成ファイルで確認してください。S3互換APIと設置するテナントの条件は、公式仕様を参照してください。[S3 APIの対応範囲](https://developers.cloudflare.com/r2/api/s3/api/) · [データ所在地](https://developers.cloudflare.com/r2/reference/data-location/)

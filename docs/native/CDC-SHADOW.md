# 標準CDCのRawから隔離復元する

<!-- doc-purpose:start -->
> **役割:** 汎用CDCの確定済みRawを読み出し、元環境から切り離したSQLiteとIcebergへ再現する。
>
> **対象読者:** 基盤構築担当 / 運用担当
>
> **読むタイミング:** 固定したRawスナップショットからの復元を、稼働中の保存先に影響を与えず検証するとき。
>
> **前提となる資料:** [ファイル・SQL Server・Kafkaの入力契約](<SOURCES.md>)
>
> **次に読む資料:** [運用、バックアップ、受入試験](<OPERATIONS.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

標準の `native.cdc` は、派生表と完了位置の確定に先立って配送を `cdc_events.events` に保存する。この手順では承認したIcebergスナップショットと完了位置を使い、別のローカルSQLiteカタログ、Icebergウェアハウス、コンシューマーStateを再現する。稼働中の参照先を切り替える操作は含まない。

## Rawの互換性と復元範囲

標準CDCが追加保存する汎用Rawでは、既存の `key_json` と `envelope_json` の値を保ち、判定用の `key_is_null` と `value_is_null` を追加する。両列はIcebergのoptional Booleanで、古い表には初回の新規追記時に列を追加する。既存列のフィールドIDとParquetはそのまま使う。既存列を指定する読者は同じ値を読み続けられる。全列を取得する読者には列が増える。 [Icebergのスキーマ変更](https://iceberg.apache.org/spec/#schema-evolution) [PyIcebergのスキーマ変更API](https://py.iceberg.apache.org/api/#schema-evolution)

| 判定列 | 対応する既存列の値 | Kafkaでの意味 |
| --- | --- | --- |
| true | 空文字 | NULL |
| false | 空文字 | 空バイト列 |
| false | 非空文字 | UTF-8の原値 |
| NULL | 非空文字 | 旧RawのUTF-8の原値 |
| NULL | 空文字 | 旧Rawに区別の記録なし |

- 標準CDCが受理したJSONオブジェクトのキー・イベント本文、および判定列で確認できるトゥームストーンを再生する。キーのNULLと空バイト列を区別したトゥームストーンも維持する。SQLiteの配送位置は、稼働処理と同じ規則で反映する。
- 値のNULLはKafkaの無値トゥームストーン。空バイトの本文はUTF-8の空文字として記録し、業務JSONとは解釈しない。完了範囲に含まれた場合は拒否する。旧Rawの判定不能な空値にも復元時の推測は行わない。 [Debezium SQL Serverの削除とトゥームストーン](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html)
- 全購読トピックが各1パーティションの基準から始まり、完了した範囲が位置0から連続していること。空のトピックも、数を確認した上で承認に `-1` を記載する。
- 承認範囲の上限は1,000イベント。元Rawの走査は10,000行または64 MiB、搬送ファイルは64 MiBまで。圧縮データの展開、Arrow・Python処理のメモリ量は、実行環境の上限で管理する。
- 元の配信位置を再受信した際は既知のキー、本文、NULL判定が一致するか検査し、異なる内容は拒否する。古い行の判定値を再配送で補完することはない。搬送形式v2はNULLをJSONの `null`、空バイトを空文字として保存し、従来v1のJSON履歴も同じ承認・版照合条件で読める。

複数パーティション、コンパクション、トランザクション中断による履歴の穴、同名トピックや倉庫の使い回し、UTF-8として読めないバイト列は対象外。汎用Rawの表プロパティにはソース所有者が保持されない。運用で承認した表UUIDと履歴の出所、SQLiteの識別値、設定・対応表を確認する。

## 書き出し

1. 消費と公開を一時停止し、承認済みの公開世代、Iceberg Rawの表UUIDとスナップショット、SQLiteの `metadata.identity` と各トピックの処理済みoffsetを、独立した承認記録に採取する。Kafkaにコミットされたoffsetは次に読む位置で、SQLiteの処理済みoffsetより1大きい。
2. Kafkaの接続を含まないsourceファイルを準備する。既存のサイト設定から `topic_has_database` と `source` 内の `CDC_SQLSERVER_HOST`、`CDC_SQLSERVER_DATABASE`、`CDC_SQLSERVER_PORT`、`CDC_TOPIC_PREFIX` を抽出し、標準CDCの表対応JSONをそのまま使う。社内のホスト名や表名が含まれ得るので、限定されたファイル権限で保管する。
3. RawのIceberg RESTカタログに対し、対象のメタデータへのGETだけを許す資格情報・経路を使う。オブジェクトストレージは対象のみへのGET/HEAD権限にする。REST側の資格情報委任、リモート署名、サーバー側の走査計画を使わず、URIやストレージ設定が許可範囲に収まることを確認する。アプリ側の読取メソッド制限とストレージ・ネットワーク側の権限を併用する。 [PyIcebergのREST設定](https://py.iceberg.apache.org/configuration/)
4. 接続の作成は読み取り権限を管理するサイト用ランナーで行い、以下を呼び出す。入出力の親ディレクトリは信頼する利用者だけが書けるようにする。バンドルには業務データの原文が含まれるため暗号化保管する。

```python
from pathlib import Path

from native.cdc_shadow import export_bundle, source_from_files

source = source_from_files(Path("source-only.json"), Path("table-map.json"))
# catalogは手順3で作成し、approvalは手順1で承認した辞書を渡す。
# approvalのキーは source_sha256、raw_table_uuid、raw_snapshot_id、completed。
# completedは全購読トピック名と、各partition 0の最終処理済みoffsetの辞書。
digest = export_bundle(
    catalog.load_table, source, approval, Path("private-archive/cdc.bundle.json")
)
```

`digest` は生成したバンドル全体を照合するSHA-256。発行者の認証は別途署名・承認経路で行い、既知の値を復元担当者へ伝える。バンドルと同一の送付先から渡されたSHAだけで承認済みと判断しない。出力は新規ファイルのみ。接続・生データに由来する例外をそのまま外部ログに流さない。

## オフライン復元

稼働先へ接続できない作業環境で、対応するコード世代のチェックアウトに移動する。読み出し時と同じSQL・CDCコード・主要依存版が必要。Python 3.14とuvを用意する。初回の依存取得は社内の通常のパッケージ設定に従う。

**Bash**

```bash
uv venv --python 3.14 .venv-shadow
uv pip sync --python .venv-shadow/bin/python --require-hashes native/requirements-shadow.lock
.venv-shadow/bin/python -m native.cdc_shadow \
  --bundle "$cdc_bundle" \
  --approval-sha256 "$cdc_approved_sha256" \
  --source "$cdc_source_file" \
  --mapping "$cdc_mapping_file" \
  --output "$cdc_new_private_directory"
```

完成したディレクトリには `state.sqlite3`、`catalog.db`、`warehouse/`、`shadow-ready.json` ができる。ローカルPyIcebergカタログを開く際のカタログ名は `cdc-shadow`。`shadow-ready.json` はRaw、Bronze、Silver、Quarantine、GoldとStateの生成が成功した場合に作成される。復元先を照合するまでは運用の参照先に指定しない。

途中で失敗したディレクトリを新規成果物として扱わない。新しいディレクトリに再試行する。既存ディレクトリは上書きされない。元のブローカー、Catalog、オブジェクトストレージ、SQL Serverへのアクセスはオフライン復元には不要である。 [PyIcebergローカルカタログ](https://py.iceberg.apache.org/)

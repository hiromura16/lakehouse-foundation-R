# 標準CDCのRawから隔離復元する

<!-- doc-purpose:start -->
> **役割:** 汎用CDCの確定済みRawを読み出し、元環境から切り離したSQLiteとIcebergへ再現する。
>
> **対象読者:** 基盤構築担当 / 運用担当
>
> **読むタイミング:** 固定したRawスナップショットからの復元を、稼働中の保存先に影響を与えず検証するとき。
>
> **前提となる資料:** [ファイル・SQL・Kafka・設備の入力契約](<SOURCES.md>)
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
- 形式v1/v2の承認は、各購読トピックの区画0の最終処理済みoffsetを整数で指定する。複数区画の形式v3では、全購読トピックごとに、区画IDを表す文字列から最終処理済みoffsetへの辞書を指定する。実際の区画IDを0から連続してすべて列挙し、承認時点で未処理の区画は `-1` を指定する。
- どの形式でも、処理済みの各区画について位置0から承認位置までの履歴が連続していること。欠落や重複、承認外の区画は拒否する。形式v3の上限は1トピック16区画、全体64区画。
- 承認範囲の上限は1,000イベント。元Rawの走査は10,000行または64 MiB、搬送ファイルは64 MiBまで。圧縮データの展開、Arrow・Python処理のメモリ量は、実行環境の上限で管理する。
- 元の配信位置を再受信した際は既知のキー、本文、NULL判定が一致するか検査し、異なる内容は拒否する。古い行の判定値を再配送で補完することはない。搬送形式v2はNULLをJSONの `null`、空バイトを空文字として保存し、従来v1のJSON履歴も同じ承認・版照合条件で読める。

形式v3は、同じ対象表・業務キーが承認範囲内で複数区画へ現れたら拒否します。
キーが確定できるトゥームストーンもこの検査に含めます。キーがNULLまたは空の
ときは配送位置だけを保持します。空でないキーが表対応どおりのJSONでなければ
拒否します。区画ごとのoffset順を
保ち、SQLiteの既存規則で同じキーのSQL Server変更版をLSNとイベント通番から
選びます。区画間の到着順と、SQL Serverのトランザクション単位で一貫した切断点
をRawから復元する機能は含みません。

コンパクション、トランザクション中断などで承認範囲に穴がある履歴、同名
トピックや倉庫の使い回し、UTF-8として読めないバイト列は対象外です。空の区画を
Rawの行やSQLiteの完了位置だけから検出することはできません。運用で元の
Kafkaクラスタ・トピックの識別と区画一覧を別途確認します。汎用Rawの表
プロパティにはソース所有者が保持されないため、承認した表UUIDと履歴の出所、
SQLiteの識別値、設定・対応表を照合します。
[Kafkaの区画内の順序](https://kafka.apache.org/documentation/) · [SQL Server CDCのコミットLSN](https://learn.microsoft.com/sql/relational-databases/track-changes/about-change-data-capture-sql-server/)

## 実Kafkaから確定した履歴の照合

隔離したKafka互換ブローカーに、6表を1区画ずつ用意して合成イベント14件を配送しました。
製品の訂正、削除、トゥームストーン、コンシューマー停止後の再登録に加え、
計画・実績・停止・受注・出荷を含めています。SQLiteの最終完了位置と、
Kafkaにコミットされた次の位置を全6区画で確認してから、固定のIceberg Rawを採取しました。

同じRawを別の作業領域に再生し、公開したGold 2表と隔離6表について、型、件数、全行の
ハッシュを照合しました。日次生産は計画20・良品12・不良2・停止15分、受注出荷は
受注20・出荷7・未出荷13で一致しました。製品の型不正1行と、他5表の隔離0行も一致し、
再生後のSQLiteの全6区画が採取時の確定位置に戻りました。
[隔離受入ワークフロー](../../.github/workflows/cdc.yml)
· [照合プログラム](../../tests/integration/native/cdc_replay_parity.py)

## 書き出し

1. 消費と公開を一時停止し、承認済みの公開世代、Iceberg Rawの表UUIDとスナップショット、SQLiteの `metadata.identity`、Kafkaのクラスタ・各トピックの識別と全区画一覧、各区画の処理済みoffsetを、独立した承認記録に採取する。Kafkaにコミットされたoffsetは次に読む位置で、SQLiteの処理済みoffsetより1大きい。
2. Kafkaの接続を含まないsourceファイルを準備する。既存のサイト設定から `topic_has_database` と `source` 内の `CDC_SQLSERVER_HOST`、`CDC_SQLSERVER_DATABASE`、`CDC_SQLSERVER_PORT`、`CDC_TOPIC_PREFIX` を抽出し、標準CDCの表対応JSONをそのまま使う。社内のホスト名や表名が含まれ得るので、限定されたファイル権限で保管する。
3. RawのIceberg RESTカタログに対し、対象のメタデータへのGETだけを許す資格情報・経路を使う。オブジェクトストレージは対象のみへのGET/HEAD権限にする。REST側の資格情報委任、リモート署名、サーバー側の走査計画を使わず、URIやストレージ設定が許可範囲に収まることを確認する。アプリ側の読取メソッド制限とストレージ・ネットワーク側の権限を併用する。 [PyIcebergのREST設定](https://py.iceberg.apache.org/configuration/)
4. 接続の作成は読み取り権限を管理するサイト用ランナーで行い、以下を呼び出す。入出力の親ディレクトリは信頼する利用者だけが書けるようにする。バンドルには業務データの原文が含まれるため暗号化保管する。

```python
from pathlib import Path

from native.cdc_shadow import export_bundle, source_from_files

source = source_from_files(Path("source-only.json"), Path("table-map.json"))
# catalogは手順3で作成し、approvalは手順1で承認した辞書を渡す。
# approvalのキーは source_sha256、raw_table_uuid、raw_snapshot_id、completed。
# v1/v2のcompletedは全トピック名 -> 区画0の最終処理済みoffset。
# v3のcompletedは全トピック名 -> 区画ID -> 最終処理済みoffset。
# v3の1トピック分の例は {"0": 2, "1": 4}。空は -1 を指定する。
digest = export_bundle(
    catalog.load_table,
    source,
    approval,
    Path("private-archive/cdc.bundle.json"),
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

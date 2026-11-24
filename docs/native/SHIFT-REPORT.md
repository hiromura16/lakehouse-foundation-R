# 工場のシフト暦で公開結果を照合する

<!-- doc-purpose:start -->
> **役割:** 公開世代のSilverを工場の現地シフト暦と照合し、実績・停止報告の根拠を保存します。
>
> **対象読者:** データ利用担当 / 運用担当
>
> **読むタイミング:** 夜勤・日勤の集計を、同じ公開世代のまま再照合するとき。
>
> **前提となる資料:** [ファイル・SQL・Kafka・設備の入力契約](<SOURCES.md>)
>
> **次に読む資料:** [運用、バックアップ、受入試験](<OPERATIONS.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

同じIceberg公開世代を、工場の夜勤・日勤などの区切りで確認したいときに
`native.shift_report` を使います。既存の工場日次Goldを使う集計と併用できます。
日次Goldの実績日はUTC、シフト照合のシフト日は工場の現地開始日です。
夜勤の途中で現地の日付が変わっても、夜勤を同じシフトとして扱います。
[工場日次Goldの契約](../../contracts/native/data-products.json)

## 1. 工場のシフト暦を用意する

会社のアクセス管理対象ディレクトリに、工場ごとの暦をUTF-8のJSONで保存します。
`factory_id` にはSilverの製造指図に入る工場IDを指定します。
以下は工場 `F01` の合成例です。日時は工場のタイムゾーンで入力します。

```json
{
  "schema_version": 1,
  "factory_id": "F01",
  "timezone": "Asia/Tokyo",
  "shifts": [
    {
      "shift_id": "night",
      "start": "2026-01-05T22:00",
      "end": "2026-01-06T06:00"
    },
    {
      "shift_id": "day",
      "start": "2026-01-06T06:00",
      "end": "2026-01-06T14:00"
    }
  ]
}
```

- 夜勤の終了には翌日の日付を書きます。休日や作業を予定しない時間は区間を登録しません。同じ工場のシフト同士に重なりがあるとエラーになります。
- `timezone` は `Asia/Tokyo` や `America/New_York` などのIANAタイムゾーンです。夏時間終了時に同じ現地時刻が2回ある境界には、`start_fold` または `end_fold` に早い側の `0` か遅い側の `1` を指定します。夏時間開始時に存在しない時刻を指定するとエラーになります。
- 暦は1 MiBまでです。1回の照合期間は1〜31シフト日、対象シフトは1〜128件、1シフトは実時間で2日以内です。

[Python zoneinfo](https://docs.python.org/3/library/zoneinfo.html)

## 2. 公開世代を指定して照合する

Dokployで配備したサーバーで、対象のComposeを操作する管理者がリポジトリの
ルートから実行します。使用中の環境ファイル、暦のパス、取込経路、対象の
公開世代IDを先に確認します。暦は標準入力でコンテナに渡せます。
公開世代IDは、経路ごとのGold応答の `release_snapshot_id` で確認できます。
[経路ごとのGold参照API](API.md)

**Bash / Zsh共通**

```bash
docker compose --env-file "$site_env" exec -T lakehouse \
  python -m native.shift_report --calendar - --factory F01 \
  --from 2026-01-05 --until 2026-01-07 --channel files \
  --release "$release_snapshot_id" < "$shift_calendar_file"
```

`--from` は最初のシフト日、`--until` は対象外となる最初のシフト日です。
`--channel` は `files`、`sql`、`cdc` から選びます。
`--release` を省略すると、その経路で実行開始時に取得した最新の世代を使います。
暦を置ける同じ実行環境からは `--calendar /path/to/factory-calendar.json` も使えます。

## 3. 結果と根拠を確認する

| 出力 | 読み方 |
| --- | --- |
| `shift_date`, `start_utc`, `end_utc` | 工場の現地開始日と、確定したUTC開始・終了。開始を含み、終了時刻そのものは含めません。 |
| `observed_event_count`, `good_qty`, `reject_qty` | そのシフトで観測した実績と数量。観測0件の数量は `null` です。受理済みの実績に数量0がある場合は `0` を記録します。 |
| `observed_stop_count` | そのシフトに重なる停止報告数。日をまたぐ同一報告は、重なった各シフトで1件と数えます。停止時間0の報告は、発生瞬間を含むシフトで扱います。 |
| `reported_stop_microseconds` | シフト内に含まれる報告区間の延べ時間です。重なりは報告ごとに加算します。 |
| `any_reported_stop_microseconds` | 工場内で1件以上の停止報告があった時間です。重なった時間は1回だけ数えます。 |
| `unscheduled_in_window` | 選択シフトの最初の開始から最後の終了までに、どのシフトにも属さなかった実績数・0秒の停止報告数・停止報告の延べ時間です。 |
| `coverage` | `unknown` は入力の完全性を確認する情報がないことを示します。停止報告0件の時間は `null` です。 |

時間はマイクロ秒の整数で返します。たとえば
`reported_stop_microseconds=1800000000` は延べ30分です。
停止報告から読み取れるのは報告区間です。設備ID、設備ごとの計画時間、
速度基準が入力にないため、設備の停止時間、工場全体の停止時間、OEEは
この出力から計算できません。計画の `planned_date` は日付単位なので、
どのシフトに計画数量を配るかは工場側で別に定めます。

工場への帰属には指定した公開世代で採用した製造指図を使います。
後の世代で指図の工場を変えると、その世代では同じイベントでも
参照される工場が変わる場合があります。

再照合には、出力の経路・公開世代ID・Silver 3表のスナップショットID・
暦のSHA-256・UTC境界と、使用したイメージのdigestを控えます。
同じ暦ファイル・イメージ・保持済み世代を指定し、再度JSONを比較します。
選択した世代のSilver参照が欠落・失効したときは終了値 `2` です。
ストレージに接続できない場合や1表当たり100,000行・Arrow 64 MiBの
読取上限を超えた場合は終了値 `1` です。どちらの場合も標準出力の結果は空です。
実績と停止のSilverには工場IDがないため、指図と結合する前の
時間範囲に該当する明細にも、この読取上限を適用します。
取得した結果にも工場IDと製造数量が含まれるため、会社で承認された
保存先だけに保管します。
[Icebergの世代指定](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table.scan)

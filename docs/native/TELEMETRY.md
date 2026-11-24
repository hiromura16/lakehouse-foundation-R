# 工場テレメトリの接続と受入

<!-- doc-purpose:start -->
> **役割:** 設備のMQTTを集め、Kafka経由の再送とGreptimeDBの時系列検索を検証します。
>
> **対象読者:** 基盤構築担当 / 運用担当
>
> **読むタイミング:** 合成の設備テレメトリを隔離環境で検証するとき。
>
> **前提となる資料:** [Foundation Rの初回導入と操作](<START.md>)
>
> **次に読む資料:** [ファイル・SQL・Kafka・設備の入力契約](<SOURCES.md>) / [会社VMの受入手順と合格条件](<ACCEPTANCE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

工場設備・内製ゲートウェイが送信した計測値を、別の保存先で検索する
追加構成です。初回は実設備から切り離し、合成温度を使って接続から
再送まで確かめます。標準構成だけを使う場合は
[ファイルの入出力契約](SOURCES.md) に設備JSONLの手順があります。

## どこで何をするか

| 構成品 | 役割 | 採用理由 |
| --- | --- | --- |
| Eclipse Mosquitto（C） | 工場側のMQTTを受信。送信と収集を別アカウントに割り当て | 軽量なブローカーでQoS 1と永続購読を試せる |
| Telegraf（Go） | MQTTからLine Protocolを解析し、Kafka互換ブローカーへ転送 | 機器のプロトコルに対応する入力プラグインが揃う |
| Nisshi（Rust、隔離試験用） | Kafka互換の独立ブローカーでSQLiteに保存 | Apache-2.0。TelegrafとVectorの非トランザクション経路をJVMなしで再現できる。 [公式互換表](https://docs.nisshi.io/docs/compatibility) |
| Redpanda（C++、隔離試験用） | Kafka互換ストリームをディスクへ保存 | JVMを起動せずに、独立した合成トピックで転送を確かめられる。[配布条件](REFERENCES.md#追加機能の配布条件) |
| Vector（Rust） | 収集済み計測を読み、ディスクバッファを介して保存先へ転送 | Kafkaによる再取得とGreptimeDB gRPCの両方を扱える |
| GreptimeDB（Rust） | 同じデバイスの時系列を保存し、SQLで検索 | JVMを使わず時刻とタグで検索できる |

`MQTT → Telegraf → Kafka → Vector → GreptimeDB` の流れは
[追加Compose](../../compose.telemetry.yaml) に定義します。
MQTTとKafkaのポートは専用のComposeネットワーク内にあり、
試験用GreptimeDBのHTTPだけがホストのループバックに割り当てられます。
実機の接続設定、TLS、承認されたOTネットワークの境界は会社ごとに決めます。

## 最初に合成データで確認する

DockerとComposeが利用できるLinux VMで、チェックアウトの直下から実行します。
Docker操作をsudoで行う環境では `--sudo` を付けます。これは
`sudo -n docker` を呼ぶため、利用者の環境でDocker権限が必要です。
合成試験はランダムな名前でComposeプロジェクトを作り、終了時にその
検証用ボリュームを片付けます。初回には固定イメージの取得が必要です。

**Bash / Zsh共通**

```bash
python3 tests/integration/telemetry/acceptance.py
```

Linux amd64・Compose 2.24.4以降では、`--nisshi` を付けると同じ試験を
[Nisshiの設定](../../compose.telemetry.nisshi.yaml) で実行できます。
この試験ではMQTTの再起動に加えてKafkaブローカーも再起動し、
Telegraf・Vectorを経由した3点の値と時刻をGreptimeDBで照合します。

**Bash / Zsh共通**

```bash
python3 tests/integration/telemetry/acceptance.py --nisshi
```

単体の検証では5イベントを冪等送信し、保存した消費者位置2から再開、
seek、削除を示すNULL値を照合しました。Nisshi 0.8.2のSQLiteは開発試験用で、
Kafkaトランザクションを初期化する操作は拒否されます。実用時は
永続化方式、認証、周辺クライアント互換を別途確認します。
[公式GitHub](https://github.com/nisshi-io/solace-nisshi/releases/tag/v0.8.2)
· [ストレージとKafka API互換性](https://docs.nisshi.io/docs/compatibility)

| 検査 | 合格条件 |
| --- | --- |
| MQTT認証 | 匿名の送信は接続拒否。読取ユーザーの送信はMQTT 5のPUBACKで拒否 |
| 初回保存 | `sim01` の値22.5と計測時刻がGreptimeDBに保存される |
| 購読の再開 | Telegraf停止中に23.5と24.5を送信し、Mosquittoを正常再起動してから購読を再開。3点の時刻・値・デバイスを照合 |
| 遅着の保持 | 元の計測時刻をペイロードに保持し、Kafkaのレコードにつける時刻は現在時刻に設定 |

[試験プログラム](../../tests/integration/telemetry/acceptance.py) に、何を起動し
どのSQLとデータで判定するかを記載しています。永続環境で試験構成を
展開するときは3構成プロファイルを使い、起動時に5サービスを指定します。
必要な環境変数は `MQTT_PASSWORD_HASH_FILE` と
`MQTT_COLLECTOR_PASSWORD` です。Composeのファイルsecretはホストから
マウントされるので、Mosquittoの実行UID 1883が読める権限を付けます。

**Bash / Zsh共通**

```bash
docker compose --env-file /path/to/private/telemetry.env \
  -p foundation-r-telemetry-lab \
  -f compose.evaluation.yaml -f compose.telemetry.yaml \
  --profile stream --profile timeseries --profile telemetry \
  up -d broker greptime mosquitto telegraf vector
```

固定合成トピックは `factory/measurements/evaluation`、許可する送信者は
`simulator`、購読者は `collector` です。合成で使うLine Protocolは
`factory_temperature,device=sim01 celsius=22.5 1767614400000000000`
の形です。最後の整数はUTC秒から換算したナノ秒時刻です。
試験集計のSQLではGreptimeDBのミリ秒精度を照合します。

## OPC UAの署名・暗号化、読取権限を試す

別の試験モードで、Pythonのasyncua 2.1.0を使用した模擬設備だけを起動
します。Telegrafは固定したノードをX.509認証、
`Basic256Sha256` / `SignAndEncrypt` で購読し、Kafka以降はMQTT試験と
同じ構成を使います。模擬設備とKafkaにホスト公開ポートはありません。
証明書は試験用に生成し、試験終了後に削除します。
`--opcua --nisshi` でも同じ受入を行えます。

**Bash / Zsh共通**

```bash
python3 tests/integration/telemetry/acceptance.py --opcua
```

| 操作 | 観測した結果 |
| --- | --- |
| 接続先に別のRSA鍵の証明書を指定 | 暗号チャネルを確立できず、時系列表は生成されない |
| 正しい鍵と登録済みの読取ユーザー | 22.5を購読。認証済みユーザーの書込要求は `BadUserAccessDenied`、読取り値は前後で22.5 |
| 発生元時刻とサーバー時刻を10分ずらし、正常3点・通信異常1点を通知 | 発生元時刻で22.5、23.5、24.5の3点。通信異常点の99.5は保存しない |
| 隔離の境界 | 模擬設備と収集器の通信、Kafkaのワイヤー転送、GreptimeDBの時刻とタグを確認 |

実機の証明書の発行・失効、信頼チェーンや証明書の入替え、利用する
暗号ポリシーは設備担当者と決定します。この試験ではセッションの署名・
暗号化と別鍵の拒否を検証します。同じ鍵で再発行された証明書や
Discovery通信を含む信頼要件は設置先で別に照合します。
[設定の例](../../native/telemetry/opcua.toml) · [Telegrafの購読設定](https://docs.influxdata.com/telegraf/v1/input-plugins/opcua_listener/) · [asyncuaの固定版](https://github.com/FreeOpcUa/opcua-asyncio/tree/v2.1.0)

## Modbus TCPの読取、倍率、欠測を試す

Pymodbus 3.16.2の模擬PLCから、Telegrafが保持レジスタだけを読みます。
Composeは外部経路のない内部ネットワークを作り、PLCにもテスト結果にも
ホストのポートを割り当てません。合成設定のUnit IDは7です。

**Bash / Zsh共通**

```bash
python3 tests/integration/modbus/acceptance.py
```

| 対象 | 合成データで確認した結果 |
| --- | --- |
| 送信機能とアドレス | 収集器はPDU番地0〜5のFC03のみ。独立した試験クライアントのFC06・FC16と範囲外読取は例外2 |
| 16bit符号と倍率 | `0xFF85` は符号付き-123で、倍率0.1を適用すると-12.3 |
| 16bitのバイト順 | `BADC` で `0x3412` を `0x1234` と解釈し、4660 |
| 32bitのワード順 | `CDAB` で `0x5678, 0x1234` を `0x12345678` と解釈し、305419896。浮動小数では23.5 |
| 切断・復帰 | 合成判定の4秒で鮮度切れとし、値はnull。再開時の欠測コード-32768は無効で工学値を除き、正常復帰した24.3を新しい値として表示 |

メーカー資料のアドレス表記で40001が保持レジスタの先頭を示す場合でも、
この試験の設定はフレーム上の番地0です。実機では対応表に記載された機能、
Unit ID、番地、バイト順・ワード順、倍率を承認済み設定に落とします。
合成の鮮度判定は検証プログラムで実施しています。時系列DBへの配信や
死活通知と、実設備への接続は設置先で別に確認します。

[試験のTelegraf設定](../../tests/integration/modbus/telegraf.toml) · [Modbusの公式仕様](https://www.modbus.org/file/secure/modbusprotocolspecification.pdf) · [TelegrafのModbus入力](https://docs.influxdata.com/telegraf/v1/input-plugins/modbus/)

## Modbus RTUの仮想シリアルで読取を試す

Telegraf 1.40.3を、ホストの機器に接続しない専用コンテナで実行します。
仮想端末（PTY）を2対作り、実収集器が局番7の読み取りに使うフレームと、
独立した試験入力に対する合成装置の応答を確認します。Dockerのネットワークと
ホストのシリアルデバイスを使いません。

**Bash / Zsh共通**

```bash
python3 tests/integration/modbus/rtu/acceptance.py
```

| 操作 | 確認した結果 |
| --- | --- |
| 19200 baud、8データビット、パリティなし、停止2ビット | 局番7、FC03、番地0〜5。符号付き-123→-12.3、BADCの16bit順、CDABの32bit順と浮動小数23.5が一致 |
| CRC異常、無応答、欠測コード | TelegrafはCRC不一致を拒否。無応答中は新規点なし、-32768は欠測扱い |
| 仮想ポートの切断と交換 | TelegrafのPIDを維持したまま再接続し、復帰値24.3を取得 |
| 別の試験端末から書込み、範囲外、局番違い | FC06/FC16と範囲外を拒否。局番違いは無応答。6レジスタの値と収集器の読取専用動作を照合 |

8E1の設定でも読み取りは確認しました。LinuxのPTYは物理のパリティを
検証できないため、RS-485配線の方向・終端、パリティ、フレーム間隔は
承認された実機の受入で確認します。
[Modbusシリアル公式仕様](https://www.modbus.org/file/secure/modbusoverserial.pdf)
· [Telegrafのシリアル設定](https://docs.influxdata.com/telegraf/v1/input-plugins/modbus/)

## 検索用データと長期の原本を分ける

GreptimeDBは設備を選んだ時系列検索に使い、原本を別経路でIcebergに
保管する構成も試せます。RisingWaveの試験ではKafkaの区画・位置と
キー・本文のバイト列をIceberg V2へ保存し、再起動後に照合します。
標準で設備の確定済みJSONLを受け取る場合は、Icebergの長期履歴、
最新値と1分集計を [入力契約](SOURCES.md) に沿って生成します。
[Kafka設備原本のIceberg受入](EVALUATION.md#kafkaの設備原本をicebergへ保管する)

## MQTTの連続送信・停止復帰を測る

使い捨てCompose環境で40台の設備を模擬し、250点ずつMQTTで送ります。
保存先で各区間の件数を確認してから次へ進みます。Vectorを停止した間の500点を
Kafkaにため、再開後にGreptimeDBで照合します。最後に50点を再送し、直後の
初出1点の確定を待ってから、全行の時刻・設備ID・測定値を比較します。
Mosquittoの停止中に発生した計測を守る試験は、この容量計測と別に実行します。

**Bash / Zsh共通**

```bash
python3 tests/integration/telemetry/acceptance.py --capacity-rows 10000
python3 tests/integration/telemetry/acceptance.py --nisshi --capacity-rows 10000
```

| Kafka互換ブローカー | 平常時9,500点の送信と確認 | 500点の転送再開から確定まで | 全値照合 |
| --- | ---: | ---: | ---: |
| Redpanda | 1,025.9秒 | 50.0秒 | 10,001件一致 |
| Nisshi | 1,015.9秒 | 49.9秒 | 10,001件一致 |

Bakuの隔離Composeで、両方とも各1回を計測しました。確認の待ち時間も含む
結果です。上限性能や本番の保持容量を決める場合は、実際の発行間隔、設備数、
ディスク、再送時間を使って別に計測します。
[GitHub Actions](../../.github/workflows/telemetry.yml) は短い1,000点で同じ
停止復帰と全値照合を実行します。OPC UAの失敗時は合成機器の書込段階と
GreptimeDBの最終行を記録し、Nisshi試験では保存位置とVectorの確定位置を
比較できます。

## 設備導入で決める条件

- 送信者と発行トピックを紐づけ、工場内のTLS証明書または認証基盤、
  MQTTとKafkaのACL、シークレット保管を設定します。実データの原本は
  計測時刻、品質コード、設備ID、配信IDやシーケンス番号ごと別経路で保全します。
- 試験設定ではパケット64KiB、購読キュー1,000件または8MiB、
  Vectorのディスクバッファ約256MiBです。停止時間と発行速度から容量を決定し、
  欠損を送信側・Kafka・保存先の連番や件数で検知します。
- MQTTのACKは保存先の全件保証に使えません。Telegrafの変換不能な入力や
  Kafkaサイズ違反で欠損が起こり得ます。ブローカーの永続化は10秒間隔で、
  合成試験の再開は正常再起動が対象です。停電、ディスク満杯、順序の入れ替え、
  実機の再接続について設置先で受入試験を行います。
- OPC UAでは接続先の証明書と送信元時刻・品質を照合します。Modbusでは
  承認済みの読取レジスタ、アドレス基準、符号、バイト順、倍率を照合します。
  Rustの `async-opcua` と `tokio-modbus` は専用収集アプリを書くときの
  候補です。既存のTelegrafを使う場合は互換プラグインを設備別に評価します。

[MQTTのMosquitto設定](https://mosquitto.org/man/mosquitto-conf-5.html) · [TelegrafのMQTT入力](https://docs.influxdata.com/telegraf/v1/input-plugins/mqtt_consumer/) · [Kafka出力の採用版](https://github.com/influxdata/telegraf/blob/v1.40.3/plugins/outputs/kafka/kafka.go) · [VectorのGreptimeDB出力](https://vector.dev/docs/reference/configuration/sinks/greptimedb_metrics/) · [GreptimeDB](https://docs.greptime.com/getting-started/installation/greptimedb-standalone/)

[TelegrafのOPC UA購読](https://docs.influxdata.com/telegraf/v1/input-plugins/opcua_listener/) · [TelegrafのModbus](https://docs.influxdata.com/telegraf/v1/input-plugins/modbus/) · [async-opcua](https://github.com/FreeOpcUa/async-opcua) · [tokio-modbus](https://github.com/slowtec/tokio-modbus)

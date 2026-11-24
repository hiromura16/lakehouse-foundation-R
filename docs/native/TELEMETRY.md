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
> **次に読む資料:** [ファイル・SQL Server・Kafkaの入力契約](<SOURCES.md>) / [会社VMの受入手順と合格条件](<ACCEPTANCE.md>)
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
| Redpanda（C++） | Kafka互換ストリームをディスクへ保存 | JVMを起動せずに、独立した合成トピックで転送を確かめられる |
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

## 設備導入で決める条件

- 送信者と発行トピックを紐づけ、工場内のTLS証明書または認証基盤、
  MQTTとKafkaのACL、シークレット保管を設定します。実データの原本は
  計測時刻、品質コード、設備ID、配信IDやシーケンス番号ごと別経路で保全します。
- 試験設定ではペイロード64KiB、購読キュー1,000件または8MiB、
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

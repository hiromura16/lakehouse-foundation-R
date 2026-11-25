# 内包CDCの権限境界と否定試験

<!-- doc-purpose:start -->
> **役割:** SQLとKafkaを主体別に許可し、異なる権限と入力を否定試験で検証する。
>
> **対象読者:** 基盤構築担当 / 開発・設計担当 / 運用担当
>
> **読むタイミング:** 権限や公開境界を設計・レビューするとき。
>
> **前提となる資料:** [Foundation R内包CDCの現状と設計差分](<cdc-self-contained-design.md>)
>
> **次に読む資料:** [内包CDCの受入と配布物の復旧](<cdc-self-contained-acceptance.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

本書は合成環境で権限を検証するための設計です。実サイトの接続先、アカウント、権限設定を公開する場所ではありません。

## 主体別の許可

| 主体 | 許可すること | 拒否の証拠 |
| --- | --- | --- |
| SQL事前準備担当（DBA） | DB・Edition・Agent・既存CDCと共有影響を確認。DB CDCの初回有効化はsysadminが別承認で実施。専用表のCDC設定はDBAがdb_owner相当で実施 | reader/writerから事前準備権限は使えず、既に有効なDB CDC、共有capture/cleanupジョブ、DBオプションは変更されない |
| 合成fixture writer | 許可6表の必要なSELECT/INSERT/UPDATE/DELETEと、明記した制御オブジェクトだけ | 別表の読取・変更、DDL、CDC設定、reader向け権限の付替が失敗し、状態が不変 |
| Connector reader | 捕捉列のSELECTと専用gating roleから始め、起動・snapshot・streamingで必要と確認した対象関数/メタデータのみ追加候補とする | 別表・別captureの読取、書込、DDL、管理権限の取得が失敗。権限不足なら停止し、広域権限で再試行しない |
| Broker管理主体 | 新しい専用クラスタのtopic・ACL・所有契約の作成と検査 | Connect/Consumerでは管理操作・他AppのtopicやACL更新が失敗 |
| Connect worker/producer | 専用worker group、internal/historyと許可topicに各必要操作のみ | 別環境のtopic、設定、groupに対する読取・上書き・登録が失敗 |
| Core Consumer | 許可した変更topicの読取と自グループのcommit、Core内の確定処理 | 他のgroupへの参加・commit、他topicへの到達、topic作成・書込が失敗 |
| 登録確認主体 | 専用管理面で、未登録の正規設定の登録と、既登録設定の機密化された比較 | 設定相違で上書き・削除・offset resetせず、安全な理由だけを返す |

SQL Serverの関数による読み取りで必要な捕捉列許可とgating roleは、[Microsoftの権限説明](https://learn.microsoft.com/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql)を基準にします。ただし[Debezium 3.7](https://debezium.io/documentation/reference/3.7/connectors/sqlserver.html)の `data.query.mode` は既定がdirect（変更表の直接参照）です。functionとdirectの権限は同一と仮定せず、採用版を限定readerで実測します。Agent稼働確認も版に応じた限定した真偽値取得を候補とし、インスタンス全体の権限やCDCスキーマ全体の許可を既定で使いません。

## 境界を破る入力で確かめる

| リスク | 否定試験と合格条件 |
| --- | --- |
| 誤配送・権限過剰 | 合成の別DB・別スキーマ・別表・別groupを読み書きしようとして拒否される。別入力はRaw・Silver・Goldに入らず、既存状態が不変。 |
| 名前が似た別物を再利用 | 識別名は同じでも、所有、capture、topicの世代、partition、契約hashのうち一つを変える。既存オフセット・状態DBを改変せず停止する。 |
| 読み取れないJSONの黙殺 | 不正JSONの受信原本を隔離したローカル領域に保全し、その位置をACKせず、同じpartitionで後続の位置を飛び越えてcommitしない。原本は公開Rawの正常イベントやログに混ぜない。 |
| 型付き異常を正常確定 | 別表を装ったイベント、型違いキー、before/afterの不整合、未承認の列・DDLを拒否する。契約で明示承認した型付き異常だけは所定のローカル隔離に確定し、隔離件数と拒否件数を区別。既に合格したイベントのACK・確定位置を巻き戻さない。 |
| 越境公開・TLS誤設定 | 許可外のネットワークからBroker・REST・SQLに到達できず、未知のCAでは接続失敗。検証省略に切り替えず、運用者へ秘匿された状態を返す。 |
| 同一DBを使う別系統への影響 | 合成共有DBで別系統の権限・所有・接続先を前後比較し不変。ログ量、capture/cleanupの遅れ、ロックを比較。実サイト側は承認済み観測のみで確認し、別系統の実データは取得しない。 |
| 診断による漏えい | 故意に失敗させても出力に設定本文、実LSN、接続詳細、秘密参照の実値が出ない。証拠として残すのは合成の識別子、件数、状態と安全な差分分類のみ。 |

公開Git、PR、CIログ、添付、スクリーンショットには架空名・合成データ・公開一次資料だけを置きます。非公開の仕様や構成図、業務識別子、実URLやホスト、アドレス、メール、Secret・証明書の内容、ログやDBの実データを持ち込みません。未確定値は「未確認」とし、私的な情報を置き換えるために同じ値を符号化・ハッシュ化して公開しません。実環境は担当者が承認済み保管先から実行時参照だけで設定し、公開試験とは別に扱います。

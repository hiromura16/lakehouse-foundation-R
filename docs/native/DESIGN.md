# レイクハウスの設計思想

<!-- doc-purpose:start -->
> **役割:** データの保存、品質、所有、AI利用の考え方をFoundation Rの実装と照らし合わせます。
>
> **対象読者:** 初めて使う人 / 基盤構築担当
>
> **読むタイミング:** 設計判断を理解するときや、用途に合わせて構成を変更するとき。
>
> **前提となる資料:** なし。必要な用語は本文で確認します。
>
> **次に読む資料:** [アーキテクチャと業務契約](<ARCHITECTURE.md>)
>
> **文書一覧:** [目的・読者から探す文書マニフェスト](<../DOCUMENTATION-MANIFEST.md>)
<!-- doc-purpose:end -->

## データレイクとウェアハウスから受け継いだもの

データレイクはCSV、JSONL、画像などをそのまま保管しやすく、将来の使い方が決まっていない原本も残せます。データウェアハウスは型、粒度、指標を定義した表を公開し、継続的に同じ意味で分析できるようにします。レイクハウスはオブジェクトストレージ上のファイルに表のメタデータとトランザクションを重ね、この2つの用途をつなぎます。Foundation Rでは同じ受注データから原本の再確認、異常行の調査、Goldの出荷率の確認ができます。[CIDRのレイクハウス原論文](https://15721.courses.cs.cmu.edu/spring2024/papers/01-modern/armbrust-cidr21.pdf)

## 設計で答える問い

| 問い | 考え方 | Foundation Rでの扱い |
| --- | --- | --- |
| データはどこに置き、どこで計算するか | ストレージと計算の分離。オブジェクトストレージを永続層に置き、読み書きは開いた仕様でつなぐ | SeaweedFSに原本とParquetを保管し、Iceberg RESTで表を見つけ、DuckDBを処理時に起動する。別エンジンを使う前に、表の形式・機能とカタログへの接続を検証する |
| どの行を分析に使えるか | メダリオン。原本から、取り込んだ行、検査済みの行、用途別の集計へ品質と意味を積み上げる | 原本はRaw。Bronzeで入力を取り込み、Silverで型とキーを検査する。代表エラーをQuarantineに残し、Goldに日次生産や出荷状況を公開する |
| 複数の表を同じ時点で読めるか | スナップショットと公開世代。表の版と利用者に公開する出力の組を記録する | Icebergのcommitは表単位。ワーカーが出力を保存した後に `gold._release` へ世代を登録し、APIはその世代に記録された表の版を読む |
| 数字の意味をどこで決めるか | DDDと分析の粒度。業務用語の意味を文脈ごとに定義し、指標の集計単位を先に決める | MESの指図・実績・停止をそのまま結ぶと行が増えるため、それぞれ工場×UTC日へ集計してから結合する。ERPの出荷は受注キーへ集計する |
| 誰が品質と利用条件を持つか | データメッシュ。ドメインがデータ製品を持ち、共通基盤のセルフサービスと連邦型ガバナンスで相互利用を支える | データ契約、出力の説明、読取APIをデータ製品の土台にする。ドメイン別の所有者、SLO、アクセス承認と連邦型ガバナンスは導入する組織で決める |
| 変更をどう取り込み、どうやり直すか | バッチとCDC。ファイルはまとまりで受け取り、DBの変更はイベントの順序と削除の意味を保存する | バッチは完了印とSHA-256で照合する。任意CDCはKafkaの配送記録とDBの変更順を区別する。どちらの経路も業務契約を通ってGoldへ到達する |
| 小さな部品をどうつなぐか | UNIXの責務分離。各部品に狭い役割を持たせ、明示したインターフェースで組み合わせる | 投入アプリ→ファイルと完了印、ワーカー→型と変換、API→利用者への公開、Prometheus→測定という境界を設ける |
| AIにどこまで任せるか | AIワークフローとエージェンティックメッシュ。ツールの権限、入力制約、根拠、実行の追跡性を定義する | LangChainに定義済みのGold読取ツールを提供する。AIは公開世代と表のスナップショットを参照できる。エージェント間の協調基盤は導入範囲に含まれない |

メダリオンはデータ品質の段階を、DDDは業務用語の境界を、データメッシュは所有と運用の分担を決めるときに使えます。同じ製造データに3つの考え方を適用できます。メダリオンの製品側の構成例はMicrosoftの図で確認できます。[メダリオン設計](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion/) · [Microsoft Fabricの公式構成図](https://learn.microsoft.com/en-us/azure/architecture/databases/architecture/dataops-mdw) · [データメッシュの原則](https://martinfowler.com/articles/data-mesh-principles.html) · [Kimballのディメンショナルモデリング](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)

## テーブル形式の選択

| 形式 | 設計上の特徴 | Foundation Rでの判断 |
| --- | --- | --- |
| Apache Iceberg | スナップショットとマニフェストで表の構成ファイルを管理する | 元のFoundationの表形式を維持でき、PythonのPyIcebergとLakekeeperのRESTを使える。標準構成で新規作成する表は形式v2を指定する。[Iceberg仕様](https://iceberg.apache.org/spec/) |
| Delta Lake | トランザクションログで表を管理する。バッチとストリーミングの双方で使われる | 保守やガバナンスの比較対象。エンジンと表形式の対応機能はそれぞれの仕様を確認する。[Delta Lake公式文書](https://docs.delta.io/) |
| Apache Hudi | コミットのタイムラインを持ち、増分読取や更新の多いデータについて複数の表タイプを提供する | 更新頻度の高いワークロードを別途評価するときの候補。[Apache Hudi概要](https://hudi.apache.org/docs/overview/) |

### Iceberg公式仕様のスナップショット図

![Icebergの表メタデータからスナップショット、マニフェスト、データファイルへたどる公式図](images/iceberg-metadata.png)

各スナップショットは、その時点で読み取るファイルの組を表します。Foundation Rの `gold._release` は、この仕組みを使い、各Gold表のスナップショットIDをまとめて記録します。古い公開世代を読みたい場合は、参照先のスナップショットとファイルも保存しておきます。[Foundation Rの公開手順](ARCHITECTURE.md#公開と失敗時の一貫性)

図はApache Iceberg公式仕様の「Iceberg snapshot structure」です。© The Apache Software Foundation。Apache License 2.0に従って原図を掲載します。[掲載元](https://iceberg.apache.org/spec/#overview) · [取得元の固定リビジョン](https://github.com/apache/iceberg/blob/c44bd8e7c183cf92339939f2aff5ca2aa2a0fd3d/site/docs/assets/images/iceberg-metadata.png) · [Apache License 2.0](images/iceberg-LICENSE.txt) · [上流NOTICE](images/iceberg-NOTICE.txt)

## 実際に使い始めるときの判断

- まず1台のVMで合成データを使い、業務契約と受入結果を揃えます。処理時間やデータ量が限界に達したら、外部エンジンからの相互運用と既存Sparkジョブの利用を検証します。[データ量と構成の境界](ARCHITECTURE.md#公開と失敗時の一貫性)
- 表のキー、時刻の基準、単位、NULLと取消の扱いを先に決めます。集計の数値が想定と違うときは、Gold、Silver・Quarantine、原本の順に照合します。[入力の契約](SOURCES.md)
- AIをつなぐ場合は、出荷状況など目的を1つ選び、返却行数と読取範囲を限定します。回答の根拠として公開世代を保持し、誤答時に同じ世代のデータを再確認します。[Gold APIとLangChain](API.md)
- 復元の練習では、オブジェクト、カタログ、設定をセットで扱い、復元先から同じ公開世代を読めることを確認します。[運用、バックアップ、受入試験](OPERATIONS.md)

## 参考書籍

- Denny Leeほか『詳解 データレイクハウスアーキテクチャ』の1章と9章を表形式と配置設計の比較に、11〜13章をCDC、ガバナンス、リネージの確認に参照しました。このリポジトリの書込と読取の根拠はApache Icebergの仕様です。[オライリー・ジャパン](https://www.oreilly.co.jp/books/9784814401390/)
- Eric Broda、Davis Broda『エージェンティックメッシュ』の3章と5〜6章を、決定的な処理とAIの判断を分けること、ツールの最小権限、追跡性の説明に参照しました。[オライリー・ジャパン](https://www.oreilly.co.jp/books/9784814401758/)

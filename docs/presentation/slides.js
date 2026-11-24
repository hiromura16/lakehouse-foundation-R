"use strict";

const slides = [
  ["概要", "Lakehouse Foundation R", "業務データの保存・集計と、調査・自動化を1台の検証VMで試せるデータ基盤",
    [["対象データ", "製造実績・受注出荷・設備のファイル、SQL Server抽出、任意のKafka CDC"],
     ["標準構成", "JVMを使わずにDuckDB、Iceberg、SeaweedFSで集計と原本を管理"],
     ["使い始める順番", "標準構成で合成データを投入し、Gold14行と公開世代を確認"],
     ["追加の運用ツール", "n8nで定型処理、OpenObserveで調査、InfluxDB 3で時系列SQL"]],
    "この資料は、導入・活用・運用の判断に必要な境界を説明します。"],
  ["アーキテクチャ", "責務を4つに分ける", "データの所在、計算、利用、監視を切り分け、処理の再実行と運用変更を行いやすくする。",
    [["受け渡し", "完了印付きCSV・JSONL・画像を受け取る。外部Kafkaに接続する構成も選べる。"],
     ["処理と保存", "DuckDBでSQL、PyIcebergで表の版を管理。SeaweedFSが原本とファイル、Lakekeeperがカタログを提供。"],
     ["閲覧と自動化", "Gold読取API・集計画面・LangChain。n8nから必要なAPIを呼び出す。"],
     ["運用監視", "Blackboxが疎通を確認し、Prometheusが収集。GrafanaまたはRust製のOpenObserveで調べる。"]],
    "役割ごとの接続先と採用理由は、運用ポータルとアーキテクチャ文書から参照できます。"],
  ["データの流れ", "原本から業務集計へ", "原本、検証済みデータ、公開済みの集計を区別すると、数字が変わった理由を追跡できる。",
    [["Raw・Bronze", "完了印のSHA-256を照合し、原本と入力の由来を保存。"],
     ["Silver・隔離", "業務キー、型、最新版を照合し、理由のある異常行はQuarantineへ。"],
     ["Gold", "工場日別の生産・受注の充足・設備集計を用途ごとに作る。"],
     ["公開世代", "すべての出力が揃った時点で表の版を登録する。利用者は同じ世代の結果を参照。"]],
    "取込・変換が失敗したときは、前回公開した世代を確認します。"],
  ["業務データ", "数値の意味を揃えてから結合する", "製造と出荷は異なる業務。行の粒度、日付の基準、欠損や取消の意味を契約で定義する。",
    [["製造", "製造指図・生産実績・停止時間を別々に工場×日へ集計してから結合。"],
     ["出荷", "複数の出荷を受注キーへ畳み、未出荷数と約束日後の出荷を確認。"],
     ["設備", "時刻をUTCに正規化。1分統計はGOODだけで計算し、最新値は品質も表示。"],
     ["確認する数字", "画面では行数、時刻、Iceberg表、公開世代IDを併せて確認する。"]],
    "合成fixtureの受入では、結合で集計値が増えないことと遅延到着の扱いを照合します。"],
  ["運用ツール", "調査内容によって道具を選ぶ", "同じ運用画面ですべてを操作する必要はない。問題の種類から入口を選ぶ。",
    [["稼働停止・応答遅延", "Grafanaで現在の疎通と履歴。OpenObserveで運用メトリクスをPromQL検索。"],
     ["入力後の業務数値", "Gold閲覧画面または読取APIで日次生産、受注、設備の結果と出所を比較。"],
     ["測定点・時間範囲", "InfluxDB 3 Coreへ書いた時系列をExplorerのSQLで探索。"],
     ["自動化・連携", "n8nの疎通フローから始め、手動テスト後に権限と実行間隔を設定。"]],
    "ポータルの疎通結果と、業務データの品質試験は別に確認します。"],
  ["自動化", "ワークフローと業務処理を分ける", "データの変換ルールはGitのSQLに置き、通知や定期実行などのつなぎをn8nに任せる。",
    [["開始点", "同梱ワークフローの手動実行で内部の /health を確認。"],
     ["権限", "読取とジョブ実行のトークンを分け、秘密は各ツールの資格情報として設定。"],
     ["安全な再実行", "同一入力で新規取込0を確認し、結果を公開世代IDで記録。"],
     ["異常時", "HTTPエラーとワーカーの診断を確認。既存の公開世代を維持して入力を修正。"]],
    "スケジュールやWebhookの有効化は、実際の接続先と運用の合意後に行います。"],
  ["時系列", "設備履歴と集計を用途別に使う", "短い時間範囲の測定点を追う作業と、業務集計として共有する作業を区別する。",
    [["InfluxDB 3 Core", "設備測定点を時刻・タグ・数値で保存し、SQLで範囲を絞る。"],
     ["InfluxDB Explorer", "認証付きの画面からクエリとチャートを試す。トークンは運用側で管理。"],
     ["Icebergの設備集計", "1分集計と品質判定をGoldの公開世代として配布。"],
     ["比較対象", "GreptimeDBとリアルタイム転送の採用は、同じ測定条件で別途評価する。"]],
    "時系列ストアへの書き込み経路と保存期間は、利用するデータ源ごとに決めます。"],
  ["配置", "会社VMで通信の失敗箇所を特定する", "端末、Dockerデーモン、BuildKit、アプリでは、使うプロキシ設定とCAが異なる。",
    [["Git", "公開HTTPSのls-remoteから疎通を確認し、OSの信頼CAを使う。"],
     ["イメージ・依存", "Docker daemonとBuildKitの経路を分け、Docker Hub、Quay、ECRなど許可先を確認。"],
     ["Dokploy", "同じComposeを読み込み、会社の環境変数とGit外の秘密ファイルを設定。"],
     ["運用画面", "ループバック、SSH転送または認証付きTLS経路を使う。既設データは専用のvolumeで保全。"]],
    "会社VMに到着したら、合成データの受入手順から結果を残します。"],
  ["受入", "何が通れば使い始められるか", "確認した環境、入力、成功条件、観測値を一緒に記録する。",
    [["標準構成", "カタログ初期化の終了0、7サービスの起動、業務契約の11検査。"],
     ["合成データ", "6ファイル→業務SQL14本→日次生産14行。同じ入力の再実行で新規0。"],
     ["監視・調査・自動化", "追加サービスの疎通、n8nの手動フロー、時系列の書込みと検索を確認。"],
     ["設置先の受入", "実プロキシ、CA、実DB、保持期間、バックアップ復元は会社VMで照合。"]],
    "受入の例、再実行方法、失敗時に見るログはリポジトリの導入文書にまとめています。"],
  ["構想からの実装", "次に評価する候補を決める", "追加の製品は、処理量、復旧、相互運用、運用負担を独立した試験で比較する。",
    [["単一VMのSQL", "DuckDBとPyIcebergで導入。DataFusion単独の変換は同じ入力で結果と制約を測定する。"],
     ["ストリーミング", "外部KafkaのCDCを先に検証。RisingWaveはCurrent StateとIceberg連携が評価項目。"],
     ["時系列", "InfluxDB 3 Coreを導入。GreptimeDBと転送経路は対照試験の候補。"],
     ["分散実行", "Sail、Ballista、Cometは要件と互換性を整理。CometはSparkの実行環境を前提とする。"]],
    "導入済み、試験済み、評価候補を区別して説明します。"],
];

const deck = document.querySelector("#deck");
const selector = document.querySelector("#selector");
const previous = document.querySelector("#previous");
const next = document.querySelector("#next");
let selected = 0;

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function select(index) {
  if (index < 0 || index >= slides.length) return;
  selected = index;
  selector.value = String(index);
  for (const [position, page] of Array.from(deck.children).entries()) page.hidden = position !== index;
  previous.disabled = index === 0;
  next.disabled = index === slides.length - 1;
}

for (const [index, [section, title, lead, items, conclusion]] of slides.entries()) {
  const option = element("option", title);
  option.value = String(index);
  selector.append(option);
  const page = element("section", "", "slide" + (index === 0 ? " cover" : ""));
  const heading = element("header");
  heading.append(element("div", section, "section"), element("div", (index + 1) + " / " + slides.length, "page"));
  const grid = element("div", "", "items");
  for (const [label, body] of items) {
    const item = element("article", "", "item");
    item.append(element("h2", label), element("p", body));
    grid.append(item);
  }
  page.append(heading, element("h1", title), element("p", lead, "lead"), grid, element("p", conclusion, "conclusion"));
  deck.append(page);
}
selector.addEventListener("change", () => select(Number(selector.value)));
previous.addEventListener("click", () => select(selected - 1));
next.addEventListener("click", () => select(selected + 1));
document.querySelector("#print").addEventListener("click", () => window.print());
document.addEventListener("keydown", (event) => {
  if (event.target.tagName === "SELECT") return;
  if (event.key === "ArrowRight") select(selected + 1);
  if (event.key === "ArrowLeft") select(selected - 1);
});
select(0);

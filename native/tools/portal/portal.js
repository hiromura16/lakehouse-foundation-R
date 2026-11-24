"use strict";

// 名前・説明と疎通先は固定する。接続ポートだけを配置時の設定から受け取る。
const entries = [
  ["n8n", "n8n", "automation", "ワークフロー", "標準の疎通フローを手動実行し、HTTPの定型処理を組み合わせます。", "/"],
  ["openobserve", "OpenObserve", "observe", "Rust製の監視・調査", "PromQLで監視履歴を調べ、サービス状態やジョブ成否のボードを開きます。", "/"],
  ["explorer", "InfluxDB Explorer", "data", "時系列のSQL閲覧", "InfluxDB 3 Coreの設備測定値をSQLとチャートで確認します。", "/"],
  ["browser", "集計結果の確認", "data", "業務集計", "日次生産や設備の集計を開き、Iceberg表と公開世代の出所を照合します。", "/"],
  ["grafana", "Grafana", "observe", "稼働監視", "サービスの現在値、疎通履歴、ジョブの成功と失敗を一覧で見ます。", "/d/lakehouse-r-runtime"],
  ["prometheus", "Prometheus", "observe", "メトリクス収集", "取得先ごとの疎通を確認し、監視結果やPromQLを調べます。", "/targets"],
  ["lakehouse", "Lakehouse API", "automation", "操作・読取の入口", "ワーカーの準備状況を確認。業務データやジョブには用途別トークンを使います。", "/health"],
  ["catalog", "Lakekeeper", "data", "テーブルカタログ", "Icebergの名前空間、テーブル、メタデータを参照します。", "/ui/"],
  ["influxdb", "InfluxDB 3 Core", "data", "Rust製の時系列ストア", "時刻を持つ測定点を書き込み、認証付きSQLで範囲集計できます。", null],
];
const categories = { all:"すべてのアプリ", automation:"自動化", observe:"監視・調査", data:"業務・時系列データ" };
const states = {};
let ports = {};
let category = "all";
const tools = document.querySelector("#tools");
const search = document.querySelector("#search");

function node(tag, value, cls) {
  const element = document.createElement(tag);
  if (value) element.textContent = value;
  if (cls) element.className = cls;
  return element;
}

function render() {
  const query = search.value.trim().toLowerCase();
  const shown = entries.filter((e) => (category === "all" || e[2] === category)
    && e.join(" ").toLowerCase().includes(query));
  tools.replaceChildren();
  document.querySelector("#selected").textContent = categories[category] + " · " + shown.length + "件";
  document.querySelector("#empty").hidden = shown.length > 0;
  for (const [id, title, , role, description, path] of shown) {
    const tile = node("article", "", "tile");
    const header = node("header");
    const heading = node("div");
    heading.append(node("h3", title), node("div", role, "role"));
    const state = states[id];
    const label = state === true ? "✓ 応答あり" : state === false ? "× 応答なし" : "確認中";
    header.append(heading, node("span", label, "status " + (state === true ? "ok" : state === false ? "fail" : "")));
    tile.append(header, node("p", description));
    const port = ports[id];
    if (path && state === true && Number.isInteger(port) && port > 0 && port <= 65535) {
      const endpoint = new URL(path, location.origin);
      endpoint.port = port;
      const link = node("a", "開く");
      link.href = endpoint.href;
      link.setAttribute("aria-label", title + "を開く");
      tile.append(link);
    }
    tools.append(tile);
  }
}

async function refresh() {
  const control = document.querySelector("#refresh");
  control.disabled = true;
  try {
    const config = await fetch("ports.json", {cache:"no-store", signal:AbortSignal.timeout(5000)});
    if (!config.ok) throw new Error("配置設定");
    ports = await config.json();
    document.querySelector("#connection-error").hidden = true;
    await Promise.all(entries.map(async ([id]) => {
      try {
        const result = await fetch("status/" + id, {cache:"no-store",signal:AbortSignal.timeout(5500)});
        states[id] = result.ok;
      } catch {
        states[id] = false;
      }
    }));
    document.querySelector("#summary").textContent = Object.values(states).filter(Boolean).length + " / " + entries.length + " 応答あり";
    document.querySelector("#checked").textContent = "確認 " + new Intl.DateTimeFormat("ja-JP", {timeStyle:"medium"}).format(new Date());
  } catch {
    document.querySelector("#connection-error").hidden = false;
    document.querySelector("#summary").textContent = "状態を取得できません";
  } finally {
    control.disabled = false;
    render();
  }
}

for (const button of document.querySelectorAll("[data-category]")) {
  button.setAttribute("aria-pressed", String(button.dataset.category === category));
  button.addEventListener("click", () => {
    category = button.dataset.category;
    for (const item of document.querySelectorAll("[data-category]")) {
      item.setAttribute("aria-pressed", String(item.dataset.category === category));
    }
    render();
  });
}
search.addEventListener("input", render);
document.querySelector("#refresh").addEventListener("click", refresh);
render();
refresh();

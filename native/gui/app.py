"""取り込み後の集計と、参照している公開世代をブラウザーで確認する。"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

import streamlit as st

REGISTRY = Path(__file__).with_name("data-products.json")
if not REGISTRY.is_file():
    # ソースから閲覧画面を起動する場合は、同じ契約をcheckoutから読む。
    REGISTRY = (
        Path(__file__).resolve().parents[2]
        / "contracts/native/data-products.json"
    )
DATASETS = json.loads(REGISTRY.read_text(encoding="utf-8"))["datasets"]
CHANNELS = {"files": "ファイル", "sql": "SQL", "cdc": "CDC"}
BUSINESS_DATASETS = ("factory_daily_production", "sales_fulfillment")
API_ORIGIN = "http://lakehouse:8080"
COLUMN_LABELS = {
    "factory_id": "工場ID",
    "production_date": "生産日（UTC）",
    "planned_qty": "計画数",
    "good_qty": "良品数",
    "reject_qty": "不良数",
    "downtime_minutes": "停止時間（分）",
    "total_qty": "総生産数",
    "yield_pct": "歩留まり（%）",
    "plan_adherence_pct": "計画達成率（%）",
    "sales_order_id": "受注ID",
    "product_id": "品目ID",
    "order_date": "受注日",
    "promised_date": "約束日",
    "ordered_qty": "受注数",
    "unit_price": "単価",
    "shipped_qty": "出荷数",
    "open_qty": "未出荷数",
    "fulfillment_pct": "充足率（%）",
    "late_delivered_qty": "遅延出荷数",
    "late_shipment_count": "遅延出荷回数",
    "is_fully_shipped": "出荷済み",
    "last_shipped_at": "最終出荷（UTC）",
    "line_id": "ラインID",
    "equipment_id": "設備ID",
    "sensor_id": "センサーID",
    "measurement": "測定項目",
    "unit": "単位",
    "timestamp": "測定時刻（UTC）",
    "sequence_no": "順序番号",
    "value": "測定値",
    "quality_code": "品質",
    "source_hash": "原本SHA-256",
    "minute": "集計窓の開始（UTC）",
    "sample_count": "測定点数",
    "good_count": "GOODの点数",
    "good_avg": "GOODの平均",
    "good_min": "GOODの最小",
    "good_max": "GOODの最大",
}


class NoRedirect(HTTPRedirectHandler):
    """読取資格情報の送信先を、Compose内部の固定APIに限定する。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """リダイレクトを拒否し、資格情報の転送を防ぐ。"""
        return None


def datasets_for(channel):
    """業務経路は日次生産と受注・出荷、ファイル経路は設備集計も扱う。"""
    return list(DATASETS) if channel == "files" else BUSINESS_DATASETS


def get_dataset(name, limit, release, filters, *, channel="files"):
    """許可した取込経路と名前空間で、指定世代の集計を取得する。"""
    if channel not in CHANNELS or name not in datasets_for(channel):
        raise ValueError("取込経路または集計が選択範囲にありません。")
    route = "/v1" if channel == "files" else f"/v1/{channel}"
    table = f"gold.{name}" if channel == "files" else f"{channel}_gold.{name}"
    parameters = {"limit": limit, **filters}
    if release is not None:
        parameters["release"] = release
    path = f"{route}/gold/{name}?{urlencode(parameters)}"
    request = Request(
        API_ORIGIN + path,
        headers={"Authorization": f"Bearer {os.environ['READER_TOKEN']}"},
    )
    # 内部サービスへの読取トークンを、環境変数の外部proxyへ渡さない。
    opener = build_opener(ProxyHandler({}), NoRedirect)
    with opener.open(request, timeout=10) as response:
        data = json.load(response)
    try:
        rows = data["rows"]
        if (
            not isinstance(rows, list)
            or not all(isinstance(row, dict) for row in rows)
            or not isinstance(data["total_rows"], int)
            or not data["release_snapshot_id"].isdigit()
            or data["table"] != table
            or (release is not None and data["release_snapshot_id"] != release)
            or not (
                data["snapshot_id"] is None
                or isinstance(data["snapshot_id"], str)
            )
        ):
            raise TypeError
        published_at = datetime.fromisoformat(data["released_at"])
        if published_at.tzinfo is None:
            raise ValueError("公開時刻にはタイムゾーンが必要です。")
    except (AttributeError, KeyError, TypeError) as error:
        raise ValueError("Gold読取APIの応答形式が異なります。") from error
    return data, published_at.astimezone(timezone.utc)


def show():
    """業務集計の確認、同じ世代での比較、最新版の取得を提供する。"""
    st.set_page_config(
        page_title="集計結果の確認 | Foundation R", layout="wide"
    )
    st.title("集計結果の確認")
    st.write(
        "投入・変換後の数値を確認し、いつ公開された結果かを追跡できます。"
    )
    channel = st.segmented_control(
        "取込経路",
        list(CHANNELS),
        format_func=CHANNELS.__getitem__,
        default="files",
        required=True,
        key="intake-channel",
        help="公開世代と絞り込みは経路ごとに保持します。",
    )
    channel_label = CHANNELS[channel]
    selection, count = st.columns([3, 1])
    name = selection.selectbox(
        "確認する集計",
        datasets_for(channel),
        format_func=lambda key: DATASETS[key]["title"],
        key=f"dataset-{channel}",
        persist_state="page",
    )
    limit = count.selectbox(
        "表示上限",
        [25, 50, 100, 200],
        index=1,
        key=f"limit-{channel}",
        persist_state="page",
    )
    dataset = DATASETS[name]
    st.caption(dataset["description"])
    with st.expander("数値の定義"):
        st.write(dataset["definition"])
        st.caption(f"確認先　{dataset['owner_role']}")
    filters = {}
    scope = f"{channel}-{name}"
    filter_label = "工場・日付で絞り込む"
    if name == "sales_fulfillment":
        filter_label = "受注ID・日付で絞り込む"
    elif name.startswith("sensor_"):
        filter_label = "工場・設備・日付で絞り込む"
    with st.expander(filter_label):
        items = (
            [("sales_order_id", "受注ID")]
            if name == "sales_fulfillment"
            else [("factory_id", "工場ID")]
        )
        if name.startswith("sensor_"):
            items.append(("equipment_id", "設備ID"))
        for key, label in items:
            if value := st.text_input(
                label,
                max_chars=128,
                key=f"{scope}-{key}",
                persist_state="page",
            ).strip():
                filters[key] = value
        start, end = st.columns(2)
        from_date = start.date_input(
            "開始日（UTC・当日を含む）",
            value=None,
            format="YYYY-MM-DD",
            key=f"{scope}-date-from",
            persist_state="page",
        )
        until_date = end.date_input(
            "終了の境界日（UTC・当日より前）",
            value=None,
            format="YYYY-MM-DD",
            key=f"{scope}-date-to",
            persist_state="page",
        )
        if from_date:
            filters["date_from"] = from_date.isoformat()
        if until_date:
            filters["date_to"] = until_date.isoformat()
        if from_date and until_date and from_date >= until_date:
            st.warning("終了の境界日は開始日より後に指定してください。")
            return
    reload_latest = st.button(
        "最新版を読み込む",
        key=f"reload-{channel}",
        help="選択中の取込経路の公開世代だけを更新します。",
    )
    # 取得に失敗した場合は、選択中の経路も元の確定世代を保持する。
    release_key = f"release-{channel}"
    release = None if reload_latest else st.session_state.get(release_key)
    try:
        data, published_at = get_dataset(
            name, limit, release, filters, channel=channel
        )
    except HTTPError as error:
        if error.code == 400:
            st.warning("入力した絞り込み条件を確認してください。")
        elif error.code == 404:
            st.info(
                f"{channel_label}経路では、この集計を表示できる公開世代がありません。"
                "対応する取込と変換を確認し、「最新版を読み込む」を押してください。"
            )
        elif error.code == 401:
            st.error("運用担当者に読取トークンの設定確認を依頼してください。")
        elif error.code == 503 and error.headers.get("Retry-After"):
            st.warning(
                "参照が集中しています。少し待ってから、この画面を再読み込みしてください。"
            )
        else:
            st.error(
                f"データを取得できませんでした（HTTP {error.code}）。"
                "Lakehouseの稼働状態を確認してください。"
            )
        return
    except (URLError, TimeoutError, ValueError):
        st.error(
            "Lakehouseから集計結果を受け取れませんでした。"
            "稼働状態を確認してから読み直してください。"
        )
        return

    st.session_state[release_key] = data["release_snapshot_id"]
    rows = data["rows"]
    sensor = data.get("sensor") or {}
    if sensor.get("status") == "retained":
        st.warning(
            "設備データの更新は保留中です。最後に確定した計測値を表示しています。"
        )
        if built_at := sensor.get("built_at"):
            stamp = datetime.fromisoformat(built_at).astimezone(timezone.utc)
            st.caption(f"設備データの確定（UTC） {stamp:%Y-%m-%d %H:%M:%S}")
    st.caption(
        f"{channel_label}経路 ／ 公開時刻（UTC） "
        f"{published_at:%Y-%m-%d %H:%M:%S} ／ "
        f"{len(rows)}行を表示（該当{data['total_rows']}行）"
    )
    if rows:
        st.dataframe(
            rows, width="stretch", hide_index=True, column_config=COLUMN_LABELS
        )
    else:
        st.info(
            "条件に一致した行がありません。絞り込み条件と公開世代を確認してください。"
        )
    with st.expander("データの出所・公開世代"):
        table, release_id, snapshot = st.columns([2, 1, 1])
        table.markdown("**Iceberg表**")
        table.code(data["table"], language=None)
        release_id.markdown("**公開世代ID**")
        release_id.code(data["release_snapshot_id"], language=None)
        snapshot.markdown("**表のスナップショットID**")
        snapshot.code(
            data["snapshot_id"] or "空のスナップショット", language=None
        )


if __name__ == "__main__":
    show()

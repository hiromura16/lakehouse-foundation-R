"""公開済みのGoldを、ブラウザーで表とスナップショット情報として確認する。"""

import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

import streamlit as st

DATASETS = {
    "工場別の日次生産": "factory_daily_production",
    "受注・出荷の充足状況": "sales_fulfillment",
    "設備の最新値": "sensor_latest",
    "設備の1分集計": "sensor_minute",
}
API_ORIGIN = "http://lakehouse:8080"


class NoRedirect(HTTPRedirectHandler):
    """読取資格情報の送信先を、Compose内部の固定APIに限定する。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """リダイレクトを拒否し、資格情報の転送を防ぐ。"""
        return None


def get_dataset(name, limit):
    """定型GETで、指定された上限までの公開データと由来情報を受け取る。"""
    path = f"/v1/gold/{DATASETS[name]}?{urlencode({'limit': limit})}"
    request = Request(
        API_ORIGIN + path,
        headers={"Authorization": f"Bearer {os.environ['READER_TOKEN']}"},
    )
    with build_opener(NoRedirect).open(request, timeout=10) as response:
        return json.load(response)


def show():
    """選択したGold表と、その結果に対応する公開世代を表示する。"""
    st.set_page_config(page_title="Foundation R データ閲覧", layout="wide")
    st.title("Foundation R データ閲覧")
    selection, count = st.columns([3, 1])
    name = selection.selectbox("データセット", list(DATASETS))
    limit = count.selectbox("表示件数", [25, 50, 100, 200], index=1)
    st.button("最新の公開世代を読み直す")
    try:
        data = get_dataset(name, limit)
    except HTTPError as error:
        if error.code == 404:
            st.info(
                "公開済みのデータがありません。変換後に読み直してください。"
            )
        else:
            st.error(f"データの取得に失敗しました。HTTP {error.code}")
        return
    except (URLError, TimeoutError, ValueError):
        st.error(
            "Lakehouseの応答を確認できません。稼働監視を確認してください。"
        )
        return

    rows = data["rows"]
    st.markdown(f"**{len(rows)}件表示** ／ 全{data['total_rows']}件")
    if rows:
        st.dataframe(rows, width="stretch", hide_index=True)
    else:
        st.info("この公開世代には行がありません。")
    with st.expander("データの出所"):
        st.markdown("**Iceberg表**")
        st.code(data["table"], language=None)
        st.markdown("**公開世代ID**")
        st.code(data["release_snapshot_id"], language=None)
        st.markdown("**表のスナップショットID**")
        st.code(data["snapshot_id"] or "空のスナップショット", language=None)
        st.markdown("**公開時刻（UTC）**")
        st.code(data["released_at"], language=None)


if __name__ == "__main__":
    show()

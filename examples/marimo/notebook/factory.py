"""承認済みの日次生産Goldを工場別に確認する。"""

import marimo

__generated_with = "0.25.2"
app = marimo.App(width="medium")


@app.cell
def _():
    """設定済みの一時データ領域だけを読み込む。"""
    from pathlib import Path

    import marimo as mo
    from approved_gold import open_snapshot_db

    provenance, database = open_snapshot_db(Path("/approved"))
    return database, mo, provenance


@app.cell
def _(database, mo, provenance):
    """読み込んだ件数と来歴を表示し、同じ来歴を担当者と照合できるようにする。"""
    count, factories = database.execute(
        "SELECT count(*), count(DISTINCT factory_id) FROM production"
    ).fetchone()
    origin = (
        "合成期待値（公開世代なし）"
        if provenance["source"] == "committed_synthetic_oracle"
        else (
            f"公開世代 {provenance['release_snapshot_id']} / "
            f"表スナップショット {provenance['snapshot_id']}"
        )
    )
    mo.md(
        f"# 工場の日次生産\n\n"
        f"**全{count}行 / {factories}工場**\n\n"
        f"{origin}\n\n"
        f"スナップショットSHA-256 `{provenance['sha256']}`"
    )
    return


@app.cell
def _(database, mo):
    """選択肢を入力データから作る。ブラウザーから自由入力SQLを受け取らない。"""
    choices = [
        item[0]
        for item in database.execute(
            "SELECT DISTINCT factory_id FROM production ORDER BY factory_id"
        ).fetchall()
    ]
    selected_factory = mo.ui.dropdown(
        choices, value=choices[0], label="工場", allow_select_none=False
    )
    selected_factory
    return (selected_factory,)


@app.cell
def _(database, mo, selected_factory):
    """選択した工場の数量と、分子・分母を合算して求めた歩留まりを表示する。"""
    from html import escape

    totals = database.execute(
        """
        SELECT count(*), sum(planned_qty), sum(good_qty), sum(reject_qty),
               round(100.0 * sum(good_qty) / nullif(sum(total_qty), 0), 2)
        FROM production
        WHERE factory_id = ?
        """,
        [selected_factory.value],
    ).fetchone()
    days, planned, good, rejected, yield_pct = totals
    rate = "未定義" if yield_pct is None else f"{yield_pct:.2f}%"
    # 工場IDをHTMLとして解釈させず、そのまま見出しに表示する。
    mo.vstack(
        [
            mo.Html(f"<h2>{escape(selected_factory.value)}の結果</h2>"),
            mo.md(
                f"**対象日 {days}日 / 計画 {planned} / "
                f"良品 {good} / 不良 {rejected}**\n\n"
                f"期間の歩留まり {rate}"
            ),
        ]
    )
    return


@app.cell
def _(database, mo, selected_factory):
    """同じ条件の日別計画と良品数を、ネットワークを使わず端末で描画する。"""
    import altair as alt

    counts = database.execute(
        """
        SELECT production_date, planned_qty, good_qty
        FROM production
        WHERE factory_id = ?
        ORDER BY production_date
        """,
        [selected_factory.value],
    ).fetchall()
    data = [
        {"日付": day.isoformat(), "区分": name, "数量": int(amount)}
        for day, plan, good in counts
        for name, amount in (("計画", plan), ("良品", good))
    ]
    chart = (
        alt.Chart(alt.Data(values=data), title="日別の計画数と良品数")
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X(
                "日付:O",
                axis=alt.Axis(
                    labelAngle=0,
                    labelExpr="utcFormat(toDate(datum.label), '%m/%d')",
                    labelOverlap="greedy",
                ),
            ),
            xOffset="区分:N",
            y=alt.Y("数量:Q", axis=alt.Axis(format=",.0f")),
            color=alt.Color(
                "区分:N",
                scale=alt.Scale(
                    domain=["計画", "良品"], range=["#8ea7c2", "#00796b"]
                ),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=["日付:O", "区分:N", "数量:Q"],
        )
        .properties(width="container", height=230)
    )
    mo.ui.altair_chart(chart, chart_selection=False, legend_selection=False)
    return


if __name__ == "__main__":
    app.run()

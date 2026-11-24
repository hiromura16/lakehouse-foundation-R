"""標準Compose内の実Gold APIを、データ閲覧画面から照合する。"""

from pathlib import Path

from streamlit.testing.v1 import AppTest


def tests(app):
    """経路ごとの公開世代、設備集計と未公開の表示を操作して確認する。"""
    page = AppTest.from_file(str(app), default_timeout=20).run()
    assert not page.exception and not page.error
    assert page.segmented_control[0].value == "files"
    assert "設備の1分集計" in page.selectbox(key="dataset-files").options
    page.selectbox(key="dataset-files").set_value("sensor_minute").run()
    assert not page.exception and not page.error and page.dataframe
    (file_table, files, _) = [element.value for element in page.code]
    assert file_table == "gold.sensor_minute" and files.isdigit()
    factory = page.text_input(key="files-sensor_minute-factory_id")
    factory.set_value("F-SYNTHETIC").run()
    assert not page.exception and not page.error and page.dataframe

    page.segmented_control[0].set_value("sql").run()
    assert not page.exception and not page.error
    assert page.selectbox(key="dataset-sql").options == [
        "日次生産",
        "受注・出荷",
    ]
    page.selectbox(key="dataset-sql").set_value("sales_fulfillment").run()
    assert not page.exception and not page.error
    (sql_table, sql, _) = [element.value for element in page.code]
    assert sql_table == "sql_gold.sales_fulfillment"
    assert sql.isdigit() and sql != files

    page.segmented_control[0].set_value("cdc").run()
    assert not page.exception and not page.error and not page.dataframe
    assert "release-cdc" not in page.session_state
    assert any("CDC経路" in message.value for message in page.info)

    page.segmented_control[0].set_value("files").run()
    assert not page.exception and not page.error and page.dataframe
    assert page.session_state["release-files"] == files
    assert page.session_state["release-sql"] == sql
    assert page.text_input(key="files-sensor_minute-factory_id").value == (
        "F-SYNTHETIC"
    )


if __name__ == "__main__":
    tests(Path.cwd() / "app.py")
    print("実APIのファイル・SQL公開世代、未公開CDCと絞り込みを確認しました。")

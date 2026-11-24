"""業務の行を出さず、公開した表の版だけを追跡できることを確かめる。"""

import unittest
from types import SimpleNamespace

from native import catalog_audit, lineage

RELEASE = {
    "release_snapshot_id": "9007199254740993",
    "released_at": "2026-01-05T12:01:00+00:00",
    "publication_started_at": "2026-01-05T12:00:00+00:00",
    "inputs": {"erp_work_orders": 12},
    "outputs": {
        "gold.factory_daily_production": 22,
        "gold.sensor_latest": None,
    },
}


class LineageTests(unittest.TestCase):
    """版付きの出力、旧世代、保持に必要な参照を確かめる。"""

    def test_new_release_is_a_stable_run(self):
        """再出力のUUID、順序、64-bit文字列と対象表を固定する。"""
        run = lineage.events(RELEASE, site="fixture", channel="cdc")
        self.assertEqual(
            run, lineage.events(RELEASE, site="fixture", channel="cdc")
        )
        start, complete = run
        self.assertEqual(start["run"], complete["run"])
        self.assertEqual(start["eventType"], "START")
        self.assertEqual(complete["eventType"], "COMPLETE")
        (source,) = complete["inputs"]
        self.assertEqual(source["name"], "cdc_gold.factory_daily_production")
        (produced,) = complete["outputs"]
        self.assertEqual(produced["name"], "cdc_gold._release")
        self.assertEqual(
            produced["facets"]["version"]["datasetVersion"],
            RELEASE["release_snapshot_id"],
        )

    def test_old_release_exports_only_existing_version(self):
        """開始時刻を持たない履歴には表の静的情報を出す。"""
        old = dict(RELEASE)
        old.pop("publication_started_at")
        (event,) = lineage.events(old, site="factory-a", channel="files")
        self.assertIn("dataset", event)
        self.assertNotIn("run", event)

    def test_unavailable_snapshot_is_visible(self):
        """現在の表が存在しても指定版がなければ不足を報告する。"""
        catalog = SimpleNamespace(
            table_exists=lambda _: True,
            load_table=lambda _: SimpleNamespace(
                snapshot_by_id=lambda snapshot: (
                    None if snapshot == 22 else snapshot
                )
            ),
        )
        result = catalog_audit.report(catalog, RELEASE, "sql_")
        self.assertFalse(result["all_metadata_available"])
        (table,) = [
            item
            for item in result["references"]
            if not item["metadata_available"]
        ]
        self.assertEqual(table["table"], "sql_gold.factory_daily_production")
        self.assertEqual(table["snapshot_id"], "22")


if __name__ == "__main__":
    unittest.main()

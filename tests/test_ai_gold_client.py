"""公開Goldクライアントの経路・世代・応答上限を合成応答で照合する。"""

import os
import unittest
from unittest.mock import MagicMock, patch

from examples.ai import gold_client

BUSINESS = "factory_daily_production"
FILE_SCOPE = "gold.factory_daily_production"
SQL_SCOPE = "sql_gold.factory_daily_production"
CDC_SCOPE = "cdc_gold.factory_daily_production"
RELEASE = "9212345678901234567"


def fixture(table=FILE_SCOPE, **updates):
    """試験側で固定した公開APIの最小応答を作る。"""
    return {
        "table": table,
        "release_snapshot_id": RELEASE,
        "snapshot_id": "456",
        "rows": [{"good_qty": 3}],
        "total_rows": 1,
        "truncated": False,
        **updates,
    }


class GoldClientTest(unittest.TestCase):
    """環境で与えた許可と、応答中の出典をそれぞれ検査する。"""

    def client(self, channels=None, origin=None, *, local_only=False):
        """プロセス全体の環境と実接続に依存しない設定を用意する。"""
        settings = {"LAKEHOUSE_READER_TOKEN": "a" * 64}
        if channels is not None:
            settings["LAKEHOUSE_GOLD_CHANNELS"] = channels
        if origin is not None:
            settings["LAKEHOUSE_API_URL"] = origin
        with patch.dict(os.environ, settings, clear=True):
            return gold_client.GoldClient(local_only=local_only)

    def test_default_is_files_and_old_arguments_still_work(self):
        client = self.client()
        response = fixture(release_snapshot_id=RELEASE[1:])
        with patch.object(client, "_get", return_value=response) as get:
            rows = client.rows(BUSINESS, 1, "0" + RELEASE[1:])
            self.assertEqual(rows["table"], FILE_SCOPE)
            get.assert_called_once_with(
                f"/v1/gold/{BUSINESS}?limit=1&release={RELEASE[1:]}"
            )

    def test_disabled_routes_and_unregistered_datasets_stop_before_http(self):
        client = self.client()
        with patch.object(client, "_get") as get:
            for name, channel in (
                (BUSINESS, "sql"),
                (BUSINESS, "cdc"),
                (BUSINESS, "unknown"),
                ("bronze.files", "files"),
            ):
                with self.subTest(name=name, channel=channel):
                    with self.assertRaises(ValueError):
                        client.rows(name, channel=channel)
            get.assert_not_called()

    def test_configured_routes_use_their_own_namespace(self):
        client = self.client("files, sql, cdc")
        for channel, scope in (("sql", SQL_SCOPE), ("cdc", CDC_SCOPE)):
            with (
                self.subTest(channel=channel),
                patch.object(
                    client, "_get", return_value=fixture(scope)
                ) as get,
            ):
                rows = client.rows(BUSINESS, 1, RELEASE, channel=channel)
                self.assertEqual(rows["table"], scope)
                get.assert_called_once_with(
                    f"/v1/{channel}/gold/{BUSINESS}?limit=1&release={RELEASE}"
                )
        with patch.object(client, "_get") as get:
            for channel in ("sql", "cdc"):
                for sensor in ("sensor_latest", "sensor_minute"):
                    with self.assertRaises(ValueError):
                        client.rows(sensor, channel=channel)
            get.assert_not_called()

    def test_latest_is_files_only_even_when_sql_is_allowed(self):
        sql_client = self.client("sql")
        with patch.object(sql_client, "_get") as get:
            with self.assertRaises(ValueError):
                sql_client.latest_release()
            with self.assertRaises(ValueError):
                sql_client.rows(BUSINESS)
            get.assert_not_called()
        files_client = self.client()
        value = {
            "release_snapshot_id": RELEASE,
            "released_at": "2026-01-05T01:02:03Z",
            "outputs": {FILE_SCOPE: "456", "gold.sales_fulfillment": None},
        }
        with patch.object(files_client, "_get", return_value=value) as get:
            self.assertEqual(
                files_client.latest_release(), {**value, "sensor": None}
            )
            get.assert_called_once_with("/v1/releases/latest")

    def test_manifest_rejects_non_file_namespace(self):
        client = self.client()
        value = {
            "release_snapshot_id": RELEASE,
            "released_at": "2026-01-05T01:02:03Z",
            "outputs": {SQL_SCOPE: "456"},
        }
        with (
            patch.object(client, "_get", return_value=value),
            self.assertRaisesRegex(ValueError, "公開世代を照合"),
        ):
            client.latest_release()

    def test_rejects_mismatched_or_oversized_logical_response(self):
        client = self.client("sql")
        for item in (
            fixture(FILE_SCOPE),
            fixture(CDC_SCOPE),
            fixture(SQL_SCOPE, release_snapshot_id="789"),
            fixture(SQL_SCOPE, release_snapshot_id=int(RELEASE)),
            fixture(SQL_SCOPE, snapshot_id=456),
            fixture(SQL_SCOPE, snapshot_id=None),
            fixture(SQL_SCOPE, rows=[{}, {}], total_rows=2, truncated=True),
            fixture(SQL_SCOPE, rows=[5]),
            fixture(SQL_SCOPE, rows=[], total_rows=1),
            fixture(SQL_SCOPE, truncated=True),
            fixture(SQL_SCOPE, truncated=0),
        ):
            with (
                self.subTest(item=item),
                patch.object(client, "_get", return_value=item),
            ):
                with self.assertRaises(ValueError):
                    client.rows(BUSINESS, 1, RELEASE, channel="sql")

    def test_empty_snapshot_and_truncated_rows_are_distinct(self):
        client = self.client("sql")
        empty = fixture(SQL_SCOPE, snapshot_id=None, rows=[], total_rows=0)
        limited = fixture(SQL_SCOPE, total_rows=2, truncated=True)
        for item in (empty, limited):
            with (
                self.subTest(item=item),
                patch.object(client, "_get", return_value=item),
            ):
                self.assertEqual(
                    client.rows(BUSINESS, 1, RELEASE, channel="sql"), item
                )

    def test_rejects_bad_opt_in_and_disallowed_origin(self):
        for value in ("", "files,unknown", "files,", "SQL"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.client(value)
        for origin in (
            "http://other.example.invalid:18502",
            "http://127.0.0.1:0",
            "http://127.0.0.1:not-a-port",
            "http://127.0.0.1/path",
            "http://user:pass@127.0.0.1",
            "http://127.0.0.1/?q=1",
            "https://other.example.invalid",
        ):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                self.client(origin=origin, local_only=True)
        self.client(origin="https://other.example.invalid")

    def test_get_sets_method_and_limits_wire_bytes(self):
        client = self.client()
        body = MagicMock()
        body.__enter__.return_value.read.return_value = b" " * 17
        with (
            patch.object(gold_client, "MAX_RESPONSE_BYTES", 16),
            patch.object(client._http, "open", return_value=body) as call,
            self.assertRaisesRegex(ValueError, "応答が大きすぎます"),
        ):
            client.rows(BUSINESS)
        (request,) = call.call_args.args
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.host, "127.0.0.1:18502")
        self.assertEqual(request.data, None)
        body.__enter__.return_value.read.assert_called_once_with(17)

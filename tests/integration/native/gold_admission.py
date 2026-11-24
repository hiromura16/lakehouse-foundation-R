"""実HTTP接続を並べ、Goldの参照枠を使い切ったときの応答を検査する。"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from secrets import token_hex
from threading import Event, Lock, Thread
from time import monotonic, sleep
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from native import api, engine

PRODUCT = "factory_daily_production"


def main():
    """参照中の2件・混雑時・計算失敗・復帰を同じサーバーで確認する。"""
    release = Event()
    started = {prefix: Event() for prefix in ("", "sql_")}
    token = token_hex(32)

    def query(core, dataset, limit, *, prefix="", **_):
        """入口の実HTTP以外は意図した時点だけを合成して返す。"""
        assert dataset == PRODUCT and limit == 50
        if prefix in started:
            started[prefix].set()
            assert release.wait(15)
            if not prefix:
                raise RuntimeError("合成クエリ失敗")
        return {"rows": [{"rank": 1}]}

    with (
        patch.object(engine, "gold_rows", query),
        patch.dict(os.environ, {"READER_TOKEN": token}),
        ThreadingHTTPServer(
            ("127.0.0.1", 0),
            api.handler(
                SimpleNamespace(
                    STATE={"ready": True, "blocked": False}, LOCK=Lock()
                )
            ),
        ) as server,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        url = f"http://127.0.0.1:{server.server_port}"

        def fetch(path, *, authenticated=True, decode=True):
            """正誤双方のHTTP応答全体を、各クライアントから読み取る。"""
            headers = (
                {"Authorization": f"Bearer {token}"} if authenticated else {}
            )
            req = Request(url + path, headers=headers)
            try:
                response = build_opener(ProxyHandler({})).open(req, timeout=10)
            except HTTPError as error:
                response = error
            with response:
                body = response.read()
                return (
                    response.status,
                    json.loads(body) if decode else body.decode(),
                    response.headers.get("Retry-After"),
                )

        def gauge(active, rejects):
            """同時件数と、飽和時だけ増えるカウンターを照合する。"""
            status, body, _ = fetch("/metrics", decode=False)
            assert status == 200
            samples = dict(line.split() for line in body.splitlines())
            assert samples["lakehouse_gold_active"] == str(active)
            assert samples["lakehouse_gold_limit"] == "2"
            assert samples["lakehouse_gold_rejected_total"] == str(rejects)

        runner = Thread(target=server.serve_forever)
        runner.start()
        try:
            path, sql, cdc = (
                f"/v1/{domain}gold/{PRODUCT}"
                for domain in ("", "sql/", "cdc/")
            )
            first, second = pool.submit(fetch, path), pool.submit(fetch, sql)
            assert all(event.wait(5) for event in started.values())
            assert fetch("/health")[0] == 200
            assert fetch(cdc, authenticated=False)[0] == 401
            assert fetch(cdc + "?limit=0")[0] == 400
            gauge(2, 0)
            assert fetch(cdc) == (503, {"error": "GOLD_BUSY"}, "2")
            gauge(2, 1)
            release.set()
            assert first.result(timeout=7) == (
                503,
                {"error": "GOLD_UNAVAILABLE"},
                None,
            )
            assert second.result(timeout=7) == (
                200,
                {"rows": [{"rank": 1}]},
                None,
            )
            assert fetch(cdc) == (200, {"rows": [{"rank": 1}]}, None)
            # クライアントの受信後に、サーバーが枠を返す場合がある。
            end = monotonic() + 1
            while (
                "lakehouse_gold_active 0"
                not in fetch("/metrics", decode=False)[1]
            ):
                assert monotonic() < end
                sleep(0.01)
            gauge(0, 1)
        finally:
            release.set()
            server.shutdown()
            runner.join()
    print(
        "Goldの2参照枠、503とRetry-After、別経路との共有、枠の回復を確認しました。"
    )


if __name__ == "__main__":
    main()

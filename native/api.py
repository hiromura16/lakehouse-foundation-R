"""認証付きの定型ジョブと、読取専用のGoldデータをHTTPで提供する。"""

import argparse
import json
import os
import re
import secrets
import socket
import sys
import time
import traceback
from datetime import date, datetime
from decimal import Decimal
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

from native import engine


def core_module():
    """検証済みの環境設定で、共有の保存・取込アダプターを読み込む。"""
    import admin

    return admin


def json_value(value):
    """小数の精度と日時のタイムゾーンをAPI応答に保持する。"""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError("JSON応答に対応していない型です。")


def run_job(core, action, body):
    """書込みを直列化し、正常に完了したジョブを記録する。"""
    if not core.LOCK.acquire(blocking=False):
        return None
    try:
        if action == "verify":
            result = core.verify(
                transform=lambda prefix: engine.medallion(core, prefix),
                run_query=lambda sql, prefix: engine.verify_query(
                    core, sql, prefix
                ),
            )
        elif action == "sql-load":
            from scripts.sql_batch_worker import load

            adapter = core.sql_worker_context()
            adapter.medallion = partial(engine.medallion, core)
            result = load(adapter, body["batch_id"], body["observed_at"])
        else:
            result = core.ingest()
            if action == "medallion":
                result.update(engine.medallion(core))
        core.STATE["successful_runs"] += 1
        core.STATE["last_success"] = time.time()
        return result
    except Exception:
        core.STATE["failed_runs"] += 1
        raise
    finally:
        core.LOCK.release()


def report_error(error):
    """入力値や資格情報を含めず、例外の型と発生箇所を記録する。"""
    frames = traceback.extract_tb(error.__traceback__)
    print(
        json.dumps(
            {
                "event": "native.operation_failed",
                "error_type": type(error).__name__,
                "locations": [
                    f"{Path(f.filename).name}:{f.lineno}" for f in frames
                ],
            }
        ),
        flush=True,
    )


def handler(core):
    """共有ストレージの状態を、機能を限定した既存のHTTP処理に渡す。"""

    class Handler(core.Handler):
        """稼働確認、メトリクス、定型ジョブ、スナップショット参照を提供する。"""

        def setup(self):
            """親クラスがHTTPを読み始める前に、待機時間の上限を設ける。"""
            self.request.settimeout(10)
            super().setup()

        def authorized(self, setting):
            """用途別のトークンを一定時間で比較する。"""
            supplied = self.headers.get("Authorization", "").encode("utf-8")
            expected = f"Bearer {os.environ[setting]}".encode()
            return secrets.compare_digest(supplied, expected)

        def respond(self, status, value, content_type="application/json"):
            """共通の応答処理へ渡す日付・日時・小数を直列化する。"""
            if content_type == "application/json":
                value = json.loads(json.dumps(value, default=json_value))
            return super().respond(status, value, content_type)

        def do_GET(self):
            """登録済みのデータだけを、指定された行数の上限まで返す。"""
            try:
                parsed = urlsplit(self.path)
            except ValueError:
                return self.respond(400, {"error": "INVALID_PATH"})
            if not parsed.path.startswith("/v1/"):
                return super().do_GET()
            if not self.authorized("READER_TOKEN"):
                return self.respond(401, {"error": "AUTHENTICATION_REQUIRED"})
            if parsed.path == "/v1/releases/latest" and not parsed.query:
                try:
                    value = engine.latest_release(core, core.catalog())
                    if value:
                        value.pop("sensor_inputs", None)
                    return self.respond(
                        200 if value else 404,
                        value or {"error": "RELEASE_NOT_READY"},
                    )
                except Exception as error:
                    report_error(error)
                    return self.respond(503, {"error": "CATALOG_UNAVAILABLE"})
            match = re.fullmatch(
                r"/v1/(?:(cdc|sql)/)?gold/([a-z_]+)", parsed.path
            )
            if not match or match[2] not in engine.GOLD:
                return self.respond(404, {"error": "UNKNOWN_RESOURCE"})
            try:
                params = parse_qs(
                    parsed.query,
                    strict_parsing=True,
                    max_num_fields=2,
                    keep_blank_values=True,
                )
                if not set(params) <= {"limit", "release"} or any(
                    len(values) != 1 for values in params.values()
                ):
                    raise ValueError
                limit = int(params.get("limit", ["50"])[0])
                release = params.get("release", [None])[0]
                if release is not None and not re.fullmatch(
                    r"[0-9]{1,19}", release
                ):
                    raise ValueError
                release_id = None if release is None else int(release)
                if not 1 <= limit <= 200:
                    raise ValueError
            except ValueError:
                return self.respond(400, {"error": "INVALID_LIMIT"})
            try:
                value = engine.gold_rows(
                    core,
                    match[2],
                    limit,
                    prefix=f"{match[1]}_" if match[1] else "",
                    release_id=release_id,
                )
                return self.respond(
                    200 if value else 404,
                    value
                    or {
                        "error": "DATASET_NOT_READY",
                    },
                )
            except Exception as error:
                report_error(error)
                return self.respond(503, {"error": "GOLD_UNAVAILABLE"})

        def do_POST(self):
            """トークン、パス、要求内容を検証し、許可済みのジョブを実行する。"""
            if self.path != "/run":
                return self.respond(404, {"error": "UNKNOWN_RESOURCE"})
            if not self.authorized("WORKER_TOKEN"):
                return self.respond(401, {"error": "AUTHENTICATION_REQUIRED"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 256 or self.headers.get(
                    "Transfer-Encoding"
                ):
                    raise ValueError
                body = json.loads(self.rfile.read(size))
                if not isinstance(body, dict):
                    raise ValueError
                action = body.get("action")
                if action == "sql-load":
                    from scripts.sql_batch_worker import apply_identifier

                    if set(body) != {"action", "batch_id", "observed_at"}:
                        raise ValueError
                    apply_identifier(body["batch_id"], body["observed_at"])
                elif action not in ("ingest", "medallion", "verify") or set(
                    body
                ) != {"action"}:
                    raise ValueError
            except (ValueError, UnicodeError, socket.timeout):
                return self.respond(400, {"error": "INVALID_JOB"})
            try:
                value = run_job(core, action, body)
                return self.respond(
                    200 if value is not None else 409,
                    value
                    or {
                        "error": "ANOTHER_JOB_RUNNING",
                    },
                )
            except Exception as error:
                report_error(error)
                return self.respond(500, {"error": "JOB_FAILED"})

    return Handler


def serve():
    """トークンの検証と保存先の初期化を済ませてからHTTPを受け付ける。"""
    worker, reader = (os.environ[k] for k in ("WORKER_TOKEN", "READER_TOKEN"))
    if worker == reader or not all(
        re.fullmatch(r"[a-f0-9]{64}", token) for token in (worker, reader)
    ):
        raise ValueError("64文字の異なるトークンを生成してください。")
    core = core_module()
    # Lakekeeperの健全性はComposeで待機する。S3の初回接続に失敗した場合は
    # 初期化を再試行し、成功するまでHTTPを受け付けない。
    for attempt in range(15):
        try:
            core.bootstrap()
            break
        except Exception as error:
            if attempt == 14:
                raise
            print(
                f"初期化を再試行します（{type(error).__name__}）。", flush=True
            )
            time.sleep(2)
    core.STATE["ready"] = True
    with ThreadingHTTPServer(("0.0.0.0", 8080), handler(core)) as http:
        http.serve_forever()


def operation(action, dataset=None, limit=50):
    """トークンをCLIの引数に載せず、同じコンテナのAPIを呼ぶ。"""
    origin = "http://127.0.0.1:8080"
    if action == "health":
        request = Request(origin + "/health")
    elif action == "gold":
        if dataset not in engine.GOLD:
            raise ValueError("UNKNOWN_DATASET")
        request = Request(
            f"{origin}/v1/gold/{dataset}?limit={limit}",
            headers={"Authorization": "Bearer " + os.environ["READER_TOKEN"]},
        )
    else:
        request = Request(
            origin + "/run",
            data=json.dumps({"action": action}).encode(),
            headers={
                "Authorization": "Bearer " + os.environ["WORKER_TOKEN"],
                "Content-Type": "application/json",
            },
        )
    with urlopen(
        request, timeout=2 if action == "health" else 600
    ) as response:
        return json.load(response)


def main():
    """結果は標準出力へJSONで返し、失敗時は非ゼロで終了する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=["serve", "health", "ingest", "medallion", "verify", "gold"],
    )
    parser.add_argument("dataset", nargs="?")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()
    try:
        if args.action == "serve":
            serve()
        else:
            value = operation(args.action, args.dataset, args.limit)
            if args.action != "health":
                print(json.dumps(value, ensure_ascii=False))
    except Exception as error:
        if args.action == "serve":
            report_error(error)
        elif args.action != "health":
            print(
                f"要求に失敗しました（{type(error).__name__}）。",
                file=sys.stderr,
            )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

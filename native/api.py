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
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

from native import engine


def core_module():
    """実行環境の資格情報を使うストレージを、起動時に読み込む。"""
    from native import storage

    return storage


def json_value(value):
    """小数の精度と日時のタイムゾーンをAPI応答に保持する。"""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError("JSON応答に対応していない型です。")


def ingestion_status(core):
    """観測の欠測と0件、直近の取込と公開中の世代を分けて返す。"""
    last_attempt = core.INGESTION_ATTEMPT
    release = engine.latest_release(core, core.catalog())
    if not release:
        return {"last_attempt": last_attempt, "published": None}
    counts = release.get("row_counts")
    published = {
        "release_snapshot_id": release["release_snapshot_id"],
        "released_at": release["released_at"],
        "gold_rows": None,
        "quarantine_rows": None,
    }
    if counts is not None:
        for layer in ("gold", "quarantine"):
            published[f"{layer}_rows"] = {
                name.split(".", 1)[1]: count
                for name, count in counts.items()
                if name.startswith(f"{layer}.")
            }
    return {"last_attempt": last_attempt, "published": published}


def run_job(core, action, body):
    """書込みを直列化し、正常に完了したジョブを記録する。"""
    if action not in {"verify", "ingest", "medallion", "sql-load"}:
        raise ValueError("INVALID_JOB")
    if not core.LOCK.acquire(blocking=False):
        return None
    track = action in {"ingest", "medallion"}
    before = core.STATE["imported_files"]
    if track:
        attempt = {
            "action": action,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "imported_files": None,
            "skipped_files": None,
            "received_deliveries": None,
            "error": None,
        }
        core.INGESTION_ATTEMPT = attempt
    try:
        if action == "verify":
            from native.verification import verify

            result = verify(
                transform=lambda prefix: engine.medallion(core, prefix),
                run_query=lambda sql, prefix: engine.verify_query(
                    core, sql, prefix
                ),
            )
        elif action == "sql-load":
            from scripts.sql_batch_worker import load

            adapter = SimpleNamespace(
                pa=core.pa,
                CONTRACT=core.CONTRACT,
                catalog=core.catalog,
                ensure_tables=core.ensure_tables,
                medallion=partial(engine.medallion, core),
                S3=core.S3,
                os=os,
            )
            result = load(adapter, body["batch_id"], body["observed_at"])
        else:
            from native.landing import ingest

            result = ingest()
            attempt = {
                **attempt,
                "skipped_files": result["skipped"],
                "received_deliveries": result["received_deliveries"],
            }
            if action == "medallion":
                result.update(engine.medallion(core))
        core.STATE["successful_runs"] += 1
        core.STATE["last_success"] = time.time()
        if track:
            core.INGESTION_ATTEMPT = {
                **attempt,
                "status": "succeeded",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "imported_files": core.STATE["imported_files"] - before,
            }
        return result
    except Exception:
        core.STATE["failed_runs"] += 1
        if track:
            core.INGESTION_ATTEMPT = {
                **attempt,
                "status": "failed",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "imported_files": core.STATE["imported_files"] - before,
                "error": "JOB_FAILED",
            }
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
    """ランタイム状態を使うHTTPハンドラークラスを組み立てる。"""

    class Handler(BaseHTTPRequestHandler):
        """稼働確認、メトリクス、定型ジョブ、スナップショット参照を提供する。"""

        def setup(self):
            """HTTPの要求を読み始める前に、接続の待機時間を制限する。"""
            self.request.settimeout(10)
            super().setup()

        def log_message(self, *_):
            """資格情報や要求行を含めないよう、既定のアクセスログを抑止する。"""

        def authorized(self, setting):
            """用途別のトークンを一定時間で比較する。"""
            supplied = self.headers.get_all("Authorization", [])
            expected = f"Bearer {os.environ[setting]}".encode()
            return len(supplied) == 1 and secrets.compare_digest(
                supplied[0].encode("utf-8"), expected
            )

        def respond(self, status, value, content_type="application/json"):
            """応答を1回だけ直列化し、業務データを共有キャッシュに残さない。"""
            content = (
                json.dumps(value, default=json_value)
                if content_type == "application/json"
                else value
            )
            body = content.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def monitoring(self):
            """業務行を含まない疎通・ジョブの集計値を返す。"""
            if self.path == "/health":
                blocked = core.STATE["blocked"]
                ready = core.STATE["ready"] and not blocked
                return self.respond(
                    200 if ready else 503,
                    {
                        "ready": ready,
                        "blocked": blocked,
                        "busy": core.LOCK.locked(),
                    },
                )
            if self.path == "/openapi.json":
                spec = Path(__file__).with_name("openapi.json")
                return self.respond(
                    200, json.loads(spec.read_text(encoding="utf-8"))
                )
            if self.path == "/metrics":
                samples = []
                for key, value in core.STATE.copy().items():
                    metric = int(value) if isinstance(value, bool) else value
                    samples.append(f"lakehouse_{key} {metric}\n")
                return self.respond(
                    200, "".join(samples), "text/plain; version=0.0.4"
                )
            return self.respond(404, {"error": "UNKNOWN_RESOURCE"})

        def do_GET(self):
            """登録済みのデータだけを、指定された行数の上限まで返す。"""
            try:
                parsed = urlsplit(self.path)
            except ValueError:
                return self.respond(400, {"error": "INVALID_PATH"})
            if not parsed.path.startswith("/v1/"):
                return self.monitoring()
            if not self.authorized("READER_TOKEN"):
                return self.respond(401, {"error": "AUTHENTICATION_REQUIRED"})
            if parsed.path == "/v1/ingestion/status" and not parsed.query:
                try:
                    return self.respond(200, ingestion_status(core))
                except Exception as error:
                    report_error(error)
                    return self.respond(503, {"error": "CATALOG_UNAVAILABLE"})
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

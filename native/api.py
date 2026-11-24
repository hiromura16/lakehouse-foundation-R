"""認証付きの定型ジョブと、読取専用のGoldデータをHTTPで提供する。"""

import argparse
import json
import os
import re
import secrets
import signal
import socket
import sys
import time
import traceback
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

from native import archive, engine


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
        "sensor": release.get("sensor"),
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
    if action not in {"verify", "ingest", "medallion", "sql-load", "archive"}:
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
        elif action == "archive":
            result = archive.receive(core, body["file"])
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

            result = ingest(
                lane="business" if action == "medallion" else "all"
            )
            attempt = {
                **attempt,
                "skipped_files": result["skipped"],
                "received_deliveries": result["received_deliveries"],
            }
            if action == "medallion":
                # 設備の受付・変換を業務の成功条件から分ける。
                def receive_sensor():
                    """独立した設備受付の結果を、ジョブの件数に加える。"""
                    sensor_files = ingest(lane="sensor")
                    result["skipped"] += sensor_files["skipped"]

                result.update(
                    engine.medallion(core, receive_sensor=receive_sensor)
                )
                if result["sensor"]["error"] == "SENSOR_INTAKE_FAILED":
                    result["skipped"] = None
                attempt = {**attempt, "skipped_files": result["skipped"]}
                result["imported"] = core.STATE["imported_files"] - before
        sensor_error = result.get("sensor", {}).get("error")
        if sensor_error:
            core.STATE["partial_runs"] += 1
        else:
            core.STATE["successful_runs"] += 1
            core.STATE["last_success"] = time.time()
        if track:
            core.INGESTION_ATTEMPT = {
                **attempt,
                "status": "partial" if sensor_error else "succeeded",
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "imported_files": core.STATE["imported_files"] - before,
                "error": sensor_error,
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


def report_error(error, *, file=sys.stdout):
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
        file=file,
    )


class GoldAdmission:
    """同時に開始するGoldの参照数と、空きがなく返した応答を記録する。"""

    def __init__(self, limit):
        self.limit = limit
        self.active = 0
        self.rejected = 0
        self.lock = Lock()

    def acquire(self):
        """枠が空いている場合だけ、PyIcebergやDuckDBの処理を許す。"""
        with self.lock:
            if self.active >= self.limit:
                self.rejected += 1
                return False
            self.active += 1
            return True

    def release(self):
        """成功・読み取り失敗・クライアント切断のいずれでも枠を返す。"""
        with self.lock:
            self.active -= 1

    def metrics(self):
        """同じロックで採取したカウンターとゲージを返す。"""
        with self.lock:
            return {
                "gold_active": self.active,
                "gold_limit": self.limit,
                "gold_rejected_total": self.rejected,
            }


def handler(core, *, gold_readers=2):
    """1つのHTTPサーバーが標準・CDC・SQLで同じGold参照枠を共有する。"""
    admission = GoldAdmission(gold_readers)

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

        def respond(
            self,
            status,
            value,
            content_type="application/json",
            *,
            retry_after=None,
        ):
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
            if retry_after is not None:
                self.send_header("Retry-After", str(retry_after))
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
                state = core.STATE.copy() | admission.metrics()
                for key, value in state.items():
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
            if parsed.path == "/v1/data-products" and not parsed.query:
                return self.respond(200, engine.PRODUCTS)
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
                        # 64-bit IDをJavaScriptの整数精度から保護する。
                        for field in ("inputs", "outputs"):
                            value[field] = {
                                name: str(snapshot)
                                if snapshot is not None
                                else None
                                for name, snapshot in value[field].items()
                            }
                        value.pop("sensor_inputs", None)
                        value.pop("sensor_recipe", None)
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
                    max_num_fields=6,
                    keep_blank_values=True,
                    errors="strict",
                )
                if any(len(values) != 1 for values in params.values()):
                    raise ValueError
                limit = int(params.pop("limit", ["50"])[0])
                release = params.pop("release", [None])[0]
                if release is not None and not re.fullmatch(
                    r"[0-9]{1,19}", release
                ):
                    raise ValueError
                release_id = None if release is None else int(release)
                if not 1 <= limit <= 200:
                    raise ValueError
            except ValueError:
                return self.respond(400, {"error": "INVALID_LIMIT"})
            filters = {name: values[0] for name, values in params.items()}
            try:
                predicate = engine.gold_filter(match[2], filters)
            except ValueError:
                return self.respond(400, {"error": "INVALID_FILTER"})
            if not admission.acquire():
                return self.respond(503, {"error": "GOLD_BUSY"}, retry_after=2)
            try:
                try:
                    value = engine.gold_rows(
                        core,
                        match[2],
                        limit,
                        prefix=f"{match[1]}_" if match[1] else "",
                        release_id=release_id,
                        row_filter=predicate,
                    )
                except Exception as error:
                    report_error(error)
                    return self.respond(503, {"error": "GOLD_UNAVAILABLE"})
                if value and filters:
                    value["filters"] = filters
                return self.respond(
                    200 if value else 404,
                    value or {"error": "DATASET_NOT_READY"},
                )
            finally:
                admission.release()

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
                elif action == "archive":
                    if set(body) != {"action", "file"}:
                        raise ValueError
                    archive.verify_name(body["file"])
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
            except archive.ArchiveInputError as error:
                return self.respond(422, {"error": str(error)})
            except Exception as error:
                report_error(error)
                return self.respond(500, {"error": "JOB_FAILED"})

    return Handler


class WorkerHTTPServer(ThreadingHTTPServer):
    """停止時に、受け付け済みのHTTP処理が終わるのを待つ。"""

    daemon_threads = False
    timeout = 0.25

    def __init__(self, address, request_handler, *, stopping):
        self.stopping = stopping
        super().__init__(address, request_handler)

    def verify_request(self, _request, _client_address):
        """待機中に停止を受けた場合は、新しく届いた接続を閉じる。"""
        return not self.stopping()


def serve():
    """トークンの検証と保存先の初期化を済ませてからHTTPを受け付ける。"""
    worker, reader = (os.environ[k] for k in ("WORKER_TOKEN", "READER_TOKEN"))
    if worker == reader or not all(
        re.fullmatch(r"[a-f0-9]{64}", token) for token in (worker, reader)
    ):
        raise ValueError("64文字の異なるトークンを生成してください。")
    archive.configured_limit(os.environ)
    readers = os.environ.get("LAKEHOUSE_GOLD_MAX_CONCURRENT", "2")
    if not re.fullmatch(r"[1-8]", readers):
        raise ValueError("Goldの同時実行数は1〜8を指定してください。")
    stopping = False

    def stop(_signal, _frame):
        """ロックと外部I/Oを使わず、主スレッドに受付停止を知らせる。"""
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    core = core_module()
    # Lakekeeperの健全性はComposeで待機する。S3の初回接続に失敗した場合は
    # 初期化を再試行し、成功するまでHTTPを受け付けない。
    for attempt in range(15):
        if stopping:
            return
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
    if stopping:
        return
    with WorkerHTTPServer(
        ("0.0.0.0", 8080),
        handler(core, gold_readers=int(readers)),
        stopping=lambda: stopping,
    ) as http:
        core.STATE["ready"] = True
        try:
            # SIGTERM後は受付をやめ、with終了時に処理中の要求を待つ。
            while not stopping:
                http.handle_request()
        finally:
            core.STATE["ready"] = False


def operation(
    action, dataset=None, limit=50, *, batch_id=None, observed_at=None
):
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
        body = {"action": action}
        if action == "archive":
            archive.verify_name(dataset)
            body["file"] = dataset
        if action == "sql-load":
            body.update(batch_id=batch_id, observed_at=observed_at)
        request = Request(
            origin + "/run",
            data=json.dumps(body).encode(),
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
        choices=[
            "serve",
            "health",
            "ingest",
            "medallion",
            "verify",
            "gold",
            "archive",
            "sql-load",
        ],
    )
    parser.add_argument(
        "dataset", nargs="?", help="Goldの集計名、または保全するファイル名"
    )
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--batch-id", help="抽出が返したSQLバッチの内容ID")
    parser.add_argument("--observed-at", help="抽出が返したUTCの観測時刻")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="medallionで設備経路に失敗したときも非ゼロで終了する",
    )
    args = parser.parse_args()
    if args.action == "sql-load" and not (args.batch_id and args.observed_at):
        parser.error("sql-loadには --batch-id と --observed-at が必要です。")
    if args.action != "sql-load" and (args.batch_id or args.observed_at):
        parser.error("--batch-id と --observed-at は sql-load で使います。")
    if args.strict and args.action != "medallion":
        parser.error("--strict は medallion で使います。")
    try:
        if args.action == "serve":
            serve()
        else:
            value = operation(
                args.action,
                args.dataset,
                args.limit,
                batch_id=args.batch_id,
                observed_at=args.observed_at,
            )
            if args.action != "health":
                print(json.dumps(value, ensure_ascii=False))
            # 業務世代とエラー情報はAPIで確定済み。自動実行へ部分成功を伝える。
            if args.strict and value.get("sensor", {}).get("error"):
                return 2
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

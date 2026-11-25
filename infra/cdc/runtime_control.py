"""秘密を含まない状態キャッシュを返し、固定CDCの再構築要求を受け付ける。

HTTPスレッドからSQLite、Kafka、Icebergにはアクセスしない。
これらはコンシューマーのメインスレッドが保持し、上限内でバッチを
処理する間にキューにある要求を取り出す。
HTTP 202は当該プロセスのキューへの受付を示す。呼出元は対応する
要求IDの状態を後から取得し、派生表の構築が完了したか確認する。
"""

import copy
import json
import re
import secrets
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ObjectPairs(list):
    """重複するキーを残したまま、JSONのオブジェクトと配列を区別する。"""


def observed_at():
    """ソース時計の詳細を公開せず、観測時刻を整形する。"""
    return datetime.now(timezone.utc).isoformat()


class Control:
    """不変の状態スナップショットと1件の待機要求をスレッド間で受け渡す。"""

    def __init__(self, mode, token, tables):
        """状態確認用の待受を開始する前に、内部認証を必須にする。"""
        if not isinstance(token, str) or not re.fullmatch(
            r"[A-Za-z0-9]{20,128}", token
        ):
            raise ValueError("Configure the internal worker authentication.")
        self.token = token
        self.lock = threading.RLock()
        self.status = {
            "mode": mode,
            "ready": False,
            "blocked": False,
            "busy": False,
            "observation_time": None,
            "last_success": None,
            "error_code": "CDC_STARTING",
            "current_rows": {name: None for name in tables},
            "partitions": [],
            "rebuild": None,
            "raw_events_appended_since_start": 0,
        }

    def snapshot(self):
        """キャッシュ済みの情報だけをコピーする。

        SQLとブローカーへのアクセスはメインスレッドで行う。
        """
        with self.lock:
            return copy.deepcopy(self.status)

    def update(self, **values):
        """余分な項目を受け付けず、メインスレッドの観測結果を公開する。"""
        with self.lock:
            if not values.keys() <= self.status.keys():
                raise ValueError("Unknown CDC status cache field.")
            self.status.update(copy.deepcopy(values))
            self.status["observation_time"] = observed_at()

    def update_partition_read(self, index, offset):
        """読取位置だけを即時更新し、確定・ACKの観測は変更しない。"""
        with self.lock:
            rows = self.status["partitions"]
            if 0 <= index < len(rows):
                rows[index]["last_read_offset"] = offset

    def clear_partition_cursors(self):
        """再割当等で全区画の一時的な観測を同時に未知へ戻す。"""
        with self.lock:
            for row in self.status["partitions"]:
                row["last_read_offset"] = None
                row["broker_ack_next_offset"] = None

    def enqueue(self):
        """再構築要求を1件だけ登録し、競合やクエリ保留時は拒否する。"""
        with self.lock:
            if self.status["mode"] != "fixed-demo":
                return 409, {"error_code": "CDC_REBUILD_NOT_SUPPORTED"}
            if self.status["blocked"]:
                return 409, {"error_code": "CDC_QUERY_REVIEW_REQUIRED"}
            prior = self.status["rebuild"]
            if prior and prior["status"] in {"queued", "running"}:
                return 409, {"error_code": "CDC_REBUILD_BUSY"}
            request_id = secrets.token_hex(16)
            self.status["rebuild"] = {
                "request_id": request_id,
                "status": "queued",
                "completed": False,
                "error_code": None,
                "raw_snapshot_id": None,
            }
            return 202, {
                "accepted": True,
                "request_id": request_id,
                "status": "queued",
                "completed": False,
            }

    def take_rebuild(self):
        """バッチの合間に、メインスレッドが待機要求を1件受け取る。"""
        with self.lock:
            request = self.status["rebuild"]
            if request is None or request["status"] != "queued":
                return None
            request["status"] = "running"
            self.status["busy"] = True
            self.status["ready"] = False
            return request["request_id"]

    def finish_rebuild(
        self, request_id, *, succeeded, snapshot_id=None, error_code=None
    ):
        """受付時と同じ要求IDで、処理が終了した結果を公開する。"""
        with self.lock:
            request = self.status["rebuild"]
            if request is None or request["request_id"] != request_id:
                raise ValueError("CDC rebuild request identity changed.")
            request.update(
                status="succeeded" if succeeded else "failed",
                completed=succeeded,
                error_code=error_code,
                raw_snapshot_id=snapshot_id,
            )
            self.status["busy"] = False
            self.status["observation_time"] = observed_at()

    def dispatch(self, method, path, authorization, body=b""):
        """キューを変更する前に、所定の形式の内部要求を検証する。"""
        if not self.authenticated(authorization):
            return 401, {"error_code": "AUTH_REQUIRED"}
        if method == "GET" and path == "/status":
            return 200, self.snapshot()
        if method != "POST" or path != "/rebuild":
            return 404, {"error_code": "NOT_FOUND"}
        if len(body) > 4096:
            return 400, {"error_code": "INVALID_REBUILD_REQUEST"}
        try:
            pairs = json.loads(body, object_pairs_hook=ObjectPairs)
            # キーと値の組の一覧で重複キーを拒否し、boolの型を厳密に確認して
            # 1とtrueの同一視を避ける。入れ子や任意の設定は受け付けない。
            valid = (
                isinstance(pairs, ObjectPairs)
                and len(pairs) == 1
                and pairs[0][0] == "confirm"
                and pairs[0][1] is True
            )
        except (ValueError, TypeError, IndexError, UnicodeError):
            valid = False
        if not valid:
            return 400, {"error_code": "INVALID_REBUILD_REQUEST"}
        return self.enqueue()

    def authenticated(self, authorization):
        """不正なUnicode入力も考慮し、Bearer認証を安全に比較する。"""
        if not isinstance(authorization, str):
            return False
        try:
            return secrets.compare_digest(
                authorization, "Bearer " + self.token
            )
        except TypeError:
            return False


def start_server(control, *, host="0.0.0.0", port=8001):
    """ホストへポートを公開せず、コンテナ内部の待受を開始する。"""

    class Handler(BaseHTTPRequestHandler):
        """HTTP通信を、単体で検証可能な制御処理へつなぐ。"""

        def setup(self):
            """読取の上限を設け、標準のHTTPヘッダー解析の制限を維持する。"""
            self.request.settimeout(5)
            super().setup()

        def log_message(self, *_):
            """接続元のアドレス、認証情報、要求の本文をログに出さない。"""
            pass

        def handle_request(self):
            """入力に上限を設け、アプリが定義したJSONだけを返す。"""
            try:
                authorization = self.headers.get("Authorization", "")
                if not control.authenticated(authorization):
                    status, payload = 401, {"error_code": "AUTH_REQUIRED"}
                else:
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 <= length <= 4096:
                        raise ValueError("Invalid body length.")
                    status, payload = control.dispatch(
                        self.command,
                        self.path,
                        authorization,
                        self.rfile.read(length),
                    )
            except Exception:
                status, payload = (
                    400,
                    {"error_code": "INVALID_REBUILD_REQUEST"},
                )
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_GET = handle_request
        do_POST = handle_request

    class Server(ThreadingHTTPServer):
        """接続処理をデーモンスレッドに限定し、通信エラーの詳細は出さない。"""

        daemon_threads = True

        def handle_error(self, *_):
            """非公開の接続情報を含み得る通信例外のスタックトレースを出さない。"""
            pass

    server = Server((host, port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server

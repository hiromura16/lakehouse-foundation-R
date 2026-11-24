"""合成のヘルス応答と通知受信を、専用の内部ネットワークで提供する。"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

FAILED = Path("/tmp/fail-probe")
ATTEMPTS = []


class Receiver(BaseHTTPRequestHandler):
    """固定の通知先と件数検査だけを提供する。"""

    def log_message(self, *_args):
        """本文とリクエストの情報をサービスログへ出さない。"""

    def reply(self, code, value=None):
        """通知の本文は出力せず、観測APIの最小項目だけを返す。"""
        self.send_response(code)
        if value is not None:
            self.send_header("Content-Type", "application/json")
        self.end_headers()
        if value is not None:
            self.wfile.write(json.dumps(value).encode())

    def do_GET(self):
        """ヘルス応答を切り替え、合成通知の結果を読み出せるようにする。"""
        if self.path == "/health":
            self.reply(503 if FAILED.exists() else 200)
        elif self.path == "/attempts":
            self.reply(200, ATTEMPTS)
        else:
            self.reply(404)

    def do_POST(self):
        """最初だけ503を返し、その後の発報と復旧を受け入れる。"""
        length = int(self.headers.get("Content-Length", "0"))
        if self.path != "/receive" or not 1 <= length <= 65536:
            self.reply(404)
            return
        if len(ATTEMPTS) >= 12:
            self.reply(507)
            return
        message = json.loads(self.rfile.read(length))
        code = 204 if ATTEMPTS else 503
        ATTEMPTS.append(
            {
                "http_status": code,
                "status": message["status"],
                "receiver": message["receiver"],
                "alerts": [
                    {
                        key: alert[key]
                        for key in ("status", "labels", "fingerprint")
                    }
                    for alert in message["alerts"]
                ],
            }
        )
        self.reply(code)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Receiver).serve_forever()

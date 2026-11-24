"""Accessで承認済みのGoldだけを公開する、読み取り専用の連携入口。"""

import json
import re
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import BoundedSemaphore
from urllib.parse import urlencode
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

import jwt

MAX_RESULT_BYTES = 4 * 1024 * 1024
APPROVED_DATASETS = {
    "factory_daily_production",
    "sales_fulfillment",
    "sensor_latest",
    "sensor_minute",
}


@dataclass(frozen=True)
class Policy:
    """公開する表の固定世代、列名、受入行数の上限。"""

    release: str
    columns: tuple[str, ...]
    max_rows: int


class NoRedirect(HTTPRedirectHandler):
    """上流からのリダイレクト先へ読取資格情報を転送しない。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """HTTPErrorを返すため、次のRequestを作らない。"""


def read_small(path, limit):
    """小さい設定ファイルだけを読み込む。"""
    with Path(path).open("rb") as stream:
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError("EDGE_CONFIG_TOO_LARGE")
    return data


def parse_config(document):
    """認証対象と、外部に出してよい表・列・固定世代を検証する。"""
    team = document["team"]
    audience = document["audience"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", team):
        raise ValueError("EDGE_TEAM_INVALID")
    if not re.fullmatch(r"[0-9a-f]{64}", audience):
        raise ValueError("EDGE_AUDIENCE_INVALID")
    entries = document["datasets"]
    if not isinstance(entries, dict) or not 1 <= len(entries) <= 4:
        raise ValueError("EDGE_POLICY_INVALID")
    policies = {}
    for name, entry in entries.items():
        release = entry["release"]
        columns = entry["columns"]
        maximum = entry["max_rows"]
        if (
            name not in APPROVED_DATASETS
            or not re.fullmatch(r"[1-9][0-9]{0,18}", release)
            or not 0 < int(release) < 2**63
            or type(maximum) is not int
            or not 1 <= maximum <= 200
            or not isinstance(columns, list)
            or not 1 <= len(columns) <= 30
            or not all(
                re.fullmatch(r"[a-z][a-z0-9_]{0,47}", c) for c in columns
            )
            or len(set(columns)) != len(columns)
        ):
            raise ValueError("EDGE_POLICY_INVALID")
        policies[f"/gold/{name}"] = Policy(release, tuple(columns), maximum)
    return f"https://{team}.cloudflareaccess.com", audience, policies


def access_check(issuer, audience, keys=None):
    """固定したチームのJWKSで署名、有効期間、iss、audを検証する。"""
    if keys is None:
        keys = jwt.PyJWKClient(
            issuer + "/cdn-cgi/access/certs", timeout=3, lifespan=300
        )

    def authenticate(token):
        """JWTのヘッダーとクレームを信頼する前に署名を検証する。"""
        signing_key = keys.get_signing_key_from_jwt(token)
        jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=issuer,
            audience=audience,
            options={"require": ["iss", "aud", "exp", "iat"]},
        )

    return authenticate


def reader(token):
    """読取専用トークンを固定した内部APIだけに送る。"""
    client = build_opener(ProxyHandler({}), NoRedirect)

    def fetch(dataset, policy):
        """指定台帳の公開世代と全件条件を確認して承認された列だけ返す。"""
        query = urlencode(
            {"release": policy.release, "limit": policy.max_rows}
        )
        url = f"http://lakehouse:8080/v1/gold/{dataset}?{query}"
        request = Request(url, headers={"Authorization": f"Bearer {token}"})
        with client.open(request, timeout=10) as response:
            payload = response.read(MAX_RESULT_BYTES + 1)
        if len(payload) > MAX_RESULT_BYTES:
            raise ValueError("EDGE_RESPONSE_TOO_LARGE")
        document = json.loads(payload)
        rows = document["rows"]
        if (
            document["release_snapshot_id"] != policy.release
            or document["truncated"] is not False
            or not isinstance(rows, list)
            or len(rows) != document["total_rows"]
            or len(rows) > policy.max_rows
        ):
            raise ValueError("EDGE_RELEASE_INVALID")
        return {
            "dataset": dataset,
            "release": policy.release,
            "rows": [
                {column: row[column] for column in policy.columns}
                for row in rows
            ],
        }

    return fetch


def build_handler(policies, authenticate, fetch):
    """固定経路・署名・読取並列数を切り分けたHTTPハンドラーを作る。"""
    capacity = BoundedSemaphore(4)

    class Handler(BaseHTTPRequestHandler):
        """署名付きの明示的なGETだけで、業務集計の保護された写しを返す。"""

        timeout = 12

        def log_message(self, format, *args):
            """受信ヘッダー、query、業務値をアクセスログへ出さない。"""

        def reply(self, status, body):
            """公開経路では応答を共有キャッシュに保存しない。"""
            payload = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "private, no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)

        def do_GET(self):
            """許可パスとアクセス署名を検証してから内部APIへ問い合わせる。"""
            policy = policies.get(self.path)
            if policy is None:
                self.reply(HTTPStatus.NOT_FOUND, {"error": "NOT_FOUND"})
                return
            proofs = self.headers.get_all("Cf-Access-Jwt-Assertion", [])
            if len(proofs) != 1 or not 1 <= len(proofs[0]) <= 8192:
                self.reply(HTTPStatus.UNAUTHORIZED, {"error": "UNAUTHORIZED"})
                return
            if not capacity.acquire(blocking=False):
                self.reply(HTTPStatus.TOO_MANY_REQUESTS, {"error": "BUSY"})
                return
            try:
                try:
                    authenticate(proofs[0])
                except (jwt.PyJWTError, OSError, ValueError):
                    self.reply(
                        HTTPStatus.UNAUTHORIZED, {"error": "UNAUTHORIZED"}
                    )
                    return
                try:
                    result = fetch(self.path.removeprefix("/gold/"), policy)
                except (OSError, KeyError, TypeError, ValueError):
                    self.reply(
                        HTTPStatus.BAD_GATEWAY,
                        {"error": "RELEASE_UNAVAILABLE"},
                    )
                    return
                self.reply(HTTPStatus.OK, result)
            finally:
                capacity.release()

        def reject_method(self):
            """参照以外のメソッドは上流に到達させない。"""
            self.reply(
                HTTPStatus.METHOD_NOT_ALLOWED, {"error": "METHOD_NOT_ALLOWED"}
            )

        do_HEAD = do_POST = do_PUT = do_DELETE = reject_method
        do_PATCH = do_OPTIONS = do_TRACE = do_CONNECT = reject_method

    return Handler


def main():
    """固定した設定と秘密ファイルを読み、専用ネットワークで待ち受ける。"""
    try:
        issuer, audience, policies = parse_config(
            json.loads(read_small("/run/secrets/edge-policy", 32768))
        )
        token = (
            read_small("/run/secrets/edge-reader-token", 128).decode().strip()
        )
        if not re.fullmatch(r"[a-f0-9]{64}", token):
            raise ValueError("EDGE_READER_INVALID")
    except (OSError, KeyError, AttributeError, TypeError, ValueError):
        raise SystemExit("Cloudflare連携の設定を確認してください。") from None
    handler = build_handler(
        policies, access_check(issuer, audience), reader(token)
    )
    with ThreadingHTTPServer(("0.0.0.0", 8080), handler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()

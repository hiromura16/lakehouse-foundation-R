"""合成RSA署名を利用し、Cloudflare連携の公開境界を試す。"""

import http.client
import json
import threading
import time
import unittest
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from types import SimpleNamespace
from unittest import mock

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from native.edge import gateway

AUDIENCE = "a" * 64
URL_PATH = "/gold/factory_daily_production"


@contextmanager
def server(handler):
    """外部へ公開しないHTTPサーバーを試験の間だけ使用する。"""
    with ThreadingHTTPServer(("127.0.0.1", 0), handler) as service:
        thread = threading.Thread(target=service.serve_forever, daemon=True)
        thread.start()
        try:
            yield service.server_port
        finally:
            service.shutdown()
            thread.join(timeout=3)


class EdgeAcceptance(unittest.TestCase):
    """Accessの署名と、許可したJSONの公開制御を合わせて検査する。"""

    @classmethod
    def setUpClass(cls):
        """このPythonプロセスだけが使用する試験RSA鍵を作成する。"""
        cls.key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048
        )

    def setUp(self):
        """単一の公開表と、ネットワーク不要なJWKS照合を準備する。"""
        self.issuer, audience, self.policies = gateway.parse_config(
            {
                "team": "example",
                "audience": AUDIENCE,
                "datasets": {
                    "factory_daily_production": {
                        "release": "123",
                        "columns": ["factory_id", "good_qty"],
                        "max_rows": 2,
                    }
                },
            }
        )
        keys = mock.Mock()
        keys.get_signing_key_from_jwt.return_value = SimpleNamespace(
            key=self.key.public_key()
        )
        now = int(time.time())
        self.claims = {
            "iss": self.issuer,
            "aud": [AUDIENCE],
            "iat": now - 30,
            "exp": now + 60,
        }
        self.verify = gateway.access_check(self.issuer, audience, keys)

    def request(self, port, method="GET", path=URL_PATH, proofs=()):
        """指定した証明ヘッダーを個別に送り、HTTP状態とJSONを返す。"""
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            client.putrequest(method, path)
            for proof in proofs:
                client.putheader("Cf-Access-Jwt-Assertion", proof)
            client.endheaders()
            response = client.getresponse()
            self.assertEqual(
                response.headers["Cache-Control"], "private, no-store"
            )
            return response.status, json.load(response)
        finally:
            client.close()

    def sign(self, **updates):
        """固定鍵で署名し、有効期限や宛先を試験ごとに差し替える。"""
        claims = {**self.claims, **updates}
        return jwt.encode(claims, self.key, algorithm="RS256")

    def test_scope_and_auth(self):
        """欠損、重複、期限、別aud、書込やqueryを拒否する。"""
        fetch = mock.Mock(return_value={"rows": [{"factory_id": "F01"}]})
        handler = gateway.build_handler(self.policies, self.verify, fetch)
        valid = self.sign()
        with server(handler) as port:
            for headers in (
                (),
                (valid, valid),
                (self.sign(exp=self.claims["iat"]),),
                (self.sign(aud=["b" * 64]),),
                (self.sign(iss="https://other.cloudflareaccess.com"),),
                ("invalid",),
            ):
                self.assertEqual(self.request(port, proofs=headers)[0], 401)
            self.assertEqual(
                self.request(port, path="/run", proofs=(valid,))[0], 404
            )
            self.assertEqual(
                self.request(port, method="POST", proofs=(valid,))[0], 405
            )
            self.assertEqual(
                self.request(
                    port, path=URL_PATH + "?release=1", proofs=(valid,)
                )[0],
                404,
            )
            self.assertEqual(
                self.request(port, proofs=(valid,)),
                (200, {"rows": [{"factory_id": "F01"}]}),
            )
        fetch.assert_called_once()

    def test_reader_rejects_incomplete_generation(self):
        """許可列だけを抜き、行数や公開世代の異なる応答を拒否する。"""
        reply = {
            "release_snapshot_id": "123",
            "rows": [
                {"factory_id": "F01", "good_qty": 12, "private_detail": "demo"}
            ],
            "truncated": False,
            "total_rows": 1,
        }
        policy = self.policies[URL_PATH]
        with mock.patch.object(gateway, "build_opener") as opener:
            stream = (
                opener.return_value.open.return_value.__enter__.return_value
            )
            stream.read.return_value = json.dumps(reply).encode()
            reader = gateway.reader("synthetic")
            expected = [{"factory_id": "F01", "good_qty": 12}]
            self.assertEqual(
                reader("factory_daily_production", policy)["rows"], expected
            )
            reply["truncated"] = True
            stream.read.return_value = json.dumps(reply).encode()
            with self.assertRaisesRegex(ValueError, "EDGE_RELEASE_INVALID"):
                reader("factory_daily_production", policy)


if __name__ == "__main__":
    unittest.main()

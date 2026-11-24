"""承認済みの搬出を、実SDK署名と専用ループバックHTTPSで照合する。"""

import io
import json
import os
import re
import secrets
import ssl
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from hashlib import sha256
from hmac import compare_digest, digest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch

from boto3 import Session
from botocore.config import Config

from native.edge import r2

ACCOUNT = "0" * 32
BUCKET = "private-exports"
PRODUCTION_ORIGIN = f"https://{ACCOUNT}.r2.cloudflarestorage.com"


def verify_signature(request, body):
    """実SDKとは別にSigV4を再計算し、保存条件と合成資格情報を照合する。"""
    headers = request.headers
    auth = re.fullmatch(
        r"AWS4-HMAC-SHA256 Credential=([0-9a-f]+)/([0-9]{8})"
        r"/auto/s3/aws4_request, SignedHeaders=([a-z0-9;-]+), "
        r"Signature=([0-9a-f]{64})",
        headers.get("Authorization", ""),
    )
    if auth is None:
        return False
    key, date, declared, signature = auth.groups()
    names = declared.split(";")
    required = {
        "host",
        "x-amz-date",
        "x-amz-content-sha256",
        "x-amz-security-token",
    }
    if request.command == "PUT":
        required.update({"if-none-match", "x-amz-meta-sha256", "content-type"})
    stamp = headers.get("X-Amz-Date", "")
    token = headers.get("X-Amz-Security-Token", "")
    body_hash = sha256(body).hexdigest()
    signed_hash = headers.get("X-Amz-Content-Sha256", "")
    state = request.server
    if (
        key != state.access_key
        or token != state.session_token
        or not stamp.startswith(date)
        or names != sorted(set(names))
        or not required <= set(names)
        or not all(name in headers for name in names)
        or signed_hash not in {body_hash, "UNSIGNED-PAYLOAD"}
    ):
        return False
    if request.command == "PUT" and (
        headers["If-None-Match"] != "*"
        or headers["X-Amz-Meta-Sha256"] != body_hash
    ):
        return False
    normalized_headers = "".join(
        name
        + ":"
        + ",".join(" ".join(v.split()) for v in headers.get_all(name))
        + "\n"
        for name in names
    )
    # 試験対象のパスはURLエンコード不要の固定文字だけで、クエリーはない。
    canonical = "\n".join(
        (
            request.command,
            request.path,
            "",
            normalized_headers,
            declared,
            signed_hash,
        )
    )
    scope = f"{date}/auto/s3/aws4_request"
    message = "\n".join(
        (
            "AWS4-HMAC-SHA256",
            stamp,
            scope,
            sha256(canonical.encode()).hexdigest(),
        )
    )
    signing_key = ("AWS4" + state.secret_key).encode()
    for component in (date, "auto", "s3", "aws4_request"):
        signing_key = digest(signing_key, component.encode(), "sha256")
    expected = digest(signing_key, message.encode(), "sha256").hex()
    return compare_digest(signature, expected)


class WireHandler(BaseHTTPRequestHandler):
    """S3の作成前提と読取を、合成オブジェクトだけで再現する。"""

    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        """署名、資格情報、業務相当の行を端末へ出さない。"""

    def reply(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def reject(self, status, code):
        self.reply(
            status,
            f"<Error><Code>{code}</Code></Error>".encode(),
            "application/xml",
        )

    def allowed(self, body):
        """署名の検証後だけ、専用バケット内の状態変更を許す。"""
        pattern = (
            rf"/{BUCKET}/exports/[a-z_]+/[0-9]+/"
            r"[a-f0-9]{64}/[A-Za-z0-9_.-]+"
        )
        if re.fullmatch(pattern, self.path) and verify_signature(self, body):
            return True
        self.server.refused += 1
        self.reject(403, "SignatureDoesNotMatch")
        return False

    def do_PUT(self):
        """条件を満たす初回の作成だけ保存し、障害と競合を区別する。"""
        size = int(self.headers.get("Content-Length", "0"))
        if not 1 <= size <= 4096:
            self.close_connection = True
            self.reject(400, "InvalidRequest")
            return
        body = self.rfile.read(size)
        if not self.allowed(body):
            return
        state = self.server
        if state.unavailable_suffix and self.path.endswith(
            state.unavailable_suffix
        ):
            state.events.append(("PUT", self.path, 503))
            self.reject(503, "ServiceUnavailable")
        elif self.path in state.objects:
            status, code = state.existing_error
            state.events.append(("PUT", self.path, status))
            self.reject(status, code)
        else:
            state.objects[self.path] = body
            state.events.append(("PUT", self.path, 200))
            if state.disconnect_suffix and self.path.endswith(
                state.disconnect_suffix
            ):
                # 保存後に応答だけが失われる場合も、SDKに再送させる。
                state.events.append(("disconnect", self.path, 0))
                self.close_connection = True
                return
            self.reply(200, b"", "application/xml")

    def do_GET(self):
        """既存データの照合に使うバイト列をTLS経由で返す。"""
        if not self.allowed(b""):
            return
        data = self.server.objects.get(self.path)
        if data is None:
            self.server.events.append(("GET", self.path, 404))
            self.reject(404, "NoSuchKey")
        else:
            self.server.events.append(("GET", self.path, 200))
            self.reply(200, data, "application/octet-stream")


def certificate(directory, name):
    """使い捨ての秘密鍵と、ループバックIPを含む信頼証明書を生成する。"""
    cert = directory / f"{name}.pem"
    key = directory / f"{name}.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-new",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-noenc",
            "-sha256",
            "-days",
            "1",
            "-subj",
            f"/CN={name}",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
        timeout=20,
    )
    return cert, key


class R2WireAcceptance(unittest.TestCase):
    """外部アカウントを使わず、TLS、SDK署名、承認、確定順序を確認する。"""

    @classmethod
    def setUpClass(cls):
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        root = Path(directory.name)
        cls.cert, cls.key = certificate(root, "foundation-r-r2-synthetic")
        cls.wrong_ca, _ = certificate(root, "foundation-r-r2-wrong-ca")

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.files = {
            "production.csv": b"factory_id,good_qty\nF01,42\n",
            "summary.json": b'{"rows":1}\n',
        }
        for name, content in self.files.items():
            (self.root / name).write_bytes(content)
        self.manifest = self.root / "approved.json"
        self.approval = {
            "format_version": 1,
            "classification": "approved_external",
            "dataset": "factory_daily_production",
            "release": "123",
            "files": [
                {
                    "name": name,
                    "size_bytes": len(content),
                    "sha256": sha256(content).hexdigest(),
                }
                for name, content in self.files.items()
            ],
        }
        self.manifest.write_text(json.dumps(self.approval), encoding="utf-8")
        prefix, _, _ = r2.load_bundle(self.manifest)
        self.keys = {
            name: f"/{BUCKET}/{prefix}/{name}"
            for name in (*self.files, "manifest.json")
        }
        server = ThreadingHTTPServer(("127.0.0.1", 0), WireHandler)
        self.server = server
        self.addCleanup(server.server_close)
        server.access_key = secrets.token_hex(16)
        server.secret_key = secrets.token_hex(32)
        server.session_token = secrets.token_hex(32)
        server.objects = {}
        server.events = []
        server.refused = 0
        server.unavailable_suffix = None
        server.disconnect_suffix = None
        server.existing_error = (412, "PreconditionFailed")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert, self.key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        self.addCleanup(server.shutdown)
        self.origin = f"https://127.0.0.1:{server.server_port}"

    def invoke(self, *, publish=True, ca=None, secret=None):
        """本番のSDK設定を保ち、宛先だけ試験用HTTPSに差し替えてCLIを実行する。"""

        def client(service, **options):
            self.assertEqual(service, "s3")
            self.assertEqual(options["endpoint_url"], PRODUCTION_ORIGIN)
            self.assertEqual(options["region_name"], "auto")
            options["endpoint_url"] = self.origin
            options["config"] = options["config"].merge(Config(proxies={}))
            return Session().client(service, **options)

        args = ["native.edge.r2", str(self.manifest)]
        if publish:
            args.extend(
                ("--publish", "--account", ACCOUNT, "--bucket", BUCKET)
            )
        environment = {
            "AWS_CA_BUNDLE": str(ca or self.cert),
            "AWS_CONFIG_FILE": str(self.root / "unused-aws-config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(self.root / "unused-aws-keys"),
            "AWS_EC2_METADATA_DISABLED": "true",
            "R2_ACCESS_KEY_ID": self.server.access_key,
            "R2_SECRET_ACCESS_KEY": secret or self.server.secret_key,
            "R2_SESSION_TOKEN": self.server.session_token,
        }
        stdout, stderr = io.StringIO(), io.StringIO()
        code = 0
        with (
            patch.dict(os.environ, environment, clear=True),
            patch.object(sys, "argv", args),
            patch("boto3.client", side_effect=client) as factory,
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            try:
                r2.main()
            except SystemExit as exit_:
                code = exit_.code
        return code, stdout.getvalue(), stderr.getvalue(), factory.call_count

    def test_dry_run_and_approval_corruption_send_nothing(self):
        """CLIが承認不成立と検査のみの操作でSDKを生成しないことを確認する。"""
        code, out, _, clients = self.invoke(publish=False)
        self.assertEqual((code, clients), (0, 0))
        self.assertFalse(json.loads(out)["published"])
        original = self.manifest.read_text(encoding="utf-8")
        variants = (
            original.replace("approved_external", "internal"),
            '{"classification":"internal",' + original[1:],
            original.replace('"format_version": 1', '"format_version": true'),
        )
        for value in variants:
            with self.subTest(approval=value[:38]):
                self.manifest.write_text(value, encoding="utf-8")
                code, _, _, clients = self.invoke()
                self.assertEqual((code, clients), (1, 0))
        self.manifest.write_text(original, encoding="utf-8")
        (self.root / "summary.json").write_bytes(b"replaced\n")
        code, _, _, clients = self.invoke()
        self.assertEqual((code, clients), (1, 0))
        self.assertEqual(self.server.events, [])
        self.assertEqual(self.server.objects, {})

    def test_outage_then_retry_and_manifest_last(self):
        """別成果物の障害でmanifestが未確定のまま、同じ承認で完了する。"""
        self.server.unavailable_suffix = "/summary.json"
        code, _, _, clients = self.invoke()
        self.assertEqual((code, clients), (1, 1))
        self.assertEqual(
            set(self.server.objects), {self.keys["production.csv"]}
        )
        self.assertGreaterEqual(
            sum(s == 503 for _, _, s in self.server.events), 2
        )
        self.server.unavailable_suffix = None
        self.server.disconnect_suffix = "/summary.json"
        code, output, _, clients = self.invoke()
        self.assertEqual((code, clients), (0, 1))
        self.assertTrue(json.loads(output)["published"])
        self.assertEqual(
            [
                key
                for method, key, status in self.server.events
                if method == "PUT" and status == 200
            ],
            [self.keys[name] for name in self.keys],
        )
        for name, content in self.files.items():
            self.assertEqual(self.server.objects[self.keys[name]], content)
        self.assertEqual(
            self.server.objects[self.keys["manifest.json"]],
            self.manifest.read_bytes(),
        )
        self.assertIn(
            ("PUT", self.keys["production.csv"], 412), self.server.events
        )
        self.assertIn(
            ("GET", self.keys["production.csv"], 200), self.server.events
        )
        self.assertIn(
            ("disconnect", self.keys["summary.json"], 0), self.server.events
        )
        self.assertIn(
            ("PUT", self.keys["summary.json"], 412), self.server.events
        )
        self.assertIn(
            ("GET", self.keys["summary.json"], 200), self.server.events
        )
        self.assertEqual(self.server.refused, 0)
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(
            sum(s == 200 and m == "PUT" for m, _, s in self.server.events), 3
        )

    def test_different_existing_object_never_commits_manifest(self):
        """412後に異なる実バイト列を読んだとき、更新も後続作成も行わない。"""
        key = self.keys["production.csv"]
        self.server.objects[key] = b"different content"
        code, _, error, clients = self.invoke()
        self.assertEqual((code, clients), (1, 1))
        self.assertIn("ValueError", error)
        self.assertEqual(
            self.server.events, [("PUT", key, 412), ("GET", key, 200)]
        )
        self.assertEqual(self.server.objects, {key: b"different content"})

    def test_bucket_lock_and_other_conflicts(self):
        """R2のロックで確定済み内容だけ照合し、無関係な409では停止する。"""
        self.assertEqual(self.invoke()[0], 0)
        saved = dict(self.server.objects)
        self.server.events.clear()
        self.server.existing_error = (409, "ObjectLockedByBucketPolicy")
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(
            self.server.events,
            [
                event
                for key in self.keys.values()
                for event in (("PUT", key, 409), ("GET", key, 200))
            ],
        )
        self.server.events.clear()
        self.server.existing_error = (409, "Conflict")
        self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(
            self.server.events, [("PUT", self.keys["production.csv"], 409)]
        )
        self.assertEqual(self.server.objects, saved)

    def test_wrong_ca_and_signature_never_write(self):
        """別CAと署名鍵不一致を区別し、どちらでもオブジェクトを作らない。"""
        code, _, error, clients = self.invoke(ca=self.wrong_ca)
        self.assertEqual((code, clients), (1, 1))
        self.assertIn("SSLError", error)
        self.assertEqual(self.server.events, [])
        self.assertEqual(self.server.refused, 0)
        code, _, error, clients = self.invoke(secret=secrets.token_hex(32))
        self.assertEqual((code, clients), (1, 1))
        self.assertIn("ClientError", error)
        self.assertEqual(self.server.refused, 1)
        self.assertEqual(self.server.objects, {})

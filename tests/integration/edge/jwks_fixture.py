"""合成した公開鍵だけをTLSで返す。Cloudflareへの接続は行わない。"""

import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path("/fixture")


class Handler(BaseHTTPRequestHandler):
    """Cloudflare Accessと同じパスを、専用ネットワーク内で使う。"""

    def do_GET(self):
        """鍵の交換を確認するため、要求ごとに固定ファイルを読み取る。"""
        if self.path != "/cdn-cgi/access/certs":
            self.send_error(404)
            return
        data = (ROOT / "keys.json").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    """試験用CAから発行した証明書で、HTTPSサーバーを開く。"""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(ROOT / "server.pem", ROOT / "server-key.pem")
    with ThreadingHTTPServer(("0.0.0.0", 443), Handler) as service:
        service.socket = context.wrap_socket(service.socket, server_side=True)
        service.serve_forever()


if __name__ == "__main__":
    main()

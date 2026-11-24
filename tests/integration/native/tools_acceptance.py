"""運用ツールの入口が認証を要求し、許可した疎通先へ応答するか確認する。"""

import argparse
import base64
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

HEALTH_TARGETS = (
    "lakehouse",
    "grafana",
    "prometheus",
    "catalog",
    "browser",
    "n8n",
    "openobserve",
    "explorer",
    "influxdb",
)


def verify(directory, port):
    """未認証の3入口を拒否し、認証付きの9つの疎通先を確認する。"""
    account = json.loads((directory / "operator.json").read_text())
    raw = (account["user"] + ":" + account["password"]).encode()
    auth = {"Authorization": "Basic " + base64.b64encode(raw).decode()}
    for offset in (0, 1, 3):
        address = f"http://127.0.0.1:{port + offset}/"
        try:
            with urlopen(address, timeout=10):
                raise AssertionError(
                    "運用ツールへの未認証アクセスを許可しました。"
                )
        except HTTPError as error:
            assert error.code == 401, error.code
    origin = f"http://127.0.0.1:{port}"
    for target in HEALTH_TARGETS:
        request = Request(f"{origin}/status/{target}", headers=auth)
        with urlopen(request, timeout=10) as response:
            assert response.status == 200, target
    for path in ("/", "/slides/"):
        with urlopen(
            Request(origin + path, headers=auth), timeout=10
        ) as reply:
            assert "Foundation R" in reply.read().decode(), path
    print("運用ツールの401認証、9件の疎通、ポータルと資料を確認しました。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--port", type=int, default=18506)
    args = parser.parse_args()
    verify(args.directory, args.port)

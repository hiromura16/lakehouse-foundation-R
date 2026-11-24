"""実際のGoldを使い、固定の合成Access発行者で公開制限を検査する。"""

import argparse
import json
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

ISSUER = "https://fixture-team.cloudflareaccess.com"
AUDIENCE = "a" * 64
DATASET = "factory_daily_production"
COLUMNS = ("factory_id", "production_date", "good_qty")


def prepare(root, release):
    """署名鍵、許可ポリシー、正しい証明と拒否用の証明を作る。"""
    now = int(time.time())
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    keyset = jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)
    keyset.update(kid="synthetic-current", alg="RS256", use="sig")
    (root / "keys.json").write_text(json.dumps({"keys": [keyset]}))
    claims = {"iss": ISSUER, "aud": [AUDIENCE], "iat": now, "exp": now + 900}
    cases = {
        "valid": {},
        "wrong_audience": {"aud": ["b" * 64]},
        "expired": {"iat": now - 30, "exp": now - 1},
        "wrong_issuer": {"iss": "https://other.cloudflareaccess.com"},
    }
    tokens = {
        name: jwt.encode(
            {**claims, **changes},
            key,
            algorithm="RS256",
            headers={"kid": "synthetic-current"},
        )
        for name, changes in cases.items()
    }
    (root / "tokens.json").write_text(json.dumps(tokens))
    (root / "policy.json").write_text(
        json.dumps(
            {
                "team": "fixture-team",
                "audience": AUDIENCE,
                "datasets": {
                    DATASET: {
                        "release": release,
                        "columns": list(COLUMNS),
                        "max_rows": 20,
                    }
                },
            }
        )
    )


def check(root, port, *, outage=False):
    """許可した行・列だけが返り、署名鍵の取得失敗時は拒否される。"""
    http = build_opener(ProxyHandler({}))
    tokens = json.loads((root / "tokens.json").read_text())
    gold = json.loads((root / "gold.json").read_text())
    path = f"/gold/{DATASET}"

    def call(token=None, url=path, method="GET"):
        """ヘッダーをログへ表示せず、試験のループバックだけを読む。"""
        headers = {}
        if token:
            headers["Cf-Access-Jwt-Assertion"] = token
        try:
            with http.open(
                Request(
                    f"http://127.0.0.1:{port}{url}",
                    headers=headers,
                    method=method,
                ),
                timeout=15,
            ) as response:
                assert response.headers["Cache-Control"] == "private, no-store"
                return response.status, json.load(response)
        except HTTPError as error:
            with error:
                return error.code, json.load(error)

    if outage:
        assert call(tokens["valid"])[0] == 401
        print("鍵取得不可 401")
        return

    assert call()[0] == 401
    for label in ("wrong_audience", "wrong_issuer", "expired"):
        assert call(tokens[label])[0] == 401
    assert call(tokens["valid"], path + "?release=1")[0] == 404
    assert call(tokens["valid"], "/run", method="POST")[0] == 405
    status, released = call(tokens["valid"])
    assert status == 200, f"許可した読み取りがHTTP {status}を返しました。"
    assert released["release"] == gold["release_snapshot_id"]
    assert released["rows"] == [
        {column: row[column] for column in COLUMNS} for row in gold["rows"]
    ]
    assert len(released["rows"]) == 14
    print("TLS署名検証、対象外・書込の拒否、固定世代14行と3列を照合")


def main():
    """公開の合成入力だけを用い、資格情報や業務値を表示しない。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", type=Path)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--port", type=int)
    parser.add_argument("--outage", action="store_true")
    args = parser.parse_args()
    if args.prepare:
        gold = json.loads((args.stage / "gold.json").read_text())
        prepare(args.stage, gold["release_snapshot_id"])
    else:
        assert args.port
        check(args.stage, args.port, outage=args.outage)


if __name__ == "__main__":
    main()

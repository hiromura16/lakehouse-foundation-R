"""Foundation Rの固定業務契約のハッシュを検証する。"""

import json
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def validate_data_products():
    """公開するデータ製品と、各経路のOpenAPI列挙が揃うことを検査する。"""
    registry = json.loads(
        (ROOT / "contracts/native/data-products.json").read_bytes()
    )
    products = registry["datasets"]
    if registry["format_version"] != 1 or not products:
        raise SystemExit("データ製品の契約が空または不明な版です。")
    for name, product in products.items():
        keys = product["key"]
        if (
            not keys
            or len(keys) != len(set(keys))
            or not product["time_column"]
            or product["time_kind"] not in {"date", "timestamp"}
            or not product["inputs"]
            or not product["owner_role"]
            or "files" not in product["channels"]
        ):
            raise SystemExit(f"データ製品の契約が不正です。対象 {name}")

    spec = json.loads((ROOT / "native/openapi.json").read_bytes())
    for channel, route in (("files", ""), ("cdc", "cdc/"), ("sql", "sql/")):
        schema = spec["paths"][f"/v1/{route}gold/{{dataset}}"]["get"]
        declared = next(
            parameter["schema"]["enum"]
            for parameter in schema["parameters"]
            if parameter.get("name") == "dataset"
        )
        expected = {
            name
            for name, info in products.items()
            if channel in info["channels"]
        }
        if expected != set(declared):
            raise SystemExit(
                f"公開するGoldとAPI仕様が異なります。経路 {channel}"
            )


def main():
    """コミット済みのマニフェストと手元のソースのバイト列を照合する。"""
    path = ROOT / "contracts/native/contract-lock.json"
    manifest = json.loads(path.read_text())
    files = manifest["sha256"]
    for name, recorded in files.items():
        source = (ROOT / name).resolve()
        if not source.is_relative_to(ROOT):
            raise SystemExit("契約マニフェストがリポジトリ外を指しています。")
        if sha256(source.read_bytes()).hexdigest() != recorded:
            raise SystemExit(f"契約内容が変わっています。対象ファイル {name}")
    validate_data_products()
    print(f"契約ファイル{len(files)}件と、公開するデータ製品を照合しました。")


if __name__ == "__main__":
    main()

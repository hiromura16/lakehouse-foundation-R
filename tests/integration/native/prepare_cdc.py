"""既存契約から隔離CDC試験の設定とトピック一覧を作る。"""

import argparse
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def write_private(path, value):
    """試験用ファイルを既存ファイルの上書きなしで作成する。"""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(value + "\n")


def prepare(directory):
    """通常の例から、認証不要な試験ブローカーへ閉じた設定を生成する。"""
    settings = json.loads(
        (ROOT / "native/kafka-settings.example.json").read_text()
    )
    source = settings["source"]
    source["CDC_GROUP_ID"] = "lakehouse-r-native-cdc-ci"
    source["CDC_POLL_SECONDS"] = "1"
    settings["kafka"] = {
        "bootstrap.servers": "test-broker:9092",
        "security.protocol": "PLAINTEXT",
    }
    tables = json.loads(
        (ROOT / "infra/source/sqlserver-tables.example.json").read_text()
    )
    topics = []
    prefix = source["CDC_TOPIC_PREFIX"]
    database = source["CDC_SQLSERVER_DATABASE"]
    for table in tables["tables"]:
        table["source_schema"] = "dbo"
        topics.append(f"{prefix}.{database}.dbo.{table['source_table']}")

    state = directory / "state"
    state.mkdir(mode=0o700)
    config = directory / "settings.json"
    mapping = directory / "map.json"
    write_private(config, json.dumps(settings))
    write_private(mapping, json.dumps(tables))
    write_private(directory / "topics.txt", "\n".join(topics))
    with (directory / "site.env").open("a", encoding="utf-8") as stream:
        stream.write(
            "CDC_ALLOW_PLAINTEXT=1\n"
            f"CDC_STATE_DIR='{state}'\n"
            f"CDC_SETTINGS_FILE='{config}'\n"
            f"CDC_TABLE_MAP_FILE='{mapping}'\n"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    prepare(parser.parse_args().directory.resolve())

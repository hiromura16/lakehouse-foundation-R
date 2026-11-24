"""標準入出力経由のMCPが、公開世代と認証の境界を維持するか検証する。"""

import argparse
import asyncio
import json
import subprocess
import sys
from pathlib import Path

from mcp import Client
from mcp.client.stdio import StdioServerParameters

EXPECTED = {"lakehouse_latest_release", "lakehouse_read_gold"}
SERVER = Path(__file__).resolve().parents[3] / "examples/ai/mcp_server.py"
LANGCHAIN = SERVER.with_name("native_tools.py")


def subprocess_client(token, port, *, channels=None):
    """テスト用の読取資格情報だけをMCP子プロセスに渡す。"""
    options = StdioServerParameters(
        command=sys.executable,
        args=[str(SERVER)],
        env={
            "LAKEHOUSE_API_URL": f"http://127.0.0.1:{port}",
            "LAKEHOUSE_READER_TOKEN": token,
            "http_proxy": "http://127.0.0.1:1",
            "no_proxy": "",
        },
    )
    if channels is not None:
        options.env["LAKEHOUSE_GOLD_CHANNELS"] = channels
    return Client(options, read_timeout_seconds=30)


async def verify(site, port):
    """公開済み合成データを読み、表とトークンの拒否条件も確認する。"""
    async with subprocess_client(site["READER_TOKEN"], port) as client:
        listing = await client.list_tools()
        assert {tool.name for tool in listing.tools} == EXPECTED
        assert all(tool.annotations.read_only_hint for tool in listing.tools)
        latest = await client.call_tool("lakehouse_latest_release", {})
        assert not latest.is_error
        release = latest.structured_content["release_snapshot_id"]
        result = await client.call_tool(
            "lakehouse_read_gold",
            {"dataset": "sensor_minute", "limit": 1, "release": release},
        )
        assert not result.is_error
        data = result.structured_content
        assert data["release_snapshot_id"] == release
        assert (
            data["snapshot_id"]
            == latest.structured_content["outputs"]["gold.sensor_minute"]
        )
        assert data["rows"][0]["good_avg"] == 24.5
        for arguments in (
            {"dataset": "bronze.files"},
            {"dataset": "sensor_minute", "limit": 201},
            {"dataset": "sensor_minute", "release": "unpublished"},
            {"dataset": "sales_fulfillment", "channel": "sql"},
            {"dataset": "sales_fulfillment", "channel": "cdc"},
            {"dataset": "sales_fulfillment", "channel": "other"},
        ):
            assert (
                await client.call_tool("lakehouse_read_gold", arguments)
            ).is_error
    # 標準受入の合成SQLバッチは製品1件を採用済みで、受注Goldは0件。
    async with subprocess_client(
        site["READER_TOKEN"], port, channels="sql"
    ) as client:
        data = {"dataset": "sales_fulfillment", "channel": "sql", "limit": 1}
        latest_sql = await client.call_tool("lakehouse_read_gold", data)
        assert not latest_sql.is_error
        payload = latest_sql.structured_content
        assert payload["table"] == "sql_gold.sales_fulfillment"
        assert payload["rows"] == [] and payload["total_rows"] == 0
        sql_release = payload["release_snapshot_id"]
        assert sql_release != release
        pinned = await client.call_tool(
            "lakehouse_read_gold", {**data, "release": sql_release}
        )
        assert not pinned.is_error and pinned.structured_content == payload
        assert (
            await client.call_tool("lakehouse_latest_release", {})
        ).is_error
        for denied in (
            {"dataset": "sales_fulfillment"},
            {"dataset": "sensor_latest", "channel": "sql"},
            {**data, "release": release},
        ):
            assert (
                await client.call_tool("lakehouse_read_gold", denied)
            ).is_error
    async with subprocess_client(site["WORKER_TOKEN"], port) as client:
        assert (
            await client.call_tool("lakehouse_latest_release", {})
        ).is_error


def verify_langchain(python, site, port):
    """任意環境の実LangChainを、モデルとトレーシングなしで1回ずつ呼ぶ。"""
    env = {
        "LAKEHOUSE_API_URL": f"http://127.0.0.1:{port}",
        "LAKEHOUSE_READER_TOKEN": site["READER_TOKEN"],
        "LANGSMITH_TRACING": "false",
        "http_proxy": "http://127.0.0.1:1",
        "no_proxy": "",
    }
    command = [str(python), str(LANGCHAIN)]
    files = subprocess.check_output(
        [*command, "sensor_minute", "--limit", "1"], env=env, timeout=20
    )
    files = json.loads(files)
    assert files["table"] == "gold.sensor_minute"
    assert files["rows"][0]["good_avg"] == 24.5

    sql_args = ["sales_fulfillment", "--channel", "sql", "--limit", "1"]
    env["LAKEHOUSE_GOLD_CHANNELS"] = "sql"
    sql = json.loads(
        subprocess.check_output([*command, *sql_args], env=env, timeout=20)
    )
    assert sql["table"] == "sql_gold.sales_fulfillment"
    assert sql["rows"] == [] and sql["total_rows"] == 0
    assert sql["release_snapshot_id"] != files["release_snapshot_id"]
    pinned = json.loads(
        subprocess.check_output(
            [*command, *sql_args, "--release", sql["release_snapshot_id"]],
            env=env,
            timeout=20,
        )
    )
    assert pinned == sql
    print("モデルを使わず、LangChainのファイルとSQLの実呼出しを照合しました。")


def main():
    """標準構成の合成受入と同じ設定ファイルから証跡を作る。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--api-port", type=int, required=True)
    parser.add_argument(
        "--langchain-python", type=Path, help="任意のLangChain専用Python"
    )
    args = parser.parse_args()
    site = dict(
        line.split("=", 1)
        for line in args.env_file.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    asyncio.run(verify(site, args.api_port))
    if args.langchain_python is not None:
        verify_langchain(args.langchain_python, site, args.api_port)
    print("MCPで経路の許可、ファイルとSQLの公開世代、読取認証を照合しました。")


if __name__ == "__main__":
    main()

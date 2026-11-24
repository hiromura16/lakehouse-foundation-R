"""標準入出力経由のMCPが、公開世代と認証の境界を維持するか検証する。"""

import argparse
import asyncio
import sys
from pathlib import Path

from mcp import Client
from mcp.client.stdio import StdioServerParameters

EXPECTED = {"lakehouse_latest_release", "lakehouse_read_gold"}
SERVER = Path(__file__).resolve().parents[3] / "examples/ai/mcp_server.py"


def subprocess_client(token, port):
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
        ):
            assert (
                await client.call_tool("lakehouse_read_gold", arguments)
            ).is_error
    async with subprocess_client(site["WORKER_TOKEN"], port) as client:
        assert (
            await client.call_tool("lakehouse_latest_release", {})
        ).is_error


def main():
    """標準構成の合成受入と同じ設定ファイルから証跡を作る。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--api-port", type=int, required=True)
    args = parser.parse_args()
    site = dict(
        line.split("=", 1)
        for line in args.env_file.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    asyncio.run(verify(site, args.api_port))
    print("MCPで公開世代、行の出典、入力制限、読取認証を照合しました。")


if __name__ == "__main__":
    main()

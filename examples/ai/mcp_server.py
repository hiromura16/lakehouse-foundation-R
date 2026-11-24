"""オンプレミスの公開済みGoldを、MCPホストへ標準入出力で渡す。"""

import sys
from typing import Annotated
from urllib.error import URLError

from gold_client import Dataset, GoldClient
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

Limit = Annotated[int, Field(strict=True, ge=1, le=200)]
Release = Annotated[str, Field(pattern=r"^[0-9]{1,19}$")]


def server(client: GoldClient) -> MCPServer:
    """データの出典を返す2種類の固定操作だけを登録する。"""
    mcp = MCPServer("Lakehouse Foundation R")
    read_only = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

    @mcp.tool(annotations=read_only, structured_output=True)
    def lakehouse_latest_release() -> dict[str, object]:
        """最新の公開世代と表のスナップショットIDを返します。"""
        try:
            return client.latest_release()
        except (URLError, TimeoutError, ValueError, KeyError):
            raise ToolError("公開世代を取得できませんでした。") from None

    @mcp.tool(annotations=read_only, structured_output=True)
    def lakehouse_read_gold(
        dataset: Dataset, limit: Limit = 25, release: Release | None = None
    ) -> dict[str, object]:
        """公開済みのGoldと出典を取得します。同じ世代で比較する場合はreleaseを指定します。"""
        try:
            return client.rows(dataset, limit, release)
        except (URLError, TimeoutError, ValueError):
            raise ToolError(
                "指定した公開データを取得できませんでした。"
            ) from None

    return mcp


def main() -> int:
    """設定誤りは標準エラーに返し、プロトコルの標準出力を保つ。"""
    try:
        client = GoldClient(local_only=True)
    except ValueError:
        print(
            "ローカルAPIの接続先と読取トークンを確認してください。",
            file=sys.stderr,
        )
        return 1
    server(client).run(transport="stdio")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

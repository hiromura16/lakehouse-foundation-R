"""Chromeでmarimoの認証、操作、ブラウザー通信を確認する。"""

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import expect, sync_playwright


def main():
    """端末やURLにトークンを出さず、ログインフォームを通して合成画面を確認する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("origin")
    parser.add_argument("token_file", type=Path)
    parser.add_argument("screenshot", type=Path)
    parser.add_argument(
        "--chromium-executable",
        type=Path,
        help="既存のChromiumを使う場合のパス",
    )
    args = parser.parse_args()
    password = args.token_file.read_text(encoding="utf-8").strip()
    requests_outside_origin = set()
    origin = urlsplit(args.origin)

    def block_external(route):
        """隔離ブラウザーに試験対象以外へのリクエストを送らせない。"""
        requests_outside_origin.add(urlsplit(route.request.url).netloc)
        route.abort()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=args.chromium_executable
        )
        context = browser.new_context(viewport={"width": 1180, "height": 830})
        context.route(
            re.compile(rf"^https?://(?!{re.escape(origin.netloc)}(?:/|$))"),
            block_external,
        )
        page = context.new_page()

        def record_socket(socket):
            target = urlsplit(socket.url).netloc
            if target != origin.netloc:
                requests_outside_origin.add(target)

        page.on("websocket", record_socket)
        page.goto(args.origin)
        login = page.locator('input[type="password"]')
        expect(login).to_be_visible()
        expect(page.get_by_text("合成期待値（公開世代なし）")).to_have_count(0)

        def login_post(response):
            return (
                response.request.method == "POST"
                and urlsplit(response.url).path == "/auth/login"
            )

        login.fill("invalid-synthetic-test-value")
        with page.expect_response(login_post) as denied:
            login.press("Enter")
        assert denied.value.status == 200
        expect(login).to_be_visible()
        expect(page.get_by_text("合成期待値（公開世代なし）")).to_have_count(0)
        login.fill(password)
        with page.expect_response(login_post) as allowed:
            login.press("Enter")
        assert allowed.value.status == 302
        expect(
            page.get_by_role("heading", name="工場の日次生産")
        ).to_be_visible(timeout=20_000)
        expect(page.get_by_text("全14行 / 2工場")).to_be_visible()
        expect(page.get_by_text("合成期待値（公開世代なし）")).to_be_visible()
        expect(page.get_by_role("heading", name="F01の結果")).to_be_visible()
        page.get_by_role("combobox").select_option("F02")
        expect(page.get_by_role("heading", name="F02の結果")).to_be_visible()
        summary = "対象日 7日 / 計画 1221 / 良品 1088 / 不良 37"
        expect(page.get_by_text(summary)).to_be_visible()
        expect(page.locator("canvas")).to_be_visible(timeout=10_000)
        edit_status = context.request.get(f"{args.origin}/api/status").status
        assert edit_status in (401, 403, 404), edit_status
        expect(page.get_by_role("button", name="Edit notebook")).to_have_count(
            0
        )
        assert "access_token" not in page.url
        page.screenshot(path=args.screenshot)
        browser.close()

    assert not requests_outside_origin, sorted(requests_outside_origin)
    print(
        json.dumps(
            {
                "unauthenticated": "login",
                "wrong_password": "blocked",
                "authenticated": "14 rows, 2 factories",
                "factory_filter": "F02, 7 days",
                "external_requests": 0,
                "edit_status": edit_status,
                "local_chart": "visible",
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

"""承認済みGoldのRill画面と認証境界をChromiumで確認する。"""

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import expect, sync_playwright


def main():
    """無認証と誤認証を拒否し、認証済み画面に合成KPIが出ることを確認する。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("origin")
    parser.add_argument("password", type=Path)
    parser.add_argument("screenshot", type=Path)
    parser.add_argument(
        "--chromium-executable",
        type=Path,
        help="既存のChromiumを使う場合のパス",
    )
    args = parser.parse_args()
    origin = urlsplit(args.origin)
    password = args.password.read_text(encoding="utf-8").strip()
    foreign_hosts = set()

    def block_external(route):
        foreign_hosts.add(urlsplit(route.request.url).netloc)
        route.abort()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            executable_path=args.chromium_executable
        )
        anonymous = browser.new_context()
        assert anonymous.request.get(args.origin).status == 401
        anonymous.close()
        wrong = browser.new_context(
            http_credentials={"username": "operator", "password": "incorrect"}
        )
        assert wrong.request.get(args.origin).status == 401
        wrong.close()

        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            http_credentials={
                "username": "operator",
                "password": password,
                "origin": args.origin,
            },
        )
        context.route(
            re.compile(rf"^https?://(?!{re.escape(origin.netloc)}(?:/|$))"),
            block_external,
        )
        page = context.new_page()
        response = page.goto(f"{args.origin}/explore/factory_explore")
        assert response.status == 200
        assert (
            "connect-src 'self'" in response.headers["content-security-policy"]
        )
        expect(
            page.get_by_text("工場別日次生産", exact=True).first
        ).to_be_visible(timeout=20_000)
        expect(page.get_by_text("計画数", exact=True).first).to_be_visible()
        expect(
            page.get_by_role("button", name="Select time range")
        ).to_contain_text("All time")
        expect(page.get_by_text("歩留まり", exact=True).first).to_be_visible()
        expect(page.get_by_text("F01", exact=True).first).to_be_visible()
        expect(page.get_by_text("F02", exact=True).first).to_be_visible()
        page.screenshot(path=args.screenshot)
        browser.close()

    assert not foreign_hosts, sorted(foreign_hosts)
    print(
        json.dumps(
            {
                "anonymous": 401,
                "wrong_password": 401,
                "authenticated": 200,
                "factories": ["F01", "F02"],
                "external_attempts": sorted(foreign_hosts),
            }
        )
    )


if __name__ == "__main__":
    main()

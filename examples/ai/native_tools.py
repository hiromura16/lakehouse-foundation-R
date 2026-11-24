"""Expose a snapshot-aware Foundation R read-only tool to LangChain."""

import argparse
import json
import os
from typing import Literal
from urllib.parse import urlencode, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    Request,
    build_opener,
)

from langchain_core.tools import tool

Dataset = Literal[
    "factory_daily_production",
    "sales_fulfillment",
    "sensor_latest",
    "sensor_minute",
]


class NoRedirect(HTTPRedirectHandler):
    """Keep the reader credential on the configured origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Reject any redirect so the credential cannot cross origins."""
        return None


@tool
def lakehouse_gold(
    dataset: Dataset, limit: int = 25, release: str | None = None
) -> dict:
    """Read a published Gold table and return rows plus Iceberg evidence.

    Use the first response's release_snapshot_id in subsequent calls to
    compare data from the same release. The server only accepts registered
    Gold datasets and a maximum of 200 rows per call.
    """
    if dataset not in Dataset.__args__ or not 1 <= limit <= 200:
        raise ValueError("Use a registered dataset and a limit from 1 to 200.")
    origin = os.environ.get("LAKEHOUSE_API_URL", "http://127.0.0.1:18502")
    url = urlsplit(origin)
    if (
        url.username is not None
        or url.password is not None
        or url.query
        or url.fragment
        or url.path not in ("", "/")
        or not url.hostname
        or (
            url.scheme != "https"
            and not (
                url.scheme == "http"
                and url.hostname in ("localhost", "127.0.0.1", "::1")
            )
        )
    ):
        raise ValueError("Use an HTTPS origin or an HTTP loopback endpoint.")
    params = {"limit": str(limit)}
    if release is not None:
        params["release"] = release
    request = Request(
        f"{origin.rstrip('/')}/v1/gold/{dataset}?{urlencode(params)}",
        headers={
            "Authorization": "Bearer " + os.environ["LAKEHOUSE_READER_TOKEN"]
        },
    )
    with build_opener(NoRedirect).open(request, timeout=15) as response:
        return json.load(response)


def main():
    """Run one tool invocation without initializing a model or tracing."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=Dataset.__args__)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument(
        "--release", help="Iceberg release snapshot ID to reuse."
    )
    args = parser.parse_args()
    result = lakehouse_gold.invoke(vars(args))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()

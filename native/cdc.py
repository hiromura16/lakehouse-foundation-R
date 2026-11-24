"""Consume a private Kafka-compatible SQL Server CDC feed with native compute.

The established Debezium decoder, offset guard, raw writer and state machine
remain shared with Foundation. This adapter supplies a different SQL engine
and explicit Kafka client settings. Source registration is an upstream task.
"""

import json
import logging
import os
import re
import sys
from functools import partial
from pathlib import Path
from types import SimpleNamespace

from native import api, engine

ALLOWED_KAFKA = {
    "bootstrap.servers",
    "security.protocol",
    "sasl.mechanism",
    "sasl.username",
    "sasl.password",
    "ssl.ca.location",
}


def settings(path):
    """Read bounded site settings and protect offset-control options."""
    with path.open("rb") as stream:
        document = json.loads(stream.read(64 * 1024 + 1))
    if set(document) != {"source", "kafka", "topic_has_database"}:
        raise ValueError("INVALID_NATIVE_CDC_SETTINGS")
    kafka = document["kafka"]
    if (
        not set(kafka) <= ALLOWED_KAFKA
        or not all(
            isinstance(value, str) and value for value in kafka.values()
        )
        or not kafka.get("bootstrap.servers")
    ):
        raise ValueError("INVALID_KAFKA_SETTINGS")
    protocol = kafka.get("security.protocol")
    if protocol not in {"SSL", "SASL_SSL", "PLAINTEXT"}:
        raise ValueError("KAFKA_SECURITY_PROTOCOL_REQUIRED")
    if (
        protocol == "PLAINTEXT"
        and os.environ.get("CDC_ALLOW_PLAINTEXT") != "1"
    ):
        raise ValueError("PLAINTEXT_REQUIRES_ISOLATED_TEST")
    source = document["source"]
    keys = {
        "CDC_SQLSERVER_HOST",
        "CDC_SQLSERVER_DATABASE",
        "CDC_SQLSERVER_PORT",
        "CDC_TOPIC_PREFIX",
        "CDC_GROUP_ID",
        "CDC_POLL_SECONDS",
        "CDC_BATCH_SIZE",
    }
    if (
        not set(source) <= keys
        or not all(isinstance(value, str) for value in source.values())
        or not re.fullmatch(
            r"lakehouse-r-[A-Za-z0-9_-]+", source.get("CDC_GROUP_ID", "")
        )
        or not source.get("CDC_TOPIC_PREFIX")
        or type(document["topic_has_database"]) is not bool
    ):
        raise ValueError("INVALID_CDC_SOURCE_OR_GROUP")
    return source, kafka, document["topic_has_database"]


def main():
    """Run a single bounded consumer against its dedicated state directory."""
    # The original image uses flat modules. Keep the adapter's own imports
    # separate and use the same controlled path inside both image variants.
    sys.path.insert(0, str(engine.ROOT / "cdc"))
    from config import settings as foundation_settings
    from consumer import run

    logging.disable(logging.CRITICAL)
    core = api.core_module()
    adapter = SimpleNamespace(
        CONTRACT=core.CONTRACT,
        STATE=core.STATE,
        catalog=core.catalog,
        ensure_tables=core.ensure_tables,
        medallion=partial(engine.medallion, core),
    )
    source, kafka, topic_has_database = settings(
        Path("/run/cdc/settings.json")
    )
    run(
        Path("/state"),
        "/run/cdc/table-map.json",
        admin_module=adapter,
        configured_values=foundation_settings(source, require_connector=False),
        kafka_connection=kafka,
        topic_has_database=topic_has_database,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        api.report_error(error)
        raise SystemExit(1) from None

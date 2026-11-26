#!/usr/bin/env python3
"""Foundation R専用CDCの隔離構成と固定契約を照合する。

既存コネクターやトピックを更新・削除せず、観測に失敗したときは停止する。
"""

import argparse
import base64
import json
import os
import re
import stat
import subprocess
import sys
import time
from hashlib import sha256
from pathlib import Path
from tarfile import TarError

from cdc_sql_contract import SCHEMA, TABLES
from cdc_stage3_images import (
    STAGE3_NAMESPACE,
    ImageError,
    inspect_command,
    local_tag,
    require_manifest_descriptor,
    verify_archive,
    verify_engine_inspection,
)

ROOT = Path(__file__).resolve().parents[1]
PROJECT = STAGE3_NAMESPACE
PREFIX = PROJECT
BROKER = "stage3-broker"
CONNECT = "stage3-connect"
CONNECTOR = PREFIX + "-sqlserver"
HISTORY = PREFIX + "-schema-history"
BOOTSTRAP = BROKER + ":9092"
NODE_ID = "31"
SQL_PROPERTIES = "/run/stage3/sql.properties"
TRUSTSTORE = "/run/stage3/sql-truststore.p12"
IMAGE_KEYS = ("KAFKA_IMAGE", "CONNECT_IMAGE")
REQUIRED_KEYS = frozenset(
    (
        *IMAGE_KEYS,
        "CLUSTER_ID",
        "SQL_HOST",
        "SQL_PORT",
        "SQL_DATABASE",
        "SQL_CERT_HOST",
        "SQL_PROPERTIES_FILE",
        "TRUSTSTORE_FILE",
    )
)
ENV_PREFIX = "CDC_STAGE3_"
DIGEST = r"sha256:[a-f0-9]{64}"
REFERENCE = r"[a-z0-9][a-z0-9._/-]*(?::[A-Za-z0-9_.-]+)?@" + DIGEST
IDENT = r"[A-Za-z][A-Za-z0-9_]{0,62}"
DNS = r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?"
CONNECTOR_CLASS = "io.debezium.connector.sqlserver.SqlServerConnector"
BROKER_NATIVE = {
    "__consumer_offsets",
    "__transaction_state",
    "__cluster_metadata",
}
COMPACT = {
    "cleanup.policy": "compact",
    "retention.ms": "-1",
    "retention.bytes": "-1",
}
TARGET_TABLES = {
    "products": "erp_products",
    "work_orders": "erp_work_orders",
    "sales_orders": "erp_sales_orders",
    "shipments": "erp_shipments",
    "equipment_events": "mes_events",
    "equipment_stops": "mes_downtime",
}
HISTORY_SETTINGS = {
    "cleanup.policy": "delete",
    "retention.ms": "-1",
    "retention.bytes": "-1",
}
CDC_SETTINGS = {
    "cleanup.policy": "delete",
    "retention.ms": "259200000",
    "retention.bytes": "-1",
}


class StageError(ValueError):
    """資格情報や接続先を含めない固定診断。"""


def insist(condition, code):
    """不明・不一致を成功と推定しない。"""
    if not condition:
        raise StageError(code)


def private_file(path, code, *, owner=None):
    """実ファイルを明示し、他のユーザーからの読書きを拒む。"""
    insist(path.is_absolute() and path.resolve() == path, code)
    attributes = path.lstat()
    insist(
        stat.S_ISREG(attributes.st_mode)
        and attributes.st_mode & 0o077 == 0
        and attributes.st_uid == (os.getuid() if owner is None else owner),
        code,
    )


def read_settings(path):
    """Composeと同じ、展開のない限定envファイルだけを受理する。"""
    path = path.absolute()
    private_file(path, "PRIVATE_SETTINGS_REQUIRED")
    values = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw or raw.startswith("#"):
            continue
        line = re.fullmatch(r"CDC_STAGE3_([A-Z_]+)=([A-Za-z0-9_.:/@-]+)", raw)
        insist(line is not None, "INVALID_SETTINGS_LINE")
        key, value = line.groups()
        insist(key not in values, "DUPLICATE_SETTINGS_KEY")
        values[key] = value
    insist(values.keys() == REQUIRED_KEYS, "SETTINGS_KEYS_MISMATCH")
    cluster = values["CLUSTER_ID"]
    insist(
        re.fullmatch(r"[A-Za-z0-9_-]{22}", cluster)
        and base64.urlsafe_b64encode(base64.urlsafe_b64decode(cluster + "=="))
        .decode()
        .rstrip("=")
        == cluster
        and base64.urlsafe_b64decode(cluster + "==") != bytes(16),
        "INVALID_CLUSTER_ID",
    )
    for key in ("SQL_HOST", "SQL_CERT_HOST"):
        insist(re.fullmatch(DNS, values[key]), "INVALID_SQL_HOST")
    insist(
        values["SQL_PORT"].isdecimal()
        and 1 <= int(values["SQL_PORT"]) <= 65535,
        "INVALID_SQL_PORT",
    )
    insist(
        values["SQL_HOST"].casefold().rstrip(".")
        not in {"localhost", BROKER, CONNECT},
        "SQL_HOST_NOT_DEDICATED",
    )
    db = values["SQL_DATABASE"]
    insist(
        re.fullmatch(IDENT, db)
        and db.casefold() not in {"master", "model", "msdb", "tempdb"},
        "INVALID_SQL_DATABASE",
    )
    for key in ("SQL_PROPERTIES_FILE", "TRUSTSTORE_FILE"):
        private_file(
            Path(values[key]), "PRIVATE_SQL_INPUT_REQUIRED", owner=1001
        )
    return values


def sequence_map(database):
    """DBを含む正準完全名だけを、既存Landingと検証した六表に対応させる。"""
    insist(TARGET_TABLES.keys() == TABLES.keys(), "CANONICAL_SOURCE_SHAPE")
    path = ROOT / "medallion/sources.json"
    document = json.loads(path.read_bytes(), object_pairs_hook=unique_json)
    targets = {
        item["table"]: set(item["columns"]) for item in document["sources"]
    }
    mapping = {}
    for table, destination in TARGET_TABLES.items():
        columns = {column.name: column.name for column in TABLES[table]}
        insist(
            set(columns) == targets.get(destination), "CANONICAL_TARGET_SHAPE"
        )
        mapping[f"{PREFIX}.{database}.{SCHEMA}.{table}"] = {
            "target_table": destination,
            "source_schema": SCHEMA,
            "source_table": table,
            "columns": columns,
        }
    return mapping


def topic_contract(database):
    """固定6表の正準ルーティングと内部ログ契約を生成する。"""
    topics = {
        PREFIX + "-connect-" + purpose: dict(COMPACT)
        for purpose in ("config", "offset", "status")
    }
    topics[HISTORY] = dict(HISTORY_SETTINGS)
    for name in sequence_map(database):
        topics[name] = dict(CDC_SETTINGS)
    return topics


def connector_contract(values):
    """SQLパスワードをREST・Kafka設定に埋め込まず固定設定を作る。"""
    db = values["SQL_DATABASE"]
    captures = [re.escape(f"{SCHEMA}.{table}") for table in TABLES]
    columns = [
        re.escape(f"{SCHEMA}.{table}.{column.name}")
        for table, items in TABLES.items()
        for column in items
    ]
    return {
        "name": CONNECTOR,
        "connector.class": CONNECTOR_CLASS,
        "tasks.max": "1",
        "database.hostname": values["SQL_HOST"],
        "database.port": values["SQL_PORT"],
        "database.names": db,
        "database.user": "${file:" + SQL_PROPERTIES + ":user}",
        "database.password": "${file:" + SQL_PROPERTIES + ":password}",
        "topic.prefix": PREFIX,
        "table.include.list": ",".join(captures),
        "column.include.list": ",".join(columns),
        "schema.history.internal.kafka.bootstrap.servers": BOOTSTRAP,
        "schema.history.internal.kafka.topic": HISTORY,
        "schema.history.internal.store.only.captured.tables.ddl": "true",
        "schema.history.internal.store.only.captured.databases.ddl": "true",
        "snapshot.mode": "initial",
        "snapshot.isolation.mode": "read_committed",
        "data.query.mode": "function",
        "include.schema.changes": "false",
        "tombstones.on.delete": "true",
        "decimal.handling.mode": "string",
        "time.precision.mode": "adaptive",
        "driver.encrypt": "true",
        "driver.trustServerCertificate": "false",
        "driver.hostNameInCertificate": values["SQL_CERT_HOST"],
        "driver.trustStore": TRUSTSTORE,
        "driver.trustStorePassword": "${file:"
        + SQL_PROPERTIES
        + ":truststorePassword}",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "event.processing.failure.handling.mode": "fail",
        "event.converting.failure.handling.mode": "fail",
    }


def hosted_fixture(environment):
    """一時CIタグは、このプロジェクトの使い捨てランナーだけで許可する。"""
    if (
        environment.get("GITHUB_ACTIONS") != "true"
        or environment.get("RUNNER_ENVIRONMENT") != "github-hosted"
        or environment.get("RUNNER_OS") != "Linux"
        or environment.get("GITHUB_REPOSITORY", "").casefold()
        != "hiromura16/lakehouse-foundation-r"
    ):
        return False
    if environment.get("GITHUB_EVENT_NAME") == "push":
        return True
    if environment.get("GITHUB_EVENT_NAME") != "pull_request":
        return False
    event = json.loads(Path(environment["GITHUB_EVENT_PATH"]).read_bytes())
    return (
        event["pull_request"]["head"]["repo"]["full_name"].casefold()
        == "hiromura16/lakehouse-foundation-r"
    )


def archive_selection(path):
    """外部保管の承認済みreceiptと2イメージのOCI生バイトを照合する。"""
    path = path.absolute()
    private_file(path, "APPROVED_OCI_RECEIPT_REQUIRED")
    with path.open("rb") as stream:
        raw = stream.read(16 * 1024 + 1)
    insist(len(raw) <= 16 * 1024, "OCI_RECEIPT_TOO_LARGE")
    records = json.loads(raw, object_pairs_hook=unique_json)
    insist(
        type(records) is dict and set(records) == {"kafka", "connect"},
        "OCI_RECEIPT_INVALID",
    )
    selected = {}
    for role, record in records.items():
        insist(
            type(record) is dict
            and set(record) == {"path", "archive_sha256", "manifest", "config"}
            and isinstance(record["path"], str)
            and all(
                isinstance(record[key], str)
                and re.fullmatch(DIGEST, record[key])
                for key in ("archive_sha256", "manifest", "config")
            ),
            "OCI_RECEIPT_INVALID",
        )
        archive = Path(record["path"])
        private_file(archive, "PRIVATE_OCI_ARCHIVE_REQUIRED")
        try:
            proof = verify_archive(
                archive,
                record["archive_sha256"][7:],
                record["manifest"],
                record["config"],
            )
        except (ImageError, ValueError, EOFError, TarError) as error:
            raise StageError("OCI_ARCHIVE_NOT_VERIFIED") from error
        insist(
            proof
            == (
                record["archive_sha256"],
                record["manifest"],
                record["config"],
            ),
            "OCI_ARCHIVE_NOT_VERIFIED",
        )
        selected[role.upper() + "_IMAGE"] = {
            "config": record["config"],
            "manifest": record["manifest"],
        }
    return selected


def image_selection(values, environment, *, offline=None):
    """管理registry、承認済みOCI、CI一時aliasの選定を分ける。"""
    selected = {}
    for key in IMAGE_KEYS:
        label = key.removesuffix("_IMAGE").lower()
        reference = values[key]
        prefix = ENV_PREFIX + key.removesuffix("_IMAGE") + "_CI_IMAGE_"
        ci_config = environment.get(prefix + "CONFIG")
        ci_manifest = environment.get(prefix + "MANIFEST")
        if offline is not None:
            insist(not (ci_config or ci_manifest), "OCI_AND_CI_MODE_CONFLICT")
            approved = offline[key]
            config = approved["config"]
            insist(
                reference == local_tag(label, "oci", config),
                "APPROVED_OCI_TAG_MISMATCH",
            )
            selected[key] = approved
        elif ci_config or ci_manifest:
            insist(hosted_fixture(environment), "CI_IMAGE_FORBIDDEN")
            insist(
                ci_config
                and ci_manifest
                and re.fullmatch(DIGEST, ci_config)
                and re.fullmatch(DIGEST, ci_manifest)
                and ci_config != ci_manifest
                and reference == local_tag(label, "ci", ci_config),
                "CI_IMAGE_PROOF_MISMATCH",
            )
            selected[key] = {"config": ci_config, "manifest": ci_manifest}
        else:
            insist(
                re.fullmatch(REFERENCE, reference),
                "IMAGE_NOT_IMMUTABLE",
            )
            selected[key] = None
    return selected


def call(args, environment, *, data=None):
    """引数・応答・標準エラーを出さず、出力を内部で比較する。"""
    try:
        completed = subprocess.run(
            args,
            input=data,
            text=True,
            capture_output=True,
            timeout=30,
            env=environment,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise StageError("STAGE3_COMMAND_TIMEOUT") from error
    except OSError as error:
        raise StageError("STAGE3_COMMAND_UNAVAILABLE") from error
    return completed.returncode, completed.stdout, completed.stderr


def wait_until(test, code, *, seconds=120, delay=2):
    """成功条件だけを待ち、明示異常は直ちに呼出側へ返す。"""
    deadline = time.monotonic() + seconds
    while True:
        if test():
            return
        if time.monotonic() >= deadline:
            raise StageError(code)
        time.sleep(delay)


def java_safe(output):
    """JDK 21の修正レベルと実際のbuildを照合する。"""
    match = re.search(r'\bversion "21\.0\.(\d+)(?:\.(\d+))?"', output)
    build = re.search(r"\(build 21\.0\.\d+(?:\.\d+)?\+(\d+)", output)
    return bool(
        match
        and build
        and (int(match[1]), int(match[2] or "0"), int(build[1])) >= (13, 1, 2)
    )


class Runtime:
    """専用Composeの所有コンテナだけを検証・操作する。"""

    def __init__(self, env_file, values, *, offline=None):
        """Compose変数をシェル環境から上書きさせない。"""
        self.values = values
        self.images = image_selection(values, os.environ, offline=offline)
        self.environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(
                (ENV_PREFIX, "COMPOSE_", "DOCKER_HOST", "DOCKER_CONTEXT")
            )
        }
        self.docker = ["docker", "--context", "default"]
        self.compose = [
            *self.docker,
            "compose",
            "--file",
            str(ROOT / "compose.cdc-stage3.yaml"),
            "--env-file",
            str(env_file.absolute()),
            "--project-name",
            PROJECT,
            "--profile",
            "stage3",
        ]

    def preflight_local(self):
        """OCIでconfigと結び付けたmanifestだけを起動前に許す。"""
        for key, expected in self.images.items():
            insist(expected is not None, "APPROVED_OCI_RECEIPT_REQUIRED")
            code, raw, _ = call(
                inspect_command(self.values[key]), self.environment
            )
            insist(code == 0, "LOADED_OCI_IMAGE_MISMATCH")
            try:
                verify_engine_inspection(raw, expected["manifest"])
            except (ImageError, ValueError) as error:
                raise StageError("LOADED_OCI_IMAGE_MISMATCH") from error

    def container(self, service, key, networks):
        """画像ID、稼動状態、所有ラベルと意図せぬ公開を全て検証する。"""
        code, found, _ = call(
            [*self.compose, "ps", "-q", service], self.environment
        )
        ident = found.strip()
        insist(
            code == 0 and re.fullmatch(r"[0-9a-f]{64}", ident),
            "OWNED_CONTAINER_REQUIRED",
        )
        fields = (
            ".State.Running",
            '.Config.Labels "com.docker.compose.project"',
            '.Config.Labels "com.docker.compose.service"',
            ".Config.Image",
            ".Image",
            ".ImageManifestDescriptor",
            ".HostConfig.PortBindings",
            ".NetworkSettings.Networks",
            ".HostConfig.Privileged",
            ".Mounts",
        )
        templates = [
            "{{json (index " + item + ")}}"
            if " " in item
            else "{{json " + item + "}}"
            for item in fields
        ]
        code, data, _ = call(
            [
                *self.docker,
                "inspect",
                "--type",
                "container",
                "--format",
                "\n".join(templates),
                ident,
            ],
            self.environment,
        )
        insist(code == 0, "CONTAINER_INSPECTION_FAILED")
        parsed = [json.loads(item) for item in data.splitlines()]
        insist(len(parsed) == len(fields), "CONTAINER_INSPECTION_FAILED")
        (
            running,
            project,
            label,
            image,
            image_id,
            image_manifest,
            ports,
            joined,
            privileged,
            mounts,
        ) = parsed
        insist(
            running
            and project == PROJECT
            and label == service
            and image == self.values[key]
            and not ports
            and privileged is False
            and isinstance(joined, dict)
            and set(joined) == {PROJECT + "_" + item for item in networks},
            "CONTAINER_BOUNDARY_MISMATCH",
        )
        check_mounts(service, mounts, self.values)
        local = self.images[key]
        if local:
            # Docker .Image can denote a stored target, not the platform
            # manifest and never the OCI config digest for this engine.
            try:
                require_manifest_descriptor(
                    image_manifest, local["manifest"], platform_required=True
                )
            except (ImageError, ValueError) as error:
                raise StageError("RUNNING_LOCAL_MANIFEST_MISMATCH") from error
        else:
            code, digests, _ = call(
                [
                    *self.docker,
                    "image",
                    "inspect",
                    "--format",
                    "{{json .RepoDigests}}",
                    image_id,
                ],
                self.environment,
            )
            pinned = image.split("@")[1]
            repository = image.split("@")[0]
            if ":" in repository.rsplit("/", 1)[-1]:
                repository = repository.rsplit(":", 1)[0]
            insist(
                code == 0 and repository + "@" + pinned in json.loads(digests),
                "RUNNING_IMAGE_DIGEST_MISMATCH",
            )
        code, uid, _ = self.execute(service, ["id", "-u"])
        insist(
            code == 0
            and re.fullmatch(r"[1-9][0-9]*", uid.strip())
            and (service != CONNECT or uid.strip() == "1001"),
            "RUNTIME_UID_MISMATCH",
        )
        code, output, errors = self.execute(service, ["java", "-version"])
        insist(code == 0 and java_safe(output + errors), "UNPATCHED_JVM")

    def execute(self, service, program, data=None):
        """コマンド文字列のシェル評価なしで専用サービス内だけ実行する。"""
        return call(
            [*self.compose, "exec", "-T", service, *program],
            self.environment,
            data=data,
        )

    def broker(self, command, arguments):
        """専用KRaftアドレスに固定された管理クライアント。"""
        return self.execute(
            BROKER,
            [
                "/opt/kafka/bin/kafka-" + command + ".sh",
                "--bootstrap-server",
                BOOTSTRAP,
                *arguments,
            ],
        )

    def http(self, path, *, post=None):
        """ループバックだけを使い、RESTの応答やエラー本文を公開しない。"""
        params = [
            "curl",
            "-q",
            "--noproxy",
            "*",
            "-sS",
            "--connect-timeout",
            "2",
            "--max-time",
            "10",
            "-w",
            "\n%{http_code}",
            "http://127.0.0.1:8083" + path,
        ]
        data = None
        if post is not None:
            params += [
                "-X",
                "POST",
                "-H",
                "Content-Type: application/json",
                "--data-binary",
                "@-",
            ]
            data = json.dumps(post)
        code, output, _ = self.execute(CONNECT, params, data)
        if code != 0:
            return 0, None
        body, separator, status = output.rpartition("\n")
        insist(separator and status.isdecimal(), "CONNECT_HTTP_INVALID")
        if status in {"200", "201"}:
            return int(status), json.loads(body, object_pairs_hook=unique_json)
        return int(status), None


def check_mounts(service, mounts, values):
    """専用ボリュームと、許可した読取専用2ファイルだけを照合する。"""
    if service == BROKER:
        expected = {
            name: ("volume", PROJECT + "_" + label, True)
            for name, label in (
                ("/var/lib/kafka/data", "broker_log"),
                ("/etc/kafka/secrets", "broker_secrets"),
                ("/mnt/shared/config", "broker_config"),
            )
        }
    else:
        expected = {
            "/kafka/" + kind: ("volume", PROJECT + "_connect_" + kind, True)
            for kind in ("data", "logs", "config")
        }
        expected[SQL_PROPERTIES] = (
            "bind",
            values["SQL_PROPERTIES_FILE"],
            False,
        )
        expected[TRUSTSTORE] = ("bind", values["TRUSTSTORE_FILE"], False)
    actual = {
        mount["Destination"]: (
            mount["Type"],
            mount.get("Source")
            if mount["Type"] == "bind"
            else mount.get("Name"),
            mount["RW"],
        )
        for mount in mounts
    }
    insist(
        len(mounts) == len(actual) and actual == expected,
        "CONTAINER_MOUNTS_MISMATCH",
    )


def unique_json(items):
    """曖昧なREST応答を読み戻し成功と扱わない。"""
    result = dict(items)
    insist(len(result) == len(items), "DUPLICATE_REST_KEY")
    return result


def broker_ready(runtime):
    """稼動中のKRaftクォーラムと期待した専用クラスタIDを確認する。"""
    code, output, _ = runtime.broker(
        "metadata-quorum", ["describe", "--status"]
    )
    if code != 0:
        return False
    cluster = re.findall(r"^ClusterId:\s*(\S+)\s*$", output, re.MULTILINE)
    leader = re.findall(r"^LeaderId:\s*(\d+)\s*$", output, re.MULTILINE)
    insist(cluster == [runtime.values["CLUSTER_ID"]], "WRONG_KRAFT_CLUSTER")
    return leader == [NODE_ID]


def list_topics(runtime, targets):
    """想定外のtopicが一つでもあれば先に中止する。"""
    code, output, _ = runtime.broker("topics", ["--list"])
    insist(code == 0, "TOPIC_LIST_FAILED")
    known = {line for line in output.splitlines() if line}
    insist(known <= targets.keys() | BROKER_NATIVE, "FOREIGN_TOPIC")
    return known


def check_topic(runtime, name, policy):
    """正準の単一partitionとISR、実効保持とcleanup設定を確認する。"""
    code, output, _ = runtime.broker("topics", ["--describe", "--topic", name])
    insist(code == 0, "TOPIC_DESCRIBE_FAILED")
    escaped = re.escape(name)
    summaries = [
        line
        for line in output.splitlines()
        if re.search(r"\bTopic:\s+" + escaped + r"(?:\s|$)", line)
    ]
    insist(
        len(summaries) == 2
        and re.search(r"\bPartitionCount:\s+1(?:\s|$)", summaries[0])
        and re.search(r"\bReplicationFactor:\s+1(?:\s|$)", summaries[0])
        and all(
            re.search(
                r"\b" + key + r":\s+" + value + r"(?:\s|$)", summaries[1]
            )
            for key, value in (
                ("Partition", "0"),
                ("Leader", NODE_ID),
                ("Replicas", NODE_ID),
                ("Isr", NODE_ID),
            )
        ),
        "TOPIC_PARTITION_OR_ISR_MISMATCH",
    )
    code, output, _ = runtime.broker(
        "configs",
        [
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            name,
            "--all",
        ],
    )
    insist(code == 0, "TOPIC_SETTINGS_UNREADABLE")
    for setting, expected in policy.items():
        matches = re.findall(
            r"^\s*" + re.escape(setting) + r"=([^\s]+)(?:\s|$)",
            output,
            re.MULTILINE,
        )
        insist(matches == [expected], "TOPIC_SETTINGS_MISMATCH")


def topics(runtime, *, create):
    """未作成分だけ作成し、全契約を読んでから次工程を許す。"""
    runtime.container(BROKER, "KAFKA_IMAGE", {"broker"})
    wait_until(lambda: broker_ready(runtime), "BROKER_NOT_READY")
    targets = topic_contract(runtime.values["SQL_DATABASE"])
    before = list_topics(runtime, targets)
    # 稼働履歴のあるクラスタで失ったログを初期状態に置き換えない。
    if not targets.keys() <= before:
        insist(not before and create, "TOPICS_INCOMPLETE_NO_REPAIR")
    for name, policy in targets.items():
        if name in before:
            check_topic(runtime, name, policy)
    for name, policy in targets.items():
        if name not in before:
            insist(create, "TOPIC_MISSING")
            code, _, _ = runtime.broker(
                "topics",
                [
                    "--create",
                    "--if-not-exists",
                    "--topic",
                    name,
                    "--partitions",
                    "1",
                    "--replication-factor",
                    "1",
                    *(
                        item
                        for key, val in policy.items()
                        for item in ("--config", f"{key}={val}")
                    ),
                ],
            )
            insist(code == 0, "TOPIC_CREATE_FAILED")
            check_topic(runtime, name, policy)
    after = list_topics(runtime, targets)
    insist(after - BROKER_NATIVE == targets.keys(), "TOPIC_MISSING")
    data_topics = {name for name in after if name.startswith(PREFIX + ".")}
    insist(
        data_topics == sequence_map(runtime.values["SQL_DATABASE"]).keys(),
        "CONSUMER_TOPIC_MAP_MISMATCH",
    )


def connect_ready(runtime):
    """Connect実体とSQL Server plugin版、接続先Kafkaクラスタを照合する。"""
    status, version = runtime.http("/")
    if status in {0, 503}:
        return False
    insist(
        status == 200
        and isinstance(version, dict)
        and version.get("version") == "4.3.1"
        and version.get("kafka_cluster_id") == runtime.values["CLUSTER_ID"],
        "CONNECT_RUNTIME_MISMATCH",
    )
    status, plugins = runtime.http("/connector-plugins")
    insist(status == 200 and isinstance(plugins, list), "PLUGIN_LIST_FAILED")
    selected = [
        item
        for item in plugins
        if isinstance(item, dict) and item.get("class") == CONNECTOR_CLASS
    ]
    insist(
        len(selected) == 1
        and selected[0].get("version") == "3.7.1.Final"
        and selected[0].get("type") == "source",
        "SQLSERVER_PLUGIN_MISMATCH",
    )
    return True


def require_own_connector(runtime):
    """専用workerに他のコネクターがあれば接続しない。"""
    status, found = runtime.http("/connectors")
    insist(
        status == 200 and found in ([], [CONNECTOR]),
        "FOREIGN_OR_UNREADABLE_CONNECTOR",
    )
    return found == [CONNECTOR]


def registered_config(runtime, expected):
    """Kafkaに保管された設定を秘密参照を含め完全一致で確認する。"""
    if not require_own_connector(runtime):
        return False
    status, config = runtime.http("/connectors/" + CONNECTOR + "/config")
    insist(status == 200 and config == expected, "CONNECTOR_CONFIG_DRIFT")
    return True


def connector_ready(runtime):
    """RUNNINGのsourceタスクがちょうど一つであることを確認する。"""
    status, data = runtime.http("/connectors/" + CONNECTOR + "/status")
    if status in {0, 404, 409, 503}:
        return False
    insist(
        status == 200
        and isinstance(data, dict)
        and data.get("name") == CONNECTOR
        and data.get("type") == "source"
        and isinstance(data.get("connector"), dict)
        and isinstance(data.get("tasks"), list),
        "CONNECTOR_STATUS_INVALID",
    )
    tasks = data["tasks"]
    insist(
        all(isinstance(task, dict) for task in tasks),
        "CONNECTOR_STATUS_INVALID",
    )
    states = [data["connector"].get("state"), *(t.get("state") for t in tasks)]
    insist(
        all(
            state in {"RUNNING", "RESTARTING", "UNASSIGNED"}
            for state in states
        ),
        "CONNECTOR_OR_TASK_UNHEALTHY",
    )
    return states == ["RUNNING", "RUNNING"] and tasks[0].get("id") == 0


def register(runtime, *, create):
    """未登録時のPOSTだけを許し、競合時も読み戻して判定する。"""
    topics(runtime, create=False)
    runtime.container(CONNECT, "CONNECT_IMAGE", {"broker", "sql_egress"})
    wait_until(lambda: connect_ready(runtime), "CONNECT_NOT_READY")
    expected = connector_contract(runtime.values)
    if not registered_config(runtime, expected):
        insist(create, "CONNECTOR_MISSING")
        status, _ = runtime.http(
            "/connectors", post={"name": CONNECTOR, "config": expected}
        )
        insist(status in {201, 409}, "CONNECTOR_CREATE_FAILED")
        insist(registered_config(runtime, expected), "CONNECTOR_MISSING")
    wait_until(lambda: connector_ready(runtime), "CONNECTOR_NOT_READY")
    insist(registered_config(runtime, expected), "CONNECTOR_MISSING")


def validate_state(data, values):
    """状態証跡の宛先・全topic ID・LSN・履歴終端が不足なら拒否する。"""
    insist(
        type(data) is dict
        and set(data)
        == {"cluster", "topics", "commit_lsn", "history_end", "broker_state"}
        and data["cluster"] == values["CLUSTER_ID"]
        and type(data["topics"]) is dict
        and data["topics"].keys()
        == topic_contract(values["SQL_DATABASE"]).keys()
        and all(
            type(value) is str and re.fullmatch(r"[A-Za-z0-9_-]{22}", value)
            for value in data["topics"].values()
        )
        and len(set(data["topics"].values())) == len(data["topics"])
        and type(data["commit_lsn"]) is str
        and re.fullmatch(
            r"[0-9a-f]{8}:[0-9a-f]{8}:[0-9a-f]{4}", data["commit_lsn"]
        )
        and int(data["commit_lsn"].replace(":", ""), 16) > 0
        and type(data["history_end"]) is int
        and data["history_end"] > 0
        and valid_broker_proof(data["broker_state"])
        and data["broker_state"]["history"]["high"] == data["history_end"],
        "CONTINUITY_RECEIPT_INVALID",
    )
    return data


def broker_state(runtime):
    """Connectを起動せず、brokerだけで状態実体の指紋を確認できる。"""
    values = runtime.values
    identities = {}
    for name in topic_contract(values["SQL_DATABASE"]):
        code, output, _ = runtime.broker(
            "topics", ["--describe", "--topic", name]
        )
        ids = re.findall(
            r"\bTopic:\s+"
            + re.escape(name)
            + r"\s+TopicId:\s+([A-Za-z0-9_-]{22})(?:\s|$)",
            output,
        )
        insist(code == 0 and len(ids) == 1, "TOPIC_IDENTITY_UNVERIFIED")
        identities[name] = ids[0]
    evidence = {}
    for name, topic in (
        ("history", HISTORY),
        ("offset", PREFIX + "-connect-offset"),
    ):
        code, output, _ = runtime.broker(
            "get-offsets", ["--topic", topic, "--time", "latest"]
        )
        high = re.fullmatch(re.escape(topic) + r":0:([0-9]+)\s*", output)
        insist(code == 0 and high, "DURABLE_TOPIC_END_UNVERIFIED")
        count = int(high[1])
        insist(
            count > 0 and (name != "history" or count <= 64),
            "DURABLE_TOPIC_STATE_EMPTY_OR_EXCEEDS_STAGE3_LIMIT",
        )
        since = "0" if name == "history" else str(count - 1)
        rows = count if name == "history" else 1
        code, payload, _ = runtime.broker(
            "console-consumer",
            [
                "--topic",
                topic,
                "--partition",
                "0",
                "--offset",
                since,
                "--max-messages",
                str(rows),
                "--timeout-ms",
                "12000",
                "--formatter-property",
                "print.key=true",
                "--formatter-property",
                "print.value=true",
            ],
        )
        values = payload.splitlines()
        insist(
            code == 0
            and len(values) == rows
            and all(
                row.rsplit("\t", 1)[-1] not in {"null", ""} for row in values
            ),
            "DURABLE_TOPIC_CONTENT_UNVERIFIED",
        )
        evidence[name] = {
            "high": count,
            "sha256": sha256(payload.encode("utf-8")).hexdigest(),
        }
    return identities, evidence


def valid_broker_proof(proof):
    """証跡の記録形式、件数とハッシュだけを受け入れる。"""
    return (
        type(proof) is dict
        and set(proof) == {"offset", "history"}
        and all(
            type(entry) is dict
            and set(entry) == {"high", "sha256"}
            and type(entry["high"]) is int
            and entry["high"] > 0
            and isinstance(entry["sha256"], str)
            and re.fullmatch(r"[a-f0-9]{64}", entry["sha256"])
            for entry in proof.values()
        )
        and proof["history"]["high"] <= 64
    )


def require_safe_broker_restore(runtime, saved):
    """他のworkerや履歴が動き出す前に、exact復旧点を読取照合する。"""
    code, output, _ = call(
        [*runtime.compose, "ps", "-q", CONNECT], runtime.environment
    )
    insist(code == 0 and not output.strip(), "CONNECT_MUST_NOT_BE_RUNNING")
    topics(runtime, create=False)
    ids, current = broker_state(runtime)
    insist(saved["topics"] == ids, "TOPIC_GENERATION_MISMATCH")
    insist(saved["broker_state"] == current, "BROKER_ARCHIVED_STATE_MISMATCH")


def runtime_state(runtime):
    """起動後には別途RESTで確定SQL位置とソースpartitionを確認する。"""
    values = runtime.values
    identities, durable = broker_state(runtime)
    code, content = runtime.http("/connectors/" + CONNECTOR + "/offsets")
    insist(code == 200 and type(content) is dict, "OFFSET_STATE_UNVERIFIED")
    offsets = content.get("offsets")
    insist(
        type(offsets) is list and len(offsets) == 1, "OFFSET_STATE_UNVERIFIED"
    )
    expected_partition = {"server": PREFIX, "database": values["SQL_DATABASE"]}
    item = offsets[0]
    insist(
        type(item) is dict
        and item.get("partition") == expected_partition
        and type(item.get("offset")) is dict,
        "OFFSET_STATE_UNVERIFIED",
    )
    commit = item["offset"].get("commit_lsn")
    insist(
        type(commit) is str and not item["offset"].get("snapshot"),
        "OFFSET_STATE_UNVERIFIED",
    )
    return validate_state(
        {
            "cluster": values["CLUSTER_ID"],
            "topics": identities,
            "commit_lsn": commit.lower(),
            "history_end": durable["history"]["high"],
            "broker_state": durable,
        },
        values,
    )


def compare_state(baseline, current):
    """同名作り直し、未確定、履歴・LSN巻き戻しを成功にしない。"""
    insist(
        baseline["topics"] == current["topics"], "TOPIC_GENERATION_MISMATCH"
    )
    insist(
        current["commit_lsn"] >= baseline["commit_lsn"]
        and current["history_end"] >= baseline["history_end"],
        "CDC_STATE_REGRESSED",
    )


def output_state(path, document):
    """既存receiptを上書きせず、所有者だけの記録を新規作成する。"""
    path = path.absolute()
    insist(path.parent.resolve() == path.parent, "PRIVATE_STATE_PATH_REQUIRED")
    with os.fdopen(
        os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        ),
        "w",
    ) as stream:
        json.dump(document, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def main():
    """成功メッセージのみを表示し、診断に非公開値を埋め込まない。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--oci-receipt", type=Path)
    parser.add_argument("--state-receipt", type=Path)
    parser.add_argument("--topic-map", type=Path)
    parser.add_argument("--initialize", action="store_true")
    parser.add_argument(
        "operation",
        choices=(
            "plan",
            "verify-local",
            "topics",
            "register",
            "check",
            "capture-state",
            "check-broker",
            "write-map",
        ),
    )
    args = parser.parse_args()
    try:
        values = read_settings(args.env_file)
        offline = (
            archive_selection(args.oci_receipt) if args.oci_receipt else None
        )
        runtime = Runtime(args.env_file, values, offline=offline)
        insist(
            (args.topic_map is not None) == (args.operation == "write-map"),
            "TOPIC_MAP_PATH_OPTION_MISMATCH",
        )
        if args.operation in {"topics", "register"}:
            insist(
                args.initialize and args.state_receipt is None,
                "EXPLICIT_FIRST_INITIALIZATION_REQUIRED",
            )
            if offline is not None:
                runtime.preflight_local()
            if args.operation == "topics":
                topics(runtime, create=True)
            else:
                register(runtime, create=True)
        elif args.operation in {"check", "check-broker", "capture-state"}:
            insist(
                not args.initialize and args.state_receipt is not None,
                "CONTINUITY_RECEIPT_REQUIRED",
            )
            if args.operation != "capture-state":
                path = args.state_receipt.absolute()
                private_file(path, "PRIVATE_STATE_RECEIPT_REQUIRED")
                with path.open("rb") as stream:
                    raw = stream.read(16 * 1024 + 1)
                insist(len(raw) <= 16 * 1024, "STATE_RECEIPT_TOO_LARGE")
                saved = validate_state(
                    json.loads(raw, object_pairs_hook=unique_json), values
                )
            if args.operation == "check-broker":
                require_safe_broker_restore(runtime, saved)
            else:
                register(runtime, create=False)
                current = runtime_state(runtime)
                if args.operation == "check":
                    compare_state(saved, current)
                else:
                    output_state(args.state_receipt, current)
        elif args.initialize or args.state_receipt is not None:
            raise StageError("UNEXPECTED_INITIALIZATION_OR_STATE_OPTION")
        elif args.operation == "verify-local":
            insist(offline is not None, "APPROVED_OCI_RECEIPT_REQUIRED")
            runtime.preflight_local()
        elif args.operation == "write-map":
            mapping = sequence_map(values["SQL_DATABASE"])
            output_state(args.topic_map, {"topics": mapping})
        print(
            json.dumps(
                {
                    "stage3_operation": args.operation,
                    "result": "ok",
                    "fixed_tables": len(TABLES),
                    "topic_count": len(topic_contract(values["SQL_DATABASE"])),
                }
            )
        )
    except StageError as error:
        print(str(error), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        print("STAGE3_PRIVATE_INPUT_OR_RESPONSE_INVALID", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

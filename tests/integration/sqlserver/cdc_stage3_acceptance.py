"""同一リポジトリの一時runnerだけで段階3の実ソースと復旧を検査する。"""

import json
import os
import re
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from secrets import token_hex

import pymssql
from cdc_contract_acceptance import (
    CONTRACT,
    EXPECTED_MAJOR,
    READER,
    configure,
    database_guid,
    database_settings,
    ensure_fixture_capture_started,
    execute,
    jobs,
    put_accounts,
    safe_failure,
    seed_sibling,
    sibling_state,
    typed_fixture,
    wait_for_agent_session,
    wait_for_live_sibling,
)
from cdc_stage3_evidence import (
    DATABASE,
    FIXTURE_IDS,
    SCHEMA,
    FixtureError,
    assert_events,
    check,
    decode_events,
    patched_runtime,
    persisted,
    poll,
    prepare_and_start_worker,
)

ROOT = Path(__file__).resolve().parents[3]
PROJECT = "foundation-r-cdc-stage3"
CONNECTOR = PROJECT + "-sqlserver"
SQL_ALIAS = "stage3-sql"
BROKER = "stage3-broker"
CONNECT = "stage3-connect"
DOCKER = ("docker", "--context", "default")
HISTORY = PROJECT + "-schema-history"
PRODUCTS = f"{PROJECT}.{DATABASE}.{CONTRACT.SCHEMA}.products"


def command(args, *, seconds=25):
    """成功したコマンドのstdoutのみ利用し、生のエラーは表示しない。"""
    result = subprocess.run(  # noqa: S603
        args,
        cwd=ROOT,
        check=False,
        capture_output=True,
        timeout=seconds,
        env=os.environ.copy(),
    )
    if result.returncode:
        raise FixtureError("COMMAND_FAILED")
    return result.stdout


def stage(name):
    """固定の段階名だけ可視化し、次へ到達したことを明らかにする。"""
    print(f"STAGE3_PHASE={name}", flush=True)


def hosted():
    """会社や自宅のdaemonを利用する前に拒否する。"""
    if not (
        os.environ.get("GITHUB_ACTIONS") == "true"
        and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted"
        and os.environ.get("RUNNER_OS") == "Linux"
        and os.environ.get("GITHUB_REPOSITORY", "").casefold()
        == "hiromura16/lakehouse-foundation-r"
    ):
        raise FixtureError("STAGE3_HOSTED_ONLY")


def archives(directory):
    """安全なダイジェスト3種類の証拠だけを受け取る。"""
    check(directory.is_dir())
    found = {}
    for kind in ("kafka", "connect"):
        body = json.loads((directory / (kind + ".json")).read_bytes())
        expected = {
            key: body[key] for key in ("archive_sha256", "manifest", "config")
        }
        check(
            all(
                isinstance(value, str)
                and re.fullmatch(r"sha256:[0-9a-f]{64}", value)
                for value in expected.values()
            )
        )
        found[kind] = expected
    return found


def image_tag(kind, evidence):
    """この試験でだけ生成するアーカイブ由来のローカル別名。"""
    return f"foundation-r-stage3-{kind}:ci-{evidence['config'][7:23]}"


def verified_loaded_image(ref, evidence):
    """検証済みarchiveが結ぶmanifestを、指定CPUのローカル像と照合。"""
    command(
        (
            sys.executable,
            "scripts/cdc_stage3_images.py",
            "inspect-local",
            ref,
            "--manifest",
            evidence["manifest"],
        )
    )


def verify_images(found):
    """検証済みOCIのmanifestを別名から読み出せることを確認。"""
    for kind, evidence in found.items():
        verified_loaded_image(image_tag(kind, evidence), evidence)


def restore_images(directory, found):
    """固定された合成タグのみ削除し、元の検証済みtarだけから読み戻す。"""
    for kind, evidence in found.items():
        tag = image_tag(kind, evidence)
        original = f"foundation-r-stage3-{kind}:archive"
        verified_loaded_image(tag, evidence)
        verified_loaded_image(original, evidence)
        command((*DOCKER, "image", "rm", tag, original))
        result = subprocess.run(
            (
                *DOCKER,
                "image",
                "inspect",
                "--platform",
                "linux/amd64",
                evidence["manifest"],
            ),
            capture_output=True,
            timeout=15,
            check=False,
        )
        check(result.returncode != 0)
        oci = directory / f"{kind}.tar"
        command(
            (
                sys.executable,
                "scripts/cdc_stage3_images.py",
                "archive",
                str(oci),
                "--sha256",
                evidence["archive_sha256"][7:],
                "--manifest",
                evidence["manifest"],
                "--config",
                evidence["config"],
            ),
            seconds=150,
        )
        command((*DOCKER, "image", "load", "--input", str(oci)), seconds=180)
        verified_loaded_image(original, evidence)
        command((*DOCKER, "image", "tag", original, tag))
    verify_images(found)


def prepare_sql(directory, port):
    """段階2のDBA入口と、別系統の番兵を同じ一時SQLだけで利用する。"""
    master = {
        "SOURCE_SQLSERVER_HOST": "localhost",
        "SOURCE_SQLSERVER_PORT": port,
        "SOURCE_SQLSERVER_DATABASE": "master",
        "SOURCE_SQLSERVER_USER": "sa",
        "SOURCE_SQLSERVER_PASSWORD": (directory / "fixture.env")
        .read_text()
        .splitlines()[0]
        .split("=", 1)[1],
        "SOURCE_SQLSERVER_CA_FILE": str(directory / "ca.pem"),
    }

    def probe_sql():
        """接続初期化中だけ一時SQLの肯定応答を待つ。"""
        try:
            return execute(master, "SELECT 1")
        except pymssql.Error:
            return []

    poll(probe_sql, lambda rows: rows == [(1,)], seconds=90)
    check(
        execute(
            master, "SELECT CONVERT(int,SERVERPROPERTY('ProductMajorVersion'))"
        )
        == [(EXPECTED_MAJOR,)]
    )
    execute(master, f"CREATE DATABASE [{DATABASE}]")
    admin = database_settings(master, DATABASE)
    guid = database_guid(admin)
    execute(admin, "EXEC sys.sp_cdc_enable_db")
    wait_for_agent_session(master)
    seed_sibling(admin)
    original_jobs = jobs(admin)
    ensure_fixture_capture_started(admin, original_jobs["capture"][0], guid)
    wait_for_live_sibling(admin)
    old = sibling_state(admin)
    installation = token_hex(16)
    check(
        configure(admin, DATABASE, guid, installation, apply=True) == "CREATED"
    )
    check(
        configure(admin, DATABASE, guid, installation, apply=False)
        == "VERIFIED"
    )
    reader, writer = put_accounts(master, admin)
    check(reader["SOURCE_SQLSERVER_USER"] == READER)
    for table, columns in CONTRACT.TABLES.items():
        target = f"[{CONTRACT.SCHEMA}].[{table}]"
        names = ",".join(f"[{column.name}]" for column in columns)
        arguments = (
            FIXTURE_IDS[0],
            *(
                "stage3 snapshot"
                if column.name == "product_name"
                else typed_fixture(column)
                for column in columns[1:]
            ),
        )
        placeholders = ",".join("%s" for _ in columns)
        execute(
            writer,
            f"INSERT {target} ({names}) VALUES ({placeholders})",
            arguments,
        )
    return admin, reader, writer, old, original_jobs


def product(writer, action, identifier, label=None):
    """許可された1表の合成キーだけを書き込み、外部データには触れない。"""
    check(identifier in FIXTURE_IDS)
    target = f"[{CONTRACT.SCHEMA}].[products]"
    if action == "insert":
        return execute(
            writer,
            f"INSERT {target} (product_id,product_name) VALUES (%s,%s)",
            (identifier, label),
        )
    if action == "update":
        return execute(
            writer,
            f"UPDATE {target} SET product_name=%s WHERE product_id=%s",
            (label, identifier),
        )
    check(action == "delete")
    return execute(
        writer, f"DELETE FROM {target} WHERE product_id=%s", (identifier,)
    )


def create_env(directory, reader, found):
    """秘密を標準出力やconnectorのJSONに入れず、専用ファイルに置く。"""
    env = directory / "stage3.env"
    properties = directory / "stage3-sql.properties"
    truststore = directory / "stage3-private.p12"
    trust_password = token_hex(24)
    properties.write_text(
        f"user={reader['SOURCE_SQLSERVER_USER']}\n"
        f"password={reader['SOURCE_SQLSERVER_PASSWORD']}\n"
        f"truststorePassword={trust_password}\n",
        encoding="ascii",
    )
    properties.chmod(0o600)
    # パスワードは一時ファイルに置き、keytoolの引数やログに値を渡さない。
    password_file = directory / "keytool-pass"
    password_file.write_text(trust_password + "\n", encoding="ascii")
    password_file.chmod(0o600)
    try:
        command(
            (
                "keytool",
                "-importcert",
                "-noprompt",
                "-alias",
                "stage3-sql-ca",
                "-file",
                str(directory / "ca.pem"),
                "-keystore",
                str(truststore),
                "-storetype",
                "PKCS12",
                "-storepass:file",
                str(password_file),
            ),
        )
    finally:
        password_file.unlink()
    truststore.chmod(0o600)
    if any(path.stat().st_uid != 1001 for path in (properties, truststore)):
        # GitHub-hostedのこの2合成ファイルだけ。daemon設定には触れない。
        command(
            (
                "sudo",
                "-n",
                "chown",
                "1001:1001",
                str(properties),
                str(truststore),
            )
        )
    cluster = (
        command(
            (
                *DOCKER,
                "run",
                "--rm",
                "--pull=never",
                "--network",
                "none",
                "--entrypoint",
                "/opt/kafka/bin/kafka-storage.sh",
                image_tag("kafka", found["kafka"]),
                "random-uuid",
            )
        )
        .decode()
        .strip()
        .splitlines()[-1]
    )
    check(re.fullmatch(r"[A-Za-z0-9_-]{22}", cluster) is not None)
    data = {
        "CDC_STAGE3_KAFKA_IMAGE": image_tag("kafka", found["kafka"]),
        "CDC_STAGE3_CONNECT_IMAGE": image_tag("connect", found["connect"]),
        "CDC_STAGE3_CLUSTER_ID": cluster,
        "CDC_STAGE3_SQL_HOST": SQL_ALIAS,
        "CDC_STAGE3_SQL_PORT": "1433",
        "CDC_STAGE3_SQL_DATABASE": DATABASE,
        "CDC_STAGE3_SQL_CERT_HOST": "localhost",
        "CDC_STAGE3_SQL_PROPERTIES_FILE": str(properties),
        "CDC_STAGE3_TRUSTSTORE_FILE": str(truststore),
    }
    env.write_text("".join(f"{key}={value}\n" for key, value in data.items()))
    env.chmod(0o600)
    return env


class Runtime:
    """一致を検査した固定projectの、runner所有の隔離環境だけを扱う。"""

    def __init__(self, private, site, found):
        self.private = private
        self.state = private.with_name("stage3-state-receipt.json")
        self.oci_receipt = None
        self.site = site
        self.cli_env = os.environ | {
            f"CDC_STAGE3_{kind.upper()}_CI_IMAGE_{item.upper()}": evidence[
                item
            ]
            for kind, evidence in found.items()
            for item in ("config", "manifest")
        }
        self.network = PROJECT + "_sql_egress"
        self.attached = False
        self.stack = (
            *DOCKER,
            "compose",
            "-f",
            "compose.cdc-stage3.yaml",
            "--env-file",
            str(private),
            "--project-name",
            PROJECT,
            "--profile",
            "stage3",
        )

    def compose(self, *args, seconds=60):
        """公開しないstdout経由でComposeの結果を検査する。"""
        return command((*self.stack, *args), seconds=seconds)

    def contract(self, action, *, expect=None, seconds=150):
        """本体入口を通り、機密無しの固定拒否だけを証拠にする。"""
        options = ()
        if action in {"topics", "register"}:
            options = ("--initialize",)
        elif action in {"check", "check-broker", "capture-state"}:
            options = ("--state-receipt", str(self.state))
        if self.oci_receipt:
            options += ("--oci-receipt", str(self.oci_receipt))
        result = subprocess.run(  # noqa: S603
            (
                sys.executable,
                "scripts/cdc_stage3.py",
                "--env-file",
                str(self.private),
                *options,
                action,
            ),
            cwd=ROOT,
            check=False,
            capture_output=True,
            timeout=seconds,
            env=self.cli_env,
        )
        output = result.stderr.decode("utf-8", errors="replace").strip()
        fixed = re.fullmatch(r"(?:ERROR: )?([A-Z][A-Z_]{2,65})", output)
        if expect is not None:
            check(result.returncode != 0 and fixed and fixed[1] == expect)
        elif result.returncode:
            if fixed:
                print(f"STAGE3_CONTRACT_ERROR={fixed[1]}", flush=True)
            raise FixtureError("CONTRACT_COMMAND_FAILED")
        return result.stdout

    def select_offline(self, directory, found):
        """保持した正しいOCIの証跡と別名にのみ切り替える。"""
        receipt = self.private.with_name("stage3-oci-receipt.json")
        evidence = {}
        loaded = self.private.read_text().splitlines()
        for kind, record in found.items():
            original = image_tag(kind, record)
            alias = f"foundation-r-stage3-{kind}:oci-{record['config'][7:]}"
            loaded = [
                line.replace("=" + original, "=" + alias) for line in loaded
            ]
            evidence[kind] = record | {"path": str(directory / f"{kind}.tar")}
            command((*DOCKER, "image", "tag", original, alias))
        self.private.write_text("\n".join(loaded) + "\n")
        with receipt.open("x") as stream:
            json.dump(evidence, stream)
        receipt.chmod(0o600)
        self.oci_receipt = receipt
        self.cli_env = os.environ.copy()

    def reject_wrong_offline_image(self, found):
        """別の合成イメージをaliasへ置き、開始前の拒否を実行する。"""
        connect = found["connect"]
        alias = f"foundation-r-stage3-connect:oci-{connect['config'][7:]}"
        command(
            (
                *DOCKER,
                "image",
                "tag",
                image_tag("kafka", found["kafka"]),
                alias,
            )
        )
        try:
            self.contract("verify-local", expect="LOADED_OCI_IMAGE_MISMATCH")
            check(not self.compose("ps", "--all", "-q").strip())
        finally:
            command(
                (*DOCKER, "image", "tag", image_tag("connect", connect), alias)
            )
        self.contract("verify-local")

    def started(self, service):
        """対象外コンテナの同一ID・開始時刻が変わらないことを観測する。"""
        check(service in (BROKER, CONNECT))
        container = self.compose("ps", "-q", service).decode().strip()
        check(re.fullmatch(r"[0-9a-f]{64}", container) is not None)
        mark = (
            command(
                (
                    *DOCKER,
                    "inspect",
                    "--type",
                    "container",
                    "--format",
                    "{{.State.StartedAt}}",
                    container,
                )
            )
            .decode()
            .strip()
        )
        check(re.fullmatch(r"[0-9TZ:.+-]+", mark) is not None)
        return container, mark

    def attach(self):
        """新規合成SQLだけを、Connectの専用ネットワークに追加する。"""
        command(
            (
                *DOCKER,
                "network",
                "connect",
                "--alias",
                SQL_ALIAS,
                self.network,
                self.site,
            )
        )
        self.attached = True

    def down(self, *, volumes=False):
        """ホストやincumbentに触れず、この環境で作った範囲だけを片付ける。"""
        if self.attached:
            command(
                (*DOCKER, "network", "disconnect", self.network, self.site)
            )
            self.attached = False
        args = ("--volumes",) if volumes else ()
        self.compose("down", *args)

    def up(self, *, pristine):
        """復旧時にはtopicやconnectorを新規に設定し直さない。"""
        self.compose("up", "-d", "--pull", "never", BROKER)
        # 復旧時に既存connectorが直ちにSQLへ接続する前に証跡と経路を確認。
        prepare_and_start_worker(
            self.compose,
            lambda: self.contract("topics" if pristine else "check-broker"),
            self.attach,
            CONNECT,
        )
        self.contract("register" if pristine else "check")

    def exercise_failed_start(self):
        """入口で終了する合成workerだけを削除し、正規データへ触れさせない。"""
        self.compose("up", "-d", "--pull", "never", BROKER)
        self.contract("check-broker")
        broker = self.started(BROKER)
        extra = self.private.with_name("stage3-failed-start.yaml")
        with extra.open("x", encoding="utf-8") as stream:
            stream.write(
                "services:\n"
                "  stage3-connect:\n"
                '    entrypoint: ["/bin/sh", "-c", "exit 42"]\n'
            )
        extra.chmod(0o600)
        prefix = (
            *self.stack,
            "-f",
            str(extra),
        )
        # 正常なcomposeのworkerとは別のfixture-only定義。同じ承認済みimageで
        # 開始直後に終了し、本物のconnectorやSQL connectionsを開始しない。
        command(
            (*prefix, "up", "-d", "--no-deps", "--pull", "never", CONNECT),
            seconds=75,
        )
        owned = self.compose("ps", "--all", "-q", CONNECT).decode().strip()
        check(re.fullmatch(r"[0-9a-f]{64}", owned) is not None)

        def failed():
            """対象の所有ラベルと実終了状態をraw logなしで確認。"""
            fields = (
                '{{index .Config.Labels "com.docker.compose.project"}} '
                '{{index .Config.Labels "com.docker.compose.service"}} '
                "{{.State.Status}} {{.State.ExitCode}}"
            )
            seen = (
                command(
                    (
                        *DOCKER,
                        "inspect",
                        "--type",
                        "container",
                        "--format",
                        fields,
                        owned,
                    )
                )
                .decode()
                .split()
            )
            check(len(seen) == 4 and seen[:2] == [PROJECT, CONNECT])
            return seen[2:]

        poll(failed, lambda state: state == ["exited", "42"], seconds=20)
        command((*DOCKER, "rm", owned))
        check(self.started(BROKER) == broker)
        self.contract("check-broker")

    def rest(self, resource):
        """固定されたループバックの読み取り専用RESTへ問い合わせる。"""
        check(resource in ("/", f"/connectors/{CONNECTOR}/offsets"))
        return json.loads(
            self.compose(
                "exec",
                "-T",
                CONNECT,
                "curl",
                "-q",
                "--noproxy",
                "*",
                "--max-time",
                "10",
                "-fsS",
                f"http://localhost:8083{resource}",
            )
        )

    def high(self, topic):
        """専用の一partitionに限り、brokerの実終端を求める。"""
        check(
            topic == HISTORY
            or topic
            in {
                f"{PROJECT}.{DATABASE}.{SCHEMA}.{table}"
                for table in CONTRACT.TABLES
            }
        )
        payload = self.compose(
            "exec",
            "-T",
            BROKER,
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            f"{BROKER}:9092",
            "--topic",
            topic,
            "--time",
            "latest",
        )
        match = re.fullmatch(
            re.escape(topic) + r":0:([0-9]+)\s*", payload.decode()
        )
        check(match is not None)
        return int(match[1])

    def state_evidence(self):
        """専用topic実体とschema historyの既存recordを値非公開で照合する。"""
        ids = []
        for topic in (PRODUCTS, HISTORY, PROJECT + "-connect-offset"):
            result = self.compose(
                "exec",
                "-T",
                BROKER,
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                f"{BROKER}:9092",
                "--topic",
                topic,
                "--describe",
            ).decode()
            expected = (
                re.escape(topic) + r"\s+TopicId:\s+([A-Za-z0-9_-]{22})(?:\s|$)"
            )
            topics = re.findall(r"\bTopic:\s+" + expected, result)
            check(len(topics) == 1)
            ids.extend(topics)
        high = self.high(HISTORY)
        check(0 < high <= 64)
        records = self.compose(
            "exec",
            "-T",
            BROKER,
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            f"{BROKER}:9092",
            "--topic",
            HISTORY,
            "--partition",
            "0",
            "--offset",
            "earliest",
            "--max-messages",
            str(high),
            "--timeout-ms",
            "15000",
        )
        check(len(records.splitlines()) == high)
        return tuple(ids), sha256(records).hexdigest()

    def high_products(self):
        """試験の固定データtopicの終端だけを照会する。"""
        return self.high(PRODUCTS)

    def prove_all_snapshots(self):
        """各物理topicの実recordが正準六表のDB/スキーマ/表/キーと一致。"""
        for table, columns in CONTRACT.TABLES.items():
            topic = f"{PROJECT}.{DATABASE}.{SCHEMA}.{table}"
            count = poll(
                lambda: self.high(topic), lambda number: number > 0, seconds=90
            )
            check(count == 1)
            key, sep, body = self.read_data(topic, 1).strip().partition(b"\t")
            check(sep == b"\t")
            identity = json.loads(key)
            record = json.loads(body)
            after = record["after"]
            pk = columns[0].name
            expected_columns = {column.name for column in columns}
            check(
                identity == {pk: FIXTURE_IDS[0]}
                and record["op"] == "r"
                and record["before"] is None
                and type(after) is dict
                and set(after) == expected_columns
                and after[pk] == FIXTURE_IDS[0]
                and record["source"]["db"] == DATABASE
                and record["source"]["schema"] == SCHEMA
                and record["source"]["table"] == table
            )

    def events(self, count):
        """決めた合成topicを順序付きで読み戻し、生payloadは公開しない。"""
        return decode_events(self.read_data(PRODUCTS, count), count)

    def read_data(self, topic, count):
        """明示した合成topicの実データを閉じたrunner内だけで取得。"""
        check(1 <= count <= 32)
        return self.compose(
            "exec",
            "-T",
            BROKER,
            "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server",
            f"{BROKER}:9092",
            "--topic",
            topic,
            "--partition",
            "0",
            "--offset",
            "earliest",
            "--max-messages",
            str(count),
            "--timeout-ms",
            "15000",
            "--formatter-property",
            "print.key=true",
            "--formatter-property",
            "print.value=true",
        )

    def offsets(self):
        """LSN付きの永続オフセットが実際に復元できることを照合する。"""
        offsets = self.rest(f"/connectors/{CONNECTOR}/offsets")["offsets"]
        check(type(offsets) is list and len(offsets) <= 1)
        if offsets:
            saved = offsets[0]
            check(
                type(saved["partition"]) is dict
                and type(saved["offset"]) is dict
            )
            lsn = saved["offset"].get("commit_lsn")
            check(
                isinstance(lsn, str)
                and bool(re.fullmatch(r"[0-9a-fA-F:]+", lsn))
            )
        return offsets

    def java(self, service):
        """稼働JVMのexeとJDKを確認し、UID・制限値だけを受入証跡にする。"""
        check(service in (BROKER, CONNECT))
        version = self.compose(
            "exec",
            "-T",
            service,
            "sh",
            "-ec",
            "for exe in /proc/[0-9]*/exe; do "
            'candidate=$(readlink "$exe" 2>/dev/null) || continue; '
            'if test "$candidate" = "$JAVA_HOME/bin/java"; then '
            'exec "$JAVA_HOME/bin/java" -XshowSettings:properties '
            "-version 2>&1; fi; "
            "done; exit 1",
        ).decode()
        full = re.findall(r"(?m)^\s*java.runtime.version = (\S+)\s*$", version)
        check(len(full) == 1 and patched_runtime(full[0]))
        uid = self.compose("exec", "-T", service, "id", "-u").decode().strip()
        check(uid in ("1000", "1001"))
        container = self.compose("ps", "-q", service).decode().strip()
        check(re.fullmatch(r"[0-9a-f]{64}", container) is not None)
        raw = (
            command(
                (
                    *DOCKER,
                    "inspect",
                    "--type",
                    "container",
                    "--format",
                    "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}",
                    container,
                )
            )
            .decode()
            .split()
        )
        check(raw == [str(2 * 1024**3), str(2 * 10**9)])
        print(
            f"STAGE3_RUNTIME={service} JAVA={full[0]} UID={uid} "
            f"MEMORY={raw[0]} CPU_NANO={raw[1]}",
            flush=True,
        )


def verify(stage_dir, port, site, directory):
    """同じ専用stateと合成SQLを使い、実変更が復旧後も届くことを確認する。"""
    stage("SQL2022_CONTRACT")
    admin, reader, writer, sibling, capture_jobs = prepare_sql(stage_dir, port)
    found = archives(directory)
    verify_images(found)
    runtime = Runtime(create_env(stage_dir, reader, found), site, found)
    check(not runtime.compose("ps", "--all", "-q").strip())
    for noun in ("volume", "network"):
        old = command(
            (
                *DOCKER,
                noun,
                "ls",
                "--filter",
                "label=com.docker.compose.project=" + PROJECT,
                "-q",
            )
        )
        check(not old.strip())
    phase = "RUNTIME_START"
    failed = False
    try:
        stage(phase)
        runtime.up(pristine=True)
        runtime.java(BROKER)
        runtime.java(CONNECT)
        broker_version = (
            runtime.compose(
                "exec",
                "-T",
                BROKER,
                "/opt/kafka/bin/kafka-topics.sh",
                "--version",
            )
            .decode()
            .strip()
        )
        check(
            re.fullmatch(r"4\.3\.2(?: \(Commit:[a-f0-9]+\))?", broker_version)
        )
        check(runtime.rest("/")["version"] == "4.3.1")
        print("STAGE3_VERSIONS=KAFKA_4_3_2_CONNECT_4_3_1", flush=True)
        phase = "REGISTRATION_IDEMPOTENT"
        stage(phase)
        runtime.contract("register")

        phase = "SNAPSHOT"
        stage(phase)
        runtime.prove_all_snapshots()
        expected = [(FIXTURE_IDS[0], "r", None, "stage3 snapshot")]
        assert_events(runtime, expected)
        initial_offset = persisted(runtime, None)
        initial_history = runtime.high(HISTORY)
        check(initial_history > 0)

        phase = "STREAM_INSERT_UPDATE_DELETE"
        stage(phase)
        key = FIXTURE_IDS[1]
        product(writer, "insert", key, "stage3 insert")
        expected.append((key, "c", None, "stage3 insert"))
        assert_events(runtime, expected)
        product(writer, "update", key, "stage3 update")
        expected.append((key, "u", "stage3 insert", "stage3 update"))
        assert_events(runtime, expected)
        product(writer, "delete", key)
        expected.extend(
            ((key, "d", "stage3 update", None), (key, "t", None, None))
        )
        commit = assert_events(runtime, expected)
        offset = persisted(runtime, initial_offset, commit=commit)
        check(runtime.high(HISTORY) >= initial_history)
        history = runtime.state_evidence()
        runtime.contract("capture-state")
        runtime.contract("check")

        for target in (CONNECT, BROKER):
            phase = (
                "CONNECT_RESTART_CONTINUITY"
                if target == CONNECT
                else "BROKER_RESTART_CONTINUITY"
            )
            stage(phase)
            other = BROKER if target == CONNECT else CONNECT
            neighbor = runtime.started(other)
            runtime.compose("restart", target)
            runtime.contract("check")
            check(runtime.started(other) == neighbor)
            check(
                runtime.offsets() == offset
                and runtime.state_evidence() == history
            )
        key = FIXTURE_IDS[2]
        product(writer, "insert", key, "stage3 resume")
        expected.append((key, "c", None, "stage3 resume"))
        commit = assert_events(runtime, expected)
        offset = persisted(runtime, offset, commit=commit)
        check(runtime.state_evidence() == history)
        runtime.state = runtime.private.with_name(
            "stage3-cold-state-receipt.json"
        )
        runtime.contract("capture-state")

        phase = "COLD_ARCHIVE_RESTORE_EXISTING_STATE"
        stage(phase)
        runtime.down()
        restore_images(directory, found)
        runtime.select_offline(directory, found)
        phase = "REJECT_BAD_ARTIFACT_AND_RESTORE"
        stage(phase)
        runtime.reject_wrong_offline_image(found)
        phase = "REJECT_FAILED_START_AND_RESTORE"
        stage(phase)
        runtime.exercise_failed_start()
        key = FIXTURE_IDS[3]
        product(writer, "insert", key, "stage3 cold")
        runtime.up(pristine=False)
        runtime.java(BROKER)
        runtime.java(CONNECT)
        check(runtime.state_evidence() == history)
        expected.append((key, "c", None, "stage3 cold"))
        commit = assert_events(runtime, expected)
        persisted(runtime, offset, commit=commit)
        check(sibling_state(admin) == sibling and jobs(admin) == capture_jobs)
        runtime.contract("check")
    except Exception:
        failed = True
        print(f"STAGE3_FAILED_PHASE={phase}", file=sys.stderr)
        raise
    finally:
        try:
            runtime.down(volumes=True)
        except Exception:
            print("STAGE3_CLEANUP_FAILED", file=sys.stderr)
            if not failed:
                raise
    stage("FIXTURE_PASS")


def main():
    """静的診断だけを出力し、scriptの引数は自動生成された合成場所に限る。"""
    hosted()
    check(len(sys.argv) == 4)
    directory, port, site = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
    check(
        directory.is_dir()
        and port.isdecimal()
        and 0 < int(port) <= 65535
        and bool(re.fullmatch(r"foundation-r-sqlserver-[0-9a-f]{16}", site))
    )
    check(SCHEMA == CONTRACT.SCHEMA)
    verify(directory, port, site, Path(os.environ["STAGE3_ARCHIVES_DIR"]))


if __name__ == "__main__":
    try:
        main()
    except FixtureError as error:
        print(f"STAGE3_FIXTURE_ERROR={error}", file=sys.stderr)
        raise SystemExit(1) from None
    except Exception as error:
        print(f"STAGE3_FIXTURE_ERROR={safe_failure(error)}", file=sys.stderr)
        raise SystemExit(1) from None

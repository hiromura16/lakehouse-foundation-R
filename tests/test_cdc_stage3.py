"""KafkaやSQLへ接続せず、Stage3の隔離と拒否条件を検証する。"""

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import cdc_stage3 as stage  # noqa: E402


def synthetic_value():
    """外部に接続しない合成の契約入力。"""
    return {
        "SQL_HOST": "sql.example.invalid",
        "SQL_PORT": "1433",
        "SQL_CERT_HOST": "sql.example.invalid",
        "SQL_DATABASE": "synthetic",
        "CLUSTER_ID": "QfT-5yMEQru5z7XQ5pDttw",
        "KAFKA_IMAGE": "example.invalid/kafka@sha256:" + "a" * 64,
        "CONNECT_IMAGE": "example.invalid/connect@sha256:" + "b" * 64,
    }


class ContractTests(unittest.TestCase):
    """入力とモデルの不変条件。"""

    def test_canonical_topics_and_connector(self):
        """管理トピックと六表の保持・境界の取り違えを検出する。"""
        values = synthetic_value()
        topics = stage.topic_contract(values["SQL_DATABASE"])
        self.assertEqual(len(topics), 10)
        self.assertEqual(
            topics[f"{stage.PREFIX}.synthetic.foundation_r_demo.products"],
            {
                "cleanup.policy": "delete",
                "retention.ms": "259200000",
                "retention.bytes": "-1",
            },
        )
        self.assertEqual(
            topics[stage.HISTORY],
            {
                "cleanup.policy": "delete",
                "retention.ms": "-1",
                "retention.bytes": "-1",
            },
        )
        for kind in ("config", "offset", "status"):
            self.assertEqual(
                topics[f"{stage.PREFIX}-connect-{kind}"],
                {
                    "cleanup.policy": "compact",
                    "retention.ms": "-1",
                    "retention.bytes": "-1",
                },
            )
        contract = stage.connector_contract(values)
        self.assertEqual(contract["driver.encrypt"], "true")
        self.assertEqual(contract["driver.trustServerCertificate"], "false")
        self.assertEqual(contract["data.query.mode"], "function")
        self.assertEqual(
            contract["database.password"],
            ("${file:/run/stage3/sql.properties:password}"),
        )
        self.assertEqual(len(contract["table.include.list"].split(",")), 6)
        self.assertIn(
            r"foundation_r_demo\.products\.product_id",
            contract["column.include.list"].split(","),
        )

    def test_no_mutable_or_wrong_job_ci_images(self):
        """ローカルCIタグで配備時の固定参照を迂回できないこと。"""
        values = synthetic_value()
        self.assertEqual(
            stage.image_selection(values, {}), dict.fromkeys(stage.IMAGE_KEYS)
        )
        ci_key = "CDC_STAGE3_KAFKA_CI_IMAGE_ID"
        config_id = "sha256:" + "a" * 64
        values["KAFKA_IMAGE"] = f"{stage.PREFIX}-kafka:ci-" + "a" * 16
        with self.assertRaisesRegex(stage.StageError, "IMAGE_NOT_IMMUTABLE"):
            stage.image_selection(values, {})
        with self.assertRaisesRegex(stage.StageError, "CI_IMAGE_FORBIDDEN"):
            stage.image_selection(values, {ci_key: config_id})
        environment = {
            ci_key: config_id,
            "GITHUB_ACTIONS": "true",
            "RUNNER_ENVIRONMENT": "github-hosted",
            "RUNNER_OS": "Linux",
            "GITHUB_REPOSITORY": "hiromura16/lakehouse-foundation-r",
            "GITHUB_EVENT_NAME": "push",
        }
        self.assertEqual(
            stage.image_selection(values, environment)["KAFKA_IMAGE"],
            config_id,
        )
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "event.json"
            path.write_text(
                json.dumps(
                    {
                        "pull_request": {
                            "head": {
                                "repo": {
                                    "full_name": "fork/lakehouse-foundation-r"
                                }
                            }
                        }
                    }
                )
            )
            environment.update(
                GITHUB_EVENT_NAME="pull_request", GITHUB_EVENT_PATH=str(path)
            )
            with self.assertRaisesRegex(
                stage.StageError, "CI_IMAGE_FORBIDDEN"
            ):
                stage.image_selection(values, environment)

    def test_patch_and_build_threshold(self):
        """実行JVMのmajor・パッチレベルとbuildの境界を確認する。"""
        for release, build, expected in (
            ("21.0.12.1", "2", False),
            ("21.0.13", "9", False),
            ("21.0.13.1", "1", False),
            ("21.0.13.1", "2", True),
            ("21.0.14", "1", True),
            ("25.0.1", "8", False),
        ):
            with self.subTest(release=release, build=build):
                self.assertIs(
                    stage.java_safe(
                        f'openjdk version "{release}"\n'
                        "OpenJDK Runtime Environment "
                        f"(build {release}+{build})"
                    ),
                    expected,
                )


class RuntimeChecks(unittest.TestCase):
    """状態の削除やPRからの意図せぬ管理接続を模擬する。"""

    def test_foreign_and_partial_topics_are_not_created(self):
        """異種topicと部分消失時は、一件もtopicを作成しない。"""
        for listed, reason in (
            ("unowned\n", "FOREIGN_TOPIC"),
            (stage.HISTORY + "\n", "TOPICS_INCOMPLETE_NO_REPAIR"),
        ):
            runtime = Mock(values=synthetic_value())
            runtime.broker.side_effect = [
                (
                    0,
                    f"ClusterId: {runtime.values['CLUSTER_ID']}\nLeaderId: 31",
                    "",
                ),
                (0, listed, ""),
            ]
            with (
                self.subTest(reason=reason),
                self.assertRaisesRegex(stage.StageError, reason),
            ):
                stage.topics(runtime, create=True)
            self.assertEqual(runtime.broker.call_count, 2)

    def test_topic_shape_and_actual_settings(self):
        """実効retentionをdescribeの宣言とは別に確認する。"""
        runtime = Mock()
        name = stage.HISTORY
        description = (
            f"Topic: {name}\tPartitionCount: 1\tReplicationFactor: 1\n"
            f"Topic: {name}\tPartition: 0\tLeader: 31\tReplicas: 31\tIsr: 31"
        )
        config = "\n".join(
            f"  {key}={value} sensitive=false synonyms={{}}"
            for key, value in stage.HISTORY_SETTINGS.items()
        )
        runtime.broker.side_effect = [(0, description, ""), (0, config, "")]
        stage.check_topic(runtime, name, stage.HISTORY_SETTINGS)
        runtime.broker.side_effect = [
            (0, description, ""),
            (
                0,
                config.replace("retention.ms=-1", "retention.ms=259200000"),
                "",
            ),
        ]
        with self.assertRaisesRegex(
            stage.StageError, "TOPIC_SETTINGS_MISMATCH"
        ):
            stage.check_topic(runtime, name, stage.HISTORY_SETTINGS)

    def test_registration_one_post_and_exact_readback(self):
        """未作成の一回POST、既存の再実行、差分停止を区別する。"""
        for status in ("absent", "already", "drift"):
            with self.subTest(status=status):
                runtime = Mock(values=synthetic_value())
                contract = stage.connector_contract(runtime.values)
                state = status != "absent"

                def endpoint(path, *, post=None):
                    nonlocal state
                    if post is not None:
                        self.assertFalse(state)
                        self.assertEqual(path, "/connectors")
                        self.assertEqual(
                            post, {"name": stage.CONNECTOR, "config": contract}
                        )
                        state = True
                        return 201, None
                    if path == "/connectors":
                        return 200, [stage.CONNECTOR] if state else []
                    if path.endswith("/config"):
                        found = dict(contract)
                        if status == "drift":
                            found["driver.trustServerCertificate"] = "true"
                        return 200, found
                    self.fail("unexpected REST path")

                runtime.http.side_effect = endpoint
                with (
                    patch.object(stage, "topics"),
                    patch.object(stage, "connect_ready", return_value=True),
                    patch.object(stage, "connector_ready", return_value=True),
                ):
                    if status == "drift":
                        with self.assertRaisesRegex(
                            stage.StageError, "CONNECTOR_CONFIG_DRIFT"
                        ):
                            stage.register(runtime, create=True)
                    else:
                        stage.register(runtime, create=True)
                posts = [
                    item
                    for item in runtime.http.call_args_list
                    if item.kwargs.get("post")
                ]
                self.assertEqual(len(posts), int(status == "absent"))

    def test_exact_read_only_secret_files_and_no_other_mounts(self):
        """管理daemonや書込可能な秘密ファイルを受け付けない。"""
        values = dict(
            synthetic_value(),
            SQL_PROPERTIES_FILE="/synthetic/sql.properties",
            TRUSTSTORE_FILE="/synthetic/truststore.p12",
        )
        mounts = [
            {
                "Destination": "/kafka/" + kind,
                "Type": "volume",
                "Name": stage.PROJECT + "_connect_" + kind,
                "RW": True,
            }
            for kind in ("data", "logs", "config")
        ]
        mounts.extend(
            {
                "Destination": target,
                "Type": "bind",
                "Source": values[key],
                "RW": False,
            }
            for target, key in (
                (stage.SQL_PROPERTIES, "SQL_PROPERTIES_FILE"),
                (stage.TRUSTSTORE, "TRUSTSTORE_FILE"),
            )
        )
        stage.check_mounts(stage.CONNECT, mounts, values)
        extra = {
            "Destination": "/synthetic-unexpected",
            "Type": "bind",
            "Source": "/synthetic",
            "RW": False,
        }
        with self.assertRaisesRegex(stage.StageError, "MOUNTS_MISMATCH"):
            stage.check_mounts(stage.CONNECT, [*mounts, extra], values)
        mounts[-1]["RW"] = True
        with self.assertRaisesRegex(stage.StageError, "MOUNTS_MISMATCH"):
            stage.check_mounts(stage.CONNECT, mounts, values)

    def test_positive_connector_and_single_running_task(self):
        """RUNNING表示だけで、task失敗や複数taskを許さない。"""
        runtime = Mock()
        data = {
            "name": stage.CONNECTOR,
            "type": "source",
            "connector": {"state": "RUNNING"},
            "tasks": [],
        }
        runtime.http.return_value = 200, data
        self.assertFalse(stage.connector_ready(runtime))
        data["tasks"] = [{"id": 0, "state": "RUNNING"}]
        self.assertTrue(stage.connector_ready(runtime))
        data["tasks"].append({"id": 1, "state": "RUNNING"})
        self.assertFalse(stage.connector_ready(runtime))
        data["tasks"][0]["state"] = "FAILED"
        with self.assertRaisesRegex(stage.StageError, "CONNECTOR_OR_TASK"):
            stage.connector_ready(runtime)

    def test_entrypoint_never_emits_private_settings(self):
        """CLIのplanは秘密の設定値やDocker接続を扱わない。"""
        with (
            patch.object(
                stage, "read_settings", return_value=synthetic_value()
            ),
            patch.object(
                sys, "argv", ["cdc_stage3.py", "--env-file", "mock", "plan"]
            ),
            patch.object(stage, "call") as command,
            redirect_stdout(io.StringIO()) as output,
            redirect_stderr(io.StringIO()) as errors,
        ):
            self.assertEqual(stage.main(), 0)
        command.assert_not_called()
        self.assertNotIn("example.invalid", output.getvalue())
        self.assertFalse(errors.getvalue())


class OperatorProofTests(unittest.TestCase):
    """配布物・topic名前・状態の見掛けだけで許可しない。"""

    def test_archived_images_remain_distinct_from_ci_and_raw_tags(self):
        """承認された両方のアーカイブから実効IDを限定する。"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "receipt.json"
            entries = {}
            for kind, digit in (("kafka", "a"), ("connect", "b")):
                archive = root / f"{kind}.tar"
                archive.touch(mode=0o600)
                entries[kind] = {
                    "path": str(archive),
                    "archive_sha256": "sha256:" + "c" * 64,
                    "manifest": "sha256:" + "d" * 64,
                    "config": "sha256:" + digit * 64,
                }
            receipt.write_text(json.dumps(entries))
            receipt.chmod(0o600)

            def expected_archive(path, digest, manifest, config):
                """Archivesの検証APIには合計三種類の承認値が必須。"""
                self.assertTrue(path.is_file())
                return "sha256:" + digest, manifest, config

            with patch.object(
                stage, "verify_archive", side_effect=expected_archive
            ) as checked:
                proof = stage.archive_selection(receipt)
                self.assertEqual(checked.call_count, 2)
            with (
                patch.object(
                    stage,
                    "verify_archive",
                    side_effect=stage.ImageError("BAD"),
                ),
                self.assertRaisesRegex(
                    stage.StageError, "OCI_ARCHIVE_NOT_VERIFIED"
                ),
            ):
                stage.archive_selection(receipt)

        values = synthetic_value()
        for kind in ("kafka", "connect"):
            key = kind.upper() + "_IMAGE"
            values[key] = f"{stage.PREFIX}-{kind}:oci-{proof[key][7:]}"
        self.assertEqual(
            stage.image_selection(values, {}, offline=proof), proof
        )
        with self.assertRaisesRegex(stage.StageError, "IMAGE_NOT_IMMUTABLE"):
            stage.image_selection(values, {})
        with self.assertRaisesRegex(
            stage.StageError, "OCI_AND_CI_MODE_CONFLICT"
        ):
            stage.image_selection(
                values,
                {"CDC_STAGE3_KAFKA_CI_IMAGE_ID": proof["KAFKA_IMAGE"]},
                offline=proof,
            )
        runtime = stage.Runtime(Path("/example-only"), values, offline=proof)
        with patch.object(
            stage, "call", return_value=(0, proof["CONNECT_IMAGE"], "")
        ):
            with self.assertRaisesRegex(
                stage.StageError, "LOADED_OCI_IMAGE_MISMATCH"
            ):
                runtime.preflight_local()

    def test_exact_target_map_and_connect_retention_negative(self):
        """DB省略名・日数を継承したconfig topicは受理しない。"""
        mapping = stage.sequence_map("synthetic")
        names = {
            f"{stage.PREFIX}.synthetic.{stage.SCHEMA}.{table}"
            for table in stage.TABLES
        }
        self.assertEqual(mapping.keys(), names)
        self.assertEqual(
            {entry["target_table"] for entry in mapping.values()},
            set(stage.TARGET_TABLES.values()),
        )
        runtime = Mock()
        name = stage.PREFIX + "-connect-config"
        runtime.broker.side_effect = [
            (
                0,
                f"Topic: {name} PartitionCount: 1 ReplicationFactor: 1\n"
                f"Topic: {name} Partition: 0 Leader: 31 Replicas: 31 Isr: 31",
                "",
            ),
            (
                0,
                "cleanup.policy=compact\nretention.ms=604800000\n"
                "retention.bytes=-1",
                "",
            ),
        ]
        with self.assertRaisesRegex(
            stage.StageError, "TOPIC_SETTINGS_MISMATCH"
        ):
            stage.check_topic(runtime, name, stage.COMPACT)

    def test_lost_or_recreated_tracked_topic_and_regression_refused(self):
        """同名・同clusterでもIDの再生成と後退を成功と誤認しない。"""
        values = synthetic_value()
        names = stage.topic_contract(values["SQL_DATABASE"])
        current = {
            "cluster": values["CLUSTER_ID"],
            "topics": {key: f"{i:022}" for i, key in enumerate(names, 1)},
            "commit_lsn": "00000001:00000002:0001",
            "history_end": 2,
            "broker_state": {
                "history": {"high": 2, "sha256": "a" * 64},
                "offset": {"high": 1, "sha256": "b" * 64},
            },
        }
        self.assertIs(stage.validate_state(current, values), current)
        with self.assertRaisesRegex(
            stage.StageError, "CONTINUITY_RECEIPT_INVALID"
        ):
            stage.validate_state(current | {"topics": {}}, values)
        old = current | {"history_end": 1}
        stage.compare_state(old, current)
        with self.assertRaisesRegex(stage.StageError, "CDC_STATE_REGRESSED"):
            stage.compare_state(current, old)
        recreated = current | {
            "topics": current["topics"] | {next(iter(names)): "z" * 22}
        }
        with self.assertRaisesRegex(
            stage.StageError, "TOPIC_GENERATION_MISMATCH"
        ):
            stage.compare_state(current, recreated)

    def test_broker_only_restore_requires_exact_byte_evidence(self):
        """最新LSNの読み直し以前に、履歴と最後offsetの同一性を要求。"""
        runtime = Mock()
        runtime.compose = ["compose"]
        runtime.environment = {}
        saved = {
            "topics": {"synthetic": "same"},
            "broker_state": {"offset": "old"},
        }
        with (
            patch.object(stage, "call", return_value=(0, "", "")),
            patch.object(stage, "topics") as topics,
            patch.object(
                stage,
                "broker_state",
                return_value=(saved["topics"], {"offset": "wrong"}),
            ),
            self.assertRaisesRegex(
                stage.StageError, "BROKER_ARCHIVED_STATE_MISMATCH"
            ),
        ):
            stage.require_safe_broker_restore(runtime, saved)
        topics.assert_called_once_with(runtime, create=False)
        with (
            patch.object(stage, "call", return_value=(0, "active", "")),
            patch.object(stage, "topics") as untouched,
            self.assertRaisesRegex(
                stage.StageError, "CONNECT_MUST_NOT_BE_RUNNING"
            ),
        ):
            stage.require_safe_broker_restore(runtime, saved)
        untouched.assert_not_called()

    def test_check_requires_existing_receipt_before_any_runtime_call(self):
        """存在しない承認stateでrestoreを試みる前に止まる。"""
        with (
            patch.object(
                stage, "read_settings", return_value=synthetic_value()
            ),
            patch.object(stage, "Runtime") as runtime,
            patch.object(stage, "register") as register,
            patch.object(
                sys, "argv", ["stage", "--env-file", "/example", "check"]
            ),
            redirect_stderr(io.StringIO()) as error,
        ):
            self.assertEqual(stage.main(), 1)
        self.assertEqual(
            error.getvalue().strip(), "CONTINUITY_RECEIPT_REQUIRED"
        )
        register.assert_not_called()
        runtime.return_value.broker.assert_not_called()


if __name__ == "__main__":
    unittest.main()

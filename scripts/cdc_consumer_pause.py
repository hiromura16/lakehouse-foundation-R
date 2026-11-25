"""閉域CDCでconsumerだけを停止し、未処理の1件を同じconsumerで回収する。"""

import subprocess

from scripts import cdc_local_continuity as closed
from scripts.cdc_storage_failure import (
    OVERLAY,
    evidence,
    inspect,
    storage_volume,
)
from scripts.influx_local_acceptance import AcceptanceError, require


def peer_epoch(docker, identity):
    """同じ埋込み資源が起動したまま、healthcheckも成功していること。"""
    state = inspect(docker, "container", identity)["State"]
    started = state["StartedAt"]
    require(
        state["Running"] is True
        and state["Status"] == "running"
        and state["Health"]["Status"] == "healthy"
        and isinstance(started, str)
        and started
    )
    return started


def consumer_epoch(docker, identity, *, running):
    """停止・再開の実状態と、自動再起動がないことを照合する。"""
    value = inspect(docker, "container", identity)
    state = value["State"]
    started = state["StartedAt"]
    require(
        value["HostConfig"]["RestartPolicy"]["Name"] == "no"
        and value["RestartCount"] == 0
        and state["Running"] is running
        and state["Status"] == ("running" if running else "exited")
        and state["OOMKilled"] is False
        and isinstance(started, str)
        and started
    )
    return started


def pause_case(docker, compose, project, original, checkpoint, body):
    """停止前、停止中、再開後の各時点で実データと資源を照合する。"""
    ids, _ = original
    consumer = ids["native-cdc"]
    broker = ids["test-broker"]
    storage = ids["storage"]

    def fixture(phase, *, timeout=130):
        """既存の閉域コンテナ内だけで、固定した合成工程を呼ぶ。"""
        return compose(
            "exec",
            "-T",
            "lakehouse",
            "python",
            "-",
            phase,
            timeout=timeout,
            input_text=body,
        ).strip()

    def peers():
        """broker・storageのhealthと起動時刻、同一volumeを照合する。"""
        return (
            peer_epoch(docker, broker),
            peer_epoch(docker, storage),
            storage_volume(docker, storage, project),
        )

    def boundary(*, paused):
        """停止対象以外に変更がないことを毎回確認する。"""
        inactive = frozenset({"native-cdc"}) if paused else frozenset()
        require(
            closed.runtime_boundary(
                docker, compose, project, inactive=inactive
            )
            == original
        )
        require(peers() == healthy)

    stage = "BASELINE"
    try:
        healthy = peers()
        boundary(paused=False)
        started = consumer_epoch(docker, consumer, running=True)
        for phase in ("first", "delete", "resume"):
            fixture(phase)
        before = evidence(fixture("storage-baseline"), 9)
        checkpoint(8)
        boundary(paused=False)
        require(consumer_epoch(docker, consumer, running=True) == started)

        stage = "STOP_OWNED_CONSUMER"
        docker("stop", "--time", "20", consumer, timeout=35)
        boundary(paused=True)
        require(consumer_epoch(docker, consumer, running=False) == started)

        stage = "PUBLISH_WHILE_PAUSED"
        # この工程は指定partitionのoffset 9への送信を確認する。
        require(fixture("storage-publish", timeout=25) == "DELIVERED")

        stage = "PAUSED_PERSISTENCE"
        # 名前は既存S3試験由来だが、ここではstorageを一切操作しない。
        # 実RawとSilverを読み、Kafka commit 9 / high 10を別々に検査する。
        require(evidence(fixture("storage-restored"), 9) == before)
        checkpoint(8)
        boundary(paused=True)
        require(consumer_epoch(docker, consumer, running=False) == started)

        stage = "START_SAME_CONSUMER"
        docker("start", consumer)
        boundary(paused=False)
        restarted = consumer_epoch(docker, consumer, running=True)
        require(restarted != started)

        stage = "EXACT_RECOVERY"
        # 再送なし。同じKafkaの1件をRawで同じID・本文・位置として検証。
        after = evidence(fixture("storage-recovered"), 10)
        require(
            after["raw_snapshot_id"] != before["raw_snapshot_id"]
            and after["release_snapshot_id"] != before["release_snapshot_id"]
        )
        checkpoint(9)
        stage = "READ_ONLY_CONFIRM"
        require(evidence(fixture("storage-recovered"), 10) == after)
        checkpoint(9)
        boundary(paused=False)
        require(consumer_epoch(docker, consumer, running=True) == restarted)
    except (
        AcceptanceError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        subprocess.SubprocessError,
    ):
        print(f"FAIL: CONSUMER_PAUSE_{stage}", flush=True)
        raise AcceptanceError from None


if __name__ == "__main__":
    raise SystemExit(
        closed.main(
            fixture=pause_case, overlay=OVERLAY, fixture_name="CONSUMER_PAUSE"
        )
    )

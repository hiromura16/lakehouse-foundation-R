"""メインスレッドのCDC処理と、HTTPで参照できる状態キャッシュを調停する。

Kafka、SQLite、Icebergへアクセスするのはコンシューマーのメインスレッドだけ。
再構築は明示的に要求をキューへ登録する保守操作である。
Rawの検証や派生表の構築に失敗した場合は、
現在の完了位置とKafkaの配送確認を進めない。
"""

from cdc_replay import RebuildError, rebuild
from runtime_control import observed_at
from state import State, assignment_offset


class Runtime:
    """ブローカーと状態の遷移を管理し、HTTPにはキャッシュ済み情報を渡す。"""

    def __init__(
        self,
        admin,
        state,
        values,
        mapping,
        topics,
        control,
        broker,
        partition_factory,
        process_batch,
        materialize,
        *,
        lost_state=False,
    ):
        self.admin = admin
        self.state = state
        self.values = values
        self.mapping = mapping
        self.topics = topics
        self.control = control
        self.broker = broker
        self.partition_factory = partition_factory
        self.process_batch = process_batch
        self.materialize = materialize
        self.fixed = values.get("CDC_SOURCE_MODE") == "fixed-demo"
        self.maintenance = self.fixed and (
            lost_state
            or bool(state.get("rebuild_pending"))
            or bool(state.get("materialization_pending"))
            or state.get("blocked") == "true"
        )
        self.partitions = []
        self.cursor_assignment = {}
        self.read_offsets = {}
        self.ack_offsets = {}
        self.error_code = "CDC_REBUILD_REQUIRED" if self.maintenance else None
        self.appended = 0
        self.observe()

    def blocked(self):
        """プロセスを再起動しても、クエリ取消が未確定という状態を保持する。"""
        return self.state.get("blocked") == "true" or bool(
            self.admin.STATE.get("blocked")
        )

    def clear_cursors(self):
        """割当や再構築をまたいで前の観測を流用しない。"""
        self.read_offsets.clear()
        self.ack_offsets.clear()
        self.control.clear_partition_cursors()

    def revoke_cursors(self, _client, _partitions):
        """配送制御は変更せず、失効した割当の観測だけを消す。"""
        self.cursor_assignment.clear()
        self.clear_cursors()

    def note_read(self, message):
        """成功したpoll直後、本文を解釈する前の位置を保持する。"""
        key = message.topic(), message.partition()
        offset = message.offset()
        if (
            key in self.cursor_assignment
            and type(offset) is int
            and offset >= 0
        ):
            self.read_offsets[key] = offset
            self.control.update_partition_read(
                self.cursor_assignment[key], offset
            )

    def note_ack(self, requested, response):
        """同期commitの応答で、成功を照合できた区画の次位置だけ保持。"""
        if not isinstance(response, list):
            return
        expected = {
            (part.topic, part.partition): part.offset for part in requested
        }
        seen = set()
        confirmed = {}
        for item in response:
            topic = getattr(item, "topic", None)
            partition = getattr(item, "partition", None)
            if not isinstance(topic, str) or type(partition) is not int:
                continue
            key = topic, partition
            offset = getattr(item, "offset", None)
            if key in seen:
                confirmed.pop(key, None)
                continue
            seen.add(key)
            if (
                key in self.cursor_assignment
                and type(offset) is int
                and offset >= 0
                and expected.get(key) == offset
                and getattr(item, "error", False) is None
            ):
                confirmed[key] = offset
        # SQLite完了位置と同じ状態更新までACKの公開を待つ。
        self.ack_offsets.update(confirmed)

    def assign(self, client, partitions):
        """再開位置を選ぶ前に、ブローカーに残る履歴範囲を確認する。"""
        self.revoke_cursors(client, partitions)
        bounds = {}
        for partition in partitions:
            if partition.topic not in self.topics:
                raise ValueError("Unexpected CDC partition assignment.")
            low, high = client.get_watermark_offsets(partition, timeout=10)
            bounds[(partition.topic, partition.partition)] = low
            try:
                partition.offset = (
                    low
                    if self.maintenance
                    else assignment_offset(
                        self.state.offset(
                            partition.topic, partition.partition
                        ),
                        low,
                        high,
                    )
                )
            except RuntimeError:
                if not self.fixed:
                    raise
                self.maintenance = True
                self.error_code = "CDC_REBUILD_REQUIRED"
        if self.maintenance:
            for partition in partitions:
                partition.offset = bounds[
                    (partition.topic, partition.partition)
                ]
        self.partitions = list(partitions)
        client.assign(partitions)
        self.cursor_assignment = {
            (part.topic, part.partition): index
            for index, part in enumerate(self.partitions)
        }
        if self.maintenance and partitions:
            client.pause(partitions)
        self.observe()

    def partition_status(self):
        """非公開名を含めずに、完了位置とブローカーの履歴範囲を観測する。"""
        result = []
        for partition in self.partitions:
            target = self.topics.get(partition.topic)
            if target is None:
                continue
            completed = self.state.offset(partition.topic, partition.partition)
            low = high = lag = None
            position = "unavailable"
            try:
                low, high = self.broker.get_watermark_offsets(
                    partition, timeout=3
                )
                position = (
                    "retention_gap"
                    if completed + 1 < low
                    else (
                        "broker_reset" if completed + 1 > high else "observed"
                    )
                )
                if position == "observed":
                    lag = high - completed - 1
            except Exception:
                low = high = None
            key = partition.topic, partition.partition
            if position in ("broker_reset", "retention_gap"):
                self.read_offsets.pop(key, None)
                self.ack_offsets.pop(key, None)
            elif type(low) is int and type(high) is int and 0 <= low <= high:
                # 完了位置が範囲内でも、未処理の読取位置や古いACKは
                # 最新のbroker範囲から外れ得る。従来の状態は変更しない。
                read_offset = self.read_offsets.get(key)
                if read_offset is not None and not low <= read_offset < high:
                    self.read_offsets.pop(key)
                ack_offset = self.ack_offsets.get(key)
                if ack_offset is not None and not low <= ack_offset <= high:
                    self.ack_offsets.pop(key)
            result.append(
                {
                    "table": target,
                    "partition": partition.partition,
                    "completed_offset": completed,
                    "last_read_offset": self.read_offsets.get(key),
                    "broker_ack_next_offset": self.ack_offsets.get(key),
                    "broker_low": low,
                    "broker_high": high,
                    "next_offset": completed + 1,
                    "lag": lag,
                    "state": position,
                }
            )
        return result

    def observe(self, *, busy=False):
        """状態キャッシュは、メインスレッドでのみ更新する。"""
        blocked = self.blocked()
        pending = self.fixed and bool(
            self.state.get("rebuild_pending")
            or self.state.get("materialization_pending")
        )
        rebuilding = self.control.snapshot()["rebuild"]
        pending = pending or bool(
            rebuilding and rebuilding["status"] == "running"
        )
        initialized = bool(self.state.get("initialized"))
        generation = bool(self.state.completed_offsets())
        # 読取可能な空ファイルだけでは、派生済みの世代を確認できない。
        partitions = self.partition_status()
        observed = (
            initialized
            and generation
            and bool(partitions)
            and all(item["state"] == "observed" for item in partitions)
        )
        error = "CDC_QUERY_REVIEW_REQUIRED" if blocked else self.error_code
        if error is None and not observed:
            error = "CDC_STARTING"
        self.control.update(
            ready=observed and not (blocked or self.maintenance or pending),
            blocked=blocked,
            busy=busy,
            error_code=error,
            current_rows=self.state.counts(self.mapping),
            partitions=partitions,
            raw_events_appended_since_start=self.appended,
        )

    def resume_plan(self, candidate):
        """候補のオフセットを、現在割り当てられたKafkaデータに照らす。"""
        assigned = {(item.topic, item.partition) for item in self.partitions}
        plan = []
        for (
            topic,
            partition,
        ), completed in candidate.completed_offsets().items():
            if (topic, partition) not in assigned:
                raise RebuildError("CDC_KAFKA_ASSIGNMENT_REQUIRED")
            item = self.partition_factory(topic, partition)
            low, high = self.broker.get_watermark_offsets(item, timeout=10)
            item.offset = assignment_offset(completed, low, high)
            plan.append(item)
        return plan

    def reopen_active_state(self):
        """接続終了や公開に失敗した後、現在の世代を開き直す。"""
        path, identity = self.state.path, self.state.identity
        try:
            self.state.connection.close()
        except Exception:
            pass
        self.state = State(path, identity)

    def handle_rebuild(self):
        """配送を一時停止し、ローカル状態の再構築後にKafkaへ確認を返す。"""
        request_id = self.control.take_rebuild()
        if request_id is None:
            return False
        snapshot_id = None
        self.clear_cursors()
        self.observe(busy=True)
        try:
            if self.blocked():
                raise RebuildError("CDC_QUERY_REVIEW_REQUIRED")
            if self.partitions:
                self.broker.pause(self.partitions)
            self.state, snapshot_id = rebuild(
                self.admin,
                self.state,
                self.mapping,
                self.topics,
                request_id,
                materialize=self.materialize,
                validate_resume=self.resume_plan,
            )
            # この時点でローカル世代は永続化済み。以降の失敗で後戻りさせない。
            # 起動時は、検証済みのローカル完了位置から再開する。
            plan = self.resume_plan(self.state)
            for partition in plan:
                self.broker.seek(partition)
            response = self.broker.commit(offsets=plan, asynchronous=False)
            self.note_ack(plan, response)
            if self.partitions:
                self.broker.resume(self.partitions)
            self.maintenance = False
            self.error_code = None
            self.control.finish_rebuild(
                request_id, succeeded=True, snapshot_id=snapshot_id
            )
            self.control.update(last_success=observed_at())
            self.observe()
        except Exception as error:
            # 公開のため、候補または旧世代の接続は閉じている場合がある。
            self.reopen_active_state()
            blocked = self.blocked()
            code = (
                "CDC_QUERY_REVIEW_REQUIRED"
                if blocked
                else (
                    "CDC_KAFKA_ACK_FAILED"
                    if self.state.get("rebuild_generation") == request_id
                    else "CDC_REBUILD_FAILED"
                )
            )
            diagnostic = (
                error.code if isinstance(error, RebuildError) else code
            )
            self.state.record_rebuild(
                request_id, "failed", snapshot_id, error=diagnostic
            )
            self.maintenance = True
            self.error_code = code
            self.control.finish_rebuild(
                request_id,
                succeeded=False,
                snapshot_id=snapshot_id,
                error_code=code,
            )
            self.observe()
        return True

    def process(self, records):
        """上限内のバッチを1つ処理し、異常時は固定ソースの配送を止める。"""
        self.observe(busy=True)
        self.state.last_raw_appended = 0
        counted = False
        try:
            result = self.process_batch(
                self.admin,
                self.state,
                records,
                self.mapping,
                self.topics,
                values=self.values,
            )
            self.appended += self.state.last_raw_appended
            counted = True
            offsets = {}
            for record in records:
                key = record["topic"], record["partition"]
                offsets[key] = max(offsets.get(key, -1), record["offset"] + 1)
            plan = [
                self.partition_factory(topic, partition, offset)
                for (topic, partition), offset in offsets.items()
            ]
            response = self.broker.commit(offsets=plan, asynchronous=False)
            self.note_ack(plan, response)
            self.error_code = None
            self.control.update(last_success=observed_at())
            self.observe()
            return result
        except Exception:
            if not self.fixed:
                raise
            if not counted:
                self.appended += self.state.last_raw_appended
            self.maintenance = True
            self.error_code = "CDC_PROCESSING_FAILED"
            if counted:
                self.error_code = "CDC_KAFKA_ACK_FAILED"
            if self.partitions:
                self.broker.pause(self.partitions)
            self.observe()
            return None

import unittest
import time
from unittest import mock

from relay_orchestrator import CopyCancelled, RelayVolumeMover
from relay_pool import RelayNode


class _Sleeper:
    """让等待逻辑在测试里立即返回，同时记录调用次数。"""

    def __init__(self):
        self.calls = 0

    def __call__(self, seconds):
        self.calls += 1


class _FakeClock:
    """可推进的假时钟，避免停滞检测依赖真实等待。"""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class _FakePool:
    def __init__(self, nodes):
        self._nodes = list(nodes)
        self.released = []

    def acquire(self, task_id):
        return self._nodes.pop(0) if self._nodes else None

    def has_live_holder(self):
        return bool(self._nodes)

    def release(self, node_id, *, lease_id=""):
        self.released.append(node_id)


class _FakeState:
    def __init__(self):
        self.enqueued = []
        self.ready = set()
        self.results = {}

    def enqueue(self, task):
        self.enqueued.append(task)

    def mark_task_ready(self, task_id):
        self.ready.add(task_id)

    def task_ready(self, task_id):
        return task_id in self.ready

    def record_result(self, task_id, status, copied_bytes, digest="", error=""):
        self.results[task_id] = {
            "status": status,
            "copied_bytes": copied_bytes,
            "digest": digest,
            "error": error,
        }

    def task_result(self, task_id):
        return self.results.get(task_id)


class _FakeLedger:
    def __init__(self):
        self.records = {}
        self.saved = 0

    def get(self, job_id, volume_id):
        return self.records.get(f"{job_id}:{volume_id}")

    def upsert(self, record):
        self.records[record.key] = record
        return record

    def save(self):
        self.saved += 1


class _Volume:
    def __init__(self, volume_id="vol-s1", size=4096):
        self.source_volume_id = volume_id
        self.size = size


class RelayVolumeMoverTest(unittest.TestCase):
    def setUp(self):
        from relay_ledger import VolumeTaskRecord
        from relay_volumes import SourceCopy

        self.VolumeTaskRecord = VolumeTaskRecord
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.wait_attachment_device.return_value = "/dev/vdb"
        self.target_os.wait_attachment_device.return_value = "/dev/vdc"
        self.lifecycle = mock.MagicMock()
        self.lifecycle.source_os = self.source_os
        self.lifecycle.target_os = self.target_os
        self.lifecycle.create_source_copy.return_value = SourceCopy(
            snapshot_id="snap-1", derived_volume_id="vol-d1"
        )
        self.lifecycle.create_target_volume.return_value = "vol-t1"
        self.source_node = RelayNode(
            node_id="n-s", name="relay-source-0", role="source", az="az1",
            server_id="srv-s", session_id="sess-s", data_address="10.0.0.7",
        )
        self.target_node = RelayNode(
            node_id="n-t", name="relay-target-0", role="target", az="az2",
            server_id="srv-t", session_id="sess-t", data_address="10.0.0.8",
        )
        self.state = _FakeState()
        self.ledger = _FakeLedger()
        self.mover = RelayVolumeMover(
            source_pool=_FakePool([self.source_node]),
            target_pool=_FakePool([self.target_node]),
            lifecycle=self.lifecycle,
            state=self.state,
            ledger=self.ledger,
            job_id="job-1",
            sleeper=_Sleeper(),
            # 这些用例断言"没有空闲机器"时的行为：池里无节点时即便 0=不限时
            # 也会快速失败（无人会释放），不会把测试挂死。
            slot_wait_timeout=0,
        )

    def _auto_complete(self):
        """模拟 agent：入队即就绪、随即返回成功结果。"""
        for task in list(self.state.enqueued):
            if task.get("kind") == "verify":
                self.state.record_result(
                    task["task_id"], "done", 4096, digest="same-digest"
                )
                continue
            self.state.mark_task_ready(task["task_id"])
            self.state.record_result(task["task_id"], "done", 4096)

    def _copy_tasks(self):
        return [t for t in self.state.enqueued if t.get("kind") != "verify"]

    def _run(self, volume=None):
        return self.mover.move(
            volume=volume or _Volume(),
            vm_name="vm-1",
            index=0,
        )

    def test_move_orders_target_task_before_source(self):
        original_enqueue = self.state.enqueue

        def enqueue(task):
            original_enqueue(task)
            self._auto_complete()

        self.state.enqueue = enqueue

        self._run()

        roles = [task["role"] for task in self._copy_tasks()]
        self.assertEqual(roles, ["target", "source"])

    def test_pool_exhausted_still_cleans_derived_copy_and_snapshot(self):
        """取不到中转机槽位时，派生卷与快照也必须回收，不能留在源云里。

        旧实现把清理挂在 ``source_node is not None`` 上，池满直接失败时
        copy 已经建好却不会清理，留下 200G 派生卷 + 快照占配额。
        """
        # 关掉失败保留：这里验证的是"保留期关闭时不留空壳"这条路径。
        self.mover.derived_retention_window = 0.0
        self.mover.source_pool = _FakePool([])  # 源端没有可用中转机
        self.mover.target_pool = _FakePool([self.target_node])

        with self.assertRaises(RuntimeError) as ctx:
            self._run()

        self.assertIn("中转机池没有空闲机器", str(ctx.exception))
        self.lifecycle.cleanup_source_copy.assert_called_once()
        args = self.lifecycle.cleanup_source_copy.call_args
        self.assertEqual(args.args[0].derived_volume_id, "vol-d1")
        # 没有中转机可卸载，relay_server_id 允许为空。
        self.assertEqual(args.args[1], "")

    def test_successful_move_does_not_double_clean_source_copy(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        # 正常路径在 try 里清理一次，finally 不应再清一次（否则重复删卷报错刷日志）。
        self.assertEqual(self.lifecycle.cleanup_source_copy.call_count, 1)

    def test_error_reports_the_side_that_actually_failed(self):
        """源端失败、目标端还挂在监听时，必须报源端的错误而不是 'target 失败: None'。"""

        def enqueue(task):
            self.state.enqueued.append(task)
            if task["role"] == "target":
                # 目标端进入监听后就一直等连接，不返回结果
                self.state.mark_task_ready(task["task_id"])
            else:
                self.state.record_result(
                    task["task_id"],
                    "failed",
                    0,
                    error="FileNotFoundError: /dev/vdb",
                )

        self.state.enqueue = enqueue

        with self.assertRaises(RuntimeError) as ctx:
            self._run()

        message = str(ctx.exception)
        self.assertIn("-s 失败", message)
        self.assertIn("FileNotFoundError", message)

    def test_target_accept_timeout_is_bounded(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        target_task = self._copy_tasks()[0]
        self.assertLessEqual(target_task["accept_timeout"], 600)

    def test_transfer_length_is_bytes_not_gib(self):
        """Cinder 的卷大小是 GiB；数据面按字节传，少乘 1024^3 会直接越界。"""
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run(volume=_Volume(size=1))

        target_task = self._copy_tasks()[0]
        self.assertEqual(target_task["length"], 1024 ** 3)

    def test_move_records_total_bytes_for_page_progress(self):
        """页面要显示百分比，台账必须带上总量（字节）。"""
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run(volume=_Volume(size=2))

        record = self.ledger.get("job-1", "vol-s1")
        self.assertEqual(record.total_bytes, 2 * 1024 ** 3)

    def test_default_stall_timeout_is_five_minutes(self):
        self.assertEqual(self.mover.stall_timeout, 300.0)

    def test_default_result_timeout_is_unlimited(self):
        """1TiB 盘按 50MB/s 要 5.8 小时，不能再用 6 小时墙钟判死。"""
        self.assertEqual(self.mover.result_timeout, 0.0)

    def test_bounded_timeout_falls_back_for_substeps(self):
        """校验/等待监听没有进度看门狗，不限时时必须有墙钟兜底。"""
        self.assertEqual(self.mover._bounded_timeout(), 6 * 3600.0)
        self.mover.result_timeout = 7200.0
        self.assertEqual(self.mover._bounded_timeout(), 7200.0)

    def test_prepare_touches_ledger_during_long_waits(self):
        """准备阶段默认不限时，等待期间必须刷新台账时间戳。

        对账清理只按 updated_at 判断残留；不刷新的话，一块盘准备超过静默
        窗口（>1 小时）就会把正在派生的卷当成孤儿删掉。
        """
        original_save = self.ledger.save
        self.ledger.save = mock.MagicMock(side_effect=original_save)

        prepared = self.mover.prepare(
            volume=_Volume(), vm_name="vm-1", index=0
        )

        on_wait = self.lifecycle.create_source_copy.call_args.kwargs["on_wait"]
        target_on_wait = self.lifecycle.create_target_volume.call_args.kwargs["on_wait"]
        self.assertTrue(callable(on_wait))
        self.assertTrue(callable(target_on_wait))

        record = self.ledger.get("job-1", "vol-s1")
        before = record.updated_at
        self.ledger.save.reset_mock()
        on_wait(600.0)

        self.assertGreaterEqual(record.updated_at, before)
        self.assertEqual(self.ledger.save.call_count, 1)
        self.assertEqual(record.volume_id, "vol-s1")
        self.assertEqual(prepared.target_volume_id, "vol-t1")

    def test_unlimited_result_timeout_still_catches_stall(self):
        """不限时也必须靠字节看门狗把卡死的拷贝判失败。"""
        clock = _FakeClock()
        self.mover.clock = clock
        self.mover.result_timeout = 0.0
        self.mover.stall_timeout = 30.0

        def sleeper(seconds):
            clock.advance(float(seconds))
            if clock.now > 3600.0:
                raise AssertionError("不限时也不能让卡死的拷贝一直空转")

        self.mover.sleeper = sleeper

        def enqueue(task):
            self.state.enqueued.append(task)
            if task["role"] == "target":
                self.state.mark_task_ready(task["task_id"])

        self.state.enqueue = enqueue

        with self.assertRaises(TimeoutError) as ctx:
            self._run()

        self.assertIn("停滞", str(ctx.exception))

    def test_transfer_without_progress_raises_stall_timeout(self):
        """字节长时间不涨必须判定卡死，而不是等到墙钟上限。"""
        clock = _FakeClock()
        self.mover.clock = clock
        self.mover.stall_timeout = 30.0

        def sleeper(seconds):
            clock.advance(float(seconds))
            if clock.now > 3600.0:
                # 没有停滞检测时这里会一直空转，用断言避免测试挂死。
                raise AssertionError("拷贝等待超过 1 小时仍未判定停滞")

        self.mover.sleeper = sleeper

        def enqueue(task):
            self.state.enqueued.append(task)
            if task["role"] == "target":
                self.state.mark_task_ready(task["task_id"])

        self.state.enqueue = enqueue

        with self.assertRaises(TimeoutError) as ctx:
            self._run()

        self.assertIn("停滞", str(ctx.exception))

    def test_progress_updates_prevent_stall_detection(self):
        """慢拷贝只要 copied_bytes 在涨就不能误判成卡死。"""
        clock = _FakeClock()
        self.mover.clock = clock
        self.mover.stall_timeout = 30.0

        def sleeper(seconds):
            clock.advance(10.0)
            record = self.ledger.get("job-1", "vol-s1")
            record.copied_bytes = int(record.copied_bytes or 0) + 1024
            if clock.now >= 60.0:
                for task in self.state.enqueued:
                    self.state.record_result(
                        task["task_id"], "done", record.copied_bytes
                    )

        self.mover.sleeper = sleeper

        def enqueue(task):
            self.state.enqueued.append(task)
            if task["role"] == "target":
                self.state.mark_task_ready(task["task_id"])

        self.state.enqueue = enqueue

        self._run()

        self.assertEqual(self.ledger.get("job-1", "vol-s1").phase, "done")

    def test_stall_cancels_pending_tasks(self):
        """判定停滞时两端任务要先撤销，否则残留 agent 会一直占住槽位。"""
        clock = _FakeClock()
        self.mover.clock = clock
        self.mover.stall_timeout = 30.0
        self.mover.copy_retries = 0

        def sleeper(seconds):
            clock.advance(float(seconds))
            if clock.now > 3600.0:
                raise AssertionError("拷贝等待超过 1 小时仍未判定停滞")

        self.mover.sleeper = sleeper
        cancelled = []
        self.state.request_cancel = cancelled.append

        def enqueue(task):
            self.state.enqueued.append(task)
            if task["role"] == "target":
                self.state.mark_task_ready(task["task_id"])

        self.state.enqueue = enqueue

        with self.assertRaises(TimeoutError):
            self._run()

        task_ids = [task["task_id"] for task in self._copy_tasks()]
        self.assertEqual(cancelled, task_ids)

    def test_offset_beyond_total_skips_transfer(self):
        """已拷完的卷重试时不能再取 length=1，否则又会越界。"""
        record = self.VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            copied_bytes=1024 ** 3,
        )
        self.ledger.upsert(record)
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run(volume=_Volume(size=1))

        self.assertEqual(self._copy_tasks(), [])

    def test_transfer_enables_sparse_when_both_agents_support_it(self):
        self.source_node.agent_version = "1.1.0"
        self.target_node.agent_version = "1.1.0"
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        for task in self._copy_tasks():
            self.assertTrue(task["sparse"])
            self.assertEqual(task["hole_mode"], "skip")

    def test_transfer_falls_back_when_any_agent_is_old(self):
        self.source_node.agent_version = "1.0.0"
        self.target_node.agent_version = "1.1.0"
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        for task in self._copy_tasks():
            self.assertNotIn("sparse", task)
            self.assertNotIn("hole_mode", task)

    def test_transfer_disables_sparse_when_mode_off(self):
        self.source_node.agent_version = "1.1.0"
        self.target_node.agent_version = "1.1.0"
        self.mover.hole_mode = "off"
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        self.assertNotIn("sparse", self._copy_tasks()[0])

    def test_sparse_decision_explains_why_it_is_disabled(self):
        from relay_orchestrator import sparse_decision

        self.assertEqual(
            sparse_decision("1.1.0", "1.1.0", "skip"), (True, "both agents >= 1.1.0, hole_mode=skip")
        )
        enabled, reason = sparse_decision("1.0.0", "1.1.0", "skip")
        self.assertFalse(enabled)
        self.assertIn("source agent", reason)
        enabled, reason = sparse_decision("1.1.0", "1.1.0", "off")
        self.assertFalse(enabled)
        self.assertIn("off", reason)

    def test_transfer_passes_skipped_base_from_ledger(self):
        record = self.VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            skipped_bytes=4096,
        )
        self.ledger.upsert(record)
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        self.assertEqual(self._copy_tasks()[0]["skipped_base"], 4096)

    def test_move_pins_tasks_to_the_attached_relay(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        target_task, source_task = self._copy_tasks()
        # 绑定用节点名：node_id 与注册产生的 agent_id 不同，按 id 绑定会没人领。
        self.assertEqual(target_task["agent_name"], "relay-target-0")
        self.assertEqual(target_task["dst_path"], "/dev/vdc")
        self.assertEqual(source_task["agent_name"], "relay-source-0")
        self.assertEqual(source_task["src_path"], "/dev/vdb")
        self.assertEqual(source_task["peer_host"], "10.0.0.8")
        self.assertEqual(source_task["peer_port"], 9200)
        self.assertEqual(source_task["ticket"], target_task["ticket"])

    def test_move_returns_target_volume_id(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        result = self._run()

        self.assertEqual(result["target_volume_id"], "vol-t1")

    def test_move_uses_per_volume_target_type_override(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())
        self.mover.target_volume_type = "pool-default"

        self.mover.move(
            volume=_Volume(),
            vm_name="vm-1",
            index=0,
            target_volume_type="per-volume-ssd",
        )

        kwargs = self.lifecycle.create_target_volume.call_args.kwargs
        self.assertEqual(kwargs["volume_type"], "per-volume-ssd")

    def test_move_uses_per_volume_source_type_override(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())
        self.mover.source_volume_type = "pool-default"

        self.mover.move(
            volume=_Volume(),
            vm_name="vm-1",
            index=0,
            source_volume_type="per-volume-ssd",
        )

        kwargs = self.lifecycle.create_source_copy.call_args.kwargs
        self.assertEqual(kwargs["volume_type"], "per-volume-ssd")

    def test_move_falls_back_to_pool_target_type(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())
        self.mover.target_volume_type = "pool-default"

        self._run()

        kwargs = self.lifecycle.create_target_volume.call_args.kwargs
        self.assertEqual(kwargs["volume_type"], "pool-default")

    def test_prepare_only_snapshots_and_builds_target_volume(self):
        """准备阶段不碰数据面：不取中转机槽位、不挂载、不传输。"""
        prepared = self.mover.prepare(volume=_Volume(), vm_name="vm-1", index=3)

        self.assertEqual(prepared.target_volume_id, "vol-t1")
        self.assertEqual(prepared.copy.derived_volume_id, "vol-d1")
        self.assertEqual(self.ledger.get("job-1", "vol-s1").phase, "attaching_target")
        self.assertEqual(self.state.enqueued, [])
        self.lifecycle.attach.assert_not_called()
        # 目标卷名按盘序号区分，多块盘并发准备不会撞名。
        self.assertEqual(
            self.lifecycle.create_target_volume.call_args.kwargs["name"],
            "vm-1-vol-3",
        )

    def test_prepare_failure_reclaims_derived_copy(self):
        """派生卷建出来之后再失败（例如建目标卷配额拒绝）也要回收。"""
        self.lifecycle.create_target_volume.side_effect = RuntimeError("quota exceeded")

        with self.assertRaises(RuntimeError):
            self.mover.prepare(volume=_Volume(), vm_name="vm-1", index=0)

        self.lifecycle.cleanup_source_copy.assert_called_once()
        self.assertEqual(self.ledger.get("job-1", "vol-s1").phase, "failed")

    def test_prepare_failure_before_copy_skips_cleanup(self):
        self.lifecycle.create_source_copy.side_effect = RuntimeError("快照失败")

        with self.assertRaises(RuntimeError):
            self.mover.prepare(volume=_Volume(), vm_name="vm-1", index=0)

        self.lifecycle.cleanup_source_copy.assert_not_called()

    def test_discard_is_noop_without_copy(self):
        from relay_orchestrator import PreparedVolume

        self.mover.discard(
            PreparedVolume(
                volume=_Volume(), record=None, copy=None, target_volume_id="", index=0
            )
        )

        self.lifecycle.cleanup_source_copy.assert_not_called()

    def test_discard_survives_cleanup_error(self):
        """回收失败只记日志，不能盖掉调用方真正要抛的失败原因。"""
        from relay_orchestrator import PreparedVolume
        from relay_volumes import SourceCopy

        self.lifecycle.cleanup_source_copy.side_effect = RuntimeError("删不掉")

        self.mover.discard(
            PreparedVolume(
                volume=_Volume(),
                record=None,
                copy=SourceCopy(snapshot_id="s", derived_volume_id="d"),
                target_volume_id="",
                index=0,
            )
        )

    def test_move_cleans_up_and_releases_on_success(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        self.lifecycle.cleanup_source_copy.assert_called_once()
        self.lifecycle.detach.assert_any_call(
            role="target", server_id="srv-t", volume_id="vol-t1"
        )
        self.assertEqual(self.mover.source_pool.released, ["n-s"])
        self.assertEqual(self.mover.target_pool.released, ["n-t"])

    def test_move_drives_ledger_to_done(self):
        seen = []
        original_save = self.ledger.save

        def save():
            original_save()
            seen.extend(record.phase for record in self.ledger.records.values())

        self.ledger.save = save
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()
        self.ledger.save()

        self.assertIn("copying", seen)
        self.assertEqual(self.ledger.get("job-1", "vol-s1").phase, "done")

    def test_move_marks_ledger_failed_and_cleans_up(self):
        self.mover.derived_retention_window = 0.0
        original_enqueue = self.state.enqueue

        def enqueue(task):
            original_enqueue(task)
            self.state.mark_task_ready(task["task_id"])
            self.state.record_result(task["task_id"], "failed", 0)

        self.state.enqueue = enqueue

        with self.assertRaises(RuntimeError):
            self._run()

        self.assertEqual(self.ledger.get("job-1", "vol-s1").phase, "failed")
        self.lifecycle.cleanup_source_copy.assert_called_once()
        self.assertEqual(self.mover.source_pool.released, ["n-s"])

    def test_transfer_failure_retains_derived_copy_within_window(self):
        """失败后不删派生卷/快照：保留期内直接复用，省掉几小时的重新派生。"""
        self.mover.derived_retention_window = 3600.0
        original_enqueue = self.state.enqueue

        def enqueue(task):
            original_enqueue(task)
            self.state.mark_task_ready(task["task_id"])
            self.state.record_result(task["task_id"], "failed", 0)

        self.state.enqueue = enqueue

        with self.assertRaises(RuntimeError):
            self._run()

        record = self.ledger.get("job-1", "vol-s1")
        self.assertEqual(record.phase, "failed_retained")
        self.assertGreater(record.retained_until, time.time())
        self.assertIn("RuntimeError", record.retain_reason)
        self.lifecycle.cleanup_source_copy.assert_not_called()
        # 派生卷与目标卷都要先从中转机卸下来，重试时才能重新 attach。
        parked = [
            call.kwargs
            for call in self.lifecycle.park_volume.call_args_list
        ]
        self.assertIn(
            {"role": "source", "server_id": "srv-s", "volume_id": "vol-d1"},
            [{k: v for k, v in item.items()} for item in parked],
        )
        self.assertIn(
            "target",
            [item.get("role") for item in parked],
        )

    def test_retained_info_exposes_deadline_and_empty_when_not_retained(self):
        self.assertEqual(self.mover.retained_info("vol-s1"), {})

        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="failed_retained",
            snapshot_id="snap-1",
            derived_volume_id="vol-d1",
            retained_until=12345.0,
            retain_reason="TimeoutError: attach",
        )
        self.ledger.records[record.key] = record

        info = self.mover.retained_info("vol-s1")

        self.assertEqual(info["retained_until"], 12345.0)
        self.assertEqual(info["snapshot_id"], "snap-1")
        self.assertEqual(info["derived_volume_id"], "vol-d1")
        self.assertEqual(info["retain_reason"], "TimeoutError: attach")

    def test_prepare_reuses_retained_copy_when_validation_passes(self):
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="failed_retained",
            snapshot_id="snap-old",
            derived_volume_id="vol-d-old",
            retained_until=time.time() + 3600,
        )
        self.ledger.records[record.key] = record
        self.lifecycle.validate_source_copy.return_value = (True, "可复用")

        prepared = self.mover.prepare(volume=_Volume(), vm_name="vm-1", index=0)

        self.lifecycle.create_source_copy.assert_not_called()
        self.assertEqual(prepared.copy.snapshot_id, "snap-old")
        self.assertEqual(prepared.copy.derived_volume_id, "vol-d-old")
        self.assertEqual(self.ledger.get("job-1", "vol-s1").retained_until, 0.0)
        self.assertEqual(
            self.lifecycle.validate_source_copy.call_args.kwargs["volume_id"],
            "vol-s1",
        )

    def test_prepare_rebuilds_when_retained_copy_validation_fails(self):
        """校验不过（卷被删/类型不符）时自动回退重建，并先清掉失效残留。"""
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="failed_retained",
            snapshot_id="snap-stale",
            derived_volume_id="vol-d-stale",
            retained_until=time.time() + 3600,
        )
        self.ledger.records[record.key] = record
        self.lifecycle.validate_source_copy.return_value = (False, "派生卷已被删除")

        prepared = self.mover.prepare(volume=_Volume(), vm_name="vm-1", index=0)

        self.lifecycle.create_source_copy.assert_called_once()
        self.assertEqual(prepared.copy.derived_volume_id, "vol-d1")
        self.lifecycle.cleanup_source_copy.assert_called_once()
        stale = self.lifecycle.cleanup_source_copy.call_args.args[0]
        self.assertEqual(stale.derived_volume_id, "vol-d-stale")
        self.assertEqual(stale.snapshot_id, "snap-stale")

    def test_prepare_rebuilds_after_retention_window_expired(self):
        """保留期已过就不再复用，省得把过期指针当成有效缓存。"""
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="failed_retained",
            snapshot_id="snap-old",
            derived_volume_id="vol-d-old",
            retained_until=time.time() - 10,
        )
        self.ledger.records[record.key] = record

        prepared = self.mover.prepare(volume=_Volume(), vm_name="vm-1", index=0)

        self.lifecycle.validate_source_copy.assert_not_called()
        self.lifecycle.create_source_copy.assert_called_once()
        self.assertEqual(prepared.copy.derived_volume_id, "vol-d1")

    def test_prepare_failure_keeps_reused_copy_for_next_retry(self):
        """复用的派生卷不会因为"建目标卷失败"被删掉，否则每一次重试都白等几小时。"""
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="failed_retained",
            snapshot_id="snap-old",
            derived_volume_id="vol-d-old",
            retained_until=time.time() + 3600,
        )
        self.ledger.records[record.key] = record
        self.lifecycle.validate_source_copy.return_value = (True, "可复用")
        self.lifecycle.create_target_volume.side_effect = RuntimeError("quota exceeded")

        with self.assertRaises(RuntimeError):
            self.mover.prepare(volume=_Volume(), vm_name="vm-1", index=0)

        self.lifecycle.cleanup_source_copy.assert_not_called()
        self.assertEqual(record.phase, "failed_retained")
        self.assertGreater(record.retained_until, time.time())
        self.assertIn("准备阶段失败", record.retain_reason)

    def test_prepare_reuses_existing_target_volume_when_valid(self):
        self.target_os.get_volume.return_value = mock.Mock(
            status="available", size=4096
        )

        prepared = self.mover.prepare(
            volume=_Volume(),
            vm_name="vm-1",
            index=0,
            reuse_target_volume_id="vol-t-old",
        )

        self.assertEqual(prepared.target_volume_id, "vol-t-old")
        self.lifecycle.create_target_volume.assert_not_called()

    def test_prepare_rebuilds_target_volume_when_it_disappeared(self):
        """目标卷被云侧删掉后继续沿用会每次 404；校验失败要回退新建。"""
        self.target_os.get_volume.side_effect = RuntimeError("404 not found")

        prepared = self.mover.prepare(
            volume=_Volume(),
            vm_name="vm-1",
            index=0,
            reuse_target_volume_id="vol-t-gone",
        )

        self.assertEqual(prepared.target_volume_id, "vol-t1")
        self.lifecycle.create_target_volume.assert_called_once()

    def test_manual_retry_reuses_retained_copy_end_to_end(self):
        """失败 → 人工重试：第二轮不再打快照/派生，直接复用后传完。"""
        from relay_ledger import VolumeTaskRecord

        self.mover.derived_retention_window = 3600.0
        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="failed_retained",
            snapshot_id="snap-old",
            derived_volume_id="vol-d-old",
            retained_until=time.time() + 3600,
        )
        self.ledger.records[record.key] = record
        self.lifecycle.validate_source_copy.return_value = (True, "可复用")
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (
            original_enqueue(task),
            self._auto_complete(),
        )

        result = self._run()

        self.lifecycle.create_source_copy.assert_not_called()
        self.assertEqual(record.phase, "done")
        self.assertEqual(result["target_volume_id"], "vol-t1")

    def test_move_raises_when_pool_exhausted(self):
        self.mover.source_pool = _FakePool([])

        with self.assertRaises(RuntimeError):
            self._run()

    def test_move_verifies_both_ends_and_detects_mismatch(self):
        def enqueue(task):
            self.state.enqueued.append(task)
            if task.get("kind") == "verify":
                digest = "src" if task["role"] == "source" else "dst"
                self.state.record_result(task["task_id"], "done", 4096, digest=digest)
                return
            self.state.mark_task_ready(task["task_id"])
            self.state.record_result(task["task_id"], "done", 4096)

        self.state.enqueue = enqueue

        with self.assertRaises(RuntimeError) as ctx:
            self._run()

        self.assertIn("摘要不一致", str(ctx.exception))

    def test_move_verifies_and_accepts_matching_digest(self):
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run()

        verify_tasks = [t for t in self.state.enqueued if t.get("kind") == "verify"]
        self.assertEqual(len(verify_tasks), 2)
        self.assertEqual({t["role"] for t in verify_tasks}, {"source", "target"})
        self.assertEqual(self.ledger.get("job-1", "vol-s1").phase, "done")

    def test_move_retries_once_and_resumes_from_recorded_offset(self):
        original_enqueue = self.state.enqueue
        failed_once = {"done": False}

        def enqueue(task):
            original_enqueue(task)
            if task.get("kind") == "verify":
                self.state.record_result(task["task_id"], "done", 4096, digest="same")
                return
            self.state.mark_task_ready(task["task_id"])
            if task["role"] == "source" and not failed_once["done"]:
                failed_once["done"] = True
                self.state.record_result(task["task_id"], "failed", 0)
                return
            self.state.record_result(task["task_id"], "done", 4096)

        self.state.enqueue = enqueue

        self._run()

        record = self.ledger.get("job-1", "vol-s1")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(record.phase, "done")
        self.assertEqual(len(self._copy_tasks()), 4)

    def test_move_does_not_retry_after_cancel(self):
        original_enqueue = self.state.enqueue

        def enqueue(task):
            original_enqueue(task)
            self.state.mark_task_ready(task["task_id"])
            status = "cancelled" if task["role"] == "source" else "done"
            self.state.record_result(task["task_id"], status, 0)

        self.state.enqueue = enqueue

        with self.assertRaises(CopyCancelled):
            self._run()

        self.assertEqual(len(self._copy_tasks()), 2)
        self.assertEqual(self.ledger.get("job-1", "vol-s1").phase, "failed")

    def test_move_resumes_from_recorded_offset(self):
        # copied_bytes 与 offset/length 都是字节
        record = self.VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            copied_bytes=1024 * 1024 ** 3,
        )
        self.ledger.upsert(record)
        original_enqueue = self.state.enqueue
        self.state.enqueue = lambda task: (original_enqueue(task), self._auto_complete())

        self._run(volume=_Volume(size=4096))

        source_task = self._copy_tasks()[-1]
        self.assertEqual(source_task["offset"], 1024 * 1024 ** 3)
        self.assertEqual(source_task["length"], 3072 * 1024 ** 3)


class PinnedDispatchTest(unittest.TestCase):
    def setUp(self):
        from relay_registry import RelayState

        self.state = RelayState(secret=b"secret")

    def _agent(self, name, role="source"):
        return self.state.register(
            job_id="job-1", role=role, name=name, version="1.0.0",
            address="10.0.0.1", now=1000.0,
        )

    def test_dispatch_skips_task_pinned_to_other_agent(self):
        agent_a = self._agent("relay-source-0")
        agent_b = self._agent("relay-source-1")
        self.state.enqueue(
            {"task_id": "t-b", "role": "source", "agent_id": agent_b.agent_id}
        )
        self.state.enqueue(
            {"task_id": "t-a", "role": "source", "agent_id": agent_a.agent_id}
        )

        task = self.state.dispatch(agent_a.session_id)

        self.assertEqual(task["task_id"], "t-a")
        self.assertIsNotNone(self.state.task("t-b"))

    def test_dispatch_returns_unpinned_task_to_any_agent(self):
        agent = self._agent("relay-source-0")
        self.state.enqueue({"task_id": "t-1", "role": "source"})

        self.assertEqual(self.state.dispatch(agent.session_id)["task_id"], "t-1")

    def test_record_and_read_task_result(self):
        self.assertIsNone(self.state.task_result("t-1"))
        self.state.record_result("t-1", "done", 4096)
        self.assertEqual(self.state.task_result("t-1")["copied_bytes"], 4096)


class AttachmentDeviceTest(unittest.TestCase):
    def setUp(self):
        from openstack_utils import OpenStackUtils

        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)
        self.sleeper = _Sleeper()

    def test_wait_attachment_device_returns_device(self):
        self.conn.compute.volume_attachments.return_value = [
            mock.Mock(id="att-1", volume_id="vol-1", device="/dev/vdb")
        ]

        device = self.os_utils.wait_attachment_device(
            "srv-1", "vol-1", sleeper=self.sleeper, timeout=1
        )

        self.assertEqual(device, "/dev/vdb")

    def test_wait_attachment_device_retries_until_device_present(self):
        self.conn.compute.volume_attachments.side_effect = [
            [mock.Mock(id="att-1", volume_id="vol-1", device="")],
            [mock.Mock(id="att-1", volume_id="vol-1", device="/dev/vdb")],
        ]

        device = self.os_utils.wait_attachment_device(
            "srv-1", "vol-1", sleeper=self.sleeper, timeout=5
        )

        self.assertEqual(device, "/dev/vdb")
        self.assertEqual(self.sleeper.calls, 1)

    def test_wait_attachment_device_times_out(self):
        self.conn.compute.volume_attachments.return_value = []

        with self.assertRaises(TimeoutError):
            self.os_utils.wait_attachment_device(
                "srv-1", "vol-1", sleeper=self.sleeper, timeout=0
            )


class DerivedRetentionWindowTest(unittest.TestCase):
    def test_defaults_to_24_hours(self):
        from relay_orchestrator import derived_retention_seconds

        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(derived_retention_seconds(), 24 * 3600.0)

    def test_env_override_and_invalid_value(self):
        from relay_orchestrator import derived_retention_seconds

        with mock.patch.dict(
            "os.environ", {"MIGRATION_DERIVED_RETENTION_HOURS": "6"}, clear=True
        ):
            self.assertEqual(derived_retention_seconds(), 6 * 3600.0)
        with mock.patch.dict(
            "os.environ", {"MIGRATION_DERIVED_RETENTION_HOURS": "0"}, clear=True
        ):
            self.assertEqual(derived_retention_seconds(), 0.0)
        with mock.patch.dict(
            "os.environ", {"MIGRATION_DERIVED_RETENTION_HOURS": "6h"}, clear=True
        ):
            self.assertEqual(derived_retention_seconds(), 24 * 3600.0)

    def test_mover_reads_env_when_no_override(self):
        from relay_orchestrator import RelayVolumeMover

        with mock.patch.dict(
            "os.environ", {"MIGRATION_DERIVED_RETENTION_HOURS": "2"}, clear=True
        ):
            mover = RelayVolumeMover(
                source_pool=_FakePool([]),
                target_pool=_FakePool([]),
                lifecycle=mock.MagicMock(),
                state=_FakeState(),
                ledger=_FakeLedger(),
                job_id="job-1",
            )
        self.assertEqual(mover.derived_retention_window, 2 * 3600.0)


class SlotWaitSnapshotTest(unittest.TestCase):
    """排队可视化：等槽位的盘要能被页面查到（位次/已等待/上限/VM）。"""

    def setUp(self):
        from relay_ledger import VolumeTaskRecord
        from relay_volumes import SourceCopy

        self.VolumeTaskRecord = VolumeTaskRecord
        self.SourceCopy = SourceCopy
        self.lifecycle = mock.MagicMock()
        self.lifecycle.source_os = mock.MagicMock()
        self.lifecycle.target_os = mock.MagicMock()
        self.lifecycle.source_os.wait_attachment_device.return_value = "/dev/vdb"
        self.lifecycle.target_os.wait_attachment_device.return_value = "/dev/vdc"
        self.lifecycle.create_source_copy.return_value = SourceCopy(
            snapshot_id="snap-1", derived_volume_id="vol-d1"
        )
        self.lifecycle.create_target_volume.return_value = "vol-t1"

    def test_waiting_disk_is_reported_with_position(self):
        from relay_orchestrator import (
            RelayVolumeMover,
            _clear_slot_wait,
            slot_wait_snapshot,
        )

        pool = _FakePool([])
        # 假池"迟早会释放"：等到表单上限就返回 None，测试不会挂死。
        pool.has_live_holder = lambda: True
        mover = RelayVolumeMover(
            source_pool=pool,
            target_pool=pool,
            lifecycle=self.lifecycle,
            state=_FakeState(),
            ledger=_FakeLedger(),
            job_id="job-wait",
            sleeper=_Sleeper(),
            slot_wait_timeout=0.05,
            poll_interval=0.02,
        )
        try:
            node = mover._acquire_with_wait(pool, "vol-1", vm_id="vm-1")
            self.assertIsNone(node)
            # 超时退出后必须清掉登记，否则页面会一直显示"排队中"。
            self.assertEqual(slot_wait_snapshot("job-wait"), [])
        finally:
            _clear_slot_wait("job-wait", "vol-1")

    def test_registry_reports_entry_fields(self):
        from relay_orchestrator import (
            _clear_slot_wait,
            _register_slot_wait,
            slot_wait_snapshot,
        )

        _register_slot_wait(
            job_id="job-x", task_id="vol-1", role="target",
            position=2, waited=95.0, limit=600.0, vm_id="vm-1",
        )
        try:
            items = slot_wait_snapshot("job-x")

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["vm_id"], "vm-1")
            self.assertEqual(items[0]["position"], 2)
            self.assertEqual(items[0]["waited"], 95.0)
            self.assertEqual(items[0]["limit"], 600.0)
            self.assertEqual(items[0]["role"], "target")
        finally:
            _clear_slot_wait("job-x", "vol-1")
        self.assertEqual(slot_wait_snapshot("job-x"), [])

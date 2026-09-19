import unittest
from unittest import mock

from relay_orchestrator import CopyCancelled, RelayVolumeMover
from relay_runtime import SchedulerPoolAdapter, parse_relay_options


class SlotWaitTest(unittest.TestCase):
    """ephemeral 池大小小于并发时，应当等槽位而不是立刻把 VM 判失败。"""

    def _mover(self, *, slot_wait_timeout: float, poll_interval: float = 1.0):
        now = {"value": 0.0}
        slept: list[float] = []

        def clock():
            return now["value"]

        def sleeper(seconds):
            slept.append(seconds)
            now["value"] += seconds

        mover = RelayVolumeMover(
            source_pool=mock.MagicMock(),
            target_pool=mock.MagicMock(),
            lifecycle=mock.MagicMock(),
            state=mock.MagicMock(),
            ledger=mock.MagicMock(),
            job_id="job-1",
            slot_wait_timeout=slot_wait_timeout,
            poll_interval=poll_interval,
            sleeper=sleeper,
            clock=clock,
        )
        return mover, slept, now

    def test_returns_node_immediately_when_available(self):
        mover, slept, _ = self._mover(slot_wait_timeout=60)
        pool = mock.MagicMock()
        pool.acquire.return_value = "node-1"

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertEqual(node, "node-1")
        self.assertEqual(slept, [])

    def test_waits_until_slot_is_released(self):
        mover, slept, _ = self._mover(slot_wait_timeout=60)
        pool = mock.MagicMock()
        pool.acquire.side_effect = [None, None, None, "node-1"]

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertEqual(node, "node-1")
        # 预判一次 + 排队循环里两次才拿到（FIFO 队首才尝试 acquire）。
        self.assertEqual(pool.acquire.call_count, 4)
        self.assertEqual(slept, [1.0, 1.0])

    def test_gives_up_after_timeout(self):
        mover, _, now = self._mover(slot_wait_timeout=5, poll_interval=2.0)
        pool = mock.MagicMock()
        pool.acquire.return_value = None

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertIsNone(node)
        self.assertGreaterEqual(now["value"], 5)

    def test_zero_timeout_waits_without_deadline(self):
        """0 = 不限时：池里还有持有者就一直等，拿到槽位再继续。"""
        mover, slept, _ = self._mover(slot_wait_timeout=0)
        pool = mock.MagicMock()
        pool.acquire.side_effect = [None, None, None, "node-1"]

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertEqual(node, "node-1")
        self.assertEqual(len(slept), 2)

    def test_no_live_holder_fails_fast_even_when_unlimited(self):
        """池为空/全部失联时无人会释放，0 = 不限时也不能永久挂起。"""
        mover, slept, _ = self._mover(slot_wait_timeout=0)
        pool = mock.MagicMock()
        pool.acquire.return_value = None
        pool.has_live_holder.return_value = False

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertIsNone(node)
        self.assertEqual(slept, [])

    def test_finite_timeout_waits_despite_transient_agent_gap(self):
        """配了有限超时时，全池 agent 暂时失联也要等满，不能提前判死。"""
        mover, slept, _ = self._mover(slot_wait_timeout=5, poll_interval=2.0)
        pool = mock.MagicMock()
        pool.acquire.return_value = None
        pool.has_live_holder.return_value = False

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertIsNone(node)
        self.assertGreaterEqual(len(slept), 2)

    def test_cancel_breaks_unlimited_wait(self):
        """不限时排队必须能被取消，否则作业取消后线程一直挂着。"""
        mover, _, _ = self._mover(slot_wait_timeout=0)
        mover.should_stop = lambda: True
        pool = mock.MagicMock()
        pool.acquire.return_value = None

        with self.assertRaises(CopyCancelled):
            mover._acquire_with_wait(pool, "vol-1")


class SlotWaitConfigTest(unittest.TestCase):
    def _options(self, **overrides):
        options = {
            "data_channel": "relay",
            "relay_platform_url": "https://platform.example.com",
            "relay_source_image": "img-src",
            "relay_source_flavor": "flv-src",
            "relay_source_az": "az1",
            "relay_source_network": "net-src",
            "relay_source_system_volume_type": "vt-src",
            "relay_source_size": "1",
            "relay_target_image": "img-tgt",
            "relay_target_flavor": "flv-tgt",
            "relay_target_az": "az2",
            "relay_target_network": "net-tgt",
            "relay_target_system_volume_type": "vt-tgt",
            "relay_target_size": "1",
        }
        options.update(overrides)
        return options

    def test_defaults_to_half_hour(self):
        config = parse_relay_options(self._options())

        self.assertEqual(config.slot_wait_timeout, 1800.0)

    def test_reads_form_override(self):
        config = parse_relay_options(
            self._options(relay_slot_wait_seconds="60")
        )

        self.assertEqual(config.slot_wait_timeout, 60.0)

    def test_zero_means_unlimited(self):
        """表单填 0 是"不限时"，不能再被 ``or 1800`` 吃掉。"""
        config = parse_relay_options(
            self._options(relay_slot_wait_seconds="0")
        )

        self.assertEqual(config.slot_wait_timeout, 0.0)

    def test_ephemeral_pool_expands_to_transfer_concurrency(self):
        """临时池大小小于传输并发时自动对齐，避免多出来的盘白等半小时。"""
        config = parse_relay_options(
            self._options(
                relay_transfer_concurrency="3",
                relay_source_size="1",
                relay_target_size="2",
            )
        )

        self.assertEqual(config.source.size, 3)
        self.assertEqual(config.target.size, 3)

    def test_explicit_ports_disable_pool_expansion(self):
        """显式给了端口就不自动扩池，否则会有节点拿不到网络。"""
        config = parse_relay_options(
            self._options(
                relay_transfer_concurrency="3",
                relay_source_size="1",
                relay_target_size="1",
                relay_source_ports="p1",
                relay_target_ports="q1",
            )
        )

        self.assertEqual(config.source.size, 1)
        self.assertEqual(config.target.size, 1)

    def test_persistent_mode_keeps_configured_size(self):
        """常驻池有自己的容量模型（slots_per_node × max_nodes），不参与扩池。"""
        config = parse_relay_options(
            self._options(
                relay_node_mode="persistent",
                relay_transfer_concurrency="4",
                relay_source_size="1",
                relay_target_size="1",
            )
        )

        self.assertEqual(config.source.size, 1)
        self.assertEqual(config.target.size, 1)

    def test_attach_timeout_field_defaults_to_none(self):
        config = parse_relay_options(self._options())

        self.assertIsNone(config.attach_ready_timeout)

    def test_attach_timeout_field_reads_form(self):
        config = parse_relay_options(
            self._options(relay_attach_ready_timeout="600")
        )

        self.assertEqual(config.attach_ready_timeout, 600.0)


class PersistentSlotAdapterTest(unittest.TestCase):
    """常驻池池满时必须返回 None，让搬运器统一排队，而不是抛异常跳过。"""

    def _adapter(self, scheduler):
        return SchedulerPoolAdapter(
            scheduler, job_id="job-1", role="source", tenant_key="t1", az="nova-1"
        )

    def test_returns_none_when_scheduler_has_no_capacity(self):
        scheduler = mock.MagicMock()
        scheduler.try_acquire.return_value = None

        node = self._adapter(scheduler).acquire("vol-1")

        self.assertIsNone(node)
        scheduler.try_acquire.assert_called_once()

    def test_returns_node_from_try_acquire(self):
        scheduler = mock.MagicMock()
        scheduler.try_acquire.return_value = "node-1"

        node = self._adapter(scheduler).acquire("vol-1")

        self.assertEqual(node, "node-1")

    def test_has_live_holder_reports_unreachable_pool(self):
        scheduler = mock.MagicMock()
        scheduler.describe.return_value = ["relay-source-0=busy(agent-unreachable)"]

        self.assertFalse(self._adapter(scheduler).has_live_holder())

        scheduler.describe.return_value = ["relay-source-0=busy"]
        self.assertTrue(self._adapter(scheduler).has_live_holder())

    def test_node_not_ready_failure_is_not_swallowed(self):
        """建机后 agent 注册不上是硬故障，不能当成"池满"无限重试建机。"""
        from relay_scheduler import NodeNotReadyError

        scheduler = mock.MagicMock()
        scheduler.try_acquire.side_effect = NodeNotReadyError(
            "新建中转机在 300s 内未注册成功"
        )

        with self.assertRaises(NodeNotReadyError):
            self._adapter(scheduler).acquire("vol-1")

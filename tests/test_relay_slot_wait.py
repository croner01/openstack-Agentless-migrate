import unittest
from unittest import mock

from relay_orchestrator import RelayVolumeMover
from relay_runtime import parse_relay_options


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
        pool.acquire.side_effect = [None, None, "node-1"]

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertEqual(node, "node-1")
        self.assertEqual(pool.acquire.call_count, 3)
        self.assertEqual(slept, [1.0, 1.0])

    def test_gives_up_after_timeout(self):
        mover, _, now = self._mover(slot_wait_timeout=5, poll_interval=2.0)
        pool = mock.MagicMock()
        pool.acquire.return_value = None

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertIsNone(node)
        self.assertGreaterEqual(now["value"], 5)

    def test_zero_timeout_keeps_fail_fast_behaviour(self):
        mover, slept, _ = self._mover(slot_wait_timeout=0)
        pool = mock.MagicMock()
        pool.acquire.return_value = None

        node = mover._acquire_with_wait(pool, "vol-1")

        self.assertIsNone(node)
        self.assertEqual(slept, [])


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

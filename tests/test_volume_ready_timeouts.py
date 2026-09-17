import unittest
from unittest import mock

from openstack_utils import (
    derive_ready_timeout,
    snapshot_ready_timeout_default,
    volume_ready_timeout_default,
)


class ReadyTimeoutDefaultsTest(unittest.TestCase):
    """默认不限时：云上还在建盘，平台不能按时间判死并回收资源。"""

    def test_snapshot_default_is_unlimited(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(snapshot_ready_timeout_default(), 0)

    def test_volume_default_is_unlimited(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(volume_ready_timeout_default(), 0)

    def test_snapshot_env_override_is_a_hard_cap(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_SNAPSHOT_READY_TIMEOUT": "1200"}, clear=True
        ):
            self.assertEqual(snapshot_ready_timeout_default(), 1200)

    def test_volume_env_override_is_a_hard_cap(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_VOLUME_READY_TIMEOUT": "7200"}, clear=True
        ):
            self.assertEqual(volume_ready_timeout_default(), 7200)

    def test_invalid_env_falls_back_to_unlimited(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_SNAPSHOT_READY_TIMEOUT": "10m"}, clear=True
        ):
            self.assertEqual(snapshot_ready_timeout_default(), 0)


class DeriveReadyTimeoutTest(unittest.TestCase):
    """显式配了上限时，商业存储的「快照 → 派生卷」按容量放大等待。"""

    def test_zero_base_stays_unlimited(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(derive_ready_timeout(0, 2048), 0.0)
            self.assertEqual(derive_ready_timeout(0, 0), 0.0)
            self.assertEqual(derive_ready_timeout(None, 2048), 0.0)
            self.assertEqual(derive_ready_timeout(-1, 2048), 0.0)

    def test_base_timeout_is_the_floor(self):
        """小卷不放大（低于基线就取基线），大卷按容量抬升。"""
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(derive_ready_timeout(600, 10), 600)
            self.assertEqual(derive_ready_timeout(600, 40), 800)

    def test_large_volume_scales_with_size(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            # 默认 20s/GiB：1024GiB ⇒ 20480s，远高于 600s 基线
            self.assertEqual(derive_ready_timeout(600, 1024), 20480)

    def test_seconds_per_gib_is_configurable(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_DERIVE_SECONDS_PER_GIB": "5"}, clear=True
        ):
            self.assertEqual(derive_ready_timeout(600, 1024), 5120)

    def test_zero_or_unknown_size_keeps_base(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(derive_ready_timeout(600, 0), 600)
            self.assertEqual(derive_ready_timeout(600, None), 600)


class WaitVolumeStatusUnlimitedTest(unittest.TestCase):
    """timeout <= 0 必须真的"一直等"，不能算成 deadline 立即到点。"""

    def _os_utils(self):
        from openstack_utils import OpenStackUtils

        conn = mock.MagicMock()
        return OpenStackUtils(conn=conn), conn

    def test_zero_timeout_keeps_polling(self):
        os_utils, conn = self._os_utils()
        conn.block_storage.get_volume.side_effect = [
            mock.Mock(status="creating"),
            mock.Mock(status="creating"),
            mock.Mock(status="available"),
        ]

        with mock.patch("openstack_utils.time.sleep"):
            volume = os_utils.wait_volume_status(
                "vol-1", timeout=0, poll_interval=1
            )

        self.assertEqual(volume.status, "available")
        self.assertEqual(conn.block_storage.get_volume.call_count, 3)

    def test_zero_timeout_snapshot_keeps_polling(self):
        os_utils, conn = self._os_utils()
        conn.block_storage.get_snapshot.side_effect = [
            mock.Mock(status="creating"),
            mock.Mock(status="available"),
        ]

        with mock.patch("openstack_utils.time.sleep"):
            snapshot = os_utils.wait_snapshot_status(
                "snap-1", timeout=0, poll_interval=1
            )

        self.assertEqual(snapshot.status, "available")
        self.assertEqual(conn.block_storage.get_snapshot.call_count, 2)

    def test_should_stop_interrupts_volume_wait(self):
        os_utils, conn = self._os_utils()
        conn.block_storage.get_volume.return_value = mock.Mock(status="creating")

        with mock.patch("openstack_utils.time.sleep"):
            with self.assertRaises(RuntimeError) as ctx:
                os_utils.wait_volume_status(
                    "vol-1",
                    timeout=0,
                    poll_interval=1,
                    context="（源卷派生 mig-vm-1-0）",
                    should_stop=lambda: True,
                )

        self.assertIn("被取消", str(ctx.exception))
        self.assertIn("mig-vm-1-0", str(ctx.exception))
        conn.block_storage.get_volume.assert_not_called()

    def test_should_stop_interrupts_snapshot_wait(self):
        os_utils, conn = self._os_utils()
        conn.block_storage.get_snapshot.side_effect = [
            mock.Mock(status="creating"),
            mock.Mock(status="creating"),
        ]
        calls = {"n": 0}

        def should_stop():
            calls["n"] += 1
            return calls["n"] > 1

        with mock.patch("openstack_utils.time.sleep"):
            with self.assertRaises(RuntimeError) as ctx:
                os_utils.wait_snapshot_status(
                    "snap-1", timeout=0, poll_interval=1, should_stop=should_stop
                )

        self.assertIn("被取消", str(ctx.exception))

    def test_cancelled_wait_is_not_a_timeout(self):
        """取消要走 RuntimeError，避免上层把它当成"超时重试"再等一轮。"""
        os_utils, conn = self._os_utils()
        conn.block_storage.get_volume.return_value = mock.Mock(status="creating")

        with mock.patch("openstack_utils.time.sleep"):
            with self.assertRaises(RuntimeError) as ctx:
                os_utils.wait_volume_status(
                    "vol-1", timeout=60, should_stop=lambda: True
                )

        self.assertNotIsInstance(ctx.exception, TimeoutError)


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest import mock

from openstack_utils import (
    derive_ready_timeout,
    snapshot_ready_timeout_default,
    volume_ready_timeout_default,
)


class SnapshotReadyTimeoutDefaultTest(unittest.TestCase):
    def test_defaults_to_600_seconds(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(snapshot_ready_timeout_default(), 600)

    def test_env_override(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_SNAPSHOT_READY_TIMEOUT": "1200"}, clear=True
        ):
            self.assertEqual(snapshot_ready_timeout_default(), 1200)

    def test_invalid_env_falls_back(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_SNAPSHOT_READY_TIMEOUT": "10m"}, clear=True
        ):
            self.assertEqual(snapshot_ready_timeout_default(), 600)


class DeriveReadyTimeoutTest(unittest.TestCase):
    """商业存储上「快照 → 派生卷」往往是存储侧全量拷贝，耗时与卷大小成正比。"""

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

    def test_volume_ready_timeout_default_still_1800(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(volume_ready_timeout_default(), 1800)


if __name__ == "__main__":
    unittest.main()

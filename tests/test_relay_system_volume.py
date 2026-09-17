import unittest
from unittest import mock

from openstack_utils import CINDER_VOLUME_AZ, OpenStackUtils, relay_boot_volume_size
from openstack_utils import volume_ready_timeout_default
from relay_runtime import parse_relay_options

GIB = 1024**3


class BootVolumeSizeTest(unittest.TestCase):
    def test_adds_ten_gib_to_image_size(self):
        self.assertEqual(relay_boot_volume_size(10 * GIB), 20)

    def test_rounds_up_partial_gib(self):
        self.assertEqual(relay_boot_volume_size(int(10.5 * GIB)), 21)


class VolumeReadyTimeoutTest(unittest.TestCase):
    def test_defaults_to_unlimited(self):
        """默认不限时：商业存储建盘几分钟到几小时都有可能，不能按时间判死。"""
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(volume_ready_timeout_default(), 0)

    def test_reads_env_override(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_VOLUME_READY_TIMEOUT": "2400"}, clear=True
        ):
            self.assertEqual(volume_ready_timeout_default(), 2400)

    def test_ignores_invalid_env_value(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_VOLUME_READY_TIMEOUT": "abc"}, clear=True
        ):
            self.assertEqual(volume_ready_timeout_default(), 0)


class RelayServerBootVolumeTest(unittest.TestCase):
    """中转机必须从云硬盘启动：本地盘在这套环境里会因 etcd 锁失败而 resume 报错。"""

    def setUp(self):
        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)
        self.conn.image.get_image.return_value = mock.Mock(size=10 * GIB)
        self.conn.block_storage.create_volume.return_value = mock.Mock(
            id="vol-root-1", status="creating"
        )
        self.conn.block_storage.get_volume.return_value = mock.Mock(
            id="vol-root-1", status="available"
        )
        self.conn.compute.create_server.return_value = mock.Mock(id="srv-1")

    def _create(self, **overrides):
        kwargs = {
            "name": "relay-1",
            "image_id": "img-1",
            "flavor_id": "flv-1",
            "port_ids": ["port-1"],
            "availability_zone": "nova",
            "user_data": "#cloud-config\n",
            "system_volume_type": "vt-1",
        }
        kwargs.update(overrides)
        return self.os_utils.create_relay_server(**kwargs)

    def test_boots_from_volume_created_with_configured_type(self):
        self._create()

        volume = self.conn.block_storage.create_volume.call_args.kwargs
        self.assertEqual(volume["name"], "relay-1-root")
        self.assertEqual(volume["size"], 20)
        self.assertEqual(volume["image_id"], "img-1")
        self.assertEqual(volume["volume_type"], "vt-1")
        # 启动卷按 Cinder 的 AZ 落盘，不能把 Nova 的「nova」透传过去。
        self.assertEqual(volume["availability_zone"], CINDER_VOLUME_AZ)

        server = self.conn.compute.create_server.call_args.kwargs
        self.assertNotIn("image_id", server)
        bdm = server["block_device_mapping_v2"]
        self.assertEqual(len(bdm), 1)
        self.assertEqual(bdm[0]["uuid"], "vol-root-1")
        self.assertEqual(bdm[0]["source_type"], "volume")
        self.assertEqual(bdm[0]["destination_type"], "volume")
        self.assertEqual(bdm[0]["boot_index"], 0)
        self.assertTrue(bdm[0]["delete_on_termination"])
        self.assertEqual(server["networks"], [{"port": "port-1"}])

    def test_requires_system_volume_type(self):
        with self.assertRaises(ValueError) as ctx:
            self._create(system_volume_type="")

        self.assertIn("系统盘", str(ctx.exception))
        self.conn.block_storage.create_volume.assert_not_called()

    def test_deletes_boot_volume_when_server_create_fails(self):
        self.conn.compute.create_server.side_effect = RuntimeError("quota")

        with self.assertRaises(RuntimeError):
            self._create()

        self.conn.block_storage.delete_volume.assert_called_once_with("vol-root-1")

    def test_deletes_boot_volume_when_volume_enters_error(self):
        self.conn.block_storage.get_volume.return_value = mock.Mock(status="error")

        with self.assertRaises(RuntimeError):
            self._create()

        self.conn.block_storage.delete_volume.assert_called_once_with("vol-root-1")


class ForcedServerDeleteTest(unittest.TestCase):
    def test_delete_server_can_force_delete(self):
        conn = mock.MagicMock()
        os_utils = OpenStackUtils(conn=conn)

        os_utils.delete_server("srv-1", force=True)

        conn.compute.delete_server.assert_called_once_with("srv-1", force=True)

    def test_delete_server_defaults_to_plain_delete(self):
        conn = mock.MagicMock()
        os_utils = OpenStackUtils(conn=conn)

        os_utils.delete_server("srv-1")

        conn.compute.delete_server.assert_called_once_with("srv-1")


class DeleteVolumeWaitTest(unittest.TestCase):
    def test_retries_until_attachment_released(self):
        conn = mock.MagicMock()
        os_utils = OpenStackUtils(conn=conn)
        attempts = {"count": 0}

        def flaky(_volume_id):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise RuntimeError("Invalid volume: Volume ... must not be attached")

        conn.block_storage.delete_volume.side_effect = flaky
        slept: list[float] = []

        os_utils.delete_volume_wait(
            "vol-1", timeout=60, poll_interval=1, sleeper=slept.append
        )

        # 前两次 400，第三次成功。
        self.assertEqual(conn.block_storage.delete_volume.call_count, 3)
        self.assertEqual(slept, [1, 1])

    def test_gives_up_after_timeout(self):
        conn = mock.MagicMock()
        conn.block_storage.delete_volume.side_effect = RuntimeError("must not be attached")
        os_utils = OpenStackUtils(conn=conn)
        clock = iter([0.0, 0.0, 4.0, 8.0, 12.0])

        with self.assertRaises(RuntimeError):
            os_utils.delete_volume_wait(
                "vol-1",
                timeout=10,
                poll_interval=4,
                sleeper=lambda _seconds: None,
                monotonic=lambda: next(clock),
            )

        # 超时前每次失败都重试：t=0/4/8 重试，t=12 超过 10s 放弃。
        self.assertEqual(conn.block_storage.delete_volume.call_count, 4)


class ParseSystemVolumeTypeTest(unittest.TestCase):
    def _options(self, **overrides):
        options = {
            "data_channel": "relay",
            "relay_platform_url": "https://platform.example.com",
            "relay_source_image": "img-src",
            "relay_source_flavor": "flv-src",
            "relay_source_az": "az1",
            "relay_source_network": "net-src",
            "relay_source_system_volume_type": "vt-src",
            "relay_source_size": "2",
            "relay_target_image": "img-tgt",
            "relay_target_flavor": "flv-tgt",
            "relay_target_az": "az2",
            "relay_target_network": "net-tgt",
            "relay_target_system_volume_type": "vt-tgt",
            "relay_target_size": "2",
        }
        options.update(overrides)
        return options

    def test_parses_system_volume_types(self):
        config = parse_relay_options(self._options())

        self.assertEqual(config.source.system_volume_type, "vt-src")
        self.assertEqual(config.target.system_volume_type, "vt-tgt")

    def test_requires_system_volume_type_for_ephemeral_mode(self):
        with self.assertRaises(ValueError) as ctx:
            parse_relay_options(
                self._options(relay_source_system_volume_type="")
            )

        self.assertIn("系统盘", str(ctx.exception))

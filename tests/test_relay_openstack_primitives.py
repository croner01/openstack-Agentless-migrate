import base64
import unittest
from unittest import mock

from openstack_utils import CINDER_VOLUME_AZ, OpenStackUtils


class RelayPrimitivesTest(unittest.TestCase):
    def setUp(self):
        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)
        # 中转机走云硬盘启动：启动卷必须在超时前 available。
        self.conn.image.get_image.return_value = mock.Mock(size=10 * 1024**3)
        self.conn.block_storage.create_volume.return_value = mock.Mock(
            id="vol-root-1", status="creating"
        )
        self.conn.block_storage.get_volume.return_value = mock.Mock(
            id="vol-root-1", status="available"
        )
        self.conn.compute.create_server.return_value = mock.Mock(id="srv-1")

    def test_create_relay_server_passes_image_and_user_data(self):
        self.os_utils.create_relay_server(
            name="relay-1",
            image_id="img-1",
            flavor_id="flv-1",
            port_ids=["port-1", "port-2"],
            availability_zone="nova",
            user_data="#cloud-config\n",
            system_volume_type="vt-1",
        )

        kwargs = self.conn.compute.create_server.call_args.kwargs
        self.assertEqual(kwargs["name"], "relay-1")
        self.assertEqual(kwargs["flavor_id"], "flv-1")
        self.assertEqual(kwargs["networks"], [{"port": "port-1"}, {"port": "port-2"}])
        self.assertEqual(kwargs["availability_zone"], "nova")
        # Nova 要求 base64：解出来必须与传入的 cloud-init 完全一致。
        self.assertEqual(
            base64.b64decode(kwargs["user_data"]).decode("utf-8"), "#cloud-config\n"
        )
        # 启动盘来自镜像生成的云硬盘（本地盘会被 etcd 锁卡死）。
        volume = self.conn.block_storage.create_volume.call_args.kwargs
        self.assertEqual(volume["image_id"], "img-1")
        self.assertEqual(
            kwargs["block_device_mapping_v2"][0]["uuid"], "vol-root-1"
        )

    def test_create_relay_server_sends_valid_utf8_after_base64(self):
        cloud_init = (
            "#cloud-config\n"
            "hostname: relay-source-0\n"
            "write_files:\n"
            "  - path: /etc/relay/agent.env\n"
            "    content: |\n"
            "      RELAY_PASSWORD=中文密码\n"
        )

        self.os_utils.create_relay_server(
            name="relay-1",
            image_id="img-1",
            flavor_id="flv-1",
            port_ids=["port-1"],
            availability_zone="nova",
            user_data=cloud_init,
            system_volume_type="vt-1",
        )

        sent = self.conn.compute.create_server.call_args.kwargs["user_data"]
        # 这一步正是 Nova 内部做的动作：它必须能成功解出 UTF-8。
        self.assertEqual(base64.b64decode(sent).decode("utf-8"), cloud_init)

    def test_create_server_from_volumes_puts_boot_volume_first(self):
        self.os_utils.create_server_from_volumes(
            name="vm-t",
            flavor_id="flv-1",
            port_ids=["port-1"],
            boot_volume_id="vol-boot",
            data_volume_ids=["vol-data"],
            availability_zone="nova",
            admin_password="secret",
        )

        bdm = self.conn.compute.create_server.call_args.kwargs[
            "block_device_mapping_v2"
        ]
        self.assertEqual(bdm[0]["uuid"], "vol-boot")
        self.assertEqual(bdm[0]["source_type"], "volume")
        self.assertEqual(bdm[0]["boot_index"], 0)
        self.assertFalse(bdm[0]["delete_on_termination"])
        self.assertEqual(bdm[1]["uuid"], "vol-data")
        self.assertEqual(bdm[1]["boot_index"], -1)

    def test_set_volume_bootable_calls_block_storage_action(self):
        """空白卷默认 bootable=false，Nova 用 boot_index=0 挂载会直接 400。"""
        self.os_utils.set_volume_bootable("vol-t1")

        self.conn.block_storage.set_volume_bootable_status.assert_called_once_with(
            "vol-t1", True
        )

    def test_create_volume_snapshot_forces_in_use_volume(self):
        self.os_utils.create_volume_snapshot(volume_id="vol-1", name="snap-1")

        kwargs = self.conn.block_storage.create_snapshot.call_args.kwargs
        self.assertEqual(kwargs["volume_id"], "vol-1")
        self.assertEqual(kwargs["name"], "snap-1")
        self.assertTrue(kwargs["force"])

    def test_create_volume_from_snapshot_passes_optional_fields(self):
        self.os_utils.create_volume_from_snapshot(
            name="derived-1",
            snapshot_id="snap-1",
            size=40,
            volume_type="ssd",
        )

        kwargs = self.conn.block_storage.create_volume.call_args.kwargs
        self.assertEqual(kwargs["snapshot_id"], "snap-1")
        self.assertEqual(kwargs["size"], 40)
        self.assertEqual(kwargs["volume_type"], "ssd")
        # Cinder 与 Nova 的 AZ 是两套命名空间，派生卷固定落在 Cinder 的默认 AZ。
        self.assertEqual(kwargs["availability_zone"], CINDER_VOLUME_AZ)

    def test_create_volume_from_snapshot_omits_empty_volume_type(self):
        self.os_utils.create_volume_from_snapshot(
            name="derived-1", snapshot_id="snap-1", size=40
        )

        kwargs = self.conn.block_storage.create_volume.call_args.kwargs
        self.assertNotIn("volume_type", kwargs)
        self.assertEqual(kwargs["availability_zone"], CINDER_VOLUME_AZ)

    def test_create_blank_volume_always_uses_cinder_az(self):
        self.os_utils.create_blank_volume(name="blank-1", size=10, volume_type="ssd")

        kwargs = self.conn.block_storage.create_volume.call_args.kwargs
        self.assertEqual(kwargs["availability_zone"], CINDER_VOLUME_AZ)

    def test_attach_volume_returns_attachment_id(self):
        self.conn.compute.create_volume_attachment.return_value = mock.Mock(id="att-1")

        attachment_id = self.os_utils.attach_volume(
            server_id="srv-1", volume_id="vol-1"
        )

        self.assertEqual(attachment_id, "att-1")
        kwargs = self.conn.compute.create_volume_attachment.call_args.kwargs
        self.assertEqual(kwargs["server"], "srv-1")
        self.assertEqual(kwargs["volume"], "vol-1")

    def test_detach_volume_calls_compute_proxy(self):
        self.os_utils.detach_volume(server_id="srv-1", volume_id="vol-1")

        kwargs = self.conn.compute.delete_volume_attachment.call_args.kwargs
        self.assertEqual(kwargs["server"], "srv-1")
        self.assertEqual(kwargs["volume"], "vol-1")

    def test_find_volume_attachment_matches_volume(self):
        self.conn.compute.get_volume_attachment.return_value = mock.Mock(id="att-2")

        self.assertEqual(
            self.os_utils.find_volume_attachment("srv-1", "vol-1"), "att-2"
        )

    def test_find_volume_attachment_returns_none_when_missing(self):
        self.conn.compute.get_volume_attachment.side_effect = RuntimeError("not found")

        self.assertIsNone(self.os_utils.find_volume_attachment("srv-1", "vol-none"))

    def test_attach_volume_falls_back_to_legacy_method(self):
        proxy = mock.MagicMock(spec=["create_server_volume"])
        proxy.create_server_volume.return_value = mock.Mock(id="att-legacy")
        self.os_utils.conn.compute = proxy

        self.assertEqual(
            self.os_utils.attach_volume(server_id="srv-1", volume_id="vol-1"),
            "att-legacy",
        )

    def test_delete_volume_and_snapshot_call_block_storage(self):
        self.os_utils.delete_volume("vol-1")
        self.os_utils.delete_volume_snapshot("snap-1")

        self.assertEqual(
            self.conn.block_storage.delete_volume.call_args.args, ("vol-1",)
        )
        self.assertEqual(
            self.conn.block_storage.delete_snapshot.call_args.args, ("snap-1",)
        )

    def test_delete_server_calls_compute_proxy(self):
        self.os_utils.delete_server("srv-1")

        self.assertEqual(self.conn.compute.delete_server.call_args.args, ("srv-1",))

    def test_create_and_delete_relay_port(self):
        self.conn.network.create_port.return_value = mock.Mock(id="port-1")

        port = self.os_utils.create_relay_port("net-1")
        self.os_utils.delete_port("port-1")

        self.assertEqual(port.id, "port-1")
        self.assertEqual(
            self.conn.network.create_port.call_args.kwargs["network_id"], "net-1"
        )
        self.assertEqual(self.conn.network.delete_port.call_args.args, ("port-1",))

    def test_create_relay_port_with_subnet_and_fixed_ip(self):
        self.os_utils.create_relay_port(
            "net-1", subnet_id="subnet-1", fixed_ip="10.0.0.11"
        )

        kwargs = self.conn.network.create_port.call_args.kwargs
        self.assertEqual(
            kwargs["fixed_ips"],
            [{"subnet_id": "subnet-1", "ip_address": "10.0.0.11"}],
        )

    def test_create_relay_port_without_subnet_omits_fixed_ips(self):
        self.os_utils.create_relay_port("net-1")

        kwargs = self.conn.network.create_port.call_args.kwargs
        self.assertNotIn("fixed_ips", kwargs)

    def test_list_subnets_returns_cidr_and_network(self):
        first = mock.Mock(id="subnet-1", cidr="10.0.0.0/24", network_id="net-1")
        first.name = "business"
        second = mock.Mock(id="subnet-2", cidr="10.1.0.0/24", network_id="net-1")
        second.name = ""
        self.conn.network.subnets.return_value = [first, second]

        subnets = self.os_utils.list_subnets()

        self.assertEqual(
            subnets,
            [
                {"id": "subnet-1", "name": "business", "cidr": "10.0.0.0/24", "network_id": "net-1"},
                {"id": "subnet-2", "name": "subnet-2", "cidr": "10.1.0.0/24", "network_id": "net-1"},
            ],
        )

    def test_create_relay_server_logs_and_reraises(self):
        self.conn.compute.create_server.side_effect = RuntimeError(
            "'utf-8' codec can't decode byte 0xca in position 4"
        )

        with self.assertLogs(level="ERROR") as captured:
            with self.assertRaises(RuntimeError):
                self.os_utils.create_relay_server(
                    name="relay-source-0",
                    image_id="img-1",
                    flavor_id="flv-1",
                    port_ids=["port-1"],
                    availability_zone="az1",
                    user_data="#cloud-config\n",
                    admin_password="P@ssw0rd",
                    system_volume_type="vt-1",
                )

        logged = "\n".join(captured.output)
        self.assertIn("relay-source-0", logged)
        self.assertIn("flv-1", logged)
        self.assertNotIn("P@ssw0rd", logged)

    def test_wait_snapshot_status_returns_when_available(self):
        self.conn.block_storage.get_snapshot.return_value = mock.Mock(
            status="available"
        )

        snapshot = self.os_utils.wait_snapshot_status("snap-1")

        self.assertEqual(snapshot.status, "available")

    def test_wait_snapshot_status_raises_on_error(self):
        self.conn.block_storage.get_snapshot.return_value = mock.Mock(status="error")

        with self.assertRaises(RuntimeError):
            self.os_utils.wait_snapshot_status("snap-1")

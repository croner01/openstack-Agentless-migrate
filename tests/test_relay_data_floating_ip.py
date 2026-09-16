import tempfile
import unittest
from pathlib import Path
from unittest import mock

from openstack_utils import OpenStackUtils
from relay_credentials import CredentialStore, Sealer
from relay_inventory import NodeInventory
from relay_node_manager import RelayNodeManager
from relay_orchestrator import RelayVolumeMover
from relay_pool import build_cloud_init
from relay_pool import RelayPool
from relay_pool_profile import PoolProfile, PoolProfileStore
from relay_registry import RelayState
from relay_runtime import RelayRuntime, parse_relay_options

KEY = b"k" * 32


class FakePort:
    def __init__(self, port_id):
        self.id = port_id


class FakeServer:
    def __init__(self, server_id):
        self.id = server_id
        self.root_volume_id = "vol-root-1"


class FakeFloatingIP:
    def __init__(self, fip_id, address):
        self.id = fip_id
        self.floating_ip_address = address


class CloudInitDataAddressTest(unittest.TestCase):
    def test_sets_data_address_when_given(self):
        text = build_cloud_init(
            platform_url="https://platform.example.com",
            token="tok-1",
            job_id="job-1",
            role="target",
            name="relay-target-0",
            data_addr="203.0.113.9",
        )

        self.assertIn("RELAY_DATA_ADDR=203.0.113.9", text)

    def test_omits_data_address_by_default(self):
        text = build_cloud_init(
            platform_url="https://platform.example.com",
            token="tok-1",
            job_id="job-1",
            role="source",
            name="relay-source-0",
        )

        self.assertNotIn("RELAY_DATA_ADDR", text)


class FloatingIpPrimitivesTest(unittest.TestCase):
    def test_create_floating_ip_binds_relay_port(self):
        conn = mock.MagicMock()
        os_utils = OpenStackUtils(conn=conn)
        conn.network.create_ip.return_value = FakeFloatingIP("fip-1", "203.0.113.9")

        fip = os_utils.create_relay_floating_ip("port-1", "ext-net-1")

        conn.network.create_ip.assert_called_once_with(
            floating_network_id="ext-net-1", port_id="port-1"
        )
        self.assertEqual(fip.floating_ip_address, "203.0.113.9")

    def test_delete_floating_ip_calls_neutron(self):
        conn = mock.MagicMock()
        os_utils = OpenStackUtils(conn=conn)

        os_utils.delete_relay_floating_ip("fip-1")

        conn.network.delete_ip.assert_called_once_with("fip-1")


class NodeManagerFloatingIpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.profiles = PoolProfileStore.load(base / "pools.json")
        self.credentials = CredentialStore.load(base / "credentials.json", Sealer(KEY))
        self.credentials.save("t1", {"auth_url": "http://keystone", "password": "p"})
        self.credentials.flush()
        self.profiles.upsert(
            PoolProfile(
                tenant_key="t1",
                role="target",
                az="nova-1",
                image="img-1",
                flavor="flv-1",
                network="net-1",
                subnet="sub-1",
                system_volume_type="vt-1",
                data_floating_network="ext-net-1",
            )
        )
        self.os_utils = mock.MagicMock()
        self.os_utils.create_relay_port.return_value = FakePort("port-1")
        self.os_utils.create_relay_server.return_value = FakeServer("server-1")
        self.os_utils.create_relay_floating_ip.return_value = FakeFloatingIP(
            "fip-1", "203.0.113.9"
        )
        self.manager = RelayNodeManager(
            inventory=self.inventory,
            profiles=self.profiles,
            credentials=self.credentials,
            registry=mock.MagicMock(),
            secret=b"s" * 32,
            platform_url="https://migrate.example.com",
            os_utils_factory=lambda auth: self.os_utils,
            clock=lambda: 500.0,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_create_node_binds_floating_ip_and_injects_data_address(self):
        record = self.manager.create_node(
            tenant_key="t1", role="target", az="nova-1"
        )

        self.os_utils.create_relay_floating_ip.assert_called_once_with(
            "port-1", "ext-net-1"
        )
        user_data = self.os_utils.create_relay_server.call_args.kwargs["user_data"]
        self.assertIn("RELAY_DATA_ADDR=203.0.113.9", user_data)
        self.assertEqual(record.floating_ip, "203.0.113.9")
        self.assertEqual(record.floating_ip_id, "fip-1")

    def test_delete_node_releases_floating_ip_before_port(self):
        record = self.manager.create_node(
            tenant_key="t1", role="target", az="nova-1"
        )
        self.os_utils.reset_mock()

        self.manager.delete_node(record.node_id)

        self.os_utils.delete_relay_floating_ip.assert_called_once_with("fip-1")
        self.os_utils.delete_port.assert_called_once_with("port-1")


class ParseDataFloatingNetworkTest(unittest.TestCase):
    def _options(self, **overrides):
        options = {
            "data_channel": "relay",
            "relay_platform_url": "https://platform.example.com",
            "relay_source_image": "img-src",
            "relay_source_flavor": "flv-src",
            "relay_source_az": "az1",
            "relay_source_network": "net-src",
            "relay_source_system_volume_type": "vt-src",
            "relay_source_data_floating_network": "ext-src",
            "relay_source_size": "1",
            "relay_target_image": "img-tgt",
            "relay_target_flavor": "flv-tgt",
            "relay_target_az": "az2",
            "relay_target_network": "net-tgt",
            "relay_target_system_volume_type": "vt-tgt",
            "relay_target_data_floating_network": "ext-tgt",
            "relay_target_size": "1",
        }
        options.update(overrides)
        return options

    def test_parses_data_floating_network(self):
        config = parse_relay_options(self._options())

        self.assertEqual(config.source.data_floating_network, "ext-src")
        self.assertEqual(config.target.data_floating_network, "ext-tgt")


class RelayPoolDataAddressTest(unittest.TestCase):
    def _pool(self, os_utils):
        return RelayPool(
            role="target",
            job_id="job-1",
            az="az1",
            size=1,
            image_id="img-1",
            flavor_id="flv-1",
            port_ids=["port-1"],
            admin_password="pw",
            platform_url="https://platform.example.com",
            token_factory=lambda job_id, role: f"{job_id}-token",
            os_utils=os_utils,
            state=RelayState(secret=b"secret"),
            system_volume_type="vt-1",
        )

    def test_provision_injects_floating_ip_as_data_address(self):
        os_utils = mock.MagicMock()
        os_utils.create_relay_server.return_value = mock.Mock(
            id="srv-1", root_volume_id="vol-1"
        )
        pool = self._pool(os_utils)
        pool.data_addresses = ["203.0.113.9"]

        pool.provision_one(0)

        user_data = os_utils.create_relay_server.call_args.kwargs["user_data"]
        self.assertIn("RELAY_DATA_ADDR=203.0.113.9", user_data)

    def test_destroy_releases_floating_ips(self):
        os_utils = mock.MagicMock()
        os_utils.create_relay_server.return_value = mock.Mock(
            id="srv-1", root_volume_id="vol-1"
        )
        pool = self._pool(os_utils)
        pool.data_fips = [{"id": "fip-1", "address": "203.0.113.9"}]
        pool.provision_one(0)

        pool.destroy()

        os_utils.delete_relay_floating_ip.assert_called_once_with("fip-1")


class BindPoolFloatingIpsTest(unittest.TestCase):
    def test_binds_one_floating_ip_per_port(self):
        os_utils = mock.MagicMock()
        os_utils.create_relay_floating_ip.side_effect = [
            FakeFloatingIP("f1", "203.0.113.1"),
            FakeFloatingIP("f2", "203.0.113.2"),
        ]
        pool = mock.Mock(port_ids=["p1", "p2"], data_fips=[])
        config = mock.Mock(data_floating_network="ext-net-1")

        RelayRuntime._bind_data_floating_ips(pool, config, os_utils)

        self.assertEqual(os_utils.create_relay_floating_ip.call_count, 2)
        self.assertEqual(pool.data_addresses, ["203.0.113.1", "203.0.113.2"])

    def test_releases_bound_ips_when_binding_fails(self):
        os_utils = mock.MagicMock()
        os_utils.create_relay_floating_ip.side_effect = [
            FakeFloatingIP("f1", "203.0.113.1"),
            RuntimeError("no more floating ips"),
        ]
        pool = mock.Mock(port_ids=["p1", "p2"], data_fips=[])
        config = mock.Mock(data_floating_network="ext-net-1")

        with self.assertRaises(RuntimeError):
            RelayRuntime._bind_data_floating_ips(pool, config, os_utils)

        os_utils.delete_relay_floating_ip.assert_called_once_with("f1")

    def test_skips_when_no_floating_network_configured(self):
        os_utils = mock.MagicMock()
        pool = mock.Mock(port_ids=["p1"], data_fips=[])
        config = mock.Mock(data_floating_network="")

        RelayRuntime._bind_data_floating_ips(pool, config, os_utils)

        os_utils.create_relay_floating_ip.assert_not_called()


class DataPathFailureTest(unittest.TestCase):
    """数据面不通属于环境问题，重试只会白等，必须立刻失败并给出可操作提示。"""

    def test_detects_no_route_to_host(self):
        self.assertTrue(
            RelayVolumeMover.is_data_path_error(
                "块拷贝任务 abc-s 失败: failed（OSError: [Errno 113] No route to host）"
            )
        )

    def test_detects_connection_refused(self):
        self.assertTrue(
            RelayVolumeMover.is_data_path_error(
                "块拷贝任务 abc-s 失败: failed（ConnectionRefusedError: [Errno 111] Connection refused）"
            )
        )

    def test_ignores_unrelated_errors(self):
        self.assertFalse(
            RelayVolumeMover.is_data_path_error(
                "块拷贝任务 abc-s 失败: failed（ValueError: magic mismatch）"
            )
        )

    def test_hint_mentions_floating_ip(self):
        hint = RelayVolumeMover.data_path_hint("192.0.2.10", 9200)

        self.assertIn("192.0.2.10:9200", hint)
        self.assertIn("浮动 IP", hint)

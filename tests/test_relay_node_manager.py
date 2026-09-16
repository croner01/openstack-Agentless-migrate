import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay_credentials import CredentialStore, Sealer
from relay_inventory import NodeInventory
from relay_node_manager import NodeManagerError, RelayNodeManager
from relay_pool_profile import PoolProfile, PoolProfileStore

KEY = b"k" * 32


class FakePort:
    def __init__(self, port_id):
        self.id = port_id


class FakeServer:
    def __init__(self, server_id):
        self.id = server_id


class NodeManagerTest(unittest.TestCase):
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
                role="source",
                az="nova-1",
                image="img-1",
                flavor="flv-1",
                network="net-1",
                subnet="sub-1",
                system_volume_type="vt-1",
            )
        )
        self.os_utils = mock.MagicMock()
        self.os_utils.create_relay_port.return_value = FakePort("port-1")
        self.os_utils.create_relay_server.return_value = FakeServer("server-1")
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

    def _create(self, **overrides):
        kwargs = {
            "tenant_key": "t1",
            "role": "source",
            "az": "nova-1",
            "slots_total": 5,
        }
        kwargs.update(overrides)
        return self.manager.create_node(**kwargs)

    def test_create_node_persists_record_and_secrets(self):
        record = self._create()

        self.assertEqual(record.server_id, "server-1")
        self.assertEqual(record.state, "provisioning")
        self.assertTrue(record.token_enc)
        self.assertTrue(record.ssh_password_enc)
        self.assertEqual(self.inventory.get(record.node_id).server_id, "server-1")

        token = self.credentials.unseal_text(record.token_enc, aad=record.node_id)
        password = self.credentials.unseal_text(
            record.ssh_password_enc, aad=record.node_id
        )
        self.assertTrue(token)
        self.assertTrue(password)
        self.assertNotEqual(record.ssh_password_enc, password)

    def test_create_node_generates_password_when_pool_has_none(self):
        record = self._create()

        password = self.credentials.unseal_text(
            record.ssh_password_enc, aad=record.node_id
        )

        self.assertGreaterEqual(len(password), 12)

    def test_create_node_reuses_password_from_existing_pool_node(self):
        first = self._create()
        expected = self.credentials.unseal_text(
            first.ssh_password_enc, aad=first.node_id
        )
        self.os_utils.create_relay_server.return_value = FakeServer("server-2")

        second = self._create()

        actual = self.credentials.unseal_text(
            second.ssh_password_enc, aad=second.node_id
        )
        self.assertEqual(actual, expected)

    def test_create_node_passes_availability_zone_and_port(self):
        self._create()

        kwargs = self.os_utils.create_relay_server.call_args.kwargs
        self.assertEqual(kwargs["availability_zone"], "nova-1")
        self.assertEqual(kwargs["port_ids"], ["port-1"])
        self.assertEqual(kwargs["image_id"], "img-1")
        self.assertEqual(kwargs["flavor_id"], "flv-1")

    def test_create_node_requires_profile(self):
        with self.assertRaises(NodeManagerError):
            self._create(az="nova-9")

    def test_create_node_requires_credentials(self):
        with self.assertRaises(NodeManagerError):
            self._create(tenant_key="t9")

    def test_delete_node_removes_server_port_and_record(self):
        record = self._create()

        self.assertTrue(self.manager.delete_node(record.node_id))

        self.assertIsNone(self.inventory.get(record.node_id))
        # 这套环境 delete 是软删除（reclaim 24h），中转机必须硬删除，
        # 否则启动卷会一直挂着占配额。
        self.os_utils.delete_server.assert_called_once_with("server-1", force=True)
        self.os_utils.delete_port.assert_called_once_with("port-1")

    def test_delete_node_removes_boot_volume(self):
        self.os_utils.create_relay_server.return_value = FakeServer("server-1")
        self.os_utils.create_relay_server.return_value.root_volume_id = "vol-root-1"
        record = self._create()

        self.manager.delete_node(record.node_id)

        # 实例删完 attachment 还没释放，删除走带重试的接口。
        self.os_utils.delete_volume_wait.assert_called_once_with("vol-root-1")

    def test_delete_node_survives_cloud_errors(self):
        record = self._create()
        self.os_utils.delete_server.side_effect = RuntimeError("gone")

        self.assertTrue(self.manager.delete_node(record.node_id))

        self.assertIsNone(self.inventory.get(record.node_id))

    def test_rebuild_reuses_name_but_issues_new_server(self):
        record = self._create()
        self.os_utils.create_relay_server.return_value = FakeServer("server-2")

        rebuilt = self.manager.rebuild(record.node_id)

        self.assertEqual(rebuilt.name, record.name)
        self.assertEqual(rebuilt.server_id, "server-2")
        self.assertNotEqual(rebuilt.token_enc, record.token_enc)

    def test_drain_and_resume(self):
        record = self._create()

        self.assertTrue(self.manager.drain(record.node_id))
        self.assertEqual(self.inventory.get(record.node_id).state, "draining")
        self.assertTrue(self.manager.resume(record.node_id))
        self.assertEqual(self.inventory.get(record.node_id).state, "ready")

    def test_wait_ready_marks_node_ready_when_agent_registers(self):
        record = self._create()
        agent = mock.MagicMock()
        agent.version = "1.0.0"
        agent.data_address = "10.0.0.7"
        self.manager.registry.find_by_name.return_value = agent

        self.assertTrue(self.manager.wait_ready(record, timeout=1))

        self.assertEqual(self.inventory.get(record.node_id).state, "ready")
        self.assertEqual(self.inventory.get(record.node_id).agent_version, "1.0.0")

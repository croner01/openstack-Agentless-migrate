import tempfile
import unittest
from pathlib import Path
from unittest import mock

from flask import Flask

from relay_admin_api import create_admin_blueprint
from relay_credentials import CredentialStore, Sealer
from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_pool_profile import PoolProfileStore
from relay_resources import RelayResourceLayer
from relay_registry import RelayState

KEY = b"k" * 32


class AdminApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.leases = LeaseStore.load(base / "leases.json")
        self.layer = RelayResourceLayer(
            inventory=self.inventory,
            leases=self.leases,
            state=RelayState(secret=b"s" * 32),
            secret=b"s" * 32,
            platform_url="https://migrate.example.com",
            profiles_path=base / "pools.json",
            credentials_path=base / "credentials.json",
            os_utils_factory=lambda auth: mock.MagicMock(),
            credentials_factory=lambda: CredentialStore.load(
                base / "credentials.json", Sealer(KEY)
            ),
        )
        self.assertTrue(self.layer.ensure())
        self.manager = mock.MagicMock()
        self.layer.node_manager = self.manager

        self.app = Flask(__name__)
        self.app.register_blueprint(create_admin_blueprint(layer=self.layer))
        self.client = self.app.test_client()
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n1",
                name="relay-source-t1-nova-1-1",
                role="source",
                tenant_key="t1",
                az="nova-1",
                server_id="server-1",
                slots_total=5,
                slots_used=0,
                state="ready",
            )
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_list_nodes_filters_by_tenant(self):
        response = self.client.get("/api/relay/nodes?tenant_key=t1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.get_json()["nodes"]), 1)

    def test_list_nodes_filters_out_other_tenant(self):
        response = self.client.get("/api/relay/nodes?tenant_key=t2")

        self.assertEqual(response.get_json()["nodes"], [])

    def test_delete_node_rejected_when_busy(self):
        record = self.inventory.get("n1")
        record.slots_used = 2
        record.state = "busy"
        self.inventory.upsert(record)

        response = self.client.delete("/api/relay/nodes/n1")

        self.assertEqual(response.status_code, 409)
        self.manager.delete_node.assert_not_called()

    def test_delete_node_calls_manager(self):
        response = self.client.delete("/api/relay/nodes/n1")

        self.assertEqual(response.status_code, 200)
        self.manager.delete_node.assert_called_once_with("n1")

    def test_password_endpoint_decrypts(self):
        record = self.inventory.get("n1")
        record.ssh_password_enc = self.layer.credentials.seal_text(
            "r00t-pass", aad="n1"
        )
        self.inventory.upsert(record)

        response = self.client.get("/api/relay/nodes/n1/password")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["password"], "r00t-pass")

    def test_password_endpoint_404_without_password(self):
        response = self.client.get("/api/relay/nodes/n1/password")

        self.assertEqual(response.status_code, 404)

    def test_save_and_list_tenant_credentials(self):
        response = self.client.put(
            "/api/relay/tenants",
            json={
                "tenant_key": "t1",
                "auth": {"auth_url": "http://k", "password": "p"},
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.layer.credentials.get("t1")["auth_url"], "http://k")
        listed = self.client.get("/api/relay/tenants").get_json()
        self.assertEqual(listed["tenants"], ["t1"])

    def test_delete_tenant_rejected_when_nodes_exist(self):
        response = self.client.delete("/api/relay/tenants?tenant_key=t1")

        self.assertEqual(response.status_code, 409)

    def test_scale_endpoint_creates_nodes_up_to_target(self):
        self.layer.profiles.upsert_profile(
            {
                "tenant_key": "t1",
                "role": "source",
                "az": "nova-1",
                "image": "img",
                "flavor": "flv",
                "network": "net",
                "system_volume_type": "vt-1",
            }
        )
        self.manager.create_node.return_value = self.inventory.get("n1")

        response = self.client.post(
            "/api/relay/pools/scale",
            json={
                "tenant_key": "t1",
                "role": "source",
                "az": "nova-1",
                "target_nodes": 2,
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.manager.create_node.call_count, 1)

    def test_upsert_pool_profile(self):
        response = self.client.put(
            "/api/relay/pools/profile",
            json={
                "tenant_key": "t1",
                "role": "source",
                "az": "nova-1",
                "image": "img",
                "flavor": "flv",
                "network": "net",
                "system_volume_type": "vt-1",
                "slots_per_node": 5,
                "max_nodes": 6,
                "min_nodes": 1,
            },
        )

        self.assertEqual(response.status_code, 200)
        profile = self.layer.profiles.get("t1", "source", "nova-1")
        self.assertEqual(profile.image, "img")
        self.assertEqual(profile.slots_per_node, 5)

    def test_delete_pool_profile_requires_identity(self):
        response = self.client.delete("/api/relay/pools/profile?tenant_key=t1")

        self.assertEqual(response.status_code, 400)

    def test_delete_pool_profile_rejected_while_nodes_exist(self):
        self.client.put(
            "/api/relay/pools/profile",
            json={
                "tenant_key": "t1",
                "role": "source",
                "az": "nova-1",
                "image": "img",
                "flavor": "flv",
                "network": "net",
                "system_volume_type": "vt-1",
            },
        )

        response = self.client.delete(
            "/api/relay/pools/profile?tenant_key=t1&role=source&az=nova-1"
        )

        self.assertEqual(response.status_code, 409)
        self.assertIsNotNone(self.layer.profiles.get("t1", "source", "nova-1"))

    def test_delete_pool_profile_removes_record(self):
        self.client.put(
            "/api/relay/pools/profile",
            json={
                "tenant_key": "t2",
                "role": "target",
                "az": "nova-2",
                "image": "img",
                "flavor": "flv",
                "network": "net",
                "system_volume_type": "vt-1",
            },
        )

        response = self.client.delete(
            "/api/relay/pools/profile?tenant_key=t2&role=target&az=nova-2"
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(self.layer.profiles.get("t2", "target", "nova-2"))

    def test_delete_pool_profile_404_when_missing(self):
        response = self.client.delete(
            "/api/relay/pools/profile?tenant_key=t9&role=source&az=nova-9"
        )

        self.assertEqual(response.status_code, 404)

    def test_admin_requires_master_key(self):
        base = Path(self.tmp.name) / "no-key"
        layer = RelayResourceLayer(
            inventory=self.inventory,
            leases=self.leases,
            state=RelayState(secret=b"s" * 32),
            secret=b"s" * 32,
            platform_url="https://migrate.example.com",
            profiles_path=base / "pools.json",
            credentials_path=base / "credentials.json",
            os_utils_factory=lambda auth: mock.MagicMock(),
            credentials_factory=lambda: CredentialStore.from_env(
                base / "credentials.json", env={}
            ),
        )
        app = Flask(__name__)
        app.register_blueprint(create_admin_blueprint(layer=layer))
        client = app.test_client()

        response = client.delete("/api/relay/nodes/n1")

        self.assertEqual(response.status_code, 503)


class FakeNodeManager:
    def __init__(self, inventory):
        self.inventory = inventory
        self.created = []

    def create_node(self, *, tenant_key, role, az, slots_total=None):
        import uuid

        record = RelayNodeRecord(
            node_id=uuid.uuid4().hex,
            name=f"relay-{role}-{az}-{len(self.created) + 1}",
            role=role,
            tenant_key=tenant_key,
            az=az,
            server_id=f"server-{len(self.created) + 1}",
            state="provisioning",
            created_at=1.0,
        )
        self.inventory.upsert(record)
        self.created.append(record)
        return record

    def wait_ready(self, record, *, timeout):
        record.state = "ready"
        self.inventory.upsert(record)
        return True


class EnsurePoolTest(unittest.TestCase):
    """作业提交时按表单自动建池：存凭据、首次写池参数、预热 min_nodes。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.leases = LeaseStore.load(base / "leases.json")
        self.layer = RelayResourceLayer(
            inventory=self.inventory,
            leases=self.leases,
            state=RelayState(secret=b"s" * 32),
            secret=b"s" * 32,
            platform_url="https://migrate.example.com",
            profiles_path=base / "pools.json",
            credentials_path=base / "credentials.json",
            os_utils_factory=lambda auth: mock.MagicMock(),
            credentials_factory=lambda: CredentialStore.load(
                base / "credentials.json", Sealer(KEY)
            ),
        )
        self.assertTrue(self.layer.ensure())
        self.manager = FakeNodeManager(self.inventory)
        self.layer.node_manager = self.manager

    def tearDown(self):
        self.tmp.cleanup()

    def _ensure(self, **overrides):
        kwargs = {
            "tenant_key": "t1",
            "role": "source",
            "az": "nova-1",
            "auth": {"auth_url": "http://k", "password": "p"},
            "profile_defaults": {
                "image": "img",
                "flavor": "flv",
                "network": "net",
                "system_volume_type": "vt-1",
                "slots_per_node": 5,
                "max_nodes": 6,
                "min_nodes": 1,
            },
            "min_nodes": 1,
        }
        kwargs.update(overrides)
        return self.layer.ensure_pool(**kwargs)

    def test_creates_credentials_profile_and_warmup_node(self):
        result = self._ensure()

        self.assertEqual(self.layer.credentials.get("t1")["auth_url"], "http://k")
        profile = self.layer.profiles.get("t1", "source", "nova-1")
        self.assertEqual(profile.image, "img")
        self.assertTrue(result["created_profile"])
        self.assertEqual(len(result["created_nodes"]), 1)
        self.assertEqual(len(self.inventory.nodes_in_pool("t1", "source", "nova-1")), 1)

    def test_existing_pool_keeps_original_build_parameters(self):
        self._ensure()
        self.manager.created.clear()

        result = self._ensure(
            profile_defaults={
                "image": "img-OTHER",
                "flavor": "flv",
                "network": "net",
                "system_volume_type": "vt-1",
            }
        )

        self.assertFalse(result["created_profile"])
        self.assertEqual(
            self.layer.profiles.get("t1", "source", "nova-1").image, "img"
        )
        # 已有 1 台 ready，min_nodes=1 时不再建机
        self.assertEqual(result["created_nodes"], [])

    def test_scales_to_min_nodes_when_pool_is_empty(self):
        result = self._ensure(min_nodes=2)

        self.assertEqual(len(result["created_nodes"]), 2)

    def test_pool_scheduler_uses_profile_capacity(self):
        self.layer.profiles.upsert_profile(
            {
                "tenant_key": "t1",
                "role": "source",
                "az": "nova-1",
                "image": "img",
                "flavor": "flv",
                "network": "net",
                "system_volume_type": "vt-1",
                "slots_per_node": 3,
                "max_nodes": 4,
                "min_nodes": 1,
            }
        )

        scheduler = self.layer.scheduler_for_pool("t1", "source", "nova-1")

        self.assertEqual(scheduler.config.slots_per_node, 3)
        self.assertEqual(scheduler.config.max_nodes, 4)
        # 共享的默认调度器配置不受影响
        self.assertEqual(self.layer.scheduler.config.slots_per_node, 5)


class MasterKeyTest(unittest.TestCase):
    """未注入 MIGRATION_SECRET_KEY 时自动生成主密钥，凭据仍可跨重启解密。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_master_key_is_created_once_and_reused(self):
        from relay_resources import load_or_create_credentials

        credentials_path = self.base / "relay-credentials.json"
        key_path = self.base / "relay-master-key"

        store = load_or_create_credentials(credentials_path, key_path, env={})
        store.save("t1", {"password": "p@ss"})
        store.flush()
        first_key = key_path.read_bytes()

        # 模拟平台重启：重新加载必须能解开旧密文
        reloaded = load_or_create_credentials(credentials_path, key_path, env={})

        self.assertEqual(reloaded.get("t1")["password"], "p@ss")
        self.assertEqual(key_path.read_bytes(), first_key)

    def test_layer_becomes_ready_without_env_key(self):
        layer = RelayResourceLayer(
            inventory=NodeInventory.load(self.base / "nodes.json"),
            leases=LeaseStore.load(self.base / "leases.json"),
            state=RelayState(secret=b"s" * 32),
            secret=b"s" * 32,
            platform_url="https://migrate.example.com",
            profiles_path=self.base / "pools.json",
            credentials_path=self.base / "credentials.json",
            master_key_path=self.base / "relay-master-key",
            os_utils_factory=lambda auth: mock.MagicMock(),
            credentials_factory=None,
        )

        self.assertTrue(layer.ensure())
        self.assertTrue(layer.ready)
        self.assertTrue((self.base / "relay-master-key").exists())

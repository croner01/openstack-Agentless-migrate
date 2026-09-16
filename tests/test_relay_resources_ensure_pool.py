import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay_credentials import CredentialStore, Sealer
from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_resources import RelayResourceLayer
from relay_registry import RelayState
from relay_scheduler import NodeNotReadyError

KEY = b"k" * 32


class FlakyNodeManager:
    """按脚本返回 wait_ready 结果，用来复现"建机成功但 agent 注册不上"。"""

    def __init__(self, inventory, ready_results):
        self.inventory = inventory
        self.ready_results = list(ready_results)
        self.created: list[str] = []
        self.deleted: list[str] = []
        self._seq = 0

    def create_node(self, *, tenant_key, role, az, slots_total=None):
        self._seq += 1
        record = RelayNodeRecord(
            node_id=f"n{self._seq}",
            name=f"relay-{role}-{self._seq}",
            role=role,
            tenant_key=tenant_key,
            az=az,
            server_id=f"server-{self._seq}",
            slots_total=int(slots_total or 5),
            state="provisioning",
            created_at=float(self._seq),
        )
        self.inventory.upsert(record)
        self.created.append(record.node_id)
        return record

    def wait_ready(self, record, *, timeout=None):
        return self.ready_results.pop(0) if self.ready_results else False

    def delete_node(self, node_id):
        record = self.inventory.remove(node_id)
        if record is not None:
            self.deleted.append(node_id)
        return record is not None


class EnsurePoolWarmupTest(unittest.TestCase):
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

    def tearDown(self):
        self.tmp.cleanup()

    def _install(self, ready_results):
        manager = FlakyNodeManager(self.inventory, ready_results)
        self.layer.node_manager = manager
        return manager

    def _ensure(self, **overrides):
        kwargs = {
            "tenant_key": "t1",
            "role": "target",
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
            "ready_timeout": 1.0,
        }
        kwargs.update(overrides)
        return self.layer.ensure_pool(**kwargs)

    def test_registration_failure_is_retried_then_succeeds(self):
        manager = self._install([False, True])

        result = self._ensure(attempts=2)

        self.assertEqual(manager.created, ["n1", "n2"])
        self.assertEqual(manager.deleted, ["n1"])
        self.assertEqual(result["created_nodes"], ["n2"])
        self.assertEqual(len(self.inventory.nodes_in_pool("t1", "target", "nova-1")), 1)

    def test_registration_failure_after_retries_raises_and_cleans_up(self):
        manager = self._install([False, False])

        with self.assertRaises(NodeNotReadyError) as ctx:
            self._ensure(attempts=2)

        message = str(ctx.exception)
        self.assertIn("target", message)
        self.assertIn("nova-1", message)
        self.assertIn("server-2", message)
        # 失败的中转机必须删掉，不能留在清单里白占建机额度。
        self.assertEqual(manager.created, ["n1", "n2"])
        self.assertEqual(manager.deleted, ["n1", "n2"])
        self.assertEqual(self.inventory.nodes_in_pool("t1", "target", "nova-1"), [])

    def test_ready_node_is_not_recreated(self):
        manager = self._install([True])

        result = self._ensure(attempts=2)

        self.assertEqual(manager.created, ["n1"])
        self.assertEqual(manager.deleted, [])
        self.assertEqual(result["created_nodes"], ["n1"])

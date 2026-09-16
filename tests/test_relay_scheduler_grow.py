import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_scheduler import (
    NodeNotReadyError,
    QueueTimeoutError,
    RelayScheduler,
    SchedulerConfig,
)


class FakeAgent:
    def __init__(self, name):
        self.name = name
        self.session_id = f"session-{name}"
        self.data_address = "10.0.0.9"
        self.data_port = 9200
        self.ssh_public_key = "ssh-ed25519 AAAA"


class FakeRegistry:
    def __init__(self):
        self._agents = {}

    def find_by_name(self, name):
        return self._agents.get(name)

    def add(self, name):
        self._agents[name] = FakeAgent(name)

    def drop(self, name):
        self._agents.pop(name, None)


class FakeNodeManager:
    """用内存清单模拟建机/删机，不访问 OpenStack。"""

    def __init__(self, inventory, registry):
        self.inventory = inventory
        self.registry = registry
        self.created = []
        self.deleted = []
        self._seq = 100

    def create_node(self, *, tenant_key, role, az, slots_total):
        self._seq += 1
        record = RelayNodeRecord(
            node_id=f"new-{self._seq}",
            name=f"relay-{role}-{tenant_key}-{az}-{self._seq}",
            role=role,
            tenant_key=tenant_key,
            az=az,
            server_id=f"server-{self._seq}",
            slots_total=slots_total,
            state="provisioning",
            created_at=float(self._seq),
        )
        self.inventory.upsert(record)
        self.created.append(record.node_id)
        return record

    def wait_ready(self, record, *, timeout):
        record.state = "ready"
        self.inventory.upsert(record)
        self.registry.add(record.name)
        return True

    def delete_node(self, node_id):
        record = self.inventory.remove(node_id)
        if record is not None:
            self.registry.drop(record.name)
            self.deleted.append(node_id)
        return record is not None


class RelaySchedulerGrowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.leases = LeaseStore.load(Path(self.tmp.name) / "leases.json")
        self.registry = FakeRegistry()
        self.manager = FakeNodeManager(self.inventory, self.registry)
        self.now = 1000.0
        self.slept = []

        def clock():
            return self.now

        def sleeper(seconds):
            self.slept.append(seconds)
            self.now += seconds

        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=self.registry,
            config=SchedulerConfig(
                slots_per_node=5,
                max_nodes=6,
                min_nodes=1,
                scale_up_wait_seconds=30.0,
                queue_timeout_seconds=1800.0,
            ),
            node_manager=self.manager,
            clock=clock,
            sleeper=sleeper,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _acquire(self, az="nova-1"):
        return self.scheduler.acquire(
            "task-1", job_id="job-1", role="source", tenant_key="t1", az=az
        )

    def test_grows_one_node_when_pool_is_empty(self):
        view = self._acquire()

        self.assertEqual(len(self.manager.created), 1)
        self.assertEqual(self.slept, [30.0])
        self.assertEqual(self.inventory.count_for_role("t1", "source"), 1)
        self.assertEqual(view.data_address, "10.0.0.9")

    def test_does_not_grow_beyond_max_nodes(self):
        for index in range(6):
            record = self.manager.create_node(
                tenant_key="t1", role="source", az="nova-1", slots_total=1
            )
            self.manager.wait_ready(record, timeout=1)
            record.state = "busy"
            record.slots_used = 1
            self.inventory.upsert(record)

        with self.assertRaises(QueueTimeoutError):
            self._acquire()

        self.assertEqual(self.inventory.count_for_role("t1", "source"), 6)

    def test_rebalance_deletes_idle_node_in_other_az(self):
        for index in range(6):
            record = self.manager.create_node(
                tenant_key="t1", role="source", az="nova-2", slots_total=1
            )
            self.manager.wait_ready(record, timeout=1)
            if index > 0:
                record.state = "busy"
                record.slots_used = 1
                self.inventory.upsert(record)

        view = self._acquire(az="nova-1")

        self.assertEqual(len(self.manager.deleted), 1)
        self.assertEqual(self.inventory.get(view.node_id).az, "nova-1")

    def test_rebalance_keeps_last_node_in_each_az(self):
        record = self.manager.create_node(
            tenant_key="t1", role="source", az="nova-2", slots_total=1
        )
        self.manager.wait_ready(record, timeout=1)
        for index in range(5):
            busy = self.manager.create_node(
                tenant_key="t1", role="source", az="nova-3", slots_total=1
            )
            self.manager.wait_ready(busy, timeout=1)
            busy.state = "busy"
            busy.slots_used = 1
            self.inventory.upsert(busy)

        with self.assertRaises(QueueTimeoutError):
            self._acquire(az="nova-1")

        self.assertEqual(self.manager.deleted, [])

    def test_node_that_never_registers_is_retried_then_cleaned_up(self):
        """agent 装不上：重试到上限后报错，且失败节点必须删掉。"""
        self.manager.wait_ready = lambda record, *, timeout: False

        with self.assertRaises(NodeNotReadyError) as ctx:
            self._acquire()

        message = str(ctx.exception)
        self.assertIn("未注册成功", message)
        self.assertIn("server-", message)
        # 只按尝试上限重试，没有把 6 台额度全浪费掉；失败节点全部清理。
        self.assertEqual(len(self.manager.created), 2)
        self.assertEqual(len(self.manager.deleted), 2)
        self.assertEqual(self.inventory.count_for_role("t1", "source"), 0)

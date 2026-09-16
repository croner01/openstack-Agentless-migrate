import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_scheduler import NoCapacityError, RelayScheduler, SchedulerConfig


class FakeAgent:
    def __init__(self, name, *, address="10.0.0.5", port=9200, version="1.0.0"):
        self.name = name
        self.session_id = f"session-{name}"
        self.data_address = address
        self.data_port = port
        self.ssh_public_key = "ssh-ed25519 AAAA"
        self.version = version


class FakeRegistry:
    def __init__(self, names=()):
        self._agents = {name: FakeAgent(name) for name in names}

    def find_by_name(self, name):
        return self._agents.get(name)

    def drop(self, name):
        self._agents.pop(name, None)


def _node(node_id, *, name=None, slots_total=5, state="ready", az="nova-1", role="source"):
    return RelayNodeRecord(
        node_id=node_id,
        name=name or f"relay-{role}-{node_id}",
        role=role,
        tenant_key="t1",
        az=az,
        server_id=f"server-{node_id}",
        slots_total=slots_total,
        state=state,
        created_at=float(node_id),
    )


class RelaySchedulerSlotTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.leases = LeaseStore.load(Path(self.tmp.name) / "leases.json")
        self.inventory.upsert(_node("1"))
        self.registry = FakeRegistry(["relay-source-1"])
        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=self.registry,
            config=SchedulerConfig(slots_per_node=2),
            clock=lambda: 100.0,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _acquire(self, task_id="task-1", az="nova-1", role="source"):
        return self.scheduler.acquire(
            task_id,
            job_id="job-1",
            role=role,
            tenant_key="t1",
            az=az,
            vm_id="vm-1",
        )

    def test_acquire_returns_node_view_with_agent_address(self):
        view = self._acquire()

        self.assertEqual(view.node_id, "1")
        self.assertEqual(view.data_address, "10.0.0.5")
        self.assertEqual(view.data_port, 9200)
        self.assertEqual(view.state, "busy")

    def test_acquire_view_carries_agent_version(self):
        self.registry._agents["relay-source-1"].version = "1.1.0"

        view = self._acquire()

        self.assertEqual(view.agent_version, "1.1.0")

    def test_acquire_marks_slot_used_and_persists_lease(self):
        self._acquire()

        record = self.inventory.get("1")
        self.assertEqual(record.slots_used, 1)
        self.assertEqual(record.state, "busy")
        self.assertEqual(len(self.leases.active_for_node("1")), 1)

    def test_acquire_skips_full_node_and_uses_next_pool_node(self):
        self.inventory.upsert(_node("2"))
        self.registry._agents["relay-source-2"] = FakeAgent("relay-source-2")

        first = self._acquire("task-1")
        self.inventory.get(first.node_id).slots_total = 1
        second = self._acquire("task-2")

        self.assertNotEqual(first.node_id, second.node_id)

    def test_acquire_skips_unhealthy_node(self):
        self.inventory.get("1").state = "unhealthy"
        self.registry.find_by_name("relay-source-1").state = "unhealthy"
        with self.assertRaises(NoCapacityError):
            self._acquire()

    def test_acquire_reuses_unhealthy_node_once_agent_is_back(self):
        """一次心跳抖动不该把节点在额度里永久拉黑。"""
        self.inventory.get("1").state = "unhealthy"

        view = self._acquire()

        self.assertEqual(view.node_id, "1")
        self.assertEqual(self.inventory.get("1").slots_used, 1)

    def test_acquire_skips_node_without_agent(self):
        self.registry.drop("relay-source-1")
        with self.assertRaises(NoCapacityError):
            self._acquire()

    def test_acquire_skips_other_role_az_and_tenant(self):
        with self.assertRaises(NoCapacityError):
            self._acquire(az="nova-2")
        with self.assertRaises(NoCapacityError):
            self._acquire(role="target")

    def test_release_frees_slot_and_records_idle_since(self):
        self._acquire()

        self.scheduler.release("1")

        record = self.inventory.get("1")
        self.assertEqual(record.slots_used, 0)
        self.assertEqual(record.state, "ready")
        self.assertEqual(record.idle_since, 100.0)
        self.assertEqual(self.leases.active_for_node("1"), [])

    def test_release_twice_does_not_go_negative(self):
        self._acquire()
        self.scheduler.release("1")
        self.scheduler.release("1")

        self.assertEqual(self.inventory.get("1").slots_used, 0)

    def test_release_job_releases_all_slots(self):
        self._acquire("task-1")
        self._acquire("task-2")

        released = self.scheduler.release_job("job-1")

        self.assertEqual(released, 2)
        self.assertEqual(self.inventory.get("1").slots_used, 0)

    def test_concurrent_slots_get_distinct_ports(self):
        first = self._acquire("task-1")
        second = self._acquire("task-2")

        self.assertEqual(first.node_id, second.node_id)
        self.assertNotEqual(first.data_port, second.data_port)

import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_scheduler import RelayScheduler, SchedulerConfig

DAY = 86400.0


class FakeNodeManager:
    def __init__(self, inventory):
        self.inventory = inventory
        self.deleted = []

    def delete_node(self, node_id):
        self.inventory.remove(node_id)
        self.deleted.append(node_id)
        return True


class ScaleDownTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.leases = LeaseStore.load(Path(self.tmp.name) / "leases.json")
        self.manager = FakeNodeManager(self.inventory)
        self.now = 10 * DAY
        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=None,
            config=SchedulerConfig(min_nodes=1, idle_scale_down_seconds=DAY),
            node_manager=self.manager,
            clock=lambda: self.now,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _add(
        self, node_id, *, created_at=0.0, idle_since=0.0, slots_used=0, az="nova-1"
    ):
        record = RelayNodeRecord(
            node_id=node_id,
            name=f"relay-source-{node_id}",
            role="source",
            tenant_key="t1",
            az=az,
            server_id=f"server-{node_id}",
            slots_total=5,
            slots_used=slots_used,
            state="busy" if slots_used else "ready",
            created_at=created_at,
            idle_since=idle_since,
        )
        self.inventory.upsert(record)
        return record

    def test_removes_idle_nodes_above_min_and_keeps_oldest(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("mid", created_at=2.0, idle_since=1.0)
        self._add("new", created_at=3.0, idle_since=1.0)

        removed = self.scheduler.scale_down()

        self.assertEqual(sorted(removed), ["mid", "new"])
        self.assertIsNotNone(self.inventory.get("old"))

    def test_busy_node_is_never_removed(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("busy", created_at=2.0, slots_used=1)

        removed = self.scheduler.scale_down()

        self.assertNotIn("busy", removed)
        self.assertIsNotNone(self.inventory.get("busy"))

    def test_node_with_active_lease_is_never_removed(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("leased", created_at=2.0)
        self.leases.acquire(
            job_id="job-1", node_id="leased", role="source", tenant_key="t1", now=1.0
        )

        removed = self.scheduler.scale_down()

        self.assertNotIn("leased", removed)
        self.assertIsNotNone(self.inventory.get("leased"))

    def test_node_idle_less_than_threshold_is_kept(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("fresh", created_at=2.0, idle_since=self.now - 3600.0)

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, ["old"])
        self.assertIsNotNone(self.inventory.get("fresh"))

    def test_min_nodes_one_per_pool(self):
        self._add("only", created_at=1.0, idle_since=1.0)

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, [])
        self.assertIsNotNone(self.inventory.get("only"))

    def test_pools_are_evaluated_independently(self):
        self._add("a1", created_at=1.0, idle_since=1.0, az="nova-1")
        self._add("a2", created_at=2.0, idle_since=1.0, az="nova-1")
        self._add("b1", created_at=1.0, idle_since=1.0, az="nova-2")

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, ["a2"])
        self.assertIsNotNone(self.inventory.get("b1"))

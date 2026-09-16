"""常驻中转机共享场景：释放不能串作业，端口不能复用正在监听的槽位。"""
import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_scheduler import RelayScheduler, SchedulerConfig


class FakeAgent:
    def __init__(self, name):
        self.name = name
        self.session_id = f"session-{name}"
        self.data_address = "10.0.0.5"
        self.data_port = 9200
        self.ssh_public_key = "ssh-ed25519 AAAA"
        self.version = "1.2.0"
        self.state = "ready"


class FakeRegistry:
    def __init__(self, names=()):
        self._agents = {name: FakeAgent(name) for name in names}

    def find_by_name(self, name):
        return self._agents.get(name)


def _node(node_id="1", *, slots_total=5):
    return RelayNodeRecord(
        node_id=node_id,
        name=f"relay-source-{node_id}",
        role="source",
        tenant_key="t1",
        az="nova-1",
        server_id=f"server-{node_id}",
        slots_total=slots_total,
        state="ready",
        created_at=1.0,
    )


class SchedulerReleaseScopingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.leases = LeaseStore.load(base / "leases.json")
        self.inventory.upsert(_node("1", slots_total=5))
        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=FakeRegistry(["relay-source-1"]),
            config=SchedulerConfig(slots_per_node=5),
            clock=lambda: 100.0,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _acquire(self, job_id, task_id):
        return self.scheduler.acquire(
            task_id,
            job_id=job_id,
            role="source",
            tenant_key="t1",
            az="nova-1",
            vm_id=task_id,
        )

    def test_release_only_frees_the_calling_jobs_lease(self):
        """节点被两个作业共享时，A 收尾不能销掉 B 还在用的租约。"""
        first = self._acquire("job-a", "vm-a")
        self._acquire("job-b", "vm-b")
        self.assertEqual(len(self.leases.active_for_node("1")), 2)

        self.scheduler.release("1", job_id="job-a")

        remaining = self.leases.active_for_node("1")
        self.assertEqual([lease.job_id for lease in remaining], ["job-b"])
        self.assertEqual(self.inventory.get("1").slots_used, 1)
        self.assertEqual(self.inventory.get("1").state, "busy")
        # 释放的是 A 的槽位，B 的端口不能被回收。
        self.assertNotEqual(first.data_port, remaining[0].data_port)

    def test_port_is_not_reused_while_a_transfer_still_listens(self):
        """乱序释放后，新租约必须避开仍在监听的端口，否则目标端 bind 冲突。"""
        first = self._acquire("job-a", "vm-a")
        second = self._acquire("job-b", "vm-b")
        self.assertNotEqual(first.data_port, second.data_port)

        self.scheduler.release("1", job_id="job-a")
        third = self._acquire("job-c", "vm-c")

        live_ports = {
            lease.data_port for lease in self.leases.active_for_node("1")
        }
        self.assertIn(third.data_port, live_ports)
        # 关键断言：不能又发一次正被 job-b 监听的端口。
        self.assertNotEqual(third.data_port, second.data_port)

    def test_release_without_job_keeps_legacy_fifo_behaviour(self):
        self._acquire("job-a", "vm-a")
        self._acquire("job-b", "vm-b")

        self.scheduler.release("1")

        self.assertEqual(len(self.leases.active_for_node("1")), 1)
        self.assertEqual(self.inventory.get("1").slots_used, 1)


if __name__ == "__main__":
    unittest.main()

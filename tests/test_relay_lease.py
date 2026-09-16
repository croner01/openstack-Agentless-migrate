import tempfile
import unittest
from pathlib import Path

from relay_lease import LeaseStore


class LeaseStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-leases.json"

    def tearDown(self):
        self.tmp.cleanup()

    def _acquire(
        self, store, *, job_id="job-1", node_id="n1", role="source", tenant="t1", now=1.0
    ):
        return store.acquire(
            job_id=job_id,
            node_id=node_id,
            role=role,
            tenant_key=tenant,
            now=now,
        )

    def test_acquire_then_reload_roundtrips(self):
        store = LeaseStore.load(self.path)
        lease = self._acquire(store)
        store.save()

        reloaded = LeaseStore.load(self.path)

        self.assertEqual(reloaded.get(lease.lease_id).job_id, "job-1")
        self.assertEqual(len(reloaded.active_for_node("n1")), 1)

    def test_release_marks_inactive(self):
        store = LeaseStore.load(self.path)
        lease = self._acquire(store)

        released = store.release(lease.lease_id, now=2.0)

        self.assertEqual(released.released_at, 2.0)
        self.assertEqual(store.active_for_node("n1"), [])

    def test_release_job_releases_only_that_job(self):
        store = LeaseStore.load(self.path)
        self._acquire(store, job_id="job-1")
        self._acquire(store, job_id="job-2")

        released = store.release_job("job-1", now=3.0)

        self.assertEqual(len(released), 1)
        self.assertEqual(len(store.active_for_job("job-2")), 1)

    def test_active_count_for_role_sums_across_az(self):
        store = LeaseStore.load(self.path)
        self._acquire(store, node_id="n1")
        self._acquire(store, node_id="n2")
        self._acquire(store, node_id="n3", role="target")

        self.assertEqual(store.active_count_for_role("t1", "source"), 2)
        self.assertEqual(store.active_count_for_role("t1", "target"), 1)

    def test_idle_since_returns_last_release_when_no_active_lease(self):
        store = LeaseStore.load(self.path)
        lease = self._acquire(store)
        store.release(lease.lease_id, now=50.0)

        self.assertEqual(store.idle_since("n1"), 50.0)

    def test_idle_since_zero_when_still_active(self):
        store = LeaseStore.load(self.path)
        self._acquire(store)

        self.assertEqual(store.idle_since("n1"), 0.0)

    def test_lease_records_slot_port(self):
        store = LeaseStore.load(self.path)
        lease = store.acquire(
            job_id="job-1",
            node_id="n1",
            role="target",
            tenant_key="t1",
            now=1.0,
            data_port=9202,
        )

        self.assertEqual(lease.data_port, 9202)

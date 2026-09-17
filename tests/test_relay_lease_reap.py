import unittest
from unittest import mock

import app as app_module
from state_machine import JobStatus


class _Lease:
    def __init__(self, lease_id: str, job_id: str, acquired_at: float, released: bool = False):
        self.lease_id = lease_id
        self.job_id = job_id
        self.acquired_at = acquired_at
        self.released_at = 1.0 if released else 0.0

    @property
    def active(self) -> bool:
        return not self.released_at


class RelayLeaseReapTest(unittest.TestCase):
    """重启后残留的槽位租约必须被回收，否则常驻节点既调度不出去也删不掉。"""

    def _reap(self, leases, jobs, *, now=10_000.0):
        store = mock.MagicMock()
        store.all.return_value = leases
        store.release_job.side_effect = lambda job_id, now=None: [
            lease for lease in leases if lease.job_id == job_id and lease.active
        ]
        manager = mock.MagicMock()
        manager.get.side_effect = lambda job_id: jobs.get(job_id)
        with mock.patch.object(app_module, "RELAY_LEASES", store), mock.patch.object(
            app_module, "job_manager", manager
        ), mock.patch.object(app_module.RELAY_RESOURCES, "scheduler", None):
            return app_module.reap_relay_leases(now=now), store

    def test_releases_leases_of_jobs_that_are_no_longer_running(self):
        lease = _Lease("l1", "job-gone", acquired_at=1000.0)

        result, store = self._reap([lease], {})

        self.assertEqual(result, ["lease-reap:job-gone"])
        store.release_job.assert_called_once_with("job-gone", now=10_000.0)

    def test_keeps_leases_of_running_jobs(self):
        lease = _Lease("l1", "job-live", acquired_at=1000.0)
        job = mock.Mock(status=JobStatus.RUNNING)

        result, store = self._reap([lease], {"job-live": job})

        self.assertEqual(result, [])
        store.release_job.assert_not_called()

    def test_keeps_fresh_leases_inside_the_grace_window(self):
        """刚申请的租约留给作业自己收尾，避免和 RelayRuntime.finish() 抢。"""
        lease = _Lease("l1", "job-gone", acquired_at=9_900.0)

        result, store = self._reap([lease], {}, now=10_000.0)

        self.assertEqual(result, [])
        store.release_job.assert_not_called()

    def test_ignores_already_released_leases(self):
        lease = _Lease("l1", "job-gone", acquired_at=1000.0, released=True)

        result, store = self._reap([lease], {})

        self.assertEqual(result, [])
        store.release_job.assert_not_called()

    def test_grace_window_is_configurable(self):
        with mock.patch.dict("os.environ", {"MIGRATION_RELAY_LEASE_GRACE_SECONDS": "5"}):
            self.assertEqual(app_module.relay_lease_grace_seconds(), 5.0)

        self.assertEqual(app_module.relay_lease_grace_seconds(), 300.0)


if __name__ == "__main__":
    unittest.main()

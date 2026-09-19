"""源 VM 清单接口：一次拉全量、占用标记，以及批量重试接口。"""
import unittest
from unittest import mock

import app as app_module
from state_machine import JobStatus, MigrationMode, VmStatus, VmTask


class SourceVmsApiTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        self.payload = {
            "auth_url": "https://src.example.com:5000/v3",
            "username": "admin",
            "password": "pw",
        }

    def _post(self, payload=None):
        os_utils = mock.MagicMock()
        os_utils.list_source_servers_bundle.return_value = {
            "servers": [
                {"server_id": "srv-1", "name": "vm-1", "volume_count": 2},
                {"server_id": "srv-2", "name": "vm-2", "volume_count": 0},
            ],
            "next_marker": None,
        }
        with mock.patch.object(app_module, "OpenStackUtils", return_value=os_utils) as cls:
            resp = self.client.post(
                "/api/source-vms", json=dict(self.payload, **(payload or {}))
            )
        return resp, os_utils, cls

    def test_all_flag_asks_backend_for_every_page(self):
        resp, os_utils, _cls = self._post({"all": True})

        self.assertEqual(resp.status_code, 200)
        kwargs = os_utils.list_source_servers_bundle.call_args.kwargs
        self.assertTrue(kwargs["fetch_all"])
        self.assertEqual(kwargs["max_results"], app_module.SOURCE_VM_ALL_LIMIT)

    def test_paging_still_uses_marker(self):
        _resp, os_utils, _cls = self._post({"marker": "srv-9"})

        kwargs = os_utils.list_source_servers_bundle.call_args.kwargs
        self.assertFalse(kwargs["fetch_all"])
        self.assertEqual(kwargs["marker"], "srv-9")

    def test_busy_vms_are_flagged(self):
        job_id = "busy-source-vms"
        job = app_module.job_manager.create_job([], job_id=job_id)
        job.vms.append(VmTask(name="vm-1", target_az="az-1", mode=MigrationMode.FULL))
        try:
            resp, _os_utils, _cls = self._post({"all": True})
            data = resp.get_json()

            flags = {item["name"]: item["busy_in_job"] for item in data["servers"]}
            # 注意：busy_names 是跨作业聚合的，同进程里其它用例的作业也会算进来，
            # 所以只断言"本作业占用的 VM 被标出来"，不断言别的 VM 一定没被标。
            self.assertTrue(flags["vm-1"])
            self.assertIn("vm-1", data["busy_names"])
        finally:
            job.status = JobStatus.FAILED
            app_module.job_manager.delete(job_id)


class BulkDiskRetryApiTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        self.job_id = "bulk-retry-job"
        self.job = app_module.job_manager.create_job([], job_id=self.job_id)
        for name in ("vm-1", "vm-2", "vm-3"):
            vm = VmTask(name=name, target_az="az-1", mode=MigrationMode.FULL)
            vm.status = VmStatus.AWAITING_DISK_RETRY
            vm.set_phase("awaiting_disk_retry")
            vm.relay_disks = [
                {"volume_id": f"{name}-boot", "role": "boot", "status": "success"},
                {"volume_id": f"{name}-data", "role": "data", "status": "failed"},
            ]
            self.job.vms.append(vm)
        # vm-3 已经不在等待重试（正在拷贝）：批量接口必须跳过它。
        self.job.vms[2].status = VmStatus.COPYING_VOLUMES
        self.job.vms[2].set_phase("copying_volumes")
        self.job.vms[2].relay_disks = []
        app_module.job_manager.save_now()

    def tearDown(self):
        job = app_module.job_manager.get(self.job_id)
        if job is not None:
            job.status = JobStatus.FAILED
        app_module.job_manager.delete(self.job_id)

    def test_retry_all_queues_every_waiting_vm(self):
        resp = self.client.post(f"/api/jobs/{self.job_id}/relay/disks/retry-all")

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        requested = {item["vm"] for item in data["requested"]}
        self.assertEqual(requested, {"vm-1", "vm-2"})
        self.assertEqual([item["vm"] for item in data["skipped"]], [])
        for vm_name in ("vm-1", "vm-2"):
            self.assertEqual(
                [f"{vm_name}-data"],
                app_module.job_manager.take_disk_retry_request(self.job_id, vm_name),
            )

    def test_retry_all_reports_unknown_job(self):
        resp = self.client.post("/api/jobs/nope/relay/disks/retry-all")

        self.assertEqual(resp.status_code, 404)

    def test_retry_all_without_failed_disks_is_ok(self):
        for vm in self.job.vms:
            vm.relay_disks = []
        app_module.job_manager.save_now()

        resp = self.client.post(f"/api/jobs/{self.job_id}/relay/disks/retry-all")

        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["requested"], [])

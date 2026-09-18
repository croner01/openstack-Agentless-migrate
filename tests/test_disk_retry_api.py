"""失败盘手动重试：job_manager 状态位 + HTTP 接口。"""
import unittest

import app as app_module
from state_machine import JobStatus, VmStatus


class DiskRetryRequestTest(unittest.TestCase):
    def setUp(self):
        self.job_id = "retry-api-job"
        self.job = app_module.job_manager.create_job([], job_id=self.job_id)
        from state_machine import MigrationMode, VmTask

        self.job.vms.append(
            VmTask(name="vm-1", target_az="az-1", mode=MigrationMode.FULL)
        )
        self.vm = self.job.vms[0]
        self.vm.status = VmStatus.AWAITING_DISK_RETRY
        self.vm.set_phase("awaiting_disk_retry")
        self.vm.relay_disks = [
            {"volume_id": "vol-s1", "role": "boot", "status": "success", "error": ""},
            {"volume_id": "vol-s2", "role": "data", "status": "failed", "error": "404"},
        ]
        app_module.job_manager.save_now()
        self.client = app_module.app.test_client()

    def tearDown(self):
        job = app_module.job_manager.get(self.job_id)
        if job is not None:
            job.status = JobStatus.FAILED
        app_module.job_manager.delete(self.job_id)

    def test_retry_all_failed_disks(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/retry", json={}
        )

        self.assertEqual(resp.status_code, 200)
        self.assertEqual([], app_module.job_manager.take_disk_retry_request(
            self.job_id, "vm-1"))
        # 请求被取走后不能重复消费。
        self.assertIsNone(
            app_module.job_manager.take_disk_retry_request(self.job_id, "vm-1")
        )

    def test_retry_one_disk(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/retry",
            json={"volume_ids": ["vol-s2"]},
        )

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(
            ["vol-s2"],
            app_module.job_manager.take_disk_retry_request(self.job_id, "vm-1"),
        )

    def test_retry_rejects_unknown_or_healthy_disk(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/retry",
            json={"volume_ids": ["vol-s1"]},
        )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("不在失败列表", resp.get_json()["error"])

    def test_retry_conflicts_when_vm_not_waiting(self):
        self.vm.status = VmStatus.COPYING_VOLUMES
        self.vm.set_phase("relay_copying")

        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/retry", json={}
        )

        self.assertEqual(resp.status_code, 409)
        self.assertIn("不在等待失败盘重试", resp.get_json()["error"])

    def test_retry_unknown_vm_is_404(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-x/disks/retry", json={}
        )

        self.assertEqual(resp.status_code, 404)

    def test_request_survives_state_roundtrip(self):
        """等待期间进程重启也不能把"用户在等人点重试"这件事丢掉。"""
        from state_machine import MigrationJob

        app_module.job_manager.request_disk_retry(self.job_id, "vm-1", ["vol-s2"])
        restored = MigrationJob.from_dict(self.job.to_dict())
        vm = restored.vms[0]

        self.assertTrue(vm.disk_retry_requested)
        self.assertEqual(["vol-s2"], vm.disk_retry_volume_ids)


if __name__ == "__main__":
    unittest.main()

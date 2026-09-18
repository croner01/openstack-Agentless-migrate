"""失败盘中间卷释放：立即回收派生卷/快照的 HTTP 入口。"""
import unittest
from unittest import mock

import app as app_module
from state_machine import JobStatus, MigrationMode, VmStatus, VmTask


class DiskReleaseApiTest(unittest.TestCase):
    def setUp(self):
        self.job_id = "release-api-job"
        self.job = app_module.job_manager.create_job([], job_id=self.job_id)
        self.job.vms.append(
            VmTask(name="vm-1", target_az="az-1", mode=MigrationMode.FULL)
        )
        self.vm = self.job.vms[0]
        self.vm.status = VmStatus.AWAITING_DISK_RETRY
        self.vm.set_phase("awaiting_disk_retry")
        self.vm.relay_disks = [
            {
                "volume_id": "vol-s1",
                "role": "boot",
                "status": "success",
                "error": "",
                "retained_until": 0.0,
                "retain_reason": "",
            },
            {
                "volume_id": "vol-s2",
                "role": "data",
                "status": "failed",
                "error": "TimeoutError: attach",
                "retained_until": 9999999999.0,
                "retain_reason": "TimeoutError: attach",
            },
        ]
        app_module.job_manager.save_now()
        self.client = app_module.app.test_client()
        self.runtime = mock.MagicMock()
        self.runtime.release_retained.return_value = ["vol-s2"]
        app_module.register_runtime(self.job_id, self.runtime)

    def tearDown(self):
        app_module.drop_runtime(self.job_id)
        job = app_module.job_manager.get(self.job_id)
        if job is not None:
            job.status = JobStatus.FAILED
        app_module.job_manager.delete(self.job_id)

    def test_release_one_disk(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/release",
            json={"volume_ids": ["vol-s2"]},
        )

        self.assertEqual(resp.status_code, 200)
        body = resp.get_json()
        self.assertEqual(body["released"], ["vol-s2"])
        self.assertEqual(body["pending"], [])
        self.runtime.release_retained.assert_called_once_with(
            ["vol-s2"], reason="manual"
        )
        # 释放成功后盘上的保留标记要清掉，页面不再提示"保留中"。
        disk = self.vm.relay_disks[1]
        self.assertEqual(disk["retained_until"], 0.0)
        self.assertEqual(disk["retain_reason"], "")
        self.assertTrue(disk["released"])

    def test_release_all_retained_disks(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/release", json={}
        )

        self.assertEqual(resp.status_code, 200)
        self.runtime.release_retained.assert_called_once_with([], reason="manual")

    def test_release_rejects_disk_without_retained_copy(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/release",
            json={"volume_ids": ["vol-s1"]},
        )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("没有可释放的中间卷", resp.get_json()["error"])
        self.runtime.release_retained.assert_not_called()

    def test_release_conflicts_when_runtime_is_gone(self):
        app_module.drop_runtime(self.job_id)

        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-1/disks/release", json={}
        )

        self.assertEqual(resp.status_code, 409)
        self.assertIn("已随任务回收", resp.get_json()["error"])

    def test_release_unknown_vm_is_404(self):
        resp = self.client.post(
            f"/api/jobs/{self.job_id}/vms/vm-x/disks/release", json={}
        )

        self.assertEqual(resp.status_code, 404)

    def test_cancel_releases_retained_immediately(self):
        resp = self.client.post(f"/api/jobs/{self.job_id}/cancel")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["retained_released"], ["vol-s2"])
        self.runtime.release_retained.assert_called_once_with(reason="cancel")


class RuntimeRetentionInfoApiTest(unittest.TestCase):
    def test_runtime_reports_derived_retention_hours(self):
        client = app_module.app.test_client()

        body = client.get("/api/runtime").get_json()

        self.assertIn("derived_retention_hours", body["runtime"])
        self.assertGreaterEqual(body["runtime"]["derived_retention_hours"], 0)

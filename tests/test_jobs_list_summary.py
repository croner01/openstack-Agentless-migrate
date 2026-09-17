"""作业列表只回摘要：完整 VM/卷明细留给详情接口，避免列表响应体线性膨胀。"""
import unittest

import app as app_module
from excel_parser import MigrationRow
from state_machine import VmStatus


def _row(name: str) -> MigrationRow:
    return MigrationRow(vm_name=name, target_az="az-1", mode="full")


class JobsListSummaryTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _seed_job(self, job_id: str, statuses: list[VmStatus]):
        job = app_module.job_manager.create_job(
            [_row(f"vm-{index}") for index in range(len(statuses))],
            job_id=job_id,
        )
        for vm, status in zip(job.vms, statuses):
            vm.status = status
        return job

    def tearDown(self):
        app_module.job_manager.delete("summary-job")

    def test_list_returns_counts_not_vm_details(self):
        self._seed_job(
            "summary-job",
            [VmStatus.SUCCESS, VmStatus.FAILED, VmStatus.PREFLIGHT_FAILED,
             VmStatus.AWAITING_CUTOVER],
        )

        payload = self.client.get("/api/jobs").get_json()
        job = next(item for item in payload["jobs"] if item["id"] == "summary-job")

        self.assertNotIn("vms", job)
        self.assertEqual(job["vm_count"], 4)
        self.assertEqual(job["vm_status_counts"]["success"], 1)
        self.assertEqual(job["vm_status_counts"]["failed"], 1)
        self.assertEqual(job["vm_status_counts"]["preflight_failed"], 1)
        self.assertEqual(job["vm_status_counts"]["awaiting_cutover"], 1)
        # 列表页要用的元信息仍在
        self.assertIn("status", job)
        self.assertIn("created_at", job)
        self.assertIn("cancelled", job)

    def test_full_flag_keeps_legacy_payload(self):
        self._seed_job("summary-job", [VmStatus.SUCCESS])

        payload = self.client.get("/api/jobs?full=1").get_json()
        job = next(item for item in payload["jobs"] if item["id"] == "summary-job")

        self.assertIn("vms", job)
        self.assertEqual(job["vms"][0]["status"], "success")

    def test_detail_endpoint_still_returns_full_job(self):
        self._seed_job("summary-job", [VmStatus.SUCCESS])

        body = self.client.get("/api/jobs/summary-job").get_json()

        self.assertTrue(body["ok"])
        self.assertEqual(len(body["job"]["vms"]), 1)
        self.assertEqual(body["job"]["vms"][0]["name"], "vm-0")


if __name__ == "__main__":
    unittest.main()

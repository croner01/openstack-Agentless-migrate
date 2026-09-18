"""失败盘重试的页面元素：按钮、提示、并发输入与状态标签。"""
import unittest

import app as app_module


class DiskRetryRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_waiting_state_has_label_and_counter(self):
        html = self._html()

        self.assertIn("awaiting_disk_retry: ['badge-warn', '待重试']", html)
        self.assertIn("awaiting_disk_retry: '等待重试失败盘'", html)
        self.assertIn("['c-warn', '待重试 ', counts.retry]", html)

    def test_disk_table_exposes_retry_action(self):
        html = self._html()

        self.assertIn("function diskRetryCell(vm, disks)", html)
        self.assertIn("function retryVmDisk(vmName, volumeIds)", html)
        self.assertIn("'/disks/retry'", html)
        self.assertIn("renderVmDiskSummary($('#relay-vm-disks'), items, { retry: true });", html)
        self.assertIn('id="relay-retry-hint"', html)
        # 只有等待重试状态才给按钮，避免和正在跑的传输抢同一块盘。
        self.assertIn("if (vm.status !== 'awaiting_disk_retry') {", html)

    def test_relay_form_has_transfer_concurrency_and_wait(self):
        html = self._html()

        self.assertIn('name="relay_transfer_concurrency"', html)
        self.assertIn('name="relay_disk_retry_wait_seconds"', html)
        self.assertIn("'relay_transfer_concurrency', 'relay_disk_retry_wait_seconds',", html)


class DiskRetryDiagnoseTest(unittest.TestCase):
    def test_hint_mentions_waiting_for_manual_retry(self):
        job = type(
            "Job",
            (),
            {"status": app_module.JobStatus.RUNNING},
        )()
        vms = [
            {
                "name": "vm-1",
                "status": "awaiting_disk_retry",
                "phase": "awaiting_disk_retry",
                "phase_seconds": 120,
                "disks": {"total": 2, "done": 1, "failed": 1, "in_flight": 0},
            }
        ]

        hints = app_module._diagnose_hints(
            job, vms, {"active": 0, "concurrency": 1, "holders": []}, None, []
        )

        self.assertTrue(any("等待人工" in hint and "重试失败盘" in hint for hint in hints))


if __name__ == "__main__":
    unittest.main()

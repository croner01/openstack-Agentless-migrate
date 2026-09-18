"""提交参数校验失败时，不能留下永远 running 的"僵尸作业"。

历史缺陷：``api_migrate`` 先 ``job_manager.create_job()`` 再校验中转机参数，
``parse_relay_options`` 抛 ValueError 时直接返回 400，作业已经注册却没有执行
线程，状态永远停在 running，用户重试后又多出一个任务（表现为重复作业）。
"""
import json
import os
import tempfile
import threading
import unittest

import app as app_module
from job_manager import JobManager
from state_machine import JobStatus


def _selected_rows() -> str:
    return json.dumps(
        [
            {
                "vm_name": "vm-1",
                "server_id": "srv-1",
                "target_az": "az-1",
                "target_image": "img-1",
            }
        ]
    )


class MigrateSubmitValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig_upload = app_module.app.config["UPLOAD_FOLDER"]
        app_module.app.config["UPLOAD_FOLDER"] = self.tmp.name
        self.before = {job.id for job in app_module.job_manager.list_jobs(limit=1000)}

    def tearDown(self):
        app_module.app.config["UPLOAD_FOLDER"] = self._orig_upload
        self.tmp.cleanup()

    def _job_ids(self) -> set[str]:
        return {job.id for job in app_module.job_manager.list_jobs(limit=1000)}

    def test_invalid_relay_options_leaves_no_job(self):
        payload = {
            "data_channel": "relay",
            "selected_rows": _selected_rows(),
            # 临时模式下的建机参数一个都不填：parse_relay_options 必须报错。
            "relay_node_mode": "ephemeral",
        }
        with app_module.app.test_client() as client:
            resp = client.post("/api/migrate", data=payload)

        self.assertEqual(resp.status_code, 400)
        self.assertIn("中转机通道缺少必填项", resp.get_json()["error"])
        # 关键：不能凭空多出一个作业，也不能留下空的作业目录。
        self.assertEqual(self.before, self._job_ids())
        self.assertEqual([], os.listdir(self.tmp.name))

    def test_invalid_numeric_option_leaves_no_job(self):
        payload = {
            "data_channel": "relay",
            "selected_rows": _selected_rows(),
            "relay_node_mode": "ephemeral",
            "relay_source_az": "az-1",
            "relay_target_az": "az-1",
            "relay_source_image": "img-1",
            "relay_target_image": "img-1",
            "relay_source_flavor": "flv-1",
            "relay_target_flavor": "flv-1",
            "relay_source_network": "net-1",
            "relay_target_network": "net-1",
            "vm_concurrency": "99",
        }
        with app_module.app.test_client() as client:
            resp = client.post("/api/migrate", data=payload)

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(self.before, self._job_ids())
        self.assertEqual([], os.listdir(self.tmp.name))


class ZombieWorkerDetectionTest(unittest.TestCase):
    """诊断接口要能识别"running 但没有执行线程"的僵尸作业。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.manager = JobManager(
            state_file=os.path.join(self.tmp.name, "jobs_state.json")
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_running_job_without_worker_is_reported(self):
        job = self.manager.create_job([], job_id="zombie-test-1")

        self.assertFalse(self.manager.is_worker_alive("zombie-test-1"))
        hints = app_module._diagnose_hints(
            job, [], {"active": 0, "concurrency": 1, "holders": []}, None, [],
            self.manager.is_worker_alive("zombie-test-1"),
        )

        self.assertTrue(any("没有执行线程" in hint for hint in hints))

    def test_running_job_with_live_worker_has_no_hint(self):
        job = self.manager.create_job([], job_id="zombie-test-2")
        stop = threading.Event()
        thread = threading.Thread(target=stop.wait)
        thread.start()
        self.manager.register_worker("zombie-test-2", thread)
        try:
            self.assertTrue(self.manager.is_worker_alive("zombie-test-2"))
            hints = app_module._diagnose_hints(
                job, [], {"active": 0, "concurrency": 1, "holders": []}, None, [],
                self.manager.is_worker_alive("zombie-test-2"),
            )
            self.assertFalse(any("没有执行线程" in hint for hint in hints))
        finally:
            stop.set()
            thread.join(timeout=5)

    def test_cancel_converges_zombie_job_so_it_can_be_deleted(self):
        """僵尸作业没有被取消的对象：取消时必须直接结算，否则永远删不掉。"""
        self.manager.create_job([], job_id="zombie-test-4")

        self.assertTrue(self.manager.cancel("zombie-test-4"))
        job = self.manager.get("zombie-test-4")

        self.assertEqual(JobStatus.CANCELLED, job.status)
        self.assertIsNone(self.manager.delete("zombie-test-4"))

    def test_cancel_keeps_running_job_running_until_worker_stops(self):
        """有执行线程时不能替工作线程结算状态，仍由 worker 收尾。"""
        job = self.manager.create_job([], job_id="zombie-test-5")
        stop = threading.Event()
        thread = threading.Thread(target=stop.wait)
        thread.start()
        self.manager.register_worker("zombie-test-5", thread)
        try:
            self.assertTrue(self.manager.cancel("zombie-test-5"))
            self.assertEqual(JobStatus.RUNNING, job.status)
        finally:
            stop.set()
            thread.join(timeout=5)

    def test_finished_job_without_worker_has_no_hint(self):
        job = self.manager.create_job([], job_id="zombie-test-3")
        job.status = JobStatus.COMPLETED

        hints = app_module._diagnose_hints(
            job, [], {"active": 0, "concurrency": 1, "holders": []}, None, [], False,
        )

        self.assertFalse(any("没有执行线程" in hint for hint in hints))


if __name__ == "__main__":
    unittest.main()

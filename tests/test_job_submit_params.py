import json
import os
import tempfile
import unittest
from unittest import mock

import app as app_module


class SubmitParamsPersistenceTest(unittest.TestCase):
    """作业提交参数快照落盘：让「调整参数重新提交」不再依赖浏览器会话。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig_upload = app_module.app.config["UPLOAD_FOLDER"]
        app_module.app.config["UPLOAD_FOLDER"] = self.tmp.name
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.app.config["UPLOAD_FOLDER"] = self._orig_upload
        self.tmp.cleanup()

    def _save(self, job_id, payload):
        app_module._save_submit_params(job_id, json.dumps(payload))

    def test_save_then_load_round_trip(self):
        self._save("job-1", {"scheme": "relay_full", "form": {"vm_concurrency": "2"}})

        params = app_module._load_submit_params("job-1")

        self.assertEqual(params["scheme"], "relay_full")
        self.assertEqual(params["form"]["vm_concurrency"], "2")

    def test_password_fields_are_not_written_to_disk(self):
        """快照会落盘，口令类字段必须剔除，不能进 jobs 目录。"""
        self._save(
            "job-1",
            {
                "form": {
                    "source_password": "s3cret",
                    "target_password": "s3cret",
                    "relay_admin_password": "s3cret",
                    "admin_password": "s3cret",
                    "source_username": "admin",
                    "relay_ssh_public_key": "ssh-ed25519 AAAA",
                }
            },
        )

        with open(
            os.path.join(self.tmp.name, "job_params", "job-1.json"), encoding="utf-8"
        ) as handle:
            raw = handle.read()
        params = app_module._load_submit_params("job-1")

        self.assertNotIn("s3cret", raw)
        self.assertEqual(params["form"]["source_username"], "admin")
        self.assertEqual(params["form"]["relay_ssh_public_key"], "ssh-ed25519 AAAA")
        self.assertNotIn("admin_password", params["form"])
        self.assertNotIn("relay_admin_password", params["form"])

    def test_nested_rows_are_kept(self):
        self._save("job-1", {"rows": [{"vm_name": "vm-1", "target_az": "az1"}]})

        params = app_module._load_submit_params("job-1")

        self.assertEqual(params["rows"][0]["vm_name"], "vm-1")

    def test_invalid_json_is_ignored(self):
        app_module._save_submit_params("job-1", "{not json")

        self.assertIsNone(app_module._load_submit_params("job-1"))

    def test_oversized_snapshot_is_skipped(self):
        with mock.patch.object(app_module, "SUBMIT_PARAMS_MAX_BYTES", 10):
            self._save("job-1", {"scheme": "rbd_full"})

        self.assertIsNone(app_module._load_submit_params("job-1"))

    def test_non_object_snapshot_is_ignored(self):
        app_module._save_submit_params("job-1", json.dumps([1, 2, 3]))

        self.assertIsNone(app_module._load_submit_params("job-1"))

    def test_params_endpoint_returns_saved_snapshot(self):
        self._save("job-1", {"scheme": "rbd_full", "profileId": "p-1"})

        with mock.patch.object(app_module, "job_manager") as manager:
            manager.get.return_value = mock.Mock()
            response = self.client.get("/api/jobs/job-1/params")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["params"]["profileId"], "p-1")

    def test_params_endpoint_404_for_unknown_job(self):
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.get.return_value = None
            response = self.client.get("/api/jobs/missing/params")

        self.assertEqual(response.status_code, 404)

    def test_params_endpoint_404_without_snapshot(self):
        """旧作业（改动前提交的）没落盘，要给可操作的提示而不是空对象。"""
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.get.return_value = mock.Mock()
            response = self.client.get("/api/jobs/job-old/params")

        self.assertEqual(response.status_code, 404)
        self.assertIn("没有保存", response.get_json()["error"])

    def test_delete_job_removes_snapshot(self):
        self._save("job-1", {"scheme": "rbd_full"})
        path = app_module._submit_params_path("job-1")
        self.assertTrue(os.path.exists(path))

        with mock.patch.object(app_module, "job_manager") as manager, \
                mock.patch.object(app_module, "drop_runtime"):
            manager.delete.return_value = None
            response = self.client.delete("/api/jobs/job-1")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(os.path.exists(path))

    def test_delete_job_without_snapshot_still_succeeds(self):
        with mock.patch.object(app_module, "job_manager") as manager, \
                mock.patch.object(app_module, "drop_runtime"):
            manager.delete.return_value = None
            response = self.client.delete("/api/jobs/job-nope")

        self.assertEqual(response.status_code, 200)


class SubmitParamsFromFormTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig_upload = app_module.app.config["UPLOAD_FOLDER"]
        app_module.app.config["UPLOAD_FOLDER"] = self.tmp.name

    def tearDown(self):
        app_module.app.config["UPLOAD_FOLDER"] = self._orig_upload
        self.tmp.cleanup()

    def test_submit_snapshot_is_persisted_from_the_form(self):
        """提交流程本身要落盘：表单里带了 submit_snapshot 就直接存下来。"""
        payload = {"scheme": "relay_full", "admin_password": "p@ss"}
        with app_module.app.test_request_context(
            "/api/migrate",
            method="POST",
            data={"submit_snapshot": json.dumps(payload)},
        ):
            app_module._save_submit_params(
                "job-form", app_module.request.form.get("submit_snapshot")
            )

        params = app_module._load_submit_params("job-form")
        self.assertEqual(params["scheme"], "relay_full")
        self.assertNotIn("admin_password", params)


if __name__ == "__main__":
    unittest.main()

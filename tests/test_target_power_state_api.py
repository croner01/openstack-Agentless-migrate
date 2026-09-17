import json
import tempfile
import unittest

import app as app_module
from state_machine import VmTask


class TargetPowerStateSerializationTest(unittest.TestCase):
    """作业详情页读 ``start_target`` 判断是否展示「迁移后不开机」。"""

    def test_serialize_vm_exposes_start_target(self):
        vm = VmTask(name="vm-a", target_az="az1", start_target=False)

        payload = app_module._serialize_vm(vm)

        self.assertFalse(payload["start_target"])

    def test_serialize_vm_defaults_to_boot(self):
        vm = VmTask(name="vm-a", target_az="az1")

        self.assertTrue(app_module._serialize_vm(vm)["start_target"])


class RowOverrideStartTargetTest(unittest.TestCase):
    """新建迁移时前端逐台 VM 传 ``start_target``，后端必须落到行上。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig_upload = app_module.app.config["UPLOAD_FOLDER"]
        app_module.app.config["UPLOAD_FOLDER"] = self.tmp.name

    def tearDown(self):
        app_module.app.config["UPLOAD_FOLDER"] = self._orig_upload
        self.tmp.cleanup()

    def _call(self, overrides):
        payload = {
            "data_channel": "relay",
            "selected_rows": json.dumps(
                [
                    {
                        "vm_name": "vm-1",
                        "server_id": "srv-1",
                        "target_az": "az1",
                    }
                ]
            ),
        }
        if overrides is not None:
            payload["row_overrides"] = json.dumps(overrides)
        with app_module.app.test_request_context(
            "/api/migrate", method="POST", data=payload
        ):
            return app_module._create_job_and_files(
                require_ceph=False, require_target_image=False
            )

    def test_override_false_disables_boot(self):
        _job_id, _job_dir, rows, _src, _tgt = self._call(
            {"vm-1": {"start_target": False}}
        )

        self.assertFalse(rows[0].start_target)

    def test_override_accepts_string_literal(self):
        _job_id, _job_dir, rows, _src, _tgt = self._call(
            {"vm-1": {"start_target": "否"}}
        )

        self.assertFalse(rows[0].start_target)

    def test_missing_override_keeps_default_boot(self):
        _job_id, _job_dir, rows, _src, _tgt = self._call(None)

        self.assertTrue(rows[0].start_target)

    def test_invalid_override_is_rejected(self):
        with self.assertRaises(ValueError):
            self._call({"vm-1": {"start_target": "也许"}})


if __name__ == "__main__":
    unittest.main()

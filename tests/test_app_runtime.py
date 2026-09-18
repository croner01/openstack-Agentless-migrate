import os
import unittest
from unittest import mock

import app as app_module
from ceph_utils import CopyGate


class RuntimeInfoApiTest(unittest.TestCase):
    """设置页「生效参数」读的是 /api/runtime：只回传调优数值，不能漏凭据。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def test_reports_copy_gate_limits_and_holders(self):
        with mock.patch.object(
            app_module.COPY_GATE, "max_active_copies", 2
        ), mock.patch.object(
            CopyGate, "active_copies", new_callable=mock.PropertyMock, return_value=1
        ), mock.patch.object(
            app_module.COPY_GATE,
            "holders",
            return_value=[("vm-a/volume-1", 322.4)],
        ):
            payload = self.client.get("/api/runtime").get_json()

        self.assertTrue(payload["ok"])
        runtime = payload["runtime"]
        self.assertEqual(runtime["rbd_copy_concurrency"], 2)
        self.assertEqual(runtime["rbd_copy_active"], 1)
        self.assertEqual(
            runtime["copy_holders"],
            [{"owner": "vm-a/volume-1", "seconds": 322}],
        )

    def test_reports_env_tunables(self):
        env = {
            "MIGRATION_MAX_UPLOAD_MB": "64",
            "MIGRATION_UPLOAD_RETENTION_DAYS": "3",
            "MIGRATION_RBD_CMD_TIMEOUT_SECONDS": "45",
            "MIGRATION_COPY_STALL_SECONDS": "90",
        }
        with mock.patch.dict(os.environ, env):
            runtime = self.client.get("/api/runtime").get_json()["runtime"]

        self.assertEqual(runtime["max_upload_mb"], 64)
        self.assertEqual(runtime["upload_retention_days"], 3)
        self.assertEqual(runtime["rbd_cmd_timeout_seconds"], 45.0)
        self.assertEqual(runtime["copy_stall_timeout_seconds"], 90.0)

    def test_reports_preflight_switch(self):
        default = self.client.get("/api/runtime").get_json()["runtime"]
        self.assertTrue(default["ceph_preflight"])

        with mock.patch.dict(os.environ, {"MIGRATION_CEPH_PREFLIGHT": "off"}):
            disabled = self.client.get("/api/runtime").get_json()["runtime"]

        self.assertFalse(disabled["ceph_preflight"])

    def test_payload_has_no_credentials(self):
        runtime = self.client.get("/api/runtime").get_json()["runtime"]

        self.assertEqual(
            set(runtime),
            {
                "diag_version",
                "stopping",
                "api_token_enabled",
                "rbd_copy_concurrency",
                "rbd_copy_active",
                "copy_holders",
                "memory_high_water",
                "copy_reserve_mb",
                "rbd_cmd_timeout_seconds",
                "copy_stall_timeout_seconds",
                "relay_heartbeat_interval",
                "relay_heartbeat_timeout",
                "relay_rebuild_seconds",
                "max_upload_mb",
                "upload_retention_days",
                "ceph_preflight",
                "log_only_migration",
                "log_only_migration_configured",
                "log_file",
            },
        )
        self.assertIsInstance(runtime["api_token_enabled"], bool)

    def test_reports_log_filter_switch(self):
        self.addCleanup(app_module._setup_logging)
        with mock.patch.dict(
            os.environ,
            {"MIGRATION_LOG_ONLY": "on", "MIGRATION_HTTP_DEBUG": ""},
        ):
            app_module._setup_logging()
            runtime = self.client.get("/api/runtime").get_json()["runtime"]

        self.assertTrue(runtime["log_only_migration"])
        self.assertTrue(runtime["log_only_migration_configured"])

    def test_log_filter_reports_off(self):
        self.addCleanup(app_module._setup_logging)
        with mock.patch.dict(
            os.environ,
            {"MIGRATION_LOG_ONLY": "off", "MIGRATION_HTTP_DEBUG": ""},
        ):
            app_module._setup_logging()
            runtime = self.client.get("/api/runtime").get_json()["runtime"]

        self.assertFalse(runtime["log_only_migration"])
        self.assertFalse(runtime["log_only_migration_configured"])


if __name__ == "__main__":
    unittest.main()

import io
import json
import os
import tempfile
import unittest
from unittest import mock

import app as app_module


class CephConfNewlineTest(unittest.TestCase):
    """conf 落盘时必须带结尾换行：ceph 会静默丢掉没有换行的最后一行。"""

    def test_conf_without_trailing_newline_is_normalized(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ceph.conf")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("[global]\nkey = AQxxxx")

            app_module._ensure_trailing_newline(path)

            with open(path, encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "[global]\nkey = AQxxxx\n")

    def test_existing_newline_is_kept_as_is(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ceph.conf")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("[global]\n")

            app_module._ensure_trailing_newline(path)

            with open(path, encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "[global]\n")

    def test_missing_file_is_ignored(self):
        # 路径为空或文件不存在时静默返回，不能因为"顺手补个换行"让提交失败。
        app_module._ensure_trailing_newline("")
        app_module._ensure_trailing_newline("/tmp/no-such-ceph-conf-xyz.conf")


class CephPreflightTest(unittest.TestCase):
    """提交前拦住 mon 不可达的 conf：否则 rbd 会永久卡住并占满拷贝名额。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.upload_dir = os.path.join(self.tmp.name, "uploads")
        os.makedirs(self.upload_dir, exist_ok=True)
        original = app_module.app.config["UPLOAD_FOLDER"]
        self.addCleanup(
            lambda: app_module.app.config.__setitem__("UPLOAD_FOLDER", original)
        )
        app_module.app.config["UPLOAD_FOLDER"] = self.upload_dir

    def _conf(self, name: str, text: str) -> str:
        path = os.path.join(self.tmp.name, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)
        return path

    def test_unreachable_mon_is_rejected(self):
        conf = self._conf("bad.conf", "[global]\nmon_host = 203.0.113.7\n")

        with mock.patch.object(app_module, "any_mon_reachable", return_value=False):
            error = app_module._ceph_conf_reachability_error(conf, conf)

        self.assertIn("不可达", error)
        self.assertIn("203.0.113.7:6789", error)

    def test_reachable_mon_passes(self):
        conf = self._conf("ok.conf", "[global]\nmon_host = 10.6.10.4\n")

        with mock.patch.object(app_module, "any_mon_reachable", return_value=True):
            self.assertEqual(
                app_module._ceph_conf_reachability_error(conf, conf), ""
            )

    def test_conf_without_mon_host_is_not_blocked(self):
        # 解析不出 mon（自定义写法/DNS-SD）时一律放行，不能因为"看不懂"挡住提交。
        conf = self._conf("weird.conf", "[global]\n# mon 走 DNS-SD\n")

        with mock.patch.object(
            app_module, "any_mon_reachable", return_value=False
        ) as probe:
            self.assertEqual(
                app_module._ceph_conf_reachability_error(conf, conf), ""
            )

        self.assertFalse(probe.called)

    def test_source_conf_is_checked_too(self):
        bad = self._conf("s.conf", "[global]\nmon_host = 203.0.113.7\n")
        good = self._conf("t.conf", "[global]\nmon_host = 10.6.10.4\n")

        with mock.patch.object(app_module, "any_mon_reachable", return_value=False):
            error = app_module._ceph_conf_reachability_error(bad, good)

        self.assertIn("源 Ceph 不可达", error)

    def test_can_be_disabled_by_env(self):
        conf = self._conf("bad.conf", "[global]\nmon_host = 203.0.113.7\n")

        with mock.patch.dict(os.environ, {"MIGRATION_CEPH_PREFLIGHT": "off"}), \
                mock.patch.object(
                    app_module, "any_mon_reachable", return_value=False
                ) as probe:
            self.assertEqual(
                app_module._ceph_conf_reachability_error(conf, conf), ""
            )

        self.assertFalse(probe.called)

    def test_submit_is_blocked_and_job_dir_cleaned_up(self):
        client = app_module.app.test_client()
        data = {
            "selected_rows": json.dumps(
                [
                    {
                        "server_id": "s1",
                        "vm_name": "vm1",
                        "target_az": "az1",
                        "target_image": "img1",
                    }
                ]
            ),
            "source_ceph_conf_file": (
                io.BytesIO(b"[global]\nmon_host = 203.0.113.7\n"),
                "s.conf",
            ),
            "target_ceph_conf_file": (
                io.BytesIO(b"[global]\nmon_host = 203.0.113.7\n"),
                "t.conf",
            ),
        }

        with mock.patch.object(app_module, "any_mon_reachable", return_value=False):
            response = client.post(
                "/api/migrate", data=data, content_type="multipart/form-data"
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("不可达", response.get_json()["error"])
        # 校验没过就不该留下空作业目录。
        self.assertEqual(os.listdir(self.upload_dir), [])

import os
import tempfile
import unittest

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

"""日志降噪开关：MIGRATION_LOG_ONLY 打开时只留迁移相关日志。"""
import logging
import os
import pathlib
import tempfile
import unittest
from unittest import mock

import app as app_module


def _record(name: str, level: int, message: str) -> logging.LogRecord:
    return logging.LogRecord(name, level, __file__, 1, message, None, None)


class LogOnlyMigrationFlagTest(unittest.TestCase):
    def test_defaults_to_migration_only(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MIGRATION_LOG_ONLY", None)

            self.assertTrue(app_module.log_only_migration())

    def test_off_values_restore_full_logs(self):
        for raw in ("off", "0", "false", "no", "OFF"):
            with self.subTest(raw=raw), mock.patch.dict(
                os.environ, {"MIGRATION_LOG_ONLY": raw}
            ):
                self.assertFalse(app_module.log_only_migration())

    def test_on_values_keep_migration_only(self):
        for raw in ("on", "1", "true", "yes"):
            with self.subTest(raw=raw), mock.patch.dict(
                os.environ, {"MIGRATION_LOG_ONLY": raw}
            ):
                self.assertTrue(app_module.log_only_migration())


class MigrationOnlyFilterTest(unittest.TestCase):
    def setUp(self):
        self.log_filter = app_module.MigrationOnlyFilter()

    def test_migration_records_pass(self):
        # 迁移自身的日志走 root logger，没有前缀也要保留。
        self.assertTrue(
            self.log_filter.filter(_record("root", logging.INFO, "VM vm-1 阶段 -> copying"))
        )
        self.assertTrue(
            self.log_filter.filter(
                _record("openstack", logging.INFO, "[MIGRATION] 等待 RBD 拷贝名额")
            )
        )

    def test_third_party_info_and_warning_dropped(self):
        self.assertFalse(
            self.log_filter.filter(
                _record("urllib3.connectionpool", logging.INFO, "Starting new HTTP connection")
            )
        )
        self.assertFalse(
            self.log_filter.filter(_record("werkzeug", logging.WARNING, "development server"))
        )

    def test_third_party_error_kept(self):
        # 降噪不能吞掉库里的真实报错。
        self.assertTrue(self.log_filter.filter(_record("openstack", logging.ERROR, "boom")))
        self.assertTrue(
            self.log_filter.filter(_record("keystoneauth", logging.CRITICAL, "auth failed"))
        )


class SetupLoggingFilterTest(unittest.TestCase):
    def tearDown(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MIGRATION_LOG_ONLY", None)
            os.environ.pop("MIGRATION_HTTP_DEBUG", None)
            app_module._setup_logging()

    def _filters(self):
        return [
            item
            for handler in logging.getLogger().handlers
            for item in handler.filters
        ]

    def test_on_installs_filter_on_every_handler(self):
        with mock.patch.dict(os.environ, {"MIGRATION_LOG_ONLY": "on"}):
            app_module._setup_logging()

        handlers = logging.getLogger().handlers
        self.assertTrue(handlers)
        for handler in handlers:
            self.assertTrue(
                any(
                    isinstance(item, app_module.MigrationOnlyFilter)
                    for item in handler.filters
                ),
                handler,
            )
        self.assertTrue(app_module._LOG_FILTER_ACTIVE)
        self.assertEqual(logging.getLogger("urllib3").level, logging.WARNING)

    def test_off_leaves_handlers_unfiltered(self):
        with mock.patch.dict(os.environ, {"MIGRATION_LOG_ONLY": "off"}):
            app_module._setup_logging()

        self.assertFalse(self._filters())
        self.assertFalse(app_module._LOG_FILTER_ACTIVE)
        self.assertEqual(logging.getLogger("urllib3").level, logging.NOTSET)

    def test_http_debug_wins_over_filter(self):
        with mock.patch.dict(
            os.environ, {"MIGRATION_LOG_ONLY": "on", "MIGRATION_HTTP_DEBUG": "1"}
        ):
            app_module._setup_logging()

        self.assertFalse(self._filters())
        self.assertFalse(app_module._LOG_FILTER_ACTIVE)

    def test_log_file_keeps_app_lines_and_drops_library_noise(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "vm.log")
            with mock.patch.dict(os.environ, {"MIGRATION_LOG_ONLY": "on"}), mock.patch.object(
                app_module, "LOG_FILE", path
            ):
                app_module._setup_logging()
                logging.getLogger().info("[MIGRATION] 这条要保留")
                logging.getLogger("urllib3.connectionpool").info("这条 INFO 要过滤")
                logging.getLogger("urllib3.connectionpool").warning("这条 WARNING 要过滤")
                logging.getLogger("openstack").error("这条 ERROR 要保留")
                for handler in logging.getLogger().handlers:
                    handler.flush()

            content = pathlib.Path(path).read_text(encoding="utf-8")

        self.assertIn("这条要保留", content)
        self.assertNotIn("要过滤", content)
        self.assertIn("这条 ERROR 要保留", content)

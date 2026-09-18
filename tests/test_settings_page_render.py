import unittest

import app as app_module


class SettingsPageRenderTest(unittest.TestCase):
    """平台设置页的契约：主题可选、有可用操作、运行参数来自 /api/runtime。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_theme_cards_offer_light_dark_and_system(self):
        html = self._html()

        for choice in ('data-theme-choice="light"', 'data-theme-choice="dark"',
                       'data-theme-choice="auto"'):
            self.assertIn(choice, html)
        self.assertNotIn('id="set-theme"', html)
        self.assertNotIn("规划项", html)

    def test_theme_switch_functions_and_bootstrap_exist(self):
        html = self._html()

        self.assertIn("function applyTheme", html)
        self.assertIn("function syncThemeUI", html)
        self.assertIn("function initThemeToggle", html)
        self.assertIn("localStorage.getItem('migrate-theme')", html)
        head = html.split("</head>", 1)[0]
        self.assertIn("migrate-theme", head)

    def test_settings_page_has_usable_quick_actions(self):
        html = self._html()

        for element_id in ("set-refresh-jobs", "set-recheck-api", "set-goto-wizard",
                           "set-goto-profiles", "btn-set-token", "btn-clear-token"):
            self.assertIn('id="' + element_id + '"', html)

    def test_runtime_parameters_come_from_api(self):
        html = self._html()

        self.assertIn("/api/runtime", html)
        for element_id in ("set-copy-slots", "set-mem-water", "set-rbd-timeout",
                           "set-stall-timeout", "set-upload-limits", "set-preflight",
                           "set-log-filter"):
            self.assertIn('id="' + element_id + '"', html)
        self.assertIn("MIGRATION_LOG_ONLY", html)


if __name__ == "__main__":
    unittest.main()

import unittest

import app as app_module


class RelayProgressRenderTest(unittest.TestCase):
    """中转机通道的卷拷贝进度必须与 RBD 路径共用同一个进度条组件。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_progress_bar_helper_is_the_only_bar_builder(self):
        html = self._html()

        self.assertIn("function progressBarHTML(label, percent, throughputMbS)", html)
        # 两套路径都拼同一段 markup，重复实现会让两侧观感再次分叉。
        self.assertEqual(html.count('class="progress-wrap"'), 1)
        self.assertNotIn('class="progress-bar" style', html)

    def test_rbd_and_relay_paths_share_the_helper(self):
        html = self._html()

        self.assertIn("return progressBarHTML(label, percent, volume.throughput_mb_s);", html)
        self.assertIn("return progressBarHTML('', percent, volume.throughput_mb_s);", html)

    def test_relay_progress_renders_as_table(self):
        html = self._html()

        self.assertIn('<tbody id="relay-volume-progress">', html)
        self.assertIn("<th>VM</th><th>卷</th><th>状态</th>", html)
        self.assertIn("function relayPhaseBadge(volume)", html)
        self.assertIn("function relayVolumeBytes(volume)", html)

    def test_relay_done_volume_matches_rbd_success_reading(self):
        html = self._html()

        self.assertIn("if (volume.phase === 'done') return '100%';", html)


if __name__ == "__main__":
    unittest.main()

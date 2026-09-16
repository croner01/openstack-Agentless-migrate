import unittest

import app as app_module


class WizardLayoutRenderTest(unittest.TestCase):
    """长网络名不能把同行的下拉挤出容器：列轨必须可收缩（minmax(0, 1fr)）。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _style(self) -> str:
        html = self.client.get("/").get_data(as_text=True)
        return html.split("<style>", 1)[1].split("</style>", 1)[0]

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_form_grid_tracks_can_shrink(self):
        style = self._style()

        self.assertIn(".form-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));", style)
        self.assertIn(".form-grid.cols-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }", style)
        self.assertIn(".form-grid.cols-4 { grid-template-columns: repeat(4, minmax(0, 1fr)); }", style)
        self.assertIn(".form-grid > div { min-width: 0; }", style)
        self.assertIn(".form-grid .fld, .form-grid .sel, .form-grid select { width: 100%; min-width: 0; }", style)

    def test_relay_pools_share_width_instead_of_inline_flex(self):
        style = self._style()
        html = self._html()

        self.assertIn(".pool-cols {", style)
        self.assertIn(".pool-col {", style)
        self.assertIn("flex: 1 1 0; min-width: 0;", style)
        self.assertIn('class="pool-cols"', html)
        self.assertIn('class="pool-col" id="relay-source-pool"', html)
        self.assertIn('class="pool-col" id="relay-target-pool"', html)

    def test_nic_rows_and_selects_are_constrained(self):
        style = self._style()
        html = self._html()

        self.assertIn("minmax(140px, 1.2fr) minmax(0, 1fr) minmax(0, 1fr)", style)
        self.assertIn(".nic-row .sel, .nic-row .fld { width: 100%; min-width: 0; }", style)
        # 被裁掉的长名字要能悬停看全
        self.assertIn("function syncSelectTitle", html)
        self.assertIn("select.title = option ? option.textContent : '';", html)


if __name__ == "__main__":
    unittest.main()

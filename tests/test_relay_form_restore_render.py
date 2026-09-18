"""中转机参数回放：动态下拉的 option 还没到位时不能把值丢掉。

缺陷表现：点「调整参数重新提交」后，中转机 AZ/镜像等下拉在「载入两侧目录」
之前没有 option，``applyWizardForm`` 直接赋 value 会被浏览器清空；再提交时
后端报"中转机通道缺少必填项"，旧代码还会因此留下永远 running 的僵尸作业。
"""
import unittest

import app as app_module


class RelayFormRestoreRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_form_replay_keeps_values_for_unready_selects(self):
        html = self._html()

        self.assertIn("function setSelectValue(el, value)", html)
        self.assertIn("function reapplyPendingSelectValues(root)", html)
        self.assertIn("else if (el.tagName === 'SELECT') setSelectValue(el, form[key]);", html)
        self.assertIn("el.dataset.pendingValue = wanted;", html)
        self.assertIn("el.removeAttribute('data-pending-value');", html)

    def test_relay_catalog_reapplies_pending_values(self):
        html = self._html()

        # 网络要先补上，子网才按它过滤；随后再统一补 AZ 等挂起值。
        self.assertIn("先把回放的网络补上，下面的子网列表才能按它过滤", html)
        self.assertIn("回放的 AZ 优先于", html)
        self.assertIn("reapplyPendingSelectValues(zones[side]);", html)
        self.assertIn("reapplyPendingSelectValues();\n    syncSelectTitle(select);", html)

    def test_empty_relay_build_params_block_submit_client_side(self):
        html = self._html()

        self.assertIn("function relayChannelIssues()", html)
        self.assertIn("const relayMissing = relayChannelIssues();", html)
        self.assertIn("中转机通道缺少：", html)
        self.assertIn("载入两侧目录 / 检查常驻池", html)

    def test_guard_mirrors_backend_required_fields(self):
        html = self._html()

        for field in (
            "relay_platform_url",
            "relay_source_image",
            "relay_source_flavor",
            "relay_source_az",
            "relay_source_system_volume_type",
            "relay_target_image",
            "relay_target_flavor",
            "relay_target_az",
            "relay_target_system_volume_type",
        ):
            self.assertIn("['" + field + "',", html)


if __name__ == "__main__":
    unittest.main()

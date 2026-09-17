import unittest

import app as app_module


class RelayPoolReuseRenderTest(unittest.TestCase):
    """已有常驻池时，作业表单不该再要求填一遍建机参数。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_catalog_reports_existing_pools(self):
        """/api/relay/catalog 顺带回已有常驻池，前端才能沿用其建机参数。"""
        html = self._html()

        self.assertIn("json.pools", html)
        self.assertIn("state.relayExistingPools[side] = pools", html)

    def test_backend_exposes_pools_in_catalog_response(self):
        import json
        from unittest import mock

        with mock.patch.object(app_module, "build_catalog", return_value={"source": {}, "target": {}}), \
                mock.patch.object(app_module, "_existing_relay_pools", return_value=[{"az": "az-1"}]) as pools, \
                mock.patch.object(app_module, "OpenStackUtils"):
            body = json.loads(
                app_module.app.test_client().post("/api/relay/catalog", data={}).get_data(as_text=True)
            )

        self.assertTrue(body["ok"])
        self.assertEqual(body["pools"]["source"], [{"az": "az-1"}])
        self.assertEqual(body["pools"]["target"], [{"az": "az-1"}])
        self.assertEqual(pools.call_count, 2)

    def test_existing_pool_hides_creation_params(self):
        """复用面板：预填池参数、收起建机参数、说明去哪里改。"""
        html = self._html()

        self.assertIn("function refreshRelayPoolPanel", html)
        self.assertIn("沿用已有常驻池", html)
        self.assertIn("details.open = false", html)
        self.assertIn("中转机资源", html)

    def test_az_select_stays_outside_the_creation_group(self):
        """AZ 决定沿用哪个池，必须始终可见（建机参数才收进 details）。"""
        html = self._html()

        self.assertIn('name="relay_${prefix}_az"', html)
        self.assertIn('id="relay-${prefix}-reuse"', html)
        self.assertIn('id="relay-${prefix}-params"', html)

    def test_switching_to_persistent_mode_checks_existing_pools(self):
        """切到常驻模式自动查一次已有池，避免用户还要手点目录。"""
        html = self._html()

        self.assertIn("loadRelayCatalog();", html)
        self.assertIn("检查常驻池", html)

    def test_switching_cloud_invalidates_cached_pools(self):
        """换云（凭据变了）后旧池必须失效，否则会沿用上一个云的常驻池。"""
        html = self._html()

        self.assertIn("function relayCatalogKey", html)
        self.assertIn("state.relayCatalogKey !== relayCatalogKey()", html)
        self.assertIn("state.relayCatalogKey = relayCatalogKey();", html)

    def test_catalog_missing_keys_do_not_break_panel(self):
        """目录里缺 azs 之类的键时只跳过该段，不能让整个复用面板渲染失败。"""
        html = self._html()

        self.assertIn("(items || []).forEach(item => {", html)
        self.assertIn("(data.azs || []).filter(az => !poolAzs.includes(az))", html)

    def test_prefilled_values_survive_missing_catalog_entries(self):
        """池里的镜像/网络可能已不在目录里，等值提交而不是清空。"""
        html = self._html()

        self.assertIn("function setRelaySelectValue", html)
        self.assertIn("（池内参数，当前目录里没有）", html)


if __name__ == "__main__":
    unittest.main()

import re
import unittest

import app as app_module


class RelayResourcesRenderTest(unittest.TestCase):
    """中转机资源页只做扩缩容与删机，且不允许出现需要手填的输入框。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def _view_markup(self) -> str:
        match = re.search(r'<section id="view-relay".*?</section>', self._html(), re.S)
        self.assertIsNotNone(match, "缺少 #view-relay 容器")
        return match.group(0)

    def test_view_is_reachable_from_nav_and_router(self):
        html = self._html()

        self.assertIn('data-nav="relay"', html)
        self.assertIn("relay: '中转机资源'", html)
        self.assertIn("(jobs|wizard|profiles|relay|settings)", html)
        self.assertIn("if (name === 'relay') loadRelayResources();", html)

    def test_view_has_no_manual_input(self):
        markup = self._view_markup()

        self.assertNotIn("<input", markup)
        self.assertNotIn("<select", markup)
        self.assertIn('id="relay-res-body"', markup)
        self.assertIn('id="relay-res-refresh"', markup)

    def test_actions_are_derived_from_platform_state(self):
        html = self._html()

        # 池/节点的身份全部来自接口，页面不出现 tenant_key/node_id 表单字段
        self.assertIn("relayJSON('/api/relay/pools')", html)
        self.assertIn("relayJSON('/api/relay/nodes')", html)
        self.assertIn("'/api/relay/pools/scale'", html)
        self.assertIn("'/api/relay/nodes/' + encodeURIComponent(node.node_id)", html)
        self.assertNotIn('name="tenant_key"', html)

    def test_scale_down_lowers_the_floor_automatically(self):
        html = self._html()

        self.assertIn("async function relayLowerFloor(pool, target)", html)
        self.assertIn("if (belowFloor) await relayLowerFloor(pool, target);", html)
        self.assertIn("profile.min_nodes = target;", html)
        self.assertIn("'/api/relay/pools/profile'", html)

    def test_tenant_key_is_shown_as_a_short_label(self):
        html = self._html()

        self.assertIn("function relayTenantShort(tenantKey)", html)
        self.assertIn("relayTenantShort(pool.tenant_key)", html)


if __name__ == "__main__":
    unittest.main()

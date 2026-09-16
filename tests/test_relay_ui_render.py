import unittest

import app as app_module
from state_machine import MigrationMode, VolumeTask


class RelayPageRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_volume_progress_cell_renders_phase_label(self):
        """增量预拷贝阶段要和全量一样显示「标签 · 百分比 · 速率」。"""
        html = self._html()

        self.assertIn("volume.progress_label", html)

    def test_ready_volume_has_its_own_badge(self):
        """一轮搬完的卷显示「已就绪」，不能一直挂着「拷贝中」。"""
        html = self._html()

        self.assertIn("ready: ['badge-success', '已就绪']", html)

    def test_volume_payload_carries_mode_and_cleanup_status(self):
        """页面的「模式」「快照清理」两列读的是这两个字段，API 不能漏传。"""
        volume = VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_rbd_name="volume-t1",
            mode=MigrationMode.INCREMENTAL,
            cleanup_status="cleaned",
        )

        payload = app_module._serialize_volume(volume)

        self.assertEqual(payload["mode"], "incremental")
        self.assertEqual(payload["cleanup_status"], "cleaned")

    def test_cutover_control_is_available(self):
        """手动切换模式：就绪后页面必须给出「开始切换」入口。"""
        html = self._html()

        self.assertIn('name="cutover_mode"', html)
        self.assertIn("awaiting_cutover", html)
        self.assertIn("开始切换", html)
        self.assertIn("requestCutover", html)

    def test_step1_has_data_channel_select(self):
        self.assertIn('id="data-channel"', self._html())

    def test_step1_has_relay_pool_panel(self):
        html = self._html()

        self.assertIn('id="relay-pool-panel"', html)
        self.assertIn('name="relay_platform_url"', html)
        self.assertIn('id="relay-preflight"', html)
        self.assertIn('id="load-relay-catalog"', html)

    def test_step1_has_agent_install_and_data_port(self):
        html = self._html()

        self.assertIn('name="relay_agent_install"', html)
        self.assertIn('value="bootstrap"', html)
        self.assertIn('name="relay_data_port"', html)

    def test_step1_has_password_and_ssh_trust_fields(self):
        html = self._html()

        self.assertIn('name="relay_admin_password"', html)
        self.assertIn('name="relay_ssh_public_key"', html)

    def test_step1_has_tuning_fields(self):
        html = self._html()

        for name in (
            "relay_ready_timeout",
            "relay_heartbeat_interval",
            "relay_heartbeat_timeout",
            "relay_copy_retries",
            "relay_full_verify",
        ):
            self.assertIn(f'name="{name}"', html)

    def test_step1_has_hole_mode_selector(self):
        html = self._html()

        self.assertIn('name="relay_hole_mode"', html)
        self.assertIn('value="skip"', html)
        self.assertIn('value="zero"', html)

    def test_relay_pool_requires_system_volume_type(self):
        html = self._html()

        self.assertIn("relay_${prefix}_system_volume_type", html)
        self.assertIn("中转机系统盘类型", html)

    def test_relay_progress_shows_skipped_bytes(self):
        html = self._html()

        self.assertIn("skipped_bytes", html)
        self.assertIn("跳过", html)

    def test_step1_has_subnet_and_fixed_ip_fields(self):
        html = self._html()

        self.assertIn('name="relay_${prefix}_subnet"', html)
        self.assertIn('name="relay_${prefix}_ips"', html)
        self.assertIn("fillRelaySubnets", html)
        self.assertIn("state.relayCatalog", html)

    def test_step1_has_volume_type_selector(self):
        html = self._html()

        self.assertIn('name="relay_${prefix}_volume_type"', html)
        self.assertIn("使用 __DEFAULT__", html)

    def test_step3_has_rebuild_and_ledger_counters(self):
        html = self._html()

        self.assertIn("relay-rebuild", html)
        self.assertIn("在途卷", html)

    def test_step2_card_shows_target_volume_hint(self):
        self.assertIn("按源盘容量向上取整到 GiB", self._html())

    def test_job_list_has_cancel_and_delete(self):
        html = self._html()

        self.assertIn("async function cancelJob", html)
        self.assertIn("async function deleteJob", html)
        self.assertIn("/cancel`, { method: 'POST' }", html)
        self.assertIn("method: 'DELETE'", html)
        self.assertIn("cancelled: '已取消'", html)

    def test_relay_resource_page_markup_exists(self):
        html = self._html()

        for element_id in (
            "relay-resource-page",
            "relay-pool-summary",
            "relay-node-grid",
            "relay-tenant-credentials",
            "relay-scale-form",
            "relay-node-password-modal",
        ):
            self.assertIn(f'id="{element_id}"', html)

    def test_wizard_step1_references_existing_pool(self):
        html = self._html()

        self.assertIn('name="relay_node_mode"', html)
        self.assertIn("中转机资源", html)

    def test_wizard_step1_has_persistent_capacity_fields(self):
        html = self._html()

        for field in (
            "relay_slots_per_node",
            "relay_max_nodes",
            "relay_min_nodes",
            "relay_idle_scale_down_hours",
        ):
            self.assertIn(f'name="{field}"', html)

    def test_resource_page_is_marked_optional_ops_view(self):
        html = self._html()

        self.assertIn("日常迁移不需要来这一页", html)
        self.assertIn("中转机资源（运维）", html)
        self.assertIn("自动加密保存该租户凭据", html)

    def test_resource_page_has_pool_profile_fields(self):
        html = self._html()

        for field in (
            "pp_slots_per_node",
            "pp_max_nodes",
            "pp_min_nodes",
            "pp_idle_hours",
            "cred_auth_url",
        ):
            self.assertIn(f'name="{field}"', html)

    def test_step2_has_per_volume_type_selector(self):
        html = self._html()

        self.assertIn("volume-section", html)
        self.assertIn("volumeOverrides", html)
        self.assertIn("volume_overrides", html)
        self.assertIn("目标卷类型（逐卷）", html)
        self.assertIn("volume-source-type", html)
        self.assertIn("volume-target-type", html)
        self.assertIn("继承源卷类型", html)

    def test_nic_state_loop_skips_volume_rows(self):
        """网卡状态标记与取值都必须跳过卷行，否则 RBD 也会受索引错位影响。"""
        html = self._html()

        self.assertEqual(html.count("classList.contains('volume-row')"), 2)

    def test_platform_url_defaults_to_current_host(self):
        html = self._html()

        self.assertIn('name="relay_platform_url" value="http://localhost/"', html)

    def test_step3_has_relay_pool_view(self):
        html = self._html()

        self.assertIn('id="relay-pool-view"', html)
        self.assertIn('id="relay-pool-grid"', html)
        self.assertIn('id="relay-reconcile"', html)

    def test_step3_renders_relay_volume_progress(self):
        """中转机通道不看 vm.volumes，进度必须单独渲染，否则一直只显示"拷贝中"。"""
        html = self._html()

        self.assertIn('id="relay-volume-progress"', html)
        self.assertIn("relayVolumeProgress", html)
        self.assertIn("progress_percent", html)
        # 与 RBD 直连一致的读法：阶段标签 · 百分比 · 速率。
        self.assertIn("volume.progress_label", html)
        self.assertIn("throughput_mb_s", html)

    def test_relay_css_is_present(self):
        html = self._html()

        self.assertIn(".relay-pool-grid", html)
        self.assertIn('.relay-node[data-state="busy"]', html)

    def test_per_vm_channel_select_is_built_in_js(self):
        html = self._html()

        self.assertIn("channelSelect.dataset.role = 'data-channel'", html)
        self.assertIn("data.append('data_channel'", html)

    def test_relay_endpoints_are_registered(self):
        rules = {rule.rule for rule in app_module.app.url_map.iter_rules()}

        self.assertIn("/api/relay/catalog", rules)
        self.assertIn("/api/relay/preflight", rules)
        self.assertIn("/api/jobs/<job_id>/relay", rules)
        self.assertIn("/api/jobs/<job_id>/relay/reconcile", rules)

    def test_scheme_picker_exists_with_three_schemes(self):
        html = self._html()

        self.assertIn('id="scheme-picker"', html)
        for scheme in ("rbd_full", "rbd_incremental", "relay_full"):
            self.assertIn(f'data-scheme="{scheme}"', html)
        self.assertIn("const SCHEMES", html)
        self.assertIn("function applyScheme", html)

    def test_config_groups_are_marked_for_visibility(self):
        html = self._html()

        self.assertIn('data-cfg-group="ceph"', html)
        self.assertIn('data-cfg-group="delta"', html)
        self.assertIn('id="scheme-needs"', html)

    def test_profile_panel_exists(self):
        html = self._html()

        for element_id in (
            "profile-panel",
            "profile-select",
            "profile-save",
            "profile-delete",
        ):
            self.assertIn(f'id="{element_id}"', html)
        self.assertIn("async function loadProfiles", html)
        self.assertIn("async function saveProfile", html)
        self.assertIn("data.append('profile_id'", html)

    def test_checklist_has_summary_and_bulk_controls(self):
        html = self._html()

        self.assertIn("plan-card-body", html)
        self.assertIn("plan-summary", html)
        self.assertIn("function togglePlanCard", html)
        self.assertIn("function autoMapNetworks", html)
        self.assertIn("function applyBulkToSelected", html)
        self.assertIn("批量应用到已选 VM", html)
        self.assertIn("自动映射目标网络", html)

    def test_relay_resource_page_not_in_main_nav(self):
        html = self._html()

        self.assertNotIn('data-step="resources"', html)

    def test_relay_ops_entry_lives_in_relay_group(self):
        html = self._html()

        self.assertIn('id="relay-ops-open"', html)
        self.assertIn('id="relay-ops-close"', html)
        self.assertIn("function openRelayOps", html)
        self.assertIn("function closeRelayOps", html)

    def test_resource_page_has_env_summary_and_when_to_use_guide(self):
        """运维页要直接说清什么时候用，并把当前环境参数摆出来，避免不知从何下手。"""
        html = self._html()

        self.assertIn('id="relay-env-summary"', html)
        self.assertIn("relay-guide", html)
        self.assertIn("function renderRelayEnvSummary", html)
        self.assertIn("只有下面三种情况需要手工介入", html)
        self.assertIn("节点卡住或不可用", html)

    def test_resource_page_can_load_existing_pool_profile(self):
        """池建机参数必须能选已有池自动填充，并提供从向导带入与删除入口。"""
        html = self._html()

        self.assertIn('id="relay-profile-pick"', html)
        self.assertIn('id="relay-profile-prefill"', html)
        self.assertIn('id="relay-profile-delete"', html)
        self.assertIn("function applyPoolProfileToForm", html)
        self.assertIn("function prefillPoolProfileFromWizard", html)
        self.assertIn("function deleteRelayProfile", html)

    def test_resource_page_can_load_existing_tenant_credential(self):
        html = self._html()

        self.assertIn('id="relay-cred-pick"', html)
        self.assertIn('id="relay-cred-prefill-source"', html)
        self.assertIn('id="relay-cred-prefill-target"', html)
        self.assertIn("function prefillCredentialFromWizard", html)

    def test_resource_page_explains_stuck_provisioning_node(self):
        """装机中卡住的节点必须给出原因和下一步动作，否则用户不知道点哪里。"""
        html = self._html()

        self.assertIn("function relayNodeHint", html)
        self.assertIn("agent 尚未注册", html)
        self.assertIn("last_seen 为空", html)

    def test_pool_profile_marks_optional_fields(self):
        html = self._html()

        self.assertIn("field-optional", html)
        self.assertIn("可选，留空自动选", html)
        self.assertIn("可选，默认当前平台", html)

    def test_job_relay_endpoint_returns_null_without_runtime(self):
        response = self.client.get("/api/jobs/no-such-job/relay")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.get_json()["relay"])

    def test_relay_reconcile_without_runtime_returns_404(self):
        response = self.client.post("/api/jobs/no-such-job/relay/reconcile")

        self.assertEqual(response.status_code, 404)

    def test_theme_defaults_to_light_with_dark_override(self):
        """默认浅色主题，深色仅作为覆盖层，两套调色板都要存在。"""
        html = self._html()

        self.assertIn("--bg: #f4f6f9;", html)
        self.assertIn('[data-theme="dark"]', html)
        self.assertIn("--bg: #0b1220;", html)
        self.assertIn("color-scheme: light;", html)
        self.assertIn("color-scheme: dark;", html)

    def test_theme_toggle_and_early_bootstrap_script(self):
        """切换按钮存在，且在 <head> 内提前设置主题，避免刷新闪白/闪黑。"""
        html = self._html()

        self.assertIn('id="theme-toggle"', html)
        self.assertIn("function initThemeToggle", html)
        self.assertIn("function applyTheme", html)
        self.assertIn("localStorage.getItem('migrate-theme')", html)
        head = html.split("</head>", 1)[0]
        self.assertIn("migrate-theme", head)

    def test_no_dark_only_hardcoded_palette_outside_theme_blocks(self):
        """主题块之外不应再出现深色专用字面色，否则浅色主题会残留暗块。"""
        html = self._html()
        style = html.split("<style>", 1)[1].split("</style>", 1)[0]
        after_dark = style.split('[data-theme="dark"]', 1)[1]
        marker = "\n        }\n"
        rules_only = after_dark[after_dark.index(marker) + len(marker):]

        for literal in ("#0b1220", "#101a2e", "#0c1524", "#0b1424", "#5e7190",
                        "#062033", "#fde68a", "#fecaca", "#a7f3d0", "#5f7390"):
            self.assertNotIn(literal, rules_only)

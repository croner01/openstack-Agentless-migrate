import unittest
from unittest import mock

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
        self.assertIn("openCutoverConfirm", html)

    def test_step1_has_data_channel_select(self):
        self.assertIn('id="data-channel"', self._html())

    def test_wizard_has_relay_section_with_platform_fields(self):
        """选中「中转机」方案后，回连地址/目录/预检这些必填项必须能填。"""
        html = self._html()

        self.assertIn('id="wiz-relay-sec"', html)
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

    def test_relay_detail_has_ledger_counters(self):
        """中转机页签要摆出台账计数，否则不知道有没有残留卷。"""
        html = self._html()

        self.assertIn('id="relay-counters"', html)
        self.assertIn("在途卷", html)
        self.assertIn("已清理", html)

    def test_step2_card_shows_target_volume_hint(self):
        """目标卷类型留空即用目标默认；逐卷可覆盖，不能让用户以为必须手填。"""
        html = self._html()

        self.assertIn('name="target_volume_type"', html)
        self.assertIn("留空使用目标默认", html)
        self.assertIn("目标卷类型（逐卷，可选）", html)
        self.assertIn("继承源卷类型", html)

    def test_job_list_has_cancel_and_delete(self):
        html = self._html()

        self.assertIn("function cancelJob", html)
        self.assertIn("function openJobDeleteConfirm", html)
        self.assertIn("'/cancel', { method: 'POST' }", html)
        self.assertIn("method: 'DELETE'", html)
        self.assertIn("cancelled: '已取消'", html)

    # 说明：原先的「中转机运维页」是被有意移除的复杂页面（需求改为只保留
    # 清理/扩容的极简资源页），针对它的表单/凭据预填/环境摘要/节点提示等断言
    # 已随页面一并删除；相关后端能力仍由 tests/test_relay_admin_api.py 覆盖。

    def test_relay_resource_page_markup_exists(self):
        html = self._html()

        for element_id in ("view-relay", "relay-res-body", "relay-res-refresh"):
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

    def test_step2_has_per_volume_type_selector(self):
        html = self._html()

        self.assertIn("_volumeOverrides", html)
        self.assertIn("volume_overrides", html)
        self.assertIn("目标卷类型（逐卷，可选）", html)
        self.assertIn("volume-source-type", html)
        self.assertIn("volume-target-type", html)
        self.assertIn("继承源卷类型", html)

    def test_nic_state_loop_skips_volume_rows(self):
        """网卡状态标记与取值都必须跳过卷行，否则 RBD 也会受索引错位影响。"""
        html = self._html()

        self.assertEqual(html.count("classList.contains('volume-row')"), 2)

    def test_platform_url_defaults_to_current_host(self):
        """回连平台地址默认填当前访问地址，省得用户手敲。"""
        html = self._html()

        self.assertIn("$('#relay-platform-url').value = location.origin + '/'", html)

    def test_job_detail_has_relay_tab(self):
        """作业详情的「中转机」页签要能对账并展示进度与台账。"""
        html = self._html()

        self.assertIn('id="jobtab-relay"', html)
        self.assertIn('id="relay-reconcile"', html)
        self.assertIn('id="relay-counters"', html)

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

        self.assertIn(".res-pool {", html)
        self.assertIn(".res-head {", html)
        self.assertIn(".res-node {", html)

    def test_per_vm_channel_select_is_built_in_js(self):
        """逐 VM 通道覆盖是方案默认之外的手动出口，必须真实提交。"""
        html = self._html()

        self.assertIn("channelSel.dataset.field = 'data_channel'", html)
        self.assertIn("new Option('跟随方案', '')", html)
        self.assertIn("data.append('data_channel'", html)

    def test_relay_endpoints_are_registered(self):
        rules = {rule.rule for rule in app_module.app.url_map.iter_rules()}

        self.assertIn("/api/relay/catalog", rules)
        self.assertIn("/api/relay/preflight", rules)
        self.assertIn("/api/jobs/<job_id>/relay", rules)
        self.assertIn("/api/jobs/<job_id>/relay/reconcile", rules)

    def test_scheme_picker_exists_with_three_schemes(self):
        html = self._html()

        self.assertIn('class="scheme-cards"', html)
        for scheme in ("rbd_full", "rbd_incremental", "relay_full"):
            self.assertIn(f'data-scheme="{scheme}"', html)
        self.assertIn("const SCHEMES", html)
        self.assertIn("function applyScheme", html)

    def test_config_groups_are_marked_for_visibility(self):
        """方案切换靠 data-cfg-group 显隐，漏标记就会把无关配置摆出来。"""
        html = self._html()

        self.assertIn('data-cfg-group="ceph"', html)
        self.assertIn('data-cfg-group="delta"', html)
        self.assertIn('data-cfg-group="relay"', html)

    def test_profile_panel_exists(self):
        html = self._html()

        self.assertIn("function loadProfiles", html)
        for element_id in (
            "prof-drawer",
            "profile-select",
            "prof-new",
            "prof-save",
        ):
            self.assertIn(f'id="{element_id}"', html)

    def test_checklist_has_summary_and_bulk_controls(self):
        html = self._html()

        self.assertIn("plan-body", html)
        self.assertIn("checklist-summary", html)
        self.assertIn("plan-expand", html)
        self.assertIn("function autoMapNetworks", html)
        self.assertIn("function applyBulkToSelected", html)
        self.assertIn("自动映射目标网络", html)

    def test_relay_resource_page_not_in_main_nav(self):
        html = self._html()

        self.assertNotIn('data-step="resources"', html)

    def test_relay_ops_entry_lives_in_relay_group(self):
        """运维入口收在侧边栏的「中转机资源」，不再挂在向导步骤里。"""
        html = self._html()

        self.assertIn('data-nav="relay"', html)
        self.assertIn('<h1 class="page-title">中转机资源</h1>', html)

    def test_job_relay_endpoint_returns_null_without_runtime(self):
        response = self.client.get("/api/jobs/no-such-job/relay")

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.get_json()["relay"])

    def test_relay_reconcile_without_runtime_returns_404(self):
        response = self.client.post("/api/jobs/no-such-job/relay/reconcile")

        self.assertEqual(response.status_code, 404)

    def test_relay_reconcile_refuses_while_job_is_running(self):
        """运行中的作业不能手工对账：会删掉正在拷贝的派生卷/目标卷。

        删掉之后轮到挂载时 Nova 直接 404 Volume ... could not be found，
        看起来就像"云上凭空少了一块盘"。
        """
        runtime = mock.MagicMock()
        running = mock.MagicMock(status=app_module.JobStatus.RUNNING)
        with mock.patch.object(app_module, "get_runtime", return_value=runtime), \
                mock.patch.object(app_module.job_manager, "get", return_value=running):
            response = app_module.app.test_client().post(
                "/api/jobs/job-1/relay/reconcile"
            )

        self.assertEqual(response.status_code, 409)
        self.assertIn("仍在运行", response.get_json()["error"])
        runtime.reaper.reconcile_job.assert_not_called()

    def test_relay_reconcile_runs_after_job_finished(self):
        runtime = mock.MagicMock()
        runtime.reaper.reconcile_job.return_value = ["job-1:vol-s1"]
        finished = mock.MagicMock(status=app_module.JobStatus.COMPLETED)
        with mock.patch.object(app_module, "get_runtime", return_value=runtime), \
                mock.patch.object(app_module.job_manager, "get", return_value=finished):
            response = app_module.app.test_client().post(
                "/api/jobs/job-1/relay/reconcile"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["cleaned"], ["job-1:vol-s1"])

    def test_reconcile_button_is_disabled_while_job_runs(self):
        """页面上也要拦住：运行中禁用按钮并写明原因。"""
        html = self._html()

        self.assertIn("reconcileBtn.disabled = running", html)
        self.assertIn("对账会删掉正在拷贝的卷", html)

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
        # 深色主题块自身以行首的 "}" 收尾；从它的闭合大括号之后开始检查，
        # 这样新增缩进或变量都不会再让这段解析失效。
        rules_only = after_dark[after_dark.index("\n}") + 2:]
        # 设置页的主题卡片预览色块本来就该显示深色底，是唯一允许的字面色。
        rules_only = "\n".join(
            line for line in rules_only.splitlines() if ".theme-preview" not in line
        )

        for literal in ("#0b1220", "#101a2e", "#0c1524", "#0b1424", "#5e7190",
                        "#062033", "#fde68a", "#fecaca", "#a7f3d0", "#5f7390"):
            self.assertNotIn(literal, rules_only)

    def test_plan_grid_has_per_vm_boot_toggle(self):
        """规划表新增「迁移后开机」列：表头、复选框字段、empty-box 列数都要对上。"""
        html = self._html()

        self.assertIn("<th>迁移后开机</th>", html)
        self.assertIn("powerCk.dataset.field = 'start_target'", html)
        self.assertIn("row.start_target !== false", html)
        self.assertNotIn(
            '<td colspan="8" class="empty-box">请先在上一步勾选源 VM 或解析 Excel 清单</td>',
            html,
        )

    def test_plan_grid_has_bulk_boot_control(self):
        """批量工具条支持一次性设置「迁移后开机 / 不开机」。"""
        html = self._html()

        self.assertIn('id="bulk-power"', html)
        self.assertIn("power === 'on'", html)

    def test_submit_payload_and_detail_view_carry_start_target(self):
        """提交参数、作业详情 chip / 抽屉概览都要带上开关状态。"""
        html = self._html()

        self.assertIn("start_target: row.start_target !== false", html)
        # 批量应用直接改 state 后必须跳过 DOM 回写，否则会被旧 DOM 覆盖
        self.assertIn("renderPlan({ keepState: true })", html)
        self.assertIn("options && options.keepState", html)
        self.assertIn("['迁移后开机', vm.start_target === false ? '否（目标机保持关机）' : '是']", html)
        self.assertIn("const keepOff = vm.start_target === false;", html)

    def test_relay_progress_keeps_byte_less_phases_visible(self):
        """打快照/派生卷阶段字节为 0，按字节过滤会让页面显示「暂无在途卷」。"""
        html = self._html()

        self.assertIn("function relayWaitText", html)
        self.assertIn("RELAY_WAIT_PHASES", html)
        self.assertIn("state.relayNow = json.relay.now", html)
        self.assertIn("finished.indexOf(volume.phase || '') < 0", html)

    def test_resubmit_button_is_not_gated_by_browser_session(self):
        """刷新/换浏览器后「调整参数重新提交」不能消失。

        旧实现按 `state.lastSubmit.jobId === job.id` 决定显隐，快照只存在浏览器
        内存里，用户过一会儿再看作业就只剩「取消任务」了。
        """
        html = self._html()

        self.assertNotIn("state.lastSubmit.jobId === job.id) ? '' : 'none'", html)
        self.assertIn("async function fetchSubmitSnapshot", html)
        self.assertIn("'/api/jobs/' + encodeURIComponent(jobId) + '/params'", html)

    def test_submit_form_carries_parameters_snapshot(self):
        """提交时把向导快照一并上报，服务器才有参数可回放。"""
        html = self._html()

        self.assertIn("data.append('submit_snapshot'", html)
        self.assertIn("captureSubmitSnapshot(null)", html)

    def test_submit_form_carries_all_relay_timeouts(self):
        """提交表单必须带上此前只在预检里传的超时，否则填了也不生效。"""
        html = self._html()

        self.assertIn('name="volume_ready_timeout"', html)
        self.assertIn('name="relay_slot_wait_seconds"', html)
        for field in ("relay_stall_timeout", "relay_slot_wait_seconds", "volume_ready_timeout"):
            self.assertIn("'" + field + "'", html)

    def test_derive_timeout_field_defaults_to_unlimited(self):
        """「卷/快照就绪超时」默认 0 = 不限制等待，页面要写清楚。"""
        html = self._html()

        self.assertIn('0 = 不限制（默认）', html)

    def test_volume_concurrency_hint_covers_relay_prepare(self):
        """「单台卷拷贝并发」同样作用于中转机的快照/派生并发，页面要说明。"""
        html = self._html()

        self.assertIn("几块盘同时打快照/派生", html)

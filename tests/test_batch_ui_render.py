"""批量迁移 UI：清单筛选、策略模板/规则映射、计划预览、排队与报表入口。"""
import unittest

import app as app_module


class SourceVmFilterRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_toolbar_exposes_full_fetch_and_filters(self):
        html = self._html()

        self.assertIn('id="load-all-source-vms"', html)
        for selector in ('vm-filter-az', 'vm-filter-status', 'vm-filter-flavor',
                         'vm-filter-image', 'vm-filter-disks', 'vm-filter-wildcard',
                         'vm-filter-hide-busy'):
            self.assertIn(f'id="{selector}"', html)
        self.assertIn('id="vm-sel-visible"', html)
        self.assertIn('id="vm-sel-clear"', html)

    def test_filters_are_applied_and_wildcards_are_literal_safe(self):
        html = self._html()

        self.assertIn("function wildcardMatch(text, pattern)", html)
        self.assertIn("function refreshVmFilterOptions()", html)
        self.assertIn("function readVmFilters()", html)
        self.assertIn("state.vmFilters", html)
        # 正则元字符要先转义，否则用户在名称里输入 "." 会被当通配处理。
        self.assertIn(".replace(/[.+^${}()|[\\]\\\\]/g, '\\\\$&')", html)

    def test_rows_render_progressively_with_disk_facts(self):
        html = self._html()

        self.assertIn("state.vmRowLimit", html)
        self.assertIn("const visible = shown.slice(0, Math.max(1, state.vmRowLimit));", html)
        self.assertIn("server.volume_count + ' 块/' + server.volume_gb + 'GiB'", html)
        self.assertIn("迁移中", html)
        # 表头多了一列「盘」，空态 colspan 必须跟着改，否则表格错位。
        self.assertIn("<th>盘</th>", html)
        self.assertIn('<tr><td colspan="8" class="empty-box">先在上方加载源 VM', html)

    def test_bulk_select_does_not_await_per_vm(self):
        html = self._html()

        self.assertIn("function addSelectedSourceServerSync(server)", html)
        self.assertIn("targets.forEach(server => {", html)
        self.assertNotIn("for (const server of targets) {\n        await setServerSelected", html)
        self.assertIn("function clearSourceSelection()", html)

    def test_excel_errors_and_template_download_hooks(self):
        html = self._html()

        self.assertIn('id="excel-template-download"', html)
        self.assertIn('id="excel-errors"', html)
        self.assertIn("function reportExcelErrors(errors)", html)


class MigrationPolicyRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_policy_card_and_rule_editor_exist(self):
        html = self._html()

        self.assertIn('id="policy-select"', html)
        self.assertIn('id="policy-apply"', html)
        self.assertIn('id="policy-save"', html)
        self.assertIn('id="policy-delete"', html)
        self.assertIn('id="policy-rules-body"', html)
        self.assertIn('id="policy-rule-add"', html)
        self.assertIn('id="policy-rule-preview"', html)
        self.assertIn('id="policy-rule-apply"', html)

    def test_rule_matching_supports_dry_run_and_apply(self):
        html = self._html()

        self.assertIn("function ruleMatches(rule, server)", html)
        self.assertIn("function applyRulesToPlan(dryRun)", html)
        self.assertIn("function updateRuleHits()", html)
        self.assertIn("规则预览：命中", html)

    def test_plan_preview_shows_pool_footprint(self):
        html = self._html()

        self.assertIn('id="plan-preview"', html)
        self.assertIn('id="relay-pool-hint"', html)
        self.assertIn("function renderPlanPreview()", html)
        self.assertIn("峰值并发盘数 ≈", html)
        self.assertIn("按需扩到并发上限", html)


class RelayQueueRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_slot_wait_table_and_retry_all_button(self):
        html = self._html()

        self.assertIn('id="relay-slot-waits"', html)
        self.assertIn('id="relay-retry-all"', html)
        self.assertIn("function relaySlotWaits(relay)", html)
        self.assertIn("function retryAllFailedDisks()", html)
        self.assertIn("formatDuration(item.waited || 0)", html)

    def test_job_progress_report_card(self):
        html = self._html()

        self.assertIn('id="relay-report"', html)
        self.assertIn("function jobProgressReport(relay, diskRows, vms)", html)
        self.assertIn("完成度 ", html)

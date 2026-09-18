import unittest

import app as app_module


class VmDiskSummaryRenderTest(unittest.TestCase):
    """RBD 全量/增量与中转机三条通道都要能按 VM 看到"几块盘完成、几块失败"。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_volume_tab_has_per_vm_disk_summary(self):
        html = self._html()

        self.assertIn('id="vols-vm-disks"', html)
        self.assertIn("renderVmDiskSummary($('#vols-vm-disks'), vmDiskSummaryItems(vms))", html)

    def test_relay_tab_and_vm_list_reuse_the_same_summary(self):
        html = self._html()

        # 一套汇总渲染，三处入口：中转机页签、卷拷贝页签、VM 列表的「云盘」列。
        self.assertEqual(html.count("function renderVmDiskSummary"), 1)
        self.assertIn("renderVmDiskSummary($('#relay-vm-disks'), items, { retry: true })", html)
        self.assertIn("function vmDiskCell", html)

    def test_relay_summary_only_offers_retry_while_waiting(self):
        """重试按钮只挂在"等待重试"的 VM 上，其它状态给提示，避免和传输抢盘。"""
        html = self._html()

        self.assertIn("function diskRetryCell(vm, disks)", html)
        self.assertIn("const retryable = vm.status === 'awaiting_disk_retry';", html)
        self.assertIn("if (retryable) {", html)
        self.assertIn("等待其余盘完成", html)
        self.assertIn("function retryVmDisk(vmName, volumeIds)", html)
        self.assertIn("/disks/retry", html)
        self.assertIn("function relayDiskFailed(disk)", html)

    def test_relay_summary_offers_release_for_retained_copy(self):
        """失败保留的中间卷要能在页面上立即释放，不必为了归还配额取消任务。"""
        html = self._html()

        self.assertIn("function releaseVmDisk(vmName, volumeIds)", html)
        self.assertIn("/disks/release", html)
        self.assertIn("function relayRetainText(disk)", html)
        self.assertIn("全部释放", html)

    def test_disk_source_prefers_relay_results_then_volumes(self):
        html = self._html()

        self.assertIn("function vmDisksOf(vm)", html)
        self.assertIn(
            "return (vm.relay_disks && vm.relay_disks.length) ? vm.relay_disks : (vm.volumes || []);",
            html,
        )

    def test_incremental_ready_counts_as_in_flight(self):
        """增量的「已就绪」还没换入目标卷，算进行中而不是完成。"""
        html = self._html()

        self.assertIn("return disk.status === 'success'", html)
        self.assertIn("disk.phase === 'done' || disk.phase === 'cleaned'", html)
        self.assertIn("未落终态的盘：排队中 / 拷贝中 / 增量已就绪待切换", html)


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


class RelayDiskDetailRenderTest(unittest.TestCase):
    """中转机页签要有一行一块盘的明细表：失败后仍能看到每块盘的状态与原因。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_detail_table_has_expected_columns(self):
        html = self._html()

        self.assertIn('id="relay-disk-detail"', html)
        self.assertIn("<th>VM</th><th>盘</th><th>源卷</th><th>目标卷</th><th>状态</th>", html)
        self.assertIn("失败原因", html)
        self.assertIn('<tbody id="relay-disk-detail"><tr><td colspan="8"', html)

    def test_detail_table_is_refreshed_with_the_relay_tab(self):
        html = self._html()

        self.assertIn("function renderRelayDiskDetail(vms, relay)", html)
        self.assertIn("renderRelayDiskDetail(vms, relay);", html)
        # 在途实时进度与作业留存的逐盘结果按源卷 id 合并。
        self.assertIn("function relayDiskDetailRows(vms, relay)", html)
        self.assertIn("const now = live[row.volume_id];", html)

    def test_detail_table_shows_error_and_bytes(self):
        html = self._html()

        self.assertIn("function relayDiskErrorCell(row)", html)
        self.assertIn("td.title = row.error;", html)
        # 长异常要单行截断，完整内容在 title 里。
        self.assertIn("td.appendChild(text('span', 'disk-err', row.error));", html)
        self.assertIn(".disk-err {", html)
        self.assertIn("function relayDiskDetailBytes(row)", html)

    def test_detail_table_offers_per_disk_actions(self):
        html = self._html()

        self.assertIn("function relayDiskDetailActionCell(vm, row)", html)
        self.assertIn("const retry = diskRetryButton(vm, row);", html)
        self.assertIn("const release = diskReleaseButton(vm, row);", html)
        # 按钮文案带盘角色，单看「重试」分不清是哪块盘。
        self.assertIn("释放数据盘中间卷", html)
        self.assertIn("重试系统盘", html)
        # 明细表里的 role 已是中文，按钮文案必须能认，否则会退化成「重试」。
        self.assertIn(
            "const key = role === '系统盘' ? 'boot' : (role === '数据盘' ? 'data' : role);",
            html,
        )

    def test_detail_role_label_accepts_persisted_and_raw_role(self):
        """relay_disks 里的 role 已转成中文，台账里可能是 boot/data，两种都要认。"""
        html = self._html()

        self.assertIn("if (role === 'boot') return '系统盘';", html)
        self.assertIn("if (role === 'data') return '数据盘';", html)


if __name__ == "__main__":
    unittest.main()

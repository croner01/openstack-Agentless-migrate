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
        # 卷拷贝页签只汇总 RBD 直连通道（vm.volumes）：中转机的盘由「中转机
        # 通道」页签负责，混进来会出现"汇总有数、下面的卷明细却是空的"。
        self.assertIn("renderVmDiskSummary($('#vols-vm-disks'), rbdItems)", html)
        self.assertIn(".filter(item => item.disks.length);", html)

    def test_volume_tab_points_relay_jobs_at_the_relay_tab(self):
        """整机走中转机时 RBD 两张表全空，必须给出去哪看盘的可操作提示。"""
        html = self._html()

        self.assertIn('id="vols-rbd-note"', html)
        self.assertIn('id="vols-rbd-body"', html)
        self.assertIn("本作业的盘走「中转机通道」", html)
        self.assertIn("逐盘明细与在途进度见「中转机通道」页签", html)
        self.assertIn("rbdBody.classList.toggle('hidden', !rbdItems.length && relayVmCount > 0);", html)

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


class VmProgressColumnRenderTest(unittest.TestCase):
    """中转机作业的 VM 列表进度列与 VM 抽屉都要读 relay_disks；只读 vm.volumes
    会让整列恒为 —，逼着运维每次切页签找盘。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_vm_progress_falls_back_to_relay_disks(self):
        html = self._html()

        self.assertIn("function vmRelayDiskRows(vm)", html)
        self.assertIn(
            "return relayDiskDetailRows([vm], state.relayPayload || null)", html
        )
        self.assertIn("function vmPrimaryDiskRow(vm)", html)
        self.assertIn("function vmPrimaryProgressHTML(vm)", html)
        self.assertIn("const progressHtml = vmPrimaryProgressHTML(vm);", html)
        # 快照/派生/排队没有字节流动，用阶段文案兜底，不能显示成空白。
        self.assertIn("disk.progress_label", html)

    def test_vm_drawer_lists_relay_disks(self):
        html = self._html()

        self.assertIn("中转机通道（逐盘）", html)
        self.assertIn("const relayRows = vmRelayDiskRows(vm);", html)
        self.assertIn("status.appendChild(relayPhaseBadge(row));", html)

    def test_ledger_fallback_is_filtered_by_vm_name(self):
        """老作业没有 relay_disks 时会退回整份台账，必须按 VM 名过滤，
        否则会把别的 VM 的盘画到这台 VM 的进度列/抽屉里。"""
        html = self._html()

        self.assertIn(".filter(row => !row.vm_id || row.vm_id === vm.name);", html)

    def test_relay_snapshot_is_scoped_to_the_current_job(self):
        html = self._html()

        self.assertIn("state.relayPayload = null;", html)
        self.assertIn(
            "if (!state.currentJob || state.currentJob.id !== job.id) state.relayPayload = null;",
            html,
        )
        # 轮询先 renderJob 再取 /relay，取到后要用实时台账补刷 VM 列表进度列。
        self.assertIn("state.relayPayload = relay;", html)
        self.assertIn("if (state.currentJob) renderVmTable(vms);", html)

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
        self.assertIn("if (vm.status !== 'awaiting_disk_retry') {", html)
        self.assertIn("等待其余盘完成", html)
        self.assertIn("function retryVmDisk(vmName, volumeIds)", html)
        self.assertIn("/disks/retry", html)
        self.assertIn("function relayDiskFailed(disk)", html)

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


if __name__ == "__main__":
    unittest.main()

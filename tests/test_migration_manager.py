import unittest
from unittest import mock

from state_machine import (
    MigrationMode,
    VolumeStatus,
    VolumeTask,
    VmStatus,
    VmTask,
)
from ceph_utils import StageImageMissingError
from migration_manager import (
    MAX_BASELINE_RESEEDS,
    FullVolumeMover,
    IncrementalVolumeMover,
    MigrationManager,
)


class StopSignalTest(unittest.TestCase):
    def test_copy_volume_respects_stop_before_starting(self):
        can_proceed = mock.Mock(return_value=False)
        manager = MigrationManager(
            source_os=None,
            target_os=None,
            ceph_utils=None,
            can_proceed=can_proceed,
        )
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
            ),
            VolumeTask(
                source_volume_id="s2",
                source_rbd_name="volume-s2",
                target_volume_id="t2",
                target_rbd_name="volume-t2",
            ),
        ]
        with self.assertRaises(RuntimeError):
            manager._copy_volumes(vm, max_workers=1)
        self.assertEqual(vm.volumes[0].status, VolumeStatus.FAILED)
        self.assertIn("停机信号中断", vm.volumes[0].error)


class CopyProgressTest(unittest.TestCase):
    def test_progress_callback_updates_volume_and_passes_rate_limit(self):
        received = {}

        def fake_replace(
            source_rbd_name,
            target_rbd_name,
            progress_cb=None,
            rate_limit_bytes_per_sec=None,
            on_phase=None,
        ):
            received["rate_limit_bytes_per_sec"] = rate_limit_bytes_per_sec
            progress_cb(
                512 * 1024 * 1024,
                1024 * 1024 * 1024,
                20 * 1024 * 1024,
            )
            return False

        ceph_utils = mock.Mock()
        ceph_utils.replace_rbd_data.side_effect = fake_replace
        manager = MigrationManager(None, None, ceph_utils=ceph_utils)
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
            )
        ]

        manager._copy_volumes(vm, max_workers=1, rate_limit_mb_s=10)

        self.assertEqual(
            received["rate_limit_bytes_per_sec"], 10 * 1024 * 1024
        )
        self.assertEqual(vm.volumes[0].status, VolumeStatus.FAILED)
        self.assertEqual(vm.volumes[0].progress_percent, 50.0)
        self.assertEqual(vm.volumes[0].throughput_mb_s, 20.0)

    def test_success_marks_progress_complete(self):
        ceph_utils = mock.Mock()
        ceph_utils.replace_rbd_data.return_value = True
        manager = MigrationManager(None, None, ceph_utils=ceph_utils)
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
            )
        ]

        manager._copy_volumes(vm, max_workers=1, rate_limit_mb_s=0)

        self.assertEqual(vm.volumes[0].status, VolumeStatus.SUCCESS)
        self.assertEqual(vm.volumes[0].progress_percent, 100.0)
        self.assertIsNone(vm.volumes[0].throughput_mb_s)

    def test_copy_retries_once_then_succeeds(self):
        calls = []

        def flaky_replace(*args, **kwargs):
            calls.append(1)
            return len(calls) == 2

        ceph_utils = mock.Mock()
        ceph_utils.replace_rbd_data.side_effect = flaky_replace
        manager = MigrationManager(
            None,
            None,
            ceph_utils=ceph_utils,
            copy_gate=mock.Mock(),
        )
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
            )
        ]

        manager._copy_volumes(vm, max_workers=1, rate_limit_mb_s=0)

        self.assertEqual(len(calls), 2)
        self.assertEqual(vm.volumes[0].status, VolumeStatus.SUCCESS)

    def test_copy_retries_at_most_once(self):
        ceph_utils = mock.Mock()
        ceph_utils.replace_rbd_data.return_value = False
        manager = MigrationManager(
            None,
            None,
            ceph_utils=ceph_utils,
            copy_gate=mock.Mock(),
        )
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
            )
        ]

        manager._copy_volumes(vm, max_workers=1, rate_limit_mb_s=0)

        self.assertEqual(ceph_utils.replace_rbd_data.call_count, 2)
        self.assertEqual(vm.volumes[0].status, VolumeStatus.FAILED)


class SourceServerLookupTest(unittest.TestCase):
    def test_provided_server_id_skips_name_lookup(self):
        source_os = mock.Mock()
        server = mock.Mock()
        server.id = "srv-abc"
        source_os.get_server_detail.return_value = server
        manager = MigrationManager(source_os, None, ceph_utils=None)
        vm = VmTask(name="web-1", target_az="az1")
        vm.source_server_id = "srv-abc"

        found = manager._resolve_source_server(vm)

        self.assertEqual(found.id, "srv-abc")
        source_os.get_server_by_name.assert_not_called()
        source_os.get_server_detail.assert_called_once_with("srv-abc")


class NetworkOverrideTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.Mock()
        self.target_os = mock.Mock()
        manager = MigrationManager(
            self.source_os,
            self.target_os,
            ceph_utils=None,
        )
        manager._vm_network_overrides = {
            "vm-a": [
                {
                    "source_network": "net-src",
                    "source_ip": "10.0.0.5",
                    "target_network": "net-dst-id",
                    "target_subnet": "subnet-dst-id",
                    "target_ip": "10.20.0.99",
                }
            ]
        }
        self.manager = manager

    def test_vm_network_override_is_used_directly(self):
        src_addr = {"net-src": [{"addr": "10.0.0.5", "version": 4}]}
        self.source_os.get_server_addresses.return_value = src_addr
        net = mock.Mock()
        net.id = "net-dst-id"
        subnet = mock.Mock()
        subnet.id = "subnet-dst-id"
        subnet.cidr = "10.20.0.0/24"
        self.target_os.find_network.return_value = net
        self.target_os.find_subnet.return_value = subnet
        port = mock.Mock()
        port.id = "port-1"
        self.target_os.create_port_with_fixed_ip.return_value = port

        vm = VmTask(name="vm-a", target_az="az1")
        port_ids = self.manager._create_target_ports(vm)
        self.assertEqual(port_ids, ["port-1"])
        self.assertEqual(vm.target_ips, ["10.20.0.99"])
        _, kwargs = self.target_os.create_port_with_fixed_ip.call_args
        self.assertEqual(kwargs["network_id"], "net-dst-id")
        self.assertEqual(kwargs["subnet_id"], "subnet-dst-id")
        self.assertEqual(kwargs["fixed_ip"], "10.20.0.99")

    def test_network_override_matches_by_server_id(self):
        src_addr = {"net-src": [{"addr": "10.0.0.5", "version": 4}]}
        self.source_os.get_server_addresses.return_value = src_addr
        net = mock.Mock()
        net.id = "net-dst-id"
        subnet = mock.Mock()
        subnet.id = "subnet-dst-id"
        subnet.cidr = "10.20.0.0/24"
        self.target_os.find_network.return_value = net
        self.target_os.find_subnet.return_value = subnet
        port = mock.Mock()
        port.id = "port-1"
        self.target_os.create_port_with_fixed_ip.return_value = port
        self.manager._vm_network_overrides = {
            "srv-abc": [
                {
                    "source_network": "net-src",
                    "source_ip": "10.0.0.5",
                    "target_network": "net-dst-id",
                    "target_subnet": "subnet-dst-id",
                }
            ]
        }

        vm = VmTask(name="web-1", target_az="az1")
        vm.source_server_id = "srv-abc"
        port_ids = self.manager._create_target_ports(vm)
        self.assertEqual(port_ids, ["port-1"])

    def test_each_fixed_ip_on_same_network_gets_its_own_port(self):
        src_addr = {
            "net-src": [
                {"addr": "10.0.0.5", "version": 4},
                {"addr": "10.0.0.6", "version": 4},
            ]
        }
        self.source_os.get_server_addresses.return_value = src_addr
        net = mock.Mock()
        net.id = "net-dst-id"
        subnet = mock.Mock()
        subnet.id = "subnet-dst-id"
        subnet.cidr = "10.20.0.0/24"
        self.target_os.find_network.return_value = net
        self.target_os.find_subnet.return_value = subnet
        self.target_os.create_port_with_fixed_ip.side_effect = [
            mock.Mock(id="port-5"),
            mock.Mock(id="port-6"),
        ]
        self.manager._vm_network_overrides = {
            "vm-a": [
                {
                    "source_network": "net-src",
                    "source_ip": "10.0.0.5",
                    "target_network": "net-dst-id",
                    "target_subnet": "subnet-dst-id",
                },
                {
                    "source_network": "net-src",
                    "source_ip": "10.0.0.6",
                    "target_network": "net-dst-id",
                    "target_subnet": "subnet-dst-id",
                },
            ]
        }
        vm = VmTask(name="vm-a", target_az="az1")
        port_ids = self.manager._create_target_ports(vm)
        self.assertEqual(port_ids, ["port-5", "port-6"])
        self.assertEqual(vm.source_ips, ["10.0.0.5", "10.0.0.6"])

    def test_missing_network_override_raises_without_global_fallback(self):
        src_addr = {"net-src": [{"addr": "10.0.0.5", "version": 4}]}
        self.source_os.get_server_addresses.return_value = src_addr

        vm = VmTask(name="vm-a", target_az="az1")
        self.manager._vm_network_overrides = {}
        with self.assertRaisesRegex(RuntimeError, "未配置目标网络/子网"):
            self.manager._create_target_ports(vm)


class CopyModeDispatchTest(unittest.TestCase):
    def _manager(self):
        ceph = mock.Mock()
        ceph.replace_rbd_data.return_value = True
        ceph.finalize_incremental_volume.return_value = True
        ceph.prune_source_snapshots.return_value = []
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        return manager, ceph

    @staticmethod
    def _volume(mode):
        return VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            source_pool="volumes",
            target_pool="volumes",
            mode=mode,
        )

    @staticmethod
    def _vm(volume):
        vm = VmTask(name="vm1", target_az="az1", mode=volume.mode)
        vm.volumes = [volume]
        return vm

    def test_full_mode_uses_full_replace_only(self):
        manager, ceph = self._manager()
        volume = self._volume(MigrationMode.FULL)
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.replace_rbd_data.assert_called_once()
        ceph.finalize_incremental_volume.assert_not_called()
        self.assertEqual(volume.status, VolumeStatus.SUCCESS)

    def test_incremental_mode_uses_finalize_only(self):
        manager, ceph = self._manager()
        volume = self._volume(MigrationMode.INCREMENTAL)
        manager._sessions[volume.source_volume_id] = mock.Mock()
        manager._job_id = "j1"
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.replace_rbd_data.assert_not_called()
        ceph.finalize_incremental_volume.assert_called_once()
        self.assertEqual(volume.status, VolumeStatus.SUCCESS)

    def test_incremental_failure_never_switches_to_full(self):
        manager, ceph = self._manager()
        ceph.finalize_incremental_volume.return_value = False
        volume = self._volume(MigrationMode.INCREMENTAL)
        manager._sessions[volume.source_volume_id] = mock.Mock()
        manager._job_id = "j1"
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.replace_rbd_data.assert_not_called()
        self.assertEqual(volume.status, VolumeStatus.FAILED)

    def test_failed_incremental_volume_cleans_its_snapshots(self):
        manager, ceph = self._manager()
        ceph.finalize_incremental_volume.return_value = False
        ceph.prune_source_snapshots.return_value = ["mig-j1-0"]
        volume = self._volume(MigrationMode.INCREMENTAL)
        volume.snapshots = [{"name": "mig-j1-0", "state": "created"}]
        manager._sessions[volume.source_volume_id] = mock.Mock()
        manager._job_id = "j1"
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.prune_source_snapshots.assert_called_once_with(
            "volume-s1", "j1", keep=0
        )
        self.assertEqual(volume.cleanup_status, "cleaned")
        # 源快照与暂存镜像都要清，否则残留会随目标卷 UUID 变化而永久漂着。
        ceph.remove_stage_image.assert_called_once_with("volume-t1")

    def test_failed_full_volume_cleans_stage_image(self):
        manager, ceph = self._manager()
        ceph.replace_rbd_data.return_value = False
        volume = self._volume(MigrationMode.FULL)
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.remove_stage_image.assert_called_once_with("volume-t1")
        self.assertEqual(volume.status, VolumeStatus.FAILED)

    def test_mover_selection_is_the_only_mode_branch(self):
        manager, _ceph = self._manager()
        self.assertIsInstance(
            manager._mover_for_mode(MigrationMode.FULL), FullVolumeMover
        )
        self.assertIsInstance(
            manager._mover_for_mode(MigrationMode.INCREMENTAL),
            IncrementalVolumeMover,
        )

    def test_full_mover_hooks_are_noops(self):
        manager, ceph = self._manager()
        mover = manager._mover_for_mode(MigrationMode.FULL)
        volume = self._volume(MigrationMode.FULL)
        self.assertIsNone(mover.prepare(self._vm(volume), {}))
        mover.on_failure(volume)
        ceph.precopy_volume.assert_not_called()
        ceph.prune_source_snapshots.assert_not_called()
        ceph.remove_stage_image.assert_called_once_with("volume-t1")


class PrecopyTest(unittest.TestCase):
    @staticmethod
    def _vm_with_volumes(count: int) -> VmTask:
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        vm.volumes = [
            VolumeTask(
                source_volume_id=f"s{index}",
                source_rbd_name=f"volume-s{index}",
                target_volume_id=f"t{index}",
                target_rbd_name=f"volume-t{index}",
                size=10,
                mode=MigrationMode.INCREMENTAL,
            )
            for index in range(1, count + 1)
        ]
        return vm

    @staticmethod
    def _ceph_with_sessions(sessions: dict[str, mock.Mock]) -> mock.Mock:
        ceph = mock.Mock()
        ceph.seed_volume.side_effect = lambda source, target, job, **kw: (
            sessions[source]
        )
        ceph.sync_volume_round.return_value = 1024
        return ceph

    def test_precopy_reports_progress_and_phase_labels(self):
        session = mock.Mock(layout={"order": 20})
        ceph = self._ceph_with_sessions({"volume-s1": session})

        def run_seed(*args, **kwargs):
            kwargs["on_phase"]("基线全量")
            kwargs["progress_cb"](5 * 1024**3, 10 * 1024**3, 200 * 1024 * 1024)
            return session

        def run_round(*args, **kwargs):
            # 轮次标签由 manager 在调用前设置，ceph 侧只汇报进度。
            kwargs["progress_cb"](1024**3, 4 * 1024**3, 100 * 1024 * 1024)
            return 1024

        ceph.seed_volume.side_effect = run_seed
        ceph.sync_volume_round.side_effect = run_round
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        vm = self._vm_with_volumes(1)
        labels: list[str] = []
        original_set_label = manager._set_volume_progress_label

        def spy_set_label(volume, label, keep_percent=False):
            labels.append(label)
            original_set_label(volume, label, keep_percent)

        manager._set_volume_progress_label = spy_set_label

        manager._precopy_volumes(vm, {"job_id": "j1", "delta_rounds": 2})

        volume = vm.volumes[0]
        distinct = [item for i, item in enumerate(labels) if i == 0 or item != labels[i - 1]]
        # 预拷贝阶段必须和全量拷贝一样有状态、百分比与速率，而不是空白。
        self.assertEqual(distinct, ["基线全量", "基线完成", "增量 1/2", "增量 2/2"])
        # 最后一轮已经搬完：卷收敛到"已就绪 100%"，不再挂着旧速率等切换。
        self.assertEqual(volume.status, VolumeStatus.READY)
        self.assertEqual(volume.progress_label, "增量 2/2")
        self.assertEqual(volume.progress_percent, 100.0)
        self.assertIsNone(volume.throughput_mb_s)

    def test_finished_round_snaps_progress_and_marks_volume_ready(self):
        """进度总量是 `rbd diff` 预估，回调常停在 99.x%；一轮搬完必须收敛到 100%。

        线上出现过 VM 已经 `awaiting_cutover`、卷行却永远显示"拷贝中
        99.2% · 7.4 MiB/s"的画面：预估值比 export-diff 实际载荷大，
        且没有任何一步把卷从 copying 收尾。
        """
        session = mock.Mock(layout={})
        ceph = mock.Mock()
        ceph.seed_volume.return_value = session
        state = {"rounds": 0}

        def run_round(*args, **kwargs):
            state["rounds"] += 1
            # 预估 107 字节、实传 106 字节：最后一帧只到 99.1%。
            kwargs["progress_cb"](106, 107, 7.4 * 1024 * 1024)
            return 106

        ceph.sync_volume_round.side_effect = run_round
        vm = self._vm_with_volumes(1)
        vm.sync_requested = True
        manager = MigrationManager(
            source_os=None,
            target_os=None,
            ceph_utils=ceph,
            cutover_requested=lambda: state["rounds"] >= 1,
            sync_requested=lambda: vm.sync_requested,
        )
        manager._job_id = "j1"
        manager._sleep_with_stop = lambda seconds: None

        manager._precopy_volumes(
            vm,
            {"job_id": "j1", "delta_rounds": 3, "cutover_mode": "manual"},
        )

        volume = vm.volumes[0]
        self.assertEqual(vm.status, VmStatus.AWAITING_CUTOVER)
        self.assertEqual(volume.status, VolumeStatus.READY)
        self.assertEqual(volume.progress_label, "已就绪，等待同步")
        self.assertEqual(volume.progress_percent, 100.0)
        self.assertIsNone(volume.throughput_mb_s)

    def test_baseline_marks_volume_copying_before_copy_starts(self):
        """基线全备一走就是几十分钟，卷必须一上来就是 copying。

        以前要等 `seed_volume` 返回才置 copying，页面按状态判空，于是整段全量
        拷贝都没有进度条，看起来像"到 100% 才突然出现"。
        """
        session = mock.Mock(layout={"order": 20})
        ceph = mock.Mock()
        ceph.sync_volume_round.return_value = 0
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        vm = self._vm_with_volumes(1)
        seen: list[VolumeStatus] = []

        def run_seed(source, target, job, **kwargs):
            seen.append(vm.volumes[0].status)
            return session

        ceph.seed_volume.side_effect = run_seed

        manager._precopy_volumes(vm, {"job_id": "j1", "delta_rounds": 0})

        self.assertEqual(seen, [VolumeStatus.COPYING])

    def test_precopy_records_session_layout_and_snapshots(self):
        session = mock.Mock(layout={"order": 20})
        ceph = self._ceph_with_sessions({"volume-s1": session})

        def run_seed(*args, **kwargs):
            kwargs["on_snapshot"]("mig-j1-0")
            return session

        ceph.seed_volume.side_effect = run_seed
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        vm = self._vm_with_volumes(1)
        manager._precopy_volumes(vm, {"job_id": "j1", "delta_rounds": 1})

        self.assertIs(manager._sessions["s1"], session)
        self.assertEqual(vm.volumes[0].layout, {"order": 20})
        self.assertEqual(vm.volumes[0].snapshots[0]["name"], "mig-j1-0")

    def test_precopy_seeds_every_volume_before_any_incremental_round(self):
        """回归：不能把卷 1 的所有轮次跑完再开始卷 2 的全备。"""
        events: list[str] = []
        sessions = {f"volume-s{i}": mock.Mock(layout={}) for i in (1, 2)}
        ceph = mock.Mock()

        def seed(source, target, job, **kwargs):
            events.append(f"seed:{source}")
            return sessions[source]

        def sync(session, **kwargs):
            events.append(f"round:{session is sessions['volume-s1'] and 's1' or 's2'}")
            return 1024

        ceph.seed_volume.side_effect = seed
        ceph.sync_volume_round.side_effect = sync
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        vm = self._vm_with_volumes(2)

        manager._precopy_volumes(vm, {"job_id": "j1", "delta_rounds": 2})

        self.assertEqual(events[:2], ["seed:volume-s1", "seed:volume-s2"])
        # 之后每轮每盘各一次：轮 1（s1, s2）→ 轮 2（s1, s2）。
        self.assertEqual(
            sorted(events[2:4]), ["round:s1", "round:s2"]
        )
        self.assertEqual(len(events), 6)

    def test_manual_mode_never_syncs_on_its_own(self):
        """手动模式：基线跑完就待命，没人点同步就必须一轮都不跑。"""
        sessions = {f"volume-s{i}": mock.Mock(layout={}) for i in (1, 2)}
        ceph = mock.Mock()
        ceph.seed_volume.side_effect = lambda source, target, job, **kw: sessions[source]
        vm = self._vm_with_volumes(2)
        state = {"rounds": 0, "checks": 0}

        def sync(session, **kwargs):
            state["rounds"] += 1
            return 4096

        ceph.sync_volume_round.side_effect = sync
        manager = MigrationManager(
            source_os=None,
            target_os=None,
            ceph_utils=ceph,
            # 第二次轮询才"点切换"，期间不该自己跑任何轮次。
            cutover_requested=lambda: state.__setitem__(
                "checks", state["checks"] + 1
            ) or state["checks"] > 1,
            sync_requested=lambda: vm.sync_requested,
        )
        manager._job_id = "j1"
        manager._sleep_with_stop = lambda seconds: None

        manager._precopy_volumes(
            vm,
            {"job_id": "j1", "delta_rounds": 3, "cutover_mode": "manual"},
        )

        self.assertEqual(state["rounds"], 0)
        self.assertEqual(vm.status, VmStatus.AWAITING_CUTOVER)
        self.assertEqual(vm.phase, "awaiting_cutover")

    def test_manual_sync_runs_exactly_one_round_per_request(self):
        """手动模式：点一次「同步一次」，每块盘各跑一轮，然后回到待命。"""
        sessions = {f"volume-s{i}": mock.Mock(layout={}) for i in (1, 2)}
        ceph = mock.Mock()
        ceph.seed_volume.side_effect = lambda source, target, job, **kw: sessions[source]
        labels: list[str] = []
        state = {"rounds": 0, "cutover": False}
        vm = self._vm_with_volumes(2)

        def sync(session, **kwargs):
            state["rounds"] += 1
            if state["rounds"] == 2:
                # 第一轮同步做完，再点一次「同步一次」。
                vm.sync_requested = True
            elif state["rounds"] == 4:
                # 第二轮同步做完，点「开始切换」。
                state["cutover"] = True
            return 4096

        ceph.sync_volume_round.side_effect = sync
        manager = MigrationManager(
            source_os=None,
            target_os=None,
            ceph_utils=ceph,
            cutover_requested=lambda: state["cutover"],
            sync_requested=lambda: vm.sync_requested,
        )
        manager._job_id = "j1"
        original_set_label = manager._set_volume_progress_label

        def spy_set_label(volume, label, keep_percent=False):
            if label not in labels:
                labels.append(label)
            original_set_label(volume, label, keep_percent)

        manager._set_volume_progress_label = spy_set_label
        vm.sync_requested = True

        manager._precopy_volumes(
            vm,
            {"job_id": "j1", "delta_rounds": 9, "cutover_mode": "manual"},
        )

        # 2 次点击 × 2 块盘 = 4 次卷级同步；delta_rounds=9 完全不影响手动模式。
        self.assertEqual(state["rounds"], 4)
        self.assertFalse(vm.sync_requested)
        # 待命时说清"在等什么"，且不再出现 "增量 1/3" 这类轮次编号。
        self.assertEqual(
            labels, ["基线全量", "基线完成", "已就绪，等待同步", "增量同步"]
        )

    def test_auto_cutover_stops_after_configured_rounds(self):
        sessions = {f"volume-s{i}": mock.Mock(layout={}) for i in (1, 2)}
        ceph = mock.Mock()
        ceph.seed_volume.side_effect = lambda source, target, job, **kw: sessions[source]
        ceph.sync_volume_round.return_value = 4096
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        vm = self._vm_with_volumes(2)

        manager._precopy_volumes(vm, {"job_id": "j1", "delta_rounds": 2})

        self.assertEqual(ceph.sync_volume_round.call_count, 4)
        self.assertNotEqual(vm.status, VmStatus.AWAITING_CUTOVER)

    def test_auto_round_interval_is_labelled(self):
        """轮次之间在等间隔时，进度条必须说清在等什么，不能停在 100% 装死。"""
        sessions = {f"volume-s{i}": mock.Mock(layout={}) for i in (1, 2)}
        ceph = mock.Mock()
        ceph.seed_volume.side_effect = lambda source, target, job, **kw: sessions[source]
        ceph.sync_volume_round.return_value = 4096
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        manager._sleep_with_stop = lambda seconds: None
        vm = self._vm_with_volumes(2)
        labels: list[str] = []
        original_set_label = manager._set_volume_progress_label

        def spy_set_label(volume, label, keep_percent=False):
            if not labels or labels[-1] != label:
                labels.append(label)
            original_set_label(volume, label, keep_percent)

        manager._set_volume_progress_label = spy_set_label

        manager._precopy_volumes(
            vm, {"job_id": "j1", "delta_rounds": 2, "delta_interval_seconds": 60}
        )

        self.assertIn("等待下一轮（60s）", labels)


class StageImageReseedTest(unittest.TestCase):
    """暂存卷中途消失时，就地重做基线全备而不是让整台 VM 失败。"""

    def _manager(self, ceph, reseed_calls: list[str]):
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        volume = VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            mode=MigrationMode.INCREMENTAL,
        )
        vm.volumes = [volume]
        manager._sessions["s1"] = mock.Mock(layout={})

        def seed(source, target, job, **kwargs):
            reseed_calls.append(source)
            return mock.Mock(layout={})

        ceph.seed_volume.side_effect = seed
        ceph.prune_source_snapshots.return_value = []
        return manager, vm, volume

    def test_missing_stage_image_reseeds_baseline_and_retries_round(self):
        ceph = mock.Mock()
        ceph.sync_volume_round.side_effect = [StageImageMissingError("boom"), 4096]
        reseeds: list[str] = []
        manager, vm, volume = self._manager(ceph, reseeds)

        copied = manager._sync_round_with_reseed(
            vm, volume, rate_limit_bytes_per_sec=0.0
        )

        self.assertEqual(copied, 4096)
        # 重做基线前必须清掉本 job 的旧源快照，否则 mig-<job>-0 会撞名。
        ceph.prune_source_snapshots.assert_called_once_with("volume-s1", "j1", keep=0)
        self.assertEqual(reseeds, ["volume-s1"])
        self.assertEqual(manager._baseline_reseeds["s1"], 1)
        self.assertIsNotNone(manager._sessions["s1"])

    def test_manual_sync_recovers_from_stage_loss_and_keeps_vm_alive(self):
        """端到端：手动同步那轮暂存卷没了，自动重做基线后本轮照常完成。"""
        ceph = mock.Mock()
        sessions = {"volume-s1": mock.Mock(layout={})}
        ceph.seed_volume.side_effect = lambda source, target, job, **kw: sessions[
            source
        ]
        ceph.prune_source_snapshots.return_value = []
        state = {"loss_done": False, "cutover": False, "sync": True}

        def sync(session, **kwargs):
            if not state["loss_done"]:
                state["loss_done"] = True
                raise StageImageMissingError("目标暂存卷 volume-t1-mig-stage 不存在")
            state["cutover"] = True
            state["sync"] = False
            return 2048

        ceph.sync_volume_round.side_effect = sync
        manager = MigrationManager(
            source_os=None,
            target_os=None,
            ceph_utils=ceph,
            cutover_requested=lambda: state["cutover"],
            sync_requested=lambda: state["sync"],
        )
        manager._job_id = "j1"
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        volume = VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            mode=MigrationMode.INCREMENTAL,
        )
        vm.volumes = [volume]

        manager._precopy_volumes(
            vm,
            {"job_id": "j1", "delta_rounds": 1, "cutover_mode": "manual"},
        )

        self.assertEqual(ceph.seed_volume.call_count, 2)  # 基线 + 重做
        self.assertNotIn(volume.status.value, ("failed", "cancelled"))
        self.assertIsNone(volume.error)

    def test_repeated_stage_loss_gives_up_after_cap(self):
        ceph = mock.Mock()
        ceph.sync_volume_round.side_effect = StageImageMissingError("boom")
        reseeds: list[str] = []
        manager, vm, volume = self._manager(ceph, reseeds)

        with self.assertRaises(StageImageMissingError):
            manager._sync_round_with_reseed(vm, volume, rate_limit_bytes_per_sec=0.0)

        # 不能"刚建好又被清掉"时无限全量拷贝：重做次数有上限。
        self.assertEqual(reseeds, ["volume-s1"] * MAX_BASELINE_RESEEDS)
        self.assertEqual(
            ceph.sync_volume_round.call_count, MAX_BASELINE_RESEEDS + 1
        )


class CancelClassificationTest(unittest.TestCase):
    """取消与普通失败必须区分清楚。"""

    def _manager(self, **kwargs):
        manager = MigrationManager(
            source_os=mock.MagicMock(),
            target_os=mock.MagicMock(),
            ceph_utils=mock.MagicMock(),
            **kwargs,
        )
        manager._migrate_vm_inner = mock.Mock(side_effect=RuntimeError("boom"))
        return manager

    def test_cancelled_vm_is_marked_cancelled(self):
        manager = self._manager(cancelled=lambda: True)
        vm = VmTask(name="vm-1", target_az="az1")

        manager.migrate_vm(vm, {"job_id": "job-1"})

        self.assertEqual(vm.status, VmStatus.CANCELLED)
        self.assertIn("取消", vm.error)

    def test_plain_failure_is_marked_failed(self):
        manager = self._manager(cancelled=lambda: False)
        vm = VmTask(name="vm-1", target_az="az1")

        manager.migrate_vm(vm, {"job_id": "job-1"})

        self.assertEqual(vm.status, VmStatus.FAILED)
        self.assertEqual(vm.error, "boom")

    def test_shutdown_interrupt_is_not_treated_as_cancel(self):
        manager = self._manager(
            can_proceed=lambda: False, cancelled=lambda: False
        )
        vm = VmTask(name="vm-1", target_az="az1")

        manager.migrate_vm(vm, {"job_id": "job-1"})

        self.assertEqual(vm.status, VmStatus.FAILED)
        self.assertEqual(vm.error, "boom")

    def test_default_cancelled_predicate_is_false(self):
        manager = self._manager()
        vm = VmTask(name="vm-1", target_az="az1")

        manager.migrate_vm(vm, {"job_id": "job-1"})

        self.assertEqual(vm.status, VmStatus.FAILED)

    def test_interrupted_vm_cleans_stage_and_snapshots(self):
        # 取消/停机走 _check_stop 抛异常，绕过了卷级 on_failure，必须兜底清理。
        manager = self._manager(can_proceed=lambda: False)
        manager._job_id = "job-1"
        ceph = manager.ceph_utils
        ceph.prune_source_snapshots.return_value = []
        vm = VmTask(name="vm-1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
                mode=MigrationMode.INCREMENTAL,
            ),
            VolumeTask(
                source_volume_id="s2",
                source_rbd_name="volume-s2",
                target_volume_id="t2",
                target_rbd_name="volume-t2",
                mode=MigrationMode.INCREMENTAL,
                status=VolumeStatus.SUCCESS,
            ),
        ]

        manager.migrate_vm(vm, {"job_id": "job-1"})

        ceph.remove_stage_image.assert_called_once_with("volume-t1")
        ceph.prune_source_snapshots.assert_called_once_with("volume-s1", "job-1", keep=0)

    def test_failed_vm_settles_volume_stuck_in_copying(self):
        # 卷停在 copying 时 VM 级失败必须落终态，否则页面与 GC 都以为它还在写。
        manager = self._manager(can_proceed=lambda: True)
        manager._job_id = "job-1"
        manager.ceph_utils.prune_source_snapshots.return_value = []
        vm = VmTask(name="vm-1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
                mode=MigrationMode.INCREMENTAL,
                status=VolumeStatus.COPYING,
            )
        ]

        manager.migrate_vm(vm, {"job_id": "job-1"})

        self.assertEqual(vm.volumes[0].status, VolumeStatus.FAILED)
        self.assertTrue(vm.volumes[0].error)


if __name__ == "__main__":
    unittest.main()

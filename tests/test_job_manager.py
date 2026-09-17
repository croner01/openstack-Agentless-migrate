import unittest
import json
import logging
import os
import shutil
import subprocess
import threading
import tempfile
import time
from unittest import mock

import job_manager
from excel_parser import MigrationRow
from job_manager import GC_SCAN_TIMEOUT_SECONDS, JobManager
from state_machine import (
    JobStatus,
    MigrationJob,
    MigrationMode,
    VolumeStatus,
    VolumeTask,
    VmStatus,
    VmTask,
)


class JobManagerTest(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self._tmp_dir = tempfile.TemporaryDirectory()
        state_file = os.path.join(self._tmp_dir.name, "jobs_state.json")
        self.manager = JobManager(state_file=state_file)

    def tearDown(self):
        self._tmp_dir.cleanup()
        logging.disable(logging.NOTSET)

    def test_create_job_carries_start_target_flag(self):
        """逐 VM 的「迁移后开机」必须落到 VmTask，否则收尾分支读不到。"""
        job = self.manager.create_job(
            [
                MigrationRow(vm_name="vm-a", target_az="az1"),
                MigrationRow(vm_name="vm-b", target_az="az1", start_target=False),
            ]
        )

        self.assertTrue(job.vms[0].start_target)
        self.assertFalse(job.vms[1].start_target)

    def test_start_target_survives_persist_and_restart(self):
        job = self.manager.create_job(
            [MigrationRow(vm_name="vm-a", target_az="az1", start_target=False)]
        )
        self.manager.save_now()

        restored = JobManager(state_file=self.manager._state_file).get(job.id)

        self.assertFalse(restored.vms[0].start_target)

    def test_job_state_round_trip_survives_restart(self):
        job = self.manager.create_job(
            [MigrationRow(vm_name="vm-a", target_az="az1")]
        )
        job.vms[0].mark_failed("interrupted")
        self.manager.save_now()

        restarted = JobManager(state_file=self.manager._state_file)
        restored = restarted.get(job.id)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.vms[0].status, VmStatus.FAILED)
        self.assertEqual(restored.vms[0].error, "interrupted")

    def test_restart_settles_volumes_left_mid_flight(self):
        """重启后卷不该停在 pending/copying，否则 GC 永远不敢回收它的暂存卷。"""
        job = self.manager.create_job(
            [MigrationRow(vm_name="vm-a", target_az="az1")]
        )
        volume = VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            mode=MigrationMode.INCREMENTAL,
            status=VolumeStatus.COPYING,
        )
        job.vms[0].volumes = [volume]
        self.manager.save_now()

        restarted = JobManager(state_file=self.manager._state_file)

        restored = restarted.get(job.id).vms[0].volumes[0]
        self.assertEqual(restored.status, VolumeStatus.FAILED)
        self.assertTrue(restored.error)

    def test_new_job_contains_vm_task_for_each_row(self):
        job = self.manager.create_job(
            [
                MigrationRow(vm_name="vm-a", target_az="az1"),
                MigrationRow(vm_name="vm-b", target_az="az2"),
            ]
        )
        self.assertEqual(len(job.vms), 2)
        self.assertEqual(job.vms[0].name, "vm-a")
        self.assertEqual(self.manager.get(job.id).id, job.id)

    def test_new_job_preserves_selected_server_id(self):
        job = self.manager.create_job(
            [
                MigrationRow(
                    vm_name="web-1",
                    target_az="az1",
                    source_server_id="srv-abc",
                )
            ]
        )
        self.assertEqual(job.vms[0].source_server_id, "srv-abc")

    def test_one_vm_failure_does_not_stop_other_vms(self):
        job = self.manager.create_job(
            [
                MigrationRow(vm_name="vm-a", target_az="az1"),
                MigrationRow(vm_name="vm-b", target_az="az1"),
            ]
        )

        def run(vm: VmTask, options):
            if vm.name == "vm-a":
                vm.mark_failed("boom")
            else:
                vm.status = VmStatus.SUCCESS
                vm.phase = "success"

        self.manager.execute(job, run, {})
        self.assertEqual(job.status.value, "completed")
        self.assertEqual(job.vms[0].status, VmStatus.FAILED)
        self.assertEqual(job.vms[1].status, VmStatus.SUCCESS)

    def test_execute_backstop_marks_vm_failed(self):
        job = self.manager.create_job(
            [MigrationRow(vm_name="vm-a", target_az="az1")]
        )

        def run(vm, options):
            raise RuntimeError("unexpected")

        self.manager.execute(job, run, {})
        self.assertEqual(job.vms[0].status, VmStatus.FAILED)
        self.assertIn("unexpected", job.vms[0].error)

    def test_execute_runs_two_vms_concurrently_when_configured(self):
        job = self.manager.create_job(
            [
                MigrationRow(vm_name="vm-a", target_az="az1"),
                MigrationRow(vm_name="vm-b", target_az="az1"),
            ]
        )
        barrier = threading.Barrier(2)

        def run(vm, options):
            barrier.wait(timeout=1)
            vm.status = VmStatus.SUCCESS
            vm.phase = "success"

        self.manager.execute(
            job,
            run,
            {"vm_concurrency": 2},
        )
        self.assertFalse(barrier.broken)

    def _make_upload_dir(self, job_id, age_seconds):
        path = os.path.join(self._tmp_dir.name, job_id)
        os.makedirs(path, exist_ok=True)
        old = time.time() - age_seconds
        os.utime(path, (old, old))
        return path

    def test_cleanup_removes_expired_terminal_job_upload_dir(self):
        job_id = "abcd1234abcd"
        path = self._make_upload_dir(job_id, 8 * 24 * 3600)
        job = MigrationJob(id=job_id, status=JobStatus.COMPLETED)
        self.manager._jobs[job_id] = job

        self.manager.cleanup_old_uploads(max_age_days=7)

        self.assertFalse(os.path.exists(path))

    def test_cleanup_keeps_active_running_job_dir(self):
        job_id = "abcd1234abcd"
        path = self._make_upload_dir(job_id, 8 * 24 * 3600)
        job = MigrationJob(id=job_id, status=JobStatus.RUNNING)
        self.manager._jobs[job_id] = job

        self.manager.cleanup_old_uploads(max_age_days=7)

        self.assertTrue(os.path.exists(path))


def running_incremental_job(
    job_id: str,
    target_rbd_name: str,
    *,
    source_rbd_name: str = "volume-s1",
) -> MigrationJob:
    """构造一个"正在迁移"的增量任务（卷停在 copying，暂存镜像在写）。"""
    job = MigrationJob(id=job_id, status=JobStatus.RUNNING)
    vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
    vm.volumes = [
        VolumeTask(
            source_volume_id=source_rbd_name,
            source_rbd_name=source_rbd_name,
            source_pool="volumes",
            target_volume_id=target_rbd_name,
            target_rbd_name=target_rbd_name,
            target_pool="volumes",
            mode=MigrationMode.INCREMENTAL,
            status=VolumeStatus.COPYING,
        )
    ]
    job.vms = [vm]
    return job


def manager_with_job(root: str, job_id: str, status: JobStatus) -> JobManager:
    """构造一个带单卷任务的 JobManager，并落好该 job 的 ceph 配置。"""
    manager = JobManager(state_file=os.path.join(root, "jobs_state.json"))
    job = MigrationJob(id=job_id, status=status)
    vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
    vm.volumes = [
        VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            source_pool="volumes",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            target_pool="volumes",
            mode=MigrationMode.INCREMENTAL,
            # 已落终态的卷才是"可以回收"的前提；在飞的卷由专门用例覆盖。
            status=VolumeStatus.FAILED,
        )
    ]
    job.vms = [vm]
    manager._jobs[job.id] = job
    job_dir = os.path.join(root, job_id)
    os.makedirs(job_dir, exist_ok=True)
    for name in ("source_ceph.conf", "target_ceph.conf"):
        with open(os.path.join(job_dir, name), "w", encoding="utf-8") as handle:
            handle.write("")
    return manager


class OrphanSnapshotSweepTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _manager_with_job(self, job_id, status):
        return manager_with_job(self.root, job_id, status)

    def test_sweeps_only_orphan_migration_snapshots(self):
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_source_snapshots.return_value = ["mig-jold-0", "daily"]

        removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, ["mig-jold-0"])
        fake.remove_source_snapshot.assert_called_once_with(
            "volume-s1", "mig-jold-0"
        )

    def test_active_job_snapshots_are_kept(self):
        manager = self._manager_with_job("jlive", JobStatus.RUNNING)
        fake = mock.Mock()
        fake.list_source_snapshots.return_value = ["mig-jlive-0"]

        removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_source_snapshot.assert_not_called()

    def test_snapshot_of_job_started_mid_sweep_is_kept(self):
        """扫描期间新起的任务，它的 mig-* 快照不能被开扫时的旧视图判成孤儿。

        线上事故：慢集群上一次 `rbd ls`/`snap ls` 能跑几十分钟，期间新任务已经
        建好快照；若保护集用开扫时的快照，这些新快照会被删掉，紧接着该轮的
        export-diff / import-diff 就报"起始快照不存在"。
        """
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()

        def list_snapshots(*args, **kwargs):
            manager._jobs["jnew"] = running_incremental_job("jnew", "volume-t9")
            return ["mig-jold-0", "mig-jnew-0"]

        fake.list_source_snapshots.side_effect = list_snapshots

        removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, ["mig-jold-0"])
        fake.remove_source_snapshot.assert_called_once_with(
            "volume-s1", "mig-jold-0"
        )

    def test_snapshot_scan_timeout_defers_to_next_round(self):
        """扫描超时说明集群很慢，本轮结论不可信，不能拿它去删快照。"""
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_source_snapshots.side_effect = subprocess.TimeoutExpired(
            ["rbd", "snap", "ls"], GC_SCAN_TIMEOUT_SECONDS
        )

        with LogCapture(logging.WARNING) as captured:
            removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_source_snapshot.assert_not_called()
        self.assertEqual(captured.with_level(logging.ERROR), [])
        self.assertTrue(any("下轮重试" in msg for msg in captured.with_level(logging.WARNING)))
        self.assertEqual(
            fake.list_source_snapshots.call_args.kwargs["timeout"],
            GC_SCAN_TIMEOUT_SECONDS,
        )

    def test_inflight_volume_snapshot_is_kept_even_when_job_finished(self):
        """任务已 completed 但卷还在 copying 时，源快照仍被 export-diff 引用。"""
        manager = self._manager_with_job("jdone", JobStatus.COMPLETED)
        job = manager.get("jdone")
        job.vms[0].volumes[0].status = VolumeStatus.COPYING
        fake = mock.Mock()
        fake.list_source_snapshots.return_value = ["mig-jdone-0"]

        removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_source_snapshot.assert_not_called()

    def test_busy_snapshot_delete_is_retried_quietly(self):
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_source_snapshots.return_value = ["mig-jold-0"]
        fake.remove_source_snapshot.side_effect = subprocess.CalledProcessError(
            16, ["rbd", "snap", "rm"], stderr="rbd: error: snapshot has watchers"
        )

        with LogCapture(logging.WARNING) as captured:
            removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        self.assertEqual(captured.with_level(logging.ERROR), [])
        self.assertTrue(
            any("下轮回收" in msg for msg in captured.with_level(logging.WARNING))
        )

    def test_missing_source_image_settles_cleanup_as_gone(self):
        """源卷已被删除时不能每轮重试，落终态收工。

        迁移成功后清理源机是常规操作，源卷一删，它上面的 mig-* 快照也随之
        消失。旧实现把 rc=2 当"环境抖动"每轮重试，同一条 WARNING 会永远刷
        下去，cleanup_status 也永远落不了地。
        """
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_source_snapshots.side_effect = subprocess.CalledProcessError(
            2,
            ["rbd", "snap", "ls"],
            stderr=(
                "rbd: error opening image volume-s1: (2) No such file or directory"
            ),
        )

        with LogCapture(logging.WARNING) as captured:
            removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        self.assertEqual(captured.with_level(logging.ERROR), [])
        self.assertTrue(
            any("源卷已不存在" in msg for msg in captured.with_level(logging.WARNING))
        )
        volume = manager.get("jold").vms[0].volumes[0]
        self.assertEqual(volume.cleanup_status, "gone")

        manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)
        self.assertEqual(fake.list_source_snapshots.call_count, 1)

        # 终态必须落盘：否则重启后 GC 又从这个卷开始刷同样的 WARNING。
        reloaded = JobManager(state_file=manager._state_file)
        self.assertEqual(
            reloaded.get("jold").vms[0].volumes[0].cleanup_status, "gone"
        )

    def test_cleaned_volume_is_not_rescanned(self):
        """已经清理干净的卷不再重扫：收工的历史任务不该每轮都摸一次集群。"""
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        manager.get("jold").vms[0].volumes[0].cleanup_status = "cleaned"
        fake = mock.Mock()

        removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.list_source_snapshots.assert_not_called()


class OrphanStageSweepTest(unittest.TestCase):
    def test_cutover_requires_awaiting_cutover_status(self):
        manager = self._manager_with_job("jlive", JobStatus.RUNNING)
        vm = manager.get("jlive").vms[0]

        # 还在预拷贝/已结束的 VM 不该响应切换按钮。
        self.assertFalse(manager.request_cutover("jlive", "vm1"))

        vm.status = VmStatus.AWAITING_CUTOVER
        self.assertTrue(manager.request_cutover("jlive", "vm1"))
        self.assertTrue(manager.is_cutover_requested("jlive", "vm1"))
        self.assertFalse(manager.is_cutover_requested("jlive", "vm-other"))

    def test_cutover_ignored_for_finished_job(self):
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        manager.get("jold").vms[0].status = VmStatus.AWAITING_CUTOVER

        self.assertFalse(manager.request_cutover("jold", "vm1"))

    def test_sync_request_only_in_awaiting_cutover(self):
        """手动同步只该在"待切换"待命时受理，别的地方点了没有意义。"""
        manager = self._manager_with_job("jlive", JobStatus.RUNNING)
        vm = manager.get("jlive").vms[0]

        self.assertFalse(manager.request_sync("jlive", "vm1"))

        vm.status = VmStatus.AWAITING_CUTOVER
        self.assertTrue(manager.request_sync("jlive", "vm1"))
        self.assertTrue(manager.is_sync_requested("jlive", "vm1"))
        # 已经排了一轮就别重复排，避免一次点击堆出多轮快照。
        self.assertFalse(manager.request_sync("jlive", "vm1"))

    def test_sync_request_ignored_for_finished_job(self):
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        manager.get("jold").vms[0].status = VmStatus.AWAITING_CUTOVER

        self.assertFalse(manager.request_sync("jold", "vm1"))
        self.assertFalse(manager.is_sync_requested("jold", "vm1"))

    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _manager_with_job(self, job_id, status):
        return manager_with_job(self.root, job_id, status)

    def test_orphan_stage_sweep_removes_stage_of_finished_job(self):
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_target_images.return_value = [
            "volume-t1-mig-stage",
            "volume-t1",
        ]
        fake.remove_stage_image.return_value = True

        removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, ["volume-t1-mig-stage"])
        fake.remove_stage_image.assert_called_once_with("volume-t1")

    def test_stage_of_job_started_mid_sweep_is_protected(self):
        """扫描期间新起的任务，它的暂存镜像不能被开扫时的旧视图判成孤儿。

        线上事故复现：启动那轮 GC 的 `rbd ls` 在慢集群上跑了 50 分钟才返回，
        期间新任务做完基线全备写下 `-mig-stage`；用开扫时的任务列表判断，就会
        把这台正在迁移的暂存镜像删掉，任务随后报
        "目标暂存卷不存在，无法应用增量 diff"。
        """
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()

        def list_images(*args, **kwargs):
            manager._jobs["jnew"] = running_incremental_job("jnew", "volume-t9")
            return ["volume-t9-mig-stage", "volume-t1-mig-stage"]

        fake.list_target_images.side_effect = list_images
        fake.remove_stage_image.return_value = True

        removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, ["volume-t1-mig-stage"])
        fake.remove_stage_image.assert_called_once_with("volume-t1")

    def test_stage_scan_timeout_defers_to_next_round(self):
        """`rbd ls` 超时（含客户端超时）时本轮不删任何东西。"""
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_target_images.side_effect = subprocess.TimeoutExpired(
            ["rbd", "ls"], GC_SCAN_TIMEOUT_SECONDS
        )

        with LogCapture(logging.WARNING) as captured:
            removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_stage_image.assert_not_called()
        self.assertEqual(captured.with_level(logging.ERROR), [])
        self.assertTrue(any("下轮重试" in msg for msg in captured.with_level(logging.WARNING)))
        self.assertEqual(
            fake.list_target_images.call_args.kwargs["timeout"],
            GC_SCAN_TIMEOUT_SECONDS,
        )

    def test_orphan_stage_sweep_keeps_stage_of_running_job(self):
        manager = self._manager_with_job("jlive", JobStatus.RUNNING)
        fake = mock.Mock()
        fake.list_target_images.return_value = ["volume-t1-mig-stage"]

        removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_stage_image.assert_not_called()

    def test_same_cluster_conf_is_scanned_once(self):
        """同一份 conf 的多个历史任务只扫一次池。

        每个任务都有自己的 conf 文件、内容是同一份 profile，按路径去重会让
        同一个集群被逐任务各扫一遍（线上 11 个任务 = 11 次 `rbd ls`）。
        """
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        other = manager_with_job(self.root, "jold2", JobStatus.COMPLETED)
        manager._jobs["jold2"] = other.get("jold2")
        fake = mock.Mock()
        fake.list_target_images.return_value = []

        manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(fake.list_target_images.call_count, 1)

    def test_different_cluster_confs_are_scanned_separately(self):
        """conf 内容不同（不同集群）时不能被去重掉，否则会漏扫整个池。"""
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        other = manager_with_job(self.root, "jold2", JobStatus.COMPLETED)
        manager._jobs["jold2"] = other.get("jold2")
        with open(
            os.path.join(self.root, "jold2", "target_ceph.conf"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("[global]\nmon_host = 10.6.10.4\n")
        fake = mock.Mock()
        fake.list_target_images.return_value = []

        with mock.patch.object(job_manager, "any_mon_reachable", return_value=True):
            manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(fake.list_target_images.call_count, 2)

    def test_unreachable_target_cluster_is_skipped_with_reason(self):
        """历史任务指向已下线的旧集群时不再空等超时，而是报出 mon 不可达。

        线上实测指向旧集群的 `rbd ls` 挂到 180s 仍无输出，既扫不出结果，还
        占着 GC 预算、刷一条看不出原因的"扫描暂存镜像超时"。
        """
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        with open(
            os.path.join(self.root, "jold", "target_ceph.conf"),
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("[global]\nmon_host = 100.100.13.21\n")
        fake = mock.Mock()

        with mock.patch.object(job_manager, "any_mon_reachable", return_value=False):
            with LogCapture(logging.WARNING) as captured:
                removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.list_target_images.assert_not_called()
        messages = captured.with_level(logging.WARNING)
        self.assertTrue(any("目标集群不可达" in msg for msg in messages))
        self.assertTrue(any("100.100.13.21:6789" in msg for msg in messages))


class LogCapture:
    """收集指定级别以上的日志消息，断言"不该报错时确实没报错"。"""

    def __init__(self, level=logging.WARNING):
        self.messages: list[tuple[int, str]] = []
        self._level = level
        self._handler = None

    def __enter__(self):
        outer = self

        class _Handler(logging.Handler):
            def emit(self, record):
                if record.levelno >= outer._level:
                    outer.messages.append((record.levelno, record.getMessage()))

        self._handler = _Handler()
        logging.getLogger().addHandler(self._handler)
        return self

    def __exit__(self, *exc_info):
        logging.getLogger().removeHandler(self._handler)
        return False

    def with_level(self, level: int) -> list[str]:
        return [msg for lvl, msg in self.messages if lvl == level]


class StateSaveConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_concurrent_saves_do_not_race_on_shared_temp_file(self):
        """固定 tmp 名会让并发保存互相抢文件，报 ENOENT 保存失败。"""
        manager = JobManager(state_file=os.path.join(self.root, "jobs_state.json"))
        manager._jobs["j1"] = MigrationJob(id="j1")

        with LogCapture(logging.WARNING) as captured:

            def save_many():
                for _ in range(25):
                    manager._save()

            threads = [threading.Thread(target=save_many) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(captured.with_level(logging.WARNING), [])
        with open(
            os.path.join(self.root, "jobs_state.json"), encoding="utf-8"
        ) as handle:
            self.assertEqual(json.load(handle)["jobs"][0]["id"], "j1")
        self.assertEqual(
            [name for name in os.listdir(self.root) if name.endswith(".tmp")], []
        )


class OrphanStageSweepResilienceTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_busy_stage_is_skipped_quietly(self):
        """EBUSY 说明镜像还被在跑的 rbd 占用，不是孤儿，不该刷 ERROR。"""
        manager = manager_with_job(self.root, "jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_target_images.return_value = ["volume-t1-mig-stage"]
        fake.remove_stage_image.side_effect = subprocess.CalledProcessError(
            16, ["rbd", "rm"], stderr="rbd: error: image has watchers - not removing"
        )
        with LogCapture(logging.WARNING) as captured:
            removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        self.assertEqual(captured.with_level(logging.ERROR), [])
        self.assertTrue(
            any("占用" in msg for msg in captured.with_level(logging.WARNING))
        )

    def test_stage_of_inflight_volume_is_protected_even_when_job_finished(self):
        """卷还在 copying 就有 rbd 在写它，绝不能当成孤儿删掉。"""
        manager = manager_with_job(self.root, "jdone", JobStatus.COMPLETED)
        manager._jobs["jdone"].vms[0].volumes[0].status = VolumeStatus.COPYING
        fake = mock.Mock()
        fake.list_target_images.return_value = ["volume-t1-mig-stage"]

        removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_stage_image.assert_not_called()

    def test_stage_of_volume_being_seeded_is_protected(self):
        """基线全备期间卷状态还是 pending，但暂存镜像已经在写。"""
        manager = manager_with_job(self.root, "jseed", JobStatus.COMPLETED)
        manager._jobs["jseed"].vms[0].volumes[0].status = VolumeStatus.PENDING
        fake = mock.Mock()
        fake.list_target_images.return_value = ["volume-t1-mig-stage"]

        removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_stage_image.assert_not_called()

    def test_signal_abort_is_retried_quietly(self):
        """rbd 客户端 SIGABRT 属于可重试，不该当成删除失败刷 ERROR。"""
        manager = manager_with_job(self.root, "jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_target_images.return_value = ["volume-t1-mig-stage"]
        fake.remove_stage_image.side_effect = subprocess.CalledProcessError(
            -6,
            ["rbd", "rm"],
            stderr="FAILED ceph_assert(q != removed_snaps_queue.end())",
        )
        with LogCapture(logging.WARNING) as captured:
            removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        self.assertEqual(captured.with_level(logging.ERROR), [])
        self.assertTrue(
            any("中断" in msg for msg in captured.with_level(logging.WARNING))
        )

    def test_scan_timeout_is_not_reported_as_error(self):
        """`rbd ls` 超时（rc=110）是集群抖动，降级为 WARNING 等下轮重试。"""
        manager = manager_with_job(self.root, "jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_target_images.side_effect = subprocess.CalledProcessError(
            110, ["rbd", "ls"], stderr="timed out"
        )
        with LogCapture(logging.WARNING) as captured:
            removed = manager.sweep_orphan_stages(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        self.assertEqual(captured.with_level(logging.ERROR), [])
        self.assertTrue(
            any("下轮重试" in msg for msg in captured.with_level(logging.WARNING))
        )


class CreateJobModeTest(unittest.TestCase):
    def test_create_job_propagates_incremental_mode(self):
        root = tempfile.mkdtemp()
        try:
            manager = JobManager(
                state_file=os.path.join(root, "jobs_state.json")
            )
            rows = [
                MigrationRow(
                    vm_name="vm1", target_az="az1", mode="incremental"
                )
            ]
            job = manager.create_job(rows, job_id="j1")
            self.assertEqual(job.vms[0].mode, MigrationMode.INCREMENTAL)
            self.assertEqual(job.to_dict()["vms"][0]["mode"], "incremental")
        finally:
            shutil.rmtree(root, ignore_errors=True)


class JobCancelDeleteTest(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.state_file = os.path.join(self._tmp_dir.name, "jobs_state.json")
        self.manager = JobManager(state_file=self.state_file)

    def tearDown(self):
        self._tmp_dir.cleanup()
        logging.disable(logging.NOTSET)

    def _job(self, job_id="job-1"):
        job = self.manager.create_job(
            [
                MigrationRow(vm_name="vm-a", target_az="az1"),
                MigrationRow(vm_name="vm-b", target_az="az1"),
            ],
            job_id=job_id,
        )
        os.makedirs(os.path.join(self._tmp_dir.name, job_id), exist_ok=True)
        return job

    def test_cancel_marks_unfinished_vms_cancelled(self):
        job = self._job()
        job.vms[0].mark_success()

        self.assertTrue(self.manager.cancel("job-1"))

        self.assertTrue(self.manager.is_cancelled("job-1"))
        self.assertEqual(job.vms[0].status, VmStatus.SUCCESS)
        self.assertEqual(job.vms[1].status, VmStatus.CANCELLED)
        self.assertEqual(job.vms[1].phase, "cancelled")

    def test_cancel_returns_false_for_finished_job(self):
        job = self._job()
        job.status = JobStatus.COMPLETED

        self.assertFalse(self.manager.cancel("job-1"))
        self.assertFalse(self.manager.is_cancelled("job-1"))

    def test_cancel_unknown_job_returns_false(self):
        self.assertFalse(self.manager.cancel("missing"))
        self.assertFalse(self.manager.is_cancelled("missing"))

    def test_cancelled_flag_survives_reload(self):
        self._job()
        self.manager.cancel("job-1")

        reloaded = JobManager(state_file=self.state_file)

        self.assertTrue(reloaded.get("job-1").cancelled)

    def test_delete_removes_job_and_upload_dir(self):
        job = self._job()
        job.status = JobStatus.COMPLETED
        job_dir = os.path.join(self._tmp_dir.name, "job-1")

        self.assertIsNone(self.manager.delete("job-1"))

        self.assertIsNone(self.manager.get("job-1"))
        self.assertFalse(os.path.exists(job_dir))

    def test_delete_refuses_running_job(self):
        self._job()

        error = self.manager.delete("job-1")

        self.assertIn("进行中", error)
        self.assertIsNotNone(self.manager.get("job-1"))

    def test_delete_unknown_job_returns_error(self):
        self.assertEqual(self.manager.delete("missing"), "任务不存在")

    def test_delete_keeps_other_jobs(self):
        first = self._job("job-1")
        second = self._job("job-2")
        first.status = JobStatus.COMPLETED
        second.status = JobStatus.COMPLETED

        self.manager.delete("job-1")

        self.assertIsNone(self.manager.get("job-1"))
        self.assertIsNotNone(self.manager.get("job-2"))


if __name__ == "__main__":
    unittest.main()

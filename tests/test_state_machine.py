import unittest

from state_machine import (
    JobStatus,
    MigrationJob,
    MigrationMode,
    VolumeStatus,
    VolumeTask,
    VmStatus,
    VmTask,
)


class VmTaskTest(unittest.TestCase):
    def test_all_volumes_success_allows_start(self):
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                status=VolumeStatus.SUCCESS,
            ),
            VolumeTask(
                source_volume_id="s2",
                source_rbd_name="volume-s2",
                target_volume_id="t2",
                status=VolumeStatus.SUCCESS,
            ),
        ]
        self.assertTrue(vm.can_start_target())

    def test_any_volume_failed_blocks_start(self):
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                status=VolumeStatus.SUCCESS,
            ),
            VolumeTask(
                source_volume_id="s2",
                source_rbd_name="volume-s2",
                target_volume_id="t2",
                status=VolumeStatus.FAILED,
            ),
        ]
        self.assertFalse(vm.can_start_target())

    def test_mark_failed_records_message_and_phase(self):
        vm = VmTask(name="vm1", target_az="az1")
        vm.mark_failed("boom")
        self.assertEqual(vm.status, VmStatus.FAILED)
        self.assertEqual(vm.phase, "failed")
        self.assertEqual(vm.error, "boom")

    def test_volume_progress_fields_survive_dict_round_trip(self):
        volume = VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            status=VolumeStatus.COPYING,
            progress_percent=42.5,
            throughput_mb_s=11.8,
            progress_label="增量 2/3",
        )
        restored = VolumeTask.from_dict(volume.to_dict())
        self.assertEqual(restored.progress_percent, 42.5)
        self.assertEqual(restored.throughput_mb_s, 11.8)
        self.assertEqual(restored.progress_label, "增量 2/3")

    def test_delta_round_fields_survive_dict_round_trip(self):
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        vm.delta_rounds_done = 2
        vm.delta_rounds_max = 3
        vm.delta_last_bytes = 4096
        vm.delta_threshold_bytes = 512 * 1024 * 1024
        vm.delta_interval_seconds = 60.0
        vm.delta_cutover_mode = "auto"

        restored = VmTask.from_dict(vm.to_dict())

        self.assertEqual(restored.delta_rounds_done, 2)
        self.assertEqual(restored.delta_rounds_max, 3)
        self.assertEqual(restored.delta_last_bytes, 4096)
        self.assertEqual(restored.delta_threshold_bytes, 512 * 1024 * 1024)
        self.assertEqual(restored.delta_interval_seconds, 60.0)
        self.assertEqual(restored.delta_cutover_mode, "auto")

    def test_delta_fields_default_for_legacy_payload(self):
        restored = VmTask.from_dict({"name": "vm1", "target_az": "az1"})

        self.assertEqual(restored.delta_rounds_done, 0)
        self.assertEqual(restored.delta_rounds_max, 0)
        self.assertIsNone(restored.delta_last_bytes)
        self.assertEqual(restored.delta_threshold_bytes, 0)
        self.assertEqual(restored.delta_interval_seconds, 0.0)
        self.assertEqual(restored.delta_cutover_mode, "")

    def test_start_target_defaults_to_boot(self):
        """默认开机：不传该字段的老作业与老前端语义不变。"""
        vm = VmTask(name="vm1", target_az="az1")

        self.assertTrue(vm.start_target)

    def test_start_target_survives_dict_round_trip(self):
        vm = VmTask(name="vm1", target_az="az1", start_target=False)

        restored = VmTask.from_dict(vm.to_dict())

        self.assertFalse(restored.start_target)

    def test_start_target_defaults_for_legacy_payload(self):
        """历史 jobs_state.json 没有该键，必须落到"开机"而不是报错。"""
        restored = VmTask.from_dict({"name": "vm1", "target_az": "az1"})

        self.assertTrue(restored.start_target)


class MigrationJobTest(unittest.TestCase):
    def test_relay_disks_survive_dict_round_trip(self):
        """中转机逐盘结果要跟着作业持久化，否则重启/收尾后就查不到了。"""
        vm = VmTask(name="vm-1", target_az="az1")
        vm.relay_disks = [
            {
                "volume_id": "vol-s1",
                "size": 40,
                "role": "boot",
                "status": "success",
                "target_volume_id": "vol-t1",
                "error": "",
            }
        ]

        restored = VmTask.from_dict(vm.to_dict())

        self.assertEqual(restored.relay_disks, vm.relay_disks)

    def test_relay_disks_default_for_legacy_payload(self):
        """老作业的 JSON 里没有这个字段，加载时不能报错。"""
        vm = VmTask(name="vm-1", target_az="az1")
        payload = vm.to_dict()
        payload.pop("relay_disks")

        self.assertEqual(VmTask.from_dict(payload).relay_disks, [])

    def test_job_completed_when_all_vms_terminal(self):
        job = MigrationJob(id="j1")
        job.vms = [
            VmTask(name="vm1", target_az="az1", status=VmStatus.SUCCESS),
            VmTask(name="vm2", target_az="az1", status=VmStatus.FAILED),
        ]
        self.assertEqual(job.refresh_status(), "completed")

    def test_job_stays_running_while_vms_pending(self):
        job = MigrationJob(id="j1")
        job.vms = [
            VmTask(name="vm1", target_az="az1", status=VmStatus.QUEUED),
            VmTask(name="vm2", target_az="az1", status=VmStatus.SUCCESS),
        ]
        self.assertEqual(job.refresh_status(), "running")

    def test_job_dict_round_trip(self):
        job = MigrationJob(id="j1")
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                status=VolumeStatus.COPYING,
            )
        ]
        vm.source_server_id = "server-src"
        job.vms = [vm]

        restored = MigrationJob.from_dict(job.to_dict())
        self.assertEqual(restored.id, "j1")
        self.assertEqual(restored.vms[0].name, "vm1")
        self.assertEqual(restored.vms[0].source_server_id, "server-src")
        self.assertEqual(
            restored.vms[0].volumes[0].status, VolumeStatus.COPYING
        )


class MigrationModePersistenceTest(unittest.TestCase):
    def test_volume_defaults_to_full_mode(self):
        volume = VolumeTask(source_volume_id="s1", source_rbd_name="volume-s1")
        self.assertEqual(volume.mode, MigrationMode.FULL)
        self.assertEqual(volume.cleanup_status, "none")

    def test_volume_round_trip_keeps_mode_layout_and_snapshots(self):
        volume = VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            source_pool="volumes",
            target_pool="volumes",
            mode=MigrationMode.INCREMENTAL,
            layout={"order": 20, "size": 10737418240},
            snapshots=[{"name": "mig-j1-0", "state": "created"}],
        )
        restored = VolumeTask.from_dict(volume.to_dict())
        self.assertEqual(restored.mode, MigrationMode.INCREMENTAL)
        self.assertEqual(restored.layout["order"], 20)
        self.assertEqual(restored.snapshots[0]["name"], "mig-j1-0")
        self.assertEqual(restored.source_pool, "volumes")

    def test_vm_round_trip_keeps_mode(self):
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        restored = VmTask.from_dict(vm.to_dict())
        self.assertEqual(restored.mode, MigrationMode.INCREMENTAL)

    def test_unknown_mode_falls_back_to_full(self):
        restored = VmTask.from_dict(
            {"name": "vm1", "target_az": "az1", "mode": "v2"}
        )
        self.assertEqual(restored.mode, MigrationMode.FULL)


class CancellationTest(unittest.TestCase):
    def test_mark_cancelled_sets_status_and_phase(self):
        vm = VmTask(name="vm-1", target_az="az1")

        vm.mark_cancelled()

        self.assertEqual(vm.status, VmStatus.CANCELLED)
        self.assertEqual(vm.phase, "cancelled")
        self.assertIn("取消", vm.error)
        self.assertIsNotNone(vm.finished_at)

    def test_refresh_status_reports_cancelled_when_any_vm_cancelled(self):
        job = MigrationJob(
            id="job-1",
            vms=[
                VmTask(name="vm-1", target_az="az1"),
                VmTask(name="vm-2", target_az="az1"),
            ],
        )
        job.vms[0].mark_success()
        job.vms[1].mark_cancelled()

        self.assertEqual(job.refresh_status(), JobStatus.CANCELLED.value)
        self.assertEqual(job.status, JobStatus.CANCELLED)

    def test_refresh_status_still_reports_completed_without_cancellation(self):
        job = MigrationJob(id="job-1", vms=[VmTask(name="vm-1", target_az="az1")])
        job.vms[0].mark_success()

        self.assertEqual(job.refresh_status(), JobStatus.COMPLETED.value)

    def test_cancelled_flag_survives_dict_round_trip(self):
        job = MigrationJob(id="job-1", cancelled=True)

        restored = MigrationJob.from_dict(
            {**job.to_dict(), "vms": []}
        )

        self.assertTrue(restored.cancelled)
        self.assertIn("cancelled", job.to_dict())


if __name__ == "__main__":
    unittest.main()

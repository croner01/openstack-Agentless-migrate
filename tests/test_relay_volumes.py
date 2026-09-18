import unittest
from unittest import mock

from relay_volumes import SourceCopy, VolumeLifecycle


class VolumeLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.create_volume_snapshot.return_value = mock.Mock(id="snap-1")
        self.source_os.create_volume_from_snapshot.return_value = mock.Mock(id="vol-d1")
        self.source_os.attach_volume.return_value = "att-1"
        self.target_os.attach_volume.return_value = "att-2"
        self.source_os.get_volume.return_value = mock.Mock(volume_type="src-ssd")
        self.lifecycle = VolumeLifecycle(self.source_os, self.target_os)

    def test_create_source_copy_snapshots_then_clones(self):
        copy = self.lifecycle.create_source_copy(
            volume_id="vol-s1",
            vm_name="vm-1",
            index=0,
            size=40,
        )

        self.assertEqual(copy.snapshot_id, "snap-1")
        self.assertEqual(copy.derived_volume_id, "vol-d1")
        snap_kwargs = self.source_os.create_volume_snapshot.call_args.kwargs
        self.assertEqual(snap_kwargs["volume_id"], "vol-s1")
        self.assertIn("vm-1", snap_kwargs["name"])
        clone_kwargs = self.source_os.create_volume_from_snapshot.call_args.kwargs
        self.assertEqual(clone_kwargs["snapshot_id"], "snap-1")
        self.assertEqual(clone_kwargs["size"], 40)
        # Cinder 的 AZ 由 OpenStackUtils 统一固定，lifecycle 不再透传 Nova AZ。
        self.assertNotIn("availability_zone", clone_kwargs)

    def test_create_source_copy_waits_for_both_resources(self):
        self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, size=40
        )

        snap_args, snap_kwargs = self.source_os.wait_snapshot_status.call_args
        self.assertEqual(snap_args, ("snap-1",))
        self.assertIn("mig-vm-1-0", snap_kwargs["context"])
        self.assertIn("40GiB", snap_kwargs["context"])
        clone_args, clone_kwargs = self.source_os.wait_volume_status.call_args
        self.assertEqual(clone_args, ("vol-d1",))
        self.assertIn("mig-vm-1-0", clone_kwargs["context"])
        self.assertIn("40GiB", clone_kwargs["context"])

    def test_derive_waits_scale_with_volume_size(self):
        """配了上限基线时，大卷在商业存储上是全量拷贝，等待必须按容量放大。"""
        env = {
            "MIGRATION_SNAPSHOT_READY_TIMEOUT": "600",
            "MIGRATION_VOLUME_READY_TIMEOUT": "600",
        }
        with mock.patch.dict("os.environ", env, clear=True):
            self.lifecycle.create_source_copy(
                volume_id="vol-s1", vm_name="vm-1", index=0, size=2048
            )

        snap_kwargs = self.source_os.wait_snapshot_status.call_args.kwargs
        clone_kwargs = self.source_os.wait_volume_status.call_args.kwargs
        self.assertEqual(snap_kwargs["timeout"], 2048 * 20)
        self.assertEqual(clone_kwargs["timeout"], 2048 * 20)

    def test_default_env_waits_unlimited(self):
        """默认不限时（0）：200GiB 以上的盘在商业存储上超过 1 小时很常见。"""
        with mock.patch.dict("os.environ", {}, clear=True):
            self.lifecycle.create_source_copy(
                volume_id="vol-s1", vm_name="vm-1", index=0, size=10
            )

        self.assertEqual(
            self.source_os.wait_snapshot_status.call_args.kwargs["timeout"], 0
        )
        self.assertEqual(
            self.source_os.wait_volume_status.call_args.kwargs["timeout"], 0
        )
        self.assertEqual(
            self.source_os.wait_volume_status.call_args.kwargs["should_stop"],
            self.lifecycle.should_stop,
        )

    def test_job_level_timeout_overrides_size_scaling(self):
        """作业表单填了超时就用它，别再按容量放大。"""
        lifecycle = VolumeLifecycle(
            self.source_os, self.target_os, ready_timeout=7200, snapshot_timeout=7200
        )

        with mock.patch.dict("os.environ", {}, clear=True):
            lifecycle.create_source_copy(
                volume_id="vol-s1", vm_name="vm-1", index=0, size=4096
            )

        self.assertEqual(
            self.source_os.wait_snapshot_status.call_args.kwargs["timeout"], 7200
        )
        self.assertEqual(
            self.source_os.wait_volume_status.call_args.kwargs["timeout"], 7200
        )

    def test_should_stop_is_forwarded_to_both_waits(self):
        """不限时等待必须能被取消，否则作业取消后线程会一直挂着。"""
        stop = lambda: False  # noqa: E731 - 只验证透传
        lifecycle = VolumeLifecycle(
            self.source_os, self.target_os, should_stop=stop
        )

        lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, size=10
        )

        self.assertIs(
            self.source_os.wait_snapshot_status.call_args.kwargs["should_stop"], stop
        )
        self.assertIs(
            self.source_os.wait_volume_status.call_args.kwargs["should_stop"], stop
        )

    def test_on_wait_hook_is_forwarded_to_both_waits(self):
        """长等待期间调用方要能刷新台账，否则会被对账当成残留清理。"""
        touched: list[float] = []

        def touch(waited: float) -> None:
            touched.append(waited)

        lifecycle = VolumeLifecycle(self.source_os, self.target_os)

        lifecycle.create_source_copy(
            volume_id="vol-s1",
            vm_name="vm-1",
            index=0,
            size=40,
            on_wait=touch,
        )

        self.assertIs(
            self.source_os.wait_snapshot_status.call_args.kwargs["on_wait"], touch
        )
        self.assertIs(
            self.source_os.wait_volume_status.call_args.kwargs["on_wait"], touch
        )

    def test_target_volume_wait_gets_on_wait_hook(self):
        self.target_os.create_blank_volume.return_value = mock.Mock(id="vol-t1")

        self.lifecycle.create_target_volume(
            name="vm-1-vol-0", size=40, on_wait=print
        )

        self.assertIs(
            self.target_os.wait_volume_status.call_args.kwargs["on_wait"], print
        )

    def test_attach_waits_with_its_own_bounded_timeout(self):
        """挂载卡住不会自己往前走，保留可预期上限 + 取消回调。"""
        from relay_volumes import ATTACH_READY_TIMEOUT_SECONDS

        self.lifecycle.attach(role="source", server_id="relay-s", volume_id="vol-d1")

        kwargs = self.source_os.wait_volume_status.call_args.kwargs
        self.assertEqual(kwargs["timeout"], ATTACH_READY_TIMEOUT_SECONDS)
        self.assertEqual(kwargs["target"], "in-use")

    def test_target_blank_volume_wait_can_be_cancelled(self):
        self.target_os.create_blank_volume.return_value = mock.Mock(id="vol-t1")

        self.lifecycle.create_target_volume(name="vm-1-0", size=40)

        kwargs = self.target_os.wait_volume_status.call_args.kwargs
        self.assertIn("40GiB", kwargs["context"])
        self.assertEqual(kwargs["should_stop"], self.lifecycle.should_stop)

    def test_attach_uses_role_cloud_and_waits_in_use(self):
        attachment = self.lifecycle.attach(
            role="source", server_id="relay-s", volume_id="vol-d1"
        )

        self.assertEqual(attachment, "att-1")
        self.source_os.attach_volume.assert_called_once_with(
            server_id="relay-s", volume_id="vol-d1"
        )
        self.assertEqual(
            self.source_os.wait_volume_status.call_args.args, ("vol-d1",)
        )
        self.assertEqual(
            self.source_os.wait_volume_status.call_args.kwargs["target"], "in-use"
        )
        self.assertIn("relay-s", self.source_os.wait_volume_status.call_args.kwargs["context"])

    def test_cleanup_retries_delete_after_forced_detach(self):
        """Cinder 报 must not be attached 时，强制卸载后再删一次，避免残留。"""
        copy = SourceCopy(snapshot_id="snap-1", derived_volume_id="vol-d1")
        # 首次查询说"没挂载"，删除被拒；随后强制卸载 + 再删成功
        self.source_os.find_volume_attachment.return_value = None
        self.source_os.delete_volume.side_effect = [
            RuntimeError("Invalid volume: must not be attached"),
            None,
        ]

        self.lifecycle.cleanup_source_copy(copy, "relay-s")

        self.assertEqual(self.source_os.delete_volume.call_count, 2)
        self.source_os.detach_volume.assert_called_with(
            server_id="relay-s", volume_id="vol-d1"
        )
        self.source_os.delete_volume_snapshot.assert_called_once_with("snap-1")

    def test_attach_detaches_volume_when_wait_times_out(self):
        self.source_os.wait_volume_status.side_effect = TimeoutError("超时")

        with self.assertRaises(TimeoutError):
            self.lifecycle.attach(role="source", server_id="relay-s", volume_id="vol-d1")

        self.source_os.detach_volume.assert_called_once_with(
            server_id="relay-s", volume_id="vol-d1"
        )

    def test_attach_rejects_unknown_role(self):
        with self.assertRaises(ValueError):
            self.lifecycle.attach(role="bogus", server_id="s", volume_id="v")

    def test_mark_bootable_flags_target_volume(self):
        self.lifecycle.mark_bootable(volume_id="vol-t1")

        self.target_os.set_volume_bootable.assert_called_once_with("vol-t1", True)

    def test_detach_ignores_missing_attachment(self):
        self.source_os.find_volume_attachment.return_value = None

        self.lifecycle.detach(role="source", server_id="relay-s", volume_id="vol-d1")

        self.source_os.detach_volume.assert_not_called()

    def test_detach_calls_api_and_waits_for_available(self):
        self.target_os.find_volume_attachment.return_value = "att-2"

        self.lifecycle.detach(role="target", server_id="relay-t", volume_id="vol-t1")

        self.target_os.detach_volume.assert_called_once_with(
            server_id="relay-t", volume_id="vol-t1"
        )
        kwargs = self.target_os.wait_volume_status.call_args.kwargs
        self.assertEqual(self.target_os.wait_volume_status.call_args.args, ("vol-t1",))
        self.assertEqual(kwargs["target"], "available")

    def test_cleanup_source_copy_detaches_then_deletes(self):
        copy = self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, size=40
        )
        self.source_os.find_volume_attachment.return_value = "att-1"

        self.lifecycle.cleanup_source_copy(copy, relay_server_id="relay-s")

        self.source_os.find_volume_attachment.assert_called_with("relay-s", "vol-d1")
        self.source_os.delete_volume.assert_called_once_with("vol-d1")
        self.source_os.delete_volume_snapshot.assert_called_once_with("snap-1")

    def test_cleanup_is_best_effort(self):
        copy = self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, size=40
        )
        self.source_os.delete_volume.side_effect = RuntimeError("boom")

        self.lifecycle.cleanup_source_copy(copy, relay_server_id="relay-s")

        self.source_os.delete_volume_snapshot.assert_called_once_with("snap-1")

    def test_create_target_volume_creates_blank_volume(self):
        self.target_os.create_blank_volume.return_value = mock.Mock(id="vol-t1")

        volume_id = self.lifecycle.create_target_volume(
            name="vm-1-vol-0",
            size=40,
            volume_type="ssd",
        )

        self.assertEqual(volume_id, "vol-t1")
        self.target_os.create_blank_volume.assert_called_once_with(
            name="vm-1-vol-0",
            size=40,
            volume_type="ssd",
        )
        kwargs = self.target_os.wait_volume_status.call_args.kwargs
        self.assertEqual(
            self.target_os.wait_volume_status.call_args.args, ("vol-t1",)
        )
        self.assertIn("vm-1-vol-0", kwargs["context"])

    def test_target_volume_passes_volume_type(self):
        self.target_os.create_blank_volume.return_value = mock.Mock(id="vol-t1")

        self.lifecycle.create_target_volume(
            name="vm-1-vol-0", size=200, volume_type="ssd"
        )

        kwargs = self.target_os.create_blank_volume.call_args.kwargs
        self.assertEqual(kwargs["volume_type"], "ssd")

    def test_source_copy_passes_volume_type(self):
        self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0,
            size=40, volume_type="src-ssd",
        )

        kwargs = self.source_os.create_volume_from_snapshot.call_args.kwargs
        self.assertEqual(kwargs["volume_type"], "src-ssd")

    def test_source_copy_inherits_source_volume_type_by_default(self):
        self.source_os.get_volume.return_value = mock.Mock(volume_type="legacy-ssd")

        self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, size=200
        )

        kwargs = self.source_os.create_volume_from_snapshot.call_args.kwargs
        self.assertEqual(kwargs["volume_type"], "legacy-ssd")

    def test_explicit_volume_type_wins_over_inherited(self):
        self.source_os.get_volume.return_value = mock.Mock(volume_type="legacy-ssd")

        self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0,
            size=200, volume_type="override-type",
        )

        kwargs = self.source_os.create_volume_from_snapshot.call_args.kwargs
        self.assertEqual(kwargs["volume_type"], "override-type")

    def test_source_copy_tolerates_volume_lookup_failure(self):
        self.source_os.get_volume.side_effect = RuntimeError("no volume api")

        self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, size=200
        )

        kwargs = self.source_os.create_volume_from_snapshot.call_args.kwargs
        self.assertIsNone(kwargs["volume_type"])

    def test_quota_error_is_translated_with_hint(self):
        self.target_os.create_blank_volume.side_effect = RuntimeError(
            "413 VolumeSizeExceedsAvailableQuota: Requested volume or snapshot "
            "exceeds allowed gigabytes___DEFAULT__ quota"
        )

        with self.assertRaises(RuntimeError) as ctx:
            self.lifecycle.create_target_volume(
                name="vm-1-vol-0", size=200
            )

        message = str(ctx.exception)
        self.assertIn("配额", message)
        self.assertIn("gigabytes___DEFAULT__", message)
        self.assertIn("200G", message)

    def test_non_quota_error_is_not_wrapped(self):
        self.target_os.create_blank_volume.side_effect = RuntimeError("disk full")

        with self.assertRaises(RuntimeError) as ctx:
            self.lifecycle.create_target_volume(
                name="vm-1-vol-0", size=200
            )

        self.assertEqual(str(ctx.exception), "disk full")


class ParkAndValidateSourceCopyTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.lifecycle = VolumeLifecycle(self.source_os, self.target_os)
        self.copy = SourceCopy(snapshot_id="snap-1", derived_volume_id="vol-d1")

    def test_park_volume_detaches_without_deleting(self):
        self.source_os.find_volume_attachment.return_value = "att-1"

        self.lifecycle.park_volume(
            role="source", server_id="relay-s", volume_id="vol-d1"
        )

        self.source_os.detach_volume.assert_called_once_with(
            server_id="relay-s", volume_id="vol-d1"
        )
        self.source_os.delete_volume.assert_not_called()
        self.source_os.delete_volume_snapshot.assert_not_called()

    def test_park_volume_is_noop_without_attachment(self):
        self.target_os.find_volume_attachment.return_value = None

        self.lifecycle.park_volume(
            role="target", server_id="relay-t", volume_id="vol-t1"
        )

        self.target_os.detach_volume.assert_not_called()

    def test_park_volume_swallows_detach_errors(self):
        """保留场景下卸载失败只记日志，不能覆盖调用方真正要抛的失败原因。"""
        self.target_os.find_volume_attachment.return_value = "att-9"
        self.target_os.detach_volume.side_effect = RuntimeError("boom")

        self.lifecycle.park_volume(
            role="target", server_id="relay-t", volume_id="vol-t1"
        )

        self.target_os.detach_volume.assert_called_once()

    def test_validate_source_copy_accepts_matching_resources(self):
        self.source_os.get_volume_snapshot.return_value = mock.Mock(status="available")
        self.source_os.get_volume.return_value = mock.Mock(
            status="available", size=40, volume_type="src-ssd"
        )

        ok, detail = self.lifecycle.validate_source_copy(
            self.copy, volume_id="vol-s1", size=40, volume_type="src-ssd"
        )

        self.assertTrue(ok, detail)

    def test_validate_source_copy_rejects_missing_derived_volume(self):
        self.source_os.get_volume_snapshot.return_value = mock.Mock(status="available")
        self.source_os.get_volume.side_effect = RuntimeError("404 not found")

        ok, detail = self.lifecycle.validate_source_copy(
            self.copy, volume_id="vol-s1", size=40
        )

        self.assertFalse(ok)
        self.assertIn("派生卷不可用", detail)

    def test_validate_source_copy_rejects_size_mismatch(self):
        self.source_os.get_volume_snapshot.return_value = mock.Mock(status="available")
        self.source_os.get_volume.return_value = mock.Mock(
            status="available", size=20, volume_type="src-ssd"
        )

        ok, detail = self.lifecycle.validate_source_copy(
            self.copy, volume_id="vol-s1", size=40
        )

        self.assertFalse(ok)
        self.assertIn("容量", detail)

    def test_validate_source_copy_rejects_volume_type_mismatch(self):
        self.source_os.get_volume_snapshot.return_value = mock.Mock(status="available")
        self.source_os.get_volume.return_value = mock.Mock(
            status="available", size=40, volume_type="old-ssd"
        )

        ok, detail = self.lifecycle.validate_source_copy(
            self.copy, volume_id="vol-s1", size=40, volume_type="new-ssd"
        )

        self.assertFalse(ok)
        self.assertIn("类型", detail)

    def test_validate_source_copy_rejects_bad_snapshot_status(self):
        self.source_os.get_volume_snapshot.return_value = mock.Mock(status="error")

        ok, detail = self.lifecycle.validate_source_copy(
            self.copy, volume_id="vol-s1", size=40
        )

        self.assertFalse(ok)
        self.assertIn("快照状态", detail)

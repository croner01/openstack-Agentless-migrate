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
        """大卷在商业存储上是全量拷贝，快照/派生等待必须按容量放大。"""
        with mock.patch.dict("os.environ", {}, clear=True):
            self.lifecycle.create_source_copy(
                volume_id="vol-s1", vm_name="vm-1", index=0, size=2048
            )

        snap_kwargs = self.source_os.wait_snapshot_status.call_args.kwargs
        clone_kwargs = self.source_os.wait_volume_status.call_args.kwargs
        self.assertEqual(snap_kwargs["timeout"], 2048 * 20)
        self.assertEqual(clone_kwargs["timeout"], 2048 * 20)

    def test_small_volume_keeps_env_defaults(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.lifecycle.create_source_copy(
                volume_id="vol-s1", vm_name="vm-1", index=0, size=10
            )

        self.assertEqual(
            self.source_os.wait_snapshot_status.call_args.kwargs["timeout"], 600
        )
        self.assertEqual(
            self.source_os.wait_volume_status.call_args.kwargs["timeout"], 1800
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
        self.target_os.wait_volume_status.assert_called_with(
            "vol-t1", target="available"
        )

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
        self.target_os.wait_volume_status.assert_called_with("vol-t1")

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

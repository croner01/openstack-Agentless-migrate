import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay_ledger import Ledger, VolumeTaskRecord
from relay_reaper import RelayReaper


class RelayReaperTest(unittest.TestCase):
    def setUp(self):
        self.ledger = mock.MagicMock()
        self.lifecycle = mock.MagicMock()
        self.reaper = RelayReaper(
            ledger=self.ledger,
            lifecycle=self.lifecycle,
            pools={"source": mock.MagicMock(), "target": mock.MagicMock()},
            stale_seconds=600.0,
        )

    def _record(self, **kwargs):
        defaults = dict(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="copying",
            derived_volume_id="vol-d1",
            snapshot_id="snap-1",
            source_relay_id="srv-s",
            target_volume_id="vol-t1",
            target_relay_id="srv-t",
            updated_at=1000.0,
        )
        defaults.update(kwargs)
        return VolumeTaskRecord(**defaults)

    def test_sweep_skips_fresh_records(self):
        self.ledger.all.return_value = [self._record(updated_at=1000.0)]

        cleaned = self.reaper.sweep(now=1100.0)

        self.assertEqual(cleaned, [])
        self.lifecycle.cleanup_source_copy.assert_not_called()

    def test_sweep_cleans_stale_record(self):
        self.ledger.all.return_value = [self._record(updated_at=1000.0)]

        cleaned = self.reaper.sweep(now=2000.0)

        self.assertEqual(cleaned, ["job-1:vol-s1"])
        self.lifecycle.cleanup_source_copy.assert_called_once()
        copy_arg, server_arg = self.lifecycle.cleanup_source_copy.call_args.args
        self.assertEqual(copy_arg.derived_volume_id, "vol-d1")
        self.assertEqual(copy_arg.snapshot_id, "snap-1")
        self.assertEqual(server_arg, "srv-s")

    def test_sweep_detaches_target_volume(self):
        self.ledger.all.return_value = [self._record(updated_at=1000.0)]

        self.reaper.sweep(now=2000.0)

        self.lifecycle.detach.assert_any_call(
            role="target", server_id="srv-t", volume_id="vol-t1"
        )

    def test_sweep_marks_record_cleaned_and_saves(self):
        record = self._record(updated_at=1000.0)
        self.ledger.all.return_value = [record]

        self.reaper.sweep(now=2000.0)

        self.assertEqual(record.phase, "cleaned")
        self.assertEqual(record.updated_at, 2000.0)
        self.ledger.upsert.assert_called_with(record)
        self.ledger.save.assert_called()

    def test_sweep_skips_terminal_records(self):
        self.ledger.all.return_value = [
            self._record(phase="done", updated_at=1000.0),
            self._record(phase="cleaned", updated_at=1000.0),
        ]

        self.assertEqual(self.reaper.sweep(now=2000.0), [])

    def test_sweep_continues_after_cleanup_error(self):
        self.lifecycle.cleanup_source_copy.side_effect = RuntimeError("boom")
        self.ledger.all.return_value = [
            self._record(volume_id="vol-a", updated_at=1000.0),
            self._record(volume_id="vol-b", updated_at=1000.0),
        ]

        cleaned = self.reaper.sweep(now=2000.0)

        # 清理失败不算已清理：两条都没清掉，但都尝试过，不会中断后续记录。
        self.assertEqual(cleaned, [])
        self.assertEqual(self.lifecycle.cleanup_source_copy.call_count, 2)
        phases = {
            call.args[0].volume_id: call.args[0].phase
            for call in self.ledger.upsert.call_args_list
        }
        self.assertEqual(phases, {"vol-a": "cleanup_failed", "vol-b": "cleanup_failed"})

    def test_cleanup_failure_is_retried_after_short_interval(self):
        record = self._record(phase="cleanup_failed", updated_at=1000.0)
        self.ledger.all.return_value = [record]

        # 距上次失败不足 300s：不重试
        self.assertEqual(self.reaper.sweep(now=1200.0), [])
        self.lifecycle.cleanup_source_copy.assert_not_called()

        # 超过 300s：重试，成功后标记 cleaned
        self.assertEqual(self.reaper.sweep(now=1400.0), ["job-1:vol-s1"])
        self.assertEqual(record.phase, "cleaned")

    def test_reconcile_job_cleans_every_unfinished_record(self):
        self.ledger.all.return_value = [
            self._record(updated_at=1000.0),
            self._record(volume_id="vol-s2", phase="done", updated_at=1000.0),
        ]

        cleaned = self.reaper.reconcile_job("job-1")

        self.assertEqual(cleaned, ["job-1:vol-s1"])
        self.lifecycle.cleanup_source_copy.assert_called_once()

    def test_reconcile_job_ignores_other_jobs(self):
        self.ledger.all.return_value = [
            self._record(job_id="job-2", updated_at=1000.0)
        ]

        self.assertEqual(self.reaper.reconcile_job("job-1"), [])

    def test_sweep_skips_records_from_other_clouds(self):
        self.ledger.all.return_value = [
            self._record(updated_at=1000.0, source_cloud="src-a", target_cloud="dst-a"),
            self._record(
                volume_id="vol-s2",
                updated_at=1000.0,
                source_cloud="src-b",
                target_cloud="dst-b",
            ),
        ]

        cleaned = self.reaper.sweep(now=2000.0, cloud_filter=("src-a", "dst-a"))

        self.assertEqual(cleaned, ["job-1:vol-s1"])
        self.lifecycle.cleanup_source_copy.assert_called_once()

    def test_real_ledger_round_trip(self):
        tmp = tempfile.TemporaryDirectory()
        ledger = Ledger.load(Path(tmp.name) / "ledger.json")
        ledger.upsert(self._record(updated_at=1000.0))
        reaper = RelayReaper(
            ledger=ledger,
            lifecycle=self.lifecycle,
            pools={},
            stale_seconds=600.0,
        )

        cleaned = reaper.sweep(now=2000.0)

        self.assertEqual(cleaned, ["job-1:vol-s1"])
        self.assertEqual(ledger.get("job-1", "vol-s1").phase, "cleaned")
        tmp.cleanup()

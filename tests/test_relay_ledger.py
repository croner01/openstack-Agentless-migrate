import json
import os
import tempfile
import unittest
from pathlib import Path

from relay_ledger import Ledger, VolumeTaskRecord


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-ledger-job-1.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_upsert_then_reload_roundtrips(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(
                job_id="job-1",
                vm_id="vm-1",
                volume_id="vol-1",
                phase="copying",
                copied_bytes=4096,
            )
        )
        ledger.save()

        record = Ledger.load(self.path).get("job-1", "vol-1")

        self.assertEqual(record.phase, "copying")
        self.assertEqual(record.copied_bytes, 4096)

    def test_ledger_persists_skipped_bytes(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(
                job_id="job-1",
                vm_id="vm-1",
                volume_id="vol-1",
                phase="copying",
                copied_bytes=4096,
                total_bytes=8192,
                skipped_bytes=2048,
            )
        )
        ledger.save()

        record = Ledger.load(self.path).get("job-1", "vol-1")

        self.assertEqual(record.total_bytes, 8192)
        self.assertEqual(record.skipped_bytes, 2048)

    def test_load_missing_file_returns_empty_ledger(self):
        self.assertEqual(Ledger.load(self.path).all(), [])

    def test_save_sets_0600_permissions(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(job_id="job-1", vm_id="vm-1", volume_id="vol-1")
        )
        ledger.save()

        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_upsert_updates_existing_record(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(job_id="job-1", vm_id="vm-1", volume_id="vol-1")
        )
        ledger.upsert(
            VolumeTaskRecord(
                job_id="job-1", vm_id="vm-1", volume_id="vol-1", phase="done"
            )
        )

        self.assertEqual(len(ledger.all()), 1)
        self.assertEqual(ledger.get("job-1", "vol-1").phase, "done")

    def test_save_leaves_no_temp_file(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(job_id="job-1", vm_id="vm-1", volume_id="vol-1")
        )
        ledger.save()
        ledger.save()

        leftovers = [
            path.name for path in self.path.parent.iterdir() if path.name != self.path.name
        ]
        self.assertEqual(leftovers, [])

    def test_reload_tolerates_unknown_future_fields(self):
        self.path.write_text(
            json.dumps(
                {
                    "records": [
                        {
                            "job_id": "job-1",
                            "vm_id": "vm-1",
                            "volume_id": "vol-1",
                            "future_field": "ignored",
                        }
                    ]
                }
            )
        )

        self.assertEqual(Ledger.load(self.path).get("job-1", "vol-1").phase, "queued")

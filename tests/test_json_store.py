"""公共 JSON 持久化库：原子性、0600 权限与旧文件兼容。"""
import json
import os
import stat
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from json_store import (
    atomic_write_json,
    dataclass_from_mapping,
    dump_dataclass_records,
    known_field_names,
    load_dataclass_records,
    read_json_object,
)


@dataclass
class _Record:
    name: str
    size: int = 0


class JsonStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "store.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_read_json_object_returns_empty_for_missing_or_empty_file(self):
        self.assertEqual(read_json_object(self.path), {})
        self.path.write_text("", encoding="utf-8")
        self.assertEqual(read_json_object(self.path), {})

    def test_read_json_object_rejects_non_object_top_level(self):
        self.path.write_text("[1, 2]", encoding="utf-8")
        with self.assertRaises(ValueError):
            read_json_object(self.path)

    def test_atomic_write_uses_0600_and_leaves_no_temp_files(self):
        atomic_write_json(self.path, {"a": 1})

        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        self.assertEqual(mode, 0o600)
        self.assertEqual(json.loads(self.path.read_text(encoding="utf-8")), {"a": 1})
        leftovers = [
            item.name for item in Path(self.tmp.name).iterdir() if item.suffix == ".tmp"
        ]
        self.assertEqual(leftovers, [])

    def test_failed_write_keeps_previous_content_and_cleans_temp(self):
        atomic_write_json(self.path, {"a": 1})

        with self.assertRaises(TypeError):
            atomic_write_json(self.path, {"a": object()})  # 不可序列化

        self.assertEqual(json.loads(self.path.read_text(encoding="utf-8")), {"a": 1})
        leftovers = [
            item.name for item in Path(self.tmp.name).iterdir() if item.suffix == ".tmp"
        ]
        self.assertEqual(leftovers, [])

    def test_dataclass_from_mapping_ignores_unknown_fields(self):
        record = dataclass_from_mapping(
            _Record, {"name": "n", "size": 5, "legacy_field": "x"}
        )

        self.assertEqual(record, _Record(name="n", size=5))
        self.assertEqual(known_field_names(_Record), {"name", "size"})

    def test_load_records_skips_corrupt_entries_but_keeps_the_rest(self):
        self.path.write_text(
            json.dumps(
                {
                    "records": [
                        {"name": "a", "size": 1},
                        "not-a-mapping",
                        {"name": "b", "size": 2, "future": True},
                    ]
                }
            ),
            encoding="utf-8",
        )

        records = load_dataclass_records(self.path, key="records", cls=_Record)

        self.assertEqual(records, [_Record("a", 1), _Record("b", 2)])

    def test_dump_dataclass_records_round_trips(self):
        payload = dump_dataclass_records([_Record("a", 1)], key="records")
        self.assertEqual(payload, {"records": [{"name": "a", "size": 1}]})


class StorePermissionRegressionTest(unittest.TestCase):
    """重构后各存储仍必须按 0600 落盘（改为公共实现时最容易丢掉的一环）。"""

    def test_ledger_lease_inventory_are_0600(self):
        from relay_inventory import NodeInventory, RelayNodeRecord
        from relay_ledger import Ledger, VolumeTaskRecord
        from relay_lease import LeaseStore

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            ledger = Ledger.load(base / "ledger.json")
            ledger.upsert(VolumeTaskRecord(job_id="j", vm_id="v", volume_id="vol"))
            ledger.save()

            leases = LeaseStore.load(base / "leases.json")
            leases.acquire(
                job_id="j", node_id="n", role="source", tenant_key="t", now=0.0
            )
            leases.save()

            inventory = NodeInventory.load(base / "nodes.json")
            inventory.upsert(
                RelayNodeRecord(
                    node_id="n", name="relay-source-0", role="source",
                    tenant_key="t", az="nova-1",
                )
            )
            inventory.save()

            for name in ("ledger.json", "leases.json", "nodes.json"):
                mode = stat.S_IMODE(os.stat(base / name).st_mode)
                self.assertEqual(mode, 0o600, f"{name} 权限应为 0600")


if __name__ == "__main__":
    unittest.main()

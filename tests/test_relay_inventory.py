import os
import stat
import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord


def _node(node_id="n1", *, tenant="t1", role="source", az="nova-1", state="ready"):
    return RelayNodeRecord(
        node_id=node_id,
        name=f"relay-{role}-{node_id}",
        role=role,
        tenant_key=tenant,
        az=az,
        server_id=f"server-{node_id}",
        state=state,
    )


class NodeInventoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-nodes.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_upsert_then_reload_roundtrips(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node())
        inventory.save()

        record = NodeInventory.load(self.path).get("n1")

        self.assertEqual(record.server_id, "server-n1")
        self.assertEqual(record.pool_key, ("t1", "source", "nova-1"))

    def test_load_missing_file_returns_empty(self):
        self.assertEqual(NodeInventory.load(self.path).all(), [])

    def test_save_sets_0600_permissions(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node())
        inventory.save()
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)

    def test_nodes_in_pool_filters_all_three_keys(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node("n1"))
        inventory.upsert(_node("n2", role="target"))
        inventory.upsert(_node("n3", az="nova-2"))
        inventory.upsert(_node("n4", tenant="t2"))

        self.assertEqual(
            [node.node_id for node in inventory.nodes_in_pool("t1", "source", "nova-1")],
            ["n1"],
        )

    def test_count_for_role_sums_across_az(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node("n1"))
        inventory.upsert(_node("n2", az="nova-2"))
        inventory.upsert(_node("n3", role="target"))

        self.assertEqual(inventory.count_for_role("t1", "source"), 2)
        self.assertEqual(inventory.count_for_role("t1", "target"), 1)

    def test_remove_returns_record_and_saves(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node())

        removed = inventory.remove("n1")
        inventory.save()

        self.assertEqual(removed.node_id, "n1")
        self.assertIsNone(NodeInventory.load(self.path).get("n1"))

    def test_load_ignores_unknown_fields(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node())
        inventory.save()
        payload = self.path.read_text(encoding="utf-8")
        self.path.write_text(
            payload.replace('"server_id": "server-n1"', '"server_id": "server-n1", "future": 1'),
            encoding="utf-8",
        )

        record = NodeInventory.load(self.path).get("n1")

        self.assertEqual(record.server_id, "server-n1")

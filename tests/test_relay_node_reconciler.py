import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_reaper import NodeReconciler


class NodeReconcilerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.leases = LeaseStore.load(base / "leases.json")
        self.ledger = mock.MagicMock()
        self.ledger.all.return_value = []
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n1",
                name="relay-source-1",
                role="source",
                tenant_key="t1",
                az="nova-1",
                server_id="server-1",
                state="ready",
            )
        )
        self.os_utils = mock.MagicMock()
        self.reconciler = NodeReconciler(
            inventory=self.inventory,
            leases=self.leases,
            ledger=self.ledger,
            os_utils_factory=lambda auth: self.os_utils,
            credentials=mock.MagicMock(
                get=lambda tenant: {"auth_url": "http://k"}
            ),
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_detaches_attachment_not_in_active_lease(self):
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-orphan", "attachment_id": "att-1"}
        ]

        detached = self.reconciler.sweep()

        self.assertEqual(detached, ["vol-orphan"])
        self.os_utils.detach_volume.assert_called_once_with("server-1", "vol-orphan")

    def test_keeps_attachment_belonging_to_active_lease(self):
        self.leases.acquire(
            job_id="job-1",
            node_id="n1",
            role="source",
            tenant_key="t1",
            volume_id="vol-live",
        )
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-live", "attachment_id": "att-1"}
        ]

        detached = self.reconciler.sweep()

        self.assertEqual(detached, [])
        self.os_utils.detach_volume.assert_not_called()

    def test_keeps_attachment_tracked_by_ledger(self):
        record = mock.MagicMock()
        record.phase = "copying"
        record.source_relay_id = "server-1"
        record.target_relay_id = ""
        record.derived_volume_id = "vol-derived"
        record.target_volume_id = ""
        self.ledger.all.return_value = [record]
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-derived", "attachment_id": "att-1"}
        ]

        detached = self.reconciler.sweep()

        self.assertEqual(detached, [])

    def test_skips_nodes_without_server_id(self):
        record = self.inventory.get("n1")
        record.server_id = ""
        self.inventory.upsert(record)

        self.assertEqual(self.reconciler.sweep(), [])
        self.os_utils.list_server_volume_attachments.assert_not_called()

    def test_skips_nodes_without_credentials(self):
        self.reconciler.credentials.get = lambda tenant: None

        self.assertEqual(self.reconciler.sweep(), [])
        self.os_utils.list_server_volume_attachments.assert_not_called()

    def test_detach_failure_does_not_break_other_nodes(self):
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n2",
                name="relay-source-2",
                role="source",
                tenant_key="t1",
                az="nova-1",
                server_id="server-2",
                state="ready",
            )
        )
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-orphan", "attachment_id": "att-1"}
        ]
        self.os_utils.detach_volume.side_effect = RuntimeError("boom")

        self.assertEqual(self.reconciler.sweep(), [])

    def test_never_detaches_node_root_volume(self):
        """节点系统盘不属于任何租约/台账，但绝不能当孤儿卸载。"""
        record = self.inventory.get("n1")
        record.root_volume_id = "vol-root"
        self.inventory.upsert(record)
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-root", "attachment_id": "att-root"}
        ]

        detached = self.reconciler.sweep()

        self.assertEqual(detached, [])
        self.os_utils.detach_volume.assert_not_called()

    def test_root_volume_skipped_even_with_orphan_present(self):
        """同一节点上根盘与真孤儿并存时，只卸孤儿。"""
        record = self.inventory.get("n1")
        record.root_volume_id = "vol-root"
        self.inventory.upsert(record)
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-root", "attachment_id": "att-root"},
            {"volume_id": "vol-orphan", "attachment_id": "att-1"},
        ]

        detached = self.reconciler.sweep()

        self.assertEqual(detached, ["vol-orphan"])
        self.os_utils.detach_volume.assert_called_once_with("server-1", "vol-orphan")

    def test_root_device_refusal_is_skipped_without_error_traceback(self):
        """清单没记 root_volume_id 时，Nova 的根设备拒绝也不该打 ERROR 堆栈。"""
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-actually-root", "attachment_id": "att-1"}
        ]
        self.os_utils.detach_volume.side_effect = Exception(
            "BadRequestException: 400: Client Error for url: http://nova/v2.1/x/"
            "servers/s/os-volume_attachments/v, Cannot detach a root device volume"
        )

        with mock.patch("relay_reaper.logging") as log:
            detached = self.reconciler.sweep()

        self.assertEqual(detached, [])
        log.exception.assert_not_called()
        log.warning.assert_called()

    def test_genuine_detach_failure_still_logs_error(self):
        self.os_utils.list_server_volume_attachments.return_value = [
            {"volume_id": "vol-orphan", "attachment_id": "att-1"}
        ]
        self.os_utils.detach_volume.side_effect = RuntimeError("boom")

        with mock.patch("relay_reaper.logging") as log:
            detached = self.reconciler.sweep()

        self.assertEqual(detached, [])
        log.exception.assert_called()

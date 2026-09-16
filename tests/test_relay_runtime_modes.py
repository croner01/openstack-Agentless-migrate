import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_runtime import PoolConfig, RelayChannelConfig, RelayRuntime


class FakeRegistry:
    def find_by_name(self, name):
        return None


def _config(mode):
    return RelayChannelConfig(
        platform_url="https://migrate.example.com",
        source=PoolConfig(
            size=2, image="img", flavor="flv", az="nova-1", network="net"
        ),
        target=PoolConfig(
            size=2, image="img", flavor="flv", az="nova-1", network="net"
        ),
        node_mode=mode,
    )


class RelayRuntimeModeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.leases = LeaseStore.load(base / "leases.json")

    def tearDown(self):
        self.tmp.cleanup()

    def test_ephemeral_mode_keeps_pool_behaviour(self):
        runtime = RelayRuntime(
            config=_config("ephemeral"),
            source_os=mock.MagicMock(),
            target_os=mock.MagicMock(),
            state=mock.MagicMock(),
            ledger=mock.MagicMock(),
            job_id="job-1",
        )
        runtime.source_pool.destroy = mock.MagicMock()
        runtime.target_pool.destroy = mock.MagicMock()
        runtime.reaper.reconcile_job = mock.MagicMock(return_value=[])

        runtime.finish()

        runtime.source_pool.destroy.assert_called_once()
        runtime.target_pool.destroy.assert_called_once()

    def test_persistent_mode_releases_leases_without_destroying_nodes(self):
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n1",
                name="relay-source-t1-nova-1-1",
                role="source",
                tenant_key="t1",
                az="nova-1",
                server_id="server-1",
                slots_total=5,
                state="ready",
            )
        )
        runtime = RelayRuntime(
            config=_config("persistent"),
            source_os=mock.MagicMock(),
            target_os=mock.MagicMock(),
            state=mock.MagicMock(),
            ledger=mock.MagicMock(),
            job_id="job-1",
            inventory=self.inventory,
            leases=self.leases,
            registry=FakeRegistry(),
        )
        runtime.reaper.reconcile_job = mock.MagicMock(return_value=[])
        self.leases.acquire(
            job_id="job-1", node_id="n1", role="source", tenant_key="t1"
        )

        runtime.finish()

        self.assertEqual(self.leases.active_for_job("job-1"), [])
        self.assertIsNotNone(self.inventory.get("n1"))
        self.assertFalse(runtime.source_pool.nodes)

    def test_persistent_start_does_not_provision_ephemeral_pool(self):
        runtime = RelayRuntime(
            config=_config("persistent"),
            source_os=mock.MagicMock(),
            target_os=mock.MagicMock(),
            state=mock.MagicMock(),
            ledger=mock.MagicMock(),
            job_id="job-1",
            inventory=self.inventory,
            leases=self.leases,
            registry=FakeRegistry(),
        )
        runtime.source_pool.provision = mock.MagicMock()
        runtime.target_pool.provision = mock.MagicMock()

        runtime.start()

        runtime.source_pool.provision.assert_not_called()
        runtime.target_pool.provision.assert_not_called()

    def test_persistent_start_requires_inventory(self):
        runtime = RelayRuntime(
            config=_config("persistent"),
            source_os=mock.MagicMock(),
            target_os=mock.MagicMock(),
            state=mock.MagicMock(),
            ledger=mock.MagicMock(),
            job_id="job-1",
        )

        with self.assertRaises(ValueError):
            runtime.start()

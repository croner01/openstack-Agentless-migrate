"""常驻中转机"被自己删掉重建"的三条回归：僵尸会话、粘性 unhealthy、占用保护。

现场表现：常驻池的机器会莫名其妙被强删重建，配额不足时甚至建不回来，池子
凭空少一台，随后作业一直卡在"没有可用槽位"。
"""
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from flask import Flask

import app as app_module
from relay_api import create_blueprint
from relay_inventory import NodeInventory, RelayNodeRecord, touch_node
from relay_protocol import issue_token
from relay_registry import RelayState

NAME = "relay-source-t1-nova-1-1"


class SupersedeSessionTest(unittest.TestCase):
    """同一个 name 只允许有一条活会话：否则旧会话会把同名节点判死。"""

    def setUp(self):
        self.state = RelayState(
            secret=b"s" * 32, heartbeat_interval=10, heartbeat_timeout=30
        )

    def _register(self, now: float, node_id: str = "n1"):
        return self.state.register(
            job_id="",
            role="source",
            name=NAME,
            version="1.1.0",
            address="10.0.0.9",
            now=now,
            node_id=node_id,
        )

    def test_reregister_drops_previous_session(self):
        old = self._register(1000.0)

        new = self._register(1030.0)

        self.assertIsNone(self.state.by_session(old.session_id))
        self.assertEqual(self.state.find_by_name(NAME).agent_id, new.agent_id)
        self.assertEqual(len(self.state.agents()), 1)

    def test_stale_agent_of_registered_node_is_not_swept(self):
        """重新注册后，巡检不应再看到一条永远超时的同名旧会话。"""
        self._register(1000.0, node_id="n1")
        live = self._register(1030.0, node_id="n1")
        self.state.heartbeat(live.session_id, now=1300.0)

        # 只有"当前会话"存在：旧会话没有留下一条永远超时的记录。
        self.assertEqual(self.state.sweep(now=1300.0), [])
        self.assertEqual(len(self.state.agents()), 1)

    def test_prune_stale_keeps_agent_with_running_tasks(self):
        idle = self._register(1000.0, node_id="n1")
        busy = self.state.register(
            job_id="",
            role="source",
            name="relay-source-t1-nova-1-2",
            version="1.1.0",
            address="10.0.0.9",
            now=1000.0,
            node_id="n2",
        )
        busy.running_tasks.append("task-1")

        dropped = self.state.prune_stale(now=1000.0 + 3600, max_age=600.0)

        self.assertEqual(dropped, [idle.agent_id])
        self.assertIsNone(self.state.by_session(idle.session_id))
        self.assertIsNotNone(self.state.by_session(busy.session_id))


class TouchInventoryTest(unittest.TestCase):
    """心跳必须能撤销被误标的 unhealthy，并按 node_id 精确命中。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n1", name=NAME, role="source", tenant_key="t1", az="nova-1",
                state="unhealthy", slots_used=0, updated_at=0.0,
            )
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_heartbeat_resets_unhealthy_to_ready(self):
        record = touch_node(self.inventory, node_id="n1", now=100.0)

        self.assertEqual(record.state, "ready")
        self.assertEqual(record.last_seen, 100.0)

    def test_heartbeat_restores_busy_node_to_busy(self):
        record = self.inventory.get("n1")
        record.slots_used = 2

        touch_node(self.inventory, node_id="n1", now=100.0)

        self.assertEqual(record.state, "busy")

    def test_node_id_wins_over_same_named_record(self):
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n2", name=NAME, role="source", tenant_key="t1", az="nova-1",
                state="unhealthy", updated_at=0.0,
            )
        )

        touch_node(self.inventory, node_id="n2", name=NAME, now=100.0)

        self.assertEqual(self.inventory.get("n2").state, "ready")
        self.assertEqual(self.inventory.get("n1").state, "unhealthy")

    def test_unknown_node_id_does_not_touch_same_named_record(self):
        """node_id 查不到时不能退回按名字改状态：那是另一台机器。"""
        record = touch_node(
            self.inventory, node_id="other-node", name=NAME, now=100.0
        )

        self.assertIsNone(record)
        self.assertEqual(self.inventory.get("n1").state, "unhealthy")

    def test_old_agent_without_node_id_falls_back_to_name(self):
        record = touch_node(self.inventory, node_id="", name=NAME, now=100.0)

        self.assertEqual(record.state, "ready")

    def test_does_not_touch_draining_node(self):
        record = self.inventory.get("n1")
        record.state = "draining"

        touch_node(self.inventory, node_id="n1", now=100.0)

        self.assertEqual(record.state, "draining")


class RelayHeartbeatApiTest(unittest.TestCase):
    """心跳接口要把清单记录一起复位——巡检判死只认清单里的 state。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n1", name=NAME, role="source", tenant_key="t1", az="nova-1",
                state="unhealthy", slots_used=0, updated_at=0.0,
            )
        )
        self.state = RelayState(
            secret=b"s" * 32, heartbeat_interval=10, heartbeat_timeout=30
        )
        self.app = Flask(__name__)
        self.app.register_blueprint(
            create_blueprint(self.state, inventory=self.inventory)
        )
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def _register(self, node_id="n1", name=NAME):
        response = self.client.post(
            "/api/relay/register",
            json={
                "token": issue_token(
                    b"s" * 32, "job-1", "source", now=time.time(), ttl=300
                ),
                "name": name,
                "agent_version": "1.1.0",
                "node_id": node_id,
            },
        )
        self.assertEqual(response.status_code, 200)
        return response.get_json()["session_id"]

    def test_register_records_node_id_and_marks_node_ready(self):
        session = self._register()

        agent = self.state.by_session(session)
        self.assertEqual(agent.node_id, "n1")
        self.assertEqual(self.inventory.get("n1").state, "ready")

    def test_heartbeat_resets_unhealthy_record(self):
        session = self._register()
        record = self.inventory.get("n1")
        record.state = "unhealthy"          # 模拟巡检误标
        record.updated_at = 0.0

        response = self.client.post(
            "/api/relay/heartbeat", json={"session_id": session}
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(record.state, "ready")
        self.assertGreater(record.last_seen, 0.0)


class RelayRebuildGuardTest(unittest.TestCase):
    """巡检重建的三条约束：按 node_id 匹配、占用不删、失败退避。"""

    def _record(self, **overrides):
        kwargs = {
            "node_id": "n1",
            "state": "unhealthy",
            "updated_at": 0.0,
            "slots_used": 0,
            "rebuild_backoff_until": 0.0,
        }
        kwargs.update(overrides)
        return RelayNodeRecord(
            tenant_key="t1", role="source", az="nova-1", name=NAME, **kwargs
        )

    def _sweep(
        self, record, *, now, rebuild_side_effect=None, leases=None, live_agent=None
    ):
        node_manager = mock.MagicMock()
        if rebuild_side_effect is not None:
            node_manager.rebuild.side_effect = rebuild_side_effect
        scheduler = mock.MagicMock()
        scheduler.scale_down.return_value = []
        layer = mock.MagicMock()
        layer.ready = True
        layer.node_manager = node_manager
        layer.scheduler = scheduler
        layer.reconciler = None
        store = mock.MagicMock()
        store.active_for_node.return_value = list(leases or [])
        with mock.patch.object(app_module, "RELAY_RESOURCES", layer), \
             mock.patch.object(app_module.RELAY_INVENTORY, "get",
                               side_effect=lambda node_id: (
                                   record if node_id == record.node_id else None)), \
             mock.patch.object(app_module.RELAY_INVENTORY, "all",
                               return_value=[record]), \
             mock.patch.object(app_module.RELAY_INVENTORY, "upsert"), \
             mock.patch.object(app_module.RELAY_INVENTORY, "save"), \
             mock.patch.object(app_module, "RELAY_LEASES", store), \
             mock.patch.object(app_module, "reap_relay_leases", return_value=[]), \
             mock.patch.object(app_module.RELAY_STATE, "sweep", return_value=[]), \
             mock.patch.object(app_module.RELAY_STATE, "find_by_name",
                               return_value=live_agent), \
             mock.patch.object(app_module.RELAY_STATE, "prune_stale",
                               return_value=[]):
            result = app_module.sweep_relay_resources(now=now)
        return result, node_manager, store

    def test_busy_node_is_not_rebuilt(self):
        record = self._record(slots_used=3)

        result, node_manager, _ = self._sweep(record, now=10000.0)

        node_manager.rebuild.assert_not_called()
        self.assertNotIn("rebuild:n1", result)
        self.assertEqual(record.state, "unhealthy")

    def test_active_lease_blocks_rebuild(self):
        record = self._record()

        _, node_manager, _ = self._sweep(record, now=10000.0, leases=["lease-1"])

        node_manager.rebuild.assert_not_called()

    def test_idle_unhealthy_node_is_rebuilt(self):
        record = self._record()

        result, node_manager, _ = self._sweep(record, now=10000.0)

        node_manager.rebuild.assert_called_once_with("n1")
        self.assertIn("rebuild:n1", result)

    def test_recovered_heartbeat_cancels_rebuild(self):
        """标记之后心跳恢复了：撤销误判，不删正在健康服务的机器。"""
        record = self._record(slots_used=2)
        live = mock.Mock()
        live.state = "ready"
        live.last_heartbeat = 9999.0

        with mock.patch.object(app_module.RELAY_STATE, "heartbeat_timeout", 30):
            result, node_manager, _ = self._sweep(
                record, now=10000.0, live_agent=live
            )

        node_manager.rebuild.assert_not_called()
        self.assertEqual(record.state, "busy")
        self.assertNotIn("rebuild:n1", result)

    def test_stale_heartbeat_does_not_cancel_rebuild(self):
        """会话还在但心跳早过期，不算恢复。"""
        record = self._record()
        stale = mock.Mock()
        stale.state = "ready"
        stale.last_heartbeat = 1000.0

        with mock.patch.object(app_module.RELAY_STATE, "heartbeat_timeout", 30):
            _, node_manager, _ = self._sweep(record, now=10000.0, live_agent=stale)

        node_manager.rebuild.assert_called_once_with("n1")

    def test_failed_rebuild_sets_backoff(self):
        record = self._record()

        self._sweep(
            record,
            now=10000.0,
            rebuild_side_effect=RuntimeError("quota exceeded"),
        )

        self.assertGreater(record.rebuild_backoff_until, 10000.0)

    def test_backoff_window_skips_retry(self):
        record = self._record(rebuild_backoff_until=20000.0)

        _, node_manager, _ = self._sweep(record, now=10000.0)

        node_manager.rebuild.assert_not_called()


class RelayHeartbeatSettingsTest(unittest.TestCase):
    """心跳参数是平台级配置，且 timeout 至少留 3 个周期。"""

    def test_timeout_is_at_least_three_intervals(self):
        with mock.patch.dict(
            app_module.os.environ,
            {
                "MIGRATION_RELAY_HEARTBEAT_INTERVAL": "10",
                "MIGRATION_RELAY_HEARTBEAT_TIMEOUT": "15",
            },
        ):
            interval, timeout = app_module.relay_heartbeat_settings()

        self.assertEqual(interval, 10)
        self.assertEqual(timeout, 30)

    def test_explicit_values_are_kept(self):
        with mock.patch.dict(
            app_module.os.environ,
            {
                "MIGRATION_RELAY_HEARTBEAT_INTERVAL": "20",
                "MIGRATION_RELAY_HEARTBEAT_TIMEOUT": "120",
            },
        ):
            self.assertEqual(app_module.relay_heartbeat_settings(), (20, 120))

    def test_bad_values_fall_back_to_defaults(self):
        with mock.patch.dict(
            app_module.os.environ,
            {
                "MIGRATION_RELAY_HEARTBEAT_INTERVAL": "10m",
                "MIGRATION_RELAY_HEARTBEAT_TIMEOUT": "",
            },
        ):
            self.assertEqual(app_module.relay_heartbeat_settings(), (10, 30))


class RelayHeartbeatSettingsRenderTest(unittest.TestCase):
    """设置页要能看到两个平台级心跳值，否则现场只能靠环境变量猜。"""

    def setUp(self):
        self.html = app_module.app.test_client().get("/").get_data(as_text=True)

    def test_settings_page_shows_relay_heartbeat(self):
        self.assertIn('id="set-relay-heartbeat"', self.html)
        self.assertIn('id="set-relay-rebuild"', self.html)
        self.assertIn("MIGRATION_RELAY_HEARTBEAT_INTERVAL/TIMEOUT", self.html)
        self.assertIn("MIGRATION_RELAY_REBUILD_SECONDS", self.html)

    def test_missing_field_renders_placeholder_instead_of_undefined(self):
        self.assertIn("rt.relay_heartbeat_interval == null", self.html)
        self.assertIn("rt.relay_rebuild_seconds == null", self.html)


if __name__ == "__main__":
    unittest.main()

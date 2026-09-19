import time
import unittest
from unittest import mock

from relay_pool import RelayPool, build_cloud_init
from relay_registry import RelayState


class CloudInitTest(unittest.TestCase):
    def test_baked_cloud_init_starts_preinstalled_service(self):
        text = build_cloud_init(
            platform_url="https://platform.example.com",
            token="tok-1",
            job_id="job-1",
            role="source",
            name="relay-source-0",
            bootstrap=False,
        )

        self.assertIn("RELAY_PLATFORM_URL=https://platform.example.com", text)
        self.assertIn("RELAY_TOKEN=tok-1", text)
        self.assertIn("RELAY_JOB_ID=job-1", text)
        self.assertIn("RELAY_ROLE=source", text)
        self.assertIn("RELAY_NAME=relay-source-0", text)
        self.assertIn("systemctl", text)

    def test_cloud_init_sets_guest_hostname(self):
        text = build_cloud_init(
            platform_url="https://platform.example.com",
            token="tok-1",
            job_id="job-1",
            role="target",
            name="relay-target-1",
            bootstrap=False,
        )

        self.assertIn("hostname: relay-target-1", text)
        self.assertIn("preserve_hostname: false", text)

    def test_bootstrap_cloud_init_fetches_installer(self):
        text = build_cloud_init(
            platform_url="https://platform.example.com",
            token="tok-1",
            job_id="job-1",
            role="target",
            name="relay-target-0",
        )

        self.assertIn("RELAY_ROLE=target", text)
        self.assertIn(
            "https://platform.example.com/api/relay/bootstrap?token=tok-1", text
        )


class RelayPoolTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(secret=b"secret")
        self.os_utils = mock.MagicMock()
        self.os_utils.create_relay_server.return_value = mock.Mock(id="srv-1")
        self.pool = RelayPool(
            role="source",
            job_id="job-1",
            az="az1",
            size=2,
            image_id="img-1",
            flavor_id="flv-1",
            port_ids=["port-1", "port-2"],
            admin_password="pw",
            platform_url="https://platform.example.com",
            token_factory=lambda job_id, role: f"{job_id}-{role}-token",
            os_utils=self.os_utils,
            state=self.state,
        )

    def _register_all(self):
        for node in self.pool.nodes:
            self.state.register(
                job_id="job-1",
                role="source",
                name=node.name,
                version="1.0.0",
                address="198.51.100.9",
                now=time.time(),
                data_address="10.0.0.9",
                data_port=9200,
            )

    def _make_ready(self):
        self.pool.provision()
        self._register_all()
        self.pool.wait_ready(timeout=1.0, poll_interval=0.01)

    def test_provision_creates_one_server_per_slot(self):
        self.pool.provision()

        self.assertEqual(self.os_utils.create_relay_server.call_count, 2)
        self.assertEqual(
            [node.name for node in self.pool.nodes],
            ["relay-source-0", "relay-source-1"],
        )
        kwargs = self.os_utils.create_relay_server.call_args_list[0].kwargs
        self.assertIn("RELAY_ROLE=source", kwargs["user_data"])
        self.assertEqual(kwargs["image_id"], "img-1")
        self.assertEqual(kwargs["port_ids"], ["port-1"])
        self.assertEqual(
            self.os_utils.create_relay_server.call_args_list[1].kwargs["port_ids"],
            ["port-2"],
        )

    def test_provision_rejects_port_count_mismatch(self):
        self.pool.port_ids = ["port-1"]

        with self.assertRaises(ValueError):
            self.pool.provision()

    def test_wait_ready_binds_session_and_data_address(self):
        self._make_ready()

        for node in self.pool.nodes:
            self.assertEqual(node.state, "ready")
            self.assertTrue(node.session_id)
            self.assertEqual(node.data_address, "10.0.0.9")
            self.assertEqual(node.data_port, 9200)

    def test_wait_ready_times_out_when_agent_missing(self):
        self.pool.provision()

        with self.assertRaises(TimeoutError):
            self.pool.wait_ready(timeout=0.05, poll_interval=0.01)

    def test_wait_ready_records_agent_version(self):
        self.pool.provision()
        for node in self.pool.nodes:
            self.state.register(
                job_id="job-1",
                role="source",
                name=node.name,
                version="1.1.0",
                address="198.51.100.9",
                now=time.time(),
                data_address="10.0.0.9",
            )

        self.pool.wait_ready(timeout=1.0, poll_interval=0.01)

        for node in self.pool.nodes:
            self.assertEqual(node.agent_version, "1.1.0")

    def test_acquire_returns_first_idle_node_and_marks_busy(self):
        self._make_ready()

        first = self.pool.acquire("t-1")
        second = self.pool.acquire("t-2")
        third = self.pool.acquire("t-3")

        self.assertEqual(first.name, "relay-source-0")
        self.assertEqual(first.state, "busy")
        self.assertEqual(second.name, "relay-source-1")
        self.assertIsNone(third)

    def test_release_returns_node_to_ready(self):
        self._make_ready()
        node = self.pool.acquire("t-1")

        self.pool.release(node.node_id)

        self.assertEqual(node.state, "ready")
        self.assertEqual(node.current_task_id, "")
        self.assertIsNotNone(self.pool.acquire("t-2"))

    def test_sweep_marks_unhealthy_when_agent_stale(self):
        self._make_ready()
        for agent in self.state.agents():
            agent.last_heartbeat = 1000.0

        changed = self.pool.sweep(now=10_000.0)

        self.assertEqual(len(changed), 2)
        self.assertTrue(all(node.state == "unhealthy" for node in self.pool.nodes))

    def test_sweep_recovers_node_when_heartbeat_returns(self):
        self._make_ready()
        for agent in self.state.agents():
            agent.last_heartbeat = time.time() - 1000.0
        self.pool.sweep(now=time.time())

        # agent 恢复心跳：不能继续把节点当成不可用
        for agent in self.state.agents():
            agent.last_heartbeat = time.time()
            agent.state = "ready"
        changed = self.pool.sweep(now=time.time())

        self.assertEqual(len(changed), 2)
        self.assertTrue(all(node.state == "ready" for node in self.pool.nodes))
        self.assertIsNotNone(self.pool.acquire("t-1"))

    def test_acquire_revives_unhealthy_node_with_live_agent(self):
        self._make_ready()
        for node in self.pool.nodes:
            node.state = "unhealthy"

        # 没有等到下一轮 sweep 也能拿到节点，避免一次抖动导致整轮失败
        self.assertIsNotNone(self.pool.acquire("t-1"))

    def test_destroy_deletes_every_server(self):
        self.pool.provision()

        self.pool.destroy()

        self.assertEqual(self.os_utils.delete_server.call_count, 2)
        self.assertEqual(self.pool.nodes, [])

    def test_rebuild_replaces_node_keeping_slot_name(self):
        self._make_ready()
        old = self.pool.nodes[0]
        self.os_utils.create_relay_server.return_value = mock.Mock(id="srv-new")

        new = self.pool.rebuild(old.node_id)

        self.assertEqual(new.name, old.name)
        self.assertNotEqual(new.node_id, old.node_id)
        self.assertEqual(new.server_id, "srv-new")
        self.assertEqual(self.os_utils.delete_server.call_args.args, ("srv-1",))
        self.assertIsNone(self.state.find_by_name(old.name))

    def test_rebuild_unknown_node_returns_none(self):
        self.pool.provision()

        self.assertIsNone(self.pool.rebuild("nope"))

    def test_sweep_uses_configured_timeout(self):
        self._make_ready()
        self.pool.heartbeat_timeout = 5
        for agent in self.state.agents():
            agent.last_heartbeat = 1000.0

        changed = self.pool.sweep(now=1006.0)

        self.assertEqual(len(changed), 2)


class RelayPoolLazyGrowthTest(unittest.TestCase):
    """临时池按需扩容：初始只建表单配置的台数，用满后逐台扩到并发上限。"""

    def setUp(self):
        self.state = RelayState(secret=b"secret")
        self.os_utils = mock.MagicMock()
        self._created = 0

        def create(**_kwargs):
            self._created += 1
            return mock.Mock(id=f"srv-{self._created}")

        self.os_utils.create_relay_server.side_effect = create
        self.pool = RelayPool(
            role="target",
            job_id="job-1",
            az="az1",
            size=4,
            initial_size=1,
            image_id="img-1",
            flavor_id="flv-1",
            port_ids=["port-1", "port-2", "port-3", "port-4"],
            admin_password="pw",
            platform_url="https://platform.example.com",
            token_factory=lambda job_id, role: "tok",
            os_utils=self.os_utils,
            state=self.state,
        )
        self.pool.provision()
        self.state.register(
            job_id="job-1", role="target", name="relay-target-0", version="1.0.0",
            address="198.51.100.9", now=time.time(),
            data_address="10.0.0.9", data_port=9200,
        )
        self.pool.wait_ready(timeout=1.0, poll_interval=0.01)

    def test_provision_only_creates_initial_size(self):
        self.assertEqual(self.os_utils.create_relay_server.call_count, 1)

    def test_acquire_grows_one_node_when_full(self):
        node = self.pool.acquire("task-1")
        self.assertIsNotNone(node)

        # 池里只有一台且已被占用：下一次 acquire 触发扩容并返回 None（由上层排队）。
        self.assertIsNone(self.pool.acquire("task-2"))
        self.assertEqual(self.os_utils.create_relay_server.call_count, 2)
        self.assertEqual(len(self.pool.nodes), 2)
        # 扩容出来的机器用的是对应的端口，不会两台抢同一个端口。
        kwargs = self.os_utils.create_relay_server.call_args_list[1].kwargs
        self.assertEqual(kwargs["port_ids"], ["port-2"])

    def test_growth_stops_at_size_limit(self):
        self.pool.acquire("task-1")
        for index in range(2, 6):
            self.pool.acquire(f"task-{index}")

        self.assertEqual(len(self.pool.nodes), 4)
        self.assertEqual(self.os_utils.create_relay_server.call_count, 4)

    def test_registered_growth_node_becomes_usable(self):
        self.pool.acquire("task-1")
        self.pool.acquire("task-2")
        self.state.register(
            job_id="job-1", role="target", name="relay-target-1", version="1.0.0",
            address="198.51.100.10", now=time.time(),
            data_address="10.0.0.10", data_port=9200,
        )

        node = self.pool.acquire("task-2")

        self.assertIsNotNone(node)
        self.assertEqual(node.name, "relay-target-1")

    def test_has_live_holder_is_true_while_growth_is_possible(self):
        self.pool.acquire("task-1")
        # 已建的那台失联，但池还没扩满：继续排队是有意义的。
        self.state.drop_by_name("relay-target-0")

        self.assertTrue(self.pool.has_live_holder())

    def test_failed_growth_keeps_pool_waitable(self):
        self.os_utils.create_relay_server.side_effect = RuntimeError("quota exceeded")
        self.pool.acquire("task-1")

        self.assertIsNone(self.pool.acquire("task-2"))
        self.assertEqual(len(self.pool.nodes), 1)


class RelayPoolGrowthFailureTest(unittest.TestCase):
    """连续扩容失败后不能继续假装"迟早会有槽位"，否则不限时排队会永久挂起。"""

    def test_repeated_growth_failure_stops_live_holder(self):
        state = RelayState(secret=b"secret")
        os_utils = mock.MagicMock()
        # 初始那台要建得出来，之后扩容才开始失败：模拟"配额用完"。
        os_utils.create_relay_server.side_effect = [
            mock.Mock(id="srv-1"),
            RuntimeError("quota exceeded"),
            RuntimeError("quota exceeded"),
            RuntimeError("quota exceeded"),
            RuntimeError("quota exceeded"),
        ]
        pool = RelayPool(
            role="source",
            job_id="job-1",
            az="az1",
            size=3,
            initial_size=1,
            image_id="img-1",
            flavor_id="flv-1",
            port_ids=["port-1", "port-2", "port-3"],
            admin_password="pw",
            platform_url="https://platform.example.com",
            token_factory=lambda job_id, role: "tok",
            os_utils=os_utils,
            state=state,
        )
        pool.provision()

        for index in range(4):
            self.assertIsNone(pool.acquire(f"task-{index}"))

        self.assertFalse(pool.has_live_holder())

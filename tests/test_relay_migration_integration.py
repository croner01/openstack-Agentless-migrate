import os
import socket
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from relay_agent import AgentClient, execute_task
from relay_ledger import Ledger
from relay_orchestrator import RelayVolumeMover
from relay_pool import RelayNode
from relay_registry import RelayState
from relay_volumes import SourceCopy


class _StatePlatform:
    """把 RelayState 适配成 agent 期望的平台接口。"""

    def __init__(self, state):
        self.state = state

    def heartbeat(self, session_id):
        self.state.heartbeat(session_id, now=time.time())
        return {"cancel": False}

    def next_task(self, session_id):
        return self.state.dispatch(session_id)

    def report_progress(self, task_id, copied_bytes, skipped_bytes=0):
        return {"cancel": False}

    def report_ready(self, task_id):
        self.state.mark_task_ready(task_id)

    def report_result(
        self, task_id, status, copied_bytes, digest="", error="", skipped_bytes=0
    ):
        self.state.record_result(
            task_id, status, copied_bytes, digest=digest, error=error
        )
        for agent in self.state.agents():
            if agent.current_task_id == task_id:
                self.state.complete_task(agent.session_id)


class _Pool:
    def __init__(self, node):
        self.node = node
        self.released = []

    def acquire(self, task_id):
        node, self.node = self.node, None
        return node

    def release(self, node_id, *, lease_id=""):
        self.released.append(node_id)


class RelayMigrationIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name) / "src.bin"
        self.dst = Path(self.tmp.name) / "dst.bin"
        self.payload = os.urandom(128 * 1024)
        self.src.write_bytes(self.payload)
        self.dst.write_bytes(b"\x00" * len(self.payload))
        self.state = RelayState(secret=b"secret")
        self.src_agent = self.state.register(
            job_id="job-1", role="source", name="relay-source-0",
            version="1.0.0", address="10.0.0.7", now=1000.0,
            data_address="127.0.0.1",
        )
        self.dst_agent = self.state.register(
            job_id="job-1", role="target", name="relay-target-0",
            version="1.0.0", address="10.0.0.8", now=1000.0,
            data_address="127.0.0.1",
        )
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        self.src_node = RelayNode(
            node_id=self.src_agent.agent_id, name="relay-source-0", role="source",
            az="az1", server_id="srv-s", session_id=self.src_agent.session_id,
            data_address="127.0.0.1", data_port=self.port,
        )
        self.dst_node = RelayNode(
            node_id=self.dst_agent.agent_id, name="relay-target-0", role="target",
            az="az2", server_id="srv-t", session_id=self.dst_agent.session_id,
            data_address="127.0.0.1", data_port=self.port,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_two_agents_copy_one_volume_end_to_end(self):
        lifecycle = mock.MagicMock()
        lifecycle.create_source_copy.return_value = SourceCopy(
            snapshot_id="snap-1", derived_volume_id="vol-d1"
        )
        lifecycle.create_target_volume.return_value = "vol-t1"
        lifecycle.source_os.wait_attachment_device.return_value = str(self.src)
        lifecycle.target_os.wait_attachment_device.return_value = str(self.dst)
        ledger = Ledger.load(Path(self.tmp.name) / "ledger.json")
        platform = _StatePlatform(self.state)
        mover = RelayVolumeMover(
            source_pool=_Pool(self.src_node),
            target_pool=_Pool(self.dst_node),
            lifecycle=lifecycle,
            state=self.state,
            ledger=ledger,
            job_id="job-1",
            poll_interval=0.01,
            sleeper=time.sleep,
        )
        stop = threading.Event()

        def loop(session_id):
            client = AgentClient(platform, session_id=session_id)
            while not stop.is_set():
                task = client.next_task()
                if task is None:
                    time.sleep(0.01)
                    continue
                execute_task(task, client)

        threads = [
            threading.Thread(target=loop, args=(self.src_agent.session_id,)),
            threading.Thread(target=loop, args=(self.dst_agent.session_id,)),
        ]
        for thread in threads:
            thread.start()
        try:
            result = mover.move(
                # size 是 Cinder 的 GiB；这里用 size_bytes 给出精确字节数
                volume=mock.Mock(
                    source_volume_id="vol-s1",
                    size=1,
                    size_bytes=len(self.payload),
                ),
                vm_name="vm-1",
                index=0,
            )
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)

        self.assertEqual(self.dst.read_bytes(), self.payload)
        self.assertEqual(result["target_volume_id"], "vol-t1")
        self.assertEqual(ledger.get("job-1", "vol-s1").phase, "done")
        self.assertTrue(ledger.get("job-1", "vol-s1").copied_bytes)


class RelayPathBranchTest(unittest.TestCase):
    def setUp(self):
        from migration_manager import MigrationManager
        from state_machine import MigrationMode, VmTask

        self.VmTask = VmTask
        self.manager = MigrationManager(
            source_os=mock.MagicMock(),
            target_os=mock.MagicMock(),
            ceph_utils=mock.MagicMock(),
        )
        self.vm = VmTask(name="vm-1", target_az="az2", mode=MigrationMode.FULL)
        # 真实实现会回填 vm.source_server_id，Mock 也要模拟，否则覆盖键会退化成 vm_name。
        def fake_resolve_source(target_vm):
            target_vm.source_server_id = "srv-src"
            return mock.Mock(id="srv-src")

        self.manager._resolve_source_server = mock.Mock(
            side_effect=fake_resolve_source
        )
        self.manager.source_os.get_server_flavor_spec.return_value = {
            "name": "m1.small", "vcpus": 1, "ram": 2048, "disk": 40,
        }
        self.manager._resolve_flavor_id = mock.Mock(return_value="flv-1")
        self.manager._create_target_ports = mock.Mock(return_value=["port-1"])
        self.manager.source_os.get_server_volumes_with_device.return_value = [
            {
                "volume_id": "vol-s1", "size": 40, "device": "/dev/vda",
                "is_bootable": True, "bootable": True,
            },
            {
                "volume_id": "vol-s2", "size": 20, "device": "/dev/vdb",
                "is_bootable": False, "bootable": False,
            },
        ]
        self.manager._stop_and_wait = mock.Mock()
        self.mover = mock.MagicMock()
        # 单卷流程已拆成「准备（打快照/派生）→ 传输」，管理器的编排按这两步断言。
        self.mover.prepare.side_effect = [
            mock.Mock(index=0, target_volume_id="vol-t0"),
            mock.Mock(index=1, target_volume_id="vol-t1"),
        ]
        self.mover.transfer.side_effect = [
            {"target_volume_id": "vol-t0", "size": 40, "source_volume_id": "vol-s1"},
            {"target_volume_id": "vol-t1", "size": 20, "source_volume_id": "vol-s2"},
        ]
        self.mover_factory = mock.Mock(return_value=self.mover)
        self.manager.target_os.create_server_from_volumes.return_value = mock.Mock(
            id="srv-tgt"
        )

    def test_relay_path_moves_every_volume_and_boots_target(self):
        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
                "admin_password": "pw",
            },
        )

        self.assertEqual(self.mover.prepare.call_count, 2)
        self.assertEqual(self.mover.transfer.call_count, 2)
        first_call = self.mover.prepare.call_args_list[0].kwargs
        self.assertEqual(first_call["index"], 0)
        self.assertEqual(first_call["volume"].source_volume_id, "vol-s1")
        second_call = self.mover.prepare.call_args_list[1].kwargs
        self.assertEqual(second_call["volume"].source_volume_id, "vol-s2")
        kwargs = self.manager.target_os.create_server_from_volumes.call_args.kwargs
        self.assertEqual(kwargs["boot_volume_id"], "vol-t0")
        self.assertEqual(kwargs["data_volume_ids"], ["vol-t1"])
        # 云侧 create 后会自动开机；平台只等状态，不再插队调用 os-start。
        self.manager.target_os.wait_server_booted.assert_called_once_with("srv-tgt")
        self.manager.target_os.start_server.assert_not_called()
        self.assertEqual(self.vm.target_server_id, "srv-tgt")

    def test_relay_path_stops_source_before_copying(self):
        order = []
        self.manager._stop_and_wait = mock.Mock(
            side_effect=lambda *args, **kwargs: order.append("stop")
        )
        self.mover.prepare.side_effect = lambda **kwargs: (
            order.append("prepare"),
            mock.Mock(),
        )[1]
        self.mover.transfer.side_effect = lambda item: (
            order.append("transfer"),
            {"target_volume_id": "vol-t0", "size": 40},
        )[1]

        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
            },
        )

        self.assertEqual(order[0], "stop")
        self.assertIn("prepare", order)
        self.assertIn("transfer", order)

    def test_relay_path_finishes_every_prepare_before_transferring(self):
        """先把所有盘准备好再开始传数据：传输要占中转机槽位，早占只会白等。"""
        events = []
        self.mover.prepare.side_effect = lambda **kwargs: (
            events.append(("prepare", kwargs["index"])),
            mock.Mock(),
        )[1]
        self.mover.transfer.side_effect = lambda item: (
            events.append(("transfer",)),
            {"target_volume_id": "vol-t0", "size": 40},
        )[1]

        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
                "volume_concurrency": 2,
            },
        )

        kinds = [event[0] for event in events]
        self.assertEqual(kinds, ["prepare", "prepare", "transfer", "transfer"])
        self.assertEqual(sorted(e[1] for e in events if e[0] == "prepare"), [0, 1])

    def test_relay_path_prepares_volumes_concurrently(self):
        """打快照/派生卷这一段按「单台卷拷贝并发」并行，等待时间不再线性叠加。"""
        lock = threading.Lock()
        inflight: list[int] = []
        peak = [0]

        def fake_prepare(**kwargs):
            with lock:
                inflight.append(kwargs["index"])
                peak[0] = max(peak[0], len(inflight))
            barrier.wait(timeout=5)     # 两块盘必须同时在准备，否则超时失败
            with lock:
                inflight.remove(kwargs["index"])
            return mock.Mock()

        barrier = threading.Barrier(2)
        self.mover.prepare.side_effect = fake_prepare

        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
                "volume_concurrency": 2,
            },
        )

        self.assertEqual(peak[0], 2)
        self.assertEqual(self.mover.transfer.call_count, 2)

    def test_relay_path_prepare_stays_serial_by_default(self):
        lock = threading.Lock()
        inflight: list[int] = []
        peak = [0]

        def fake_prepare(**kwargs):
            with lock:
                inflight.append(kwargs["index"])
                peak[0] = max(peak[0], len(inflight))
            time.sleep(0.05)
            with lock:
                inflight.remove(kwargs["index"])
            return mock.Mock()

        self.mover.prepare.side_effect = fake_prepare

        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
            },
        )

        self.assertEqual(peak[0], 1)

    def test_relay_path_discards_prepared_volumes_when_one_fails(self):
        """一块盘准备失败就不能继续传输，已准备好的派生卷要回收。"""
        self.mover.prepare.side_effect = [
            mock.Mock(target_volume_id="vol-t0"),
            RuntimeError("快照超时"),
        ]

        with self.assertRaises(RuntimeError):
            self.manager._migrate_vm_inner(
                self.vm,
                {
                    "job_id": "job-1",
                    "data_channel": "relay",
                    "relay_mover_factory": self.mover_factory,
                    "volume_concurrency": 1,
                },
            )

        self.mover.transfer.assert_not_called()
        self.mover.discard.assert_called_once()

    def test_relay_path_waits_instead_of_starting_immediately(self):
        """create 返回后实例可能还不可见，立刻 os-start 会拿到 404 并把建机判失败。"""
        self.manager.target_os.wait_server_booted = mock.Mock(
            return_value=mock.Mock(id="srv-tgt", status="ACTIVE")
        )

        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
            },
        )

        self.manager.target_os.wait_server_booted.assert_called_once_with("srv-tgt")
        self.manager.target_os.start_server.assert_not_called()

    def test_relay_path_marks_boot_volume_bootable_before_booting(self):
        """空白启动盘在 Cinder 里 bootable=false，不置位会被 Nova 直接拒。"""
        order = []
        self.mover.lifecycle.mark_bootable.side_effect = (
            lambda **kwargs: order.append(("bootable", kwargs["volume_id"]))
        )
        self.manager.target_os.create_server_from_volumes.side_effect = (
            lambda **kwargs: (
                order.append(("create", kwargs["boot_volume_id"])),
                mock.Mock(id="srv-tgt"),
            )[1]
        )

        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
            },
        )

        self.assertEqual(order, [("bootable", "vol-t0"), ("create", "vol-t0")])

    def test_relay_path_passes_per_volume_type_override(self):
        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
                "volume_overrides": {
                    "srv-src": {
                        "vol-s1": {"source": "src-ssd", "target": "boot-ssd"},
                        "vol-s2": {"target": "data-hdd"},
                    }
                },
            },
        )

        first = self.mover.prepare.call_args_list[0].kwargs
        second = self.mover.prepare.call_args_list[1].kwargs
        self.assertEqual(first["source_volume_type"], "src-ssd")
        self.assertEqual(first["target_volume_type"], "boot-ssd")
        self.assertIsNone(second["source_volume_type"])
        self.assertEqual(second["target_volume_type"], "data-hdd")

    def test_relay_path_accepts_legacy_string_override(self):
        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
                "volume_overrides": {"srv-src": {"vol-s1": "boot-ssd"}},
            },
        )

        first = self.mover.prepare.call_args_list[0].kwargs
        self.assertEqual(first["target_volume_type"], "boot-ssd")

    def test_relay_path_omits_override_when_not_configured(self):
        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
            },
        )

        first = self.mover.prepare.call_args_list[0].kwargs
        self.assertIsNone(first["target_volume_type"])

    def test_relay_path_requires_mover_factory(self):
        with self.assertRaises(RuntimeError):
            self.manager._migrate_vm_inner(
                self.vm, {"job_id": "job-1", "data_channel": "relay"}
            )

    def test_rbd_channel_does_not_enter_relay_branch(self):
        self.manager.source_os.get_server_volumes_with_device.side_effect = (
            RuntimeError("沿 RBD 路径继续执行")
        )
        relay_branch = mock.Mock()

        with mock.patch.object(
            self.manager, "_migrate_vm_via_relay", relay_branch
        ):
            with self.assertRaises(RuntimeError):
                self.manager._migrate_vm_inner(
                    self.vm, {"job_id": "job-1", "data_channel": "rbd"}
                )

        relay_branch.assert_not_called()

    def test_relay_path_loads_network_overrides_before_ports(self):
        """页面上的网络映射必须在建端口前装载，否则会被误判为未配置。"""
        from migration_manager import MigrationManager

        # setUp 把 _create_target_ports 换成了 Mock，这里恢复真实实现。
        self.manager._create_target_ports = types.MethodType(
            MigrationManager._create_target_ports, self.manager
        )
        # 真实实现会回填 vm.source_server_id，Mock 也要模拟这一步。
        def fake_resolve(target_vm):
            target_vm.source_server_id = "srv-src"
            return mock.Mock(id="srv-src")

        self.manager._resolve_source_server = mock.Mock(side_effect=fake_resolve)
        self.manager.source_os.get_server_addresses.return_value = {
            "share_net": [{"addr": "192.168.111.67", "version": 4, "type": "fixed"}]
        }
        self.manager.target_os.find_network.return_value = mock.Mock(id="net-t")
        self.manager.target_os.find_subnet.return_value = mock.Mock(
            id="sub-t", cidr="10.1.0.0/24"
        )
        self.manager.target_os.create_port_with_fixed_ip.return_value = mock.Mock(
            id="port-new"
        )
        self.manager.source_os.get_server_volumes_with_device.return_value = [
            {
                "volume_id": "vol-s1",
                "size": 40,
                "device": "/dev/vda",
                "is_bootable": True,
                "bootable": True,
            }
        ]

        self.manager._migrate_vm_inner(
            self.vm,
            {
                "job_id": "job-1",
                "data_channel": "relay",
                "relay_mover_factory": self.mover_factory,
                "vm_network_overrides": {
                    "srv-src": [
                        {
                            "source_network": "share_net",
                            "source_ip": "192.168.111.67",
                            "target_network": "net-t",
                            "target_subnet": "sub-t",
                        }
                    ]
                },
            },
        )

        self.manager.target_os.create_port_with_fixed_ip.assert_called_once()

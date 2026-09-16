import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from relay_agent import AgentClient, execute_task, resolve_device, run_once


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class FakePlatform:
    """用内存队列模拟平台，避免测试依赖真实 HTTP 服务。"""

    def __init__(self, roles):
        self.roles = roles
        self.tasks = {"source": [], "target": []}
        self.progress = []
        self.results = []
        self.ready = []
        self.errors = []
        self.skipped = []
        self.cancel_requested = False

    def heartbeat(self, session_id):
        return {"cancel": False}

    def next_task(self, session_id):
        queue = self.tasks[self.roles[session_id]]
        return queue.pop(0) if queue else None

    def report_progress(self, task_id, copied_bytes, skipped_bytes=0):
        self.progress.append((task_id, copied_bytes))
        self.skipped.append(skipped_bytes)
        return {"cancel": self.cancel_requested}

    def report_result(
        self, task_id, status, copied_bytes, digest="", error="", skipped_bytes=0
    ):
        self.results.append((task_id, status, copied_bytes))
        self.errors.append(error)
        self.skipped.append(skipped_bytes)

    def report_ready(self, task_id):
        self.ready.append(task_id)


class ExecuteTaskTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name) / "src.bin"
        self.dst = Path(self.tmp.name) / "dst.bin"
        self.payload = os.urandom(150 * 1024)
        self.src.write_bytes(self.payload)
        self.dst.write_bytes(b"\x00" * len(self.payload))

    def tearDown(self):
        self.tmp.cleanup()

    def test_execute_target_then_source_copies_device(self):
        platform = FakePlatform({"s-src": "source", "s-dst": "target"})
        port = _free_port()
        ready = threading.Event()
        target_result = {}

        def run_target():
            target_result["value"] = execute_task(
                {
                    "task_id": "t-dst",
                    "role": "target",
                    "dst_path": str(self.dst),
                    "listen_host": "127.0.0.1",
                    "listen_port": port,
                    "ticket": "ticket-1",
                    "offset": 0,
                    "length": len(self.payload),
                    "chunk_size": 32 * 1024,
                },
                AgentClient(platform, session_id="s-dst"),
                ready_event=ready,
            )

        thread = threading.Thread(target=run_target)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        source_result = execute_task(
            {
                "task_id": "t-src",
                "role": "source",
                "src_path": str(self.src),
                "peer_host": "127.0.0.1",
                "peer_port": port,
                "ticket": "ticket-1",
                "offset": 0,
                "length": len(self.payload),
                "chunk_size": 32 * 1024,
            },
            AgentClient(platform, session_id="s-src"),
        )
        thread.join(timeout=10)

        self.assertEqual(self.dst.read_bytes(), self.payload)
        self.assertEqual(source_result["status"], "done")
        self.assertEqual(target_result["value"]["status"], "done")
        self.assertIn(("t-src", "done", len(self.payload)), platform.results)
        self.assertIn(("t-dst", "done", len(self.payload)), platform.results)

    def test_agent_version_supports_sparse(self):
        import relay_agent

        self.assertEqual(relay_agent.AGENT_VERSION, "1.1.0")

    def test_sparse_task_skips_zero_chunks_and_reports_skipped_bytes(self):
        zeros = b"\0" * (256 * 1024)
        self.src.write_bytes(zeros)
        self.dst.write_bytes(zeros)
        platform = FakePlatform({"s-src": "source", "s-dst": "target"})
        port = _free_port()
        ready = threading.Event()

        def run_target():
            execute_task(
                {
                    "task_id": "t-dst",
                    "role": "target",
                    "dst_path": str(self.dst),
                    "listen_host": "127.0.0.1",
                    "listen_port": port,
                    "ticket": "ticket-1",
                    "offset": 0,
                    "length": len(zeros),
                    "chunk_size": 64 * 1024,
                    "sparse": True,
                    "hole_mode": "skip",
                },
                AgentClient(platform, session_id="s-dst"),
                ready_event=ready,
            )

        thread = threading.Thread(target=run_target)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        execute_task(
            {
                "task_id": "t-src",
                "role": "source",
                "src_path": str(self.src),
                "peer_host": "127.0.0.1",
                "peer_port": port,
                "ticket": "ticket-1",
                "offset": 0,
                "length": len(zeros),
                "chunk_size": 64 * 1024,
                "sparse": True,
                "hole_mode": "skip",
            },
            AgentClient(platform, session_id="s-src"),
        )
        thread.join(timeout=10)

        self.assertEqual(self.dst.read_bytes(), zeros)
        self.assertEqual(max(platform.skipped), len(zeros))

    def test_execute_task_reports_failure_when_peer_refuses(self):
        platform = FakePlatform({"s-src": "source"})

        result = execute_task(
            {
                "task_id": "t-src",
                "role": "source",
                "src_path": str(self.src),
                "peer_host": "127.0.0.1",
                "peer_port": _free_port(),
                "ticket": "ticket-1",
                "offset": 0,
                "length": len(self.payload),
            },
            AgentClient(platform, session_id="s-src"),
        )

        self.assertEqual(result["status"], "failed")
        self.assertTrue(platform.results)
        self.assertEqual(platform.results[0][1], "failed")

    def test_execute_task_rejects_unknown_role(self):
        platform = FakePlatform({"s-src": "source"})

        with self.assertRaises(ValueError):
            execute_task(
                {"task_id": "t", "role": "bogus"},
                AgentClient(platform, session_id="s-src"),
            )

    def test_run_once_returns_none_without_task(self):
        platform = FakePlatform({"s-src": "source"})

        self.assertIsNone(run_once(AgentClient(platform, session_id="s-src")))

    def test_run_once_executes_queued_task(self):
        platform = FakePlatform({"s-src": "source"})
        platform.tasks["source"].append(
            {
                "task_id": "t-src",
                "role": "source",
                "src_path": str(self.src),
                "peer_host": "127.0.0.1",
                "peer_port": _free_port(),
                "ticket": "ticket-1",
                "offset": 0,
                "length": len(self.payload),
            }
        )

        result = run_once(AgentClient(platform, session_id="s-src"))

        self.assertEqual(result["status"], "failed")


class ResolveDeviceTest(unittest.TestCase):
    """Nova 汇报的 /dev/vdb 不等于 guest 内的设备名，必须按卷 id 兜底解析。"""

    def test_existing_path_is_used_as_is(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "disk"
            path.write_bytes(b"x")

            self.assertEqual(resolve_device(str(path), "vol-1"), str(path))

    def test_missing_path_raises_with_device_hint(self):
        from relay_transfer import TransferError

        with self.assertRaises(TransferError) as ctx:
            resolve_device("/dev/definitely-not-here", "vol-1")

        message = str(ctx.exception)
        self.assertIn("/dev/definitely-not-here", message)
        self.assertIn("vol-1", message)

    def test_lookup_by_volume_id_in_disk_by_dir(self):
        from unittest import mock

        missing = "/dev/not-a-real-device-for-test"
        with mock.patch("relay_agent.Path") as path_cls:
            real_path = Path
            base = mock.MagicMock()
            base.is_dir.return_value = True
            entry = mock.MagicMock()
            entry.name = "virtio-abc-vol-1"
            entry.resolve.return_value = real_path("/dev/sdb")
            base.iterdir.return_value = [entry]

            def make(value):
                if value == "/dev/disk/by-id":
                    return base
                if value == "/dev/disk/by-path":
                    return base
                if value == "/dev/disk/by-uuid":
                    return base
                return real_path(value)

            path_cls.side_effect = make
            self.assertEqual(resolve_device(missing, "vol-1"), "/dev/sdb")

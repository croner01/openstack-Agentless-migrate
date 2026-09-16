import os
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from flask import Flask

from relay_agent import (
    AgentClient,
    execute_task,
    local_public_key,
    resolve_data_address,
)
from relay_api import create_blueprint
from relay_registry import RelayState
from relay_transfer import receive_device, send_device


class _FakeSock:
    def __init__(self, addr):
        self._addr = addr
        self.connected = None

    def connect(self, target):
        self.connected = target

    def getsockname(self):
        return self._addr

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class ResolveDataAddressTest(unittest.TestCase):
    def test_local_public_key_reads_file(self):
        tmp = tempfile.TemporaryDirectory()
        key_path = Path(tmp.name) / "id_ed25519.pub"
        key_path.write_text("ssh-ed25519 AAAA relay@vm\n", encoding="utf-8")

        self.assertEqual(local_public_key(str(key_path)), "ssh-ed25519 AAAA relay@vm")
        tmp.cleanup()

    def test_local_public_key_missing_file_returns_empty(self):
        self.assertEqual(local_public_key("/nonexistent/id_ed25519.pub"), "")

    def test_uses_local_address_of_route_to_platform(self):
        created = []

        def factory(family, type_):
            sock = _FakeSock(("10.0.0.7", 41234))
            created.append(sock)
            return sock

        address = resolve_data_address("platform.example.com", sock_factory=factory)

        self.assertEqual(address, "10.0.0.7")
        self.assertEqual(created[0].connected, ("platform.example.com", 80))

    def test_returns_empty_string_when_probe_fails(self):
        def factory(family, type_):
            raise OSError("no route")

        self.assertEqual(
            resolve_data_address("platform.example.com", sock_factory=factory), ""
        )


class RegistryReadyTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(secret=b"secret")

    def test_register_stores_data_address_and_port(self):
        agent = self.state.register(
            job_id="job-1",
            role="target",
            name="relay-target-0",
            version="1.0.0",
            address="198.51.100.9",
            now=1000.0,
            data_address="10.0.0.8",
            data_port=9200,
        )

        self.assertEqual(agent.data_address, "10.0.0.8")
        self.assertEqual(agent.data_port, 9200)

    def test_find_by_name_returns_agent(self):
        self.state.register(
            job_id="job-1",
            role="target",
            name="relay-target-0",
            version="1.0.0",
            address="198.51.100.9",
            now=1000.0,
        )

        self.assertEqual(self.state.find_by_name("relay-target-0").name, "relay-target-0")
        self.assertIsNone(self.state.find_by_name("missing"))

    def test_task_ready_flag(self):
        self.assertFalse(self.state.task_ready("t-1"))
        self.state.mark_task_ready("t-1")
        self.assertTrue(self.state.task_ready("t-1"))

    def test_register_stores_ssh_public_key(self):
        agent = self.state.register(
            job_id="job-1",
            role="source",
            name="relay-source-0",
            version="1.0.0",
            address="198.51.100.9",
            now=1000.0,
            ssh_public_key="ssh-ed25519 AAAA relay@vm",
        )

        self.assertEqual(agent.ssh_public_key, "ssh-ed25519 AAAA relay@vm")


class ReadyApiTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(secret=b"secret")
        app = Flask(__name__)
        app.register_blueprint(create_blueprint(self.state))
        self.client = app.test_client()

    def test_ready_endpoint_marks_task(self):
        response = self.client.post("/api/relay/tasks/t-1/ready", json={})

        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.state.task_ready("t-1"))

    def test_register_passes_data_address(self):
        from relay_protocol import issue_token

        token = issue_token(self.state.secret, "job-1", "target", now=time.time(), ttl=60)

        response = self.client.post(
            "/api/relay/register",
            json={
                "token": token,
                "name": "relay-target-0",
                "agent_version": "1.0.0",
                "data_address": "10.0.0.8",
                "data_port": 9300,
            },
        )

        self.assertEqual(response.status_code, 200)
        agent = self.state.find_by_name("relay-target-0")
        self.assertEqual(agent.data_address, "10.0.0.8")
        self.assertEqual(agent.data_port, 9300)


class OnListeningTest(unittest.TestCase):
    def test_receive_device_invokes_on_listening_before_accept(self):
        tmp = tempfile.TemporaryDirectory()
        target = Path(tmp.name) / "dst.bin"
        target.write_bytes(b"\x00" * 1024)
        port = _free_port()
        seen = threading.Event()
        errors = []

        def run_receiver():
            try:
                receive_device(
                    str(target),
                    listen_host="127.0.0.1",
                    listen_port=port,
                    expected_ticket="ticket-1",
                    offset=0,
                    length=1024,
                    on_listening=seen.set,
                )
            except Exception as exc:  # noqa: BLE001 - 测试需要看到接收端异常
                errors.append(exc)

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(seen.wait(timeout=5))

        send_device(
            str(target),
            peer_host="127.0.0.1",
            peer_port=port,
            ticket="ticket-1",
            offset=0,
            length=1024,
        )
        thread.join(timeout=5)

        self.assertEqual(errors, [])
        tmp.cleanup()


class AgentReadyReportTest(unittest.TestCase):
    class _Platform:
        def __init__(self):
            self.ready = []
            self.ready_event = threading.Event()

        def heartbeat(self, session_id):
            return {"cancel": False}

        def next_task(self, session_id):
            return None

        def report_progress(self, task_id, copied_bytes, skipped_bytes=0):
            pass

        def report_result(
            self, task_id, status, copied_bytes, digest="", error="", skipped_bytes=0
        ):
            pass

        def report_ready(self, task_id):
            self.ready.append(task_id)
            self.ready_event.set()

    def test_target_task_reports_ready_before_sender_connects(self):
        tmp = tempfile.TemporaryDirectory()
        source = Path(tmp.name) / "src.bin"
        target = Path(tmp.name) / "dst.bin"
        payload = os.urandom(2048)
        source.write_bytes(payload)
        target.write_bytes(b"\x00" * len(payload))
        port = _free_port()
        platform = self._Platform()
        client = AgentClient(platform, session_id="s-dst")
        sent = {}

        def sender():
            # 只有目标端上报 ready 之后才连接，验证握手语义。
            platform.ready_event.wait(timeout=5)
            sent["bytes"] = send_device(
                str(source),
                peer_host="127.0.0.1",
                peer_port=port,
                ticket="ticket-1",
                offset=0,
                length=len(payload),
            )

        sender_thread = threading.Thread(target=sender)
        sender_thread.start()
        result = execute_task(
            {
                "task_id": "t-1",
                "role": "target",
                "dst_path": str(target),
                "listen_host": "127.0.0.1",
                "listen_port": port,
                "ticket": "ticket-1",
                "offset": 0,
                "length": len(payload),
            },
            client,
        )
        sender_thread.join(timeout=5)

        self.assertEqual(platform.ready, ["t-1"])
        self.assertEqual(result["status"], "done")
        self.assertEqual(target.read_bytes(), payload)
        tmp.cleanup()


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]

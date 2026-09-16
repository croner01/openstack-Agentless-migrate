import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from flask import Flask

from relay_api import AGENT_PACKAGE_FILES, build_bootstrap_script, create_blueprint
from relay_pool import build_cloud_init
from relay_protocol import issue_token
from relay_registry import RelayState

APP_DIR = Path(__file__).resolve().parent.parent


class BootstrapScriptTest(unittest.TestCase):
    def test_script_downloads_whitelisted_files_and_enables_service(self):
        script = build_bootstrap_script("https://platform.example.com/")

        for name in AGENT_PACKAGE_FILES:
            self.assertIn(f"/api/relay/pkg/{name}?token=$TOKEN", script)
        self.assertIn('BASE="https://platform.example.com"', script)
        self.assertIn("systemctl enable --now relay-agent", script)
        self.assertIn("RELAY_TOKEN", script)


class BootstrapApiTest(unittest.TestCase):
    SECRET = b"bootstrap-secret"

    def setUp(self):
        self.state = RelayState(secret=self.SECRET)
        app = Flask(__name__)
        app.register_blueprint(create_blueprint(self.state))
        self.client = app.test_client()
        self.token = issue_token(self.SECRET, "job-1", "source", now=time.time(), ttl=300)

    def test_bootstrap_requires_valid_token(self):
        self.assertEqual(self.client.get("/api/relay/bootstrap").status_code, 401)
        self.assertEqual(
            self.client.get("/api/relay/bootstrap?token=bogus").status_code, 401
        )

    def test_bootstrap_returns_shell_script(self):
        response = self.client.get(f"/api/relay/bootstrap?token={self.token}")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("#!/bin/bash", body)
        self.assertIn("/api/relay/pkg/relay_agent.py", body)

    def test_pkg_serves_whitelisted_file(self):
        response = self.client.get(
            f"/api/relay/pkg/relay_protocol.py?token={self.token}"
        )

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("class ProtocolError", body)
        self.assertEqual(
            body, (APP_DIR / "relay_protocol.py").read_text(encoding="utf-8")
        )

    def test_pkg_rejects_path_outside_whitelist(self):
        for name in ("app.py", "..%2Fapp.py", "relay_api.py"):
            response = self.client.get(f"/api/relay/pkg/{name}?token={self.token}")
            self.assertEqual(response.status_code, 404, name)

    def test_pkg_requires_valid_token(self):
        response = self.client.get("/api/relay/pkg/relay_agent.py")

        self.assertEqual(response.status_code, 401)


class AgentBootstrapEndToEndTest(unittest.TestCase):
    """把 cloud-init 生成的配置直接喂给 agent 子进程，验证真实注册链路。"""

    SECRET = b"e2e-secret"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = RelayState(secret=self.SECRET)
        self.app = Flask(__name__)
        self.app.register_blueprint(create_blueprint(self.state))
        self.port = _free_port()
        self.server = threading.Thread(
            target=lambda: self.app.run(
                host="127.0.0.1", port=self.port, threaded=True, use_reloader=False
            ),
            daemon=True,
        )
        self.server.start()
        _wait_for_port(self.port)
        self.process = None

    def tearDown(self):
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.tmp.cleanup()

    def test_agent_registers_from_cloud_init_env(self):
        platform_url = f"http://127.0.0.1:{self.port}"
        token = issue_token(
            self.SECRET, "job-1", "source", now=time.time(), ttl=300
        )
        cloud_init = build_cloud_init(
            platform_url=platform_url,
            token=token,
            job_id="job-1",
            role="source",
            name="relay-source-0",
        )
        env = _env_from_cloud_init(cloud_init)
        env.update(
            {
                "RELAY_DATA_ADDR": "127.0.0.1",
                "PATH": os.environ.get("PATH", ""),
            }
        )

        self.process = subprocess.Popen(
            [sys.executable, "-u", str(APP_DIR / "relay_agent.py")],
            cwd=str(APP_DIR),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 15
        agent = None
        while time.time() < deadline:
            agent = self.state.find_by_name("relay-source-0")
            if agent is not None:
                break
            time.sleep(0.2)

        self.assertIsNotNone(agent, "agent 未能在超时前完成注册")
        self.assertEqual(agent.role, "source")
        self.assertEqual(agent.job_id, "job-1")
        self.assertEqual(agent.data_address, "127.0.0.1")
        self.assertTrue(agent.session_id)


def _env_from_cloud_init(text: str) -> dict[str, str]:
    """从 cloud-init 文本里解析出 RELAY_* 键值，模拟 VM 内的 agent.env。"""
    env: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("RELAY_") and "=" in stripped:
            key, _, value = stripped.partition("=")
            env[key] = value
    return env


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"本地平台未在 {timeout}s 内就绪")

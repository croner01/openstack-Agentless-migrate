# 中转机全量迁移（2/3）：OpenStack 编排与卷生命周期 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把第 1 份计划的数据面接进平台：作业级中转机池、快照派生卷、attach/detach、单卷中转编排、对账清理，并让 `MigrationManager` 能按数据通道分流到中转机路径。

**Architecture:** OpenStack 调用集中在 `OpenStackUtils`（新增中转机与卷原语）；`relay_pool.py` 管池与调度、`relay_volumes.py` 管卷生命周期、`relay_orchestrator.py` 编排单卷中转、`relay_reaper.py` 周期对账；`migration_manager.py` 按 `data_channel` 分流，中转机路径为「建目标卷 → 拷贝 → 用目标卷建 VM」。agent 仍只做块拷贝，attach/detach 全部由平台完成。

**Tech Stack:** Python 3.10、Flask、openstacksdk、第 1 份计划的 relay 模块、标准库 `unittest` + `unittest.mock`。

---

## 说明

- 前置：第 1 份计划（`docs/superpowers/plans/2026-09-10-relay-vm-data-plane.md`）已完成，
  `relay_protocol.py`、`relay_ledger.py`、`relay_registry.py`、`relay_api.py`、
  `relay_transfer.py`、`relay_agent.py` 已存在并通过测试。
- 设计文档：`docs/superpowers/specs/2026-09-10-relay-vm-full-copy-migration-design.md`。
- 当前目录不是 git 仓库（`.git` 为空），所有任务跳过 commit 步骤。
- 新增模块需同步更新平台 ConfigMap 的 `--from-file` 列表。
- 本计划不做增量、不做 detach 源卷降级：源端驱动不支持 in-use 快照时预检直接拦截。
- 测试全部 mock OpenStack，唯一需要真 socket 的是 Task 7 的端到端用例。

## 文件结构

| 文件 | 职责 |
| --- | --- |
| `openstack_utils.py` | 新增中转机建机、从快照建卷、attach/detach、删除等原语 |
| `relay_transfer.py` | 新增 `on_listening` 回调，用于目标端监听就绪上报 |
| `relay_agent.py` | 目标端监听成功后上报 `ready`，并上报数据面地址 |
| `relay_api.py` | 新增 `/tasks/<id>/ready` 端点 |
| `relay_registry.py` | 新增数据面地址字段、任务就绪标记、按名字查 agent |
| `relay_pool.py` | 中转机池：cloud-init 生成、建机、预热、调度、销毁 |
| `relay_volumes.py` | 卷生命周期：快照派生、attach/detach、清理 |
| `relay_orchestrator.py` | 单卷中转编排：下发两端任务、等待就绪与完成、同步台账 |
| `relay_reaper.py` | 周期对账：中间态超时、孤儿 attachment、作业收尾 |
| `migration_manager.py` | 按 `data_channel` 分流到中转机路径 |
| `tests/test_relay_openstack_primitives.py` | 新增 OpenStack 原语单测 |
| `tests/test_relay_ready_handshake.py` | 就绪握手单测 |
| `tests/test_relay_pool.py` | 池与调度单测 |
| `tests/test_relay_volumes.py` | 卷生命周期单测 |
| `tests/test_relay_orchestrator.py` | 单卷编排单测 |
| `tests/test_relay_reaper.py` | 对账单测 |
| `tests/test_relay_migration_integration.py` | 端到端集成（mock 云 + 真 agent 拷贝） |

---

### Task 1: OpenStack 原语

**Files:**
- Modify: `openstack_utils.py`
- Test: `tests/test_relay_openstack_primitives.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from openstack_utils import OpenStackUtils


class RelayPrimitivesTest(unittest.TestCase):
    def setUp(self):
        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)

    def test_create_relay_server_passes_image_and_user_data(self):
        self.os_utils.create_relay_server(
            name="relay-1",
            image_id="img-1",
            flavor_id="flv-1",
            port_ids=["port-1", "port-2"],
            availability_zone="nova",
            user_data="#cloud-config\n",
        )

        kwargs = self.conn.compute.create_server.call_args.kwargs
        self.assertEqual(kwargs["name"], "relay-1")
        self.assertEqual(kwargs["image_id"], "img-1")
        self.assertEqual(kwargs["flavor_id"], "flv-1")
        self.assertEqual(kwargs["networks"], [{"port": "port-1"}, {"port": "port-2"}])
        self.assertEqual(kwargs["availability_zone"], "nova")
        self.assertEqual(kwargs["user_data"], "#cloud-config\n")

    def test_create_server_from_volumes_puts_boot_volume_first(self):
        self.os_utils.create_server_from_volumes(
            name="vm-t",
            flavor_id="flv-1",
            port_ids=["port-1"],
            boot_volume_id="vol-boot",
            data_volume_ids=["vol-data"],
            availability_zone="nova",
            admin_password="secret",
        )

        bdm = self.conn.compute.create_server.call_args.kwargs["block_device_mapping_v2"]
        self.assertEqual(bdm[0]["uuid"], "vol-boot")
        self.assertEqual(bdm[0]["source_type"], "volume")
        self.assertEqual(bdm[0]["boot_index"], 0)
        self.assertEqual(bdm[1]["uuid"], "vol-data")
        self.assertEqual(bdm[1]["boot_index"], -1)

    def test_create_volume_snapshot_forces_in_use_volume(self):
        self.os_utils.create_volume_snapshot(volume_id="vol-1", name="snap-1")

        kwargs = self.conn.block_storage.create_snapshot.call_args.kwargs
        self.assertEqual(kwargs["volume_id"], "vol-1")
        self.assertEqual(kwargs["name"], "snap-1")
        self.assertTrue(kwargs["force"])

    def test_create_volume_from_snapshot_passes_optional_fields(self):
        self.os_utils.create_volume_from_snapshot(
            name="derived-1",
            snapshot_id="snap-1",
            size=40,
            volume_type="ssd",
            availability_zone="az1",
        )

        kwargs = self.conn.block_storage.create_volume.call_args.kwargs
        self.assertEqual(kwargs["snapshot_id"], "snap-1")
        self.assertEqual(kwargs["size"], 40)
        self.assertEqual(kwargs["volume_type"], "ssd")
        self.assertEqual(kwargs["availability_zone"], "az1")

    def test_create_volume_from_snapshot_omits_empty_optionals(self):
        self.os_utils.create_volume_from_snapshot(
            name="derived-1", snapshot_id="snap-1", size=40
        )

        kwargs = self.conn.block_storage.create_volume.call_args.kwargs
        self.assertNotIn("volume_type", kwargs)
        self.assertNotIn("availability_zone", kwargs)

    def test_attach_volume_returns_attachment_id(self):
        self.conn.compute.create_server_volume.return_value = mock.Mock(id="att-1")

        attachment_id = self.os_utils.attach_volume(server_id="srv-1", volume_id="vol-1")

        self.assertEqual(attachment_id, "att-1")
        kwargs = self.conn.compute.create_server_volume.call_args.kwargs
        self.assertEqual(kwargs["server_id"], "srv-1")
        self.assertEqual(kwargs["volume_id"], "vol-1")

    def test_detach_volume_calls_compute_proxy(self):
        self.os_utils.detach_volume(server_id="srv-1", volume_id="vol-1")

        kwargs = self.conn.compute.delete_server_volume.call_args.kwargs
        self.assertEqual(kwargs["server_id"], "srv-1")
        self.assertEqual(kwargs["volume_id"], "vol-1")

    def test_find_volume_attachment_matches_volume(self):
        self.conn.compute.volume_attachments.return_value = [
            mock.Mock(id="att-1", volume_id="vol-other"),
            mock.Mock(id="att-2", volume_id="vol-1"),
        ]

        self.assertEqual(
            self.os_utils.find_volume_attachment("srv-1", "vol-1"), "att-2"
        )
        self.assertIsNone(self.os_utils.find_volume_attachment("srv-1", "vol-none"))

    def test_delete_volume_and_snapshot_call_block_storage(self):
        self.os_utils.delete_volume("vol-1")
        self.os_utils.delete_volume_snapshot("snap-1")

        self.assertEqual(
            self.conn.block_storage.delete_volume.call_args.args, ("vol-1",)
        )
        self.assertEqual(
            self.conn.block_storage.delete_snapshot.call_args.args, ("snap-1",)
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_openstack_primitives -v`

Expected: FAIL with `AttributeError: 'OpenStackUtils' object has no attribute 'create_relay_server'`

- [ ] **Step 3: Write minimal implementation**

把以下方法追加到 `openstack_utils.py` 的 `OpenStackUtils` 内，
位置放在 `create_bfv_server` 之后、`enable_http_debug_logging` 之前：

```python
    def create_relay_server(
        self,
        name: str,
        image_id: str,
        flavor_id: str,
        port_ids: list[str],
        availability_zone: str,
        user_data: str,
    ):
        """创建中转机：普通 server，配置全部由 cloud-init 注入。"""
        logging.info(
            "[MIGRATION] 创建中转机 name=%s image=%s flavor=%s az=%s",
            name,
            image_id,
            flavor_id,
            availability_zone,
        )
        return self.conn.compute.create_server(
            name=name,
            image_id=image_id,
            flavor_id=flavor_id,
            networks=[{"port": port_id} for port_id in port_ids],
            availability_zone=availability_zone,
            user_data=user_data,
        )

    def create_server_from_volumes(
        self,
        name: str,
        flavor_id: str,
        port_ids: list[str],
        boot_volume_id: str,
        data_volume_ids: list[str],
        availability_zone: str,
        admin_password: str,
    ):
        """用已建好的卷作启动盘建 VM（中转机路径的目标 VM）。"""
        bdm = [
            {
                "uuid": boot_volume_id,
                "source_type": "volume",
                "destination_type": "volume",
                "boot_index": 0,
                "delete_on_termination": False,
            }
        ]
        for volume_id in data_volume_ids:
            bdm.append(
                {
                    "uuid": volume_id,
                    "source_type": "volume",
                    "destination_type": "volume",
                    "boot_index": -1,
                    "delete_on_termination": False,
                }
            )
        return self.conn.compute.create_server(
            name=name,
            flavor_id=flavor_id,
            networks=[{"port": port_id} for port_id in port_ids],
            block_device_mapping_v2=bdm,
            availability_zone=availability_zone,
            admin_password=admin_password,
        )

    def create_volume_snapshot(self, volume_id: str, name: str, *, force: bool = True):
        """对卷打快照；force 允许对 in-use（仍挂在源 VM 上）的卷打快照。"""
        return self.conn.block_storage.create_snapshot(
            volume_id=volume_id,
            name=name,
            force=force,
        )

    def create_volume_from_snapshot(
        self,
        name: str,
        snapshot_id: str,
        size: int,
        volume_type: str | None = None,
        availability_zone: str | None = None,
    ):
        kwargs: dict[str, Any] = {
            "name": name,
            "snapshot_id": snapshot_id,
            "size": size,
        }
        if volume_type:
            kwargs["volume_type"] = volume_type
        if availability_zone:
            kwargs["availability_zone"] = availability_zone
        return self.conn.block_storage.create_volume(**kwargs)

    def attach_volume(self, server_id: str, volume_id: str) -> str:
        """把卷挂到实例上，返回 attachment id。"""
        attachment = self.conn.compute.create_server_volume(
            server_id=server_id, volume_id=volume_id
        )
        return getattr(attachment, "id", "") or ""

    def detach_volume(self, server_id: str, volume_id: str) -> None:
        self.conn.compute.delete_server_volume(
            server_id=server_id, volume_id=volume_id
        )

    def find_volume_attachment(self, server_id: str, volume_id: str) -> str | None:
        for attachment in self.conn.compute.volume_attachments(server_id):
            if attachment.volume_id == volume_id:
                return attachment.id
        return None

    def delete_volume(self, volume_id: str) -> None:
        self.conn.block_storage.delete_volume(volume_id)

    def delete_volume_snapshot(self, snapshot_id: str) -> None:
        self.conn.block_storage.delete_snapshot(snapshot_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_openstack_primitives -v`

Expected: PASS（10 个用例）

---

### Task 2: 目标端监听就绪握手与数据面地址

**Files:**
- Modify: `relay_transfer.py`
- Modify: `relay_registry.py`
- Modify: `relay_api.py`
- Modify: `relay_agent.py`
- Test: `tests/test_relay_ready_handshake.py`

背景：源 agent 必须等目标 agent 真正进入 `listen()` 之后才能连接，否则会
`ConnectionRefused`。另外平台看到的 `remote_addr` 可能是 NAT 后的地址，
源 agent 需要的是目标 agent 在数据面上的真实地址，因此 agent 注册时要上报
自己的 `data_address` / `data_port`。

- [ ] **Step 1: Write the failing test**

```python
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from flask import Flask

from relay_agent import AgentClient, execute_task, resolve_data_address
from relay_api import create_blueprint
from relay_registry import RelayState
from relay_transfer import receive_device


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
    def test_uses_local_address_of_data_route(self):
        created = []

        def factory(family, type_):
            sock = _FakeSock(("10.0.0.7", 41234))
            created.append((family, type_, sock))
            return sock

        address = resolve_data_address("platform.example.com", sock_factory=factory)

        self.assertEqual(address, "10.0.0.7")
        self.assertEqual(created[0][2].connected, ("platform.example.com", 80))

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
            name="relay-t-0",
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
            name="relay-t-0",
            version="1.0.0",
            address="198.51.100.9",
            now=1000.0,
        )

        self.assertEqual(self.state.find_by_name("relay-t-0").name, "relay-t-0")
        self.assertIsNone(self.state.find_by_name("missing"))

    def test_task_ready_flag(self):
        self.assertFalse(self.state.task_ready("t-1"))
        self.state.mark_task_ready("t-1")
        self.assertTrue(self.state.task_ready("t-1"))


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


class OnListeningTest(unittest.TestCase):
    def test_receive_device_invokes_on_listening_before_accept(self):
        import os

        tmp = tempfile.TemporaryDirectory()
        target = Path(tmp.name) / "dst.bin"
        target.write_bytes(b"\x00" * 1024)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        seen = []
        done = threading.Event()

        def run_receiver():
            receive_device(
                str(target),
                listen_host="127.0.0.1",
                listen_port=port,
                expected_ticket="ticket-1",
                offset=0,
                length=1024,
                on_listening=lambda: seen.append(True),
            )
            done.set()

        thread = threading.Thread(target=run_receiver)
        thread.start()
        for _ in range(200):
            if seen:
                break
            threading.Event().wait(0.01)
        self.assertTrue(seen)
        thread.join(timeout=5)
        tmp.cleanup()


class AgentReadyReportTest(unittest.TestCase):
    class _Platform:
        def __init__(self):
            self.ready = []

        def heartbeat(self, session_id):
            return {"cancel": False}

        def next_task(self, session_id):
            return None

        def report_progress(self, task_id, copied_bytes):
            pass

        def report_result(self, task_id, status, copied_bytes):
            pass

        def report_ready(self, task_id):
            self.ready.append(task_id)

    def test_target_task_reports_ready(self):
        import os

        tmp = tempfile.TemporaryDirectory()
        target = Path(tmp.name) / "dst.bin"
        target.write_bytes(b"\x00" * 2048)
        platform = self._Platform()
        client = AgentClient(platform, session_id="s-dst")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]

        import threading as _threading

        sender = _threading.Thread(
            target=lambda: __import__("relay_transfer").send_device(
                str(target),
                peer_host="127.0.0.1",
                peer_port=port,
                ticket="ticket-1",
                offset=0,
                length=2048,
            )
        )

        def run():
            execute_task(
                {
                    "task_id": "t-1",
                    "role": "target",
                    "dst_path": str(target),
                    "listen_host": "127.0.0.1",
                    "listen_port": port,
                    "ticket": "ticket-1",
                    "offset": 0,
                    "length": 2048,
                },
                client,
            )

        receiver = _threading.Thread(target=run)
        receiver.start()
        for _ in range(200):
            if platform.ready:
                break
            _threading.Event().wait(0.01)
        sender.start()
        sender.join(timeout=5)
        receiver.join(timeout=5)

        self.assertEqual(platform.ready, ["t-1"])
        tmp.cleanup()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_ready_handshake -v`

Expected: FAIL with `ImportError: cannot import name 'resolve_data_address'`

- [ ] **Step 3: Write minimal implementation**

`relay_transfer.py`：给 `receive_device` 增加 `on_listening` 参数，
在 `server.listen(1)` 之后、`server.accept()` 之前调用：

```python
    on_listening: Callable[[], None] | None = None,
) -> int:
    """监听一次连接并把数据写入 dst_path，返回接收字节数。"""
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((listen_host, listen_port))
        server.listen(1)
        if ready_event is not None:
            ready_event.set()
        if on_listening is not None:
            on_listening()
        conn, _ = server.accept()
```

`relay_registry.py`：`AgentRecord` 增加两个字段，`RelayState` 增加按名查找与任务就绪标记：

```python
@dataclass
class AgentRecord:
    ...
    current_task_id: str = ""
    data_address: str = ""
    data_port: int = 9200
```

```python
    def register(
        self,
        *,
        job_id: str,
        role: str,
        name: str,
        version: str,
        address: str,
        now: float,
        data_address: str = "",
        data_port: int = 9200,
    ) -> AgentRecord:
        ...
        agent = AgentRecord(
            ...,
            data_address=data_address,
            data_port=data_port,
        )

    def find_by_name(self, name: str) -> AgentRecord | None:
        for agent in self._agents.values():
            if agent.name == name:
                return agent
        return None

    def mark_task_ready(self, task_id: str) -> None:
        self._ready_tasks.add(task_id)

    def task_ready(self, task_id: str) -> bool:
        return task_id in self._ready_tasks
```

`RelayState.__init__` 里补 `self._ready_tasks: set[str] = set()`。

`relay_api.py`：注册接口透传数据面地址，并新增就绪端点：

```python
            agent = state.register(
                ...,
                data_address=str(body.get("data_address", "")),
                data_port=int(body.get("data_port", 9200) or 9200),
            )
```

```python
    @blueprint.post("/tasks/<task_id>/ready")
    def task_ready(task_id: str):
        state.mark_task_ready(task_id)
        return jsonify({"ok": True})
```

`relay_agent.py`：新增数据面地址探测与就绪上报：

```python
def resolve_data_address(
    probe_host: str,
    *,
    probe_port: int = 80,
    sock_factory: Callable[..., Any] = socket.socket,
) -> str:
    """探测本机到平台方向使用的地址；失败返回空串。"""
    try:
        with sock_factory(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((probe_host, probe_port))
            return str(sock.getsockname()[0])
    except OSError:
        return ""
```

`AgentClient` 增加：

```python
    def report_ready(self, task_id: str) -> None:
        self.platform.report_ready(task_id)
```

`execute_task` 的 target 分支传入回调：

```python
            copied = receive_device(
                ...,
                on_listening=lambda: client.report_ready(task_id),
            )
```

`_HttpPlatform` 增加：

```python
    def report_ready(self, task_id: str) -> None:
        _http_json(f"{self.base_url}/api/relay/tasks/{task_id}/ready", {})
```

`main()` 注册时补上数据面地址：

```python
    data_address = os.environ.get("RELAY_DATA_ADDR") or resolve_data_address(
        urllib.parse.urlsplit(platform_url).hostname or ""
    )
    registration = _http_json(
        f"{platform_url}/api/relay/register",
        {
            "token": token,
            "name": name,
            "agent_version": AGENT_VERSION,
            "data_address": data_address,
            "data_port": int(os.environ.get("RELAY_DATA_PORT") or 9200),
        },
    )
```

并在 `relay_agent.py` 顶部补 `import os`、`import urllib.parse` 与
`from typing import Any, Callable`（`main()` 用到 `os.environ`，
不能只依赖 `__main__` 块里的局部导入）。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_ready_handshake -v`
（需要非沙箱模式，用例里创建了 socket）

Expected: PASS（7 个用例）

- [ ] **Step 5: Run the full suite to confirm no regression**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS，无失败用例

---

### Task 3: 中转机池

**Files:**
- Create: `relay_pool.py`
- Modify: `openstack_utils.py`（新增 `delete_server`）
- Test: `tests/test_relay_pool.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from relay_pool import RelayPool, build_cloud_init
from relay_registry import RelayState


class CloudInitTest(unittest.TestCase):
    def test_cloud_init_injects_agent_env(self):
        text = build_cloud_init(
            platform_url="https://platform.example.com",
            token="tok-1",
            job_id="job-1",
            role="source",
        )

        self.assertIn("RELAY_PLATFORM_URL=https://platform.example.com", text)
        self.assertIn("RELAY_TOKEN=tok-1", text)
        self.assertIn("RELAY_JOB_ID=job-1", text)
        self.assertIn("RELAY_ROLE=source", text)
        self.assertIn("systemctl", text)


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
            port_ids=["port-1"],
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
                now=1000.0,
                data_address="10.0.0.9",
                data_port=9200,
            )

    def test_provision_creates_one_server_per_slot(self):
        self.pool.provision()

        self.assertEqual(self.os_utils.create_relay_server.call_count, 2)
        self.assertEqual(
            [node.name for node in self.pool.nodes],
            ["relay-source-0", "relay-source-1"],
        )
        kwargs = self.os_utils.create_relay_server.call_args.kwargs
        self.assertIn("RELAY_ROLE=source", kwargs["user_data"])
        self.assertEqual(kwargs["image_id"], "img-1")
        self.assertEqual(kwargs["port_ids"], ["port-1"])

    def test_wait_ready_binds_session_and_data_address(self):
        self.pool.provision()
        self._register_all()

        self.pool.wait_ready(timeout=1.0, poll_interval=0.01)

        for node in self.pool.nodes:
            self.assertEqual(node.state, "ready")
            self.assertTrue(node.session_id)
            self.assertEqual(node.data_address, "10.0.0.9")
            self.assertEqual(node.data_port, 9200)

    def test_wait_ready_times_out_when_agent_missing(self):
        self.pool.provision()

        with self.assertRaises(TimeoutError):
            self.pool.wait_ready(timeout=0.05, poll_interval=0.01)

    def test_acquire_returns_first_idle_node_and_marks_busy(self):
        self.pool.provision()
        self._register_all()
        self.pool.wait_ready(timeout=1.0, poll_interval=0.01)

        first = self.pool.acquire("t-1")
        second = self.pool.acquire("t-2")
        third = self.pool.acquire("t-3")

        self.assertEqual(first.name, "relay-source-0")
        self.assertEqual(first.state, "busy")
        self.assertEqual(second.name, "relay-source-1")
        self.assertIsNone(third)

    def test_release_returns_node_to_ready(self):
        self.pool.provision()
        self._register_all()
        self.pool.wait_ready(timeout=1.0, poll_interval=0.01)
        node = self.pool.acquire("t-1")

        self.pool.release(node.node_id)

        self.assertEqual(node.state, "ready")
        self.assertEqual(node.current_task_id, "")
        self.assertIsNotNone(self.pool.acquire("t-2"))

    def test_sweep_marks_unhealthy_when_agent_stale(self):
        self.pool.provision()
        self._register_all()
        self.pool.wait_ready(timeout=1.0, poll_interval=0.01)

        changed = self.pool.sweep(now=10_000.0)

        self.assertEqual(len(changed), 2)
        self.assertTrue(all(node.state == "unhealthy" for node in self.pool.nodes))

    def test_destroy_deletes_every_server(self):
        self.pool.provision()

        self.pool.destroy()

        self.assertEqual(self.os_utils.delete_server.call_count, 2)
        self.assertEqual(self.pool.nodes, [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_pool -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_pool'`

- [ ] **Step 3: Write minimal implementation**

先给 `OpenStackUtils` 补一个删除实例的方法（放在 `stop_server` 附近）：

```python
    def delete_server(self, server_id: str) -> None:
        self.conn.compute.delete_server(server_id)
```

新建 `relay_pool.py`：

```python
"""作业级中转机池：建机、预热、调度与销毁。

池按 (云, AZ) 分组；每台机器同一时刻只服务一个卷对，
agent 负责数据面，卷的挂载卸载由平台完成。
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from relay_registry import RelayState

CLOUD_INIT_TEMPLATE = """#cloud-config
write_files:
  - path: /etc/relay/agent.env
    permissions: '0600'
    content: |
      RELAY_PLATFORM_URL={platform_url}
      RELAY_TOKEN={token}
      RELAY_JOB_ID={job_id}
      RELAY_ROLE={role}
      RELAY_NAME={name}
      RELAY_DATA_PORT={data_port}
runcmd:
  - [ systemctl, enable, --now, relay-agent ]
"""


def build_cloud_init(
    *,
    platform_url: str,
    token: str,
    job_id: str,
    role: str,
    name: str,
    data_port: int = 9200,
) -> str:
    """生成注入注册令牌与角色的 cloud-init。"""
    return CLOUD_INIT_TEMPLATE.format(
        platform_url=platform_url,
        token=token,
        job_id=job_id,
        role=role,
        name=name,
        data_port=data_port,
    )


@dataclass
class RelayNode:
    node_id: str
    name: str
    role: str
    az: str
    server_id: str
    session_id: str = ""
    data_address: str = ""
    data_port: int = 9200
    state: str = "provisioning"
    current_task_id: str = ""


class RelayPool:
    """一台中转机同一时刻只服务一个卷对，复用方式为串行。"""

    def __init__(
        self,
        *,
        role: str,
        job_id: str,
        az: str,
        size: int,
        image_id: str,
        flavor_id: str,
        port_ids: list[str],
        admin_password: str,
        platform_url: str,
        token_factory: Callable[[str, str], str],
        os_utils: Any,
        state: RelayState,
        data_port: int = 9200,
    ):
        self.role = role
        self.job_id = job_id
        self.az = az
        self.size = size
        self.image_id = image_id
        self.flavor_id = flavor_id
        self.port_ids = port_ids
        self.admin_password = admin_password
        self.platform_url = platform_url
        self.token_factory = token_factory
        self.os_utils = os_utils
        self.state = state
        self.data_port = data_port
        self.nodes: list[RelayNode] = []

    def provision(self) -> None:
        """按池大小创建中转机，cloud-init 注入注册令牌。"""
        for index in range(self.size):
            name = f"relay-{self.role}-{index}"
            token = self.token_factory(self.job_id, self.role)
            user_data = build_cloud_init(
                platform_url=self.platform_url,
                token=token,
                job_id=self.job_id,
                role=self.role,
                name=name,
                data_port=self.data_port,
            )
            server = self.os_utils.create_relay_server(
                name=name,
                image_id=self.image_id,
                flavor_id=self.flavor_id,
                port_ids=list(self.port_ids),
                availability_zone=self.az,
                user_data=user_data,
            )
            self.nodes.append(
                RelayNode(
                    node_id=uuid.uuid4().hex,
                    name=name,
                    role=self.role,
                    az=self.az,
                    server_id=server.id,
                )
            )
        logging.info(
            "[MIGRATION] 中转机池 role=%s az=%s 创建 %s 台", self.role, self.az, len(self.nodes)
        )

    def wait_ready(
        self,
        *,
        timeout: float = 300.0,
        poll_interval: float = 2.0,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        """等待池内每台机器的 agent 完成注册。"""
        deadline = time.monotonic() + timeout
        pending = list(self.nodes)
        while pending and time.monotonic() < deadline:
            still_pending = []
            for node in pending:
                agent = self.state.find_by_name(node.name)
                if agent is None:
                    still_pending.append(node)
                    continue
                node.session_id = agent.session_id
                node.data_address = agent.data_address
                node.data_port = agent.data_port
                node.state = "ready"
            pending = still_pending
            if pending:
                sleeper(poll_interval)
        if pending:
            names = ", ".join(node.name for node in pending)
            raise TimeoutError(f"中转机 agent 注册超时: {names}")

    def acquire(self, task_id: str) -> RelayNode | None:
        for node in self.nodes:
            if node.state == "ready":
                node.state = "busy"
                node.current_task_id = task_id
                return node
        return None

    def release(self, node_id: str) -> None:
        for node in self.nodes:
            if node.node_id == node_id:
                node.state = "ready"
                node.current_task_id = ""
                return

    def sweep(self, *, now: float) -> list[str]:
        """心跳超时的机器标记为 unhealthy，供上层换机重试。"""
        changed: list[str] = []
        for node in self.nodes:
            if node.state == "unhealthy":
                continue
            agent = self.state.find_by_name(node.name)
            if agent is None or now - agent.last_heartbeat > self.state.heartbeat_timeout:
                node.state = "unhealthy"
                changed.append(node.node_id)
        return changed

    def destroy(self) -> None:
        for node in list(self.nodes):
            try:
                self.os_utils.delete_server(node.server_id)
            except Exception:  # noqa: BLE001 - 清理失败不阻塞其余机器
                logging.exception("[MIGRATION] 删除中转机失败 server=%s", node.server_id)
        self.nodes = []
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_pool -v`

Expected: PASS（8 个用例）

---

### Task 4: 卷生命周期

**Files:**
- Create: `relay_volumes.py`
- Modify: `openstack_utils.py`（新增 `wait_snapshot_status`）
- Test: `tests/test_relay_volumes.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from relay_volumes import VolumeLifecycle


class VolumeLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.create_volume_snapshot.return_value = mock.Mock(id="snap-1")
        self.source_os.create_volume_from_snapshot.return_value = mock.Mock(id="vol-d1")
        self.target_os.create_volume_from_snapshot.return_value = mock.Mock(id="vol-t1")
        self.source_os.attach_volume.return_value = "att-1"
        self.target_os.attach_volume.return_value = "att-2"
        self.lifecycle = VolumeLifecycle(self.source_os, self.target_os)

    def test_create_source_copy_snapshots_then_clones(self):
        copy = self.lifecycle.create_source_copy(
            volume_id="vol-s1",
            vm_name="vm-1",
            index=0,
            az="az1",
        )

        self.assertEqual(copy.snapshot_id, "snap-1")
        self.assertEqual(copy.derived_volume_id, "vol-d1")
        snap_kwargs = self.source_os.create_volume_snapshot.call_args.kwargs
        self.assertEqual(snap_kwargs["volume_id"], "vol-s1")
        self.assertIn("vm-1", snap_kwargs["name"])
        clone_kwargs = self.source_os.create_volume_from_snapshot.call_args.kwargs
        self.assertEqual(clone_kwargs["snapshot_id"], "snap-1")
        self.assertEqual(clone_kwargs["size"], 40)
        self.assertEqual(clone_kwargs["availability_zone"], "az1")

    def test_create_source_copy_waits_for_both_resources(self):
        self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, az="az1", size=40
        )

        self.source_os.wait_snapshot_status.assert_called_once_with("snap-1")
        self.source_os.wait_volume_status.assert_called_once_with("vol-d1")

    def test_attach_uses_role_cloud(self):
        attachment = self.lifecycle.attach(
            role="source", server_id="relay-s", volume_id="vol-d1"
        )

        self.assertEqual(attachment, "att-1")
        self.source_os.attach_volume.assert_called_once_with(
            server_id="relay-s", volume_id="vol-d1"
        )
        self.source_os.wait_volume_status.assert_called_with(
            "vol-d1", target="in_use"
        )

    def test_attach_rejects_unknown_role(self):
        with self.assertRaises(ValueError):
            self.lifecycle.attach(role="bogus", server_id="s", volume_id="v")

    def test_detach_ignores_missing_attachment(self):
        self.source_os.find_volume_attachment.return_value = None

        self.lifecycle.detach(role="source", server_id="relay-s", volume_id="vol-d1")

        self.source_os.detach_volume.assert_not_called()

    def test_detach_calls_api_and_waits_for_available(self):
        self.target_os.find_volume_attachment.return_value = "att-2"

        self.lifecycle.detach(role="target", server_id="relay-t", volume_id="vol-t1")

        self.target_os.detach_volume.assert_called_once_with(
            server_id="relay-t", volume_id="vol-t1"
        )
        self.target_os.wait_volume_status.assert_called_with(
            "vol-t1", target="available"
        )

    def test_cleanup_source_copy_deletes_derived_then_snapshot(self):
        copy = self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, az="az1"
        )

        self.lifecycle.cleanup_source_copy(copy, relay_server_id="relay-s")

        self.source_os.find_volume_attachment.assert_called_with("relay-s", "vol-d1")
        self.source_os.delete_volume.assert_called_once_with("vol-d1")
        self.source_os.delete_volume_snapshot.assert_called_once_with("snap-1")

    def test_cleanup_is_best_effort(self):
        copy = self.lifecycle.create_source_copy(
            volume_id="vol-s1", vm_name="vm-1", index=0, az="az1"
        )
        self.source_os.delete_volume.side_effect = RuntimeError("boom")

        self.lifecycle.cleanup_source_copy(copy, relay_server_id="relay-s")

        self.source_os.delete_volume_snapshot.assert_called_once_with("snap-1")

    def test_create_target_volume_creates_blank_volume(self):
        self.target_os.create_blank_volume.return_value = mock.Mock(id="vol-t1")

        volume_id = self.lifecycle.create_target_volume(
            name="vm-1-vol-0",
            size=40,
            az="az1",
            volume_type="ssd",
        )

        self.assertEqual(volume_id, "vol-t1")
        self.target_os.create_blank_volume.assert_called_once_with(
            name="vm-1-vol-0",
            size=40,
            volume_type="ssd",
            availability_zone="az1",
        )
        self.target_os.wait_volume_status.assert_called_with("vol-t1")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_volumes -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_volumes'`

- [ ] **Step 3: Write minimal implementation**

先给 `OpenStackUtils` 补快照等待（放在 `wait_volume_status` 之后）：

```python
    def wait_snapshot_status(
        self,
        snapshot_id: str,
        target: str = "available",
        timeout: int = 600,
        poll_interval: int = 5,
    ):
        deadline = time.time() + timeout
        while time.time() < deadline:
            snapshot = self.conn.block_storage.get_snapshot(snapshot_id)
            if snapshot.status == target:
                return snapshot
            if snapshot.status == "error":
                raise RuntimeError(f"快照 {snapshot_id} 状态异常: {snapshot.status}")
            time.sleep(poll_interval)
        raise TimeoutError(f"等待快照 {snapshot_id} 达到 {target} 超时")
```

新建 `relay_volumes.py`：

```python
"""卷生命周期：源快照、派生卷、挂载卸载与清理。

源卷全程不 detach，派生卷是唯一拷贝源；所有资源都以台账为准登记与回收。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any


@dataclass
class SourceCopy:
    snapshot_id: str
    derived_volume_id: str


class VolumeLifecycle:
    def __init__(self, source_os: Any, target_os: Any):
        self.source_os = source_os
        self.target_os = target_os

    def _os_for(self, role: str):
        if role == "source":
            return self.source_os
        if role == "target":
            return self.target_os
        raise ValueError(f"unknown role: {role!r}")

    def create_source_copy(
        self,
        *,
        volume_id: str,
        vm_name: str,
        index: int,
        az: str,
        size: int = 0,
        volume_type: str | None = None,
    ) -> SourceCopy:
        """对源卷打快照并派生一份可挂载的拷贝源。"""
        name = f"mig-{vm_name}-{index}"
        snapshot = self.source_os.create_volume_snapshot(volume_id=volume_id, name=name)
        self.source_os.wait_snapshot_status(snapshot.id)
        derived = self.source_os.create_volume_from_snapshot(
            name=f"{name}-copy",
            snapshot_id=snapshot.id,
            size=size or 0,
            volume_type=volume_type,
            availability_zone=az,
        )
        self.source_os.wait_volume_status(derived.id)
        logging.info(
            "[MIGRATION] 源卷派生完成 volume=%s snapshot=%s derived=%s",
            volume_id,
            snapshot.id,
            derived.id,
        )
        return SourceCopy(snapshot_id=snapshot.id, derived_volume_id=derived.id)

    def create_target_volume(
        self,
        *,
        name: str,
        size: int,
        az: str,
        volume_type: str | None = None,
    ) -> str:
        """在目标云建空白卷，内容由中转机全量拷贝写入。"""
        target = self.target_os.create_blank_volume(
            name=name,
            size=size,
            volume_type=volume_type,
            availability_zone=az,
        )
        self.target_os.wait_volume_status(target.id)
        return target.id

    def attach(self, *, role: str, server_id: str, volume_id: str) -> str:
        os_utils = self._os_for(role)
        attachment_id = os_utils.attach_volume(
            server_id=server_id, volume_id=volume_id
        )
        os_utils.wait_volume_status(volume_id, target="in_use")
        return attachment_id

    def detach(self, *, role: str, server_id: str, volume_id: str) -> None:
        os_utils = self._os_for(role)
        if os_utils.find_volume_attachment(server_id, volume_id) is None:
            return
        os_utils.detach_volume(server_id=server_id, volume_id=volume_id)
        os_utils.wait_volume_status(volume_id, target="available")

    def cleanup_source_copy(self, copy: SourceCopy, relay_server_id: str) -> None:
        """清理派生卷与快照；任一步失败只记录，不阻塞后续清理。"""
        try:
            if (
                relay_server_id
                and self.source_os.find_volume_attachment(
                    relay_server_id, copy.derived_volume_id
                )
            ):
                self.detach(
                    role="source",
                    server_id=relay_server_id,
                    volume_id=copy.derived_volume_id,
                )
        except Exception:  # noqa: BLE001 - 清理是尽力而为
            logging.exception(
                "[MIGRATION] 卸载派生卷失败 volume=%s", copy.derived_volume_id
            )
        try:
            self.source_os.delete_volume(copy.derived_volume_id)
        except Exception:  # noqa: BLE001
            logging.exception(
                "[MIGRATION] 删除派生卷失败 volume=%s", copy.derived_volume_id
            )
        try:
            self.source_os.delete_volume_snapshot(copy.snapshot_id)
        except Exception:  # noqa: BLE001
            logging.exception("[MIGRATION] 删除快照失败 snapshot=%s", copy.snapshot_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_volumes -v`

Expected: PASS（9 个用例）

---

### Task 5: 单卷中转编排

**Files:**
- Create: `relay_orchestrator.py`
- Modify: `openstack_utils.py`（新增 `wait_attachment_device`）
- Modify: `relay_registry.py`（任务结果记录 + 任务绑定到指定 agent）
- Modify: `relay_api.py`（结果落库）
- Test: `tests/test_relay_orchestrator.py`

两个关键点决定了本任务的接口：

1. **任务必须绑定到具体 agent**。卷是挂到某一台中转机上的，若被同角色的另一台
   agent 领走，它看不到这个块设备。因此任务带 `agent_id`，派发时只匹配该 agent。
2. **设备路径由平台解析**。平台在 attach 后等待 attachment 的 `device` 字段
   （形如 `/dev/vdb`）非空，再把它写进任务下发，agent 不需要自己猜设备。

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from relay_orchestrator import RelayVolumeMover
from relay_pool import RelayNode


class _Sleeper:
    """让等待逻辑在测试里立即返回。"""

    def __init__(self):
        self.calls = 0

    def __call__(self, seconds):
        self.calls += 1


class _FakePool:
    def __init__(self, nodes):
        self._nodes = list(nodes)
        self.released = []

    def acquire(self, task_id):
        return self._nodes.pop(0) if self._nodes else None

    def release(self, node_id):
        self.released.append(node_id)


class _FakeState:
    def __init__(self):
        self.enqueued = []
        self.ready = set()
        self.results = {}

    def enqueue(self, task):
        self.enqueued.append(task)

    def mark_task_ready(self, task_id):
        self.ready.add(task_id)

    def task_ready(self, task_id):
        return task_id in self.ready

    def record_result(self, task_id, status, copied_bytes):
        self.results[task_id] = {"status": status, "copied_bytes": copied_bytes}

    def task_result(self, task_id):
        return self.results.get(task_id)


class _FakeLedger:
    def __init__(self):
        self.records = {}
        self.saved = 0

    def get(self, job_id, volume_id):
        return self.records.get(f"{job_id}:{volume_id}")

    def upsert(self, record):
        self.records[record.key] = record
        return record

    def save(self):
        self.saved += 1


class _Volume:
    def __init__(self, volume_id="vol-s1", size=4096):
        self.source_volume_id = volume_id
        self.size = size


class RelayVolumeMoverTest(unittest.TestCase):
    def setUp(self):
        from relay_ledger import VolumeTaskRecord
        from relay_volumes import SourceCopy

        self.VolumeTaskRecord = VolumeTaskRecord
        self.SourceCopy = SourceCopy
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.wait_attachment_device.return_value = "/dev/vdb"
        self.target_os.wait_attachment_device.return_value = "/dev/vdc"
        self.lifecycle = mock.MagicMock()
        self.lifecycle.source_os = self.source_os
        self.lifecycle.target_os = self.target_os
        self.lifecycle.create_source_copy.return_value = SourceCopy(
            snapshot_id="snap-1", derived_volume_id="vol-d1"
        )
        self.lifecycle.create_target_volume.return_value = "vol-t1"
        self.source_node = RelayNode(
            node_id="n-s", name="relay-source-0", role="source", az="az1",
            server_id="srv-s", session_id="sess-s", data_address="10.0.0.7",
        )
        self.target_node = RelayNode(
            node_id="n-t", name="relay-target-0", role="target", az="az2",
            server_id="srv-t", session_id="sess-t", data_address="10.0.0.8",
        )
        self.state = _FakeState()
        self.ledger = _FakeLedger()
        self.mover = RelayVolumeMover(
            source_pool=_FakePool([self.source_node]),
            target_pool=_FakePool([self.target_node]),
            lifecycle=self.lifecycle,
            state=self.state,
            ledger=self.ledger,
            job_id="job-1",
            sleeper=_Sleeper(),
        )

    def _run(self, volume=None):
        self.mover.move(
            volume=volume or _Volume(),
            vm_name="vm-1",
            index=0,
            source_az="az1",
            target_az="az2",
        )

    def test_move_happy_path_orders_tasks_target_first(self):
        def auto_complete():
            # 目标端 ready 后立刻补上两端结果，模拟 agent 正常完成。
            for task in self.state.enqueued:
                self.state.mark_task_ready(task["task_id"])
                self.state.record_result(task["task_id"], "done", 4096)

        self.state.enqueue = lambda task: (
            self.state.enqueued.append(task),
            auto_complete(),
        )

        self._run()

        roles = [task["role"] for task in self.state.enqueued]
        self.assertEqual(roles, ["target", "source"])
        target_task = self.state.enqueued[0]
        source_task = self.state.enqueued[1]
        self.assertEqual(target_task["agent_id"], "n-t")
        self.assertEqual(target_task["dst_path"], "/dev/vdc")
        self.assertEqual(source_task["agent_id"], "n-s")
        self.assertEqual(source_task["src_path"], "/dev/vdb")
        self.assertEqual(source_task["peer_host"], "10.0.0.8")
        self.assertEqual(source_task["peer_port"], 9200)
        self.assertEqual(source_task["ticket"], target_task["ticket"])

    def test_move_cleans_up_and_releases_on_success(self):
        self.state.enqueue = lambda task: (
            self.state.enqueued.append(task),
            self.state.mark_task_ready(task["task_id"]),
            self.state.record_result(task["task_id"], "done", 4096),
        )

        self._run()

        self.lifecycle.cleanup_source_copy.assert_called_once()
        self.lifecycle.detach.assert_any_call(
            role="target", server_id="srv-t", volume_id="vol-t1"
        )

    def test_move_marks_ledger_copying_then_done(self):
        seen = []
        # 每次落库都记录一次阶段，便于断言阶段顺序。
        original_save = self.ledger.save

        def save():
            original_save()
            seen.extend(r.phase for r in self.ledger.records.values())

        self.ledger.save = save
        self.state.enqueue = lambda task: (
            self.state.enqueued.append(task),
            self.state.mark_task_ready(task["task_id"]),
            self.state.record_result(task["task_id"], "done", 4096),
        )

        self._run()
        self.ledger.save()

        self.assertIn("copying", seen)
        self.assertEqual(
            self.ledger.get("job-1", "vol-s1").phase, "done"
        )

    def test_move_raises_when_agent_reports_failure(self):
        self.state.enqueue = lambda task: (
            self.state.enqueued.append(task),
            self.state.mark_task_ready(task["task_id"]),
            self.state.record_result(task["task_id"], "failed", 0),
        )

        with self.assertRaises(RuntimeError):
            self._run()

        self.lifecycle.cleanup_source_copy.assert_called_once()

    def test_move_raises_when_pool_exhausted(self):
        self.mover.source_pool = _FakePool([])
        self.lifecycle.attach.side_effect = None

        with self.assertRaises(RuntimeError):
            self._run()

    def test_move_records_partial_offset_in_ledger(self):
        record = self.VolumeTaskRecord(
            job_id="job-1", vm_id="vm-1", volume_id="vol-s1", copied_bytes=1024
        )
        self.ledger.upsert(record)
        self.state.enqueue = lambda task: (
            self.state.enqueued.append(task),
            self.state.mark_task_ready(task["task_id"]),
            self.state.record_result(task["task_id"], "done", 3072),
        )

        self.mover.move(
            volume=_Volume(size=4096),
            vm_name="vm-1",
            index=0,
            source_az="az1",
            target_az="az2",
        )

        source_task = self.state.enqueued[-1]
        self.assertEqual(source_task["offset"], 1024)
        self.assertEqual(source_task["length"], 3072)


class PinnedDispatchTest(unittest.TestCase):
    def setUp(self):
        from relay_registry import RelayState

        self.state = RelayState(secret=b"secret")

    def _agent(self, name, role="source"):
        return self.state.register(
            job_id="job-1", role=role, name=name, version="1.0.0",
            address="10.0.0.1", now=1000.0,
        )

    def test_dispatch_skips_task_pinned_to_other_agent(self):
        agent_a = self._agent("relay-source-0")
        agent_b = self._agent("relay-source-1")
        self.state.enqueue({"task_id": "t-b", "role": "source", "agent_id": agent_b.agent_id})
        self.state.enqueue({"task_id": "t-a", "role": "source", "agent_id": agent_a.agent_id})

        task = self.state.dispatch(agent_a.session_id)

        self.assertEqual(task["task_id"], "t-a")
        self.assertEqual(self.state.task("t-b")["task_id"], "t-b")

    def test_dispatch_returns_unpinned_task_to_any_agent(self):
        agent = self._agent("relay-source-0")
        self.state.enqueue({"task_id": "t-1", "role": "source"})

        self.assertEqual(self.state.dispatch(agent.session_id)["task_id"], "t-1")

    def test_record_and_read_task_result(self):
        self.assertIsNone(self.state.task_result("t-1"))
        self.state.record_result("t-1", "done", 4096)
        self.assertEqual(self.state.task_result("t-1")["copied_bytes"], 4096)


class AttachmentDeviceTest(unittest.TestCase):
    def setUp(self):
        from openstack_utils import OpenStackUtils

        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)
        self.sleeper = _Sleeper()

    def test_wait_attachment_device_returns_device(self):
        self.conn.compute.volume_attachments.return_value = [
            mock.Mock(id="att-1", volume_id="vol-1", device="/dev/vdb")
        ]

        device = self.os_utils.wait_attachment_device(
            "srv-1", "vol-1", sleeper=self.sleeper, timeout=1
        )

        self.assertEqual(device, "/dev/vdb")

    def test_wait_attachment_device_retries_until_device_present(self):
        self.conn.compute.volume_attachments.side_effect = [
            [mock.Mock(id="att-1", volume_id="vol-1", device="")],
            [mock.Mock(id="att-1", volume_id="vol-1", device="/dev/vdb")],
        ]

        device = self.os_utils.wait_attachment_device(
            "srv-1", "vol-1", sleeper=self.sleeper, timeout=5
        )

        self.assertEqual(device, "/dev/vdb")
        self.assertEqual(self.sleeper.calls, 1)

    def test_wait_attachment_device_times_out(self):
        self.conn.compute.volume_attachments.return_value = []

        with self.assertRaises(TimeoutError):
            self.os_utils.wait_attachment_device(
                "srv-1", "vol-1", sleeper=self.sleeper, timeout=0
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_orchestrator -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_orchestrator'`

- [ ] **Step 3: Write minimal implementation**

`openstack_utils.py` 新增设备路径等待（放在 `find_volume_attachment` 之后）：

```python
    def wait_attachment_device(
        self,
        server_id: str,
        volume_id: str,
        *,
        timeout: float = 120.0,
        poll_interval: float = 2.0,
        sleeper=time.sleep,
    ) -> str:
        """等待 attachment 的 device 字段就绪，返回块设备路径。"""
        deadline = time.monotonic() + timeout
        while True:
            for attachment in self.conn.compute.volume_attachments(server_id):
                if attachment.volume_id == volume_id and attachment.device:
                    return attachment.device
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"等待卷 {volume_id} 在实例 {server_id} 上的设备路径超时"
                )
            sleeper(poll_interval)
```

`relay_registry.py`：任务派发支持按 agent 绑定，并记录结果。

```python
    def enqueue(self, task: dict[str, Any]) -> None:
        self._tasks[task["task_id"]] = task
        self._queues[task["role"]].append(task)

    def dispatch(self, session_id: str) -> dict[str, Any] | None:
        agent = self.by_session(session_id)
        if agent is None or agent.state != "ready":
            return None
        queue = self._queues.get(agent.role)
        if not queue:
            return None
        for task in list(queue):
            pinned = task.get("agent_id")
            if pinned and pinned != agent.agent_id:
                continue
            queue.remove(task)
            agent.state = "busy"
            agent.current_task_id = task["task_id"]
            return task
        return None

    def record_result(self, task_id: str, status: str, copied_bytes: int) -> None:
        self._results[task_id] = {"status": status, "copied_bytes": copied_bytes}

    def task_result(self, task_id: str) -> dict[str, Any] | None:
        return self._results.get(task_id)
```

`RelayState.__init__` 补 `self._results: dict[str, dict[str, Any]] = {}`。

`relay_api.py` 的 `task_result` 端点补一行落库：

```python
        state.record_result(
            task_id,
            str(body.get("status", "")),
            int(body.get("copied_bytes", 0)),
        )
```

新建 `relay_orchestrator.py`：

```python
"""单卷中转编排：快照派生、挂载、下发任务、等待完成、清理。"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Callable

from relay_protocol import DEFAULT_CHUNK
from relay_volumes import VolumeLifecycle


def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float,
    poll_interval: float,
    sleeper: Callable[[float], None],
    message: str,
) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if predicate():
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(message)
        sleeper(poll_interval)


class RelayVolumeMover:
    """按设计文档 8.1 的单条流程搬运一个卷。"""

    def __init__(
        self,
        *,
        source_pool: Any,
        target_pool: Any,
        lifecycle: VolumeLifecycle,
        state: Any,
        ledger: Any,
        job_id: str,
        chunk_size: int = DEFAULT_CHUNK,
        rate_limit_bytes_per_sec: float = 0.0,
        ready_timeout: float = 180.0,
        result_timeout: float = 6 * 3600.0,
        poll_interval: float = 1.0,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.source_pool = source_pool
        self.target_pool = target_pool
        self.lifecycle = lifecycle
        self.state = state
        self.ledger = ledger
        self.job_id = job_id
        self.chunk_size = chunk_size
        self.rate_limit_bytes_per_sec = rate_limit_bytes_per_sec
        self.ready_timeout = ready_timeout
        self.result_timeout = result_timeout
        self.poll_interval = poll_interval
        self.sleeper = sleeper

    def record(self, volume: Any):
        from relay_ledger import VolumeTaskRecord

        record = self.ledger.get(self.job_id, volume.source_volume_id)
        if record is None:
            record = VolumeTaskRecord(
                job_id=self.job_id, vm_id="", volume_id=volume.source_volume_id
            )
        return record

    def _save(self, record, **fields) -> None:
        for key, value in fields.items():
            setattr(record, key, value)
        record.updated_at = time.time()
        self.ledger.upsert(record)
        self.ledger.save()

    def move(
        self,
        *,
        volume: Any,
        vm_name: str,
        index: int,
        source_az: str,
        target_az: str,
    ) -> None:
        record = self.record(volume)
        source_node = target_node = None
        copy = None
        cleaned = False
        target_volume_id = ""
        started_bytes = max(int(record.copied_bytes or 0), 0)
        self._save(record, phase="snapshotting")
        try:
            copy = self.lifecycle.create_source_copy(
                volume_id=volume.source_volume_id,
                vm_name=vm_name,
                index=index,
                az=source_az,
                size=int(volume.size or 0),
            )
            self._save(
                record,
                phase="cloning",
                snapshot_id=copy.snapshot_id,
                derived_volume_id=copy.derived_volume_id,
            )
            target_volume_id = self.lifecycle.create_target_volume(
                name=f"{vm_name}-vol-{index}", size=int(volume.size or 0), az=target_az
            )
            self._save(record, phase="attaching_target", target_volume_id=target_volume_id)

            source_node = self.source_pool.acquire(volume.source_volume_id)
            target_node = self.target_pool.acquire(volume.source_volume_id)
            if source_node is None or target_node is None:
                raise RuntimeError("中转机池没有空闲机器，无法继续拷贝")
            self._save(
                record,
                source_relay_id=source_node.server_id,
                target_relay_id=target_node.server_id,
                phase="attaching_source",
            )

            self.lifecycle.attach(
                role="target", server_id=target_node.server_id, volume_id=target_volume_id
            )
            self.lifecycle.attach(
                role="source",
                server_id=source_node.server_id,
                volume_id=copy.derived_volume_id,
            )
            source_device = self.lifecycle.source_os.wait_attachment_device(
                source_node.server_id, copy.derived_volume_id
            )
            target_device = self.lifecycle.target_os.wait_attachment_device(
                target_node.server_id, target_volume_id
            )
            self._save(record, phase="copying")
            self._transfer(
                volume=volume,
                record=record,
                source_node=source_node,
                target_node=target_node,
                source_device=source_device,
                target_device=target_device,
                offset=started_bytes,
            )
            self._save(record, phase="detaching", copied_bytes=int(volume.size or 0))
            self.lifecycle.detach(
                role="target", server_id=target_node.server_id, volume_id=target_volume_id
            )
            self._save(record, phase="cleaning")
            self.lifecycle.cleanup_source_copy(copy, source_node.server_id)
            cleaned = True
            self._save(record, phase="done")
            return {
                "target_volume_id": target_volume_id,
                "source_volume_id": volume.source_volume_id,
                "size": int(volume.size or 0),
            }
        except Exception:
            self._save(record, phase="failed")
            raise
        finally:
            if copy is not None and source_node is not None and not cleaned:
                try:
                    self.lifecycle.cleanup_source_copy(copy, source_node.server_id)
                except Exception:  # noqa: BLE001 - 清理失败不覆盖原始错误
                    logging.exception("[MIGRATION] 兜底清理派生卷失败")
            if source_node is not None:
                self.source_pool.release(source_node.node_id)
            if target_node is not None:
                self.target_pool.release(target_node.node_id)

    def _transfer(
        self,
        *,
        volume: Any,
        record: Any,
        source_node: Any,
        target_node: Any,
        source_device: str,
        target_device: str,
        offset: int,
    ) -> None:
        total = int(volume.size or 0)
        ticket = uuid.uuid4().hex
        target_task_id = f"{ticket}-t"
        source_task_id = f"{ticket}-s"
        common = {
            "job_id": self.job_id,
            "volume_id": volume.source_volume_id,
            "vm_id": getattr(record, "vm_id", ""),
            "ticket": ticket,
            "offset": offset,
            "length": max(total - offset, 1),
            "chunk_size": self.chunk_size,
        }
        self.state.enqueue(
            dict(
                common,
                task_id=target_task_id,
                role="target",
                agent_id=target_node.node_id,
                dst_path=target_device,
                listen_host="0.0.0.0",
                listen_port=target_node.data_port,
            )
        )
        _wait_until(
            lambda: self.state.task_ready(target_task_id),
            timeout=self.ready_timeout,
            poll_interval=self.poll_interval,
            sleeper=self.sleeper,
            message="目标端中转机未能在超时前进入监听状态",
        )
        self.state.enqueue(
            dict(
                common,
                task_id=source_task_id,
                role="source",
                agent_id=source_node.node_id,
                src_path=source_device,
                peer_host=target_node.data_address,
                peer_port=target_node.data_port,
                rate_limit_bytes_per_sec=self.rate_limit_bytes_per_sec,
            )
        )
        _wait_until(
            lambda: self.state.task_result(target_task_id) is not None
            and self.state.task_result(source_task_id) is not None,
            timeout=self.result_timeout,
            poll_interval=self.poll_interval,
            sleeper=self.sleeper,
            message="块拷贝任务超时未返回结果",
        )
        for task_id in (target_task_id, source_task_id):
            result = self.state.task_result(task_id)
            if result.get("status") != "done":
                raise RuntimeError(
                    f"块拷贝任务 {task_id} 失败: {result.get('status')}"
                )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_orchestrator -v`

Expected: PASS（9 个用例）

- [ ] **Step 5: Run the full suite to confirm no regression**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS，无失败用例

---

### Task 6: 对账与清理

**Files:**
- Create: `relay_reaper.py`
- Test: `tests/test_relay_reaper.py`

reaper 是"资源不堆积"的最后一道保险：agent 崩溃或平台重启后，
控制面的请求可能丢失，只有按台账对账才能把孤儿资源收回来。

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from relay_ledger import Ledger, VolumeTaskRecord
from relay_reaper import RelayReaper


class RelayReaperTest(unittest.TestCase):
    def setUp(self):
        self.ledger = mock.MagicMock()
        self.lifecycle = mock.MagicMock()
        self.reaper = RelayReaper(
            ledger=self.ledger,
            lifecycle=self.lifecycle,
            pools={"source": mock.MagicMock(), "target": mock.MagicMock()},
            stale_seconds=600.0,
        )

    def _record(self, **kwargs):
        defaults = dict(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="copying",
            derived_volume_id="vol-d1",
            snapshot_id="snap-1",
            source_relay_id="n-s",
            target_volume_id="vol-t1",
            target_relay_id="n-t",
            updated_at=1000.0,
        )
        defaults.update(kwargs)
        return VolumeTaskRecord(**defaults)

    def test_sweep_skips_fresh_records(self):
        self.ledger.all.return_value = [self._record(updated_at=1000.0)]

        cleaned = self.reaper.sweep(now=1100.0)

        self.assertEqual(cleaned, [])
        self.lifecycle.cleanup_source_copy.assert_not_called()

    def test_sweep_cleans_stale_record(self):
        self.ledger.all.return_value = [self._record(updated_at=1000.0)]

        cleaned = self.reaper.sweep(now=2000.0)

        self.assertEqual(cleaned, ["job-1:vol-s1"])
        self.lifecycle.cleanup_source_copy.assert_called_once()
        copy_arg = self.lifecycle.cleanup_source_copy.call_args.args[0]
        self.assertEqual(copy_arg.derived_volume_id, "vol-d1")
        self.assertEqual(
            self.lifecycle.cleanup_source_copy.call_args.args[1], "n-s"
        )

    def test_sweep_detaches_target_volume(self):
        self.ledger.all.return_value = [self._record(updated_at=1000.0)]

        self.reaper.sweep(now=2000.0)

        self.lifecycle.detach.assert_any_call(
            role="target", server_id="n-t", volume_id="vol-t1"
        )

    def test_sweep_marks_record_cleaned_and_saves(self):
        record = self._record(updated_at=1000.0)
        self.ledger.all.return_value = [record]

        self.reaper.sweep(now=2000.0)

        self.assertEqual(record.phase, "cleaned")
        self.ledger.save.assert_called()

    def test_sweep_skips_terminal_records(self):
        self.ledger.all.return_value = [
            self._record(phase="done", updated_at=1000.0),
            self._record(phase="cleaned", updated_at=1000.0),
        ]

        self.assertEqual(self.reaper.sweep(now=2000.0), [])

    def test_sweep_continues_after_cleanup_error(self):
        self.lifecycle.cleanup_source_copy.side_effect = RuntimeError("boom")
        self.ledger.all.return_value = [
            self._record(volume_id="vol-a", updated_at=1000.0),
            self._record(volume_id="vol-b", updated_at=1000.0),
        ]

        cleaned = self.reaper.sweep(now=2000.0)

        self.assertEqual(len(cleaned), 2)
        self.assertEqual(self.lifecycle.cleanup_source_copy.call_count, 2)

    def test_reconcile_job_cleans_every_unfinished_record(self):
        self.ledger.all.return_value = [
            self._record(updated_at=1000.0),
            self._record(volume_id="vol-s2", phase="done", updated_at=1000.0),
        ]

        cleaned = self.reaper.reconcile_job("job-1")

        self.assertEqual(cleaned, ["job-1:vol-s1"])

    def test_real_ledger_round_trip(self):
        import tempfile
        from pathlib import Path

        tmp = tempfile.TemporaryDirectory()
        ledger = Ledger.load(Path(tmp.name) / "ledger.json")
        ledger.upsert(self._record(updated_at=1000.0))
        reaper = RelayReaper(
            ledger=ledger,
            lifecycle=self.lifecycle,
            pools={},
            stale_seconds=600.0,
        )

        cleaned = reaper.sweep(now=2000.0)

        self.assertEqual(cleaned, ["job-1:vol-s1"])
        self.assertEqual(ledger.get("job-1", "vol-s1").phase, "cleaned")
        tmp.cleanup()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_reaper -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_reaper'`

- [ ] **Step 3: Write minimal implementation**

```python
"""周期对账：把超过阈值仍未完成的卷任务清理掉，避免资源堆积。"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from relay_volumes import SourceCopy

TERMINAL_PHASES = {"done", "cleaned", "failed_retained"}


class RelayReaper:
    def __init__(
        self,
        *,
        ledger: Any,
        lifecycle: Any,
        pools: dict[str, Any],
        stale_seconds: float = 600.0,
        now: Callable[[], float] = time.time,
    ):
        self.ledger = ledger
        self.lifecycle = lifecycle
        self.pools = pools
        self.stale_seconds = stale_seconds
        self._now = now

    def sweep(self, *, now: float | None = None) -> list[str]:
        """清理超时未完成的卷任务，返回被清理的 key 列表。"""
        current = self._now() if now is None else now
        cleaned: list[str] = []
        for record in list(self.ledger.all()):
            if record.phase in TERMINAL_PHASES:
                continue
            if current - float(record.updated_at or 0.0) < self.stale_seconds:
                continue
            self._cleanup(record)
            record.phase = "cleaned"
            record.updated_at = current
            self.ledger.upsert(record)
            cleaned.append(record.key)
        if cleaned:
            self.ledger.save()
        return cleaned

    def reconcile_job(self, job_id: str) -> list[str]:
        """作业收尾：不管是否超时，清理该作业所有未完成记录。"""
        cleaned: list[str] = []
        current = self._now()
        for record in list(self.ledger.all()):
            if record.job_id != job_id or record.phase in TERMINAL_PHASES:
                continue
            self._cleanup(record)
            record.phase = "cleaned"
            record.updated_at = current
            self.ledger.upsert(record)
            cleaned.append(record.key)
        if cleaned:
            self.ledger.save()
        return cleaned

    def _cleanup(self, record: Any) -> None:
        try:
            if record.derived_volume_id:
                self.lifecycle.cleanup_source_copy(
                    SourceCopy(
                        snapshot_id=record.snapshot_id,
                        derived_volume_id=record.derived_volume_id,
                    ),
                    record.source_relay_id,
                )
        except Exception:  # noqa: BLE001 - 清理失败不能中断其它记录
            logging.exception("[MIGRATION] 对账清理派生卷失败 key=%s", record.key)
        try:
            if record.target_volume_id and record.target_relay_id:
                self.lifecycle.detach(
                    role="target",
                    server_id=record.target_relay_id,
                    volume_id=record.target_volume_id,
                )
        except Exception:  # noqa: BLE001
            logging.exception("[MIGRATION] 对账卸载目标卷失败 key=%s", record.key)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_reaper -v`

Expected: PASS（8 个用例）

---

### Task 7: 接入 MigrationManager 与端到端集成

**Files:**
- Modify: `migration_manager.py`
- Test: `tests/test_relay_migration_integration.py`

- [ ] **Step 1: Write the failing test**

```python
import os
import tempfile
import threading
import time
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

    def report_progress(self, task_id, copied_bytes):
        pass

    def report_ready(self, task_id):
        self.state.mark_task_ready(task_id)

    def report_result(self, task_id, status, copied_bytes):
        self.state.record_result(task_id, status, copied_bytes)
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

    def release(self, node_id):
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
        import socket

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
            mover.move(
                volume=mock.Mock(source_volume_id="vol-s1", size=len(self.payload)),
                vm_name="vm-1",
                index=0,
                source_az="az1",
                target_az="az2",
            )
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)

        self.assertEqual(self.dst.read_bytes(), self.payload)
        self.assertEqual(ledger.get("job-1", "vol-s1").phase, "done")


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
        self.manager._resolve_source_server = mock.Mock(
            return_value=mock.Mock(id="srv-src")
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
        self.mover_factory = mock.Mock(return_value=self.mover)
        self.manager.target_os.create_server_from_volumes.return_value = mock.Mock(
            id="srv-tgt"
        )
        # 两个源卷各自返回一个目标卷 id，顺序与 boot + data 一致。
        self.mover.move.side_effect = [
            {"target_volume_id": "vol-t0", "size": 40, "source_volume_id": "vol-s1"},
            {"target_volume_id": "vol-t1", "size": 20, "source_volume_id": "vol-s2"},
        ]

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

        self.assertEqual(self.mover.move.call_count, 2)
        first = self.mover.move.call_args_list[0].kwargs
        self.assertEqual(first["index"], 0)
        kwargs = self.manager.target_os.create_server_from_volumes.call_args.kwargs
        self.assertEqual(kwargs["boot_volume_id"], "vol-t0")
        self.assertEqual(kwargs["data_volume_ids"], ["vol-t1"])
        self.manager.target_os.start_server.assert_called_once_with("srv-tgt")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_migration_integration -v`

（需要非沙箱模式，端到端用例会创建 socket）

Expected: FAIL，`migration_manager` 走的是原有 RBD 路径或报缺少分支

- [ ] **Step 3: Write minimal implementation**

`relay_orchestrator.py` 的 `move` 在 Task 5 里已返回
`{"target_volume_id", "source_volume_id", "size"}` 字典，本任务直接使用。

`migration_manager.py`：

1. 在 `_migrate_vm_inner` 最前面插入分流：

```python
    def _migrate_vm_inner(self, vm: VmTask, options: dict[str, Any]) -> None:
        if str(options.get("data_channel") or "rbd") == "relay":
            return self._migrate_vm_via_relay(vm, options)
        ...  # 原有实现不变
```

2. 新增 `_migrate_vm_via_relay`：

```python
    def _migrate_vm_via_relay(self, vm: VmTask, options: dict[str, Any]) -> None:
        """中转机通道：停源 → 逐卷快照派生并拷贝 → 用目标卷建 VM。"""
        mover_factory = options.get("relay_mover_factory")
        if mover_factory is None:
            raise RuntimeError("未配置中转机搬运器，无法走中转机通道")

        self._check_stop()
        source_server = self._resolve_source_server(vm)
        source_flavor = self.source_os.get_server_flavor_spec(source_server)
        target_flavor_id = self._resolve_flavor_id(vm, source_flavor)

        port_ids = self._create_target_ports(vm)
        if not port_ids:
            raise RuntimeError("目标 VM 没有可用端口，源 VM 必须至少有一个固定 IP")

        entries = order_system_and_data(
            self.source_os.get_server_volumes_with_device(source_server.id)
        )
        boot_entries = [entry for entry in entries if entry["is_bootable"]]
        data_entries = [entry for entry in entries if not entry["is_bootable"]]
        if len(boot_entries) != 1:
            raise RuntimeError(
                f"源 VM {vm.name} 必须有且只有一个系统盘，当前 {len(boot_entries)} 个"
            )

        # 中转机通道要求源数据在拷贝期间不变，因此先停源 VM 再打快照。
        self._set_phase(vm, VmStatus.STOPPING_SOURCE, "relay_stopping_source")
        self._stop_and_wait(source_server.id, self.source_os)

        mover = mover_factory(vm, options)
        self._set_phase(vm, VmStatus.COPYING_VOLUMES, "relay_copying")
        target_volume_ids: list[str] = []
        for index, entry in enumerate([boot_entries[0]] + data_entries):
            result = mover.move(
                volume=SimpleNamespace(
                    source_volume_id=entry["volume_id"],
                    size=int(entry.get("size") or 0),
                ),
                vm_name=vm.name,
                index=index,
                source_az=str(options.get("source_az") or vm.target_az),
                target_az=vm.target_az,
            )
            target_volume_ids.append(str(result["target_volume_id"]))

        self._set_phase(vm, VmStatus.CREATING_TARGET_BFV, "relay_creating_target_vm")
        server = self.target_os.create_server_from_volumes(
            name=f"{vm.name}-relay",
            flavor_id=target_flavor_id,
            port_ids=port_ids,
            boot_volume_id=target_volume_ids[0],
            data_volume_ids=target_volume_ids[1:],
            availability_zone=vm.target_az,
            admin_password=str(options.get("admin_password") or default_vm_pass),
        )
        vm.target_server_id = server.id
        vm.created_resources.append({"type": "server", "id": server.id})
        vm.target_network_ports = list(vm.target_network_ports or port_ids)

        self._set_phase(vm, VmStatus.STARTING_TARGET, "starting_target")
        self.target_os.start_server(server.id)
        self.target_os.wait_server_status(server.id, target="ACTIVE")
        self._set_phase(vm, VmStatus.VERIFYING, "verifying")
```

`migration_manager.py` 顶部确认已导入 `default_vm_pass`（`from config import default_vm_pass`），
并补 `from types import SimpleNamespace`（把源卷条目转成搬运器接受的入参对象）。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_migration_integration -v`

Expected: PASS（2 个用例）

- [ ] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS，无失败用例

---

## 完成标准

- 全量测试套件通过；
- 两台 relay agent 能用真实 socket 完成一个卷的全量拷贝，台账阶段收敛到 `done`；
- 中转机池能按 (云, AZ) 建池、按名绑定 agent、串行复用、心跳超时标 unhealthy、作业收尾销毁；
- 源卷全程不 detach，派生卷与快照在成功、失败、对账三条路径上都会被清理；
- `MigrationManager` 在 `data_channel=relay` 时走新分支，默认仍走原 RBD 路径不变；
- reaper 能把超时未完成的卷任务收敛到 `cleaned`。

## 后续

第 3 份计划（页面）负责把中转机池配置、数据通道选择与池监控接进
`templates/index.html` 的三步向导，并在 `app.py` 里把表单参数组装成
`RelayPool` / `RelayVolumeMover`，通过 `relay_mover_factory` 注入本计划的编排层。

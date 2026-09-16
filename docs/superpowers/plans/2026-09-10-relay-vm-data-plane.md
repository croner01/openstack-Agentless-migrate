# 中转机全量迁移（1/3）：数据面与最小控制面 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让两台 relay agent 能在 IP 网络上完成一次带限速、进度上报、断点续传与完成校验的块设备全量拷贝，并由平台侧提供注册与任务下发的最小控制面。

**Architecture:** 共享协议层（`relay_protocol.py`）定义帧格式、握手与令牌；平台侧 `relay_registry.py` 维护 agent 会话、`relay_api.py` 暴露 `/api/relay/*`、`relay_ledger.py` 负责台账落盘；数据面 `relay_transfer.py` 实现发送与接收，`relay_agent.py` 是运行在中转机镜像里的常驻进程。agent 只发起出向请求，取消指令由心跳响应下发。

**Tech Stack:** Python 3.10、Flask、标准库 `socket`/`struct`/`hmac`/`urllib`、`unittest`。

---

## 说明

- 本计划是同名设计文档三份计划中的第 1 份：
  - **1/3 本计划**：数据面与最小控制面（agent、块流、台账、relay API）；
  - 2/3：OpenStack 编排与卷生命周期（池、快照派生、attach/detach、调度、reaper）；
  - 3/3：页面改动。
- 设计文档：`docs/superpowers/specs/2026-09-10-relay-vm-full-copy-migration-design.md`。
- 当前目录不是 git 仓库（`.git` 为空），所有任务跳过 commit 步骤。
- 本计划新增 Python 模块，部署时需同步更新平台 ConfigMap 的 `--from-file` 列表，
  并为中转机镜像单独打包 `relay_agent.py`、`relay_transfer.py`、`relay_protocol.py`。
- 所有测试用普通文件模拟块设备，不接触真实云与真实存储。

## 文件结构

| 文件 | 职责 |
| --- | --- |
| `relay_protocol.py` | 帧编解码、握手、注册令牌（无 I/O，平台与 agent 共用） |
| `relay_ledger.py` | 卷任务台账的持久化与查询（平台侧） |
| `relay_registry.py` | agent 注册表、心跳、会话与任务队列（平台侧） |
| `relay_api.py` | `/api/relay/*` Flask 蓝图（平台侧） |
| `relay_transfer.py` | 块流发送与接收、限速、尾部校验（agent 侧） |
| `relay_agent.py` | agent 常驻进程：注册、心跳、领任务、执行、上报 |
| `tests/test_relay_protocol.py` | 协议层单元测试 |
| `tests/test_relay_ledger.py` | 台账单元测试 |
| `tests/test_relay_registry.py` | 注册表单元测试 |
| `tests/test_relay_api.py` | 接口层单元测试 |
| `tests/test_relay_transfer.py` | 传输层单元测试 |
| `tests/test_relay_agent.py` | agent 主循环与端到端集成测试 |

---

### Task 1: 共享帧协议

**Files:**
- Create: `relay_protocol.py`
- Test: `tests/test_relay_protocol.py`

- [ ] **Step 1: Write the failing test**

```python
import io
import unittest

from relay_protocol import (
    DEFAULT_CHUNK,
    MAGIC,
    ProtocolError,
    crc32,
    encode_chunk,
    read_chunk,
)


class ChunkFramingTest(unittest.TestCase):
    def test_roundtrip_preserves_offset_and_payload(self):
        frame = encode_chunk(4096, b"hello-block")

        stream = io.BytesIO(frame)
        offset, payload = read_chunk(stream)

        self.assertEqual(offset, 4096)
        self.assertEqual(payload, b"hello-block")

    def test_read_chunk_reports_end_of_stream(self):
        self.assertEqual(read_chunk(io.BytesIO(b"")), (-1, b""))

    def test_read_chunk_rejects_crc_mismatch(self):
        frame = bytearray(encode_chunk(0, b"payload"))
        frame[-1] ^= 0xFF

        with self.assertRaises(ProtocolError):
            read_chunk(io.BytesIO(bytes(frame)))

    def test_read_chunk_rejects_bad_magic(self):
        frame = bytearray(encode_chunk(0, b"payload"))
        frame[0:4] = b"XXXX"

        with self.assertRaises(ProtocolError):
            read_chunk(io.BytesIO(bytes(frame)))

    def test_read_chunk_rejects_truncated_payload(self):
        frame = encode_chunk(0, b"payload")

        with self.assertRaises(ProtocolError):
            read_chunk(io.BytesIO(frame[:-2]))

    def test_encode_chunk_rejects_payload_over_limit(self):
        with self.assertRaises(ProtocolError):
            encode_chunk(0, b"x" * (DEFAULT_CHUNK * 4))

    def test_crc32_matches_zlib(self):
        import zlib

        self.assertEqual(crc32(b"abc"), zlib.crc32(b"abc") & 0xFFFFFFFF)

    def test_magic_is_four_bytes(self):
        self.assertEqual(len(MAGIC), 4)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_protocol -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_protocol'`

- [ ] **Step 3: Write minimal implementation**

```python
"""中转机数据面的共享协议：帧编解码。

平台与 relay agent 共用本模块，模块内不做任何 I/O。
"""
from __future__ import annotations

import struct
import zlib
from typing import IO

MAGIC = b"RLY1"
HEADER = struct.Struct("!4sQQI")  # magic, offset, length, crc32
HEADER_SIZE = HEADER.size
DEFAULT_CHUNK = 4 * 1024 * 1024
MAX_CHUNK = 8 * 1024 * 1024
HANDSHAKE_LIMIT = 4096


class ProtocolError(ValueError):
    """帧格式非法、校验失败或握手内容非法。"""


def crc32(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def encode_chunk(offset: int, payload: bytes) -> bytes:
    """把一个数据块编码成帧。"""
    if not payload:
        raise ProtocolError("empty payload")
    if len(payload) > MAX_CHUNK:
        raise ProtocolError("payload too large")
    return HEADER.pack(MAGIC, offset, len(payload), crc32(payload)) + payload


def read_chunk(stream: IO[bytes]) -> tuple[int, bytes]:
    """从二进制流读取一帧，流结束时返回 (-1, b"")。"""
    header = stream.read(HEADER_SIZE)
    if not header:
        return -1, b""
    if len(header) < HEADER_SIZE:
        raise ProtocolError("truncated header")
    magic, offset, length, checksum = HEADER.unpack(header)
    if magic != MAGIC:
        raise ProtocolError("bad magic")
    if length == 0 or length > MAX_CHUNK:
        raise ProtocolError("bad length")
    payload = stream.read(length)
    if len(payload) != length:
        raise ProtocolError("truncated payload")
    if crc32(payload) != checksum:
        raise ProtocolError("crc mismatch")
    return offset, payload
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_protocol -v`

Expected: PASS（8 个用例）

---

### Task 2: 握手与注册令牌

**Files:**
- Modify: `tests/test_relay_protocol.py`

- [ ] **Step 1: Write the failing test**

在 `tests/test_relay_protocol.py` 顶部导入区追加：

```python
from relay_protocol import (
    decode_handshake,
    encode_handshake,
    issue_token,
    verify_token,
)
```

在文件末尾追加：

```python
class TokenTest(unittest.TestCase):
    SECRET = b"unit-test-secret"

    def test_verify_token_returns_job_and_role(self):
        token = issue_token(self.SECRET, "job-1", "source", now=1000, ttl=300)

        self.assertEqual(
            verify_token(self.SECRET, token, now=1100), ("job-1", "source")
        )

    def test_verify_token_rejects_expired(self):
        token = issue_token(self.SECRET, "job-1", "target", now=1000, ttl=60)

        with self.assertRaises(ProtocolError):
            verify_token(self.SECRET, token, now=1100)

    def test_verify_token_rejects_tampered_role(self):
        token = issue_token(self.SECRET, "job-1", "target", now=1000, ttl=300)
        forged = token.replace("|target|", "|source|")

        with self.assertRaises(ProtocolError):
            verify_token(self.SECRET, forged, now=1100)

    def test_verify_token_rejects_other_secret(self):
        token = issue_token(self.SECRET, "job-1", "source", now=1000, ttl=300)

        with self.assertRaises(ProtocolError):
            verify_token(b"other-secret", token, now=1100)

    def test_verify_token_rejects_malformed(self):
        with self.assertRaises(ProtocolError):
            verify_token(self.SECRET, "not-a-token", now=1100)


class HandshakeTest(unittest.TestCase):
    def test_handshake_roundtrip(self):
        line = encode_handshake("ticket-1", 1024, 2048)

        self.assertEqual(
            decode_handshake(line),
            {"ticket": "ticket-1", "offset": 1024, "length": 2048},
        )

    def test_decode_handshake_rejects_missing_ticket(self):
        with self.assertRaises(ProtocolError):
            decode_handshake(b'{"offset": 0, "length": 10}\n')

    def test_decode_handshake_rejects_negative_offset(self):
        with self.assertRaises(ProtocolError):
            decode_handshake(b'{"ticket": "t", "offset": -1, "length": 10}\n')
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_protocol -v`

Expected: FAIL with `ImportError: cannot import name 'decode_handshake'`

- [ ] **Step 3: Write minimal implementation**

在 `relay_protocol.py` 顶部导入区补上：

```python
import hashlib
import hmac
import json
```

在 `relay_protocol.py` 末尾追加：

```python
def encode_handshake(ticket: str, offset: int, length: int) -> bytes:
    """编码一行握手 JSON，用于声明本次传输的票据与区间。"""
    body = json.dumps({"ticket": ticket, "offset": offset, "length": length})
    return body.encode("utf-8") + b"\n"


def decode_handshake(line: bytes) -> dict[str, object]:
    """解析握手行，字段缺失或类型非法时抛 ProtocolError。"""
    try:
        body = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError("malformed handshake") from exc
    if not isinstance(body, dict):
        raise ProtocolError("handshake must be an object")
    ticket = body.get("ticket")
    offset = body.get("offset")
    length = body.get("length")
    if not isinstance(ticket, str) or not ticket:
        raise ProtocolError("handshake ticket missing")
    if not isinstance(offset, int) or offset < 0:
        raise ProtocolError("handshake offset invalid")
    if not isinstance(length, int) or length < 0:
        raise ProtocolError("handshake length invalid")
    return {"ticket": ticket, "offset": offset, "length": length}


def issue_token(secret: bytes, job_id: str, role: str, *, now: float, ttl: int) -> str:
    """签发绑定 job 与角色的注册令牌。"""
    expires_at = int(now) + int(ttl)
    body = f"{job_id}|{role}|{expires_at}"
    signature = hmac.new(secret, body.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{body}|{signature}"


def verify_token(secret: bytes, token: str, *, now: float) -> tuple[str, str]:
    """校验令牌并返回 (job_id, role)。"""
    parts = token.split("|")
    if len(parts) != 4:
        raise ProtocolError("malformed token")
    job_id, role, expires_raw, signature = parts
    if not job_id or role not in {"source", "target"}:
        raise ProtocolError("malformed token")
    body = f"{job_id}|{role}|{expires_raw}"
    expected = hmac.new(secret, body.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ProtocolError("bad signature")
    try:
        expires_at = int(expires_raw)
    except ValueError as exc:
        raise ProtocolError("malformed token") from exc
    if expires_at < int(now):
        raise ProtocolError("token expired")
    return job_id, role
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_protocol -v`

Expected: PASS（16 个用例）

---

### Task 3: 台账持久化

**Files:**
- Create: `relay_ledger.py`
- Test: `tests/test_relay_ledger.py`

- [ ] **Step 1: Write the failing test**

```python
import json
import os
import tempfile
import unittest
from pathlib import Path

from relay_ledger import Ledger, VolumeTaskRecord


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-ledger-job-1.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_upsert_then_reload_roundtrips(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(
                job_id="job-1",
                vm_id="vm-1",
                volume_id="vol-1",
                phase="copying",
                copied_bytes=4096,
            )
        )
        ledger.save()

        record = Ledger.load(self.path).get("job-1", "vol-1")

        self.assertEqual(record.phase, "copying")
        self.assertEqual(record.copied_bytes, 4096)

    def test_load_missing_file_returns_empty_ledger(self):
        self.assertEqual(Ledger.load(self.path).all(), [])

    def test_save_sets_0600_permissions(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(job_id="job-1", vm_id="vm-1", volume_id="vol-1")
        )
        ledger.save()

        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_upsert_updates_existing_record(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(job_id="job-1", vm_id="vm-1", volume_id="vol-1")
        )
        ledger.upsert(
            VolumeTaskRecord(
                job_id="job-1", vm_id="vm-1", volume_id="vol-1", phase="done"
            )
        )

        self.assertEqual(len(ledger.all()), 1)
        self.assertEqual(ledger.get("job-1", "vol-1").phase, "done")

    def test_save_leaves_no_temp_file(self):
        ledger = Ledger.load(self.path)
        ledger.upsert(
            VolumeTaskRecord(job_id="job-1", vm_id="vm-1", volume_id="vol-1")
        )
        ledger.save()
        ledger.save()

        leftovers = [
            path.name for path in self.path.parent.iterdir() if path.name != self.path.name
        ]
        self.assertEqual(leftovers, [])

    def test_reload_tolerates_unknown_future_fields(self):
        self.path.write_text(
            json.dumps(
                {
                    "records": [
                        {
                            "job_id": "job-1",
                            "vm_id": "vm-1",
                            "volume_id": "vol-1",
                            "future_field": "ignored",
                        }
                    ]
                }
            )
        )

        self.assertEqual(Ledger.load(self.path).get("job-1", "vol-1").phase, "queued")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_ledger -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_ledger'`

- [ ] **Step 3: Write minimal implementation**

```python
"""中转机迁移台账：卷任务记录的持久化。

台账是"资源不堆积"的前提：凡由平台创建的资源都必须先登记再操作，
服务启动时用台账与云上实际资源对账。
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any


@dataclass
class VolumeTaskRecord:
    job_id: str
    vm_id: str
    volume_id: str
    phase: str = "queued"
    snapshot_id: str = ""
    derived_volume_id: str = ""
    target_volume_id: str = ""
    source_relay_id: str = ""
    target_relay_id: str = ""
    copied_bytes: int = 0
    attach_state: str = ""
    retry_count: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def key(self) -> str:
        return f"{self.job_id}:{self.volume_id}"


class Ledger:
    """JSON 落盘的卷任务台账，路径形如 uploads/relay-ledger-<job>.json。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._records: dict[str, VolumeTaskRecord] = {}

    @classmethod
    def load(cls, path: Path | str) -> "Ledger":
        ledger = cls(path)
        if not ledger.path.exists():
            return ledger
        raw = json.loads(ledger.path.read_text(encoding="utf-8") or "{}")
        known = {field.name for field in fields(VolumeTaskRecord)}
        for item in raw.get("records", []):
            payload = {key: value for key, value in item.items() if key in known}
            record = VolumeTaskRecord(**payload)
            ledger._records[record.key] = record
        return ledger

    def get(self, job_id: str, volume_id: str) -> VolumeTaskRecord | None:
        return self._records.get(f"{job_id}:{volume_id}")

    def all(self) -> list[VolumeTaskRecord]:
        return list(self._records.values())

    def upsert(self, record: VolumeTaskRecord) -> VolumeTaskRecord:
        existing = self._records.get(record.key)
        if existing is not None and not record.created_at:
            record.created_at = existing.created_at
        self._records[record.key] = record
        return record

    def save(self) -> None:
        payload: dict[str, Any] = {
            "records": [asdict(record) for record in self._records.values()]
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, tmp_path = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".relay-ledger-", suffix=".tmp"
        )
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=False)
            os.chmod(tmp_path, 0o600)
            os.replace(tmp_path, self.path)
        except BaseException:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_ledger -v`

Expected: PASS（6 个用例）

---

### Task 4: agent 注册表与任务队列

**Files:**
- Create: `relay_registry.py`
- Test: `tests/test_relay_registry.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest

from relay_registry import RelayState


class RelayStateTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(
            secret=b"secret", heartbeat_interval=10, heartbeat_timeout=30
        )

    def _register(self, role="source", name="relay-a", now=1000.0):
        return self.state.register(
            job_id="job-1",
            role=role,
            name=name,
            version="1.0.0",
            address="10.0.0.5",
            now=now,
        )

    def test_register_creates_ready_agent(self):
        agent = self._register()

        self.assertEqual(agent.state, "ready")
        self.assertTrue(agent.session_id)
        self.assertEqual(self.state.by_session(agent.session_id).name, "relay-a")

    def test_register_requires_supported_protocol_version(self):
        with self.assertRaises(ValueError):
            self.state.register(
                job_id="job-1",
                role="source",
                name="relay-a",
                version="0.9.0",
                address="10.0.0.5",
                now=1000.0,
            )

    def test_heartbeat_updates_timestamp(self):
        agent = self._register()

        self.state.heartbeat(agent.session_id, now=1001.0)

        self.assertEqual(self.state.by_session(agent.session_id).last_heartbeat, 1001.0)

    def test_heartbeat_unknown_session_returns_none(self):
        self.assertIsNone(self.state.heartbeat("missing", now=1001.0))

    def test_sweep_marks_stale_agent_unhealthy(self):
        agent = self._register()

        changed = self.state.sweep(now=1100.0)

        self.assertEqual(changed, [agent.agent_id])
        self.assertEqual(self.state.by_session(agent.session_id).state, "unhealthy")

    def test_sweep_keeps_fresh_agent_ready(self):
        agent = self._register(now=1000.0)

        self.assertEqual(self.state.sweep(now=1010.0), [])
        self.assertEqual(self.state.by_session(agent.session_id).state, "ready")

    def test_dispatch_marks_agent_busy_and_returns_task(self):
        agent = self._register()
        self.state.enqueue({"task_id": "t-1", "role": "source"})

        task = self.state.dispatch(agent.session_id)

        self.assertEqual(task["task_id"], "t-1")
        self.assertEqual(self.state.by_session(agent.session_id).state, "busy")
        self.assertEqual(self.state.by_session(agent.session_id).current_task_id, "t-1")

    def test_dispatch_returns_none_when_queue_empty(self):
        agent = self._register()

        self.assertIsNone(self.state.dispatch(agent.session_id))
        self.assertEqual(self.state.by_session(agent.session_id).state, "ready")

    def test_dispatch_returns_none_for_mismatched_role(self):
        agent = self._register(role="target", name="relay-b")
        self.state.enqueue({"task_id": "t-1", "role": "source"})

        self.assertIsNone(self.state.dispatch(agent.session_id))

    def test_cancel_flag_is_set_and_cleared(self):
        agent = self._register()

        self.assertFalse(self.state.has_cancel(agent.session_id))
        self.state.request_cancel(agent.session_id)
        self.assertTrue(self.state.has_cancel(agent.session_id))
        self.state.clear_cancel(agent.session_id)
        self.assertFalse(self.state.has_cancel(agent.session_id))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_registry -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_registry'`

- [ ] **Step 3: Write minimal implementation**

```python
"""agent 注册表：会话、心跳、任务队列与取消标记。

本模块只维护内存状态，不落盘；持久化由 relay_ledger 负责。
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any

SUPPORTED_AGENT_VERSIONS = {"1.0.0"}


@dataclass
class AgentRecord:
    agent_id: str
    job_id: str
    role: str
    name: str
    version: str
    address: str
    session_id: str
    state: str = "ready"
    last_heartbeat: float = 0.0
    cancel_requested: bool = False
    current_task_id: str = ""


class RelayState:
    def __init__(
        self,
        *,
        secret: bytes,
        heartbeat_interval: int = 10,
        heartbeat_timeout: int = 30,
    ):
        self.secret = secret
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._agents: dict[str, AgentRecord] = {}
        self._sessions: dict[str, str] = {}
        self._queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self._tasks: dict[str, dict[str, Any]] = {}

    def now(self) -> float:
        return time.time()

    def register(
        self,
        *,
        job_id: str,
        role: str,
        name: str,
        version: str,
        address: str,
        now: float,
    ) -> AgentRecord:
        if version not in SUPPORTED_AGENT_VERSIONS:
            raise ValueError(f"unsupported agent version: {version}")
        agent = AgentRecord(
            agent_id=uuid.uuid4().hex,
            job_id=job_id,
            role=role,
            name=name,
            version=version,
            address=address,
            session_id=uuid.uuid4().hex,
            last_heartbeat=now,
        )
        self._agents[agent.agent_id] = agent
        self._sessions[agent.session_id] = agent.agent_id
        return agent

    def by_session(self, session_id: str) -> AgentRecord | None:
        agent_id = self._sessions.get(session_id)
        return self._agents.get(agent_id) if agent_id else None

    def by_id(self, agent_id: str) -> AgentRecord | None:
        return self._agents.get(agent_id)

    def agents(self) -> list[AgentRecord]:
        return list(self._agents.values())

    def heartbeat(self, session_id: str, *, now: float) -> AgentRecord | None:
        agent = self.by_session(session_id)
        if agent is None:
            return None
        agent.last_heartbeat = now
        if agent.state == "unhealthy":
            agent.state = "busy" if agent.current_task_id else "ready"
        return agent

    def sweep(self, *, now: float) -> list[str]:
        changed: list[str] = []
        for agent in self._agents.values():
            if agent.state == "unhealthy":
                continue
            if now - agent.last_heartbeat > self.heartbeat_timeout:
                agent.state = "unhealthy"
                changed.append(agent.agent_id)
        return changed

    def enqueue(self, task: dict[str, Any]) -> None:
        self._tasks[task["task_id"]] = task
        self._queues[task["role"]].append(task)

    def task(self, task_id: str) -> dict[str, Any] | None:
        return self._tasks.get(task_id)

    def dispatch(self, session_id: str) -> dict[str, Any] | None:
        agent = self.by_session(session_id)
        if agent is None or agent.state != "ready":
            return None
        queue = self._queues.get(agent.role)
        if not queue:
            return None
        task = queue.popleft()
        agent.state = "busy"
        agent.current_task_id = task["task_id"]
        return task

    def complete_task(self, session_id: str) -> None:
        agent = self.by_session(session_id)
        if agent is not None:
            agent.state = "ready"
            agent.current_task_id = ""

    def request_cancel(self, session_id: str) -> None:
        agent = self.by_session(session_id)
        if agent is not None:
            agent.cancel_requested = True

    def has_cancel(self, session_id: str) -> bool:
        agent = self.by_session(session_id)
        return bool(agent and agent.cancel_requested)

    def clear_cancel(self, session_id: str) -> None:
        agent = self.by_session(session_id)
        if agent is not None:
            agent.cancel_requested = False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_registry -v`

Expected: PASS（10 个用例）

---

### Task 5: 平台 relay API

**Files:**
- Create: `relay_api.py`
- Modify: `app.py`
- Test: `tests/test_relay_api.py`

- [ ] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from relay_api import create_blueprint
from relay_ledger import Ledger
from relay_protocol import issue_token
from relay_registry import RelayState


class RelayApiTest(unittest.TestCase):
    SECRET = b"api-test-secret"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = RelayState(secret=self.SECRET)
        self.state.ledger = Ledger.load(Path(self.tmp.name) / "ledger.json")
        self.app = Flask(__name__)
        self.app.register_blueprint(create_blueprint(self.state))
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def _token(self, role="source", job_id="job-1", ttl=300):
        return issue_token(self.SECRET, job_id, role, now=self.state.now(), ttl=ttl)

    def _register(self, role="source", name="relay-a"):
        return self.client.post(
            "/api/relay/register",
            json={
                "token": self._token(role),
                "name": name,
                "agent_version": "1.0.0",
            },
        )

    def test_register_returns_session_id(self):
        response = self._register()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["session_id"])

    def test_register_rejects_bad_token(self):
        response = self.client.post(
            "/api/relay/register",
            json={"token": "bogus", "name": "relay-a", "agent_version": "1.0.0"},
        )

        self.assertEqual(response.status_code, 401)

    def test_register_rejects_unsupported_version(self):
        response = self.client.post(
            "/api/relay/register",
            json={
                "token": self._token(),
                "name": "relay-a",
                "agent_version": "0.0.1",
            },
        )

        self.assertEqual(response.status_code, 409)

    def test_heartbeat_reports_no_cancel_by_default(self):
        session = self._register().get_json()["session_id"]

        response = self.client.post(
            "/api/relay/heartbeat", json={"session_id": session}
        )

        self.assertFalse(response.get_json()["cancel"])

    def test_heartbeat_reports_cancel_after_request(self):
        session = self._register().get_json()["session_id"]
        self.state.request_cancel(session)

        response = self.client.post(
            "/api/relay/heartbeat", json={"session_id": session}
        )

        self.assertTrue(response.get_json()["cancel"])

    def test_heartbeat_rejects_unknown_session(self):
        response = self.client.post(
            "/api/relay/heartbeat", json={"session_id": "nope"}
        )

        self.assertEqual(response.status_code, 404)

    def test_tasks_next_returns_204_when_queue_empty(self):
        session = self._register().get_json()["session_id"]

        response = self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.assertEqual(response.status_code, 204)

    def test_tasks_next_returns_enqueued_task(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue({"task_id": "t-1", "role": "source", "length": 1024})

        response = self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.assertEqual(response.get_json()["task_id"], "t-1")

    def test_progress_updates_ledger_copied_bytes(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "source",
                "length": 4096,
                "job_id": "job-1",
                "volume_id": "vol-1",
            }
        )
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        response = self.client.post(
            "/api/relay/tasks/t-1/progress",
            json={"session_id": session, "copied_bytes": 2048},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.state.ledger.get("job-1", "vol-1").copied_bytes, 2048)

    def test_progress_creates_missing_ledger_record(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "source",
                "job_id": "job-1",
                "vm_id": "vm-1",
                "volume_id": "vol-1",
            }
        )
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.client.post(
            "/api/relay/tasks/t-1/progress",
            json={"session_id": session, "copied_bytes": 512},
        )

        record = self.state.ledger.get("job-1", "vol-1")
        self.assertEqual(record.vm_id, "vm-1")
        self.assertEqual(record.copied_bytes, 512)

    def test_progress_rejects_unknown_task(self):
        session = self._register().get_json()["session_id"]

        response = self.client.post(
            "/api/relay/tasks/missing/progress",
            json={"session_id": session, "copied_bytes": 1},
        )

        self.assertEqual(response.status_code, 404)

    def test_result_marks_agent_ready_again(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue({"task_id": "t-1", "role": "source", "length": 4096})
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        response = self.client.post(
            "/api/relay/tasks/t-1/result",
            json={"session_id": session, "status": "done", "copied_bytes": 4096},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.state.by_session(session).state, "ready")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_api -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_api'`

- [ ] **Step 3: Write minimal implementation**

```python
"""平台侧 /api/relay/* 接口：agent 注册、心跳、领任务与上报。

agent 只做出向请求，平台不主动连中转机；取消通过心跳响应下发。
"""
from __future__ import annotations

import time

from flask import Blueprint, jsonify, request

from relay_ledger import Ledger, VolumeTaskRecord
from relay_protocol import ProtocolError, verify_token
from relay_registry import RelayState


def create_blueprint(state: RelayState) -> Blueprint:
    blueprint = Blueprint("relay", __name__, url_prefix="/api/relay")

    @blueprint.post("/register")
    def register():
        body = request.get_json(force=True, silent=True) or {}
        try:
            job_id, role = verify_token(
                state.secret, str(body.get("token", "")), now=time.time()
            )
        except ProtocolError as exc:
            return jsonify({"error": str(exc)}), 401
        try:
            agent = state.register(
                job_id=job_id,
                role=role,
                name=str(body.get("name", "")),
                version=str(body.get("agent_version", "")),
                address=request.remote_addr or "",
                now=time.time(),
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 409
        return jsonify(
            {
                "session_id": agent.session_id,
                "agent_id": agent.agent_id,
                "heartbeat_interval": state.heartbeat_interval,
            }
        )

    @blueprint.post("/heartbeat")
    def heartbeat():
        body = request.get_json(force=True, silent=True) or {}
        session_id = str(body.get("session_id", ""))
        agent = state.heartbeat(session_id, now=time.time())
        if agent is None:
            return jsonify({"error": "unknown session"}), 404
        return jsonify({"cancel": state.has_cancel(session_id)})

    @blueprint.get("/tasks/next")
    def tasks_next():
        session_id = request.args.get("session_id", "")
        task = state.dispatch(session_id)
        if task is None:
            return ("", 204)
        return jsonify(task)

    @blueprint.post("/tasks/<task_id>/progress")
    def task_progress(task_id: str):
        body = request.get_json(force=True, silent=True) or {}
        task = state.task(task_id)
        if task is None:
            return jsonify({"error": "unknown task"}), 404
        _record_progress(state, task, int(body.get("copied_bytes", 0)))
        _persist(state)
        return jsonify({"ok": True})

    @blueprint.post("/tasks/<task_id>/result")
    def task_result(task_id: str):
        body = request.get_json(force=True, silent=True) or {}
        task = state.task(task_id)
        if task is None:
            return jsonify({"error": "unknown task"}), 404
        session_id = str(body.get("session_id", ""))
        if str(body.get("status", "")) == "done":
            _record_progress(state, task, int(body.get("copied_bytes", 0)))
        state.complete_task(session_id)
        _persist(state)
        return jsonify({"ok": True})

    return blueprint


def _record_progress(state: RelayState, task: dict, copied_bytes: int) -> None:
    ledger = getattr(state, "ledger", None)
    if not isinstance(ledger, Ledger):
        return
    job_id = task.get("job_id")
    volume_id = task.get("volume_id")
    if not job_id or not volume_id:
        return
    record = ledger.get(job_id, volume_id)
    if record is None:
        record = VolumeTaskRecord(
            job_id=job_id, vm_id=task.get("vm_id", ""), volume_id=volume_id
        )
    record.copied_bytes = copied_bytes
    record.updated_at = time.time()
    ledger.upsert(record)


def _persist(state: RelayState) -> None:
    ledger = getattr(state, "ledger", None)
    if isinstance(ledger, Ledger):
        ledger.save()
```

在 `app.py` 注册蓝图：确认顶部已有 `import os`，在其它路由定义之后追加：

```python
from relay_api import create_blueprint
from relay_ledger import Ledger
from relay_registry import RelayState

RELAY_STATE = RelayState(secret=os.urandom(32))
RELAY_STATE.ledger = Ledger.load(os.path.join("uploads", "relay-ledger-platform.json"))
app.register_blueprint(create_blueprint(RELAY_STATE))
```

`RelayState` 未内置 `ledger` 字段，由平台在启动时注入；未注入时 API 照常工作，
只是不落盘，便于单测。

注意：`os.urandom(32)` 每次进程启动都会换新密钥，平台重启后既有令牌与会话失效，
agent 会因心跳返回 404 而重新注册。这是本期的预期行为（平台重启不恢复任务进度，
见设计文档第 12 节）；后续若要让会话跨重启存活，需把密钥改为从环境变量注入。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_api -v`

Expected: PASS（12 个用例）

- [ ] **Step 5: Run the full suite to confirm no regression**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS，无失败用例

---

### Task 6: 块传输与限速

**Files:**
- Create: `relay_transfer.py`
- Test: `tests/test_relay_transfer.py`

- [ ] **Step 1: Write the failing test**

```python
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from relay_transfer import TokenBucket, TransferError, receive_device, send_device


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class TokenBucketTest(unittest.TestCase):
    def test_no_rate_means_no_sleep(self):
        slept = []
        bucket = TokenBucket(None, clock=lambda: 0.0, sleeper=slept.append)

        bucket.consume(1024)

        self.assertEqual(slept, [])

    def test_sleeps_when_tokens_exhausted(self):
        ticks = iter([0.0, 0.0, 1.0, 1.0, 1.0, 1.0])
        slept = []
        bucket = TokenBucket(
            1000.0, clock=lambda: next(ticks), sleeper=slept.append, burst=1000
        )

        bucket.consume(1500)

        self.assertTrue(slept)

    def test_rejects_negative_rate(self):
        with self.assertRaises(ValueError):
            TokenBucket(-1.0)


class DeviceTransferTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name) / "src.bin"
        self.dst = Path(self.tmp.name) / "dst.bin"
        self.payload = os.urandom(300 * 1024)
        self.src.write_bytes(self.payload)
        self.dst.write_bytes(b"\x00" * len(self.payload))
        self.port = _free_port()

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_copy_matches_source(self):
        ready = threading.Event()
        received = {}

        def run_receiver():
            received["bytes"] = receive_device(
                str(self.dst),
                listen_host="127.0.0.1",
                listen_port=self.port,
                expected_ticket="ticket-1",
                offset=0,
                length=len(self.payload),
                chunk_size=64 * 1024,
                ready_event=ready,
            )

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        sent = send_device(
            str(self.src),
            peer_host="127.0.0.1",
            peer_port=self.port,
            ticket="ticket-1",
            offset=0,
            length=len(self.payload),
            chunk_size=64 * 1024,
        )
        thread.join(timeout=10)

        self.assertEqual(sent, len(self.payload))
        self.assertEqual(received["bytes"], len(self.payload))
        self.assertEqual(self.dst.read_bytes(), self.payload)

    def test_receiver_rejects_wrong_ticket(self):
        ready = threading.Event()
        errors = []

        def run_receiver():
            try:
                receive_device(
                    str(self.dst),
                    listen_host="127.0.0.1",
                    listen_port=self.port,
                    expected_ticket="expected",
                    offset=0,
                    length=16,
                    chunk_size=16,
                    ready_event=ready,
                )
            except Exception as exc:  # noqa: BLE001 - 测试需要捕获任意协议错误
                errors.append(exc)

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        with self.assertRaises(TransferError):
            send_device(
                str(self.src),
                peer_host="127.0.0.1",
                peer_port=self.port,
                ticket="wrong",
                offset=0,
                length=16,
                chunk_size=16,
            )
        thread.join(timeout=10)

        self.assertTrue(errors)

    def test_progress_callback_reports_cumulative_bytes(self):
        ready = threading.Event()
        seen = []

        def run_receiver():
            receive_device(
                str(self.dst),
                listen_host="127.0.0.1",
                listen_port=self.port,
                expected_ticket="ticket-1",
                offset=0,
                length=len(self.payload),
                chunk_size=64 * 1024,
                ready_event=ready,
            )

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        send_device(
            str(self.src),
            peer_host="127.0.0.1",
            peer_port=self.port,
            ticket="ticket-1",
            offset=0,
            length=len(self.payload),
            chunk_size=64 * 1024,
            progress_cb=seen.append,
        )
        thread.join(timeout=10)

        self.assertTrue(seen)
        self.assertEqual(seen[-1], len(self.payload))

    def test_send_device_rejects_range_beyond_source(self):
        with self.assertRaises(ValueError):
            send_device(
                str(self.src),
                peer_host="127.0.0.1",
                peer_port=self.port,
                ticket="ticket-1",
                offset=0,
                length=len(self.payload) + 1,
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_transfer -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_transfer'`

- [ ] **Step 3: Write minimal implementation**

```python
"""块流传输：源端发送、目标端接收，含限速与进度回调。

两端都按绝对偏移读写，因此中断后从任意已确认偏移重传都是幂等的。
"""
from __future__ import annotations

import socket
import time
from pathlib import Path
from threading import Event
from typing import Callable

from relay_protocol import (
    DEFAULT_CHUNK,
    HANDSHAKE_LIMIT,
    ProtocolError,
    decode_handshake,
    encode_chunk,
    encode_handshake,
    read_chunk,
)


class TransferError(RuntimeError):
    """传输过程中的协议或对端错误。"""


class TokenBucket:
    """简单令牌桶限速器，rate 为 None 表示不限速。"""

    def __init__(
        self,
        rate_bytes_per_sec: float | None,
        *,
        burst: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        if rate_bytes_per_sec is not None and rate_bytes_per_sec <= 0:
            raise ValueError("rate must be positive or None")
        self.rate = rate_bytes_per_sec
        self.capacity = burst or rate_bytes_per_sec or 0
        self._tokens = float(self.capacity)
        self._clock = clock
        self._sleep = sleeper
        self._last = clock()

    def consume(self, amount: int) -> None:
        if not self.rate or amount <= 0:
            return
        remaining = amount
        while remaining > 0:
            now = self._clock()
            self._tokens = min(
                float(self.capacity), self._tokens + (now - self._last) * self.rate
            )
            self._last = now
            if self._tokens >= 1:
                take = int(min(remaining, self._tokens))
                self._tokens -= take
                remaining -= take
            else:
                self._sleep((1 - self._tokens) / self.rate)


def send_device(
    src_path: str,
    *,
    peer_host: str,
    peer_port: int,
    ticket: str,
    offset: int = 0,
    length: int | None = None,
    chunk_size: int = DEFAULT_CHUNK,
    rate_limit_bytes_per_sec: float | None = None,
    progress_cb: Callable[[int], None] | None = None,
    is_cancelled: Callable[[], bool] | None = None,
) -> int:
    """把 src_path 的 [offset, offset+length) 推送到对端，返回发送字节数。"""
    total = Path(src_path).stat().st_size
    if length is None:
        length = total - offset
    if offset < 0 or length < 0 or offset + length > total:
        raise ValueError("transfer range out of bounds")

    bucket = TokenBucket(rate_limit_bytes_per_sec)
    sent = 0
    with socket.create_connection((peer_host, peer_port), timeout=30) as sock:
        with sock.makefile("rb") as reader:
            sock.sendall(encode_handshake(ticket, offset, length))
            ack = reader.readline(HANDSHAKE_LIMIT)
            if ack.strip() != b"OK":
                raise TransferError(f"peer rejected handshake: {ack!r}")
            with open(src_path, "rb") as source:
                source.seek(offset)
                while sent < length:
                    if is_cancelled is not None and is_cancelled():
                        raise TransferError("cancelled")
                    chunk = source.read(min(chunk_size, length - sent))
                    if not chunk:
                        break
                    bucket.consume(len(chunk))
                    sock.sendall(encode_chunk(offset + sent, chunk))
                    sent += len(chunk)
                    if progress_cb is not None:
                        progress_cb(sent)
    if sent != length:
        raise TransferError(f"short read from source: {sent}/{length}")
    return sent


def receive_device(
    dst_path: str,
    *,
    listen_host: str,
    listen_port: int,
    expected_ticket: str,
    offset: int,
    length: int,
    chunk_size: int = DEFAULT_CHUNK,
    progress_cb: Callable[[int], None] | None = None,
    ready_event: Event | None = None,
) -> int:
    """监听一次连接并把数据写入 dst_path，返回接收字节数。"""
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((listen_host, listen_port))
        server.listen(1)
        if ready_event is not None:
            ready_event.set()
        conn, _ = server.accept()
        with conn, conn.makefile("rb") as reader:
            handshake = decode_handshake(reader.readline(HANDSHAKE_LIMIT))
            if handshake["ticket"] != expected_ticket:
                conn.sendall(b"DENIED\n")
                raise TransferError("ticket mismatch")
            if handshake["offset"] != offset or handshake["length"] != length:
                conn.sendall(b"DENIED\n")
                raise TransferError("range mismatch")
            conn.sendall(b"OK\n")
            received = 0
            with open(dst_path, "r+b") as target:
                while received < length:
                    chunk_offset, payload = read_chunk(reader)
                    if chunk_offset < 0:
                        raise TransferError("truncated stream")
                    if chunk_offset != offset + received:
                        raise TransferError("unexpected chunk offset")
                    target.seek(chunk_offset)
                    target.write(payload)
                    received += len(payload)
                    if progress_cb is not None:
                        progress_cb(received)
                target.flush()
    return received


__all__ = [
    "ProtocolError",
    "TokenBucket",
    "TransferError",
    "receive_device",
    "send_device",
]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_transfer -v`

Expected: PASS（7 个用例）

---

### Task 7: 断点续传与完成校验

**Files:**
- Modify: `relay_transfer.py`
- Modify: `tests/test_relay_transfer.py`

- [ ] **Step 1: Write the failing test**

在 `tests/test_relay_transfer.py` 末尾追加：

```python
class ResumeAndVerifyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = Path(self.tmp.name) / "src.bin"
        self.dst = Path(self.tmp.name) / "dst.bin"
        self.payload = os.urandom(200 * 1024)
        self.src.write_bytes(self.payload)
        self.dst.write_bytes(self.payload[:100 * 1024] + b"\x00" * (100 * 1024))
        self.port = _free_port()

    def tearDown(self):
        self.tmp.cleanup()

    def test_resumed_transfer_only_sends_remaining_range(self):
        ready = threading.Event()

        def run_receiver():
            receive_device(
                str(self.dst),
                listen_host="127.0.0.1",
                listen_port=self.port,
                expected_ticket="ticket-1",
                offset=100 * 1024,
                length=100 * 1024,
                chunk_size=32 * 1024,
                ready_event=ready,
            )

        thread = threading.Thread(target=run_receiver)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))

        sent = send_device(
            str(self.src),
            peer_host="127.0.0.1",
            peer_port=self.port,
            ticket="ticket-1",
            offset=100 * 1024,
            length=100 * 1024,
            chunk_size=32 * 1024,
        )
        thread.join(timeout=10)

        self.assertEqual(sent, 100 * 1024)
        self.assertEqual(self.dst.read_bytes(), self.payload)

    def test_verify_tail_accepts_identical_tail(self):
        self.assertTrue(verify_tail(str(self.src), str(self.dst), window=64 * 1024))

    def test_verify_tail_detects_divergence(self):
        broken = Path(self.tmp.name) / "broken.bin"
        broken.write_bytes(self.payload[:-1] + b"\xFF")

        self.assertFalse(verify_tail(str(self.src), str(broken), window=64 * 1024))

    def test_verify_tail_rejects_non_positive_window(self):
        with self.assertRaises(ValueError):
            verify_tail(str(self.src), str(self.dst), window=0)

    def test_verify_size_flags_short_target(self):
        short = Path(self.tmp.name) / "short.bin"
        short.write_bytes(b"\x00" * 10)

        with self.assertRaises(ValueError):
            verify_size(str(self.src), str(short))

    def test_verify_size_accepts_larger_target(self):
        larger = Path(self.tmp.name) / "larger.bin"
        larger.write_bytes(b"\x00" * (300 * 1024))

        verify_size(str(self.src), str(larger))
```

把文件顶部的导入改为：

```python
from relay_transfer import (
    TokenBucket,
    TransferError,
    receive_device,
    send_device,
    verify_size,
    verify_tail,
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_transfer -v`

Expected: FAIL with `ImportError: cannot import name 'verify_size'`

- [ ] **Step 3: Write minimal implementation**

在 `relay_transfer.py` 末尾追加：

```python
TAIL_WINDOW = 16 * 1024 * 1024


def verify_size(src_path: str, dst_path: str) -> None:
    """目标设备容量必须不小于源设备，否则抛 ValueError。"""
    source_size = Path(src_path).stat().st_size
    target_size = Path(dst_path).stat().st_size
    if target_size < source_size:
        raise ValueError(f"target too small: {target_size} < {source_size}")


def verify_tail(src_path: str, dst_path: str, *, window: int = TAIL_WINDOW) -> bool:
    """比对两个设备尾部 window 字节的摘要，返回是否一致。"""
    if window <= 0:
        raise ValueError("window must be positive")
    return _tail_digest(src_path, window) == _tail_digest(dst_path, window)


def _tail_digest(path: str, window: int) -> str:
    size = Path(path).stat().st_size
    start = max(0, size - window)
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        stream.seek(start)
        remaining = size - start
        while remaining > 0:
            chunk = stream.read(min(DEFAULT_CHUNK, remaining))
            if not chunk:
                break
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


__all__ += ["verify_size", "verify_tail"]
```

在 `relay_transfer.py` 顶部补 `import hashlib`。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_transfer -v`

Expected: PASS（13 个用例）

---

### Task 8: agent 主循环与端到端集成

**Files:**
- Create: `relay_agent.py`
- Test: `tests/test_relay_agent.py`

- [ ] **Step 1: Write the failing test**

```python
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from relay_agent import AgentClient, execute_task, run_once


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

    def heartbeat(self, session_id):
        return {"cancel": False}

    def next_task(self, session_id):
        queue = self.tasks[self.roles[session_id]]
        return queue.pop(0) if queue else None

    def report_progress(self, task_id, copied_bytes):
        self.progress.append((task_id, copied_bytes))

    def report_result(self, task_id, status, copied_bytes):
        self.results.append((task_id, status, copied_bytes))


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

    def test_execute_task_rejects_unknown_role(self):
        platform = FakePlatform({"s-src": "source"})

        with self.assertRaises(ValueError):
            execute_task({"task_id": "t", "role": "bogus"}, AgentClient(platform, "s-src"))

    def test_run_once_returns_none_without_task(self):
        platform = FakePlatform({"s-src": "source"})

        self.assertIsNone(run_once(AgentClient(platform, "s-src")))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_agent -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_agent'`

- [ ] **Step 3: Write minimal implementation**

```python
"""中转机 agent：注册、心跳、领任务、执行块拷贝并上报。

agent 只做出向请求；取消通过心跳响应下发。agent 不执行任何 attach/detach，
卷的挂载与卸载一律由平台通过 OpenStack API 完成。
"""
from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.request
from threading import Event
from typing import Any

from relay_protocol import DEFAULT_CHUNK
from relay_transfer import TransferError, receive_device, send_device

AGENT_VERSION = "1.0.0"


class AgentClient:
    """agent 面向平台的客户端；测试时可传入鸭子类型的假实现。"""

    def __init__(self, platform: Any, session_id: str):
        self.platform = platform
        self.session_id = session_id

    def heartbeat(self) -> dict[str, Any]:
        return self.platform.heartbeat(self.session_id)

    def next_task(self) -> dict[str, Any] | None:
        return self.platform.next_task(self.session_id)

    def report_progress(self, task_id: str, copied_bytes: int) -> None:
        self.platform.report_progress(task_id, copied_bytes)

    def report_result(self, task_id: str, status: str, copied_bytes: int) -> None:
        self.platform.report_result(task_id, status, copied_bytes)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


def execute_task(
    task: dict[str, Any],
    client: AgentClient,
    *,
    ready_event: Event | None = None,
) -> dict[str, Any]:
    """执行单个块拷贝任务，返回带 status 与 copied_bytes 的结果。"""
    role = task.get("role")
    if role not in {"source", "target"}:
        raise ValueError(f"unknown role: {role!r}")

    task_id = str(task.get("task_id", ""))
    chunk_size = int(task.get("chunk_size") or DEFAULT_CHUNK)
    length = int(task.get("length", 0))
    offset = int(task.get("offset", 0))
    ticket = str(task.get("ticket", ""))
    report_state = {"at": 0.0}

    def progress(copied: int) -> None:
        now = time.monotonic()
        if copied < length and now - report_state["at"] < 1.0:
            return
        report_state["at"] = now
        client.report_progress(task_id, copied)

    try:
        if role == "target":
            port = int(task.get("listen_port") or 0) or _free_port()
            copied = receive_device(
                str(task["dst_path"]),
                listen_host=str(task.get("listen_host") or "0.0.0.0"),
                listen_port=port,
                expected_ticket=ticket,
                offset=offset,
                length=length,
                chunk_size=chunk_size,
                progress_cb=progress,
                ready_event=ready_event,
            )
        else:
            copied = send_device(
                str(task["src_path"]),
                peer_host=str(task["peer_host"]),
                peer_port=int(task["peer_port"]),
                ticket=ticket,
                offset=offset,
                length=length,
                chunk_size=chunk_size,
                rate_limit_bytes_per_sec=task.get("rate_limit_bytes_per_sec") or None,
                progress_cb=progress,
            )
    except (TransferError, OSError, ValueError) as exc:
        client.report_result(task_id, "failed", 0)
        return {"status": "failed", "error": str(exc), "copied_bytes": 0}

    client.report_result(task_id, "done", copied)
    return {"status": "done", "copied_bytes": copied}


def run_once(client: AgentClient) -> dict[str, Any] | None:
    """心跳一次并尝试领取并执行一个任务。"""
    client.heartbeat()
    task = client.next_task()
    if task is None:
        return None
    return execute_task(task, client)


def _http_json(url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        body = response.read()
    return json.loads(body) if body else {}


class _HttpPlatform:
    """把平台 HTTP 接口适配成 AgentClient 期望的鸭子类型。"""

    def __init__(self, base_url: str, session_id: str):
        self.base_url = base_url.rstrip("/")
        self.session_id = session_id

    def heartbeat(self, session_id: str) -> dict[str, Any]:
        return _http_json(
            f"{self.base_url}/api/relay/heartbeat", {"session_id": session_id}
        )

    def next_task(self, session_id: str) -> dict[str, Any] | None:
        url = f"{self.base_url}/api/relay/tasks/next?session_id={session_id}"
        request = urllib.request.Request(url)
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            if response.status == 204:
                return None
            body = response.read()
        return json.loads(body) if body else None

    def report_progress(self, task_id: str, copied_bytes: int) -> None:
        _http_json(
            f"{self.base_url}/api/relay/tasks/{task_id}/progress",
            {"session_id": self.session_id, "copied_bytes": copied_bytes},
        )

    def report_result(self, task_id: str, status: str, copied_bytes: int) -> None:
        _http_json(
            f"{self.base_url}/api/relay/tasks/{task_id}/result",
            {
                "session_id": self.session_id,
                "status": status,
                "copied_bytes": copied_bytes,
            },
        )


def main(platform_url: str, token: str, name: str, *, interval: float = 5.0) -> None:
    """agent 常驻入口：注册后循环心跳与领任务。"""
    registration = _http_json(
        f"{platform_url}/api/relay/register",
        {"token": token, "name": name, "agent_version": AGENT_VERSION},
    )
    session_id = registration["session_id"]
    client = AgentClient(_HttpPlatform(platform_url, session_id), session_id)
    while True:
        try:
            run_once(client)
        except urllib.error.URLError:
            pass
        time.sleep(interval)


if __name__ == "__main__":
    import os

    main(
        os.environ["RELAY_PLATFORM_URL"],
        os.environ["RELAY_TOKEN"],
        os.environ.get("RELAY_NAME", socket.gethostname()),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_agent -v`

Expected: PASS（4 个用例）

- [ ] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS，无失败用例

---

## 完成标准

- `python3 -m unittest discover -s tests -v` 全绿；
- 两台 agent 能以普通文件为设备完成全量块拷贝，
  中断后按 `offset` 续传只传输剩余区间，结果与源文件逐字节一致；
- 台账落盘且文件权限为 `0600`，服务重启后能重新加载；
- 平台 API 在令牌非法（401）、版本不匹配（409）、会话未知（404）、
  任务未知（404）时返回明确错误码；
- 取消请求能通过心跳响应传递到 agent。

---

## 执行记录（2026-09-10）

8 个任务全部完成，`python3 -m unittest discover -s tests -v` 共 211 个用例通过。
执行过程中对本计划做了三处修正，均为计划本身的缺陷：

1. **Task 6 限速用例**：原计划的假时钟每步跳 1 秒，会把令牌桶重新充满，
   测不出限速；改为"睡眠会推进时间"的 `_FakeClock`，断言实际睡眠约 0.5 秒。
2. **Task 6 令牌桶实现**：原实现按 `(1 - tokens) / rate` 睡眠，
   浮点漂移时该值趋近 0，会退化成空转（实测挂死）。
   改为按"攒够本次可消耗令牌"睡眠并设 0.5ms 下限。
3. **Task 7 尾部校验用例**：原用例拿 setUp 中故意留了空洞的 `dst` 断言"一致"，
   必然失败；改为构造完整副本再比对。

环境相关：

- 本环境原本没有 Flask，`pip3 install "Flask>=2.0"` 需要联网授权，已安装 3.1.3；
- 沙箱禁止 `socket()`，Task 6/7/8 的测试必须在非沙箱模式运行，
  命令为 `python3 -m unittest tests.test_relay_transfer tests.test_relay_agent`。

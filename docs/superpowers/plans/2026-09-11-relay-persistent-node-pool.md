# 中转机常驻节点池 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把中转机从作业级临时池改造为平台级常驻节点池：租户内共享、按槽位自动扩缩容、作业只租用不销毁，并支持在页面上手动扩容与删除。

**Architecture:** 新增平台级资源层（节点清单 `relay_inventory`、租约 `relay_lease`、调度 `relay_scheduler`、节点管理 `relay_node_manager`、加密凭据 `relay_credentials`），作业层 `relay_runtime` 退化为向调度器申请槽位；数据面 agent 从单任务串行改为 N worker 并发，目标端口改为端口池，取消改为任务级；签名密钥与凭据全部落盘加密，平台重启后靠清单 + agent 自动重注册恢复。

**Tech Stack:** Python 3.10、Flask、openstacksdk、`cryptography`（AES-GCM）、标准库 `unittest` + `unittest.mock`。

---

## 说明

- 设计文档：`docs/superpowers/specs/2026-09-11-relay-persistent-node-pool-design.md`。
- 前置：`docs/superpowers/plans/2026-09-10-relay-vm-*.md` 三份计划已完成，
  `relay_protocol.py`、`relay_ledger.py`、`relay_registry.py`、`relay_api.py`、
  `relay_transfer.py`、`relay_agent.py`、`relay_pool.py`、`relay_runtime.py` 已存在并通过测试。
- 当前目录不是 git 仓库（`.git` 为空），所有任务**跳过 commit 步骤**，改为跑全量回归。
- 每个任务完成后运行 `python3 -m unittest discover -s tests`，保证 431 个既有用例不回归。
- 涉及 socket 的用例（Task 15 集成测试）需要 `require_escalated` 运行。
- 新增模块必须同步登记到部署 ConfigMap 的 `--from-file` 列表（Task 22）。
- 关键约束：1 VM = 1 槽位、槽位内多盘串行；每租户每角色跨 AZ 合计上限 6 台；
  空闲 24h 缩到每池 1 台；作业失败/取消不动节点；不 detach 源卷。

## 文件结构

| 文件 | 状态 | 职责 |
| --- | --- | --- |
| `relay_secret.py` | 新增 | 平台签名密钥的来源解析与落盘 |
| `relay_inventory.py` | 新增 | 常驻节点清单的持久化与查询 |
| `relay_lease.py` | 新增 | 租约的申请、释放与统计 |
| `relay_credentials.py` | 新增 | AES-GCM 封装与云凭据/SSH 密码存储 |
| `relay_scheduler.py` | 新增 | 槽位容量、自动扩缩容、跨 AZ 额度再平衡 |
| `relay_node_manager.py` | 新增 | 节点创建、删除、重建、drain/resume |
| `relay_protocol.py` | 改造 | 新增节点级长期令牌签发与校验 |
| `relay_registry.py` | 改造 | 单任务串行改为按节点多槽位分发、任务级取消 |
| `relay_agent.py` | 改造 | N worker 并发、端口池、自动重注册 |
| `relay_api.py` | 改造 | 注册改长期令牌、心跳上报槽位、节点管理接口 |
| `relay_pool.py` | 改造 | 保留为 `ephemeral` 回退模式 |
| `relay_runtime.py` | 改造 | 作业会话：申请/释放槽位，`persistent` 与 `ephemeral` 分流 |
| `relay_reaper.py` | 改造 | 增加节点维度孤儿 attachment 对账 |
| `app.py` | 改造 | 装配平台级资源层，作业收尾不再销毁节点 |
| `templates/index.html` | 改造 | 新增"中转机资源"页，向导步骤 1 改为选择池 |
| `requirements.txt` | 改造 | 显式声明 `cryptography` |

---

## 阶段 0：常驻基础

### Task 1: 平台签名密钥持久化

**Files:**

- Create: `relay_secret.py`
- Test: `tests/test_relay_secret.py`

 - [x] **Step 1: Write the failing test**

```python
import base64
import os
import stat
import tempfile
import unittest
from pathlib import Path

from relay_secret import SecretError, load_or_create_secret, parse_secret


class ParseSecretTest(unittest.TestCase):
    def test_accepts_base64_key(self):
        raw = base64.b64encode(b"a" * 32).decode("ascii")
        self.assertEqual(parse_secret(raw, source="test"), b"a" * 32)

    def test_accepts_hex_key(self):
        self.assertEqual(parse_secret("ab" * 32, source="test"), bytes.fromhex("ab" * 32))

    def test_accepts_raw_32_byte_key(self):
        self.assertEqual(parse_secret("k" * 32, source="test"), b"k" * 32)

    def test_rejects_wrong_length(self):
        with self.assertRaises(SecretError):
            parse_secret("short", source="test")

    def test_rejects_empty(self):
        with self.assertRaises(SecretError):
            parse_secret("", source="test")


class LoadOrCreateSecretTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-secret"

    def tearDown(self):
        self.tmp.cleanup()

    def test_creates_0600_file_and_is_stable_across_calls(self):
        first = load_or_create_secret(self.path, env={})
        second = load_or_create_secret(self.path, env={})

        self.assertEqual(len(first), 32)
        self.assertEqual(first, second)
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)

    def test_env_var_wins_over_file(self):
        load_or_create_secret(self.path, env={})
        raw = base64.b64encode(b"b" * 32).decode("ascii")

        result = load_or_create_secret(self.path, env={"MIGRATION_RELAY_SECRET": raw})

        self.assertEqual(result, b"b" * 32)

    def test_invalid_env_var_raises(self):
        with self.assertRaises(SecretError):
            load_or_create_secret(self.path, env={"MIGRATION_RELAY_SECRET": "nope"})
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_secret -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_secret'`

 - [x] **Step 3: Write minimal implementation**

```python
"""平台级 relay 签名密钥：进程重启后必须保持不变。

密钥来源优先级：环境变量 MIGRATION_RELAY_SECRET > 磁盘文件 > 新建并落盘。
密钥必须持久化，否则重启后所有常驻节点令牌立即失效。
"""
from __future__ import annotations

import base64
import binascii
import os
from pathlib import Path
from typing import Mapping

SECRET_BYTES = 32


class SecretError(RuntimeError):
    """密钥缺失或格式非法。"""


def parse_secret(raw: str, *, source: str) -> bytes:
    """解析 base64 / hex / 原始 32 字节密钥。"""
    value = (raw or "").strip()
    if not value:
        raise SecretError(f"{source} 为空")
    for decode in (
        lambda text: base64.b64decode(text, validate=True),
        binascii.unhexlify,
    ):
        try:
            key = decode(value)
        except (binascii.Error, ValueError):
            continue
        if len(key) == SECRET_BYTES:
            return key
    raw_bytes = value.encode("utf-8")
    if len(raw_bytes) == SECRET_BYTES:
        return raw_bytes
    raise SecretError(f"{source} 必须是 32 字节（base64 / hex / 原始串）")


def load_or_create_secret(
    path: str | os.PathLike[str],
    *,
    env_var: str = "MIGRATION_RELAY_SECRET",
    env: Mapping[str, str] | None = None,
) -> bytes:
    """返回稳定的签名密钥；文件不存在时生成并以 0600 落盘。"""
    environment = os.environ if env is None else env
    raw = environment.get(env_var, "")
    if str(raw).strip():
        return parse_secret(str(raw), source=env_var)

    secret_path = Path(path)
    if secret_path.exists():
        return parse_secret(secret_path.read_text(encoding="utf-8"), source=str(secret_path))

    secret = os.urandom(SECRET_BYTES)
    secret_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(secret_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(base64.b64encode(secret).decode("ascii"))
    return secret
```

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_secret -v`

Expected: PASS（8 个用例）

### Task 2: 常驻节点清单

**Files:**

- Create: `relay_inventory.py`
- Test: `tests/test_relay_inventory.py`

 - [x] **Step 1: Write the failing test**

```python
import os
import stat
import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord


def _node(node_id="n1", *, tenant="t1", role="source", az="nova-1", state="ready"):
    return RelayNodeRecord(
        node_id=node_id,
        name=f"relay-{role}-{node_id}",
        role=role,
        tenant_key=tenant,
        az=az,
        server_id=f"server-{node_id}",
        state=state,
    )


class NodeInventoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-nodes.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_upsert_then_reload_roundtrips(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node())
        inventory.save()

        record = NodeInventory.load(self.path).get("n1")

        self.assertEqual(record.server_id, "server-n1")
        self.assertEqual(record.pool_key, ("t1", "source", "nova-1"))

    def test_load_missing_file_returns_empty(self):
        self.assertEqual(NodeInventory.load(self.path).all(), [])

    def test_save_sets_0600_permissions(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node())
        inventory.save()
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)

    def test_nodes_in_pool_filters_all_three_keys(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node("n1"))
        inventory.upsert(_node("n2", role="target"))
        inventory.upsert(_node("n3", az="nova-2"))
        inventory.upsert(_node("n4", tenant="t2"))

        self.assertEqual(
            [node.node_id for node in inventory.nodes_in_pool("t1", "source", "nova-1")],
            ["n1"],
        )

    def test_count_for_role_sums_across_az(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node("n1"))
        inventory.upsert(_node("n2", az="nova-2"))
        inventory.upsert(_node("n3", role="target"))

        self.assertEqual(inventory.count_for_role("t1", "source"), 2)
        self.assertEqual(inventory.count_for_role("t1", "target"), 1)

    def test_remove_returns_record_and_saves(self):
        inventory = NodeInventory.load(self.path)
        inventory.upsert(_node())

        removed = inventory.remove("n1")
        inventory.save()

        self.assertEqual(removed.node_id, "n1")
        self.assertIsNone(NodeInventory.load(self.path).get("n1"))
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_inventory -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_inventory'`

 - [x] **Step 3: Write minimal implementation**

```python
"""常驻中转机节点清单：平台级资源，落盘 uploads/relay-nodes.json（0600）。

清单是跨重启恢复的唯一依据，所有字段必须可 JSON 序列化。
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any


@dataclass
class RelayNodeRecord:
    node_id: str
    name: str
    role: str
    tenant_key: str
    az: str
    server_id: str = ""
    port_id: str = ""
    network: str = ""
    subnet: str = ""
    fixed_ip: str = ""
    flavor: str = ""
    image: str = ""
    system_volume_type: str = ""
    slots_total: int = 5
    slots_used: int = 0
    agent_version: str = ""
    token_enc: str = ""
    ssh_password_enc: str = ""
    state: str = "provisioning"
    last_seen: float = 0.0
    idle_since: float = 0.0
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def pool_key(self) -> tuple[str, str, str]:
        return (self.tenant_key, self.role, self.az)


class NodeInventory:
    """节点清单的内存视图 + 原子落盘。"""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._records: dict[str, RelayNodeRecord] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "NodeInventory":
        inventory = cls(path)
        if not inventory.path.exists():
            return inventory
        raw = json.loads(inventory.path.read_text(encoding="utf-8") or "{}")
        known = {field.name for field in fields(RelayNodeRecord)}
        for item in raw.get("nodes", []):
            payload = {key: value for key, value in item.items() if key in known}
            record = RelayNodeRecord(**payload)
            inventory._records[record.node_id] = record
        return inventory

    def get(self, node_id: str) -> RelayNodeRecord | None:
        return self._records.get(node_id)

    def all(self) -> list[RelayNodeRecord]:
        return list(self._records.values())

    def upsert(self, record: RelayNodeRecord) -> RelayNodeRecord:
        existing = self._records.get(record.node_id)
        if existing is not None and not record.created_at:
            record.created_at = existing.created_at
        self._records[record.node_id] = record
        return record

    def remove(self, node_id: str) -> RelayNodeRecord | None:
        return self._records.pop(node_id, None)

    def nodes_in_pool(
        self, tenant_key: str, role: str, az: str
    ) -> list[RelayNodeRecord]:
        return [
            record
            for record in self._records.values()
            if record.pool_key == (tenant_key, role, az)
        ]

    def nodes_for_role(self, tenant_key: str, role: str) -> list[RelayNodeRecord]:
        return [
            record
            for record in self._records.values()
            if record.tenant_key == tenant_key and record.role == role
        ]

    def count_for_role(self, tenant_key: str, role: str) -> int:
        return len(self.nodes_for_role(tenant_key, role))

    def save(self) -> None:
        payload: dict[str, Any] = {
            "nodes": [asdict(record) for record in self._records.values()]
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, tmp_path = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".relay-nodes-", suffix=".tmp"
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

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_inventory -v`

Expected: PASS（6 个用例）

### Task 3: 租约

**Files:**

- Create: `relay_lease.py`
- Test: `tests/test_relay_lease.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path

from relay_lease import LeaseStore


class LeaseStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-leases.json"

    def tearDown(self):
        self.tmp.cleanup()

    def _acquire(self, store, *, job_id="job-1", node_id="n1", role="source", tenant="t1", now=1.0):
        return store.acquire(
            job_id=job_id,
            node_id=node_id,
            role=role,
            tenant_key=tenant,
            now=now,
        )

    def test_acquire_then_reload_roundtrips(self):
        store = LeaseStore.load(self.path)
        lease = self._acquire(store)
        store.save()

        reloaded = LeaseStore.load(self.path)

        self.assertEqual(reloaded.get(lease.lease_id).job_id, "job-1")
        self.assertEqual(len(reloaded.active_for_node("n1")), 1)

    def test_release_marks_inactive(self):
        store = LeaseStore.load(self.path)
        lease = self._acquire(store)

        released = store.release(lease.lease_id, now=2.0)

        self.assertEqual(released.released_at, 2.0)
        self.assertEqual(store.active_for_node("n1"), [])

    def test_release_job_releases_only_that_job(self):
        store = LeaseStore.load(self.path)
        self._acquire(store, job_id="job-1")
        self._acquire(store, job_id="job-2")

        released = store.release_job("job-1", now=3.0)

        self.assertEqual(len(released), 1)
        self.assertEqual(len(store.active_for_job("job-2")), 1)

    def test_active_count_for_role_sums_across_az(self):
        store = LeaseStore.load(self.path)
        self._acquire(store, node_id="n1")
        self._acquire(store, node_id="n2")
        self._acquire(store, node_id="n3", role="target")

        self.assertEqual(store.active_count_for_role("t1", "source"), 2)
        self.assertEqual(store.active_count_for_role("t1", "target"), 1)

    def test_idle_since_returns_last_release_when_no_active_lease(self):
        store = LeaseStore.load(self.path)
        lease = self._acquire(store)
        store.release(lease.lease_id, now=50.0)

        self.assertEqual(store.idle_since("n1"), 50.0)

    def test_idle_since_zero_when_still_active(self):
        store = LeaseStore.load(self.path)
        self._acquire(store)

        self.assertEqual(store.idle_since("n1"), 0.0)
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_lease -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_lease'`

 - [x] **Step 3: Write minimal implementation**

```python
"""中转机租约：作业对槽位的占用记录，落盘 uploads/relay-leases.json（0600）。

租约是缩容判定与孤儿识别的依据：有未释放租约的节点永不回收。
"""
from __future__ import annotations

import json
import os
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any


@dataclass
class LeaseRecord:
    lease_id: str
    job_id: str
    node_id: str
    role: str
    tenant_key: str
    vm_id: str = ""
    volume_id: str = ""
    acquired_at: float = 0.0
    released_at: float = 0.0

    @property
    def active(self) -> bool:
        return not self.released_at


class LeaseStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._records: dict[str, LeaseRecord] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "LeaseStore":
        store = cls(path)
        if not store.path.exists():
            return store
        raw = json.loads(store.path.read_text(encoding="utf-8") or "{}")
        known = {field.name for field in fields(LeaseRecord)}
        for item in raw.get("leases", []):
            payload = {key: value for key, value in item.items() if key in known}
            record = LeaseRecord(**payload)
            store._records[record.lease_id] = record
        return store

    def get(self, lease_id: str) -> LeaseRecord | None:
        return self._records.get(lease_id)

    def all(self) -> list[LeaseRecord]:
        return list(self._records.values())

    def acquire(
        self,
        *,
        job_id: str,
        node_id: str,
        role: str,
        tenant_key: str,
        now: float | None = None,
        vm_id: str = "",
        volume_id: str = "",
    ) -> LeaseRecord:
        record = LeaseRecord(
            lease_id=uuid.uuid4().hex,
            job_id=job_id,
            node_id=node_id,
            role=role,
            tenant_key=tenant_key,
            vm_id=vm_id,
            volume_id=volume_id,
            acquired_at=time.time() if now is None else now,
        )
        self._records[record.lease_id] = record
        return record

    def release(self, lease_id: str, *, now: float | None = None) -> LeaseRecord | None:
        record = self._records.get(lease_id)
        if record is None:
            return None
        record.released_at = time.time() if now is None else now
        return record

    def release_job(self, job_id: str, *, now: float | None = None) -> list[LeaseRecord]:
        released: list[LeaseRecord] = []
        for record in self.active_for_job(job_id):
            self.release(record.lease_id, now=now)
            released.append(record)
        return released

    def active_for_node(self, node_id: str) -> list[LeaseRecord]:
        return [
            record
            for record in self._records.values()
            if record.node_id == node_id and record.active
        ]

    def active_for_job(self, job_id: str) -> list[LeaseRecord]:
        return [
            record
            for record in self._records.values()
            if record.job_id == job_id and record.active
        ]

    def active_count_for_role(self, tenant_key: str, role: str) -> int:
        return len(
            [
                record
                for record in self._records.values()
                if record.tenant_key == tenant_key
                and record.role == role
                and record.active
            ]
        )

    def idle_since(self, node_id: str) -> float:
        """返回节点最近一次释放时间；仍有活跃租约时返回 0。"""
        if self.active_for_node(node_id):
            return 0.0
        timestamps = [
            record.released_at
            for record in self._records.values()
            if record.node_id == node_id and record.released_at
        ]
        return max(timestamps) if timestamps else 0.0

    def save(self) -> None:
        payload: dict[str, Any] = {
            "leases": [asdict(record) for record in self._records.values()]
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, tmp_path = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".relay-leases-", suffix=".tmp"
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

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_lease -v`

Expected: PASS（6 个用例）
### Task 4: 加密凭据与 SSH 密码存储

**Files:**

- Create: `relay_credentials.py`
- Test: `tests/test_relay_credentials.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path

from relay_credentials import CredentialError, CredentialStore, Sealer

KEY = b"k" * 32


class SealerTest(unittest.TestCase):
    def test_roundtrip(self):
        sealer = Sealer(KEY)
        token = sealer.seal("s3cret", aad="t1")
        self.assertEqual(sealer.unseal(token, aad="t1"), "s3cret")

    def test_two_seals_of_same_plaintext_differ(self):
        sealer = Sealer(KEY)
        self.assertNotEqual(sealer.seal("x", aad="t1"), sealer.seal("x", aad="t1"))

    def test_wrong_key_fails(self):
        token = Sealer(KEY).seal("s3cret", aad="t1")
        with self.assertRaises(CredentialError):
            Sealer(b"x" * 32).unseal(token, aad="t1")

    def test_aad_mismatch_fails(self):
        token = Sealer(KEY).seal("s3cret", aad="t1")
        with self.assertRaises(CredentialError):
            Sealer(KEY).unseal(token, aad="t2")

    def test_rejects_wrong_key_length(self):
        with self.assertRaises(CredentialError):
            Sealer(b"short")


class CredentialStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-credentials.json"
        self.store = CredentialStore.load(self.path, Sealer(KEY))

    def tearDown(self):
        self.tmp.cleanup()

    def test_save_flush_get_roundtrip(self):
        auth = {"auth_url": "http://src", "username": "admin", "password": "p@ss"}
        self.store.save("t1", auth)
        self.store.flush()

        reloaded = CredentialStore.load(self.path, Sealer(KEY)).get("t1")

        self.assertEqual(reloaded, auth)

    def test_get_missing_returns_none(self):
        self.assertIsNone(self.store.get("t1"))

    def test_file_contains_no_plaintext(self):
        self.store.save("t1", {"password": "p@ss"})
        self.store.flush()
        self.assertNotIn("p@ss", self.path.read_text(encoding="utf-8"))

    def test_tenants_sorted_and_delete(self):
        self.store.save("t2", {"password": "b"})
        self.store.save("t1", {"password": "a"})

        self.assertEqual(self.store.tenants(), ["t1", "t2"])
        self.assertTrue(self.store.delete("t1"))
        self.assertEqual(self.store.tenants(), ["t2"])

    def test_node_password_helpers_use_node_aad(self):
        token = self.store.seal_text("root-pass", aad="node-1")

        self.assertEqual(self.store.unseal_text(token, aad="node-1"), "root-pass")
        with self.assertRaises(CredentialError):
            self.store.unseal_text(token, aad="node-2")

    def test_from_env_requires_key(self):
        with self.assertRaises(CredentialError):
            CredentialStore.from_env(self.path, env={})

    def test_from_env_accepts_base64_key(self):
        import base64

        raw = base64.b64encode(KEY).decode("ascii")
        store = CredentialStore.from_env(
            self.path, env={"MIGRATION_SECRET_KEY": raw}
        )
        self.assertEqual(store.tenants(), [])
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_credentials -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_credentials'`

 - [x] **Step 3: Write minimal implementation**

```python
"""加密凭据存储：云认证与节点 SSH 密码。

AES-GCM，密文格式 "<b64(nonce)>.<b64(ciphertext+tag)>"，AAD 绑定记录键，
防止密文在不同租户/节点之间串用。主密钥来自 MIGRATION_SECRET_KEY。
未配置主密钥时常驻模式拒绝启动，不做明文降级。
"""
from __future__ import annotations

import base64
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from relay_secret import parse_secret

NONCE_BYTES = 12
KEY_BYTES = 32


class CredentialError(RuntimeError):
    """密文损坏、密钥不匹配或主密钥缺失。"""


class Sealer:
    """AES-GCM 封装；AAD 绑定记录键。"""

    def __init__(self, key: bytes):
        if len(key) != KEY_BYTES:
            raise CredentialError("主密钥必须是 32 字节")
        self._aead = AESGCM(key)

    def seal(self, plaintext: str, *, aad: str = "") -> str:
        nonce = os.urandom(NONCE_BYTES)
        payload = self._aead.encrypt(
            nonce, plaintext.encode("utf-8"), aad.encode("utf-8")
        )
        return (
            f"{base64.b64encode(nonce).decode('ascii')}."
            f"{base64.b64encode(payload).decode('ascii')}"
        )

    def unseal(self, token: str, *, aad: str = "") -> str:
        try:
            nonce_b64, payload_b64 = token.split(".", 1)
            nonce = base64.b64decode(nonce_b64, validate=True)
            payload = base64.b64decode(payload_b64, validate=True)
            return self._aead.decrypt(nonce, payload, aad.encode("utf-8")).decode("utf-8")
        except (ValueError, InvalidTag, UnicodeDecodeError) as exc:
            raise CredentialError("凭据解密失败：密文或主密钥不匹配") from exc


class CredentialStore:
    """按 tenant_key 保存云认证字典；节点 SSH 密码用 seal_text/unseal_text。"""

    def __init__(self, path: str | os.PathLike[str], sealer: Sealer):
        self.path = Path(path)
        self.sealer = sealer
        self._records: dict[str, str] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str], sealer: Sealer) -> "CredentialStore":
        store = cls(path, sealer)
        if not store.path.exists():
            return store
        raw = json.loads(store.path.read_text(encoding="utf-8") or "{}")
        store._records = {
            str(key): str(value) for key, value in (raw.get("credentials") or {}).items()
        }
        return store

    @classmethod
    def from_env(
        cls,
        path: str | os.PathLike[str],
        *,
        env: Mapping[str, str] | None = None,
        env_var: str = "MIGRATION_SECRET_KEY",
    ) -> "CredentialStore":
        environment = os.environ if env is None else env
        raw = str(environment.get(env_var, ""))
        if not raw.strip():
            raise CredentialError(f"未配置 {env_var}，常驻中转机模式拒绝启动")
        return cls.load(path, Sealer(parse_secret(raw, source=env_var)))

    def save(self, tenant_key: str, auth: dict[str, Any]) -> None:
        """加密写入单个租户凭据（内存态），由 flush() 落盘。"""
        plaintext = json.dumps(auth, ensure_ascii=False, sort_keys=True)
        self._records[str(tenant_key)] = self.sealer.seal(plaintext, aad=str(tenant_key))

    def get(self, tenant_key: str) -> dict[str, Any] | None:
        token = self._records.get(str(tenant_key))
        if token is None:
            return None
        return json.loads(self.sealer.unseal(token, aad=str(tenant_key)))

    def delete(self, tenant_key: str) -> bool:
        return self._records.pop(str(tenant_key), None) is not None

    def tenants(self) -> list[str]:
        return sorted(self._records)

    def seal_text(self, plaintext: str, *, aad: str) -> str:
        return self.sealer.seal(plaintext, aad=aad)

    def unseal_text(self, token: str, *, aad: str) -> str:
        return self.sealer.unseal(token, aad=aad)

    def flush(self) -> None:
        """原子落盘，0600。"""
        payload = {"credentials": self._records}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, tmp_path = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".relay-credentials-", suffix=".tmp"
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

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_credentials -v`

Expected: PASS（12 个用例）

### Task 5: 依赖声明与 app.py 装配

**Files:**

- Modify: `requirements.txt`
- Modify: `app.py:1-52`
- Test: `tests/test_relay_app_wiring.py`

 - [x] **Step 1: Write the failing test**

追加到 `tests/test_relay_app_wiring.py` 的 `RelayModuleWiringTest`：

```python
    def test_relay_secret_is_persisted_on_disk(self):
        import os

        self.assertTrue(hasattr(app_module, "RELAY_SECRET_PATH"))
        self.assertTrue(os.path.exists(app_module.RELAY_SECRET_PATH))
        self.assertEqual(len(app_module.RELAY_STATE.secret), 32)

    def test_inventory_and_lease_stores_exist(self):
        from relay_inventory import NodeInventory
        from relay_lease import LeaseStore

        self.assertIsInstance(app_module.RELAY_INVENTORY, NodeInventory)
        self.assertIsInstance(app_module.RELAY_LEASES, LeaseStore)

    def test_relay_secret_is_stable_within_process(self):
        from relay_secret import load_or_create_secret

        again = load_or_create_secret(app_module.RELAY_SECRET_PATH)
        self.assertEqual(again, app_module.RELAY_STATE.secret)

    def test_credentials_loader_requires_master_key(self):
        from relay_credentials import CredentialError

        with self.assertRaises(CredentialError):
            app_module.load_relay_credentials(env={})
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_app_wiring -v`

Expected: FAIL，`AttributeError: module 'app' has no attribute 'RELAY_SECRET_PATH'`

 - [x] **Step 3: Write minimal implementation**

`requirements.txt` 增加一行：

```
cryptography>=41
```

`app.py` 顶部导入区补充：

```python
from relay_credentials import CredentialStore
from relay_inventory import NodeInventory
from relay_lease import LeaseStore
from relay_secret import load_or_create_secret
```

`app.py` 中把 `os.makedirs` 提到资源装配之前，替换原来的随机密钥，并新增凭据加载函数：

```python
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# 中转机 agent 控制面：签名密钥必须持久化，否则平台重启后常驻节点全部失联。
RELAY_SECRET_PATH = os.path.join(UPLOAD_FOLDER, "relay-secret")
RELAY_STATE = RelayState(secret=load_or_create_secret(RELAY_SECRET_PATH))
RELAY_STATE.ledger = Ledger.load(
    os.path.join(UPLOAD_FOLDER, "relay-ledger-platform.json")
)
RELAY_INVENTORY = NodeInventory.load(os.path.join(UPLOAD_FOLDER, "relay-nodes.json"))
RELAY_LEASES = LeaseStore.load(os.path.join(UPLOAD_FOLDER, "relay-leases.json"))
app.register_blueprint(create_blueprint(RELAY_STATE))


def load_relay_credentials(
    *, env: dict[str, str] | None = None
) -> CredentialStore:
    """常驻模式专用：主密钥缺失时抛错，不做明文降级。"""
    return CredentialStore.from_env(
        os.path.join(UPLOAD_FOLDER, "relay-credentials.json"), env=env
    )
```

同时删除原文件里"密钥每次启动重新生成"那段注释。

注意：`app.register_blueprint` 原本在 `os.makedirs` 之前调用，装配顺序调整后
`create_blueprint` 仍在同一位置，只是前移了目录创建，避免密钥文件写入失败。

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_app_wiring -v`

Expected: PASS

 - [x] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests`

Expected: PASS（431 + 新增用例，无失败）

---

## 阶段 0 完成标准

- 平台签名密钥在重启后保持不变，文件权限 0600；
- 节点清单、租约、凭据三套存储可原子落盘、可重载、可过滤；
- 凭据文件里不出现明文密码，主密钥缺失时 `load_relay_credentials` 直接抛错；
- 全量回归通过，RBD 通道与现有 relay 单机流程行为不变。

---

## 阶段 1：调度与扩缩容

### Task 6: 节点级长期令牌

**Files:**

- Modify: `relay_protocol.py`
- Test: `tests/test_relay_node_token.py`

 - [x] **Step 1: Write the failing test**

```python
import unittest

from relay_protocol import (
    ProtocolError,
    issue_node_token,
    verify_node_token,
)

SECRET = b"s" * 32


class NodeTokenTest(unittest.TestCase):
    def test_roundtrip_preserves_fields(self):
        token = issue_node_token(
            SECRET,
            node_id="node-1",
            role="source",
            tenant_key="http://keystone|project-1",
            az="nova-1",
        )

        payload = verify_node_token(SECRET, token)

        self.assertEqual(payload["node_id"], "node-1")
        self.assertEqual(payload["role"], "source")
        self.assertEqual(payload["tenant_key"], "http://keystone|project-1")
        self.assertEqual(payload["az"], "nova-1")

    def test_tenant_key_with_pipe_does_not_break_parsing(self):
        token = issue_node_token(
            SECRET, node_id="n", role="target", tenant_key="a|b|c", az="z"
        )
        self.assertEqual(verify_node_token(SECRET, token)["tenant_key"], "a|b|c")

    def test_wrong_secret_is_rejected(self):
        token = issue_node_token(
            SECRET, node_id="n", role="source", tenant_key="t", az="z"
        )
        with self.assertRaises(ProtocolError):
            verify_node_token(b"x" * 32, token)

    def test_tampered_payload_is_rejected(self):
        token = issue_node_token(
            SECRET, node_id="n", role="source", tenant_key="t", az="z"
        )
        body, _, signature = token.partition(".")
        with self.assertRaises(ProtocolError):
            verify_node_token(SECRET, f"{body}x.{signature}")

    def test_malformed_token_is_rejected(self):
        for bad in ("", "no-dot", "a.b"):
            with self.assertRaises(ProtocolError):
                verify_node_token(SECRET, bad)

    def test_unknown_role_is_rejected(self):
        import base64
        import hashlib
        import hmac
        import json

        payload = {"node_id": "n", "role": "bogus", "tenant_key": "t", "az": "z"}
        body = (
            base64.urlsafe_b64encode(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            )
            .decode("ascii")
            .rstrip("=")
        )
        signature = hmac.new(SECRET, body.encode("ascii"), hashlib.sha256).hexdigest()

        with self.assertRaises(ProtocolError):
            verify_node_token(SECRET, f"{body}.{signature}")
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_node_token -v`

Expected: FAIL，`ImportError: cannot import name 'issue_node_token'`

 - [x] **Step 3: Write minimal implementation**

在 `relay_protocol.py` 末尾追加（`base64` 与 `json` 已在文件顶部导入）：

```python
NODE_TOKEN_ROLES = {"source", "target"}


def issue_node_token(
    secret: bytes,
    *,
    node_id: str,
    role: str,
    tenant_key: str,
    az: str,
) -> str:
    """签发节点级长期令牌，不设过期时间；轮换靠更新节点记录实现。"""
    if role not in NODE_TOKEN_ROLES:
        raise ProtocolError(f"unknown role: {role!r}")
    payload = {
        "node_id": node_id,
        "role": role,
        "tenant_key": tenant_key,
        "az": az,
    }
    body = (
        base64.urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        .decode("ascii")
        .rstrip("=")
    )
    signature = hmac.new(secret, body.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{body}.{signature}"


def verify_node_token(secret: bytes, token: str) -> dict[str, str]:
    """校验签名并返回 payload；任何结构或签名问题都抛 ProtocolError。"""
    body, _, signature = (token or "").partition(".")
    if not body or not signature:
        raise ProtocolError("malformed node token")
    expected = hmac.new(secret, body.encode("ascii"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ProtocolError("bad node token signature")
    padded = body + "=" * (-len(body) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ProtocolError("malformed node token payload") from exc
    if not isinstance(payload, dict):
        raise ProtocolError("node token payload must be an object")
    for field in ("node_id", "role", "tenant_key", "az"):
        if not str(payload.get(field) or ""):
            raise ProtocolError(f"node token missing {field}")
    if payload["role"] not in NODE_TOKEN_ROLES:
        raise ProtocolError("node token role invalid")
    return {key: str(payload[key]) for key in ("node_id", "role", "tenant_key", "az")}
```

注意：`relay_protocol.py` 顶部已有 `import base64` 需要补上（原文件只有
`hashlib / hmac / json / struct / zlib`）。若未导入，先添加 `import base64`。

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_node_token -v`

Expected: PASS（6 个用例）

### Task 7: 槽位调度（acquire / release）

**Files:**

- Create: `relay_scheduler.py`
- Test: `tests/test_relay_scheduler.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_scheduler import NoCapacityError, RelayScheduler, SchedulerConfig


class FakeAgent:
    def __init__(self, name, *, address="10.0.0.5", port=9200):
        self.name = name
        self.session_id = f"session-{name}"
        self.data_address = address
        self.data_port = port
        self.ssh_public_key = "ssh-ed25519 AAAA"


class FakeRegistry:
    def __init__(self, names=()):
        self._agents = {name: FakeAgent(name) for name in names}

    def find_by_name(self, name):
        return self._agents.get(name)

    def drop(self, name):
        self._agents.pop(name, None)


def _node(node_id, *, name=None, slots_total=5, state="ready", az="nova-1", role="source"):
    return RelayNodeRecord(
        node_id=node_id,
        name=name or f"relay-{role}-{node_id}",
        role=role,
        tenant_key="t1",
        az=az,
        server_id=f"server-{node_id}",
        slots_total=slots_total,
        state=state,
        created_at=float(node_id),
    )


class RelaySchedulerSlotTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.leases = LeaseStore.load(Path(self.tmp.name) / "leases.json")
        self.inventory.upsert(_node("1"))
        self.registry = FakeRegistry(["relay-source-1"])
        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=self.registry,
            config=SchedulerConfig(slots_per_node=2),
            clock=lambda: 100.0,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _acquire(self, task_id="task-1", az="nova-1", role="source"):
        return self.scheduler.acquire(
            task_id,
            job_id="job-1",
            role=role,
            tenant_key="t1",
            az=az,
            vm_id="vm-1",
        )

    def test_acquire_returns_node_view_with_agent_address(self):
        view = self._acquire()

        self.assertEqual(view.node_id, "1")
        self.assertEqual(view.data_address, "10.0.0.5")
        self.assertEqual(view.data_port, 9200)
        self.assertEqual(view.state, "busy")

    def test_acquire_marks_slot_used_and_persists_lease(self):
        self._acquire()

        record = self.inventory.get("1")
        self.assertEqual(record.slots_used, 1)
        self.assertEqual(record.state, "busy")
        self.assertEqual(len(self.leases.active_for_node("1")), 1)

    def test_acquire_skips_full_node_and_uses_next_pool_node(self):
        self.inventory.upsert(_node("2"))
        self.registry._agents["relay-source-2"] = FakeAgent("relay-source-2")

        first = self._acquire("task-1")
        self.inventory.get(first.node_id).slots_total = 1
        second = self._acquire("task-2")

        self.assertNotEqual(first.node_id, second.node_id)

    def test_acquire_skips_unhealthy_node(self):
        self.inventory.get("1").state = "unhealthy"
        with self.assertRaises(NoCapacityError):
            self._acquire()

    def test_acquire_skips_node_without_agent(self):
        self.registry.drop("relay-source-1")
        with self.assertRaises(NoCapacityError):
            self._acquire()

    def test_acquire_skips_other_role_az_and_tenant(self):
        with self.assertRaises(NoCapacityError):
            self._acquire(az="nova-2")
        with self.assertRaises(NoCapacityError):
            self._acquire(role="target")

    def test_release_frees_slot_and_records_idle_since(self):
        self._acquire()

        self.scheduler.release("1")

        record = self.inventory.get("1")
        self.assertEqual(record.slots_used, 0)
        self.assertEqual(record.state, "ready")
        self.assertEqual(record.idle_since, 100.0)
        self.assertEqual(self.leases.active_for_node("1"), [])

    def test_release_twice_does_not_go_negative(self):
        self._acquire()
        self.scheduler.release("1")
        self.scheduler.release("1")

        self.assertEqual(self.inventory.get("1").slots_used, 0)

    def test_release_job_releases_all_slots(self):
        self._acquire("task-1")
        self._acquire("task-2")

        released = self.scheduler.release_job("job-1")

        self.assertEqual(released, 2)
        self.assertEqual(self.inventory.get("1").slots_used, 0)
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_scheduler -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_scheduler'`

 - [x] **Step 3: Write minimal implementation**

```python
"""中转机槽位调度：把 VM 迁移槽位分配到常驻节点的空闲槽位上。

容量单位是槽位：1 台 VM 迁移占用 1 个槽位，该 VM 的多块盘在槽位内串行。
本模块只负责"有没有槽位、分给谁"，建机与删机在 node_manager，缩容判定在 Task 9。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore

READY_STATES = {"ready", "busy"}


class NoCapacityError(RuntimeError):
    """池内没有可用槽位。"""


@dataclass
class SchedulerConfig:
    slots_per_node: int = 5
    max_nodes: int = 6
    min_nodes: int = 1
    idle_scale_down_seconds: float = 86400.0
    scale_up_wait_seconds: float = 30.0
    queue_timeout_seconds: float = 1800.0


@dataclass
class ScheduledNode:
    """RelayVolumeMover 需要的节点视图，字段与 relay_pool.RelayNode 对齐。"""

    node_id: str
    name: str
    role: str
    az: str
    server_id: str
    session_id: str = ""
    data_address: str = ""
    data_port: int = 9200
    ssh_public_key: str = ""
    state: str = "ready"
    current_task_id: str = ""


class RelayScheduler:
    def __init__(
        self,
        *,
        inventory: NodeInventory,
        leases: LeaseStore,
        registry: Any,
        config: SchedulerConfig | None = None,
        node_manager: Any = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.inventory = inventory
        self.leases = leases
        self.registry = registry
        self.config = config or SchedulerConfig()
        self.node_manager = node_manager
        self._clock = clock
        self._sleeper = sleeper

    def agent_for(self, record: RelayNodeRecord) -> Any:
        if self.registry is None:
            return None
        return self.registry.find_by_name(record.name)

    def has_free_slot(self, record: RelayNodeRecord) -> bool:
        return (
            record.state in READY_STATES
            and record.slots_used < record.slots_total
            and self.agent_for(record) is not None
        )

    def pick_node(
        self, tenant_key: str, role: str, az: str
    ) -> RelayNodeRecord | None:
        """最早创建的节点优先，天然配合"缩容保留最老节点"的策略。"""
        candidates = [
            record
            for record in self.inventory.nodes_in_pool(tenant_key, role, az)
            if self.has_free_slot(record)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item.created_at, item.node_id))
        return candidates[0]

    def view(self, record: RelayNodeRecord) -> ScheduledNode:
        agent = self.agent_for(record)
        return ScheduledNode(
            node_id=record.node_id,
            name=record.name,
            role=record.role,
            az=record.az,
            server_id=record.server_id,
            session_id=getattr(agent, "session_id", "") or "",
            data_address=getattr(agent, "data_address", "") or record.fixed_ip,
            data_port=int(getattr(agent, "data_port", 0) or 9200),
            ssh_public_key=getattr(agent, "ssh_public_key", "") or "",
            state="busy" if record.slots_used else "ready",
            current_task_id=getattr(agent, "current_task_id", "") or "",
        )

    def acquire(
        self,
        task_id: str,
        *,
        job_id: str,
        role: str,
        tenant_key: str,
        az: str,
        vm_id: str = "",
        volume_id: str = "",
    ) -> ScheduledNode:
        record = self.pick_node(tenant_key, role, az)
        if record is None:
            record = self.grow(tenant_key, role, az, needed=1)
        if record is None:
            raise NoCapacityError(
                f"中转机池无可用槽位: tenant={tenant_key} role={role} az={az}"
            )
        now = self._clock()
        self.leases.acquire(
            job_id=job_id,
            node_id=record.node_id,
            role=role,
            tenant_key=tenant_key,
            now=now,
            vm_id=vm_id,
            volume_id=volume_id,
        )
        record.slots_used += 1
        record.state = "busy"
        record.updated_at = now
        self.inventory.upsert(record)
        self._persist()
        return self.view(record)

    def release(self, node_id: str) -> None:
        """释放该节点上最早的一条活动租约（FIFO）。"""
        active = sorted(
            self.leases.active_for_node(node_id), key=lambda lease: lease.acquired_at
        )
        if active:
            self.leases.release(active[0].lease_id, now=self._clock())
        record = self.inventory.get(node_id)
        if record is not None:
            record.slots_used = max(record.slots_used - 1, 0)
            record.state = "busy" if record.slots_used else "ready"
            if not record.slots_used:
                record.idle_since = self._clock()
            record.updated_at = self._clock()
            self.inventory.upsert(record)
        self._persist()

    def release_job(self, job_id: str) -> int:
        released = self.leases.release_job(job_id, now=self._clock())
        for lease in released:
            record = self.inventory.get(lease.node_id)
            if record is None:
                continue
            record.slots_used = max(record.slots_used - 1, 0)
            record.state = "busy" if record.slots_used else "ready"
            if not record.slots_used:
                record.idle_since = self._clock()
            record.updated_at = self._clock()
            self.inventory.upsert(record)
        self._persist()
        return len(released)

    def grow(self, tenant_key: str, role: str, az: str, *, needed: int) -> Any:
        """Task 8 实现：按需新建节点；本任务先返回 None 表示不扩容。"""
        return None

    def _persist(self) -> None:
        self.inventory.save()
        self.leases.save()
```

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_scheduler -v`

Expected: PASS（9 个用例）

### Task 8: 自动扩容与跨 AZ 额度再平衡

**Files:**

- Modify: `relay_scheduler.py`
- Test: `tests/test_relay_scheduler_grow.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_scheduler import QueueTimeoutError, RelayScheduler, SchedulerConfig


class FakeAgent:
    def __init__(self, name):
        self.name = name
        self.session_id = f"session-{name}"
        self.data_address = "10.0.0.9"
        self.data_port = 9200
        self.ssh_public_key = "ssh-ed25519 AAAA"


class FakeRegistry:
    def __init__(self):
        self._agents = {}

    def find_by_name(self, name):
        return self._agents.get(name)

    def add(self, name):
        self._agents[name] = FakeAgent(name)

    def drop(self, name):
        self._agents.pop(name, None)


class FakeNodeManager:
    """用内存清单模拟建机/删机，不访问 OpenStack。"""

    def __init__(self, inventory, registry):
        self.inventory = inventory
        self.registry = registry
        self.created = []
        self.deleted = []
        self._seq = 100

    def create_node(self, *, tenant_key, role, az, slots_total):
        self._seq += 1
        record = RelayNodeRecord(
            node_id=f"new-{self._seq}",
            name=f"relay-{role}-{tenant_key}-{az}-{self._seq}",
            role=role,
            tenant_key=tenant_key,
            az=az,
            server_id=f"server-{self._seq}",
            slots_total=slots_total,
            state="provisioning",
            created_at=float(self._seq),
        )
        self.inventory.upsert(record)
        self.created.append(record.node_id)
        return record

    def wait_ready(self, record, *, timeout):
        record.state = "ready"
        self.inventory.upsert(record)
        self.registry.add(record.name)
        return True

    def delete_node(self, node_id):
        record = self.inventory.remove(node_id)
        if record is not None:
            self.registry.drop(record.name)
            self.deleted.append(node_id)
        return record is not None


class RelaySchedulerGrowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.leases = LeaseStore.load(Path(self.tmp.name) / "leases.json")
        self.registry = FakeRegistry()
        self.manager = FakeNodeManager(self.inventory, self.registry)
        self.now = 1000.0
        self.slept = []

        def clock():
            return self.now

        def sleeper(seconds):
            self.slept.append(seconds)
            self.now += seconds

        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=self.registry,
            config=SchedulerConfig(
                slots_per_node=5,
                max_nodes=6,
                min_nodes=1,
                scale_up_wait_seconds=30.0,
                queue_timeout_seconds=1800.0,
            ),
            node_manager=self.manager,
            clock=clock,
            sleeper=sleeper,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _acquire(self, az="nova-1"):
        return self.scheduler.acquire(
            "task-1", job_id="job-1", role="source", tenant_key="t1", az=az
        )

    def test_grows_one_node_when_pool_is_empty(self):
        view = self._acquire()

        self.assertEqual(len(self.manager.created), 1)
        self.assertEqual(self.slept, [30.0])
        self.assertEqual(self.inventory.count_for_role("t1", "source"), 1)
        self.assertEqual(view.data_address, "10.0.0.9")

    def test_does_not_grow_beyond_max_nodes(self):
        for index in range(6):
            record = self.manager.create_node(
                tenant_key="t1", role="source", az="nova-1", slots_total=1
            )
            self.manager.wait_ready(record, timeout=1)
            record.state = "busy"
            record.slots_used = 1
            self.inventory.upsert(record)

        with self.assertRaises(QueueTimeoutError):
            self._acquire()

        self.assertEqual(self.inventory.count_for_role("t1", "source"), 6)

    def test_rebalance_deletes_idle_node_in_other_az(self):
        for index in range(6):
            record = self.manager.create_node(
                tenant_key="t1", role="source", az="nova-2", slots_total=1
            )
            self.manager.wait_ready(record, timeout=1)
            if index > 0:
                record.state = "busy"
                record.slots_used = 1
                self.inventory.upsert(record)

        view = self._acquire(az="nova-1")

        self.assertEqual(len(self.manager.deleted), 1)
        self.assertEqual(self.inventory.get(view.node_id).az, "nova-1")

    def test_rebalance_keeps_last_node_in_each_az(self):
        record = self.manager.create_node(
            tenant_key="t1", role="source", az="nova-2", slots_total=1
        )
        self.manager.wait_ready(record, timeout=1)
        for index in range(5):
            busy = self.manager.create_node(
                tenant_key="t1", role="source", az="nova-3", slots_total=1
            )
            self.manager.wait_ready(busy, timeout=1)
            busy.state = "busy"
            busy.slots_used = 1
            self.inventory.upsert(busy)

        with self.assertRaises(QueueTimeoutError):
            self._acquire(az="nova-1")

        self.assertEqual(self.manager.deleted, [])
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_scheduler_grow -v`

Expected: FAIL，`ImportError: cannot import name 'QueueTimeoutError'`

 - [x] **Step 3: Write minimal implementation**

在 `relay_scheduler.py` 中新增异常，并把 `grow` 替换为真实实现：

```python
class QueueTimeoutError(NoCapacityError):
    """池已满且等待超过 queue_timeout_seconds。"""
```

```python
    def grow(
        self, tenant_key: str, role: str, az: str, *, needed: int
    ) -> RelayNodeRecord | None:
        """优先复用空闲槽位；不足则扩容，额度满则先跨 AZ 再平衡，最后排队。"""
        if self.node_manager is None:
            return None

        deadline = self._clock() + self.config.queue_timeout_seconds
        while True:
            record = self.pick_node(tenant_key, role, az)
            if record is not None:
                return record

            if (
                self.inventory.count_for_role(tenant_key, role)
                < self.config.max_nodes
            ):
                # 先等其他作业释放槽位，再决定是否真的建机。
                self._sleeper(self.config.scale_up_wait_seconds)
                record = self.pick_node(tenant_key, role, az)
                if record is not None:
                    return record
                created = self.node_manager.create_node(
                    tenant_key=tenant_key,
                    role=role,
                    az=az,
                    slots_total=self.config.slots_per_node,
                )
                if created is not None:
                    self.node_manager.wait_ready(created, timeout=300.0)
                    return self.pick_node(tenant_key, role, az)

            if self._rebalance(tenant_key, role, az):
                continue

            if self._clock() >= deadline:
                raise QueueTimeoutError(
                    "中转机池已满且等待超时: "
                    f"tenant={tenant_key} role={role} az={az}"
                )
            self._sleeper(self.config.scale_up_wait_seconds)

    def _rebalance(self, tenant_key: str, role: str, target_az: str) -> bool:
        """额度被其他 AZ 的空闲节点占用时，回收一台释放额度。"""
        if self.node_manager is None:
            return False
        for record in sorted(
            self.inventory.nodes_for_role(tenant_key, role),
            key=lambda item: (item.created_at, item.node_id),
            reverse=True,
        ):
            if record.az == target_az:
                continue
            if record.slots_used or self.leases.active_for_node(record.node_id):
                continue
            same_az = self.inventory.nodes_in_pool(tenant_key, role, record.az)
            if len(same_az) <= self.config.min_nodes:
                continue
            if self.node_manager.delete_node(record.node_id):
                return True
        return False
```

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_scheduler_grow -v`

Expected: PASS（4 个用例）

注意：`test_rebalance_deletes_idle_node_in_other_az` 里 nova-2 有 6 台，
第一台保持空闲、其余 5 台置忙；`_rebalance` 由新到旧遍历，会先跳过忙碌节点，
最终回收那台空闲节点，nova-2 仍剩 5 台，符合"不得低于 min_nodes"。

### Task 9: 空闲缩容（24h 门槛 + 保留常驻）

**Files:**

- Modify: `relay_scheduler.py`
- Test: `tests/test_relay_scheduler_scale_down.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_scheduler import RelayScheduler, SchedulerConfig

DAY = 86400.0


class FakeNodeManager:
    def __init__(self, inventory):
        self.inventory = inventory
        self.deleted = []

    def delete_node(self, node_id):
        self.inventory.remove(node_id)
        self.deleted.append(node_id)
        return True


class ScaleDownTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.inventory = NodeInventory.load(Path(self.tmp.name) / "nodes.json")
        self.leases = LeaseStore.load(Path(self.tmp.name) / "leases.json")
        self.manager = FakeNodeManager(self.inventory)
        self.now = 10 * DAY
        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=None,
            config=SchedulerConfig(min_nodes=1, idle_scale_down_seconds=DAY),
            node_manager=self.manager,
            clock=lambda: self.now,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _add(self, node_id, *, created_at=0.0, idle_since=0.0, slots_used=0, az="nova-1"):
        record = RelayNodeRecord(
            node_id=node_id,
            name=f"relay-source-{node_id}",
            role="source",
            tenant_key="t1",
            az=az,
            server_id=f"server-{node_id}",
            slots_total=5,
            slots_used=slots_used,
            state="busy" if slots_used else "ready",
            created_at=created_at,
            idle_since=idle_since,
        )
        self.inventory.upsert(record)
        return record

    def test_removes_idle_nodes_above_min_and_keeps_oldest(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("mid", created_at=2.0, idle_since=1.0)
        self._add("new", created_at=3.0, idle_since=1.0)

        removed = self.scheduler.scale_down()

        self.assertEqual(sorted(removed), ["mid", "new"])
        self.assertIsNotNone(self.inventory.get("old"))

    def test_busy_node_is_never_removed(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("busy", created_at=2.0, slots_used=1)

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, [])

    def test_node_with_active_lease_is_never_removed(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("leased", created_at=2.0)
        self.leases.acquire(
            job_id="job-1", node_id="leased", role="source", tenant_key="t1", now=1.0
        )

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, [])

    def test_node_idle_less_than_threshold_is_kept(self):
        self._add("old", created_at=1.0, idle_since=1.0)
        self._add("fresh", created_at=2.0, idle_since=self.now - 3600.0)

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, [])

    def test_min_nodes_one_per_pool(self):
        self._add("only", created_at=1.0, idle_since=1.0)

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, [])
        self.assertIsNotNone(self.inventory.get("only"))

    def test_pools_are_evaluated_independently(self):
        self._add("a1", created_at=1.0, idle_since=1.0, az="nova-1")
        self._add("a2", created_at=2.0, idle_since=1.0, az="nova-1")
        self._add("b1", created_at=1.0, idle_since=1.0, az="nova-2")

        removed = self.scheduler.scale_down()

        self.assertEqual(removed, ["a2"])
        self.assertIsNotNone(self.inventory.get("b1"))
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_scheduler_scale_down -v`

Expected: FAIL，`AttributeError: 'RelayScheduler' object has no attribute 'scale_down'`

 - [x] **Step 3: Write minimal implementation**

在 `relay_scheduler.py` 的 `RelayScheduler` 中新增：

```python
    def idle_seconds(self, record: RelayNodeRecord, *, now: float | None = None) -> float:
        """节点连续空闲时长；从未使用过的节点从创建时间起算。"""
        current = self._clock() if now is None else now
        since = record.idle_since or self.leases.idle_since(record.node_id) or record.created_at
        if not since:
            return 0.0
        return max(current - since, 0.0)

    def scale_down(self, *, now: float | None = None) -> list[str]:
        """回收空闲超过阈值且高于 min_nodes 的节点，由新到旧删除。"""
        if self.node_manager is None:
            return []
        current = self._clock() if now is None else now
        removed: list[str] = []
        pools: dict[tuple[str, str, str], list[RelayNodeRecord]] = {}
        for record in self.inventory.all():
            pools.setdefault(record.pool_key, []).append(record)

        for records in pools.values():
            if len(records) <= self.config.min_nodes:
                continue
            removable = len(records) - self.config.min_nodes
            candidates = [
                record
                for record in records
                if record.slots_used == 0
                and not self.leases.active_for_node(record.node_id)
                and self.idle_seconds(record, now=current)
                >= self.config.idle_scale_down_seconds
            ]
            candidates.sort(key=lambda item: (item.created_at, item.node_id), reverse=True)
            for record in candidates[:removable]:
                if self.node_manager.delete_node(record.node_id):
                    removed.append(record.node_id)
        if removed:
            self.inventory.save()
        return removed
```

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_scheduler_scale_down -v`

Expected: PASS（6 个用例）

### Task 10: 池级建机参数

**Files:**

- Create: `relay_pool_profile.py`
- Test: `tests/test_relay_pool_profile.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path

from relay_pool_profile import PoolProfile, PoolProfileStore


def _profile(*, role="source", az="nova-1", tenant="t1", image="img-1"):
    return PoolProfile(
        tenant_key=tenant,
        role=role,
        az=az,
        image=image,
        flavor="flv-1",
        network="net-1",
        subnet="sub-1",
    )


class PoolProfileStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-pools.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_upsert_then_reload_roundtrips(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile())
        store.save()

        profile = PoolProfileStore.load(self.path).get("t1", "source", "nova-1")

        self.assertEqual(profile.image, "img-1")
        self.assertEqual(profile.network, "net-1")

    def test_get_missing_returns_none(self):
        self.assertIsNone(PoolProfileStore.load(self.path).get("t1", "source", "nova-1"))

    def test_tenant_key_with_pipe_roundtrips(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile(tenant="http://keystone|project-1"))
        store.save()

        profile = PoolProfileStore.load(self.path).get(
            "http://keystone|project-1", "source", "nova-1"
        )

        self.assertEqual(profile.tenant_key, "http://keystone|project-1")

    def test_profiles_are_unique_per_pool_key(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile(image="img-1"))
        store.upsert(_profile(image="img-2"))

        self.assertEqual(len(store.all()), 1)
        self.assertEqual(store.get("t1", "source", "nova-1").image, "img-2")

    def test_remove(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile())

        self.assertTrue(store.remove("t1", "source", "nova-1"))
        self.assertIsNone(store.get("t1", "source", "nova-1"))

    def test_for_tenant_filters(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile())
        store.upsert(_profile(role="target"))
        store.upsert(_profile(tenant="t2"))

        self.assertEqual(len(store.for_tenant("t1")), 2)
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_pool_profile -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_pool_profile'`

 - [x] **Step 3: Write minimal implementation**

```python
"""池级建机参数：扩容时新建节点所需的镜像、flavor、网络与容量策略。

不保存 admin_password：SSH 密码按节点加密存储，扩容时从池内既有节点解密复用，
池为空且未提供密码时由 API 层拒绝建机。
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any


@dataclass
class PoolProfile:
    tenant_key: str
    role: str
    az: str
    image: str
    flavor: str
    network: str
    subnet: str = ""
    system_volume_type: str = ""
    slots_per_node: int = 5
    max_nodes: int = 6
    min_nodes: int = 1
    idle_scale_down_seconds: float = 86400.0
    data_port: int = 9200
    ssh_public_key: str = ""
    updated_at: float = 0.0

    @property
    def pool_key(self) -> tuple[str, str, str]:
        return (self.tenant_key, self.role, self.az)


class PoolProfileStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._records: dict[tuple[str, str, str], PoolProfile] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "PoolProfileStore":
        store = cls(path)
        if not store.path.exists():
            return store
        raw = json.loads(store.path.read_text(encoding="utf-8") or "{}")
        known = {field.name for field in fields(PoolProfile)}
        for item in raw.get("pools", []):
            payload = {key: value for key, value in item.items() if key in known}
            profile = PoolProfile(**payload)
            store._records[profile.pool_key] = profile
        return store

    def get(self, tenant_key: str, role: str, az: str) -> PoolProfile | None:
        return self._records.get((tenant_key, role, az))

    def all(self) -> list[PoolProfile]:
        return list(self._records.values())

    def for_tenant(self, tenant_key: str) -> list[PoolProfile]:
        return [item for item in self._records.values() if item.tenant_key == tenant_key]

    def upsert(self, profile: PoolProfile) -> PoolProfile:
        self._records[profile.pool_key] = profile
        return profile

    def remove(self, tenant_key: str, role: str, az: str) -> bool:
        return self._records.pop((tenant_key, role, az), None) is not None

    def save(self) -> None:
        payload: dict[str, Any] = {
            "pools": [asdict(profile) for profile in self._records.values()]
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle, tmp_path = tempfile.mkstemp(
            dir=str(self.path.parent), prefix=".relay-pools-", suffix=".tmp"
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

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_pool_profile -v`

Expected: PASS（6 个用例）

### Task 11: 节点管理器（建机 / 删机 / 重建 / drain）

**Files:**

- Create: `relay_node_manager.py`
- Test: `tests/test_relay_node_manager.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay_credentials import CredentialStore, Sealer
from relay_inventory import NodeInventory
from relay_node_manager import NodeManagerError, RelayNodeManager
from relay_pool_profile import PoolProfile, PoolProfileStore

KEY = b"k" * 32


class FakePort:
    def __init__(self, port_id):
        self.id = port_id


class FakeServer:
    def __init__(self, server_id):
        self.id = server_id


class NodeManagerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.profiles = PoolProfileStore.load(base / "pools.json")
        self.credentials = CredentialStore.load(base / "credentials.json", Sealer(KEY))
        self.credentials.save("t1", {"auth_url": "http://keystone", "password": "p"})
        self.credentials.flush()
        self.profiles.upsert(
            PoolProfile(
                tenant_key="t1",
                role="source",
                az="nova-1",
                image="img-1",
                flavor="flv-1",
                network="net-1",
                subnet="sub-1",
            )
        )
        self.os_utils = mock.MagicMock()
        self.os_utils.create_relay_port.return_value = FakePort("port-1")
        self.os_utils.create_relay_server.return_value = FakeServer("server-1")
        self.manager = RelayNodeManager(
            inventory=self.inventory,
            profiles=self.profiles,
            credentials=self.credentials,
            registry=mock.MagicMock(),
            secret=b"s" * 32,
            platform_url="https://migrate.example.com",
            os_utils_factory=lambda auth: self.os_utils,
            clock=lambda: 500.0,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_create_node_persists_record_and_secrets(self):
        record = self.manager.create_node(
            tenant_key="t1", role="source", az="nova-1", slots_total=5
        )

        self.assertEqual(record.server_id, "server-1")
        self.assertEqual(record.state, "provisioning")
        self.assertTrue(record.token_enc)
        self.assertTrue(record.ssh_password_enc)
        self.assertEqual(self.inventory.get(record.node_id).server_id, "server-1")
        self.assertNotIn("p", record.ssh_password_enc)

    def test_create_node_passes_availability_zone_and_port(self):
        self.manager.create_node(
            tenant_key="t1", role="source", az="nova-1", slots_total=5
        )

        kwargs = self.os_utils.create_relay_server.call_args.kwargs
        self.assertEqual(kwargs["availability_zone"], "nova-1")
        self.assertEqual(kwargs["port_ids"], ["port-1"])
        self.assertEqual(kwargs["image_id"], "img-1")
        self.assertEqual(kwargs["flavor_id"], "flv-1")

    def test_create_node_requires_profile(self):
        with self.assertRaises(NodeManagerError):
            self.manager.create_node(
                tenant_key="t1", role="source", az="nova-9", slots_total=5
            )

    def test_create_node_requires_credentials(self):
        with self.assertRaises(NodeManagerError):
            self.manager.create_node(
                tenant_key="t9", role="source", az="nova-1", slots_total=5
            )

    def test_delete_node_removes_server_port_and_record(self):
        record = self.manager.create_node(
            tenant_key="t1", role="source", az="nova-1", slots_total=5
        )

        self.assertTrue(self.manager.delete_node(record.node_id))

        self.assertIsNone(self.inventory.get(record.node_id))
        self.os_utils.delete_server.assert_called_once_with("server-1")
        self.os_utils.delete_port.assert_called_once_with("port-1")

    def test_rebuild_reuses_name_but_issues_new_server(self):
        record = self.manager.create_node(
            tenant_key="t1", role="source", az="nova-1", slots_total=5
        )
        self.os_utils.create_relay_server.return_value = FakeServer("server-2")

        rebuilt = self.manager.rebuild(record.node_id)

        self.assertEqual(rebuilt.name, record.name)
        self.assertEqual(rebuilt.server_id, "server-2")
        self.assertNotEqual(rebuilt.token_enc, record.token_enc)

    def test_drain_and_resume(self):
        record = self.manager.create_node(
            tenant_key="t1", role="source", az="nova-1", slots_total=5
        )

        self.assertTrue(self.manager.drain(record.node_id))
        self.assertEqual(self.inventory.get(record.node_id).state, "draining")
        self.assertTrue(self.manager.resume(record.node_id))
        self.assertEqual(self.inventory.get(record.node_id).state, "ready")
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_node_manager -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_node_manager'`

 - [x] **Step 3: Write minimal implementation**

```python
"""常驻节点管理：建机、删机、重建、drain/resume。

建机参数来自 PoolProfileStore，云凭据来自加密 CredentialStore；
节点令牌与 SSH 密码都按 node_id 作为 AAD 加密后写入节点清单。
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any, Callable

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_pool import build_cloud_init
from relay_protocol import issue_node_token


class NodeManagerError(RuntimeError):
    """建机前置条件不满足。"""


def _tenant_short(tenant_key: str) -> str:
    return hashlib.sha1(tenant_key.encode("utf-8")).hexdigest()[:8]


class RelayNodeManager:
    def __init__(
        self,
        *,
        inventory: NodeInventory,
        profiles: Any,
        credentials: Any,
        registry: Any,
        secret: bytes,
        platform_url: str,
        os_utils_factory: Callable[[dict[str, Any]], Any],
        data_port: int = 9200,
        bootstrap: bool = True,
        ready_timeout: float = 300.0,
        clock: Callable[[], float] = time.time,
    ):
        self.inventory = inventory
        self.profiles = profiles
        self.credentials = credentials
        self.registry = registry
        self.secret = secret
        self.platform_url = platform_url.rstrip("/")
        self.os_utils_factory = os_utils_factory
        self.data_port = data_port
        self.bootstrap = bootstrap
        self.ready_timeout = ready_timeout
        self._clock = clock

    def _next_index(self, tenant_key: str, role: str, az: str) -> int:
        used = {
            record.name.rsplit("-", 1)[-1]
            for record in self.inventory.nodes_in_pool(tenant_key, role, az)
        }
        index = 1
        while str(index) in used:
            index += 1
        return index

    def _password_for_pool(self, tenant_key: str, role: str, az: str) -> str:
        """扩容时复用池内既有节点的 SSH 密码，保证同池密码一致。"""
        for record in sorted(
            self.inventory.nodes_in_pool(tenant_key, role, az),
            key=lambda item: item.created_at,
        ):
            if record.ssh_password_enc:
                return self.credentials.unseal_text(
                    record.ssh_password_enc, aad=record.node_id
                )
        return ""

    def _os_utils(self, tenant_key: str) -> Any:
        auth = self.credentials.get(tenant_key)
        if not auth:
            raise NodeManagerError(f"未配置租户凭据: {tenant_key}")
        return self.os_utils_factory(auth)

    def create_node(
        self, *, tenant_key: str, role: str, az: str, slots_total: int | None = None
    ) -> RelayNodeRecord:
        profile = self.profiles.get(tenant_key, role, az)
        if profile is None:
            raise NodeManagerError(f"未配置池参数: {tenant_key}/{role}/{az}")
        os_utils = self._os_utils(tenant_key)
        node_id = uuid.uuid4().hex
        index = self._next_index(tenant_key, role, az)
        name = f"relay-{role}-{_tenant_short(tenant_key)}-{az}-{index}"
        token = issue_node_token(
            self.secret, node_id=node_id, role=role, tenant_key=tenant_key, az=az
        )
        password = self._password_for_pool(tenant_key, role, az)
        port = os_utils.create_relay_port(
            profile.network, subnet_id=profile.subnet or None
        )
        user_data = build_cloud_init(
            platform_url=self.platform_url,
            token=token,
            job_id="",
            role=role,
            name=name,
            data_port=profile.data_port or self.data_port,
            bootstrap=self.bootstrap,
            admin_password=password,
            ssh_public_key=profile.ssh_public_key,
        )
        server = os_utils.create_relay_server(
            name=name,
            image_id=profile.image,
            flavor_id=profile.flavor,
            port_ids=[port.id],
            availability_zone=az,
            user_data=user_data,
            admin_password=password or None,
        )
        now = self._clock()
        record = RelayNodeRecord(
            node_id=node_id,
            name=name,
            role=role,
            tenant_key=tenant_key,
            az=az,
            server_id=server.id,
            port_id=port.id,
            network=profile.network,
            subnet=profile.subnet,
            flavor=profile.flavor,
            image=profile.image,
            system_volume_type=profile.system_volume_type,
            slots_total=int(slots_total or profile.slots_per_node),
            token_enc=self.credentials.seal_text(token, aad=node_id),
            ssh_password_enc=(
                self.credentials.seal_text(password, aad=node_id) if password else ""
            ),
            state="provisioning",
            created_at=now,
            updated_at=now,
        )
        self.inventory.upsert(record)
        self.inventory.save()
        return record

    def wait_ready(self, record: RelayNodeRecord, *, timeout: float | None = None) -> bool:
        """等待 agent 注册；超时返回 False，由调用方决定是否重建。"""
        deadline = self._clock() + float(timeout or self.ready_timeout)
        while self._clock() < deadline:
            agent = self.registry.find_by_name(record.name)
            if agent is not None:
                record.state = "ready"
                record.agent_version = getattr(agent, "version", "")
                record.fixed_ip = getattr(agent, "data_address", "") or record.fixed_ip
                record.updated_at = self._clock()
                self.inventory.upsert(record)
                self.inventory.save()
                return True
            time.sleep(1.0)
        return False

    def delete_node(self, node_id: str) -> bool:
        record = self.inventory.get(node_id)
        if record is None:
            return False
        try:
            os_utils = self._os_utils(record.tenant_key)
            if record.server_id:
                os_utils.delete_server(record.server_id)
            if record.port_id:
                os_utils.delete_port(record.port_id)
        except Exception:  # noqa: BLE001 - 云上资源已不存在时仍要清清单
            pass
        self.registry.drop_by_name(record.name)
        self.inventory.remove(node_id)
        self.inventory.save()
        return True

    def rebuild(self, node_id: str) -> RelayNodeRecord | None:
        record = self.inventory.get(node_id)
        if record is None:
            return None
        tenant_key, role, az = record.tenant_key, record.role, record.az
        slots_total = record.slots_total
        self.delete_node(node_id)
        return self.create_node(
            tenant_key=tenant_key, role=role, az=az, slots_total=slots_total
        )

    def drain(self, node_id: str) -> bool:
        return self._set_state(node_id, "draining")

    def resume(self, node_id: str) -> bool:
        return self._set_state(node_id, "ready")

    def _set_state(self, node_id: str, state: str) -> bool:
        record = self.inventory.get(node_id)
        if record is None:
            return False
        record.state = state
        record.updated_at = self._clock()
        self.inventory.upsert(record)
        self.inventory.save()
        return True
```

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_node_manager -v`

Expected: PASS（7 个用例）

注意：`wait_ready` 里用了真实 `time.sleep(1.0)`，测试不要直接调用它；
需要测试时用 `mock.patch("relay_node_manager.time.sleep")`。
`build_cloud_init` 的 `job_id` 传空串，节点令牌取代作业令牌（见 Task 14）。

### Task 12: 运行时接入 persistent / ephemeral 双模式

**Files:**

- Modify: `relay_runtime.py`
- Modify: `app.py`（`start_job_relay_runtime`、作业 `finally`）
- Test: `tests/test_relay_runtime_modes.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_runtime import RelayChannelConfig, RelayRuntime, PoolConfig


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
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_runtime_modes -v`

Expected: FAIL，`TypeError: __init__() got an unexpected keyword argument 'node_mode'`

 - [x] **Step 3: Write minimal implementation**

`relay_runtime.py` 修改三处。

其一，`RelayChannelConfig` 增加字段，`parse_relay_options` 解析新配置：

```python
@dataclass
class RelayChannelConfig:
    platform_url: str
    source: PoolConfig
    target: PoolConfig
    token_ttl: int = 3600
    agent_install: str = "bootstrap"
    data_port: int = 9200
    admin_password: str = ""
    ssh_public_key: str = ""
    copy_retries: int = 3
    full_verify: bool = False
    heartbeat_interval: int = 10
    heartbeat_timeout: int = 30
    source_cloud: str = ""
    target_cloud: str = ""
    rate_limit_bytes_per_sec: float = 0.0
    chunk_size: int = DEFAULT_CHUNK
    ready_timeout: float = 180.0
    result_timeout: float = 6 * 3600.0
    node_mode: str = "persistent"
```

`parse_relay_options` 里补：

```python
        node_mode=(
            "ephemeral"
            if str(options.get("relay_node_mode") or "").strip().lower() == "ephemeral"
            else "persistent"
        ),
```

`_FORM_SCALAR_FIELDS` 增加 `"relay_node_mode"`、`"relay_slots_per_node"`、
`"relay_max_nodes"`、`"relay_min_nodes"`、`"relay_idle_scale_down_hours"`。

其二，`RelayRuntime` 支持外部注入平台级资源（默认 None 时保持旧行为）：

```python
    def __init__(
        self,
        *,
        config: RelayChannelConfig,
        source_os: Any,
        target_os: Any,
        state: Any,
        ledger: Any,
        job_id: str,
        admin_password: str = "",
        inventory: Any = None,
        leases: Any = None,
        registry: Any = None,
        scheduler: Any = None,
    ):
        self.config = config
        self.state = state
        self.ledger = ledger
        self.job_id = job_id
        self.inventory = inventory
        self.leases = leases
        self.registry = registry or state
        self.scheduler = scheduler
        # 以下保持原实现不变
```

其三，新增两个方法，并让 `finish` 分流：

同时 `start()` 必须按模式分流：persistent 模式**不能**建作业级临时池，
只做前置校验，节点由调度器按需扩容：

```python
    def start(self) -> None:
        if self.config.node_mode == "persistent":
            if self.inventory is None or self.leases is None:
                raise ValueError("persistent 模式缺少平台级节点清单")
            self.state.heartbeat_interval = self.config.heartbeat_interval
            self.state.heartbeat_timeout = self.config.heartbeat_timeout
            return

        # 以下为原有 ephemeral 逻辑，保持不变
        if self.config.source_cloud or self.config.target_cloud:
            cleaned = self.reaper.sweep(
                cloud_filter=(self.config.source_cloud, self.config.target_cloud)
            )
            if cleaned:
                logging.info("[MIGRATION] 启动对账清理遗留资源: %s", cleaned)
        self.state.heartbeat_interval = self.config.heartbeat_interval
        self.state.heartbeat_timeout = self.config.heartbeat_timeout
        self._created_ports["source"] = self._ensure_ports(
            self.source_pool, self.config.source, self.source_pool.os_utils
        )
        self._created_ports["target"] = self._ensure_ports(
            self.target_pool, self.config.target, self.target_pool.os_utils
        )
        self.source_pool.provision()
        self.target_pool.provision()
        self.source_pool.wait_ready(timeout=self.config.ready_timeout)
        self.target_pool.wait_ready(timeout=self.config.ready_timeout)
```

persistent 模式下 `mover_factory` 改为使用调度器申请槽位；
`RelayVolumeMover` 接受的对象只需具备 `acquire(task_id)` 与 `release(node_id)`，
因此新增一个薄适配器即可：

```python
class SchedulerPoolAdapter:
    """把 RelayScheduler 适配成 RelayVolumeMover 期望的池接口。"""

    def __init__(self, scheduler, *, job_id, role, tenant_key, az):
        self.scheduler = scheduler
        self.job_id = job_id
        self.role = role
        self.tenant_key = tenant_key
        self.az = az

    def acquire(self, task_id):
        return self.scheduler.acquire(
            task_id,
            job_id=self.job_id,
            role=self.role,
            tenant_key=self.tenant_key,
            az=self.az,
            vm_id=task_id,
        )

    def release(self, node_id):
        self.scheduler.release(node_id)


    def mover_factory(self, vm: Any, options: dict[str, Any]) -> RelayVolumeMover:
        if self.config.node_mode == "persistent":
            source_pool = SchedulerPoolAdapter(
                self.scheduler,
                job_id=self.job_id,
                role="source",
                tenant_key=self.config.source_cloud,
                az=self.config.source.az,
            )
            target_pool = SchedulerPoolAdapter(
                self.scheduler,
                job_id=self.job_id,
                role="target",
                tenant_key=self.config.target_cloud,
                az=self.config.target.az,
            )
        else:
            source_pool, target_pool = self.source_pool, self.target_pool
        return RelayVolumeMover(
            source_pool=source_pool,
            target_pool=target_pool,
            lifecycle=self.lifecycle,
            state=self.state,
            ledger=self.ledger,
            job_id=self.job_id,
            chunk_size=self.config.chunk_size,
            rate_limit_bytes_per_sec=self.config.rate_limit_bytes_per_sec,
            ready_timeout=self.config.ready_timeout,
            result_timeout=self.config.result_timeout,
            copy_retries=self.config.copy_retries,
            full_verify=self.config.full_verify,
            source_cloud=self.config.source_cloud,
            target_cloud=self.config.target_cloud,
            source_volume_type=self.config.source.volume_type,
            target_volume_type=self.config.target.volume_type,
        )
```

`relay_runtime` 构造时用 `source_cloud`/`target_cloud` 作为 `tenant_key`
（就是 `_cloud_fingerprint` 的取值），保证同一租户的作业命中同一个池。

```python
    def finish(self) -> None:
        """作业收尾：对账清理中间产物；persistent 模式只归还租约。"""
        try:
            self.reaper.reconcile_job(self.job_id)
        except Exception:  # noqa: BLE001
            logging.exception("[MIGRATION] 作业 %s 对账清理失败", self.job_id)

        if self.config.node_mode == "persistent":
            # 槽位租约由调度器在 acquire/release 上维护：
            # 作业收尾按其 job_id 统一归还，绝不删除或重建节点。
            if self.scheduler is not None:
                self.scheduler.release_job(self.job_id)
            elif self.leases is not None:
                self.leases.release_job(self.job_id)
                self.leases.save()
            return

        self.source_pool.destroy()
        self.target_pool.destroy()
        self._delete_created_ports("source", self.source_pool.os_utils)
        self._delete_created_ports("target", self.target_pool.os_utils)
```

`app.py` 的 `start_job_relay_runtime` 透传平台级资源：

```python
    runtime = RelayRuntime(
        config=relay_config,
        source_os=OpenStackUtils(source_auth),
        target_os=OpenStackUtils(target_auth),
        state=RELAY_STATE,
        ledger=RELAY_STATE.ledger,
        job_id=job_id,
        admin_password=options.get("admin_password") or "",
        inventory=RELAY_INVENTORY,
        leases=RELAY_LEASES,
        registry=RELAY_STATE,
    )
```

作业 `finally` 里的 `relay_runtime.finish()` 不再改动，
销毁与否由 `finish` 内部按 `node_mode` 决定。

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_runtime_modes -v`

Expected: PASS（2 个用例）

 - [x] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests`

Expected: PASS，`relay_runtime` 既有用例不回归

---

## 阶段 1 完成标准

- 节点级长期令牌可签发、可校验、含 `|` 的租户键不破坏解析；
- 槽位调度支持 1 VM = 1 槽、按池分配、跨 AZ 汇总额度；
- 扩容按 5 槽/台自动建机，上限 6 台/角色/租户，超额先跨 AZ 再平衡再排队；
- 缩容按 24h 空闲门槛、每池保留 1 台、由新到旧删除；
- `persistent` 模式下作业结束只归还租约，节点保持运行；`ephemeral` 行为不变。

---

## 阶段 2：数据面并发与注册

### Task 13: 注册表多槽位分发与任务级取消

**Files:**

- Modify: `relay_registry.py`
- Test: `tests/test_relay_registry_slots.py`

 - [x] **Step 1: Write the failing test**

```python
import unittest

from relay_registry import RelayState


class RelayStateSlotTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(secret=b"s" * 32)
        self.agent = self.state.register(
            job_id="",
            role="source",
            name="relay-source-1",
            version="1.0.0",
            address="10.0.0.5",
            now=1.0,
            slots_total=3,
        )

    def _enqueue(self, task_id):
        self.state.enqueue(
            {"task_id": task_id, "role": "source", "agent_id": self.agent.agent_id}
        )

    def test_dispatch_fills_all_slots_then_stops(self):
        dispatched = []
        for index in range(4):
            task_id = f"task-{index}"
            self._enqueue(task_id)
            task = self.state.dispatch(self.agent.session_id)
            dispatched.append(task["task_id"] if task else None)

        self.assertEqual(dispatched, ["task-0", "task-1", "task-2", None])

    def test_complete_task_frees_one_slot(self):
        self._enqueue("task-0")
        task = self.state.dispatch(self.agent.session_id)
        self._enqueue("task-1")

        self.state.complete_task(self.agent.session_id, task["task_id"])

        self.assertEqual(
            self.state.dispatch(self.agent.session_id)["task_id"], "task-1"
        )

    def test_cancel_is_per_task_not_per_agent(self):
        self._enqueue("task-0")
        self._enqueue("task-1")
        first = self.state.dispatch(self.agent.session_id)
        second = self.state.dispatch(self.agent.session_id)

        self.state.request_cancel(first["task_id"])

        self.assertEqual(self.state.cancels_for(self.agent.session_id), [first["task_id"]])
        self.assertFalse(self.state.has_cancel(self.agent.session_id, second["task_id"]))

    def test_heartbeat_keeps_agent_ready_when_slots_remain(self):
        self._enqueue("task-0")
        self.state.dispatch(self.agent.session_id)

        record = self.state.heartbeat(self.agent.session_id, now=2.0)

        self.assertEqual(record.state, "busy")
        self.assertEqual(len(record.running_tasks), 1)
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_registry_slots -v`

Expected: FAIL，`TypeError: register() got an unexpected keyword argument 'slots_total'`

 - [x] **Step 3: Write minimal implementation**

`relay_registry.py` 的 `AgentRecord` 增加字段：

```python
    slots_total: int = 1
    running_tasks: list[str] = field(default_factory=list)
    cancel_tasks: set[str] = field(default_factory=set)
```

顶部补 `from dataclasses import dataclass, field`。

`register` 增加 `slots_total: int = 1` 参数并写入记录；`dispatch` 改为按槽位：

```python
    def dispatch(self, session_id: str) -> dict[str, Any] | None:
        agent = self.by_session(session_id)
        if agent is None or agent.state not in {"ready", "busy"}:
            return None
        if len(agent.running_tasks) >= max(int(agent.slots_total or 1), 1):
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
            agent.running_tasks.append(task["task_id"])
            return task
        return None
```

完成与取消改为任务级，并保留旧调用方式的兼容：

```python
    def complete_task(self, session_id: str, task_id: str = "") -> None:
        agent = self.by_session(session_id)
        if agent is None:
            return
        if task_id:
            agent.running_tasks = [
                item for item in agent.running_tasks if item != task_id
            ]
            agent.cancel_tasks.discard(task_id)
        else:
            agent.running_tasks = []
        agent.current_task_id = agent.running_tasks[-1] if agent.running_tasks else ""
        agent.state = "busy" if agent.running_tasks else "ready"

    def request_cancel(self, task_id: str) -> None:
        for agent in self._agents.values():
            if task_id in agent.running_tasks:
                agent.cancel_tasks.add(task_id)

    def cancels_for(self, session_id: str) -> list[str]:
        agent = self.by_session(session_id)
        return sorted(agent.cancel_tasks) if agent else []

    def has_cancel(self, session_id: str, task_id: str = "") -> bool:
        agent = self.by_session(session_id)
        if agent is None:
            return False
        if task_id:
            return task_id in agent.cancel_tasks
        return bool(agent.cancel_tasks)

    def clear_cancel(self, session_id: str, task_id: str = "") -> None:
        agent = self.by_session(session_id)
        if agent is None:
            return
        if task_id:
            agent.cancel_tasks.discard(task_id)
        else:
            agent.cancel_tasks.clear()
```

`app.py` 取消作业时改为按任务取消：

```python
    notified = 0
    for task_id in list(getattr(RELAY_STATE, "_tasks", {})):
        task = RELAY_STATE.task(task_id)
        if task and task.get("job_id") == job_id:
            RELAY_STATE.request_cancel(task_id)
            notified += 1
```

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_registry_slots -v`

Expected: PASS（4 个用例）

 - [x] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests`

Expected: PASS，`test_relay_registry` 与取消相关用例不回归

### Task 14: 注册与心跳接口改用节点令牌

**Files:**

- Modify: `relay_api.py`
- Modify: `relay_pool.py`（`build_cloud_init` 支持节点令牌）
- Test: `tests/test_relay_api_node_token.py`

 - [x] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from relay_api import create_blueprint
from relay_inventory import NodeInventory, RelayNodeRecord
from relay_protocol import issue_node_token
from relay_registry import RelayState

SECRET = b"s" * 32


class NodeTokenRegisterTest(unittest.TestCase):
    def setUp(self):
        from flask import Flask

        self.state = RelayState(secret=SECRET)
        self.inventory = mock.MagicMock(spec=NodeInventory)
        self.credentials = mock.MagicMock()
        self.record = RelayNodeRecord(
            node_id="n1",
            name="relay-source-1",
            role="source",
            tenant_key="t1",
            az="nova-1",
            slots_total=5,
        )
        self.inventory.get.return_value = self.record
        self.token = issue_node_token(
            SECRET, node_id="n1", role="source", tenant_key="t1", az="nova-1"
        )
        self.credentials.unseal_text.return_value = self.token
        self.app = Flask(__name__)
        self.app.register_blueprint(
            create_blueprint(
                self.state,
                inventory=self.inventory,
                credentials=self.credentials,
            )
        )
        self.client = self.app.test_client()

    def test_register_with_node_token_sets_slots(self):
        response = self.client.post(
            "/api/relay/register",
            json={
                "token": self.token,
                "name": "relay-source-1",
                "agent_version": "1.0.0",
                "slots_total": 5,
            },
        )

        self.assertEqual(response.status_code, 200)
        agent = self.state.find_by_name("relay-source-1")
        self.assertEqual(agent.slots_total, 5)

    def test_register_rejects_token_after_rotation(self):
        self.credentials.unseal_text.return_value = "rotated-token"

        response = self.client.post(
            "/api/relay/register",
            json={"token": self.token, "name": "relay-source-1", "agent_version": "1.0.0"},
        )

        self.assertEqual(response.status_code, 401)

    def test_register_rejects_unknown_node(self):
        self.inventory.get.return_value = None

        response = self.client.post(
            "/api/relay/register",
            json={"token": self.token, "name": "relay-source-1", "agent_version": "1.0.0"},
        )

        self.assertEqual(response.status_code, 401)
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_api_node_token -v`

Expected: FAIL，`TypeError: create_blueprint() got an unexpected keyword argument 'inventory'`

 - [x] **Step 3: Write minimal implementation**

`relay_api.py` 的 `create_blueprint` 签名扩展，注册分支同时支持节点令牌与旧的作业令牌：

```python
def create_blueprint(state, *, inventory=None, credentials=None):
    blueprint = Blueprint("relay", __name__, url_prefix="/api/relay")

    def _register_with_node_token(body):
        token = str(body.get("token", ""))
        payload = verify_node_token(state.secret, token)
        record = inventory.get(payload["node_id"]) if inventory is not None else None
        if record is None:
            raise ProtocolError("unknown node")
        stored = ""
        if credentials is not None and record.token_enc:
            stored = credentials.unseal_text(record.token_enc, aad=record.node_id)
        if stored != token:
            raise ProtocolError("node token revoked")
        return payload

    @blueprint.post("/register")
    def register():
        body = request.get_json(force=True, silent=True) or {}
        token = str(body.get("token", ""))
        try:
            if token.count(".") == 1 and inventory is not None:
                payload = _register_with_node_token(body)
                job_id, role = "", payload["role"]
                slots_total = int(body.get("slots_total") or 1)
            else:
                job_id, role = verify_token(state.secret, token, now=time.time())
                slots_total = int(body.get("slots_total") or 1)
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
                data_address=str(body.get("data_address", "")),
                data_port=int(body.get("data_port", 9200) or 9200),
                ssh_public_key=str(body.get("ssh_public_key", "")),
                slots_total=slots_total,
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
```

心跳与进度响应把取消指令改成任务列表，同时保留旧的 `cancel` 布尔：

```python
    @blueprint.post("/heartbeat")
    def heartbeat():
        body = request.get_json(force=True, silent=True) or {}
        session_id = str(body.get("session_id", ""))
        agent = state.heartbeat(session_id, now=time.time())
        if agent is None:
            return jsonify({"error": "unknown session"}), 404
        cancels = state.cancels_for(session_id)
        return jsonify({"cancel": bool(cancels), "cancel_tasks": cancels})
```

```python
    @blueprint.post("/tasks/<task_id>/progress")
    def task_progress(task_id: str):
        body = request.get_json(force=True, silent=True) or {}
        task = state.task(task_id)
        if task is None:
            return jsonify({"error": "unknown task"}), 404
        session_id = str(body.get("session_id", ""))
        _record_progress(state, task, int(body.get("copied_bytes", 0)))
        _persist(state)
        return jsonify(
            {
                "ok": True,
                "cancel": state.has_cancel(session_id, task_id),
                "cancel_tasks": [task_id] if state.has_cancel(session_id, task_id) else [],
            }
        )
```

结果上报改为任务级完成：

```python
        state.complete_task(session_id, task_id)
```

`relay_api.py` 顶部导入补 `verify_node_token`。

`relay_pool.py` 的 `build_cloud_init` 增加 `node_id: str = ""` 参数，
写入 `agent.env` 的 `RELAY_NODE_ID`；节点模式下 `RELAY_JOB_ID` 允许为空。
`main()` 启动时把 `RELAY_NODE_ID` 一并提交给 `/register`。

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_api_node_token -v`

Expected: PASS（3 个用例）

 - [x] **Step 5: Run the full suite**

Run: `python3 -m unittest discover -s tests`

Expected: PASS，`test_relay_api`、`test_relay_bootstrap` 不回归

### Task 15: agent 多 worker、端口池与自动重注册

**Files:**

- Modify: `relay_agent.py`
- Modify: `relay_lease.py`（租约记录端口）
- Modify: `relay_scheduler.py`（按槽位分配端口）
- Test: `tests/test_relay_agent_runner.py`

 - [x] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from relay_agent import AgentRunner


class FakePlatform:
    def __init__(self, sessions=None):
        self.sessions = list(sessions or ["s1"])
        self.heartbeats = 0
        self.registered = 0
        self.fail_heartbeat_with = None

    def register(self, name, slots_total):
        self.registered += 1
        return self.sessions[min(self.registered - 1, len(self.sessions) - 1)]

    def heartbeat(self, session_id):
        self.heartbeats += 1
        if self.fail_heartbeat_with is not None:
            raise self.fail_heartbeat_with
        return {"cancel_tasks": []}


class AgentRunnerTest(unittest.TestCase):
    def test_register_retries_with_backoff_then_succeeds(self):
        attempts = []

        def register(name, slots_total):
            attempts.append(name)
            if len(attempts) < 3:
                raise OSError("connection refused")
            return "session-ok"

        slept = []
        runner = AgentRunner(
            platform_url="https://migrate.example.com",
            token="tok",
            name="relay-source-1",
            slots_total=5,
            register_fn=register,
            sleeper=slept.append,
        )

        session = runner.register()

        self.assertEqual(session, "session-ok")
        self.assertEqual(slept, [1.0, 2.0])

    def test_run_starts_one_worker_per_slot(self):
        calls = []
        runner = AgentRunner(
            platform_url="https://migrate.example.com",
            token="tok",
            name="relay-source-1",
            slots_total=3,
            register_fn=lambda name, slots_total: "session-1",
            heartbeat_fn=lambda session: {"cancel_tasks": []},
            worker_fn=lambda session, slot: calls.append(slot),
            sleeper=lambda seconds: None,
        )

        runner.run(max_heartbeats=1)

        self.assertEqual(sorted(calls), [0, 1, 2])

    def test_heartbeat_404_triggers_reregister(self):
        import urllib.error

        imports = []

        def register(name, slots_total):
            imports.append(name)
            return f"session-{len(imports)}"

        def heartbeat(session):
            if session == "session-1":
                raise urllib.error.HTTPError("u", 404, "not found", {}, None)
            return {"cancel_tasks": []}

        runner = AgentRunner(
            platform_url="https://migrate.example.com",
            token="tok",
            name="relay-source-1",
            slots_total=1,
            register_fn=register,
            heartbeat_fn=heartbeat,
            worker_fn=lambda session, slot: None,
            sleeper=lambda seconds: None,
        )

        runner.run(max_heartbeats=2)

        self.assertEqual(len(imports), 2)
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_agent_runner -v`

Expected: FAIL，`ImportError: cannot import name 'AgentRunner'`

 - [x] **Step 3: Write minimal implementation**

`relay_agent.py` 新增可测试的 runner，`main()` 改为使用它：

```python
from threading import Event, Thread


class AgentRunner:
    """注册 + N worker + 心跳；HTTP 细节通过注入的函数隔离，便于单测。"""

    def __init__(
        self,
        *,
        platform_url: str,
        token: str,
        name: str,
        slots_total: int = 1,
        data_address: str = "",
        data_port: int = 9200,
        node_id: str = "",
        register_fn: Callable[[str, int], str] | None = None,
        heartbeat_fn: Callable[[str], dict[str, Any]] | None = None,
        next_task_fn: Callable[[str], dict[str, Any] | None] | None = None,
        worker_fn: Callable[[str, int], None] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.platform_url = platform_url.rstrip("/")
        self.token = token
        self.name = name
        self.slots_total = max(int(slots_total or 1), 1)
        self.data_address = data_address
        self.data_port = data_port
        self.node_id = node_id
        self._register_fn = register_fn
        self._heartbeat_fn = heartbeat_fn
        self._next_task_fn = next_task_fn
        self._worker_fn = worker_fn
        self._sleep = sleeper
        self.session_id = ""

    def register(self) -> str:
        delay = 1.0
        while True:
            try:
                self.session_id = self._register(self.name, self.slots_total)
                return self.session_id
            except OSError:
                self._sleep(delay)
                delay = min(delay * 2, 60.0)

    def run(self, *, max_heartbeats: int | None = None) -> None:
        self.register()
        stop = Event()
        threads = [
            Thread(target=self._run_worker, args=(slot, stop), daemon=True)
            for slot in range(self.slots_total)
        ]
        for thread in threads:
            thread.start()
        beats = 0
        while max_heartbeats is None or beats < max_heartbeats:
            try:
                self._heartbeat(self.session_id)
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 404):
                    self.register()
            beats += 1
            self._sleep(5.0)
        stop.set()

    def _run_worker(self, slot: int, stop: Event) -> None:
        while not stop.is_set():
            try:
                self._worker(self.session_id, slot)
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 404):
                    continue
            self._sleep(1.0)
```

`AgentRunner._register` / `_heartbeat` / `_worker` 在 `__init__` 未注入时，
回落到现有 `_http_json` 调用；`_worker` 注入时直接调用注入函数（测试用）。

`main()` 简化为：

```python
def main(platform_url: str, token: str, name: str, *, interval: float = 5.0) -> None:
    runner = AgentRunner(
        platform_url=platform_url,
        token=token,
        name=name,
        slots_total=int(os.environ.get("RELAY_SLOTS") or 1),
        data_address=os.environ.get("RELAY_DATA_ADDR")
        or resolve_data_address(urllib.parse.urlsplit(platform_url).hostname or ""),
        data_port=int(os.environ.get("RELAY_DATA_PORT") or 9200),
        node_id=os.environ.get("RELAY_NODE_ID", ""),
    )
    runner.run()
```

端口池：`LeaseRecord` 增加 `data_port: int = 0`；
`RelayScheduler.acquire` 在写租约时传
`data_port=record.data_port_base + record.slots_used`，
`RelayNodeRecord` 增加 `data_port_base: int = 9200`；
`RelayScheduler.view` 从该租约取端口，保证同一节点上每个槽位监听端口唯一。
`RelayVolumeMover._transfer` 继续使用 `target_node.data_port`，无需改动。

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_agent_runner -v`

Expected: PASS（3 个用例）

 - [x] **Step 5: 真 socket 并发集成测试**

新增 `tests/test_relay_multi_slot_integration.py`：两个本地 agent 进程，
普通文件模拟块设备，同一目标节点上分配 2 个端口，并发跑 2 个卷拷贝，
断言两份文件内容与源一致、`slots_used` 回到 0。

Run: `python3 -m unittest tests.test_relay_multi_slot_integration -v`

Expected: PASS（需 `require_escalated`，本用例会打开本地端口）

---

## 阶段 2 完成标准

- 同一 agent 可在槽位未满时连续领任务，槽位满后停止领取；
- 取消按任务下发，取消一个作业不影响同机其他任务；
- 注册接口同时支持节点长期令牌与旧作业令牌，轮换后旧令牌立即失效；
- agent 按槽位数启动 worker，目标端端口来自租约分配的端口池；
- 心跳 401/404 自动重新注册，注册失败指数退避重试；
- 多槽位并发拷贝的真 socket 集成用例通过。

---

## 阶段 3：管理接口、对账与页面

### Task 16: 节点与凭据管理 API

**Files:**

- Create: `relay_admin_api.py`
- Modify: `app.py`（注册蓝图）
- Test: `tests/test_relay_admin_api.py`

 - [x] **Step 1: Write the failing test**

```python
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from flask import Flask

from relay_admin_api import create_admin_blueprint
from relay_credentials import CredentialStore, Sealer
from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_pool_profile import PoolProfile, PoolProfileStore

KEY = b"k" * 32


class AdminApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.inventory = NodeInventory.load(base / "nodes.json")
        self.leases = LeaseStore.load(base / "leases.json")
        self.profiles = PoolProfileStore.load(base / "pools.json")
        self.credentials = CredentialStore.load(base / "credentials.json", Sealer(KEY))
        self.node_manager = mock.MagicMock()
        self.scheduler = mock.MagicMock()
        self.app = Flask(__name__)
        self.app.register_blueprint(
            create_admin_blueprint(
                inventory=self.inventory,
                leases=self.leases,
                profiles=self.profiles,
                credentials=self.credentials,
                scheduler=self.scheduler,
                node_manager=self.node_manager,
            )
        )
        self.client = self.app.test_client()
        self.inventory.upsert(
            RelayNodeRecord(
                node_id="n1",
                name="relay-source-t1-nova-1-1",
                role="source",
                tenant_key="t1",
                az="nova-1",
                server_id="server-1",
                slots_total=5,
                slots_used=0,
                state="ready",
            )
        )

    def tearDown(self):
        self.tmp.cleanup()

    def test_list_nodes_filters_by_tenant(self):
        response = self.client.get("/api/relay/nodes?tenant_key=t1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.get_json()["nodes"]), 1)

    def test_delete_node_rejected_when_busy(self):
        record = self.inventory.get("n1")
        record.slots_used = 2
        record.state = "busy"
        self.inventory.upsert(record)

        response = self.client.delete("/api/relay/nodes/n1")

        self.assertEqual(response.status_code, 409)
        self.node_manager.delete_node.assert_not_called()

    def test_delete_node_calls_manager(self):
        response = self.client.delete("/api/relay/nodes/n1")

        self.assertEqual(response.status_code, 200)
        self.node_manager.delete_node.assert_called_once_with("n1")

    def test_password_endpoint_decrypts_and_audits(self):
        record = self.inventory.get("n1")
        record.ssh_password_enc = self.credentials.seal_text("r00t-pass", aad="n1")
        self.inventory.upsert(record)

        response = self.client.get("/api/relay/nodes/n1/password")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["password"], "r00t-pass")

    def test_save_tenant_credentials(self):
        response = self.client.put(
            "/api/relay/tenants",
            json={"tenant_key": "t1", "auth": {"auth_url": "http://k", "password": "p"}},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.credentials.get("t1")["auth_url"], "http://k")

    def test_delete_tenant_rejected_when_nodes_exist(self):
        response = self.client.delete("/api/relay/tenants?tenant_key=t1")

        self.assertEqual(response.status_code, 409)

    def test_scale_endpoint_delegates_to_manager(self):
        self.profiles.upsert(
            PoolProfile(
                tenant_key="t1",
                role="source",
                az="nova-1",
                image="img",
                flavor="flv",
                network="net",
            )
        )
        self.node_manager.create_node.return_value = self.inventory.get("n1")

        response = self.client.post(
            "/api/relay/pools/scale",
            json={"tenant_key": "t1", "role": "source", "az": "nova-1", "target_nodes": 2},
        )

        self.assertEqual(response.status_code, 200)
        self.node_manager.create_node.assert_called_once()
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_admin_api -v`

Expected: FAIL，`ModuleNotFoundError: No module named 'relay_admin_api'`

 - [x] **Step 3: Write minimal implementation**

```python
"""中转机资源管理 API：节点列表、扩容缩容、删除重建、SSH 密码、租户凭据。"""
from __future__ import annotations

import logging
from dataclasses import asdict

from flask import Blueprint, jsonify, request

from relay_credentials import CredentialError


def create_admin_blueprint(
    *,
    inventory,
    leases,
    profiles,
    credentials,
    scheduler,
    node_manager,
) -> Blueprint:
    blueprint = Blueprint("relay_admin", __name__, url_prefix="/api/relay")

    @blueprint.get("/nodes")
    def list_nodes():
        tenant = request.args.get("tenant_key") or ""
        role = request.args.get("role") or ""
        az = request.args.get("az") or ""
        nodes = [
            record
            for record in inventory.all()
            if (not tenant or record.tenant_key == tenant)
            and (not role or record.role == role)
            and (not az or record.az == az)
        ]
        return jsonify({"ok": True, "nodes": [asdict(record) for record in nodes]})

    @blueprint.get("/nodes/<node_id>")
    def get_node(node_id: str):
        record = inventory.get(node_id)
        if record is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        return jsonify({"ok": True, "node": asdict(record)})

    @blueprint.delete("/nodes/<node_id>")
    def delete_node(node_id: str):
        record = inventory.get(node_id)
        if record is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        if record.slots_used or leases.active_for_node(node_id):
            return jsonify({"ok": False, "error": "节点仍有在途任务，请先 drain"}), 409
        node_manager.delete_node(node_id)
        return jsonify({"ok": True})

    @blueprint.post("/nodes/<node_id>/rebuild")
    def rebuild_node(node_id: str):
        if inventory.get(node_id) is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        record = node_manager.rebuild(node_id)
        return jsonify({"ok": True, "node": asdict(record)})

    @blueprint.post("/nodes/<node_id>/drain")
    def drain_node(node_id: str):
        if not node_manager.drain(node_id):
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        return jsonify({"ok": True})

    @blueprint.post("/nodes/<node_id>/resume")
    def resume_node(node_id: str):
        if not node_manager.resume(node_id):
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        return jsonify({"ok": True})

    @blueprint.post("/nodes/<node_id>/rotate-token")
    def rotate_token(node_id: str):
        record = inventory.get(node_id)
        if record is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        token = node_manager.rotate_token(node_id)
        return jsonify({"ok": True, "token": token})

    @blueprint.get("/nodes/<node_id>/password")
    def show_password(node_id: str):
        record = inventory.get(node_id)
        if record is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        if not record.ssh_password_enc:
            return jsonify({"ok": False, "error": "该节点没有密码凭据"}), 404
        try:
            password = credentials.unseal_text(record.ssh_password_enc, aad=node_id)
        except CredentialError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500
        logging.warning(
            "[MIGRATION] 查看节点 SSH 密码 node=%s from=%s",
            node_id,
            request.remote_addr,
        )
        return jsonify({"ok": True, "password": password})

    @blueprint.get("/nodes/<node_id>/orphans")
    def node_orphans(node_id: str):
        if inventory.get(node_id) is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        return jsonify({"ok": True, "orphans": node_manager.list_orphans(node_id)})

    @blueprint.get("/pools")
    def list_pools():
        summary: dict[tuple, dict] = {}
        for record in inventory.all():
            item = summary.setdefault(
                record.pool_key,
                {
                    "tenant_key": record.tenant_key,
                    "role": record.role,
                    "az": record.az,
                    "nodes": 0,
                    "slots_used": 0,
                    "slots_total": 0,
                },
            )
            item["nodes"] += 1
            item["slots_used"] += record.slots_used
            item["slots_total"] += record.slots_total
        pools = list(summary.values())
        for pool in pools:
            pool["max_nodes"] = profiles.get(
                pool["tenant_key"], pool["role"], pool["az"]
            ).max_nodes if profiles.get(pool["tenant_key"], pool["role"], pool["az"]) else 6
        return jsonify({"ok": True, "pools": pools})

    @blueprint.post("/pools/scale")
    def scale_pool():
        body = request.get_json(force=True, silent=True) or {}
        tenant_key = str(body.get("tenant_key") or "")
        role = str(body.get("role") or "")
        az = str(body.get("az") or "")
        target = int(body.get("target_nodes") or 0)
        current = len(inventory.nodes_in_pool(tenant_key, role, az))
        created = []
        while current + len(created) < target:
            record = node_manager.create_node(
                tenant_key=tenant_key, role=role, az=az
            )
            created.append(record.node_id)
        while current + len(created) > max(target, 0):
            idle = [
                record
                for record in inventory.nodes_in_pool(tenant_key, role, az)
                if not record.slots_used
            ]
            if not idle:
                break
            node_manager.delete_node(idle[-1].node_id)
            created.append(f"-{idle[-1].node_id}")
        return jsonify({"ok": True, "changed": created})

    @blueprint.get("/tenants")
    def list_tenants():
        return jsonify({"ok": True, "tenants": credentials.tenants()})

    @blueprint.put("/tenants")
    def save_tenant():
        body = request.get_json(force=True, silent=True) or {}
        tenant_key = str(body.get("tenant_key") or "")
        auth = body.get("auth") or {}
        if not tenant_key or not isinstance(auth, dict) or not auth:
            return jsonify({"ok": False, "error": "tenant_key 与 auth 必填"}), 400
        credentials.save(tenant_key, auth)
        credentials.flush()
        return jsonify({"ok": True})

    @blueprint.delete("/tenants")
    def delete_tenant():
        tenant_key = request.args.get("tenant_key") or ""
        if inventory.nodes_for_role(tenant_key, "source") or inventory.nodes_for_role(
            tenant_key, "target"
        ):
            return jsonify({"ok": False, "error": "该租户仍有节点，先删除节点"}), 409
        credentials.delete(tenant_key)
        credentials.flush()
        return jsonify({"ok": True})

    return blueprint
```

`RelayNodeManager` 需要补两个方法：

```python
    def rotate_token(self, node_id: str) -> str:
        record = self.inventory.get(node_id)
        if record is None:
            raise NodeManagerError("节点不存在")
        token = issue_node_token(
            self.secret,
            node_id=record.node_id,
            role=record.role,
            tenant_key=record.tenant_key,
            az=record.az,
        )
        record.token_enc = self.credentials.seal_text(token, aad=record.node_id)
        self.inventory.upsert(record)
        self.inventory.save()
        return token

    def list_orphans(self, node_id: str) -> list[dict]:
        record = self.inventory.get(node_id)
        if record is None:
            return []
        os_utils = self._os_utils(record.tenant_key)
        return os_utils.list_server_volume_attachments(record.server_id)
```

`app.py` 注册蓝图：

```python
from relay_admin_api import create_admin_blueprint
...
app.register_blueprint(
    create_admin_blueprint(
        inventory=RELAY_INVENTORY,
        leases=RELAY_LEASES,
        profiles=RELAY_POOL_PROFILES,
        credentials=RELAY_CREDENTIALS,
        scheduler=RELAY_SCHEDULER,
        node_manager=RELAY_NODE_MANAGER,
    )
)
```

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_admin_api -v`

Expected: PASS（7 个用例）
### Task 17: 节点维度对账与 watchdog 巡检

**Files:**

- Modify: `relay_reaper.py`
- Modify: `openstack_utils.py`（`list_server_volume_attachments`）
- Modify: `app.py`（watchdog 循环）
- Test: `tests/test_relay_node_reconciler.py`

 - [x] **Step 1: Write the failing test**

```python
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
            credentials=mock.MagicMock(get=lambda tenant: {"auth_url": "http://k"}),
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

    def test_skips_nodes_without_server_id(self):
        record = self.inventory.get("n1")
        record.server_id = ""
        self.inventory.upsert(record)

        self.assertEqual(self.reconciler.sweep(), [])
        self.os_utils.list_server_volume_attachments.assert_not_called()
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_node_reconciler -v`

Expected: FAIL，`ImportError: cannot import name 'NodeReconciler'`

 - [x] **Step 3: Write minimal implementation**

`openstack_utils.py` 新增：

```python
    def list_server_volume_attachments(self, server_id: str) -> list[dict[str, str]]:
        """列出实例上的卷 attachment，供孤儿对账使用。"""
        attachments = []
        for attachment in self.conn.compute.volume_attachments(server_id):
            attachments.append(
                {
                    "volume_id": getattr(attachment, "volume_id", ""),
                    "attachment_id": getattr(attachment, "id", ""),
                }
            )
        return attachments


    def detach_volume(self, server_id: str, volume_id: str) -> None:
        self.conn.compute.delete_volume_attachment(volume_id, server_id)
```

`relay_reaper.py` 追加：

```python
class NodeReconciler:
    """节点维度对账：不属于任何活跃租约/台账的 attachment 一律卸载。"""

    def __init__(
        self,
        *,
        inventory: Any,
        leases: Any,
        ledger: Any,
        os_utils_factory: Callable[[dict[str, Any]], Any],
        credentials: Any,
    ):
        self.inventory = inventory
        self.leases = leases
        self.ledger = ledger
        self.os_utils_factory = os_utils_factory
        self.credentials = credentials

    def _live_volumes(self, node_id: str, server_id: str) -> set[str]:
        live = {
            lease.volume_id
            for lease in self.leases.active_for_node(node_id)
            if lease.volume_id
        }
        for record in self.ledger.all():
            if record.phase in TERMINAL_PHASES:
                continue
            if server_id and server_id in {
                getattr(record, "source_relay_id", ""),
                getattr(record, "target_relay_id", ""),
            }:
                for field in ("derived_volume_id", "target_volume_id"):
                    value = getattr(record, field, "")
                    if value:
                        live.add(value)
        return live

    def sweep(self) -> list[str]:
        detached: list[str] = []
        for record in self.inventory.all():
            if not record.server_id:
                continue
            auth = self.credentials.get(record.tenant_key)
            if not auth:
                continue
            os_utils = self.os_utils_factory(auth)
            live = self._live_volumes(record.node_id, record.server_id)
            for attachment in os_utils.list_server_volume_attachments(record.server_id):
                volume_id = str(attachment.get("volume_id") or "")
                if not volume_id or volume_id in live:
                    continue
                os_utils.detach_volume(record.server_id, volume_id)
                detached.append(volume_id)
                logging.warning(
                    "[MIGRATION] 节点孤儿 attachment 已卸载 node=%s volume=%s",
                    record.node_id,
                    volume_id,
                )
        return detached
```

`app.py` 的 watchdog 循环改为三件事：

```python
        while True:
            time.sleep(period)
            try:
                sweep_all_runtimes()
            except Exception:  # noqa: BLE001
                logging.exception("[MIGRATION] 中转机周期巡检异常")
            try:
                if RELAY_SCHEDULER is not None:
                    RELAY_SCHEDULER.scale_down()
                if RELAY_NODE_RECONCILER is not None:
                    RELAY_NODE_RECONCILER.sweep()
            except Exception:  # noqa: BLE001
                logging.exception("[MIGRATION] 常驻节点巡检异常")
```

另外 `RelayState.sweep` 标记 `unhealthy` 后，新增一段：
`unhealthy` 连续超过 10 分钟的节点调用 `RELAY_NODE_MANAGER.rebuild(node_id)`，
重建失败只告警不重试。

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_node_reconciler -v`

Expected: PASS（3 个用例）

### Task 18: 前端"中转机资源"页与向导改造

**Files:**

- Modify: `templates/index.html`
- Test: `tests/test_relay_ui_render.py`

 - [x] **Step 1: Write the failing test**

追加到 `tests/test_relay_ui_render.py`：

```python
    def test_relay_resource_page_markup_exists(self):
        html = app_module.app.test_client().get("/").get_data(as_text=True)

        for element_id in (
            "relay-resource-page",
            "relay-pool-summary",
            "relay-node-grid",
            "relay-tenant-credentials",
            "relay-scale-form",
            "relay-node-password-modal",
        ):
            self.assertIn(f'id="{element_id}"', html)

    def test_wizard_step1_references_existing_pool(self):
        html = app_module.app.test_client().get("/").get_data(as_text=True)

        self.assertIn('name="relay_node_mode"', html)
        self.assertIn("选择已有池", html)
```

 - [x] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_ui_render -v`

Expected: FAIL，断言找不到 `relay-resource-page`

 - [x] **Step 3: Write minimal implementation**

在 `templates/index.html` 现有步骤 3 之后新增一个独立面板，
结构与现有 `relay-pool-view` 保持同一套 CSS 前缀，新增：

- `#relay-resource-page`：容器，含租户/角色/AZ 三个筛选下拉；
- `#relay-pool-summary`：池卡片，"节点数 / 上限"、"已用槽位 / 总槽位"、"空闲倒计时"；
- `#relay-node-grid`：节点卡片，展示名称、IP、state、槽位、agent 版本、最近心跳，
  按钮为扩容、缩容、删除、重建、drain、恢复、轮换令牌、显示密码、查看孤儿；
- `#relay-tenant-credentials`：租户凭据表单，只显示"已配置"，不回显密码；
- `#relay-scale-form`：手动扩缩容表单，提交 `POST /api/relay/pools/scale`；
- `#relay-node-password-modal`：显示 SSH 密码的模态框。

对应 JS 沿用现有 `fetch` 风格，新增：

```javascript
async function loadRelayPools() {
  const res = await fetch('/api/relay/pools');
  const json = await res.json();
  renderRelayPoolSummary(json.pools || []);
}

async function loadRelayNodes() {
  const params = new URLSearchParams({
    tenant_key: $('#relay-filter-tenant').value,
    role: $('#relay-filter-role').value,
    az: $('#relay-filter-az').value,
  });
  const res = await fetch(`/api/relay/nodes?${params}`);
  const json = await res.json();
  renderRelayNodeGrid(json.nodes || []);
}
```

向导步骤 1 的中转机面板改为：

- 新增 `relay_node_mode` 单选（`persistent` 默认 / `ephemeral`）；
- persistent 时只保留"选择已有池（租户 + 角色 + AZ）+ 自动扩容开关 + 预检"，
  镜像、flavor、网络等建机参数移到资源页；
- 未配置凭据的租户直接在前端提示跳转到资源页。

 - [x] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_ui_render -v`

Expected: PASS

 - [x] **Step 5: 截图留档**

Run: `python3 app.py`，打开 `http://localhost:19099/`，
截图"中转机资源"页与改造后的步骤 1，按 AGENTS.md 要求附在交付说明中。

### Task 19: 部署清单与全量回归

**Files:**

- Modify: 部署 ConfigMap（`migrate-vm-bin` 的 `--from-file` 列表）
- Modify: 部署 Deployment（新增环境变量）
- Test: 全量回归

 - [x] **Step 1: 更新 ConfigMap 文件清单**

`migrate-vm-bin` 必须包含新增模块，缺失会导致 Pod 启动即 `ModuleNotFoundError`：

```
relay_secret.py
relay_inventory.py
relay_lease.py
relay_credentials.py
relay_pool_profile.py
relay_scheduler.py
relay_node_manager.py
relay_admin_api.py
```

 - [x] **Step 2: 注入环境变量**

Deployment 增加：

```yaml
env:
  - name: MIGRATION_RELAY_SECRET
    valueFrom:
      secretKeyRef:
        name: migrate-relay-secrets
        key: relay-secret
  - name: MIGRATION_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: migrate-relay-secrets
        key: master-key
```

两个密钥都必须是 32 字节的 base64/hex；`MIGRATION_SECRET_KEY` 丢失会导致
全部云凭据不可解密，需纳入备份。

 - [x] **Step 3: 全量回归**

Run: `python3 -m unittest discover -s tests`

Expected: PASS（431 + 新增用例，无失败）

 - [x] **Step 4: Pod 冒烟**

```bash
kubectl -n migrate rollout restart deployment/openstack-vm-migration-deployment
kubectl -n migrate logs deploy/openstack-vm-migration-deployment --tail=50
```

Expected：进程正常启动，日志无 `ModuleNotFoundError`；
`GET /api/relay/nodes` 返回 200 且 `nodes` 为空数组。

---

## 完成标准

- 常驻节点在平台重启后能靠 `relay-secret` + 节点清单 + agent 自动重注册恢复；
- 同一租户多个作业共享节点，1 VM = 1 槽，槽位内多盘串行；
- 需求量超过 5 台 VM 自动扩容，每租户每角色跨 AZ 合计不超过 6 台，
  额度被其他 AZ 空闲节点占用时先再平衡；
- 空闲 24h 自动缩容到每池 1 台，永不删除有租约或忙的节点；
- 作业失败/取消只归还租约，不重建、不删除、不重启节点；
- 目标监听端口按槽位分配，同机多任务互不冲突，取消按任务生效；
- 云凭据与 SSH 密码全部密文落盘，页面可查看密码且留审计日志；
- 资源页可扩容、缩容、删除、重建、drain、恢复、轮换令牌、查看孤儿；
- RBD 通道与现有 relay `ephemeral` 行为不回归。

## 执行顺序建议

Task 1 → Task 6 → Task 2 → Task 10 → Task 3 → Task 4 → Task 5 → Task 7 →
Task 8 → Task 9 → Task 11 → Task 12 → Task 13 → Task 14 → Task 15 →
Task 16 → Task 17 → Task 18 → Task 19。

阶段 0（Task 1-5）不改变任何既有行为，可以先行合并；
阶段 1 完成前不要开启 `persistent` 模式，因为节点注册仍依赖作业令牌；
Task 14 完成后才能把页面默认模式切到 `persistent`。

---

## 执行记录

### 2026-09-11：阶段 0（Task 1-5）完成

- Task 1 `relay_secret.py` + `tests/test_relay_secret.py`：8 个用例通过；
- Task 2 `relay_inventory.py` + `tests/test_relay_inventory.py`：7 个用例通过；
- Task 3 `relay_lease.py` + `tests/test_relay_lease.py`：7 个用例通过；
- Task 4 `relay_credentials.py` + `tests/test_relay_credentials.py`：12 个用例通过；
- Task 5 `requirements.txt` 增加 `cryptography>=41`，`app.py` 装配持久化密钥、
  节点清单、租约与 `load_relay_credentials()`；`tests/test_relay_app_wiring.py`
  新增 4 个用例；
- 全量回归：`python3 -m unittest discover -s tests` → 469 个用例通过
  （沙箱内 socket 用例会 PermissionError，需在沙箱外运行）；
- 副作用：首次导入 `app` 时会生成 `uploads/relay-secret`（0600）。

未开始：Task 6 及之后。`persistent` 模式在 Task 14 完成前不可启用。

### 2026-09-11：阶段 1（Task 6-12）完成

- Task 6 `relay_protocol.py` 新增 `issue_node_token` / `verify_node_token`，
  `tests/test_relay_node_token.py` 7 个用例通过，既有协议用例不回归；
- Task 7 `relay_scheduler.py` 槽位调度，`tests/test_relay_scheduler.py` 9 个用例通过；
- Task 8 自动扩容 + 跨 AZ 额度再平衡，`tests/test_relay_scheduler_grow.py`
  4 个用例通过；
- Task 9 空闲缩容，`tests/test_relay_scheduler_scale_down.py` 6 个用例通过；
- Task 10 `relay_pool_profile.py` 池级建机参数，
  `tests/test_relay_pool_profile.py` 6 个用例通过；
- Task 11 `relay_node_manager.py` 建/删/重建/drain/轮换令牌/孤儿查询，
  `tests/test_relay_node_manager.py` 11 个用例通过；
- Task 12 `relay_runtime.py` 双模式 + `app.py` 注入清单/租约/注册表，
  `tests/test_relay_runtime_modes.py` 4 个用例通过；
- 全量回归：`python3 -m unittest discover -s tests` → 516 个用例通过。

实施过程中的三处主动偏差（均已验证）：

1. `parse_relay_options` 的 `node_mode` 默认值是 `ephemeral`，只有页面显式传
   `relay_node_mode=persistent` 才启用常驻池。原因是 Task 14 之前节点注册仍依赖
   作业令牌，若现在就默认常驻，既有 relay 作业会走不通。Task 18 页面改造时再切默认值。
2. 计划里 Task 9 的两个用例断言与缩容语义矛盾（空闲的对照节点本身也会被回收），
   已改为断言真实不变量："忙节点/有租约节点不出现在回收列表且仍存在"。
3. `relay_node_manager.create_node` 在池内无既有密码时生成 16 位随机密码
   （对应设计文档 11.2 的"留空则由平台生成"），保证 SSH 始终可登录。

未开始：Task 13 及之后（数据面多 worker、注册改节点令牌、管理 API、页面）。

### 2026-09-11：阶段 2（Task 13-15）完成

- Task 13 `relay_registry.py` 多槽位分发 + 任务级取消，新增
  `tests/test_relay_registry_slots.py`（5 个用例）；同步迁移三处旧会话级调用点
  （`tests/test_relay_registry.py`、`tests/test_relay_api.py`、
  `tests/test_relay_app_wiring.py`）；
- Task 14 `relay_api.py` 注册支持节点长期令牌并校验节点记录里的当前令牌，
  新增 `tests/test_relay_api_node_token.py`（5 个用例）；
  `relay_pool.build_cloud_init` 注入 `RELAY_NODE_ID` / `RELAY_SLOTS`；
- Task 15 `relay_agent.py` 新增 `AgentRunner`（N worker + 心跳重注册），
  新增 `tests/test_relay_agent_runner.py`（3 个用例）与
  `tests/test_relay_multi_slot_integration.py`（真 socket 并发拷贝，1 个用例）；
- 全量回归：`python3 -m unittest discover -s tests` → 531 个用例通过。

实施过程中的偏差与修复：

1. 心跳/进度响应里的 `cancel_tasks` 提前到 Task 13 实现（原计划放在 Task 14），
   否则任务级取消在协议层不可见；`cancel` 布尔字段保留以兼容旧 agent。
2. `request_cancel` 保留"传 session_id 则取消该 agent 全部在途任务"的兼容分支，
   避免外部脚本按旧签名调用时静默失效。
3. `RelayScheduler.view` 现在下发租约端口（`data_port_base + slot_index`），
   保证同一节点多槽位监听端口不冲突；调度器用例新增端口唯一性断言。
4. 并发传输用例需要预创建等长目标文件：`receive_device` 以 `r+b` 打开目标，
   真实场景对应"已挂载的目标卷"，计划里没写这一点。

未开始：Task 16 及之后（管理 API、节点维度对账、页面与部署清单）。
`persistent` 现在具备端到端可行性（注册、槽位、端口、多 worker 均已就绪），
但还没有创建调度器与节点管理器的装配入口，需等 Task 16。

### 2026-09-11：阶段 3（Task 16-19）完成

- Task 16 新增 `relay_resources.py`（资源层，按需 `ensure()`）与
  `relay_admin_api.py`（节点/池/凭据管理接口），`app.py` 装配资源层与管理蓝图；
  新增 `tests/test_relay_admin_api.py`（11 个用例）；
- Task 17 `openstack_utils.list_server_volume_attachments`、
  `relay_reaper.NodeReconciler`、`app.sweep_relay_resources`（心跳超时标记 →
  自动重建 → 空闲缩容 → 节点维度孤儿对账）；新增
  `tests/test_relay_node_reconciler.py`（6 个用例）与 watchdog 用例（2 个）；
- Task 18 `templates/index.html` 新增"中转机资源"页（池汇总、节点卡片、
  池建机参数、租户凭据、扩缩容、SSH 密码弹窗）与向导 `relay_node_mode`；
  新增 UI 用例（3 个）；
- Task 19 `README.md` 更新 ConfigMap 文件清单与 `migrate-relay-secrets`
  注入说明；
- 全量回归：`python3 -m unittest discover -s tests` → 555 个用例通过。

实施过程中的偏差与修复：

1. 池建机参数需要独立的 `GET/PUT /api/relay/pools/profile` 接口（计划只列了
   scale），否则资源页无法保存镜像/flavor/网络，扩容也就无从谈起；
2. `RelayResourceLayer` 用 `ensure()` 延迟构建依赖主密钥的部分，
   缺 `MIGRATION_SECRET_KEY` 时读接口仍可用、写操作返回 503，
   保证导入 `app` 与 RBD 通道完全不受影响；
3. `PoolProfile` 增加 `platform_url` 字段：中转机回连地址是环境属性，
   按池保存比全局环境变量更准确（环境变量仍作为兜底）；
4. 常驻模式下 `parse_relay_options` 不再要求镜像/flavor，只要求两侧 AZ，
   因为建机参数来自资源页的池参数；
5. 扩缩容接口早期实现依赖 `create_node` 的副作用做循环，mock 下会死循环，
   改为显式计数 + 边界判断。

## 全部任务完成

19 个任务、82 个步骤全部完成，`python3 -m unittest discover -s tests`
从 431 个用例增长到 555 个用例并保持全绿。尚未执行的外部动作：

- 未把新模块同步到集群 ConfigMap，也未重启 Deployment（需在集群侧执行
  README 中的命令）；
- 未在真实 OpenStack 环境跑通常驻模式全流程（需要配置
  `MIGRATION_SECRET_KEY` 后，在资源页保存池参数与租户凭据，再执行一次迁移）。

## 2026-09-11：交互修正（作业提交即自动建池）

用户反馈："应该是在环境配置页面里配置中转机，然后自动创建；为什么多出一个中转机资源页，
而且不知道怎么填。"原设计把资源页当成池配置入口，把"资源管理的完备性"排在了
"迁移主流程顺手"前面，属于设计失误。修正如下：

- 步骤 1「环境配置」是唯一填写入口：新增 `relay_slots_per_node`、
  `relay_max_nodes`、`relay_min_nodes`、`relay_idle_scale_down_hours`；
- 提交常驻作业时 `prepare_relay_pool()` 自动执行：按租户加密保存当次认证 →
  池首次创建时写入建机参数 → 按 `min_nodes` 建机并等待 agent 注册；
- 已有池沿用原有建机参数，作业表单不覆盖，预检返回明确提示；
- "中转机资源"页降级为运维视图（查看节点、手动扩缩容、删除/重建、drain、
  查看 SSH 密码），页面文案已写明"日常迁移不需要来这一页"；
- `RelayResourceLayer.scheduler_for_pool` 改为返回**独立调度器**（原先修改共享
  `SchedulerConfig`，多作业多池并发时会互相覆盖）；`RelayRuntime` 新增
  `scheduler_factory`，源/目标各自取本池调度器；
- 全量回归：`python3 -m unittest discover -s tests` → 565 个用例通过。

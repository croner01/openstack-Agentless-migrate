# 跨 OpenStack / 跨 Ceph RBD BFV 迁移改造实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把现有“先迁卷再建 VM”原型改造为“目标 BFV 建机 → 关机 → 逐卷 RBD 替换 → 全部成功后开机”的批量迁移服务，并重构前端。

**Architecture:** 保持单 Flask 进程，新增纯状态/规划模块与 JobManager，将 OpenStack 交互和 Ceph RBD 交互收敛为可注入接口；HTTP 只负责提交和查询状态。

**Tech Stack:** Python 3.10、Flask、openstacksdk、pandas/openpyxl、Ceph `rbd` CLI、Bootstrap。

**说明：** 当前目录不是有效 git 仓库，因此本计划的“commit”步骤替换为“运行测试并记录 checkpoint”。以下任务的代码/测试路径均相对仓库根目录 `/root/migrate/vm_migrate_v2`。

---

## 文件结构

| 文件 | 责任 |
| --- | --- |
| `state_machine.py` | Job/VM/卷状态与转换 |
| `excel_parser.py` | Excel 行解析与校验 |
| `migration_planner.py` | flavor 匹配、卷顺序、网络规划等纯逻辑 |
| `ceph_utils.py` | 单卷 RBD 替换原语 |
| `openstack_utils.py` | OpenStack 查询/资源创建/启停 |
| `migration_manager.py` | 单 VM 迁移编排 |
| `job_manager.py` | Job 注册、状态存储、后台执行 |
| `app.py` | HTTP API 与文件上传 |
| `templates/index.html` | 前端工作台 |
| `tests/*` | `unittest` 测试 |

---

### Task 1: 状态模型

**Files:**
- Create: `state_machine.py`
- Create: `tests/test_state_machine.py`

- [ ] **Step 1: 写失败测试**

```python
import unittest
from state_machine import MigrationJob, VmStatus, VmTask, VolumeStatus, VolumeTask

class VmTaskTest(unittest.TestCase):
    def test_all_volumes_success_allows_start(self):
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(source_volume_id="s1", target_volume_id="t1", status=VolumeStatus.SUCCESS),
            VolumeTask(source_volume_id="s2", target_volume_id="t2", status=VolumeStatus.SUCCESS),
        ]
        self.assertTrue(vm.can_start_target())

    def test_any_volume_failed_blocks_start(self):
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [
            VolumeTask(source_volume_id="s1", target_volume_id="t1", status=VolumeStatus.SUCCESS),
            VolumeTask(source_volume_id="s2", target_volume_id="t2", status=VolumeStatus.FAILED),
        ]
        self.assertFalse(vm.can_start_target())

class MigrationJobTest(unittest.TestCase):
    def test_job_completed_when_all_vms_terminal(self):
        job = MigrationJob(id="j1")
        job.vms = [
            VmTask(name="vm1", target_az="az1", status=VmStatus.SUCCESS),
            VmTask(name="vm2", target_az="az1", status=VmStatus.FAILED),
        ]
        self.assertEqual(job.refresh_status(), "completed")

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行并确认失败**

Run: `python3 -m unittest tests.test_state_machine -v`
Expected: `ModuleNotFoundError: No module named 'state_machine'`

- [ ] **Step 3: 实现最小状态模型**

```python
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone

class JobStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class VmStatus(str, Enum):
    QUEUED = "queued"
    PREFLIGHT_FAILED = "preflight_failed"
    CREATING_TARGET_BFV = "creating_target_bfv"
    STOPPING_TARGET = "stopping_target"
    STOPPING_SOURCE = "stopping_source"
    COLLECTING_VOLUMES = "collecting_volumes"
    COPYING_VOLUMES = "copying_volumes"
    STARTING_TARGET = "starting_target"
    VERIFYING = "verifying"
    SUCCESS = "success"
    FAILED = "failed"
    AWAITING_FLAVOR = "awaiting_flavor"

class VolumeStatus(str, Enum):
    PENDING = "pending"
    COPYING = "copying"
    SUCCESS = "success"
    FAILED = "failed"

@dataclass
class VolumeTask:
    source_volume_id: str
    source_rbd_name: str
    target_volume_id: str | None = None
    target_rbd_name: str | None = None
    size: int = 0
    status: VolumeStatus = VolumeStatus.PENDING
    error: str | None = None

@dataclass
class VmTask:
    name: str
    target_az: str
    target_image: str | None = None
    target_flavor: str | None = None
    status: VmStatus = VmStatus.QUEUED
    phase: str = "queued"
    error: str | None = None
    source_server_id: str | None = None
    target_server_id: str | None = None
    target_network_ports: list = field(default_factory=list)
    created_resources: list = field(default_factory=list)
    volumes: list[VolumeTask] = field(default_factory=list)

    def can_start_target(self) -> bool:
        return bool(self.volumes) and all(
            v.status == VolumeStatus.SUCCESS for v in self.volumes
        )

    def mark_failed(self, message: str) -> None:
        self.status = VmStatus.FAILED
        self.phase = "failed"
        self.error = message

@dataclass
class MigrationJob:
    id: str
    status: JobStatus = JobStatus.RUNNING
    error: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    vms: list[VmTask] = field(default_factory=list)

    def refresh_status(self) -> str:
        terminal = {VmStatus.SUCCESS, VmStatus.FAILED, VmStatus.PREFLIGHT_FAILED}
        if self.vms and all(v.status in terminal for v in self.vms):
            self.status = JobStatus.COMPLETED
        return self.status.value
```

- [ ] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_state_machine -v`
Expected: 3 tests pass。

- [ ] **Step 5: checkpoint**

Run: `python3 -m unittest discover -s tests -v`
Expected: 当前目录所有测试通过。

---

### Task 2: Excel 行解析

**Files:**
- Create: `excel_parser.py`
- Create: `tests/test_excel_parser.py`

- [ ] **Step 1: 写失败测试**

```python
import unittest
from excel_parser import parse_rows

class ExcelParserTest(unittest.TestCase):
    def test_returns_one_row_per_input(self):
        rows = parse_rows([
            {"vm_name": "vm-a", "target_az": "az1"},
            {"vm_name": "vm-b", "target_az": "az2"},
        ])
        self.assertEqual([(r.vm_name, r.target_az) for r in rows],
                         [("vm-a", "az1"), ("vm-b", "az2")])

    def test_blank_row_raises(self):
        with self.assertRaises(ValueError):
            parse_rows([{"vm_name": "", "target_az": "az1"}])

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行并确认失败**

Run: `python3 -m unittest tests.test_excel_parser -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现**

```python
from dataclasses import dataclass

@dataclass
class MigrationRow:
    vm_name: str
    target_az: str
    target_image: str | None = None
    target_flavor: str | None = None

REQUIRED_COLUMNS = {"vm_name", "target_az"}
OPTIONAL_COLUMNS = {"target_image", "target_flavor"}

def parse_rows(records) -> list[MigrationRow]:
    if not records:
        return []
    if not REQUIRED_COLUMNS.issubset(records[0].keys()):
        missing = REQUIRED_COLUMNS - set(records[0].keys())
        raise ValueError(f"Excel 缺少列: {', '.join(sorted(missing))}")
    rows = []
    for index, record in enumerate(records, start=2):
        vm_name = str(record.get("vm_name") or "").strip()
        target_az = str(record.get("target_az") or "").strip()
        if not vm_name or not target_az:
            raise ValueError(f"第 {index} 行 vm_name/target_az 不能为空")
        rows.append(MigrationRow(
            vm_name=vm_name,
            target_az=target_az,
            target_image=(record.get("target_image") or "").strip() or None,
            target_flavor=(record.get("target_flavor") or "").strip() or None,
        ))
    return rows
```

- [ ] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_excel_parser -v`
Expected: 2 tests pass。

---

### Task 3: 纯规划逻辑（flavor/卷顺序/网络 IP）

**Files:**
- Create: `migration_planner.py`
- Create: `tests/test_migration_planner.py`

- [ ] **Step 1: 写失败测试**

```python
import unittest
from migration_planner import match_flavor, order_system_and_data, ip_belongs_to_subnet

class FlavorMatchTest(unittest.TestCase):
    def test_matches_vcpu_ram_disk(self):
        source = {"vcpus": 4, "ram": 8192, "disk": 80, "name": "m1.small"}
        flavors = [{"id": "f1", "name": "m1.medium", "vcpus": 8, "ram": 16384, "disk": 80}]
        self.assertIsNone(match_flavor(source, flavors))

    def test_prefers_same_name(self):
        source = {"vcpus": 4, "ram": 8192, "disk": 80, "name": "m1.x"}
        flavors = [
            {"id": "a", "name": "zzz", "vcpus": 4, "ram": 8192, "disk": 80},
            {"id": "b", "name": "m1.x", "vcpus": 4, "ram": 8192, "disk": 80},
        ]
        self.assertEqual(match_flavor(source, flavors)["id"], "b")

class VolumeOrderTest(unittest.TestCase):
    def test_system_disk_first(self):
        volumes = [
            {"id": "data", "is_bootable": False, "device": "vdb"},
            {"id": "system", "is_bootable": True, "device": "vda"},
        ]
        self.assertEqual(order_system_and_data(volumes)[0]["id"], "system")

class SubnetTest(unittest.TestCase):
    def test_ip_in_subnet(self):
        self.assertTrue(ip_belongs_to_subnet("10.0.0.7", "10.0.0.0/24"))
        self.assertFalse(ip_belongs_to_subnet("10.0.1.7", "10.0.0.0/24"))

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行并确认失败**

Run: `python3 -m unittest tests.test_migration_planner -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现**

```python
import ipaddress
from typing import Any

def match_flavor(source, flavors):
    candidates = [
        f for f in flavors
        if int(f.get("vcpus", 0)) == int(source.get("vcpus", 0))
        and int(f.get("ram", 0)) == int(source.get("ram", 0))
        and int(f.get("disk", 0)) == int(source.get("disk", 0))
    ]
    if not candidates:
        return None
    same_name = [f for f in candidates if f.get("name") == source.get("name")]
    return sorted(same_name or candidates, key=lambda f: f.get("name") or "")[0]

def order_system_and_data(volumes):
    volumes = sorted(
        volumes,
        key=lambda v: (not bool(v.get("is_bootable")), v.get("device") or v.get("id") or ""),
    )
    return volumes

def ip_belongs_to_subnet(ip: str, cidr: str) -> bool:
    return ipaddress.ip_address(ip) in ipaddress.ip_network(cidr, strict=False)
```

- [ ] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_migration_planner -v`
Expected: 3 tests pass。

---

### Task 4: RBD 单卷替换

**Files:**
- Rewrite: `ceph_utils.py`
- Create: `tests/test_ceph_utils.py`

接口：

```text
CephUtils(source_conf, source_pool, target_conf, target_pool, runner=None)
replace_rbd_data(source_rbd_name, target_rbd_name) -> bool
```

`runner` 是可注入的 `subprocess.run` 替身，用于测试命令与失败路径。

- [ ] **Step 1: 写失败测试**

```python
import unittest
from ceph_utils import CephUtils, ValidationError

class FakeRunner:
    def __init__(self, results=None):
        self.results = list(results or [])
        self.calls = []
    def __call__(self, command, **kwargs):
        self.calls.append(command)
        if not self.results:
            return
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result

class ReplaceRbdTest(unittest.TestCase):
    def test_validation_rejects_unsafe_name(self):
        with self.assertRaises(ValidationError):
            CephUtils("/a", "rbd", "/b", "rbd").replace_rbd_data("x; rm -rf /", "y")

if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 运行并确认失败**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现 `ceph_utils.py`**

```python
import logging
import re
import shlex
import subprocess

IDENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

class ValidationError(ValueError):
    pass

class CephUtils:
    def __init__(self, source_conf, source_pool, target_conf, target_pool, runner=None):
        self.source_conf = source_conf
        self.source_pool = source_pool
        self.target_conf = target_conf
        self.target_pool = target_pool
        self._runner = runner or subprocess.run
        self._validate_identifier(source_pool)
        self._validate_identifier(target_pool)

    @staticmethod
    def _validate_identifier(value):
        if not IDENT_RE.match(value):
            raise ValidationError(f"非法 RBD 标识符: {value}")

    def _run(self, command, shell=False):
        self._runner(command, shell=shell, check=True)

    def _info(self, conf, pool, name):
        command = ["rbd", "--conf", conf, "info", "--format", "json", f"{pool}/{name}"]
        # runner 需要支持 stdout=subprocess.PIPE 与文本解析；真实 subprocess.run 通过
        # 包一层返回 size；此处为计划内约定，测试使用 Fake 覆盖 info。
        result = self._runner(command, shell=False, check=True, stdout=subprocess.PIPE,
                              text=True)
        return result.stdout if hasattr(result, "stdout") else {}

    def replace_rbd_data(self, source_rbd_name, target_rbd_name):
        try:
            self._validate_identifier(source_rbd_name)
            self._validate_identifier(target_rbd_name)
            self._run(["rbd", "--conf", self.source_conf, "info",
                       f"{self.source_pool}/{source_rbd_name}"])
            self._run(["rbd", "--conf", self.target_conf, "info",
                       f"{self.target_pool}/{target_rbd_name}"])
            self._run(["rbd", "--conf", self.target_conf, "status",
                       f"{self.target_pool}/{target_rbd_name}"])
            self._run(["rbd", "--conf", self.target_conf, "snap", "ls",
                       f"{self.target_pool}/{target_rbd_name}"])
            self._run(["rbd", "--conf", self.target_conf, "rm",
                       f"{self.target_pool}/{target_rbd_name}"])
            src_spec = f"{self.source_pool}/{source_rbd_name}"
            dst_spec = f"{self.target_pool}/{target_rbd_name}"
            pipeline = "set -o pipefail; " + " ".join([
                "rbd", "--conf", shlex.quote(self.source_conf), "export",
                shlex.quote(src_spec), "-", "|",
                "rbd", "--conf", shlex.quote(self.target_conf), "import",
                "-", shlex.quote(dst_spec),
            ])
            self._run(pipeline, shell=True)
            self._run(["rbd", "--conf", self.target_conf, "info",
                       f"{self.target_pool}/{target_rbd_name}"])
            logging.info("[MIGRATION] RBD %s -> %s 替换完成",
                         src_spec, dst_spec)
            return True
        except subprocess.CalledProcessError as e:
            logging.error("[MIGRATION] RBD 替换失败: %s", e)
            return False
        except ValidationError as e:
            logging.error("[MIGRATION] RBD 参数非法: %s", e)
            return False

    def create_volumes_in_target(self, *args, **kwargs):  # 删除旧方法占位
        raise NotImplementedError("旧建卷逻辑已移除，请使用 MigrationManager")
```

> 注意：真实 runner 中 `_info` 需要解析 JSON 以拿到 size/features；实现阶段补一个 `parse_rbd_info_json` 纯函数并由测试覆盖。

- [ ] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: 1 test pass。

---

### Task 5: OpenStack 封装重构

**Files:**
- Rewrite: `openstack_utils.py`

职责收敛为：

- `OpenStackUtils(auth_args=None, conn=None)`：支持注入 connection；
- 查询：`get_server_by_name`、`get_server_volumes_with_device`、`get_server_addresses`；
- 目标创建：`list_images`、`list_flavors`、`find_subnet`、`create_port_with_fixed_ip`、`create_blank_volume`、`create_bfv_server`、`stop_server`、`start_server`、`wait_server_status`；
- 不再实现 `ensure_network_exists`/`ensure_security_group_exists`/`create_port_with_ip` 的旧错误逻辑；网络/安全组只使用已存在的目标资源。

方法签名如下（实现以 openstacksdk 实际对象模型为准）：

```python
class OpenStackUtils:
    def __init__(self, auth_args=None, conn=None): ...

    def get_server_by_name(self, name): ...
    def get_server_detail(self, server_id): ...
    def get_server_volumes_with_device(self, server_id):
        """返回 [{volume, device, is_bootable}]，device 来自 attachment."""
    def get_server_addresses(self, server_id): ...

    def list_images(self): ...
    def list_flavors(self): ...
    def find_subnet(self, name_or_id): ...
    def create_port_with_fixed_ip(self, network_id, subnet_id, fixed_ip): ...
    def create_blank_volume(self, name, size, volume_type=None, availability_zone=None): ...
    def create_bfv_server(self, name, image_id, flavor_id, port_ids,
                          volume_size, data_volume_ids, availability_zone,
                          admin_password): ...
    def stop_server(self, server_id): ...
    def start_server(self, server_id): ...
    def wait_server_status(self, server_id, target, fail_states, timeout=600): ...
    def get_volume(self, volume_id): ...
```

约定：

- `create_bfv_server` 的 BDM 顺序固定为“系统盘 boot_index=0 + 数据卷依次 -1”；
- 等待源/目标关机统一走 `wait_server_status(..., "SHUTOFF", {"ERROR", "PAUSED"})`；
- 固定 IP 冲突时抛出带明确信息的异常；
- 密码默认从请求配置读取，不再使用代码常量 `P@ssw0rd`。

---

### Task 6: 单 VM 迁移编排

**Files:**
- Rewrite: `migration_manager.py`

核心方法：

```python
class MigrationManager:
    def __init__(self, source_os, target_os, ceph_utils): ...

    def migrate_vm(self, vm_task: VmTask, network_mappings, options) -> None:
        """按 spec 第 4 节状态机执行单台 VM。"""
```

实现必须遵循的步骤：

1. 源信息采集；flavor 匹配失败则 `vm_task.status = AWAITING_FLAVOR` 后返回；
2. 创建目标端口（失败则该 VM 失败并跳过）；
3. 创建目标数据卷；
4. `create_bfv_server` 创建 BFV VM；
5. 等待目标 ACTIVE 后 stop 并等待 SHUTOFF；
6. stop 源 VM 并等待 SHUTOFF；
7. 用 `get_server_volumes_with_device` 建立卷映射，写入 `vm_task.volumes`；
8. 对每块卷调用 `ceph_utils.replace_rbd_data`，并更新 VolumeTask 状态；
9. `vm_task.can_start_target()` 通过后 start 目标 VM 并等待 ACTIVE；
10. 任何一步异常调用 `vm_task.mark_failed(str(e))`，不再抛到 Job 层。

---

### Task 7: JobManager 与 API

**Files:**
- Create: `job_manager.py`
- Rewrite: `app.py`

`job_manager.py` 提供：

```python
class JobManager:
    def create_job(self, rows, config) -> MigrationJob: ...
    def start(self, job): ...
    def get(self, job_id) -> MigrationJob | None: ...
    def update(self, job_id, fn): ...
```

`app.py` 提供：

- `GET /`：前端页面；
- `POST /api/migrate`：保存上传文件到 `uploads/<job_id>/`，解析 Excel，创建 Job，后台线程执行，立即返回 `{"job_id": ...}`；
- `GET /api/jobs/<job_id>`：返回 Job 的 JSON 状态；
- `GET /api/jobs`：最近任务；
- `GET /healthz`：健康检查；
- 不再返回纯文本给 `response.json()` 调用方；API 一律 JSON。

请求中的固定网络映射以 JSON 字段提交：

```json
[{"source_network": "net-a", "target_network": "net-a", "target_subnet": "subnet-a"}]
```

---

### Task 8: 前端重构

**Files:**
- Rewrite: `templates/index.html`

前端为单页三区：

1. 环境配置区：源/目标 OpenStack、源/目标 Ceph、默认密码、两个并发数；
2. 清单与映射区：上传 Excel -> 解析表格 -> 目标镜像下拉、flavor 状态、网络映射编辑；
3. 监控区：Job 汇总、VM 卡片、卷进度、当前 VM 日志。

要求：

- 页面使用 Bootstrap 并与现有静态资源保持一致，视觉专业、简洁；
- 所有动态文本用 `textContent`，禁止 `innerHTML` 拼接日志；
- 提交成功后轮询 `GET /api/jobs/<job_id>`；
- 单个 VM 失败时保留卡片并展示错误；其余 VM 继续；
- “开始迁移”前必须通过前端校验，未选目标镜像/flavor 的 VM 标红；
- 实施前读取 `frontend-design` skill 并按其流程产出页面。

---

### Task 9: 集成检查与文档

**Files:**
- Modify: `README.md`（如存在则创建）

- [ ] **Step 1: 静态语法检查**

Run: `python3 -m py_compile app.py ceph_utils.py openstack_utils.py migration_manager.py job_manager.py state_machine.py excel_parser.py migration_planner.py`

- [ ] **Step 2: 单元测试**

Run: `python3 -m unittest discover -s tests -v`
Expected: 全部通过。

- [ ] **Step 3: 记录运行前提**

在 README 写明：需要 Python 包 `flask/openstack/pandas/openpyxl`、`rbd` CLI、源/目标 OpenStack 与 Ceph 网络可达，以及各 API 端点说明。

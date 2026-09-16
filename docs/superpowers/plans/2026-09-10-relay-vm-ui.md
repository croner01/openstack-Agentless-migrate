# 中转机全量迁移（3/3）：页面与运行时装配 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把中转机通道接进页面的三步向导：环境配置里配置中转机池并预检，迁移清单里逐台选择数据通道，迁移监控里展示池状态与卷阶段，同时把表单参数组装成运行时对象注入编排层。

**Architecture:** 新增 `relay_runtime.py` 负责「表单参数 → 池/搬运器/对账器」的装配与作业收尾；`app.py` 解析表单、暴露 `/api/relay/catalog` 与 `/api/jobs/<id>/relay`；`templates/index.html` 沿用既有暗色 panel + stepper 设计语言，只新增面板、卡片标识与池监控区，不引入新的前端框架。

**Tech Stack:** Python 3.10、Flask、Jinja2 模板 + 原生 JS（既有的 `fetch`/`FormData` 风格）、标准库 `unittest` + `unittest.mock`。

---

## 说明

- 前置：第 1 份（数据面）与第 2 份（编排）计划已完成。
- 设计文档：`docs/superpowers/specs/2026-09-10-relay-vm-full-copy-migration-design.md` 第 10 节。
- 当前目录不是 git 仓库（`.git` 为空），所有任务跳过 commit 步骤。
- 页面按 AGENTS.md 要求附截图；视觉上跟随既有设计语言，不做整体改版。
- 本计划不改 `templates/index.html` 的既有步骤语义：仍是「环境配置 / 迁移清单 / 迁移监控」三步。

## 文件结构

| 文件 | 职责 |
| --- | --- |
| `relay_runtime.py` | 表单参数解析、池与搬运器装配、作业收尾、池状态快照 |
| `openstack_utils.py` | 新增 `list_networks`（预检与目录接口用） |
| `relay_api.py` | 新增池状态相关的只读查询辅助（不改数据面协议） |
| `app.py` | 解析中转机表单、目录接口、池状态接口、作业收尾钩子 |
| `templates/index.html` | 步骤 1 中转机池面板、步骤 2 通道标识、步骤 3 池监控 |
| `tests/test_relay_runtime.py` | 运行时装配单测 |
| `tests/test_relay_catalog_api.py` | 目录接口单测 |
| `tests/test_relay_ui_render.py` | 模板渲染与表单字段单测 |

---

### Task 1: 运行时装配

**Files:**
- Create: `relay_runtime.py`
- Modify: `openstack_utils.py`（新增 `create_relay_port` / `delete_port`）
- Test: `tests/test_relay_runtime.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from relay_runtime import (
    PoolConfig,
    RelayRuntime,
    get_runtime,
    parse_relay_options,
    register_runtime,
    drop_runtime,
)


class ParseRelayOptionsTest(unittest.TestCase):
    def _options(self, **overrides):
        options = {
            "data_channel": "relay",
            "relay_platform_url": "https://platform.example.com",
            "relay_source_image": "img-src",
            "relay_source_flavor": "flv-src",
            "relay_source_az": "az1",
            "relay_source_ports": ["port-s"],
            "relay_source_size": "2",
            "relay_target_image": "img-tgt",
            "relay_target_flavor": "flv-tgt",
            "relay_target_az": "az2",
            "relay_target_ports": ["port-t"],
            "relay_target_size": "2",
            "rate_limit_mb_s": "0",
        }
        options.update(overrides)
        return options

    def test_returns_none_when_channel_is_rbd(self):
        self.assertIsNone(parse_relay_options(self._options(data_channel="rbd")))

    def test_parses_pool_configs(self):
        config = parse_relay_options(self._options())

        self.assertEqual(config.platform_url, "https://platform.example.com")
        self.assertEqual(config.source.image, "img-src")
        self.assertEqual(config.source.size, 2)
        self.assertEqual(config.source.port_ids, ["port-s"])
        self.assertEqual(config.target.az, "az2")

    def test_rejects_missing_platform_url(self):
        with self.assertRaises(ValueError) as ctx:
            parse_relay_options(self._options(relay_platform_url=""))

        self.assertIn("relay_platform_url", str(ctx.exception))

    def test_rejects_missing_pool_field(self):
        with self.assertRaises(ValueError) as ctx:
            parse_relay_options(self._options(relay_target_image=""))

        self.assertIn("relay_target_image", str(ctx.exception))

    def test_rejects_non_positive_size(self):
        with self.assertRaises(ValueError):
            parse_relay_options(self._options(relay_source_size="0"))

    def test_converts_rate_limit_to_bytes(self):
        config = parse_relay_options(self._options(rate_limit_mb_s="8"))

        self.assertEqual(config.rate_limit_bytes_per_sec, 8 * 1024 * 1024)


class RelayRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.state = mock.MagicMock()
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.ledger = mock.MagicMock()
        self.config = mock.MagicMock()
        self.config.platform_url = "https://platform.example.com"
        self.config.token_ttl = 3600
        self.config.source = PoolConfig(
            size=1, image="img-src", flavor="flv-src", az="az1",
            network="net-src", port_ids=["port-s"],
        )
        self.config.target = PoolConfig(
            size=1, image="img-tgt", flavor="flv-tgt", az="az2",
            network="net-tgt", port_ids=["port-t"],
        )
        self.config.rate_limit_bytes_per_sec = 0.0
        self.config.chunk_size = 4 * 1024 * 1024
        self.config.ready_timeout = 5.0
        self.config.result_timeout = 60.0
        self.runtime = RelayRuntime(
            config=self.config,
            source_os=self.source_os,
            target_os=self.target_os,
            state=self.state,
            ledger=self.ledger,
            job_id="job-1",
        )

    def test_start_provisions_and_waits_for_both_pools(self):
        with mock.patch.object(
            self.runtime.source_pool, "provision"
        ) as src_provision, mock.patch.object(
            self.runtime.source_pool, "wait_ready"
        ) as src_wait, mock.patch.object(
            self.runtime.target_pool, "provision"
        ) as tgt_provision, mock.patch.object(
            self.runtime.target_pool, "wait_ready"
        ) as tgt_wait:
            self.runtime.start()

        src_provision.assert_called_once()
        tgt_provision.assert_called_once()
        src_wait.assert_called_once()
        tgt_wait.assert_called_once()

    def test_pools_use_configured_az_and_ports(self):
        self.assertEqual(self.runtime.source_pool.az, "az1")
        self.assertEqual(self.runtime.source_pool.port_ids, ["port-s"])
        self.assertEqual(self.runtime.target_pool.az, "az2")
        self.assertEqual(self.runtime.target_pool.role, "target")

    def test_token_factory_binds_job_and_role(self):
        token = self.runtime.source_pool.token_factory("job-1", "source")

        self.assertTrue(token.startswith("job-1|source|"))

    def test_mover_factory_shares_pools_and_lifecycle(self):
        mover = self.runtime.mover_factory(mock.MagicMock(), {})

        self.assertIs(mover.source_pool, self.runtime.source_pool)
        self.assertIs(mover.target_pool, self.runtime.target_pool)
        self.assertIs(mover.ledger, self.ledger)
        self.assertEqual(mover.job_id, "job-1")

    def test_finish_reconciles_then_destroys_pools(self):
        with mock.patch.object(self.runtime.reaper, "reconcile_job") as reconcile, \
                mock.patch.object(self.runtime.source_pool, "destroy") as src_destroy, \
                mock.patch.object(self.runtime.target_pool, "destroy") as tgt_destroy:
            self.runtime.finish()

        reconcile.assert_called_once_with("job-1")
        src_destroy.assert_called_once()
        tgt_destroy.assert_called_once()

    def _patch_lifecycle(self):
        return mock.patch.multiple(
            self.runtime.source_pool, provision=mock.DEFAULT, wait_ready=mock.DEFAULT
        ), mock.patch.multiple(
            self.runtime.target_pool, provision=mock.DEFAULT, wait_ready=mock.DEFAULT
        )

    def test_start_creates_one_port_per_node_when_not_configured(self):
        self.config.source.port_ids = []
        self.source_os.create_relay_port.side_effect = [
            mock.Mock(id="port-new-1"),
            mock.Mock(id="port-new-2"),
        ]
        src_patch, tgt_patch = self._patch_lifecycle()
        with src_patch, tgt_patch:
            self.runtime.start()

        self.assertEqual(
            self.runtime.source_pool.port_ids, ["port-new-1", "port-new-2"]
        )
        self.assertEqual(self.source_os.create_relay_port.call_count, 2)

    def test_start_requires_network_or_ports(self):
        self.config.source.port_ids = []
        self.config.source.network = ""

        with self.assertRaises(ValueError):
            self.runtime.start()

    def test_finish_deletes_ports_created_by_runtime(self):
        self.config.source.port_ids = []
        self.source_os.create_relay_port.side_effect = [
            mock.Mock(id="port-new-1"),
            mock.Mock(id="port-new-2"),
        ]
        src_patch, tgt_patch = self._patch_lifecycle()
        with src_patch, tgt_patch:
            self.runtime.start()
        with mock.patch.object(self.runtime.reaper, "reconcile_job"), \
                mock.patch.object(self.runtime.source_pool, "destroy"), \
                mock.patch.object(self.runtime.target_pool, "destroy"):
            self.runtime.finish()

        self.assertEqual(self.source_os.delete_port.call_count, 2)
        self.assertEqual(
            self.source_os.delete_port.call_args_list[0].args, ("port-new-1",)
        )

    def test_snapshot_lists_both_pools(self):
        self.runtime.source_pool.nodes = []
        self.runtime.target_pool.nodes = []

        snapshot = self.runtime.snapshot()

        self.assertEqual(snapshot["source"], [])
        self.assertEqual(snapshot["target"], [])


class RuntimeRegistryTest(unittest.TestCase):
    def test_register_get_and_drop(self):
        runtime = mock.MagicMock()

        register_runtime("job-9", runtime)
        self.assertIs(get_runtime("job-9"), runtime)
        drop_runtime("job-9")
        self.assertIsNone(get_runtime("job-9"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_runtime -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_runtime'`

- [ ] **Step 3: Write minimal implementation**

`openstack_utils.py` 新增端口原语（放在 `create_port_with_fixed_ip` 之后）：

```python
    def create_relay_port(self, network_id: str):
        """为中转机建一个不指定固定 IP 的端口。"""
        return self.conn.network.create_port(network_id=network_id)

    def delete_port(self, port_id: str) -> None:
        self.conn.network.delete_port(port_id)
```

```python
"""中转机通道的运行时装配：表单参数 → 池 / 搬运器 / 对账器。"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from relay_orchestrator import RelayVolumeMover
from relay_pool import RelayPool
from relay_protocol import DEFAULT_CHUNK, issue_token
from relay_reaper import RelayReaper
from relay_volumes import VolumeLifecycle


@dataclass
class PoolConfig:
    size: int
    image: str
    flavor: str
    az: str
    network: str
    port_ids: list[str] = field(default_factory=list)


@dataclass
class RelayChannelConfig:
    platform_url: str
    source: PoolConfig
    target: PoolConfig
    token_ttl: int = 3600
    rate_limit_bytes_per_sec: float = 0.0
    chunk_size: int = DEFAULT_CHUNK
    ready_timeout: float = 180.0
    result_timeout: float = 6 * 3600.0


_REQUIRED_FIELDS = (
    ("relay_platform_url", "中转机回连平台地址"),
    ("relay_source_image", "源端中转机镜像"),
    ("relay_source_flavor", "源端中转机 flavor"),
    ("relay_source_az", "源端中转机可用区"),
    ("relay_target_image", "目标端中转机镜像"),
    ("relay_target_flavor", "目标端中转机 flavor"),
    ("relay_target_az", "目标端中转机可用区"),
)


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value in (None, ""):
        return []
    return [str(value)]


def parse_relay_options(options: dict[str, Any]) -> RelayChannelConfig | None:
    """解析中转机通道配置；未启用该通道时返回 None。"""
    if str(options.get("data_channel") or "rbd").strip().lower() != "relay":
        return None

    missing = [
        label
        for key, label in _REQUIRED_FIELDS
        if not str(options.get(key) or "").strip()
    ]
    if missing:
        raise ValueError("中转机通道缺少必填项: " + "、".join(missing))

    source_size = int(options.get("relay_source_size") or 2)
    target_size = int(options.get("relay_target_size") or 2)
    if source_size < 1 or target_size < 1:
        raise ValueError("中转机池大小必须大于 0")

    rate_limit_mb = float(options.get("rate_limit_mb_s") or 0)
    return RelayChannelConfig(
        platform_url=str(options["relay_platform_url"]).strip().rstrip("/"),
        source=PoolConfig(
            size=source_size,
            image=str(options["relay_source_image"]).strip(),
            flavor=str(options["relay_source_flavor"]).strip(),
            az=str(options["relay_source_az"]).strip(),
            network=str(options.get("relay_source_network") or "").strip(),
            port_ids=_as_list(options.get("relay_source_ports")),
        ),
        target=PoolConfig(
            size=target_size,
            image=str(options["relay_target_image"]).strip(),
            flavor=str(options["relay_target_flavor"]).strip(),
            az=str(options["relay_target_az"]).strip(),
            network=str(options.get("relay_target_network") or "").strip(),
            port_ids=_as_list(options.get("relay_target_ports")),
        ),
        token_ttl=int(options.get("relay_token_ttl") or 3600),
        rate_limit_bytes_per_sec=rate_limit_mb * 1024 * 1024,
    )


class RelayRuntime:
    """一次作业共享一套池与搬运器；作业结束统一收尾。"""

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
    ):
        self.config = config
        self.state = state
        self.ledger = ledger
        self.job_id = job_id
        self._created_ports: dict[str, list[str]] = {"source": [], "target": []}
        self.lifecycle = VolumeLifecycle(source_os, target_os)
        self.source_pool = RelayPool(
            role="source",
            job_id=job_id,
            az=config.source.az,
            size=config.source.size,
            image_id=config.source.image,
            flavor_id=config.source.flavor,
            port_ids=config.source.port_ids,
            admin_password=admin_password,
            platform_url=config.platform_url,
            token_factory=self._token,
            os_utils=source_os,
            state=state,
        )
        self.target_pool = RelayPool(
            role="target",
            job_id=job_id,
            az=config.target.az,
            size=config.target.size,
            image_id=config.target.image,
            flavor_id=config.target.flavor,
            port_ids=config.target.port_ids,
            admin_password=admin_password,
            platform_url=config.platform_url,
            token_factory=self._token,
            os_utils=target_os,
            state=state,
        )
        self.reaper = RelayReaper(
            ledger=ledger,
            lifecycle=self.lifecycle,
            pools={"source": self.source_pool, "target": self.target_pool},
        )

    def _token(self, job_id: str, role: str) -> str:
        return issue_token(
            self.state.secret, job_id, role, now=time.time(), ttl=self.config.token_ttl
        )

    def start(self) -> None:
        """建池并等待 agent 注册；失败时抛错由调用方决定是否降级。"""
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

    @staticmethod
    def _ensure_ports(pool: Any, pool_config: PoolConfig, os_utils: Any) -> list[str]:
        """池未显式给端口时，按配置的网络为每台中转机建一个端口。"""
        if pool.port_ids:
            return []
        if not pool_config.network:
            raise ValueError("中转机池必须指定网络，或显式提供端口")
        created = [
            str(os_utils.create_relay_port(pool_config.network).id)
            for _ in range(pool_config.size)
        ]
        pool.port_ids = created
        return created

    def mover_factory(self, vm: Any, options: dict[str, Any]) -> RelayVolumeMover:
        return RelayVolumeMover(
            source_pool=self.source_pool,
            target_pool=self.target_pool,
            lifecycle=self.lifecycle,
            state=self.state,
            ledger=self.ledger,
            job_id=self.job_id,
            chunk_size=self.config.chunk_size,
            rate_limit_bytes_per_sec=self.config.rate_limit_bytes_per_sec,
            ready_timeout=self.config.ready_timeout,
            result_timeout=self.config.result_timeout,
        )

    def finish(self) -> None:
        """作业收尾：先对账清理中间产物，再销毁中转机。"""
        try:
            self.reaper.reconcile_job(self.job_id)
        except Exception:  # noqa: BLE001 - 收尾失败不能阻止销毁中转机
            logging.exception("[MIGRATION] 作业 %s 对账清理失败", self.job_id)
        self.source_pool.destroy()
        self.target_pool.destroy()
        self._delete_created_ports("source", self.source_pool.os_utils)
        self._delete_created_ports("target", self.target_pool.os_utils)

    def _delete_created_ports(self, role: str, os_utils: Any) -> None:
        for port_id in self._created_ports.pop(role, []):
            try:
                os_utils.delete_port(port_id)
            except Exception:  # noqa: BLE001 - 端口清理失败不影响作业结果
                logging.exception("[MIGRATION] 删除中转机端口失败 port=%s", port_id)

    def snapshot(self) -> dict[str, Any]:
        """供页面轮询的池状态。"""
        return {
            "job_id": self.job_id,
            "source": [asdict(node) for node in self.source_pool.nodes],
            "target": [asdict(node) for node in self.target_pool.nodes],
        }


_RUNTIMES: dict[str, RelayRuntime] = {}


def register_runtime(job_id: str, runtime: RelayRuntime) -> None:
    _RUNTIMES[job_id] = runtime


def get_runtime(job_id: str) -> RelayRuntime | None:
    return _RUNTIMES.get(job_id)


def drop_runtime(job_id: str) -> None:
    _RUNTIMES.pop(job_id, None)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_runtime -v`

Expected: PASS（12 个用例）

---

### Task 2: 中转机目录接口

**Files:**
- Create: `relay_catalog.py`
- Modify: `openstack_utils.py`（新增 `list_networks`）
- Modify: `app.py`（新增 `/api/relay/catalog`）
- Test: `tests/test_relay_catalog_api.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest
from unittest import mock

from openstack_utils import OpenStackUtils
from relay_catalog import build_catalog


class ListNetworksTest(unittest.TestCase):
    def test_list_networks_returns_id_and_name(self):
        conn = mock.MagicMock()
        conn.network.networks.return_value = [
            mock.Mock(id="net-1", name="storage"),
            mock.Mock(id="net-2", name=""),
        ]
        os_utils = OpenStackUtils(conn=conn)

        networks = os_utils.list_networks()

        self.assertEqual(
            networks,
            [{"id": "net-1", "name": "storage"}, {"id": "net-2", "name": "net-2"}],
        )


class BuildCatalogTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.list_images.return_value = [mock.Mock(id="img-s", name="web")]
        self.target_os.list_images.return_value = [mock.Mock(id="img-t", name="web")]
        self.source_os.list_flavors.return_value = [mock.Mock(id="flv-s", name="m1")]
        self.target_os.list_flavors.return_value = [mock.Mock(id="flv-t", name="m1")]
        self.source_os.list_availability_zones.return_value = [
            {"name": "az1", "state": "available"}
        ]
        self.target_os.list_availability_zones.return_value = [
            {"name": "az2", "state": "available"}
        ]
        self.source_os.list_networks.return_value = [{"id": "net-s", "name": "s"}]
        self.target_os.list_networks.return_value = [{"id": "net-t", "name": "t"}]

    def test_catalog_contains_both_sides(self):
        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(
            catalog["source"]["images"], [{"id": "img-s", "name": "web"}]
        )
        self.assertEqual(
            catalog["target"]["flavors"], [{"id": "flv-t", "name": "m1"}]
        )
        self.assertEqual(
            catalog["source"]["networks"], [{"id": "net-s", "name": "s"}]
        )

    def test_catalog_azs_are_plain_names(self):
        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(catalog["source"]["azs"], ["az1"])
        self.assertEqual(catalog["target"]["azs"], ["az2"])

    def test_catalog_tolerates_one_side_failure(self):
        self.target_os.list_images.side_effect = RuntimeError("boom")

        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(catalog["target"]["images"], [])
        self.assertTrue(catalog["target"]["errors"])

    def test_catalog_reports_error_per_section(self):
        self.source_os.list_networks.side_effect = RuntimeError("no network api")

        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(catalog["source"]["networks"], [])
        self.assertIn("networks", catalog["source"]["errors"])
        self.assertEqual(catalog["source"]["images"][0]["id"], "img-s")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_catalog_api -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'relay_catalog'`

- [ ] **Step 3: Write minimal implementation**

`openstack_utils.py` 新增（放在 `list_availability_zones` 之后）：

```python
    def list_networks(self) -> list[dict[str, Any]]:
        """列出网络，供中转机池的网络下拉使用。"""
        networks = []
        for network in self.conn.network.networks():
            network_id = str(getattr(network, "id", "") or "")
            name = str(getattr(network, "name", "") or "")
            networks.append({"id": network_id, "name": name or network_id})
        return networks
```

新建 `relay_catalog.py`：

```python
"""中转机池配置所需的两侧云目录：镜像、flavor、可用区、网络。"""
from __future__ import annotations

import logging
from typing import Any, Callable


def _resources(
    os_utils: Any,
    *,
    az_loader: Callable[[Any], list[str]] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "images": [],
        "flavors": [],
        "azs": [],
        "networks": [],
        "errors": {},
    }

    def load(name: str, loader) -> None:
        try:
            result[name] = loader()
        except Exception as exc:  # noqa: BLE001 - 单段失败不影响其它下拉
            logging.exception("[MIGRATION] 目录加载失败 section=%s", name)
            result["errors"][name] = str(exc)

    load(
        "images",
        lambda: [
            {"id": str(image.id), "name": str(getattr(image, "name", "") or image.id)}
            for image in os_utils.list_images()
        ],
    )
    load(
        "flavors",
        lambda: [
            {
                "id": str(flavor.id),
                "name": str(getattr(flavor, "name", "") or flavor.id),
            }
            for flavor in os_utils.list_flavors()
        ],
    )
    load(
        "azs",
        lambda: [
            str(zone.get("name"))
            for zone in os_utils.list_availability_zones()
            if zone.get("name")
        ],
    )
    load("networks", lambda: list(os_utils.list_networks()))
    return result


def build_catalog(source_os: Any, target_os: Any) -> dict[str, Any]:
    """返回 {'source': {...}, 'target': {...}} 两侧目录。"""
    return {
        "source": _resources(source_os),
        "target": _resources(target_os),
    }
```

`app.py` 新增只读接口（放在 `/api/source-vm-networks` 之后）：

```python
@app.post("/api/relay/catalog")
def api_relay_catalog():
    """拉取两侧云的镜像/flavor/AZ/网络，供中转机池下拉使用。"""
    try:
        source_os = OpenStackUtils(_auth_args("source"))
        target_os = OpenStackUtils(_auth_args("target"))
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 中转机目录鉴权失败")
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, "catalog": build_catalog(source_os, target_os)})
```

并在 `app.py` 顶部补 `from relay_catalog import build_catalog`。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_catalog_api -v`

Expected: PASS（5 个用例）

---

### Task 3: 表单解析与作业接线

**Files:**
- Modify: `relay_runtime.py`（新增 `relay_options_from_form`）
- Modify: `app.py`
- Test: `tests/test_relay_runtime.py`

- [ ] **Step 1: Write the failing test**

在 `tests/test_relay_runtime.py` 末尾追加：

```python
class RelayOptionsFromFormTest(unittest.TestCase):
    class _Form(dict):
        def getlist(self, key):
            value = self.get(key)
            if value is None:
                return []
            return [value] if isinstance(value, str) else list(value)

    def test_extracts_channel_and_pool_fields(self):
        form = self._Form(
            {
                "data_channel": "relay",
                "relay_platform_url": "https://platform.example.com",
                "relay_source_image": "img-src",
                "relay_source_flavor": "flv-src",
                "relay_source_az": "az1",
                "relay_source_network": "net-src",
                "relay_source_ports": ["port-s1", "port-s2"],
                "relay_target_image": "img-tgt",
                "relay_target_flavor": "flv-tgt",
                "relay_target_az": "az2",
                "relay_target_network": "net-tgt",
                "relay_target_ports": ["port-t1"],
            }
        )

        options = relay_options_from_form(form)

        self.assertEqual(options["data_channel"], "relay")
        self.assertEqual(options["relay_source_ports"], ["port-s1", "port-s2"])
        self.assertEqual(options["relay_target_ports"], ["port-t1"])
        config = parse_relay_options(options)
        self.assertEqual(config.target.network, "net-tgt")

    def test_defaults_to_rbd_when_field_absent(self):
        options = relay_options_from_form(self._Form({}))

        self.assertEqual(options["data_channel"], "rbd")
        self.assertIsNone(parse_relay_options(options))
```

顶部导入补 `relay_options_from_form`。

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_runtime -v`

Expected: FAIL with `ImportError: cannot import name 'relay_options_from_form'`

- [ ] **Step 3: Write minimal implementation**

`relay_runtime.py` 追加：

```python
_FORM_SCALAR_FIELDS = (
    "relay_platform_url",
    "relay_source_image",
    "relay_source_flavor",
    "relay_source_az",
    "relay_source_network",
    "relay_source_size",
    "relay_target_image",
    "relay_target_flavor",
    "relay_target_az",
    "relay_target_network",
    "relay_target_size",
    "relay_token_ttl",
)

_FORM_LIST_FIELDS = ("relay_source_ports", "relay_target_ports")


def relay_options_from_form(form: Any) -> dict[str, Any]:
    """把表单字段收敛成 options 片段，便于单测与复用。"""
    options: dict[str, Any] = {
        "data_channel": str(form.get("data_channel") or "rbd").strip().lower()
    }
    for key in _FORM_SCALAR_FIELDS:
        options[key] = str(form.get(key) or "").strip()
    for key in _FORM_LIST_FIELDS:
        getlist = getattr(form, "getlist", None)
        options[key] = list(getlist(key)) if callable(getlist) else []
    return options
```

`app.py` 接线三处：

1. `/api/migrate` 里把表单字段并入 options（放在既有 options 组装之后）：

```python
        options.update(relay_options_from_form(request.form))
        try:
            relay_config = parse_relay_options(options)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
```

2. 作业后台线程里按通道装配运行时（放在 `MigrationManager(...)` 创建之前）：

```python
                relay_runtime = None
                if relay_config is not None:
                    relay_runtime = RelayRuntime(
                        config=relay_config,
                        source_os=source_os,
                        target_os=target_os,
                        state=RELAY_STATE,
                        ledger=RELAY_STATE.ledger,
                        job_id=job_id,
                        admin_password=options.get("admin_password") or "",
                    )
                    relay_runtime.start()
                    register_runtime(job_id, relay_runtime)
                    options["relay_mover_factory"] = relay_runtime.mover_factory
```

3. 收尾：把 `finally` 块改为同时收尾中转机运行时：

```python
            finally:
                if relay_runtime is not None:
                    try:
                        relay_runtime.finish()
                    finally:
                        drop_runtime(job_id)
                job_manager.unregister_worker(job_id)
                job_manager.sweep_best_effort()
```

4. 新增池状态与手动对账接口（放在 `/api/jobs/<job_id>` 之后）：

```python
@app.get("/api/jobs/<job_id>/relay")
def api_job_relay(job_id: str):
    runtime = get_runtime(job_id)
    if runtime is None:
        return jsonify({"ok": True, "relay": None})
    return jsonify({"ok": True, "relay": runtime.snapshot()})


@app.post("/api/jobs/<job_id>/relay/reconcile")
def api_job_relay_reconcile(job_id: str):
    runtime = get_runtime(job_id)
    if runtime is None:
        return jsonify({"ok": False, "error": "该任务没有中转机运行时"}), 404
    cleaned = runtime.reaper.reconcile_job(job_id)
    return jsonify({"ok": True, "cleaned": cleaned})
```

`app.py` 顶部补：

```python
from relay_runtime import (
    RelayRuntime,
    drop_runtime,
    get_runtime,
    parse_relay_options,
    register_runtime,
    relay_options_from_form,
)
```

注意：`RELAY_STATE` 已在第 1 份计划里创建，其 `ledger` 也已在启动时加载。
中转机通道要求 `data_channel=relay` 时源端 Cinder 支持对 in-use 卷打快照，
这不在这里做额外探测，交给 `/api/relay/preflight`（见 Task 4 的预检调用）。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_runtime tests.test_relay_catalog_api -v`

Expected: PASS（15 个用例）

---

### Task 4: 步骤 1 中转机池面板与预检

**Files:**
- Modify: `relay_catalog.py`（新增 `preflight`）
- Modify: `app.py`（新增 `/api/relay/preflight`）
- Modify: `templates/index.html`
- Test: `tests/test_relay_catalog_api.py`

- [ ] **Step 1: Write the failing test**

追加到 `tests/test_relay_catalog_api.py`：

```python
from relay_catalog import preflight
from relay_runtime import PoolConfig, RelayChannelConfig


class PreflightTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.list_images.return_value = [mock.Mock(id="img-s", name="web")]
        self.target_os.list_images.return_value = [mock.Mock(id="img-t", name="web")]
        self.source_os.list_flavors.return_value = [mock.Mock(id="flv-s", name="m1")]
        self.target_os.list_flavors.return_value = [mock.Mock(id="flv-t", name="m1")]
        self.source_os.list_availability_zones.return_value = [{"name": "az1"}]
        self.target_os.list_availability_zones.return_value = [{"name": "az2"}]
        self.source_os.list_networks.return_value = [{"id": "net-s", "name": "s"}]
        self.target_os.list_networks.return_value = [{"id": "net-t", "name": "t"}]
        self.config = RelayChannelConfig(
            platform_url="https://platform.example.com",
            source=PoolConfig(
                size=1, image="img-s", flavor="flv-s", az="az1",
                network="net-s", port_ids=["port-s"],
            ),
            target=PoolConfig(
                size=1, image="img-t", flavor="flv-t", az="az2",
                network="net-t", port_ids=["port-t"],
            ),
        )

    def test_preflight_passes_for_valid_config(self):
        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(result["ok"])
        self.assertEqual(result["errors"], [])

    def test_preflight_reports_missing_image(self):
        self.config.source.image = "img-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("镜像" in item for item in result["errors"]))

    def test_preflight_reports_missing_az(self):
        self.config.target.az = "az-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("可用区" in item for item in result["errors"]))

    def test_preflight_reports_missing_network_only_when_set(self):
        self.config.source.network = "net-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("网络" in item for item in result["errors"]))

    def test_preflight_warns_about_unverifiable_snapshot_support(self):
        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(any("in-use" in item for item in result["warnings"]))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_catalog_api -v`

Expected: FAIL with `ImportError: cannot import name 'preflight'`

- [ ] **Step 3: Write minimal implementation**

`relay_catalog.py` 追加：

```python
def _names(items: list[dict[str, str]]) -> set[str]:
    names: set[str] = set()
    for item in items:
        names.add(str(item.get("id", "")))
        names.add(str(item.get("name", "")))
    return names


def preflight(config: Any, source_os: Any, target_os: Any) -> dict[str, Any]:
    """校验中转机池配置；无法自动校验的能力以 warning 返回。"""
    catalog = build_catalog(source_os, target_os)
    errors: list[str] = []
    for side, pool in (("源端", config.source), ("目标端", config.target)):
        data = catalog["source" if side == "源端" else "target"]
        if pool.image not in _names(data["images"]):
            errors.append(f"{side}中转机镜像不存在: {pool.image}")
        if pool.flavor not in _names(data["flavors"]):
            errors.append(f"{side}中转机 flavor 不存在: {pool.flavor}")
        if pool.az not in set(data["azs"]):
            errors.append(f"{side}中转机可用区不存在: {pool.az}")
        if not pool.network and not pool.port_ids:
            errors.append(f"{side}中转机必须指定网络或端口")
        elif pool.network and pool.network not in _names(data["networks"]):
            errors.append(f"{side}中转机网络不存在: {pool.network}")
    warnings = [
        "无法自动校验源端 Cinder 驱动是否支持对 in-use 卷打快照，"
        "请确认后再执行；不支持时预检通过的配置仍会在拷贝阶段失败。"
    ]
    return {"ok": not errors, "errors": errors, "warnings": warnings}
```

`app.py` 新增（放在 `/api/relay/catalog` 之后）：

```python
@app.post("/api/relay/preflight")
def api_relay_preflight():
    """校验中转机池配置，避免拷到一半才发现镜像/flavor/AZ 不存在。"""
    options = relay_options_from_form(request.form)
    try:
        config = parse_relay_options(options)
    except ValueError as exc:
        return jsonify({"ok": False, "errors": [str(exc)], "warnings": []}), 400
    if config is None:
        return jsonify({"ok": True, "errors": [], "warnings": []})
    try:
        source_os = OpenStackUtils(_auth_args("source"))
        target_os = OpenStackUtils(_auth_args("target"))
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 中转机预检鉴权失败")
        return jsonify({"ok": False, "errors": [str(exc)], "warnings": []}), 400
    return jsonify(preflight(config, source_os, target_os))
```

并在 `app.py` 顶部把 `relay_catalog` 的导入补成
`from relay_catalog import build_catalog, preflight`。

`templates/index.html`：在步骤 1 的 Ceph 凭据面板之后、迁移参数之前插入新面板。
样式沿用既有 `.panel` / `.form-group` / `.form-control` / `.mono` 类：

```html
        <div class="panel" id="relay-pool-panel">
            <div class="d-flex align-items-center justify-content-between">
                <div class="panel-title mb-0">中转机池</div>
                <div class="d-flex align-items-center gap-2">
                    <button type="button" class="btn btn-ghost btn-sm" id="load-relay-catalog">载入两侧目录</button>
                    <button type="button" class="btn btn-ghost btn-sm" id="relay-preflight">预检</button>
                </div>
            </div>
            <div class="form-group mt-3">
                <label class="form-label">数据通道</label>
                <select class="form-control" name="data_channel" id="data-channel">
                    <option value="rbd">RBD 直连（Ceph）</option>
                    <option value="relay">中转机全量拷贝（iSCSI/FC 商业存储）</option>
                </select>
            </div>
            <div id="relay-pool-config" class="hidden">
                <div class="form-group">
                    <label class="form-label">中转机回连平台地址</label>
                    <input class="form-control mono" name="relay_platform_url" placeholder="https://migrate.example.com:19099">
                </div>
                <div class="row">
                    <div class="col-lg-6" id="relay-source-pool"></div>
                    <div class="col-lg-6" id="relay-target-pool"></div>
                </div>
            </div>
            <div id="relay-preflight-result" class="mt-2"></div>
        </div>
```

同文件 `<script>` 内新增：

```javascript
function relayPoolFields(prefix, label) {
    return `
        <div class="d-flex align-items-center mb-3">
            <span class="badge badge-info mr-2">${label}</span>
            <strong>${label}中转机</strong>
        </div>
        <div class="form-group"><label class="form-label">镜像</label>
            <select class="form-control" name="relay_${prefix}_image" data-role="image"></select></div>
        <div class="form-group"><label class="form-label">Flavor</label>
            <select class="form-control" name="relay_${prefix}_flavor" data-role="flavor"></select></div>
        <div class="form-group"><label class="form-label">可用区</label>
            <select class="form-control" name="relay_${prefix}_az" data-role="az"></select></div>
        <div class="form-group"><label class="form-label">网络</label>
            <select class="form-control" name="relay_${prefix}_network" data-role="network"></select></div>
        <div class="form-group"><label class="form-label">池大小</label>
            <input class="form-control mono" type="number" min="1" max="8" name="relay_${prefix}_size" value="2"></div>
    `;
}

function fillRelaySelect(root, role, items, idKey, nameKey) {
    const select = root.querySelector(`[data-role="${role}"]`);
    select.innerHTML = '';
    items.forEach(item => {
        const option = document.createElement('option');
        option.value = item[idKey];
        option.textContent = item[nameKey] || item[idKey];
        select.appendChild(option);
    });
}

$('#data-channel').addEventListener('change', event => {
    $('#relay-pool-config').classList.toggle('hidden', event.target.value !== 'relay');
});

$('#relay-source-pool').innerHTML = relayPoolFields('source', '源');
$('#relay-target-pool').innerHTML = relayPoolFields('target', '目标');

$('#load-relay-catalog').addEventListener('click', async () => {
    const data = new FormData();
    ['source', 'target'].forEach(prefix => {
        Object.entries(readAuth(prefix)).forEach(([key, value]) => data.append(`${prefix}_${key}`, value));
    });
    const res = await fetch('/api/relay/catalog', { method: 'POST', body: data });
    const json = await res.json();
    if (!json.ok) { toast('目录载入失败：' + json.error, 'error'); return; }
    const zones = { source: $('#relay-source-pool'), target: $('#relay-target-pool') };
    ['source', 'target'].forEach(side => {
        const data2 = json.catalog[side];
        fillRelaySelect(zones[side], 'image', data2.images, 'id', 'name');
        fillRelaySelect(zones[side], 'flavor', data2.flavors, 'id', 'name');
        fillRelaySelect(
            zones[side], 'az', data2.azs.map(name => ({ id: name, name })), 'id', 'name'
        );
        fillRelaySelect(zones[side], 'network', data2.networks, 'id', 'name');
    });
    toast('两侧目录已载入', 'success');
});

$('#relay-preflight').addEventListener('click', async () => {
    const data = new FormData();
    ['source', 'target'].forEach(prefix => {
        Object.entries(readAuth(prefix)).forEach(([key, value]) => data.append(`${prefix}_${key}`, value));
    });
    data.append('data_channel', $('#data-channel').value);
    data.append('relay_platform_url', $('[name="relay_platform_url"]').value.trim());
    ['source', 'target'].forEach(prefix => {
        ['image', 'flavor', 'az', 'network'].forEach(field => {
            const el = $(`[name="relay_${prefix}_${field}"]`);
            data.append(`relay_${prefix}_${field}`, el ? el.value : '');
        });
        data.append(`relay_${prefix}_size`, $(`[name="relay_${prefix}_size"]`).value);
    });
    const res = await fetch('/api/relay/preflight', { method: 'POST', body: data });
    const json = await res.json();
    renderRelayPreflight(json);
});

function renderRelayPreflight(json) {
    const box = $('#relay-preflight-result');
    const lines = [];
    (json.errors || []).forEach(item => lines.push(`<div class="text-danger small">✗ ${item}</div>`));
    (json.warnings || []).forEach(item => lines.push(`<div class="text-warning small">! ${item}</div>`));
    if (!lines.length) lines.push('<div class="text-success small">✓ 预检通过</div>');
    box.innerHTML = lines.join('');
}
```

注意：页面上只让用户选网络与池大小，`relay_*_ports` 默认留空；
建池时由 `RelayRuntime._ensure_ports` 按网络为每台中转机自动创建端口，
作业收尾时一并删除，因此用户不需要手工管理端口。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_relay_catalog_api -v`

Expected: PASS（10 个用例）

---

### Task 5: 步骤 2 通道标识与逐台覆盖

**Files:**
- Modify: `templates/index.html`

- [ ] **Step 1: 增加逐台通道选择**

在预览卡片构建处（`card.className = 'preview-card'` 附近）加入通道下拉，
并把它写入 `rowOverrides`：

```javascript
        const channelSelect = document.createElement('select');
        channelSelect.className = 'form-control form-control-sm';
        channelSelect.dataset.role = 'data-channel';
        ['rbd', 'relay'].forEach(value => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = value === 'rbd' ? 'RBD 直连' : '中转机全量';
            channelSelect.appendChild(option);
        });
        channelSelect.value = row.data_channel || $('#data-channel').value || 'rbd';
        channelSelect.addEventListener('change', () => {
            row.data_channel = channelSelect.value;
        });
        card.querySelector('.plan-card-head').appendChild(channelSelect);
```

并在 `collectPreviewValues()` 读取该字段：

```javascript
        data_channel: (card.querySelector('[data-role="data-channel"]') || {}).value || 'rbd',
```

以及启动迁移时写进 `rowOverrides`：

```javascript
        rowOverrides[row.server_id || row.vm_name] = {
            target_az: row.target_az,
            target_image: row.target_image,
            target_flavor: row.target_flavor,
            mode: row.mode || 'full',
            data_channel: row.data_channel || 'rbd',
        };
```

- [ ] **Step 2: 后端读取逐台通道**

`migration_planner.py` 追加（该模块已是纯函数集合，`migration_manager` 也已导入它，
放在这里可以避免 `migration_manager` 依赖整个中转机运行时栈）：

```python
def vm_channel(options: dict[str, Any], vm_name: str) -> str:
    """逐台通道优先，其次取作业级 data_channel。"""
    overrides = options.get("row_overrides") or {}
    row = overrides.get(vm_name) or {}
    return str(row.get("data_channel") or options.get("data_channel") or "rbd")
```

`migration_manager.py` 的分流判断由
`str(options.get("data_channel") or "rbd") == "relay"` 改为
`vm_channel(options, vm.name) == "relay"`。

并把 `migration_manager.py` 顶部 `from migration_planner import (...)` 的导入列表
补上 `vm_channel`。

- [ ] **Step 3: 验证**

在 `tests/test_migration_planner.py` 追加：

```python
class VmChannelTest(unittest.TestCase):
    def test_per_vm_override_wins(self):
        options = {"data_channel": "rbd", "row_overrides": {"vm-1": {"data_channel": "relay"}}}

        self.assertEqual(vm_channel(options, "vm-1"), "relay")

    def test_falls_back_to_job_level(self):
        self.assertEqual(vm_channel({"data_channel": "relay"}, "vm-1"), "relay")

    def test_defaults_to_rbd(self):
        self.assertEqual(vm_channel({}, "vm-1"), "rbd")
```

Run: `python3 -m unittest tests.test_migration_planner tests.test_migration_manager -v`

Expected: PASS

---

### Task 6: 步骤 3 池监控视图

**Files:**
- Modify: `templates/index.html`

- [ ] **Step 1: 增加池面板容器**

在步骤 3 的 `#vm-list` 之前插入：

```html
            <div id="relay-pool-view" class="hidden">
                <div class="plan-section-title">中转机池</div>
                <div class="d-flex align-items-center gap-3 mb-2" id="relay-pool-counters"></div>
                <div class="relay-pool-grid" id="relay-pool-grid"></div>
                <div class="mt-2">
                    <button type="button" class="btn btn-ghost btn-sm" id="relay-reconcile">立即对账清理</button>
                </div>
                <div class="divider"></div>
            </div>
```

配套 CSS（追加到既有 `<style>` 内，沿用 CSS 变量）：

```css
        .relay-pool-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
            gap: 0.75rem;
        }
        .relay-node {
            background: var(--panel-2);
            border: 1px solid rgba(255, 255, 255, 0.06);
            border-radius: 8px;
            padding: 0.75rem 0.9rem;
            font-size: 0.85rem;
        }
        .relay-node .name { font-weight: 600; }
        .relay-node .meta { color: var(--muted); font-size: 0.78rem; }
        .relay-node[data-state="ready"] { border-left: 3px solid #34d399; }
        .relay-node[data-state="busy"] { border-left: 3px solid #60a5fa; }
        .relay-node[data-state="unhealthy"] { border-left: 3px solid #f87171; }
        .relay-node[data-state="provisioning"] { border-left: 3px solid #fbbf24; }
```

- [ ] **Step 2: 轮询渲染池状态**

在既有 `startPolling()` 的轮询回调里追加：

```javascript
async function refreshRelayPool() {
    if (!state.jobId) return;
    const res = await fetch(`/api/jobs/${state.jobId}/relay`);
    const json = await res.json();
    const view = $('#relay-pool-view');
    if (!json.relay) { view.classList.add('hidden'); return; }
    view.classList.remove('hidden');
    const nodes = [...json.relay.source, ...json.relay.target];
    const busy = nodes.filter(node => node.state === 'busy').length;
    const unhealthy = nodes.filter(node => node.state === 'unhealthy').length;
    $('#relay-pool-counters').innerHTML = `
        <span class="badge badge-info">共 ${nodes.length} 台</span>
        <span class="badge badge-info">忙碌 ${busy}</span>
        <span class="badge badge-info">异常 ${unhealthy}</span>
    `;
    $('#relay-pool-grid').innerHTML = nodes.map(node => `
        <div class="relay-node" data-state="${node.state}">
            <div class="name">${node.name}</div>
            <div class="meta">${node.role === 'source' ? '源端' : '目标端'} · ${node.az}</div>
            <div class="meta">${node.data_address || '—'}:${node.data_port}</div>
            <div class="meta">状态 ${node.state}${node.current_task_id ? ' · ' + node.current_task_id : ''}</div>
        </div>
    `).join('');
}

$('#relay-reconcile').addEventListener('click', async () => {
    const res = await fetch(`/api/jobs/${state.jobId}/relay/reconcile`, { method: 'POST' });
    const json = await res.json();
    toast(json.ok ? `已清理 ${json.cleaned.length} 项` : json.error, json.ok ? 'success' : 'error');
});
```

并在 `startPolling()` 里与任务状态一起调用 `refreshRelayPool()`。

- [ ] **Step 3: 验证渲染**

Run: `python3 -m unittest tests.test_relay_ui_render -v`

Expected: PASS（Task 7 建立该测试）

---

### Task 7: 页面渲染验证

**Files:**
- Test: `tests/test_relay_ui_render.py`

- [ ] **Step 1: Write the failing test**

```python
import unittest

import app as app_module


class RelayPageRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_step1_has_data_channel_select(self):
        self.assertIn('id="data-channel"', self._html())

    def test_step1_has_relay_pool_panel(self):
        html = self._html()

        self.assertIn('id="relay-pool-panel"', html)
        self.assertIn('name="relay_platform_url"', html)
        self.assertIn('id="relay-preflight"', html)
        self.assertIn('id="load-relay-catalog"', html)

    def test_step3_has_relay_pool_view(self):
        html = self._html()

        self.assertIn('id="relay-pool-view"', html)
        self.assertIn('id="relay-pool-grid"', html)
        self.assertIn('id="relay-reconcile"', html)

    def test_relay_css_is_present(self):
        self.assertIn(".relay-pool-grid", self._html())

    def test_relay_endpoints_are_registered(self):
        rules = {rule.rule for rule in app_module.app.url_map.iter_rules()}

        self.assertIn("/api/relay/catalog", rules)
        self.assertIn("/api/relay/preflight", rules)
        self.assertIn("/api/jobs/<job_id>/relay", rules)
        self.assertIn("/api/jobs/<job_id>/relay/reconcile", rules)
```

- [ ] **Step 2: Run test**

Run: `python3 -m unittest tests.test_relay_ui_render -v`

Expected: 全部 PASS（若无 Flask 依赖需先 `pip3 install -r requirements.txt`）

- [ ] **Step 3: 截图**

按 AGENTS.md 要求，UI 改动需附截图：

```bash
python app.py &
curl -s http://localhost:19099/ -o /tmp/relay-page.html
```

浏览器打开 `http://localhost:19099/`，对步骤 1（中转机池面板）、
步骤 3（池监控网格）各截一张图附在 PR/交付说明里。

- [ ] **Step 4: 全量回归**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS，无失败用例

---

## 完成标准

- 页面三步向导新增中转机池配置与池监控，且不破坏既有 RBD 路径的任何交互；
- `data_channel=rbd` 时行为与现在完全一致，表单里不出现中转机相关字段也不会报错；
- `data_channel=relay` 时：预检能拦住缺失的镜像/flavor/AZ/网络，
  提交后作业启动即建池，收尾时对账并销毁中转机；
- 逐台 VM 可以覆盖作业级数据通道；
- 步骤 3 能实时展示每台中转机的角色、AZ、数据面地址、状态与当前任务，
  并提供手动对账入口；
- 全量测试套件通过，并附步骤 1、步骤 3 截图。

## 三份计划的关系

| 计划 | 产出 |
| --- | --- |
| 1/3 数据面与最小控制面 | agent、块流协议、台账、relay API（已执行，211 用例通过） |
| 2/3 OpenStack 编排与卷生命周期 | 池、卷生命周期、单卷编排、对账、MigrationManager 分流 |
| 3/3 页面与运行时装配 | 表单解析与装配、目录与预检接口、三步向导改动 |

三份计划的执行顺序为 1 → 2 → 3；第 3 份依赖第 2 份的 `RelayRuntime` 装配对象，
第 2 份依赖第 1 份的 `RelayState` / `Ledger` / agent 协议。

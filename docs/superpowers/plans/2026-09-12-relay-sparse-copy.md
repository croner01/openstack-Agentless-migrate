# 中转机通道空洞跳过（sparse copy）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让中转机通道跳过源端全零块（不占网络、不写目标），在保证数据完整性的前提下把空盘场景的拷贝时间从"整盘时长"降到"真实数据时长"。

**Architecture:** 协议层新增无载荷的 `RLYH` 空洞帧；源端 agent 按块判零后决定发数据帧还是空洞帧；目标端按 `skip`/`zero` 模式落地空洞；平台侧用 agent 版本门控是否启用 sparse，并把 `skipped_bytes` 落台账、通过 `/api/jobs/<id>/relay` 暴露给页面。老 agent 自动回退到现有全量逻辑。

**Tech Stack:** Python 3.10、标准库 socket/struct/fcntl、Flask、`unittest` + `unittest.mock`。

---

## 说明

- 设计文档：`docs/superpowers/specs/2026-09-12-relay-sparse-copy-design.md`。
- 当前目录不是 git 仓库（`.git` 为空且只读），所有任务**跳过 commit 步骤**，改为每个任务完成后跑全量回归。
- 基线：本计划开始时 `python3 -m unittest discover -s tests` 为 **615 个用例全绿**。
- 涉及真实 socket 的用例（Task 2～4）需要 `require_escalated` 运行。
- agent 侧文件（`relay_protocol.py`、`relay_transfer.py`、`relay_agent.py`）通过 `/api/relay/pkg/<name>` 下发给中转机，因此必须保证老 agent 不会被新帧打挂（Task 1 兼容性用例 + Task 8 版本门控）。

## 文件结构

| 文件 | 状态 | 职责 |
| --- | --- | --- |
| `relay_protocol.py` | 改造 | 空洞帧编解码、版本能力判定 |
| `relay_transfer.py` | 改造 | 源端判零跳洞、目标端空洞落地、字节分类统计 |
| `relay_agent.py` | 改造 | 任务字段透传、三类字节上报、版本号 1.1.0 |
| `relay_registry.py` | 改造 | 接受 1.1.0 |
| `relay_pool.py` / `relay_scheduler.py` | 改造 | 节点视图携带 `agent_version` |
| `relay_orchestrator.py` | 改造 | sparse 门控、任务字段、台账 `skipped_bytes` |
| `relay_ledger.py` | 改造 | `VolumeTaskRecord.skipped_bytes` |
| `relay_api.py` | 改造 | 进度/结果接口接收并落 `skipped_bytes` |
| `relay_runtime.py` | 改造 | 空洞模式选项解析、`snapshot().volumes` 增加 `skipped_bytes`/`sent_bytes` |
| `app.py` | 改造 | 平台级 `MIGRATION_RELAY_HOLE_MODE` 强制覆盖 |
| `templates/index.html` | 改造 | 空洞模式下拉、进度行展示跳过量 |

---

## Task 1: 协议层空洞帧

**Files:**
- Modify: `relay_protocol.py`
- Test: `tests/test_relay_protocol.py`

- [x] **Step 1: 写失败测试**

```python
def test_encode_hole_round_trips_through_read_frame(self):
    stream = io.BytesIO(encode_hole(4 * 1024 * 1024, 4096))
    self.assertEqual(read_frame(stream), ("hole", 4 * 1024 * 1024, 4096, b""))


def test_read_frame_parses_data_frame(self):
    stream = io.BytesIO(encode_chunk(1024, b"payload"))
    self.assertEqual(
        read_frame(stream), ("data", 1024, 7, b"payload")
    )


def test_read_frame_reports_eof(self):
    self.assertEqual(read_frame(io.BytesIO(b"")), ("eof", -1, 0, b""))


def test_read_chunk_rejects_hole_frame(self):
    with self.assertRaises(ProtocolError):
        read_chunk(io.BytesIO(encode_hole(0, 4096)))


def test_agent_supports_sparse_compares_numeric_segments(self):
    self.assertFalse(agent_supports_sparse("1.0.0"))
    self.assertTrue(agent_supports_sparse("1.1.0"))
    self.assertTrue(agent_supports_sparse("1.10.0"))
    self.assertFalse(agent_supports_sparse(""))
    self.assertFalse(agent_supports_sparse("bogus"))
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_protocol -v`
Expected: FAIL — `ImportError: cannot import name 'encode_hole'`

- [x] **Step 3: 最小实现**

```python
HOLE_MAGIC = b"RLYH"
SPARSE_MIN_AGENT_VERSION = (1, 1, 0)


def encode_hole(offset: int, length: int) -> bytes:
    """把"源端该区间全零"编码成无载荷帧。"""
    if length <= 0:
        raise ProtocolError("empty hole")
    if length > MAX_CHUNK:
        raise ProtocolError("hole too large")
    return HEADER.pack(HOLE_MAGIC, offset, length, 0)


def read_frame(stream: IO[bytes]) -> tuple[str, int, int, bytes]:
    """读取一帧，返回 (kind, offset, length, payload)；流结束返回 ("eof", -1, 0, b"")。"""
    header = stream.read(HEADER_SIZE)
    if not header:
        return "eof", -1, 0, b""
    if len(header) < HEADER_SIZE:
        raise ProtocolError("truncated header")
    magic, offset, length, checksum = HEADER.unpack(header)
    if magic == HOLE_MAGIC:
        if 0 < length <= MAX_CHUNK:
            return "hole", offset, length, b""
        raise ProtocolError("bad hole length")
    if magic != MAGIC:
        raise ProtocolError("bad magic")
    if length == 0 or length > MAX_CHUNK:
        raise ProtocolError("bad length")
    payload = stream.read(length)
    if len(payload) != length:
        raise ProtocolError("truncated payload")
    if crc32(payload) != checksum:
        raise ProtocolError("crc mismatch")
    return "data", offset, length, payload


def agent_supports_sparse(version: str) -> bool:
    """版本号按数字段比较；解析失败视为不支持，宁可不优化也不冒险。"""
    parts = str(version or "").split(".")
    try:
        numbers = tuple(int(part) for part in parts)
    except ValueError:
        return False
    padded = numbers + (0,) * max(0, 3 - len(numbers))
    return padded[:3] >= SPARSE_MIN_AGENT_VERSION
```

`read_chunk` 改为基于 `read_frame`：`eof` → `(-1, b"")`；`hole` → 抛
`ProtocolError("unexpected hole frame")`；`data` → `(offset, payload)`。

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_protocol`
Expected: PASS

## Task 2: 源端判零跳洞

**Files:**
- Modify: `relay_transfer.py`
- Test: `tests/test_relay_transfer.py`

- [x] **Step 1: 写失败测试**

```python
def test_send_device_skips_zero_chunks(self):
    src.write_bytes(b"\0" * (8 * 1024 * 1024) + b"tail")
    stats = TransferStats()
    sent = send_device(
        str(src), peer_host="127.0.0.1", peer_port=port,
        ticket="t", chunk_size=4 * 1024 * 1024, skip_zero=True, stats=stats,
    )
    self.assertEqual(sent, 8 * 1024 * 1024 + 4)
    self.assertEqual(stats.skipped_bytes, 8 * 1024 * 1024)
    self.assertEqual(stats.data_bytes, 4)


def test_send_device_never_skips_chunk_with_single_nonzero_byte(self):
    payload = bytearray(4 * 1024 * 1024)
    payload[-1] = 1
    src.write_bytes(bytes(payload))
    stats = TransferStats()
    send_device(..., skip_zero=True, stats=stats)
    self.assertEqual(stats.skipped_bytes, 0)
    self.assertEqual(stats.data_bytes, 4 * 1024 * 1024)
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_transfer -v`
Expected: FAIL — `ImportError: cannot import name 'TransferStats'`

- [x] **Step 3: 最小实现**

```python
@dataclass
class TransferStats:
    """按"是否真的传了数据"记账：数据字节与跳过字节。"""

    data_bytes: int = 0
    skipped_bytes: int = 0

    @property
    def processed_bytes(self) -> int:
        return self.data_bytes + self.skipped_bytes


def is_zero_block(payload: bytes, zeros: bytes) -> bool:
    """memcmp 判零；4 MiB 块的开销可以忽略。"""
    if len(payload) == len(zeros):
        return payload == zeros
    return payload == zeros[: len(payload)]
```

`send_device` 增加 `skip_zero: bool = False`、`stats: TransferStats | None = None`：
循环内先 `if skip_zero and is_zero_block(chunk, zeros)` → 发 `encode_hole` 帧、
`stats.skipped_bytes += len(chunk)`；否则走原有数据帧路径并
`stats.data_bytes += len(chunk)`；两种情况都推进游标并回调 `progress_cb(processed)`。
`zeros = b"\0" * chunk_size` 只在 `skip_zero` 为真时构造。

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_transfer`
Expected: PASS

## Task 3: 目标端空洞落地

**Files:**
- Modify: `relay_transfer.py`
- Test: `tests/test_relay_transfer.py`

- [x] **Step 1: 写失败测试**

```python
def test_receive_device_zero_mode_overwrites_existing_data(self):
    target.write_bytes(b"\xff" * 4096)
    ...  # 源端 skip_zero=True 发一个空洞
    self.assertEqual(target.read_bytes(), b"\0" * 4096)


def test_receive_device_skip_mode_leaves_target_untouched(self):
    target.write_bytes(b"\xff" * 4096)
    ...  # hole_mode="skip"
    self.assertEqual(target.read_bytes(), b"\xff" * 4096)


def test_receive_device_rejects_hole_when_mode_off(self):
    with self.assertRaises(TransferError):
        ...  # hole_mode="off" 收到空洞帧
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_transfer -v`
Expected: FAIL — `receive_device() got an unexpected keyword argument 'hole_mode'`

- [x] **Step 3: 最小实现**

```python
BLKZEROOUT = 0x1277


def _apply_hole(handle, offset: int, length: int, mode: str) -> None:
    """把源端全零区间在目标设备上落实。"""
    if mode == "skip":
        return
    if mode != "zero":
        raise TransferError(f"unexpected hole frame (hole_mode={mode!r})")
    if not _zero_out(handle, offset, length):
        _write_zeros(handle, offset, length)


def _zero_out(handle, offset: int, length: int) -> bool:
    """优先 BLKZEROOUT；后端不支持时返回 False 由调用方写零。"""
    if fcntl is None:
        return False
    try:
        fcntl.ioctl(handle.fileno(), BLKZEROOUT, struct.pack("<QQ", offset, length))
        return True
    except OSError:
        return False


def _write_zeros(handle, offset: int, length: int, *, block: int = 1024 * 1024) -> None:
    handle.seek(offset)
    zeros = b"\0" * min(block, length)
    remaining = length
    while remaining > 0:
        piece = zeros[: min(len(zeros), remaining)]
        handle.write(piece)
        remaining -= len(piece)
```

`receive_device` 增加 `hole_mode: str = "skip"`、`stats: TransferStats | None = None`，
接收循环改用 `read_frame`；空洞帧先校验 `chunk_offset == offset + processed`，
数据帧沿用原写入逻辑；两者都推进 `processed` 并回调进度。

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_transfer`
Expected: PASS

## Task 4: 端到端数据一致性

**Files:**
- Test: `tests/test_relay_migration_integration.py`

- [x] **Step 1: 写失败测试（真实 socket）**

```python
def test_sparse_copy_reproduces_source_exactly(self):
    """源端全零夹带数据：目标必须与源逐字节一致。"""
    source = bytearray(12 * 1024 * 1024)
    source[5 * 1024 * 1024:5 * 1024 * 1024 + 4096] = b"x" * 4096
    ...  # send_device(skip_zero=True) + receive_device(hole_mode="skip")
    self.assertEqual(Path(dst).read_bytes(), bytes(source))
```

- [x] **Step 2: 运行测试确认失败**

Run: `timeout 180 python3 -m unittest tests.test_relay_migration_integration -v`
Expected: 新用例 FAIL（参数未接线），既有用例保持 PASS

- [x] **Step 3: 接线**

测试直接组合 `send_device(skip_zero=True, stats=...)` 与
`receive_device(hole_mode="skip", stats=...)`，用两个线程跑源/目标两端。
不新增生产代码。

- [x] **Step 4: 运行测试**

Run: `timeout 180 python3 -m unittest tests.test_relay_migration_integration`
Expected: PASS

## Task 5: agent 侧接线与版本号

**Files:**
- Modify: `relay_agent.py`
- Test: `tests/test_relay_agent.py`

- [x] **Step 1: 写失败测试**

```python
def test_agent_version_supports_sparse(self):
    self.assertEqual(relay_agent.AGENT_VERSION, "1.1.0")


def test_run_task_forces_hole_mode_off_without_sparse(self):
    # 未带 sparse 的任务：receive_device 收到 hole_mode="off"
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_agent -v`
Expected: FAIL（版本号仍为 1.0.0、无 sparse 透传）

- [x] **Step 3: 最小实现**

```python
AGENT_VERSION = "1.1.0"
...
sparse = bool(task.get("sparse"))
hole_mode = str(task.get("hole_mode") or "skip") if sparse else "off"
stats = TransferStats()
```

`report_progress(task_id, processed, skipped_bytes=stats.skipped_bytes)`；
`report_result(..., skipped_bytes=stats.skipped_bytes)`。

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_agent`
Expected: PASS

## Task 6: 平台接口落 skipped_bytes

**Files:**
- Modify: `relay_api.py`、`relay_ledger.py`
- Test: `tests/test_relay_api.py`、`tests/test_relay_ledger.py`

- [x] **Step 1: 写失败测试**

```python
def test_progress_records_skipped_bytes_with_base(self):
    """task.offset=1024, skipped_base=512；上报 copied=2048, skipped=1024。"""
    ...
    self.assertEqual(record.copied_bytes, 3072)
    self.assertEqual(record.skipped_bytes, 1536)
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_api -v`
Expected: FAIL（`skipped_bytes` 属性不存在）

- [x] **Step 3: 最小实现**

```python
@dataclass
class VolumeTaskRecord:
    ...
    skipped_bytes: int = 0
```

```python
def _record_progress(state, task, copied_bytes, skipped_bytes=0):
    ...
    base = int(task.get("skipped_base") or 0)
    record.copied_bytes = int(task.get("offset") or 0) + int(copied_bytes or 0)
    record.skipped_bytes = base + int(skipped_bytes or 0)
```

`/progress` 与 `/result` 都从 body 读取 `skipped_bytes` 并透传。

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_api tests.test_relay_ledger`
Expected: PASS

## Task 7: 节点视图携带 agent 版本

**Files:**
- Modify: `relay_registry.py`、`relay_pool.py`、`relay_scheduler.py`
- Test: `tests/test_relay_registry.py`、`tests/test_relay_pool.py`、`tests/test_relay_scheduler.py`

- [x] **Step 1: 写失败测试**

```python
def test_register_accepts_sparse_capable_agent(self):
    response = self._register(agent_version="1.1.0")
    self.assertEqual(response.status_code, 200)


def test_relay_node_exposes_agent_version_after_registration(self):
    # wait_ready 之后 node.agent_version == "1.1.0"
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_registry tests.test_relay_pool -v`
Expected: FAIL（1.1.0 不被接受、节点无该字段）

- [x] **Step 3: 最小实现**

```python
SUPPORTED_AGENT_VERSIONS = {"1.0.0", "1.1.0"}
```

`RelayNode` 与 `ScheduledNode` 各增加 `agent_version: str = ""`；
`RelayPool.wait_ready` 回填处加 `node.agent_version = agent.version`；
`RelayScheduler.view()` 用
`getattr(agent, "version", "") or record.agent_version` 回填。

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_registry tests.test_relay_pool tests.test_relay_scheduler`
Expected: PASS

## Task 8: 编排层 sparse 门控

**Files:**
- Modify: `relay_orchestrator.py`
- Test: `tests/test_relay_orchestrator.py`

- [x] **Step 1: 写失败测试**

```python
def test_transfer_enables_sparse_when_both_agents_support_it(self):
    self.source_node.agent_version = "1.1.0"
    self.target_node.agent_version = "1.1.0"
    ...
    self.assertTrue(self._copy_tasks()[0]["sparse"])
    self.assertEqual(self._copy_tasks()[0]["hole_mode"], "skip")


def test_transfer_falls_back_when_any_agent_is_old(self):
    self.source_node.agent_version = "1.0.0"
    self.target_node.agent_version = "1.1.0"
    ...
    self.assertNotIn("sparse", self._copy_tasks()[0])


def test_transfer_disables_sparse_when_mode_off(self):
    self.mover.hole_mode = "off"
    ...
    self.assertNotIn("sparse", self._copy_tasks()[0])
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_orchestrator -v`
Expected: FAIL（任务里没有 sparse 字段）

- [x] **Step 3: 最小实现**

`RelayVolumeMover.__init__` 增加 `hole_mode: str = "skip"`；`_transfer` 组装
`common` 时追加：

```python
common["skipped_base"] = int(getattr(record, "skipped_bytes", 0) or 0)
if self._sparse_enabled(source_node, target_node):
    common["sparse"] = True
    common["hole_mode"] = self.hole_mode
```

```python
def _sparse_enabled(self, source_node, target_node) -> bool:
    """两端 agent 都支持且模式不为 off 才启用空洞跳过。"""
    if self.hole_mode == "off":
        return False
    return agent_supports_sparse(
        getattr(source_node, "agent_version", "")
    ) and agent_supports_sparse(getattr(target_node, "agent_version", ""))
```

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_orchestrator`
Expected: PASS

## Task 9: 运行时选项与进度输出

**Files:**
- Modify: `relay_runtime.py`、`app.py`
- Test: `tests/test_relay_runtime.py`、`tests/test_relay_app_wiring.py`

- [x] **Step 1: 写失败测试**

```python
def test_hole_mode_defaults_to_skip(self):
    self.assertEqual(parse_relay_options(self._options()).hole_mode, "skip")


def test_hole_mode_accepts_zero_and_rejects_unknown(self):
    self.assertEqual(
        parse_relay_options(self._options(relay_hole_mode="zero")).hole_mode, "zero"
    )
    self.assertEqual(
        parse_relay_options(self._options(relay_hole_mode="bogus")).hole_mode, "skip"
    )


def test_snapshot_reports_skipped_and_sent_bytes(self):
    # copied=3072, skipped=1024 → sent_bytes=2048


def test_env_override_forces_hole_mode_off(self):
    # MIGRATION_RELAY_HOLE_MODE=off 时 options["relay_hole_mode"] 被覆盖
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_runtime -v`
Expected: FAIL（无 `hole_mode` 属性）

- [x] **Step 3: 最小实现**

```python
@dataclass
class RelayChannelConfig:
    ...
    hole_mode: str = "skip"
```

`_FORM_SCALAR_FIELDS` 增加 `relay_hole_mode`；解析时
`hole_mode = value if value in {"skip", "zero", "off"} else "skip"`；
`mover_factory` 传 `hole_mode=self.config.hole_mode`；
`volume_progress()` 输出
`"skipped_bytes": skipped`、`"sent_bytes": max(copied - skipped, 0)`；
`app.py` 组装 options 后：

```python
forced_hole_mode = (os.environ.get("MIGRATION_RELAY_HOLE_MODE") or "").strip().lower()
if forced_hole_mode:
    options["relay_hole_mode"] = forced_hole_mode
```

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_runtime tests.test_relay_app_wiring`
Expected: PASS

## Task 10: 页面字段与展示

**Files:**
- Modify: `templates/index.html`
- Test: `tests/test_relay_ui_render.py`

- [x] **Step 1: 写失败测试**

```python
def test_step1_has_hole_mode_selector(self):
    html = self._html()
    self.assertIn('name="relay_hole_mode"', html)


def test_relay_progress_shows_skipped_bytes(self):
    html = self._html()
    self.assertIn("skipped_bytes", html)
    self.assertIn("跳过", html)
```

- [x] **Step 2: 运行测试确认失败**

Run: `python3 -m unittest tests.test_relay_ui_render -v`
Expected: FAIL

- [x] **Step 3: 最小实现**

中转机配置区新增 `<select class="form-control" name="relay_hole_mode">`，
三个 option：`skip`（默认，最快）、`zero`（目标端写零，最稳）、`off`（关闭）；
`startMigration` 的追加列表加入 `relay_hole_mode`；
`relayVolumeProgress()` 的行文本追加
`· 跳过 ${formatGiB(volume.skipped_bytes)}`。

- [x] **Step 4: 运行测试**

Run: `python3 -m unittest tests.test_relay_ui_render`
Expected: PASS

## Task 11: 全量回归与部署

- [x] **Step 1: 全量回归**

Run: `timeout 600 python3 -m unittest discover -s tests`
Expected: `OK`，用例数 ≥ 615

- [x] **Step 2: 构建并推送镜像**

```bash
docker build -t hub.ecns.io/migration/openstack-vm-migration:v1.3.0 .
docker push hub.ecns.io/migration/openstack-vm-migration:v1.3.0
```

- [x] **Step 3: 滚动发布并验证**

```bash
kubectl -n migrate set image deployment/openstack-vm-migration-deployment \
  openstack-vm-migration-container=hub.ecns.io/migration/openstack-vm-migration:v1.3.0
kubectl -n migrate rollout status deployment/openstack-vm-migration-deployment --timeout=180s
```

进 Pod 校验：`AGENT_VERSION == "1.1.0"`、页面含 `relay_hole_mode`、
`/api/relay/pkg/relay_protocol.py` 返回的源码含 `encode_hole`。

- [x] **Step 4: 更新 README**

补充"空洞模式"说明与 `MIGRATION_RELAY_HOLE_MODE` 环境变量。

---

## 执行记录（2026-09-12）

- Task 1–10 全部按 TDD 完成：每条新增用例都先跑红、再实现、再跑绿。
- 全量回归：`python3 -m unittest discover -s tests` → **Ran 646 tests, OK**
  （基线 615，新增 31 条）。
- 新增/改动模块：`relay_protocol.py`、`relay_transfer.py`、`relay_agent.py`、
  `relay_registry.py`、`relay_pool.py`、`relay_scheduler.py`、
  `relay_orchestrator.py`、`relay_ledger.py`、`relay_api.py`、`relay_runtime.py`、
  `app.py`、`templates/index.html`。
- 执行中发现的额外问题：测试里的假平台（`_StatePlatform`、
  `test_relay_ready_handshake` 的 fake）未同步 `skipped_bytes` 签名，导致
  agent 线程抛 `TypeError` 后测试挂到超时；已补齐签名，未改产品逻辑。
- 部署：镜像 `hub.ecns.io/migration/openstack-vm-migration:v1.3.0` 已推送并
  滚动发布；Pod 内校验 `AGENT_VERSION == 1.1.0`、页面含 `relay_hole_mode`、
  `/app/relay_protocol.py` 含 `encode_hole`。
- 尚未做：真实环境跑一次带空洞跳过的完整迁移（等用户重跑后看
  "已发送 / 跳过" 两个数字）。

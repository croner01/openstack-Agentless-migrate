# 中转机通道空洞跳过（sparse copy）设计

日期：2026-09-12
状态：待审阅
关联设计：`2026-09-10-relay-vm-full-copy-migration-design.md`、
`2026-09-11-relay-persistent-node-pool-design.md`

## 1. 背景与问题

中转机通道当前把源卷的整个区间逐 4 MiB 块**无条件**读出来、通过网络发送、
再写到目标卷，完全不看块内容。对于"大部分是空盘"的迁移场景，这会把大量
全零区域当成有效数据传输和落盘。

2026-09-12 的真实作业（job `53aea0c0db78`）实测：

| 盘 | 时间窗 | 时长 | 等效速率 |
| --- | --- | --- | --- |
| 系统盘 200 GiB | 16:30:02 建卷完成 → 16:57:34 拷完 | 27.5 min | ~130 MB/s |
| 数据盘 100 GiB | 17:01:20 → 17:18:55 | 17.6 min | ~102 MB/s |

即 300 GiB 中绝大部分是零值区域，却占用了约 45 分钟的跨网传输与目标端写入，
同时把 300 GiB 的零实际写进了目标存储（占用配额与 OSD 空间）。目标端此前
已经出现过 `VolumeSizeExceedsAvailableQuota`，减少无效写入本身也有收益。

## 2. 目标与非目标

### 目标

- 源端全零块不经过网络、不写目标卷，拷贝耗时与传输量随真实数据量下降；
- 数据完整性可论证、可审计、可校验，不引入"看起来成功但数据不一致"的可能；
- 对老 agent 自动回退到现有全量逻辑，不因版本不齐导致作业失败；
- 页面能看到"已处理 / 已跳过 / 已发送"三类字节，便于判断真实瓶颈。

### 非目标

- 不依赖存储底层能力：不读取 Ceph 分配图、不使用 SCSI GET LBA STATUS、
  不调用阵列 API；
- 不做数据压缩（作为后续可选叠加项）；
- 不做卷内多流并发（等本设计落地后的实测数据再决定）；
- 不改变"源 VM 先关机、打快照、派生卷挂载"的既有流程；
- 不改变增量/CBT 相关的任何设计。

## 3. 术语与业界对照

本设计实现的是业界通称的 **sparse copy / zero-block skipping / thin-aware copy**：

| 工具 | 对应做法 |
| --- | --- |
| `qemu-img convert` | 零块检测（`-S` 控制检测粒度），全零块不写目标 |
| `nbdcopy` | 查询 extent，空洞下发 `WRITE_ZEROES` 而不是搬数据 |
| `virt-v2v` / `virt-sparsify` | 迁移/转换时稀疏化，空块不落盘 |
| `dd conv=sparse`、`cp --sparse=always`、`rsync --sparse` | 文件与块层跳零 |
| Ceph `rbd sparsify`、`rbd cp` | 按分配图跳洞，未分配区域读出即零 |

与上述工具一致，本设计**不假设源端必须稀疏**（源卷可能是全量分配的），
只根据块内容判定，属于纯读取侧判定 + 目标侧应用的实现。

## 4. 安全不变量（本设计成立的前提）

跳过一个零块，只有当目标对应区域读出来也是零时才等价于写零。平台用以下
三条不变量保证这一点，任一条不成立时必须关闭空洞跳过：

1. **目标卷由本次作业新建且为空白卷**。`VolumeLifecycle.create_target_volume`
   走 `create_blank_volume`，Ceph/RBD 未分配区域读出即为零，平台自身是唯一写者。
   台账记录 `target_volume_id`，实现上要求该字段存在且由本作业产生。
2. **源数据在拷贝期间冻结**。relay 通道先关机、打快照、挂派生卷，拷贝期间
   源内容不会变化；因此"某块现在全零"在整个拷贝期间都成立。
3. **判定是保守的**。以 4 MiB 为粒度逐块比较，只要块内有一个字节非零，
   整块按原始字节发送，不存在把非零数据判成零的路径。

不满足前提时的行为：空洞跳过自动关闭，退回现有全量搬移，且不报错。

## 5. 协议设计

### 5.1 新增 Hole 帧

现有帧：4 字节魔数 `RLY1` + `(offset, length, crc32)` 头 + 载荷。

新增 Hole 帧：魔数 `RLYH`，头结构不变，`length` 表示被跳过的区间长度，
**不带载荷**。语义为"源端该区间全零，目标端按空洞模式处理"。

- `encode_hole(offset, length)` / `read_frame(stream)` 为新增 API；
- `read_chunk` 保持原语义不变（老 agent 的兼容路径）；
- 接收循环改用 `read_frame`，返回 `("data", offset, payload)` 或
  `("hole", offset, length)`；非法帧类型、长度越界、校验失败一律报
  `ProtocolError`，不会静默丢数据。

### 5.2 发送侧（source agent）

`send_device(..., skip_zero: bool)`（发送侧只需知道"要不要判零跳洞"，
空洞在目标端怎么落地由接收侧的 `hole_mode` 决定）：

- 按 `chunk_size` 读取后先判零：`chunk == zeros(len(chunk))`（C 级 memcmp，
  4 MiB 块下开销可忽略）；
- 全零且 `skip_zero` 为真：发送 Hole 帧，`skipped_bytes += len(chunk)`；
- 其余情况按原逻辑发送数据帧，`sent_bytes += len(chunk)`；
- `copied_bytes`（进度口径）为"已处理字节" = 已发送 + 已跳过；
- 结束时上报 `copied_bytes / sent_bytes / skipped_bytes`。

### 5.3 接收侧（target agent）

`receive_device(..., hole_mode: str)` 遇到 Hole 帧时按模式处理：

| 模式 | 目标端动作 | 依赖前提 |
| --- | --- | --- |
| `skip`（默认） | 仅 `seek`，不写 | 目标卷确为空白卷（不变量 1） |
| `zero` | 对区间下发 `BLKZEROOUT`；不支持时回退为分块写零 | 无（对已有数据的卷也安全） |
| `off` | 不会收到 Hole 帧（平台不下发 sparse） | — |

接收侧同样累计 `received_bytes / skipped_bytes`，进度口径与发送侧一致。
Hole 帧的区间与数据帧一样参与"chunk 偏移必须严格递增、必须落在
`[offset, offset+length)` 内"的校验，避免出现覆盖或空洞错位。

## 6. 版本协商与回退

协议升级必须保证老 agent 不会挂：

- `AGENT_VERSION` 提升到 `1.1.0`；平台 `SUPPORTED_AGENT_VERSIONS` 同时接受
  `1.0.0` 与 `1.1.0`（注册不因版本升级被拒）；
- `RelayNode` / `ScheduledNode` 增加 `agent_version` 字段，注册与心跳时回填；
  常驻模式从节点清单记录读取（已有 `agent_version`）；
- 版本比较按点分数字段逐段比较（`1.10.0 > 1.9.0`），不按字符串比较；
- 平台在 `_transfer` 决定是否下发 sparse：**仅当源、目标两端 agent 版本
  ≥ 1.1.0，且本次作业空洞模式不为 `off`** 时，任务里带 `sparse: true`；
  否则任务不带该字段，两端行为与今天完全一致；
- 老 agent 收到不带 `sparse` 的任务不会产生 Hole 帧，因此不会触发新帧解析。

## 7. 进度、台账与页面

### 7.1 台账

`VolumeTaskRecord` 增加 `skipped_bytes: int = 0`。`copied_bytes` 语义明确为
**该卷已处理字节（绝对偏移口径）**，空洞跳过同样计入，保证进度百分比单调递增，
停滞检测继续有效。

### 7.2 API

`/api/jobs/<id>/relay` 的 `volumes[]` 增加：

```json
{
  "volume_id": "...", "vm_id": "...", "phase": "copying",
  "copied_bytes": 0, "total_bytes": 0, "skipped_bytes": 0,
  "sent_bytes": 0, "progress_percent": 0.0, "throughput_mb_s": 0.0
}
```

`sent_bytes = copied_bytes - skipped_bytes`，由平台计算，避免两侧口径不一致。

### 7.3 页面

卷进度行展示为：

```
win-跳板机11 · copying · 12.30 GiB / 200.00 GiB（6.2% · 118.4 MiB/s · 跳过 187.70 GiB）
```

中转机配置区新增"空洞模式"下拉（`skip` / `zero` / `off`，默认 `skip`），
与现有"停滞判死"字段同排。

## 8. 校验与审计

- 现有尾部 16 MiB sha256 比对与可选整盘 sha256（`relay_full_verify`）读的是
  **逻辑内容**：跳过区间读出为全零，若目标残留了非零数据，整盘校验会直接报
  不一致。开启整盘校验即可获得端到端证据。
- `skipped_bytes` 落台账，作业结束后可核对"跳过占比"是否符合预期。
- 关闭方式有三层：单作业表单选 `off`、平台级环境变量
  `MIGRATION_RELAY_HOLE_MODE=off`（对所有作业强制关闭）、以及源/目标
  agent 版本不齐时的自动回退。

## 9. 边界与降级

| 场景 | 行为 |
| --- | --- |
| 源盘是 LUKS/dm-crypt 加密卷 | 未写区域在块层读出来不是零，判定自然不成立，退化为全量搬移（安全，无收益） |
| 源端读路径本身就是瓶颈 | 判零必须先读出来，耗时下降幅度小于传输量下降幅度；由上报的三类字节定位 |
| 目标端 `BLKZEROOUT` 不被支持 | `zero` 模式回退为分块写零；`skip` 模式不涉及 |
| 断点续传（`offset > 0`） | 已写区域与源一致，跳过区间仍为初始零，语义不变 |
| 作业中途取消 | 与现有行为一致：取消即中止，目标卷按既有策略保留/清理 |

## 10. 模块改动清单

| 模块 | 改动 |
| --- | --- |
| `relay_protocol.py` | Hole 帧常量、`encode_hole`、`read_frame` |
| `relay_transfer.py` | `send_device` 判零与 Hole 发送；`receive_device` 空洞处理与计时 |
| `relay_agent.py` | 任务字段透传（`sparse` / `hole_mode`）、三类字节上报、版本号 1.1.0 |
| `relay_registry.py` | 接受 1.1.0；节点记录带 `agent_version` |
| `relay_pool.py` / `relay_scheduler.py` | 节点视图携带 `agent_version` |
| `relay_orchestrator.py` | 版本判定后下发 `sparse`；台账写入 `total_bytes` / `skipped_bytes` |
| `relay_ledger.py` | `VolumeTaskRecord.skipped_bytes` |
| `relay_runtime.py` | 空洞模式选项解析、`snapshot().volumes` 增加 `skipped_bytes` / `sent_bytes` |
| `templates/index.html` | 空洞模式下拉、进度行展示跳过量 |

## 11. 测试策略（TDD）

先写失败测试，再实现。关键用例：

1. 协议：Hole 帧编解码往返；`read_frame` 同时正确解析数据帧与空洞帧；
   损坏魔数/长度/校验仍抛 `ProtocolError`。
2. 发送侧：全零块不产生数据帧；**含单个非零字节的块必须原样发送**（安全回归）；
   跳过字节统计准确。
3. 接收侧：`skip` 模式目标区间保持零；`zero` 模式对预置了非零数据的目标区间
   写零；`BLKZEROOUT` 不可用时回退写零。
4. 端到端：真实 socket 下"源全零 → 目标全零"与"源含数据 → 目标逐字节一致"。
5. 版本协商：任一端为 1.0.0 时任务不带 `sparse`，行为与现状一致。
6. 台账/接口：`skipped_bytes` 落盘；`snapshot().volumes` 输出 `sent_bytes`。
7. 停滞检测：空洞跳过导致的 `copied_bytes` 增长不算停滞，也不误判。
8. 页面：空洞模式字段与"跳过"展示存在。

## 12. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| 目标卷不是空白卷导致漏写 | 只有平台新建的空白卷才允许 `skip`；`target_volume_id` 必须来自本作业；需要绝对保险时用 `zero` 模式或整盘校验 |
| 判零开销拖慢非空盘 | 4 MiB 粒度 + 缓存零缓冲 memcmp；真实数据多的盘退化为接近原速 |
| 版本不齐导致新帧被老 agent 拒收 | 双端版本门控 + 老版本自动回退；注册层同时接受 1.0.0/1.1.0 |
| `BLKZEROOUT` 在部分后端行为不一致 | 失败即回退写零；`skip` 模式完全不用该能力 |

## 13. 后续可叠加项（本设计不做）

- 非零数据压缩（lz4/zlib，CPU 换带宽）；
- 卷内多流并行（需先确认链路是否 1 GbE 打满）；
- 存储侧 extent 快速路径（Ceph `rbd diff` / SCSI GET LBA STATUS），
  仅在明确允许依赖底层时作为可选加速。

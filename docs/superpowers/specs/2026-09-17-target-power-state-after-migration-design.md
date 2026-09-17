# 迁移完成后目标机电源状态（逐 VM 开关）设计

日期：2026-09-17
状态：已确认（方案 A）
关联设计：`2026-09-08-cross-openstack-ceph-migration-design.md`、
`2026-09-10-relay-vm-full-copy-migration-design.md`、
`2026-09-10-rbd-layout-aligned-incremental-migration-design.md`

## 1. 背景与问题

现在三条迁移路径在收尾时都会把目标机开起来：

- RBD 直连（全量）：换完卷后 `os-start` → 等 `ACTIVE` → `SUCCESS`；
- RBD 直连（增量）：增量收敛/手动切换后同样走上面这段；
- 中转机通道：目标机由 Nova 从目标卷建出来，建机过程本身就会开机。

但真实交付里存在"先把数据搬过去、暂不切业务"的场景，例如：

- 目标侧还要先做安全加固、改 IP 规划、挂监控，起机就跑业务会出事故；
- 需要人工校验目标盘数据后再选停机窗口开机；
- 批量迁移时想按批次错峰开机，避免目标云瞬时资源争抢。

目前没有任何开关，运维只能迁移完手工逐台关机，既费力又容易漏。

## 2. 目标与非目标

### 目标

- 向导步骤④按**每台 VM**选择「迁移完成后是否开机」，默认开机（与现状一致）；
- 关掉后：目标机照常建成、数据照常换入、作业状态照常算成功，但**保持关机**；
- 三条路径（RBD 全量 / RBD 增量 / 中转机）语义一致，含增量·手动模式的
  「开始切换」收尾（方案 A：切换流程走完但目标保持关机）；
- 迁移状态与"目标未开机"这一事实在作业详情里可辨；
- 存量作业与不传该字段的老客户端行为不变。

### 非目标

- 不做"迁移完成后自动延时开机/定时开机"；
- 不做目标机开机后的业务健康检查（现网的 `wait_server_status(ACTIVE)` 不变）；
- 不新增 `VmStatus`（避免改动 terminal 集合与既有状态判定）；
- 不改环境档案（profile）：这是作业级/VM 级选项，不随档案保存。

## 3. 关键决策

| 决策 | 选择 | 理由 |
| --- | --- | --- |
| 默认值 | **开机** | 与现行为完全一致，存量任务与老客户端不受影响 |
| 粒度 | 逐 VM（步骤④一行一个复选框）+ 批量设置 | 用户要求逐台；批量避免几十台逐个点 |
| 增量·手动切换 | **遵循开关**（方案 A） | 语义前后一致；「开始切换」弹窗里明确提示 |
| 状态模型 | 复用 `SUCCESS`，新增 `VmTask.start_target` 字段 | 不新增状态，不改 terminal 判定 |
| 落点 | `row_overrides[key].start_target` | 复用既有的逐 VM 覆盖通道，不新增顶层表单字段 |
| Excel | `start_target` 作为可选列，留空=开机 | 与 `mode`/`target_flavor` 同一套列约定 |

## 4. 三条路径的行为

### 4.1 RBD 直连（全量 / 增量）

换卷要求目标机处于关机态，`_copy_volumes` 成功后目标本来就是 `SHUTOFF`。
因此关掉开关时**只需跳过 `os-start`**，不产生额外的开关机动作：

```
卷全部替换成功
  ├─ start_target=True  → STARTING_TARGET → os-start → 等 ACTIVE → VERIFYING
  └─ start_target=False → 记日志「按配置保持目标机关机」，直接收敛为成功
```

### 4.2 中转机通道

目标机是在卷拷完后由 `create_server_from_volumes` 建出来的，Nova 建机必然
先开机一次（没有"建成即关机"的接口）。所以要"迁移后不开机"必须：

```
wait_server_booted(ACTIVE) → os-stop → wait_server_status(SHUTOFF) → VERIFYING
```

**已知副作用**：这条路径下目标 OS 仍会启动一次（cloud-init 等会跑一遍）。
这是 Nova 语义决定的，需在文档与页面上说明，避免运维误以为"从未开机"。

### 4.3 增量·手动模式

`_await_manual_actions` 收到「开始切换」后照常执行：停源 → 末轮增量 → 换卷，
随后进入 4.1 的分支。即：数据已切换、源机已停，但目标按配置保持关机，
等人工开机。前端「开始切换」确认弹窗对该 VM 追加提示。

## 5. 数据流

| 层 | 变更 |
| --- | --- |
| 前端向导步骤④ | 新增一列复选框「迁移后开机」（默认勾选）；工具条加批量下拉 |
| 提交 | `row_overrides[key].start_target` (bool) |
| `excel_parser.MigrationRow` | 新增 `start_target: bool = True`；`parse_start_target()` 解析布尔字面量 |
| `state_machine.VmTask` | 新增 `start_target: bool = True`，进 `to_dict`/`from_dict`（重启后不丢） |
| `job_manager.create_job` | 由 `MigrationRow` 透传到 `VmTask` |
| `migration_manager` | `_start_target_if_requested()`（RBD）与中转机路径的 `os-stop` 收尾 |
| `app._serialize_vm` | 下发 `start_target`，供作业详情报展示 |

## 6. 页面呈现

- 步骤④规划表：新增列「迁移后开机」，逐行复选框；工具条批量项
  「批量设置…」可选 `迁移后开机 / 迁移后不开机`，应用到全部规划行；
- 步骤④摘要：统计「N 台将保持关机」，提交前可见；
- 作业详情 VM 行：`start_target=False` 的 VM 名称旁加 chip「迁移后不开机」；
- VM 抽屉：「迁移后开机：是/否」；
- 「开始切换」确认弹窗：该 VM 为不开机时追加一行提示。

## 7. 兼容性

- 不传 `start_target`（老前端 / 老 Excel / 老 JSON 快照）→ 默认 `True`，行为同今天；
- `jobs_state.json` 里没有该字段的历史作业 → `from_dict` 走默认值，可正常加载；
- `?full=1` 与详情接口的响应只多一个字段，不影响既有消费方。

## 8. 测试计划

| 用例 | 断言 |
| --- | --- |
| `state_machine` 序列化 | 默认 True；`to_dict`/`from_dict` 往返保持 False |
| `excel_parser` | 缺列/留空=开机；`false/0/no/否/不开机` 解析为 False；非法值报错并带行号 |
| `job_manager.create_job` | `start_target` 透传到 `VmTask` |
| `migration_manager`（RBD） | 默认调 `start_server`；`start_target=False` 时**不调** `start_server` |
| `migration_manager`（中转机） | `start_target=False` 时 `stop_server` + 等 `SHUTOFF`；默认不调 `stop_server` |
| `app.api_migrate` | `row_overrides.start_target=false` 落到 VM；缺省为 True |
| 渲染 | 步骤④存在该列与批量控件；作业详情渲染 chip |

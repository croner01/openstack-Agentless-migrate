# 布局对齐的 RBD 增量迁移设计（方案 2）

日期：2026-09-10
状态：待审阅

## 1. 背景与目标

现有 `ceph_utils.replace_rbd_data()` 使用裸命令 `rbd import - <stage>`，目标
映像取的是 rbd 默认参数（`order=22`、默认 features），与源卷实际布局无关。
线上用快照做增量迁移后业务无法启动，根因不是快照，而是换入后的数据面布局
与源卷不一致：`order`/`object_size`、`stripe_unit`/`stripe_count`、
`features`、`data_pool` 等字段存在差异。

约束：目标卷必须由 Cinder 创建，不在底层直接 `rbd create` 卷本体。

关键事实：Cinder 的 `rbd_store_chunk_size` 是 backend 级配置
（`cinder.conf` 每个 backend 一份），不是卷类型级，因此一个 Cinder 部署只能
产出一套 `order`，无法逐卷对齐；而 Cinder 只认 `volume-<uuid>` 这个映像名与
所需 features，换入后不会校验 `order`/`stripe`。因此方案可行：
Cinder 照常建卷，我们在其建完后按源参数重建数据面并以同名换入。

目标：

- 换入后的目标卷布局与源卷逐字段一致，消除启动失败；
- 停机窗口从全量拷贝时长降到最后一轮增量；
- 迁移过程中产生的临时快照可追踪、可清理，不留残骸。

## 2. 范围

范围内：布局预检与对齐（两条路径共用）、**全量路径保持独立可用**、增量预拷贝
作为独立功能项、临时快照生命周期与清理、断点续传与孤儿回收。

范围之外：快照历史与克隆关系迁移、跨集群 `rbd migration`、走
`cinder backup-create`/`backup-restore` 的 Cinder 原生通路、删除源 VM、
删除源卷上属于用户的既有快照。

## 3. 核心原则

### 3.1 布局参数分两类

| 字段（`rbd info --format json`） | 可变性 | 对齐手段 |
| --- | --- | --- |
| `features` | 可变 | `rbd feature enable/disable` 事后修（`object-map` 需 `exclusive-lock`，`fast-diff` 需 `object-map`） |
| `size` | 可变 | `rbd resize` |
| `order` / `object_size` | 不可变 | 按源参数建像 |
| `stripe_unit` / `stripe_count` | 不可变 | 按源参数建像 |
| `data_pool` | 不可变 | 按源参数建像，跨集群需显式池映射 |
| `image_format` | 不可变 | format 1 无 features，无法增量，降级为全量 |

### 3.2 features 取并集

目标 features = 源 features ∪ Cinder 运行所需（至少 `exclusive-lock`、
`layering`）。源缺失 `object-map`/`fast-diff` 时，导入后按依赖顺序补齐，
既保证 Cinder attach 正常，也让后续增量 diff 可用。

校验只对"布局特性"（`LAYOUT_FEATURES`：layering / striping / exclusive-lock /
object-map / fast-diff / deep-flatten / journaling / data-pool）要求目标端覆盖。
源集群版本较新时会带上 `operations` ——它由 op_features（如 `snap-trash`）
触发，只描述镜像操作能力，目标端 librbd 建像时按自身 op_features 归一化，
无法也不需要复制过来。这类特性缺失只记 INFO 日志，不判失败，否则一台已经
跑完全量拷贝的 VM 会在收尾校验时白费一轮。

### 3.3 不绕过 Cinder 的卷生命周期

卷的创建、ID、`volume-<uuid>` 记录、挂载、配额仍由 Cinder 拥有；我们只替换
该映像的数据面内容。改动全部落在已挂载文件，不新增 Python 模块。

### 3.4 增量与全量是两个独立功能项

增量迁移不是全量迁移的增强，而是与之并列的第二个功能项。两者必须能各自
独立运行，这样增量出问题时仍可用全量路径完成迁移。

- 全量迁移（`mode=full`）：既有能力，保持可用。一次性 `rbd export/import`
  全量拷贝，不创建任何迁移快照。
- 增量迁移（`mode=incremental`）：新增能力。开关、预拷贝、快照治理独立，
  不改变全量路径的行为与语义。
- 默认 `mode=full`，与今天完全一致；增量必须逐 VM 显式开启。
- **功能项独立不等于代码分支独立。** 迁移流水线的全部生命周期步骤——解析
  源 VM、创建端口、创建目标数据卷、创建 BFV、停目标、采集卷映射、停源、
  开机、验证——只有一份实现，两条路径共用；真正按 mode 分叉的只有
  "卷数据搬运"这一个接缝。
- 实现方式：`MigrationManager._mover_for_mode(mode)` 返回数据搬运器
  （`FullVolumeMover` / `IncrementalVolumeMover`），二者接口一致
  （`prepare` / `finish` / `on_failure`）。`_migrate_vm_inner` 内不出现
  mode 判断：全量实现的 `prepare` 与 `on_failure` 都是空操作。
- 选择方式：模式由**前端显式选择**。页面提供作业级"迁移模式"下拉
  （全量拷贝 / 增量预拷贝），并保留逐 VM 覆盖；后端按所选模式选用搬运器。
- **不做自动降级**：增量失败只把该 VM 标记为失败并保留目标资源，要改走全量
  必须由操作员把模式改成"全量拷贝"后重新提交，不做同一次运行内的自动切换。
- 布局对齐是两条路径共用的基础修复，对全量路径同样生效；全量路径的对外
  语义（一次全量拷贝、不建快照）不变，只是建像参数由默认值改为按源对齐。

## 4. 参数对齐

### 4.1 建像命令

```text
rbd --conf <dst> import \
  --order <源 order> \
  --stripe-unit <源 stripe_unit，仅源有 striping 时> \
  --stripe-count <源 stripe_count，仅源有 striping 时> \
  --image-feature <源 ∪ Cinder 必需> \
  --data-pool <显式映射后的池，如启用> \
  - <pool>/<target>-mig-stage
```

参数名以现场 `rbd import --help` 为准（新版本 `--order` 与 `--object-size`
等价）。源为 format 1 时禁用增量与 striping 对齐，只做等 layout 全量替换。

### 4.2 硬校验

新增 `assert_layout_matches(source_info, stage_info)`，在建像后、换入前
逐字段比对上表全部字段，并继续校验 `size`；任一不符即失败退出，不换入、
不删原目标映像。`VolumeTask` 记录该差异清单，页面可直接展示。

## 5. 总体流程

```text
预检（源运行）    P0 读源 rbd info → 生成建像参数 + 布局清单
在线（无停机）    P1 源打 snap_0 → rbd export 全量 → 按参数建 stage → 校验布局
在线（无停机）    P2 每轮：源打 snap_n → export-diff --from-snap snap_{n-1}
                     → import-diff 到 stage → 轮询直到 delta < 阈值
切换（短停机）    P3 源 VM 干净关机 → 打最终快照 → 最后一轮 export-diff/import-diff
                     → 校验 size/布局 → 删同名目标映像 → rename 换入 → 开机验证
收尾              P4 清理该卷全部迁移快照 → 置 success
```

基像与中间快照可以是崩溃一致（crash-consistent），最终一致性由停机后再打
最后一轮增量保证，因此在线阶段不需要 guest 停机。现有 stage + rename 的
短窗口换入设计保留，用于崩溃一致性。

## 6. 临时快照生命周期（本次新增重点）

### 6.1 命名与归属

- 迁移快照统一命名 `mig-<job_id>-<seq>`，只创建在源卷上；目标 stage 像不建快照。
- 只操作带 `mig-` 前缀且能在迁移记录中找到归属的快照，绝不触碰用户快照。
- `VolumeTask` 记录 快照名 → 创建时间 → 所属 job/VM/卷，并随
  `uploads/jobs_state.json` 落盘，作为清理与孤儿回收的唯一依据。

### 6.2 创建时机

- P1 建 `snap_0` 作为基像与增量的起点；
- P2 每轮建 `snap_n`，保留 `snap_{n-1}` 供 `export-diff --from-snap` 使用；
- P3 在源 VM 干净关机后创建最终快照。

### 6.3 保留规则

`snap_{n-1}` 必须保留到第 n 轮 `import-diff` 成功为止；因此同一卷任一时刻
最多存在 2 个迁移快照（当前 + 上一个），每轮成功后立即清理更早的。

### 6.4 清理时机

- 成功路径：该 VM 所有卷换入并校验通过后，删除该卷全部迁移快照，再置 `success`；
- 失败路径：VM 级失败同样要清理自己创建的快照。快照会持续占用空间、影响
  `flatten`，并可能导致源卷删除失败，不能留给人工；
- 清理失败：按指数退避重试并告警，记入 `cleanup_pending`，不静默成功，
  但不回滚已完成的迁移结果。

### 6.5 孤儿回收（Orphan GC）

服务启动时与每个 Job 结束后各执行一次：扫描源池中 `mig-` 前缀快照，与迁移
记录比对；归属 job 已完成或不存在者视为孤儿，删除并写审计日志。非 `mig-`
前缀一律不动。

几条"不再重试"的判定（都是线上持续刷警告、空等超时换来的）：

- 卷级 `cleanup_status` 已落终态（`cleaned`）的卷不再重扫：收工的历史任务不
  该每轮都去摸一次集群；
- 源卷已不存在（rbd rc=2 / ENOENT）时把 `cleanup_status` 记为 `gone`：迁移
  成功后清理源机是常规操作，快照随卷消失，无从回收；旧实现按"环境抖动"每轮
  重试，同一条 WARNING 会永远刷下去；
- 目标池扫描前先探一次 `mon_host` 端口：历史任务的 conf 可能指向早已下线的
  旧集群（线上有 conf 指向 `100.100.13.21`，`rbd ls` 实测挂到 180s 仍无输出），
  联不上就跳过并报出 mon 地址，而不是空等超时；同一份 conf 内容的多个任务
  只扫一次池（按 conf 内容指纹去重）。

### 6.6 与现有逻辑的冲突处理

- 现有 `replace_rbd_data` 会拒绝目标 RBD 存在快照的换入，保留该校验：
  目标侧本就不应出现迁移快照；
- 源侧快照不影响换入，需放行；
- 预检若发现源卷已有用户快照，仅告警不迁移快照历史，不读取、不删除。

### 6.7 保护上限

- 单卷迁移快照 ≤ 2（当前 + 上一个），由每轮 `keep=2` 与结束时 `keep=0` 保证；
- 孤儿快照由服务启动与每个任务结束后的 GC 回收，另有并发拷贝门控限制总量；
- 基于保留时长的超时告警列为后续增强，本期依靠"任务结束即清理 + GC"闭环；
- 删除前二次确认前缀与归属，禁止通配或批量删除。

## 7. 状态机与数据模型

增量复用同一套状态机，只做两处调整：把只读的"采集卷映射"提前到停源之前，
并在它与"停源"之间插入一个 `precopying` 阶段（源 VM 仍运行）。停机后的最终
增量与换入仍走原来的 `copying_volumes`：

```text
creating_target_bfv -> stopping_target -> collecting_volumes
  -> [precopying]              # 仅 mode=incremental，源 VM 运行中；全量为空操作
  -> stopping_source -> copying_volumes   # 增量：末轮 diff + 换入；全量：一次性全量
  -> starting_target -> verifying -> success / failed
```

`collecting_volumes` 只做 attachment 读取与 `rbd info`，不依赖关机状态；提前后
对全量路径的副作用是"映射失败会更早暴露、源 VM 尚未停机"，属于更安全的顺序。

`VmTask` 新增 `mode`；`VolumeTask` 新增 `source_pool`、`target_pool`、`mode`、
`layout`（源参数清单）、`snapshots`（创建/删除记录）、`cleanup_status`。
`failed` 仍保留目标资源供人工排查。

## 8. 配置与 API

增量参数与既有并发/限速一致，走请求级表单字段，默认值仅作兜底：

- `delta_rounds`：增量轮次上限，默认 3；
- `delta_threshold_mb`：delta 小于该值即可进入切换，默认 512；
- `delta_interval_seconds`：轮次间隔，默认 300；
- 迁移模式：作业级下拉 `global-mode`（`full` / `incremental`），逐 VM 可在
  清单卡片上覆盖，最终通过 `selected_rows`/`row_overrides` 的 `mode` 传到后端。

API：`POST /api/migrate` 增加每 VM 的增量迁移开关；
`GET /api/jobs/<job_id>` 返回每卷布局清单、delta 进度与快照清理状态。

### 卷级状态与进度口径

- 卷在"数据在搬"时是 `copying`；基线全备或某一轮增量**搬完**就落到
  `ready`（页面徽标「已就绪」），表示暂存卷数据可用、在等下一步动作
  （用户点「同步一次」或「开始切换」）；只有换入占位卷成功才置 `success`。
- 增量轮次的进度总量取自 `rbd diff` 预估，比 `rbd export-diff` 的实际载荷略大
  （线上见过预估 5.75 GiB / 实传 5.71 GiB），回调因此可能停在 99.x%。所以
  "一轮搬完"这一刻由编排层强制把进度收敛到 100% 并清掉速率，避免出现
  VM 已 `awaiting_cutover`、卷行却停在「拷贝中 99.2% · 7.4 MiB/s」的画面。

## 9. 失败恢复与回滚

- stage 像与 `jobs_state.json` 记录保留，服务重启后可从上一轮 diff 继续，
  或按记录清理孤儿快照后重试；
- 目标卷仍遵循换入成功才开机，失败不自动删除目标资源、不自动删除源 VM；
- 任何情况下不删除用户快照与用户卷。

## 10. 测试策略

沿用标准库 `unittest` 与注入式 Fake runner：

- 建像参数生成：order/stripe 透传、features 并集与补齐顺序；
- `assert_layout_matches`：逐字段不一致用例；
- 快照时序：每轮保留上一快照、清理更早、结束时全清；
- 失败与中断路径：VM 失败后清理、孤儿 GC、`cleanup_pending` 重试；
- 归属保护：用户快照与不带 `mig-` 前缀的快照不被删除。

## 11. 关键假设与待确认

- 允许在 Cinder 卷上直接创建 rbd 快照（我们自管生命周期并负责清理）；
  若运维要求全程经 Cinder，可改用 `volume snapshot` API，代价是需按
  `snapshot-<uuid>` 建立映射关系；
- 源卷具备 `fast-diff`/`object-map`，否则每轮接近全量，增量收益消失；
- 两套 Ceph 集群可从迁移服务节点访问，`export-diff`/`import-diff` 流经本服务；
- 服务保持单副本，快照记录与 Job 状态一致。

## 12. 实施步骤

1. 共用基础（两条路径都受益）：布局清单比对 + 建像参数显式化，直接修掉全量
   路径的启动失败；
2. 功能项 A（全量）：确认 `mode=full` 行为与今天一致，作为默认可独立使用；
3. 功能项 B（增量）：`mode=incremental` 开关 + 基像 + `export-diff`/
   `import-diff` 增量轮次 + 停机后最终增量与换入；
4. 快照治理：清理、`cleanup_pending` 与孤儿 GC，仅作用于增量路径；
5. 前端与回归：逐 VM 模式选择、以全量重跑入口、README 与 ConfigMap 同步。

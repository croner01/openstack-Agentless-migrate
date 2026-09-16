# 中转机常驻节点池设计（租户内共享、自动扩缩容）

日期：2026-09-11
状态：待审阅
关联设计：`2026-09-10-relay-vm-full-copy-migration-design.md`（下称"旧设计"）

## 1. 背景与目标

旧设计把中转机池定义为**作业级**：作业创建时建池，作业结束销毁，并把
"中转机常驻池（跨作业复用）"明确列为范围之外。实际运行中这个模型暴露三个问题：

1. 每个作业都要重新建机并等待 agent 注册（默认 300s 超时），建机受配额、
   调度失败、cloud-init 波动影响，任一环节出问题会直接卡住整个作业；
2. 反复创建、删除虚拟机与端口，在租户配额紧张时失败率更高，且没有人工兜底入口；
3. 单机只能串行服务一个卷对，迁移吞吐上不去。

本设计把中转机升级为**平台级常驻资源**：按租户共享、自动扩缩容、可在页面手动
扩容/删除/重建。作业只"租用"节点，不再拥有节点生命周期。

目标：

- 中转机跨作业复用，同一租户内多作业共享；
- 单机支持 5 台 VM 并发迁移，超过阈值自动扩容；
- 每租户每角色全局上限 6 台、跨 AZ 共享额度（源 6 + 目标 6 各自独立）；
- 空闲 24 小时自动缩容到 1 台，保留常驻；
- 作业失败/取消不影响节点；
- 云凭据由平台加密保存，节点管理脱离作业表单；
- 用户可用 root 密码 SSH 登录中转机排查。

## 2. 范围

### 范围内

- 常驻节点清单、租约、调度与自动扩缩容；
- 单机多任务并发（数据面多 worker）；
- 节点级长期注册凭据与 agent 自动重注册；
- 云凭据与节点 SSH 密码的加密存储；
- 节点管理 API 与独立页面；
- 节点维度的孤儿 attachment 对账。

### 范围之外

沿用旧设计已排除的内容，本设计不重新讨论：

- 增量拷贝、CBT、阵列远程复制；
- 运行中卷的在线快照模式；
- 对源卷 detach，或任何变更源 VM 卷挂载状态的操作；
- 平台重启后的作业进度续跑（只恢复节点与清理孤儿，不恢复任务）；
- 跨平台多副本部署（平台仍为单副本，状态靠落盘恢复）；
- 传输加密与静态加密（沿用旧设计的未支持声明）。

## 3. 与旧设计的差异

旧设计未失效，本设计是在其之上的增量修订。差异清单：

| 主题 | 旧设计 | 本设计 |
| --- | --- | --- |
| 池的归属 | 作业级，作业结束销毁 | 平台级常驻，作业只租用 |
| 池的键 | (云, AZ) | (租户, 角色, AZ) |
| 单机并发 | 1 个卷对，完全串行 | 5 个 VM 槽位，槽位内多盘串行 |
| 注册令牌 | 绑定 `job_id`，1 小时过期 | 节点级长期令牌，可轮换 |
| 平台签名密钥 | 每次启动随机生成 | 必须持久化，否则重启后节点全部失联 |
| 目标监听端口 | 固定 9200 | 端口池 `base + slot` |
| 云凭据 | 作业表单，不落盘 | 加密落盘，节点管理复用 |
| 失败语义 | 中转机失败保留，成功后销毁 | 作业失败/取消不动节点；节点故障自动重建 |
| 页面位置 | 迁移向导步骤 1 | 独立"中转机资源"页 + 向导内引用池 |

旧设计中继续有效的部分：停机后快照派生、绝不 detach 源卷、块帧协议与一次性票据、
限速、断点续传、完成校验、卷台账与 reaper 思路。

## 4. 关键决策

以下为本次确认的设计输入，实施时不得擅自更改：

1. 作用域：租户内共享；
2. 容量单位：1 VM = 1 槽位，槽位内多块盘串行拷贝；
3. 每租户每角色全局上限 6 台、跨 AZ 共享额度，源 6 + 目标 6 各自独立计数；
4. 自动缩容：空闲满 24 小时缩到 1 台，始终保留 1 台常驻；
5. 作业失败/取消：节点不做任何操作，只归还租约；
6. 允许一台节点被多个作业共享；
7. 云凭据由平台加密保存，同时节点支持 root 密码 SSH 登录。

## 5. 总体架构

控制面仍在现有 Flask 进程内，新增平台级资源层；作业层退化为"申请槽位"。

```
                    ┌───────────────────────────────────────────┐
                    │  平台级资源层（新增，常驻）               │
                    │  relay_inventory  节点清单（落盘）        │
                    │  relay_lease      租约（落盘）            │
                    │  relay_scheduler  容量判定与自动扩缩容    │
                    │  relay_node_manager 建/删/重建/drain      │
                    │  relay_credentials 加密凭据与 SSH 密码    │
                    └───────────────┬───────────────────────────┘
                                    │ acquire / release
                    ┌───────────────┴───────────────────────────┐
                    │  作业层（沿用现有 relay_runtime 装配）    │
                    │  RelayVolumeMover：快照派生、挂载、拷贝   │
                    └───────────────┬───────────────────────────┘
                                    │ 任务下发（现有 agent 协议）
                    ┌───────────────┴───────────────────────────┐
                    │  数据面：常驻中转机 agent（多 worker）    │
                    └───────────────────────────────────────────┘
```

模块划分（新增/改造）：

| 模块 | 状态 | 职责 |
| --- | --- | --- |
| `relay_inventory.py` | 新增 | 节点清单的加载、落盘、查询、状态更新 |
| `relay_lease.py` | 新增 | 租约申请、续租、释放、按作业/节点索引 |
| `relay_pool_profile.py` | 新增 | 池级建机参数（镜像/flavor/网络/子网/卷类型/密码策略） |
| `relay_scheduler.py` | 新增 | 池容量计算、扩容判定、缩容判定、排队 |
| `relay_node_manager.py` | 新增 | 节点创建、删除、重建、drain/resume |
| `relay_credentials.py` | 新增 | 云凭据与 SSH 密码的加密存储 |
| `relay_pool.py` | 改造 | 从"每作业建池"改为"按池键取常驻节点" |
| `relay_registry.py` | 改造 | 单任务串行注册表改为按节点多任务 |
| `relay_agent.py` | 改造 | 单循环改为 N worker，任务级取消与端口池 |
| `relay_api.py` | 改造 | 注册接口改为长期令牌，新增节点管理接口 |
| `relay_runtime.py` | 改造 | 作业会话：申请租约、装配 mover、释放租约 |
| `app.py` | 改造 | 作业收尾不再销毁节点；装配平台级资源层 |
| `templates/index.html` | 改造 | 新增"中转机资源"页，向导步骤 1 改为选择池 |

## 6. 资源模型

### 6.1 RelayNode

平台级常驻资源，一条记录代表一台中转机。

```
node_id            平台内唯一 ID（uuid4 hex，落盘后不变）
name               relay-<role>-<tenant_short>-<az>-<index>
role               source | target
tenant_key         auth_url|project_id（复用 _cloud_fingerprint）
az                 可用区
server_id          Nova 实例 ID
port_id            Neutron 端口 ID
network / subnet   网络与子网
fixed_ip           固定 IP（未指定时记录实际分配地址）
flavor / image     规格与镜像
system_volume_type 系统盘卷类型（可空）
slots_total        并发槽位总数，默认 5
slots_used         当前占用槽位数
agent_version      agent 上报版本
ssh_password_enc   root 密码密文（AES-GCM）
state              provisioning | ready | busy | draining | maintenance | unhealthy | deleting
last_seen          最近心跳时间
created_at / updated_at
```

状态流转：

```
provisioning → ready ⇄ busy
                  ↘ draining → ready
                  ↘ unhealthy → deleting → provisioning
                  ↘ maintenance → ready
                  ↘ deleting
```

`busy` 表示 `slots_used > 0`，`ready` 表示 `slots_used == 0` 且心跳正常。
`slots_used` 是调度依据，`state` 是健康与运维状态的投影，两者不允许互相推导。

### 6.2 RelayLease

作业对节点的占用记录，用于审计、缩容判定与孤儿识别。

```
lease_id           uuid4 hex
job_id             作业 ID
node_id            节点 ID
role               source | target
tenant_key         auth
vm_id / volume_id  该租约关联的迁移对象（VM 级槽位，volume_id 可空）
acquired_at        申请时间
released_at        释放时间，0 表示仍持有
```

### 6.3 持久化与平台重启恢复

| 文件 | 内容 | 权限 |
| --- | --- | --- |
| `uploads/relay-nodes.json` | 节点清单 | 0600 |
| `uploads/relay-leases.json` | 租约 | 0600 |
| `uploads/relay-pools.json` | 池级建机参数 | 0600 |
| `uploads/relay-credentials.json` | 加密云凭据 | 0600 |
| `uploads/relay-secret` | 平台签名密钥（32 字节） | 0600 |
| `uploads/relay-ledger-platform.json` | 卷任务台账（已有） | 0600 |

写入方式沿用 `relay_ledger.py` 的临时文件 + `os.replace` 原子替换模式。

平台重启恢复顺序：

1. 读取 `relay-secret`，不存在则生成并落盘；存在则直接使用，
   保证重启后旧令牌仍然有效；
2. 加载节点清单，把所有节点标记为 `provisioning`，清空 `slots_used`；
3. 加载租约，对 `job_id` 已不存在于 `job_manager` 的租约直接释放；
4. 启动 watchdog，节点收到 agent 心跳后回到 `ready`/`busy`；
5. 对超时未注册的节点执行一次节点维度对账。

## 7. 池作用域与租户隔离

池的唯一键是三元组：

```
pool_key = (tenant_key, role, az)
tenant_key = <auth_url>|<project_id 或 project_name>
```

- 不同租户不共享节点，即使底层是同一朵云；
- 源与目标角色不共享节点；
- 不同 AZ 不共享节点（Cinder 卷只能挂同 AZ 实例，这个约束不可绕过）；
- 池仍然按 (租户, 角色, AZ) 分组，但**配额按 (租户, 角色) 跨 AZ 汇总**：
  一个租户的一个角色在所有权 AZ 上的节点总数不超过 6 台。

`tenant_key` 直接复用 `app.py` 现有的 `_cloud_fingerprint(auth)`，避免另造一套标识。

## 8. 容量模型与自动扩缩容

### 8.1 槽位定义

- 1 台 VM 迁移占用 1 个槽位（源、目标各占 1 个）；
- 该 VM 的多块盘在同一槽位内**串行**拷贝，保证单机并发传输流数不超过槽位数；
- 默认 `slots_per_node = 5`，可配置；
- 槽位从 `acquire` 成功开始占用，到该 VM 的卷搬运全部结束（成功或失败）释放。

### 8.2 扩容

作业申请槽位时的决策顺序：

1. 池内存在 `state in {ready, busy}` 且 `slots_used < slots_total` 的节点 → 直接占用；
2. 不存在空闲槽位，且 `节点数 < max_nodes` → 先等待 `scale_up_wait_seconds`
   （默认 30s，给其他作业释放槽位的机会），仍无槽位则新建 1 台并等待注册；
3. 已达 `max_nodes` → 进入等待队列；
4. 等待超过 `queue_timeout_seconds`（默认 1800s）→ 该作业报错退出，不清空队列外资源。

`max_nodes` 默认 6，含义是**每租户、每角色跨 AZ 合计上限**。判定扩容时先按
`(tenant_key, role)` 汇总所有权 AZ 的现有节点数，再决定能否新建。

容量对照（`slots_per_node = 5`，`max_nodes = 6`）：

| 需要并发迁移的 VM 数 | 节点数 |
| --- | --- |
| 1 – 5 | 1 |
| 6 – 10 | 2 |
| 11 – 15 | 3 |
| 16 – 20 | 4 |
| 21 – 25 | 5 |
| 26 – 30 | 6 |
| > 30 | 排队等待，不新建节点 |

### 8.3 缩容

- 判定空闲：`slots_used == 0` 且不存在 `released_at == 0` 的租约；
- 连续空闲达到 `idle_scale_down_seconds`（默认 86400s = 24h）后回收；
- 每次回收一台，删除顺序为**由新到旧**，保留 `created_at` 最早的节点，
  即保留 agent 最稳定、镜像最旧已跑通的那台；
- 每个 (租户, 角色, AZ) 池的节点数不允许低于 `min_nodes`（默认 1）；
- 正在 `busy`、`draining`、有未释放租约的节点永不参与缩容。

### 8.4 手动操作

页面可显式扩容/缩容，手动操作不受 30s 等待与 24h 空闲门槛限制，但仍受
`min_nodes`/`max_nodes` 约束。手动删除节点必须满足 `slots_used == 0`，
否则先提示 drain。

### 8.5 跨 AZ 额度再平衡

额度跨 AZ 共享后会出现"额度被其他 AZ 的空闲节点占满"的情况：目标 AZ 需要新建
节点，但 `(tenant_key, role)` 已用满 6 台，而其他 AZ 存在长期空闲的节点。
此时不能简单排队，否则作业会一直卡住。扩容决策在 8.2 第 2 步之后增加一步：

1. 检查同租户同角色其他 AZ 是否存在满足 `slots_used == 0` 且无未释放租约的节点；
2. 存在则立即回收其中一台（不受 24h 空闲门槛限制，删除顺序仍为由新到旧，
   且不得使该 AZ 低于 `min_nodes`），释放 1 个额度；
3. 释放成功后重试新建节点；
4. 若每个 AZ 都已到 `min_nodes` 下限、额度仍不足，则退回 8.2 第 3 步排队。

再平衡只在**扩容受阻**时触发，不会在后台主动搬移或删除正在使用的节点。

## 9. 数据面并发改造

当前 agent 是"心跳 → 领一个任务 → 同步执行 → 循环"的单线程模型，目标端口固定
9200（`relay_agent.py`、`relay_pool.py`）。要支持单机 5 并发必须改：

1. **多 worker**：注册成功后启动 `slots_total` 个 worker 线程，每个线程独立
   执行"领任务 → 执行 → 上报"；主线程只负责心跳与重注册；
2. **端口池**：目标端监听端口为 `base_port + slot_index`，`slot_index ∈ [0, slots_total)`；
   平台在任务里下发 `listen_port`，agent 上报 `ready` 时附带实际监听端口；
   现有 `_free_port()` 只作为端口冲突时的兜底；
3. **任务级取消**：`RelayState` 的取消标志从 agent 级改为 task 级，
   `request_cancel(task_id)`；心跳与进度响应返回 `cancel_tasks: [task_id]`，
   agent 按任务停止对应传输，避免取消一个作业误伤同机其他作业；
4. **心跳载荷**：增加 `slots_used`、`running_tasks`、`slots_total`，
   平台据此维护节点占用视图；
5. **调度并发安全**：`RelayState.dispatch` 允许同一 agent 在
   `running_tasks < slots_total` 时继续领任务，仍按 `agent_id` 固定分发；
6. **限速**：`rate_limit_bytes_per_sec` 保持按流生效；节点级总带宽上限
   作为后续增强项，本期不做。

数据协议（帧格式、票据、CRC、`O_DIRECT`）不改。

## 10. 注册与常驻生命周期

### 10.1 平台签名密钥必须持久化

现状：`app.py` 启动时 `RELAY_STATE = RelayState(secret=os.urandom(32))`，
重启即换密钥，所有既有令牌立即失效。常驻节点的令牌必须是长期有效的，因此：

- 密钥优先取环境变量 `MIGRATION_RELAY_SECRET`（base64 或 hex，32 字节）；
- 未配置时读取 `uploads/relay-secret`，不存在则生成并落盘（0600）；
- 两种来源都不允许在每次启动时随机生成。

### 10.2 节点级长期令牌

令牌格式沿用 HMAC 结构，但把 `job_id` 替换为 `node_id`，并去掉过期时间：

```
issue_node_token(secret, node_id, role, tenant_key, az) -> "<node_id>|<role>|<tenant_key>|<az>|<sig>"
verify_node_token(secret, token) -> (node_id, role, tenant_key, az)
```

- 令牌在节点创建时签发，密文写入节点记录，不写日志；
- 支持轮换：`POST /api/relay/nodes/<id>/rotate-token` 生成新令牌，
  旧令牌立即失效，节点侧由平台重新下发 `agent.env` 并重启 agent；
- cloud-init 只注入平台地址、节点令牌、节点名、角色、租户键，不注入 `job_id`。

### 10.3 agent 自动重注册

现状 `relay_agent.main()` 只注册一次，心跳 404 后永久空转。改为：

1. 注册失败按指数退避重试（1s、2s、4s，上限 60s）；
2. 心跳或领任务返回 401/404（会话失效）→ 清空会话、重新注册；
3. 注册成功后上报 `agent_version` 与 `slots_total`，平台版本不匹配时拒绝注册
   并把原因写入节点记录。

### 10.4 版本漂移

常驻节点会长期存在，镜像内 agent 版本必然落后。本期只做可见性与手动重建：
节点卡片展示 `agent_version` 与平台期望版本的差异，提供"重建以升级"入口；
滚动自动升级不列入本期。

## 11. 云凭据与 SSH 密码

### 11.1 加密存储

新增 `relay_credentials.py`，使用 AES-GCM（`cryptography` 库）加密，主密钥来自
环境变量 `MIGRATION_SECRET_KEY`（base64 编码的 32 字节）。未配置主密钥时，
常驻模式拒绝启动，不做明文降级。

凭据按 `tenant_key` 保存：

```
auth_url / username / password / user_domain_name
project_id 或 project_name / project_domain_name
```

用途：节点管理页的扩容、删除、重建、drain、孤儿对账，全部用这份凭据调用
OpenStack，不再依赖作业表单。迁移作业本身仍可使用页面上当次填写的认证。

### 11.2 SSH 密码

- 建机时使用页面上填写的 root 密码；留空则由平台生成 16 位随机密码；
- 密码以 AES-GCM 密文写入节点记录；
- `GET /api/relay/nodes/<id>/password` 返回明文，页面点击"显示密码"后可见，
  每次调用写审计日志（时间、来源 IP、node_id）；
- VM 侧沿用现有 cloud-init：`ssh_pwauth: true`、`disable_root: false`、
  `chpasswd` 设置 root 密码，安全组需放通用户侧到中转机的 22 端口。

## 12. 调度与多作业共享

- 调度入口：`RelayScheduler.acquire(job_id, role, tenant_key, az, count)`
  返回租约列表，`release(lease_id)` 归还；
- 一台节点被多作业共享时，每个槽位独立，按 `slots_total` 限流；
- 任务仍按 `agent_id` 固定分发，保证卷挂在哪台机器上就由哪台执行；
- 队列按池先进先出，不做优先级；同一作业的多台 VM 不保证落在同一节点；
- 心跳超时或 `draining` 的节点不再分配新槽位，已有任务继续跑完或被判定失败。

## 13. 失败语义与对账

### 13.1 作业失败/取消

- 释放该作业的全部租约，`slots_used` 相应减少；
- 清理该作业的派生卷、目标卷与 attachment（沿用 `RelayReaper.reconcile_job`）；
- **不重建、不删除、不重启任何节点**；
- 节点保持原状态，其他作业可继续使用。

### 13.2 节点故障

| 场景 | 处理 |
| --- | --- |
| 心跳超时（默认 30s） | 标记 `unhealthy`，停止分配新槽位；其上任务判失败并按现有重试逻辑换机 |
| 节点 `unhealthy` 持续超过 10 分钟 | 自动 `rebuild`：删除并重建同名节点，旧令牌失效换新令牌 |
| 节点重建后 agent 未注册 | 标记 `provisioning` 超时告警，不再自动重试，等人工介入 |

### 13.3 节点维度对账

新增遍历每台节点实际 attachment 的对账：

- attachment 对应的卷不在活跃租约/卷台账中 → detach；
- 无租约的节点不参与缩容判定以外的任何删除；
- 对账在 watchdog 每轮执行，与现有作业维度 reaper 并存。

## 14. API 设计

新增（平台级，不依赖 job）：

```
GET    /api/relay/pools                       池汇总：租户/角色/AZ/节点数/槽位占用/空闲时长
POST   /api/relay/pools/scale                 手动扩缩容 {tenant_key, role, az, target_nodes}
GET    /api/relay/nodes                       节点列表（支持 tenant/role/az/state 过滤）
GET    /api/relay/nodes/<node_id>             节点详情
DELETE /api/relay/nodes/<node_id>             删除节点（要求 slots_used == 0）
POST   /api/relay/nodes/<node_id>/rebuild     重建节点
POST   /api/relay/nodes/<node_id>/drain       drain：跑完不再派新任务
POST   /api/relay/nodes/<node_id>/resume      恢复可调度
POST   /api/relay/nodes/<node_id>/rotate-token 轮换节点令牌
GET    /api/relay/nodes/<node_id>/password    显示 SSH 密码（审计）
GET    /api/relay/nodes/<node_id>/orphans     列出孤儿 attachment
GET    /api/relay/tenants                     已保存凭据的租户列表
PUT    /api/relay/tenants/<tenant_key>        保存/更新加密凭据
DELETE /api/relay/tenants/<tenant_key>        删除凭据（要求该租户无节点）
```

沿用并调整：

```
POST /api/relay/register          长期令牌校验，返回 session 与心跳参数
POST /api/relay/heartbeat         返回按任务的取消指令
GET  /api/relay/tasks/next        同一 agent 可在槽位未满时继续领任务
GET  /api/jobs/<id>/relay         返回该作业的租约与槽位占用，而非池快照
```

## 15. 页面设计

### 15.1 新增"中转机资源"页

**该页只做运维，不是迁移的前置步骤**（2026-09-11 修正：原设计把它当作池配置入口，
实际使用中"必须先配好才能迁移"无法接受）。日常迁移在向导步骤 1 填完参数提交即可，
平台会自动存凭据、写池参数、建机；本页用于：

- 顶部：租户选择、角色（源/目标）、AZ 过滤；池卡片显示"节点数 / 上限 6"、
  "已用槽位 / 总槽位"、"空闲倒计时"；
- 节点卡片：名称、角色、租户、AZ、IP、state、`slots_used/slots_total`、
  agent 版本、最近心跳；
- 操作按钮：扩容、缩容、删除、重建、drain、恢复、轮换令牌、显示 SSH 密码、
  查看孤儿 attachment；
- 租户凭据管理：新增/更新/删除加密凭据，页面只显示"已配置"，不回显密码。

### 15.2 迁移向导调整

- 步骤 1 的"中转机池"面板是**唯一填写入口**：选常驻模式后填写镜像、flavor、AZ、
  网络、子网、卷类型、单机槽位、节点数上下限、空闲缩容小时数；
- 提交作业时平台自动完成：按租户加密保存当次认证 → 首次创建该池时写入建机参数 →
  按 `min_nodes` 创建并等待 agent 注册 → 后续按槽位自动扩容；
- 池已存在时沿用原有建机参数，作业表单不覆盖，预检会明确提示；
- 步骤 3 的中转机视图改为"本作业租约视图"：显示本作业占用的节点与槽位，
  以及所属池的总体容量；
- 常驻模式缺少 `MIGRATION_SECRET_KEY` 时在预检阶段给出明确错误，不进入建机流程。

## 16. 配置项

| 配置 | 默认 | 位置 | 说明 |
| --- | --- | --- | --- |
| `relay_node_mode` | `persistent` | 向导/资源页 | `persistent` / `ephemeral` 回退开关 |
| `slots_per_node` | 5 | 资源页 | 单机并发 VM 槽位数 |
| `max_nodes` | 6 | 资源页 | 每租户每角色跨 AZ 合计上限 |
| `min_nodes` | 1 | 资源页 | 缩容底线，保留常驻 |
| `idle_scale_down_seconds` | 86400 | 资源页 | 空闲满 24h 回收 |
| `scale_up_wait_seconds` | 30 | 资源页 | 无空闲槽位先等待再扩容 |
| `queue_timeout_seconds` | 1800 | 资源页 | 池满时的排队上限 |
| `heartbeat_interval` / `heartbeat_timeout` | 10s / 30s | 资源页 | 改为平台级，不再由作业覆写 |
| `MIGRATION_RELAY_SECRET` | 无 | 环境变量 | 节点令牌签名密钥 |
| `MIGRATION_SECRET_KEY` | 无 | 环境变量 | 凭据 AES-GCM 主密钥 |

## 17. 测试策略

- 单元测试（mock OpenStack，沿用 `unittest`）：
  - 清单与租约的落盘/加载/原子替换；
  - 扩容判定：1–5 台 1 个节点、6–10 台 2 个节点、31 台排队；
  - 缩容判定：24h 门槛、保留 1 台、busy 节点不回收、由新到旧删除；
  - 长期令牌签发/校验/轮换与旧令牌失效；
  - 凭据加密往返、错误主密钥解密失败、未配置主密钥拒绝启动；
- agent 测试：多 worker 并发领取、端口池分配、任务级取消不误伤其他任务、
  心跳 404 后自动重注册；
- 集成测试：本地两个 agent 进程 + 普通文件模拟块设备，验证 5 并发下数据一致；
- 页面：路由与 API 冒烟测试，按 AGENTS.md 要求附截图；
- 全量回归：`python3 -m unittest discover -s tests`，重点确认 RBD 通道不受影响。

## 18. 风险与未支持场景

- **常驻节点占用配额**：6 台/角色/租户（跨 AZ 合计）会持续占用 vCPU、内存、端口与 IP，
  需要把节点数纳入容量告警；
- **长期凭据与长期令牌**：凭据加密与令牌轮换是硬要求，主密钥丢失会导致
  全部凭据不可解密、节点令牌失效，需要纳入备份；
- **故障域变大**：一台坏节点影响多个作业，`unhealthy` 隔离与自动重建必须可靠；
- **版本漂移**：本期只做可见性与手动重建，不做滚动自动升级；
- **单副本部署**：平台仍为单副本，节点与租约靠落盘恢复，运行中任务不续跑；
- **传输未加密**：跨云块流仍是明文 TCP，沿用旧设计的未支持声明；
- **管界与安全组**：SSH 密码登录要求用户侧到中转机 22 端口可达，需在部署说明
  中明确安全组要求。

## 19. 实施分期

1. **阶段 0（基础）**：持久化密钥、节点清单、租约、加密凭据；
2. **阶段 1（调度）**：池键、槽位、自动扩缩容、ephemeral 回退开关；
3. **阶段 2（数据面）**：agent 多 worker、端口池、任务级取消、自动重注册；
4. **阶段 3（页面与运维）**：中转机资源页、节点管理 API、SSH 密码展示、
   节点维度对账。

每个阶段完成后都必须跑全量回归，确保 RBD 通道与既有 relay 单机流程不回归。

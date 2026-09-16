# 方案驱动向导与环境档案设计

日期：2026-09-14
状态：待审阅
关联设计：
`2026-09-09-source-vm-list-selection-design.md`、
`2026-09-10-rbd-layout-aligned-incremental-migration-design.md`、
`2026-09-10-relay-vm-full-copy-migration-design.md`、
`2026-09-11-relay-persistent-node-pool-design.md`

## 1. 背景与目标

当前页面把三类互斥的配置塞进同一屏「环境配置」：OpenStack 凭据、Ceph
conf、增量参数、中转机池（30+ 字段）以及作业参数同时可见；「数据通道」
下拉埋在页面中部，中转机资源页作为第 4 个常驻导航项与迁移流程并列。
迁移清单每台 VM 一次性铺开目标规格、逐网卡映射、逐卷类型，缺少默认值
表达与批量操作。结果是用户"不知道哪些该填、哪些不用管"。

目标：

- 按迁移方案组织布局，每个方案只呈现其必需配置，其余折叠或隐藏；
- 中转机运维入口按需出现，不再与迁移主流程并列；
- 迁移清单改为"摘要 + 展开 + 批量 + 自动映射"，默认继承作业配置；
- 新增可复用的「环境档案」，凭据与 Ceph conf 加密落盘、一次配置多次引用。

## 2. 迁移方案模型（权威定义）

后端是两条正交的轴：

- 轴 A｜数据通道 `data_channel`：`rbd`（Ceph RBD 直连）/ `relay`（中转机）；
- 轴 B｜迁移模式 `mode`：`full`（全量）/ `incremental`（增量预拷贝，实验）。

增量只走 RBD 直连；中转机通道只支持全量（`migration_manager._migrate_vm_via_relay`
无增量路径）。组合后实际是 3 个有效方案：

| 方案 id | 名称 | data_channel | mode | 适用存储 |
| --- | --- | --- | --- | --- |
| `rbd_full` | RBD 直连·全量（默认） | `rbd` | `full` | Ceph |
| `rbd_incremental` | RBD 直连·增量预拷贝（实验） | `rbd` | `incremental` | Ceph |
| `relay_full` | 中转机·全量 | `relay` | `full` | 商业集中式存储 iSCSI/FC |

方案只决定前端可见性与默认值，提交时仍映射回现有 `data_channel` +
`mode` 表单字段，**后端协议不变**。

各方案必需配置：

| 配置 | rbd_full | rbd_incremental | relay_full |
| --- | --- | --- | --- |
| 源/目标 OpenStack 凭据 + 项目 UUID | ✓ | ✓ | ✓ |
| 源/目标 Ceph conf + pool 名 | ✓ | ✓ | ✗ |
| 目标镜像（作业级/逐台） | ✓ | ✓ | ✗ |
| 增量轮次/阈值/间隔 | ✗ | ✓ | ✗ |
| 中转机池参数 | ✗ | ✗ | ✓ |
| 逐台 AZ/flavor/网卡映射/卷类型 | ✓ | ✓ | ✓ |

依据：`app.py:627` 以通道决定 `require_ceph` / `require_target_image`；
`relay_runtime.parse_relay_options` 的 `required` 在常驻模式下只剩两侧 AZ。

## 3. 设计决策

1. 方案是前端概念，映射到现有表单字段，不新增作业 API 参数。
2. 环境档案是"作业输入的可选来源"：提交时档案字段被展开为现有 form 字段，
   显式表单值优先于档案值；无档案时行为与今天完全一致。
3. 密码与 Ceph conf 一律服务端 AES-GCM 加密，前端只显示"已配置"，不回显。
4. 中转机资源页从主导航移除，仅在 `relay_full` 方案下作为"中转机运维"入口
   出现；页面 Markup 与 API 全部保留。
5. 迁移清单默认折叠为摘要行，展开才显示明细；默认值来自作业级配置与自动映射。
6. 不引入前端框架，沿用现有 `templates/index.html` + 原生 JS 与既有 CSS 变量。

## 4. 第 1 部分：方案驱动向导

顶部新增方案选择区（3 张可选卡片，默认 `rbd_full`），保留 3 步导航
（环境配置 / 迁移清单 / 迁移监控）。选择方案后：

- 卡片下方显示一行"本方案需要填写：…"的摘要，直接从第 2 节矩阵渲染；
- 切换方案时提示"将重置方案专属字段"，但保留凭据与档案选择；
- 方案写入 `state.scheme`，并在提交时映射 `data_channel` / `mode`。

环境配置页改为分组 + 按方案显隐：

| 分组 | 默认态 | 可见条件 |
| --- | --- | --- |
| 环境档案（选择/保存/另存/删除） | 展开 | 始终 |
| OpenStack 凭据（源/目标） | 展开 | 始终 |
| Ceph 数据面（conf + pool） | 展开 | `rbd_*` |
| 增量参数 | 折叠 | `rbd_incremental` |
| 中转机池 | 折叠 | `relay_full` |
| 作业参数（并发/限速/卷类型/QoS） | 折叠 | 始终 |

「查询目标目录」「环境诊断」按钮保持页尾位置。方案切换不触发网络请求。

## 5. 第 2 部分：环境档案

### 5.1 数据模型

沿用 `json_store` 原子落盘与 dataclass 装载模式，新增模块
`environment_profiles.py`：

```
EnvironmentProfile:
    profile_id: str            # uuid4 hex
    name: str                  # 用户可见名称，唯一
    source: dict               # auth_url/project_name/username/user_domain_name/
                               #   project_domain_name/project_id/password(密文)
    target: dict               # 同上
    source_ceph_conf: str      # 密文；conf 文本，含 keyring
    target_ceph_conf: str      # 密文
    source_ceph_pool: str      # 默认 volumes
    target_ceph_pool: str
    target_volume_type: str    # 默认卷类型（可空）
    created_at / updated_at: float
```

- 落盘路径 `uploads/environment-profiles.json`，0600；
- 密文复用 `relay_credentials.Sealer`（AES-GCM，AAD 绑定 `profile_id:字段名`），
  主密钥沿用 `MIGRATION_SECRET_KEY` / `uploads/relay-master-key`，不新增必填环境变量；
- 读取列表接口绝不返回 `password` 与 conf 文本，只返回 `has_password` /
  `has_ceph_conf` 布尔。

### 5.2 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/profiles` | 列表（脱敏） |
| POST | `/api/profiles` | 新建/更新；`profile_id` 缺省为新建；密码/conf 留空表示沿用旧值 |
| DELETE | `/api/profiles/<profile_id>` | 删除 |

### 5.3 与作业提交的衔接

`POST /api/migrate` 新增可选表单字段 `profile_id`：

- `_create_job_and_files` 在 **没有** `source_ceph_conf_file` /
  `target_ceph_conf_file` 时，用档案中的 conf 文本写入作业目录，语义与原上传一致；
- 凭据字段（源/目标 auth）缺省时从档案取值，显式提交的字段优先；
- 未传 `profile_id` 时逻辑与今天完全一致，兼容 Excel 与脚本调用；
- 作业提交后档案被改动不影响已建作业：`_create_job_and_files` 已把 conf
  落到作业目录、凭据进 `options`，天然形成快照，无需额外版本表。

### 5.4 页面交互

环境配置页顶部「环境档案」区块：

- 下拉选择已有档案 → 填充凭据与 Ceph 摘要（密码显示占位"已配置"）；
- 「保存为新档案」/「保存」/「删除」；
- 未选择档案时保持手填，作业提交不强制要求档案。

## 6. 第 3 部分：迁移清单重构

### 6.1 摘要 + 展开

每台 VM 默认渲染为一行摘要：

```
VM 01  web-prod-01   [已就绪/待配置 2 项]      ▸ 展开
源 4C/8G/80G · 2 网卡 · 3 卷   目标 AZ1 · 镜像 ● · flavor ●
```

摘要字段：VM 名、就绪状态、源规格摘要、目标 AZ/镜像/Flavor、网卡映射进度、
卷类型来源。展开后显示：目标规格三个下拉、逐网卡映射、逐卷类型，以及
"本台通道（高级）"。

### 6.2 默认继承与自动映射

- 目标 AZ/镜像/Flavor：作业级新增可选默认值，逐台留空即继承，摘要里显示
  "继承"chip；显式选择后显示"已覆盖"。
- 网络：加载源网卡后按"子网 CIDR 相同"或"网络名相同"自动预选目标网络/子网；
  目标 IP 留空即复用源 IP（同网段）否则自动分配，沿用后端 `resolve_port_ip` 语义。
- 卷：默认"跟随统一配置"，仅偏离时展示逐卷下拉。

### 6.3 批量与校验

- 工具栏新增"批量应用到已选 VM"：目标 AZ/镜像/Flavor/网络映射一键铺开；
- 页尾汇总保留，但每张卡片头部显示红点与"待配置 N 项"，点击展开并定位；
- "开始迁移"保持禁用直到校验通过（沿用 `networkIssues`）。

## 7. 第 4 部分：中转机入口调整

- 主导航删除 `data-step="resources"`；`switchStep` 只处理 3 步；
- 仅当方案为 `relay_full` 时，环境配置页的中转机分组内出现「中转机运维」
  按钮，展开资源页内容（复用现有 `#relay-resource-page` Markup 与 API），
  并提供返回迁移向导入口；
- 临时池模式：只平铺 9 个必填项（平台地址、两侧镜像/flavor/AZ/系统盘类型），
  其余（网络/子网/IP/端口/心跳/空洞模式/整卷校验/槽位）收进「高级」折叠；
- 常驻池模式：作业表单只显示两侧 AZ，并提示"其余参数在「中转机运维」的池
  参数中维护"。

## 8. 改动清单

新增：

- `environment_profiles.py`
- `tests/test_environment_profiles.py`
- `docs/superpowers/specs/2026-09-14-scheme-driven-wizard-and-environment-profiles-design.md`

修改：

- `templates/index.html`：方案选择器、环境配置分组、迁移清单摘要/展开/批量、
  中转机入口、导航调整、样式补充
- `app.py`：`/api/profiles` 三个路由；`_create_job_and_files` 支持 `profile_id`
- `README.md`：方案矩阵与环境档案说明

不改：`migration_manager.py`、`relay_*` 运行时、Deployment 挂载。

## 9. 测试与验收

自动化：

- `environment_profiles` 单测：新建/更新/删除、脱敏列表、留空沿用旧密钥、
  密文损坏报错、未知字段忽略；
- `_create_job_and_files` 回归：无档案时行为不变；有档案且无上传文件时
  conf 正确落盘；显式字段覆盖档案；
- 现有全量：`python3 -m unittest discover -s tests -v`。

手工验收：

- 三个方案切换，确认可见配置与矩阵一致；
- 环境档案：保存 → 刷新页面 → 载入 → 提交，密码不回显；
- 迁移清单：加载 VM → 自动映射 → 批量应用 → 逐台覆盖 → 开始迁移；
- 中转机方案下「中转机运维」可达；其他方案不可见；
- 窄屏（≤900px）摘要行与展开布局不溢出。

## 10. 上线方式

改动集中在 `app.py` 与 `templates/index.html`，均为现有 ConfigMap/
Deployment 挂载内容；`environment_profiles.py` 随镜像或 ConfigMap 同步。
新增 `uploads/environment-profiles.json` 由应用首次写入，随 uploads 持久化。

## 11. 非目标

- 不做迁移模板（默认 AZ/镜像/Flavor/网络映射规则的可复用模板），留待下一期；
- 不做作业对档案的显式版本号与审计日志（当前用 conf 落盘快照满足可复现）；
- 不删除中转机后端能力与资源页 API；
- 不重做品牌视觉，沿用现有深色控制台风格与 IBM Plex 字体。

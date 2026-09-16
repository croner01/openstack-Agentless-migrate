# 源 VM 列表选择迁移设计

日期：2026-09-09
状态：已与需求方确认，待审阅

## 背景与目标

当前迁移必须上传 Excel（`vm_name` / `target_az` 必填）才能建立迁移清单。
目标改为：连接源环境后直接拉取当前源项目下的 VM 列表，勾选即可进入迁移
清单，不再必须准备 Excel。

Excel 入口保留，但收进“高级：批量导入”折叠区，主流程为列表勾选。

## 设计决策

- 源列表范围：页面当前选择的“源项目”下可访问的全部 VM。
- 目标 AZ / 镜像 / Flavor：全部从目标环境实际接口加载，逐台独立选择，不提供
  批量默认填充。
- Excel 与列表勾选最终进入同一套“迁移清单”状态与表单。
- VM 身份在列表模式下使用 `server_id`，Excel 模式优先用解析网络时获得的
  `server_id`，无法获得时回退为按名称查找。
- 不新增独立后端进程、数据库或 Python 模块；改动全部落在现有已挂载文件中。

## 第 1 部分：页面交互

Step 2 布局调整为：

1. 清单来源区
   - 主入口：按钮“从源项目加载 VM”。
   - 加载后展示源 VM 选择器：
     - 关键字搜索（按名称过滤）；
     - 分页；
     - 每行复选框、当前页全选；
     - 行内容：VM 名称、状态、源 AZ、源 flavor（名称 + vCPU/内存/磁盘）、
       源镜像。
   - “高级：Excel 批量导入”折叠区，展开后可上传解析。
2. 已选迁移清单
   - 勾选 VM 后生成现有清单卡片；
   - 每张卡片可选择目标 AZ（真实下拉）、目标镜像、目标 Flavor；
   - 每个源网卡独立选择目标网络/子网/IP；
   - 卡片右侧显示“待配置 / 已就绪”；
   - 显示来源标记（列表选择 / Excel 导入）；
   - 未启动任务的 VM 可在源列表中取消勾选并从清单移除。
3. 启动迁移
   - 列表模式提交 `selected_rows`（JSON）；
   - Excel 模式仍提交 `excel_file`；
   - 网络规划、认证、Ceph conf、并发/限速参数保持一致；
   - 逐台校验目标 AZ/镜像/Flavor/网络，未齐全不允许启动。

## 第 2 部分：后端接口与任务创建

### 新增 `POST /api/source-vms`

输入：源环境认证参数。

输出：

```json
{
  "ok": true,
  "servers": [
    {
      "server_id": "...",
      "name": "...",
      "status": "ACTIVE",
      "availability_zone": "...",
      "flavor": {
        "id": "...",
        "name": "...",
        "vcpus": 4,
        "ram": 8192,
        "disk": 80
      },
      "image_id": "...",
      "image_name": "..."
    }
  ],
  "page": 1,
  "has_more": true
}
```

实现要点：

- 服务端调用 `compute.servers()` 拉取当前项目范围的 VM；
- 支持 `search`、`limit`、`marker` 分页参数；
- flavor/image 仅对列表中的唯一 ID 批量补全，避免逐 VM 查询；
- AZ 取 server 扩展属性；取不到时返回空并由前端显示“未知”。

### 扩展 `POST /api/catalog`

响应增加 `availability_zones` 数组，数据来自目标计算环境真实可用区。
前端将目标 AZ 从手填改为下拉；Excel 提供的 AZ 文本不在真实列表中时，
提示不匹配且不允许启动。

### 任务创建统一数据流

内部统一为 selected rows：

```python
{
    "server_id": "uuid",
    "vm_name": "display-name",
    "target_az": "...",
    "target_image": "...",
    "target_flavor": "..."
}
```

- `/api/migrate` 接收 `selected_rows` JSON 或 `excel_file`；
- `excel_parser.MigrationRow` 增加可选 `source_server_id`；
- `state_machine.VmTask` 可在创建时写入 `source_server_id`；
- `MigrationManager` 若目标/源 VM 已有 `source_server_id`，直接按 ID 取源 VM，
  否则按名称查找；
- Ceph conf 上传与现有 options 结构不变；
- Job 状态文件沿用现有结构，重启恢复不受影响。

## 第 3 部分：错误与边界

- 勾选后 VM 被删除或状态异常：迁移器在取源 VM 阶段失败，只标记该 VM 失败，
  批次继续；
- 超大项目分页加载，前端不一次渲染全部 VM；
- 列表选择与 Excel 重复：按 `server_id`（无 ID 时按名称）去重；
- 已开始迁移的 VM 不允许从清单移除；
- 取消勾选不会自动删除已创建的目标资源；继续遵循现有“人工确认后清理”策略。

## 测试策略

- 单元测试：
  - `/api/source-vms` 的响应组装与分页；
  - flavor/image 批量补全去重；
  - selected rows 与 Excel 两种入口最终行结构一致；
  - AZ/镜像/Flavor 下拉数据来源与缺失提示；
  - `server_id` 存在时不再按名称查找。
- 回归测试：
  - Excel 高级入口；
  - 迁移监控与失败卷自动重试；
  - Job 重启恢复。
- 手工验收：
  - 加载源列表 → 搜索 → 勾选 → 目标配置 → 网络规划 → 启动；
  - 目标 AZ 不存在时的错误提示；
  - 大批量 VM 的分页与性能。

## 上线方式

改动文件均为现有 ConfigMap 挂载文件，不新增模块，不修改 Deployment 挂载：

- `openstack_utils.py`
- `app.py`
- `excel_parser.py`
- `state_machine.py`
- `migration_manager.py`
- `job_manager.py`
- `templates/index.html`

更新 ConfigMap 后重启 Deployment。

## 非目标

- 不做跨项目全量列表（后续如需可加项目切换）；
- 不自动选择目标规格；
- 不自动规划目标网络；
- 不做源 VM 自动关机提醒之外的新调度策略。

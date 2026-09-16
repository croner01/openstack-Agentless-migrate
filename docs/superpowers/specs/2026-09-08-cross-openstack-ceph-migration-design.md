# 跨 OpenStack / 跨 Ceph RBD 批量迁移改造设计

日期：2026-09-08

## 1. 背景与目标

现有代码（`app.py`、`migration_manager.py`、`openstack_utils.py`、`ceph_utils.py`）是一个跨 OpenStack 平台、跨 Ceph 存储集群的 VM 迁移原型，但存在以下问题：

- 当前流程是“先建目标 Cinder 卷 -> 替换 RBD -> 再建目标 VM”，与目标迁移流程“先建 BFV 空白 VM -> 关机 -> 替换 RBD -> 开机”不一致。
- 删除目标 RBD 发生在拷贝之前，没有校验、状态机和回滚信息。
- 多卷 VM 的卷顺序、系统盘 boot index 靠列表顺序猜测。
- 网络、flavor、安全组在目标端重建存在明确 bug（例如 IP 字符串拼接、用目标连接查源 VM）。
- 请求是同步 HTTP，长时间占用请求，页面无法获得任务级状态。
- 前端简陋，无法配置网络映射/目标镜像，无法展示逐 VM、逐卷进度。

本改造目标：把迁移流程重构为可由页面配置、可跟踪状态的 BFV 迁移流程，并同步重构前端。批量任务中单台 VM 失败不影响其余 VM。

## 2. 范围

### 范围之内

- 单台 VM 的完整迁移状态机。
- 目标 BFV VM 创建、源/目标关机、卷映射、RBD 替换、开机验证。
- 源固定 IP 保留、目标网络/子网映射。
- 目标镜像由页面指定；flavor 自动匹配并提供页面兜底选择。
- 多 VM 批量任务、失败跳过继续。
- 前端重构为专业、简洁的三步工作流。

### 范围之外（本期不做）

- 在线迁移（源 VM 运行中直接迁移）。
- 迁移后的源 VM 自动删除。
- Ceph RBD 快照历史/克隆关系迁移。
- 任务状态数据库持久化、多副本高可用（状态先存进程内内存）。
- 系统级认证、TLS、CSRF（作为后续安全专项，不在本次设计中展开）。
- 目标卷暂存映像短窗口替换（列为后续增强项，见第 10 节）。

## 3. 总体架构

继续运行在单个 Flask 进程内，但职责拆分：

| 模块 | 职责 |
| --- | --- |
| `app.py` | HTTP API、文件上传、任务创建、状态查询 |
| `job_manager.py`（新增） | 维护 Job/VM/卷状态，调度后台执行 |
| `migration_manager.py` | 单 VM 迁移状态机编排 |
| `openstack_utils.py` | 源侧查询、目标侧资源创建/启停、卷与网络映射 |
| `ceph_utils.py` | 单卷 RBD 替换原语 |
| `templates/index.html` | 单页三区工作台 |

## 4. 任务与状态模型

一次 Excel 提交生成一个 `Job`，内含多个 `VM Task`，每个 VM Task 内含卷状态。

### Job

- `id`
- `status`: `running` / `completed` / `failed`
- 各 VM 状态统计
- 创建时间、结束时间
- 上传文件目录

### VM Task

状态：

```text
queued
  -> preflight_failed        # 规格校验失败，跳过
  -> creating_target_bfv     # 创建端口、目标数据卷、BFV VM
  -> stopping_target         # 停目标 BFV VM 并等待 SHUTOFF
  -> stopping_source         # 停源 VM 并等待 SHUTOFF
  -> collecting_volumes      # 建立源/目标卷映射并校验
  -> copying_volumes         # 逐卷替换 RBD
  -> starting_target         # 仅所有卷成功才开机
  -> verifying               # 等待 ACTIVE 并做基础校验
  -> success
  -> failed                  # 保留目标资源供人工排查
```

顺序说明：先建好目标 BFV 并停掉，再停源 VM，尽量缩短源 VM 停机时间；但进入 `copying_volumes` 前源、目标都必须为 `SHUTOFF`。

批量失败策略：任一 VM 失败只标记该 VM 为 `failed`，Job 继续处理下一台；Job 级异常（配置错误、Excel 无法解析）才把整个 Job 标记为 `failed`。

失败语义：不自动删除本轮已创建的目标卷/端口/VM。任务记录失败阶段、卷级成功/失败明细、已创建资源清单，供人工清理。

## 5. 源侧数据采集

在开始创建目标资源前，从源 OpenStack 查询并缓存：

- VM 状态；
- flavor 规格：vCPU、内存、根磁盘、ephemeral、swap；
- 所有 volume attachment：卷 ID、设备名、bootable、大小、是否系统盘；
- 每个网络的固定 IP（保留全部固定 IP，IPv4/IPv6 都保留）；
- 镜像/元数据信息（仅用于日志与页面展示）。

系统盘判定：优先使用 `volume.is_bootable`；如果多块卷都是 bootable，结合源 attachment 的设备顺序（通常 `vda` 为系统盘）作为兜底。

## 6. 目标 BFV VM 创建

### 6.1 前置校验

- 源 VM 存在且可达；
- 目标 flavor 可匹配或用户已指定；
- 每个源网络都有页面提供的目标网络/子网映射；
- 源固定 IP 在对应目标子网内。

校验失败的 VM 进入 `preflight_failed`，不创建任何目标资源。

### 6.2 网络端口

按映射关系在目标子网创建 port，保留源固定 IP：

- 同一个源网络可对应一个目标网络/子网；
- 目标端口冲突（IP 被占用）时该 VM 失败，不启动；
- port 创建成功但后续步骤失败时，保留该 port 供人工清理。

端口按源 attachment 顺序创建；端口顺序必须与最终 BDM/VM 网络列表一致，避免固定 IP 与网卡错位。

### 6.3 数据卷

非系统盘在目标 Cinder 创建空白卷：

- 大小与源数据卷一致；
- 默认使用目标项目默认 volume_type，不写死 `hdd`；
- 如页面填写目标 volume_type，则按页面值创建；
- 卷名称建议使用可辨识格式，例如 `<源VM名>-<源卷名>-mig`，避免覆盖目标已有卷。

### 6.4 BFV VM

目标 VM 由脚本创建：

- 系统盘：通过 Nova BFV 从页面选择的目标镜像生成，`source_type=image`、`destination_type=volume`、`boot_index=0`；BDM 中指定 `volume_size=源系统盘大小`，避免目标卷大小与源不一致；
- 数据卷：按源设备顺序放入 BDM，作为后续 `vdb/vdc/...`；
- flavor：自动匹配源规格，匹配不到时该 VM 进入等待用户选择 flavor 的状态；
- 网络：使用 6.2 预创建的 port；
- 安全组：使用目标项目默认安全组；
- keypair：不复制；使用页面配置的默认密码；
- 可用区：使用 Excel 中该 VM 的 `target_az`；
- `create_server` 后轮询目标 VM，直到 `ACTIVE` 或明确 `ERROR`；`ACTIVE` 后执行 stop 并等待 `SHUTOFF`，然后进入 `stopping_source`；若创建即 `ERROR`，该 VM 失败并记录已建资源。

### 6.5 flavor 匹配

按源 flavor 的 vCPU、内存（RAM）、根磁盘规格在目标云匹配；匹配条件为三者相等。若目标存在多个同规格 flavor：

- 名称与源一致者优先；
- 其次选名称最接近者；
- 都不满足时进入“待选择 flavor”状态，页面展示候选列表，由用户选择后继续。

若目标没有任何同规格 flavor，VM 不自动失败，进入“待选择 flavor”状态。

## 7. 卷映射

目标 BFV VM 创建完成后，从目标 VM 的真实 attachment 反向读取目标卷 UUID，与源卷映射：

```text
源系统盘 <-> 目标 BFV 系统盘（volume-<target uuid>）
源数据盘按设备顺序 <-> 目标数据卷按设备顺序
```

映射建立后：

- 源侧 RBD：`<source_pool>/volume-<source volume id>`；
- 目标侧 RBD：`<target_pool>/volume-<target volume id>`；
- 执行 `rbd info` 确认两侧映像存在，并确认大小一致。

不再依赖“target_volumes 返回列表顺序”推断系统盘。

## 8. RBD 替换原语

`ceph_utils.replace_rbd_data(...)` 只负责单卷替换，签名大致为：

```text
replace_rbd_data(
    source_conf, source_pool, source_rbd_name,
    target_conf, target_pool, target_rbd_name
) -> ReplaceResult
```

执行步骤：

1. 确认源 VM、目标 VM 均已 `SHUTOFF`；
2. `rbd info` 源与目标，核对映像与大小；
3. `rbd status` 目标确认无 watcher，`rbd snap ls` 确认无快照；
4. `rbd rm` 删除目标空卷映像；
5. 以 `bash -o pipefail -c` 执行
   `rbd --conf <src> export <pool>/<src> - | rbd --conf <dst> import - <pool>/<dst>`；
6. 再次 `rbd info` 目标，核对大小与源一致，记录 features/order 等属性到日志；
7. 任一步失败返回失败结果，不进入后续卷、不开机。

命令参数不允许再出现未转义的用户输入拼接；pool、映像名只接受校验过的字符集（字母/数字/下划线/短横线），conf 路径由 Job 目录生成。

## 9. 任务接口与执行模型

### API

- `POST /api/migrate`：multipart 表单（Excel、OpenStack 凭据、Ceph conf/pool、并发、网络映射、目标镜像等）。立即返回 `job_id`。
- `GET /api/jobs/<job_id>`：返回 Job 汇总、VM 列表、卷明细、日志路径。
- `GET /api/jobs`：最近任务列表。
- `GET /api/preflight`：解析 Excel，校验 flavor/网络/IP，返回可编辑的任务预览。
- `GET /api/logs/<job_id>?vm=<name>`：返回指定 VM 的迁移日志。

### 执行模型

- 后台线程池执行 VM Task；
- 页面配置两个并发维度：`同时迁移 VM 数`、`单台 VM 内卷拷贝并发数`；
- 卷拷贝统一限制，避免当前“VM 并发 × 卷并发”嵌套放大；
- 状态保存在进程内内存，关键事件写入文件日志；
- 容器/进程重启后运行中任务丢失，但落盘日志保留（本期接受，作为已声明限制）。

## 10. 后续增强（不在本期实现）

- 目标暂存 RBD + 短窗口改名替换，替代“先 rm 再长时间 export/import”，失败可快速回滚；
- 使用 Ceph import-only live migration / export-diff 搬运数据，保留稀疏分配与快照历史；
- SQLite/数据库持久化任务状态；
- 独立 worker + 任务队列；
- 认证、TLS、密钥托管。

## 11. 前端设计

单页三区工作台，视觉要求：专业、简洁。

### 区域一：环境配置

- 源/目标 OpenStack 凭据；
- 源/目标 Ceph conf 与 pool；
- 默认密码；
- 两个并发输入；
- 必填项校验与错误提示。

### 区域二：清单与映射

- Excel 上传后先解析为表格；
- 每行一台 VM：VM 名称、目标 AZ、目标镜像下拉、flavor 匹配结果/手动选择、校验状态；
- 网络映射表：源网络 -> 目标网络/子网，可增删多行；
- 全部校验通过后才允许开始迁移。

### 区域三：迁移监控

- 整批汇总：总数/成功/失败/进行中；
- 每台 VM 卡片：状态、阶段、错误原因、已创建资源清单；
- VM 卡片展开后展示卷级进度；
- 日志只展示当前 VM，使用 `textContent` 渲染，修复现有 XSS 与 `response.json()` 不匹配问题；
- 失败提示明确“资源已保留，请人工清理”。

## 12. 测试策略

使用 Python 标准库 `unittest`，不引入额外测试依赖；外部 OpenStack/Ceph 通过注入 Fake/回调对象隔离。

覆盖：

- Excel 解析与行级校验；
- flavor 自动匹配；
- 源/目标卷映射与设备顺序；
- 网络映射与 IP 冲突；
- Job/VM/卷状态迁移；
- RBD 替换原语的命令生成与失败处理。

## 13. 关键假设

- 两个 OpenStack/Ceph 环境之间控制网络可达，迁移服务能同时访问源/目标 OpenStack API 与源/目标 Ceph；
- 源卷在 Cinder/Ceph 中的映像名遵循 `volume-<uuid>`，代码会在执行前用 `rbd info` 校验，不再无条件信任；
- 目标 BFV 从目标镜像创建后可被稳定 stop 到 `SHUTOFF`；
- 目标卷在 VM 关机后无 RBD watcher；
- 源 VM 允许由脚本执行 stop；
- 页面/接口中目标镜像、网络映射等配置按“一次批量迁移一组 VM”的粒度提供。

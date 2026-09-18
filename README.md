# OpenStack 跨云 RBD BFV 迁移

## 功能

在目标 OpenStack 云创建 BFV（Boot From Volume）VM：

1. 直接从源项目加载 VM 列表勾选，或通过高级入口解析 Excel
   （`vm_name`、`target_az` 必填；`target_image`、`target_flavor`、
   `start_target` 可选）；
2. 创建目标端口与数据卷，使用指定目标镜像创建 BFV VM；
3. 依次停止目标 VM 与源 VM；
4. 从源 Ceph 集群逐卷导出并导入到目标 BFV VM 的实际 RBD 卷；
5. 所有卷替换成功后按每台 VM 的「迁移后开机」开关决定是否启动目标 VM；
   单个 VM 失败不会影响批次内其他 VM；
6. 失败时不自动删除目标资源，任务状态保留已创建资源与错误原因。

「迁移后开机」逐 VM 生效（规划表里的复选框，或 Excel 的 `start_target` 列，
也支持工具条批量设置），**默认勾选 = 迁移完成后启动目标机**，与历史行为一致：

- `开机`（默认）：卷替换完成 → `os-start` → 等 `ACTIVE`；
- `不开机`：卷替换完成后目标机保持 `SHUTOFF`，需要时再手动开机。
  - RBD 直连通道的目标机在换卷期间本来就是关机状态，勾掉开关只是跳过
    `os-start`，不会多一次开关机动作；
  - 中转机通道由 Nova 建机时必然先开机（`create` 后即 `ACTIVE`），关掉
    开关会在建机完成后显式 `os-stop` 并等 `SHUTOFF`，因此目标机的系统盘
    会被真实引导一次。
  - 手动切换模式下「开始切换」仍然完成停源 → 末轮增量 → 换卷，但换卷后
    按该开关决定是否起机。

迁移模式为两个**相互独立**的功能项，逐 VM 选择：

- `全量拷贝`（默认）：一次性 `rbd export/import`，不创建快照；
- `增量预拷贝`（实验）：源 VM 运行期间分两阶段搬运，停机后只补最后一轮
  增量；增量参数（轮次/阈值/间隔/切换方式）在表单里配置。
  - 阶段一「基线全量」：先把**所有磁盘**各做一次 `rbd export/import` 全备，
    并在目标暂存卷上补建同名基线快照（`rbd import-diff` 的起点）；
  - 阶段二「增量同步」：按轮次**锁步**推进，每轮让每块盘各传一次
    `rbd export-diff/import-diff`，不做"单卷榨干 N 轮"。

  `切换方式` 支持两种：
  - `自动`：跑满 `delta_rounds` 轮或在增量小于阈值时收敛，自动停源 → 末轮
    增量 → 换入 → 起目标机；
  - `手动`：基线全备跑完**不自动做任何增量轮次**，该 VM 直接进入 `待切换`
    待命（源机照常运行）。用户在清单卡片上显式操作：
    - **同步一次**（`POST /api/jobs/<id>/vms/<name>/sync`）——只做一轮
      `export-diff/import-diff`，做完回到待命；点几次就几轮，
      `delta_rounds`/`delta_interval_seconds` 在手动模式下不生效；
    - **开始切换**（`POST /api/jobs/<id>/vms/<name>/cutover`）——停源 → 补齐
      最后一轮增量 → 换入 → 起目标机。
    因此停机窗口只取决于末轮增量大小，与等待时长无关，也不会在后台
    按间隔无脑堆快照。

模式在页面显式选择：配置区有作业级"迁移模式"下拉（全量拷贝 / 增量预拷贝），
清单卡片上可逐台覆盖。

作业详情页的 **调整参数重新提交** 会回放该作业提交时的向导参数（VM 清单、
目标 AZ/镜像/flavor、网络与卷映射、迁移模式、`start_target` 开关、通道与超时
等）。参数快照在提交时随表单一起落盘到 `uploads/job_params/<job_id>.json`，
所以刷新页面、换浏览器、甚至平台重启后都还能重新载入；口令类字段（
`*password` / `secret` / `token`）在写盘前就被剔除，重新载入后需要再次填写。
删除作业会一并删除该快照。

两条路径共用同一套生命周期逻辑，并共用布局对齐：建像参数
（`order`/`stripe_unit`/`stripe_count`/`features`）按源 RBD 对齐，换入前逐字段
校验，避免目标卷布局与源不一致导致迁移后业务无法启动。默认不自动降级；
两条路径不做运行中自动切换：增量失败只标记该 VM 失败并保留目标资源，要改走
全量请把该 VM 的模式改成"全量拷贝"后重新提交。

增量迁移会在源卷创建 `mig-<job_id>-<seq>` 临时快照，任务结束（成功或失败）
自动清理，服务启动与每个任务结束后回收孤儿快照；非 `mig-` 前缀的用户快照
不会被触碰。

中间快照的保留量是收敛的：源端每轮跑完只保留最新 2 条（`keep=2`，再早的
对后续 `export-diff` 已无意义），目标暂存卷只保留最新 1 条（`keep=1`，
`import-diff` 只需要"上一轮那个起始快照"）；不清的话每同步一轮都会在两端
各多堆一条快照，等间隔自动同步跑一夜就能堆出几十条。

迁移清单为每台 VM 独立选择目标 AZ、镜像、Flavor，以及每个源网卡对应的
目标网络/子网与目标 IP（留空自动分配/复用源 IP）；不再提供全局网络映射。
目标 AZ/镜像/Flavor 均来自目标环境真实数据（AZ 为下拉选择）。
Cinder 与 Nova 的可用区是两套命名空间：页面选的 AZ 只用于 Nova 建机，
所有云硬盘（数据盘、派生卷、中转机启动卷）统一落在 Cinder 的 `default-az`。
RBD 卷拷贝支持按单卷限速（MiB/s，0 不限），页面与日志实时显示进度百分比
和吞吐。

进度展示对全量与增量一致：每条卷都显示「阶段标签 · 百分比 · 速率」和进度条，
增量路径分三段上报——`基线全量`（按源卷容量）、`增量 i/N`（先对**本轮每块盘**
分别 `rbd diff` 预扫要传的字节数，再按该总量计算百分比）、`末轮增量`（停机后
补齐，同样预扫总量）。每段开始时百分比归零，避免拿一个小 diff 去比整卷容量而永远停在
0.x%；预扫失败时自动降级为按卷容量估算，不会中断迁移。

拷到 100% 之后仍有两段"看不见的尾巴"，现在都会报阶段名并**保持 100%**
而不是把进度条打回 0：`收尾：校验布局与基线快照`（全量/基线导入完成后的
`rbd info` + 补建 `mig-<job>-0` 基线快照）、`收尾：校验与换入`（末轮增量
后的布局校验与 `rename` 换入）。自动模式的轮次间隔同理，报
`等待下一轮（Ns）`——否则进度条会停在上一轮的 100% 干等，看起来像卡死。

中转机通道复用同一读法：`/api/jobs/<id>/relay` 的 `volumes[].progress_label`
把台账里的英文阶段（`copying`/`snapshotting`/…）翻成中文，作业详情的
「中转机通道」页签按 `VM / 卷 / 状态 / 进度 / 数据量` 成表，进度列直接复用
RBD 路径的 `progressBarHTML`（「百分比 · 速率」+ 进度条），阶段标签落成状态徽标，
字节数（已传/总量/跳过）单独成列；百分比只增不减（重试续传、总量被修正都不会
回退），轮询窗口过短时沿用上一次速率，避免高频轮询时数值抖动。

每台 VM 迁移了几块盘、成没成，三条通道（RBD 全量 / RBD 增量 / 中转机）都有：
作业详情的 **VM 列表**有一列「云盘」（形如 `3/4 · 失败 1 · 进行中 2`），
**「卷拷贝」页签**（RBD 路径，读 `vm.volumes`）与**「中转机通道」页签**
（读中转机的逐盘台账）各有一张按 VM 汇总的表
「云盘 / 完成 / 失败 / 进行中 / 合计容量」，下面再逐块列出源卷、目标卷、状态与
失败原因。增量的 `ready`（基像就绪、等切换）算「进行中」——它还没换入目标卷。

中转机的运行时在作业结束后会被回收（`/api/jobs/<id>/relay` 变成 `null`），
所以逐盘结果同时写进作业本身（`vm.relay_disks`，
`volume_id`/`target_volume_id`/`size`/`status`/`error`）：历史作业、平台重启后
都还能看到完成与失败的块数，不再依赖 7 天保留期的台账；作业结束后页签不会
再消失，只是实时字节进度换成留存的最终结果。

为避免多台 VM/多个数据盘并发时 cgroup OOM 杀掉某个 `rbd export/import`
（日志中 `rc=-9`），RBD 拷贝使用全服务共享的内存门控：限制同时运行的
拷贝对数，并在 Pod 内存接近水位时暂不放行新的拷贝；单个卷失败后自动重试
一次，重试会重新校验源/目标 RBD 并清理同名残留的 `-mig-stage` 暂存镜像；
失败/取消的卷还会由失败清理与启动 GC 兜底删除其暂存镜像（见「注意事项」）。

## 迁移方案

页面顶部显式选择方案，环境配置只呈现该方案必需的字段：

- `RBD 直连 · 全量`（默认）：需要源/目标 OpenStack 凭据、源/目标 Ceph conf
  与 pool、目标镜像，以及逐台 AZ/Flavor/网络规划；
- `RBD 直连 · 增量预拷贝`（实验）：在 RBD 全量之外增加增量轮次/阈值/间隔，
  并可选择 `自动` / `手动` 切换（手动模式点「开始切换」才停机）；
- `中转机 · 全量`：适用于 iSCSI/FC 商业集中式存储（源卷没有可寻址 RBD
  对象），不需要 Ceph conf；常驻池模式下作业表单填两侧中转机建机参数，
  首次提交即建池，之后的扩容/清理在「中转机资源」页完成（见下文）。

中转机通道的建机参数在向导第 ④ 步「中转机参数」里填；池建立之后作业表单
不再要求重填：点「载入两侧目录 / 检查常驻池」（或把节点模式切到「常驻」）会
调用 `/api/relay/catalog`，响应里的 `pools.source/target` 带回该云已有常驻池的
建机参数，前端据此按「可用区」预填镜像/flavor/网络/系统盘并收起建机参数，
只显示「沿用已有常驻池」。填了仍生效，但只影响扩容出来的新节点；要长期改池
参数请到「中转机资源」页。因此日常只用到「中转机资源」页的扩容与清理。

## 环境档案

环境配置页支持把源/目标 OpenStack 凭据与 Ceph conf 保存为命名档案
（`uploads/environment-profiles.json`，0600，密文落盘，与中转机共用主密钥）。
提交作业时选择档案即可复用；密码与 conf 不回显，留空表示沿用旧值。
不选档案时行为与旧版完全一致。迁移清单默认折叠为摘要行，支持按子网/网络名
自动映射目标网络、批量应用目标 AZ/镜像，以及逐台展开覆盖。

## 界面主题

控制台默认跟随操作系统外观（浅色底 + 白色卡片，深色为暗蓝面板）。
右上角按钮在浅色/深色之间快速切换，「平台设置 → 界面主题」提供
浅色 / 深色 / 跟随系统三张主题卡，选择写入 `localStorage.migrate-theme`
（取值 `light` / `dark` / `auto`）。两套配色都由 `<style>` 顶部的 CSS 变量
驱动：`:root` 是浅色调色板，`[data-theme="dark"]` 覆盖为深色，页面渲染前
在 `<head>` 内先行应用，避免刷新时闪白/闪黑；处于 `auto` 时会跟随
`prefers-color-scheme` 变化实时切换。状态色、边框、阴影等语义变量也成对
定义，新增样式请复用变量而不是写死颜色。

## 平台设置

「平台设置」页把本机偏好与平台生效参数放在一起：

- 界面主题：浅色 / 深色 / 跟随系统三张卡，点选立即生效并记住（见上节）。
- 访问令牌：写入 / 清除浏览器里的 `MIGRATION_API_TOKEN`，供 `/api/*` 鉴权；
  也可以用 `/?token=<令牌>` 访问一次自动记忆。
- 快捷操作：刷新作业列表、重新检测 API、打开新建迁移、管理环境档案。
- 运行信息与生效参数（只读）：健康检查、诊断版本、API 地址、部署形态，
  以及 `GET /api/runtime` 返回的 RBD 并发名额（含当前占用者与已占用时长）、
  内存放行阈值、`rbd` 命令超时、拷贝停滞判定、上传上限 / 保留天数、
  提交前 mon 探测开关。名额被占满时新任务日志会出现「等待 RBD 拷贝名额」，
  此页可直接看出是谁占着名额、占了多久，不必登录机器查环境变量。

## 运行

```bash
pip install -r requirements.txt
python app.py
```

页面访问 `http://<host>:19099/`。生产环境建议使用 gunicorn 等 WSGI 服务，并保持单副本以便 JobManager 状态可见。

## Pod 删除 / 滚动更新

- 应用注册 SIGTERM/SIGINT：收到信号后停止接收新任务，等待正在进行的
  RBD 卷拷贝收尾（默认宽限 60s，可用环境变量 `MIGRATION_SHUTDOWN_GRACE_SECONDS`
  调整，部署里与 `terminationGracePeriodSeconds: 600` 配套）；
- 单卷替换采用“暂存镜像导入 → 大小校验 → rename 换入”，即使被 SIGKILL，
  目标卷也不会停在“已删除/写一半”的中间态；残留暂存镜像会在下次运行自动清理；
- rbd export/import 不再通过 `/bin/sh` 管道（容器基础镜像的 dash 不支持
  `set -o pipefail`），改为 Python 内存流式转发，支持限速与进度回调，并对
  export/import 分别记录 rc 与 stderr，失败原因可直接从日志定位；
- Job/VM 状态定期快照到 `uploads/jobs_state.json`（uploads 为持久卷），
  容器重启后页面仍能看到已跑到哪一步；
- 若任务被停机中断，已建资源保留、未跑 VM 会标记为失败，可人工核对后
  重新发起剩余 VM 的迁移。

## RBD 拷贝内存门控参数（环境变量）

- `MIGRATION_MAX_RBD_COPIES`：全服务同时运行的 RBD 拷贝对数，默认 2；
- `MIGRATION_MEMORY_HIGH_WATER`：放行阈值，默认 0.85（达到 limit 的 85% 后
  不再放行新的拷贝）；
- `MIGRATION_COPY_RESERVE_MB`：为每个新拷贝预留的内存（MiB），默认 1024。

以上值根据 Pod 的 `memory.limit` 和实际 rbd RSS 日志调整；拷贝过程中会每
10 秒输出 exporter/importer 的 RSS 与当前内存使用量。

### 卡死保护（环境变量）

ceph 客户端连不上 mon 时不会立刻报错，而是无限重连——一条 `rbd info` 就能挂
几十分钟并一直占着拷贝名额，导致后面所有任务都在排队。因此：

- `MIGRATION_RBD_CMD_TIMEOUT_SECONDS`：单条 `rbd` 控制命令（info/ls/snap/rm）
  的默认超时，默认 120 秒，`0` 表示不限制；超时按 ETIMEDOUT(rc=110) 上报，
  走既有失败路径而不是把线程挂住；
- `MIGRATION_COPY_STALL_SECONDS`：拷贝管道允许的"零字节"时长，默认 300 秒，
  `0` 表示关闭看门狗；超时会杀掉 `rbd export`/`rbd import` 并把卷标为失败，
  错误文案直接写明"拷贝停滞超过 Ns（可能是 mon/OSD 不可达）"；
- `MIGRATION_CEPH_PREFLIGHT`：提交迁移前是否探测 conf 里的 `mon_host` 端口，
  默认 `on`；设为 `off`/`0`/`false`/`no` 可跳过。拦下时返回 400 并说明哪个
  地址连不上，避免"提交成功但永远卡在拷贝"。
- `MIGRATION_SNAPSHOT_READY_TIMEOUT` / `MIGRATION_VOLUME_READY_TIMEOUT` /
  `MIGRATION_DERIVE_SECONDS_PER_GIB`：中转机通道「快照 → 派生卷」的等待上限
  基线（默认 `0` = 不限时），见下面的「快照 / 派生卷等待超时」；
- `MIGRATION_ATTACH_READY_TIMEOUT`：挂载 / 卸载卷的等待上限，默认 1800 秒。
  与建卷不同，Cinder 卡在 `attaching` 不会自己往前走（多半是宿主机连不上
  存储后端），保留可预期上限后走卸载回滚比无限等待更安全；
- `MIGRATION_RELAY_RESULT_TIMEOUT`：中转机通道**单卷数据面拷贝**的墙钟上限，
  默认 `0` = 不限时（防卡死靠 `MIGRATION_COPY_STALL_SECONDS` 的字节进度
  看门狗：拷贝停滞才算失败，只是慢不算）。

排查"为什么一直排队"时先看日志里这一行，它会直接列出占用者：

```
[MIGRATION] 等待 RBD 拷贝名额/内存余量: active=2/2 current=113MiB
limit=16384MiB 已等待 322s 占用中: ren-跳板机/volume-xxx(1383s)
```

## 快照 / 派生卷等待超时（中转机通道）

中转机通道要先在源端「打快照 → 从快照派生一份拷贝源卷」，这两步在 Ceph 这类
CoW 后端上是秒级，但换到商业存储后往往退化成**存储侧全量拷贝**，耗时与卷容量
成正比：实测一块 **200GiB** 盘在这一步超过 1 小时，1TiB 盘几小时起步。此前
按固定时间判死的后果是"云上还在正常建盘，平台已经把任务标成失败并回收资源"，
重试还得从头再等一遍。

因此默认策略是**不限时**：

- 快照与派生卷的等待上限默认都是 `0` = 不限时，等待期间每 60 秒打一条
  `[MIGRATION] 快照 ... 已等待 Ns` / `[MIGRATION] 卷 ... 已等待 Ns`，
  作业详情的「中转机」页签同时显示每个卷的当前阶段与「已等待 Xm Ys」；
  只要云侧状态还在 `creating` 就继续等，只有 `error*` 才立刻失败；
- 任务取消 / 平台停机信号会立刻打断等待（走 `should_stop` 回调），
  不会留下挂着轮询的后台线程；
- 需要兜底上限时用环境变量 `MIGRATION_SNAPSHOT_READY_TIMEOUT` /
  `MIGRATION_VOLUME_READY_TIMEOUT`，实际等待上限变成
  `max(基线, 卷大小GiB × MIGRATION_DERIVE_SECONDS_PER_GIB)`（最后一项默认
  20，约合 50MB/s 的全量拷贝估算）。给正数基线后：40GiB 卷 800s、1TiB 卷
  约 5.7 小时；小于基线的卷仍按基线等。

页面「中转机配置」里的 **卷/快照就绪超时 (秒)** 是作业级覆盖：`0`（默认）
表示不限制等待，填正数则快照与派生卷都按这个值等（上限 7 天，且仍会按上面
的公式随卷大小放宽）。同一区域的另两个参数也在这里：**停滞判死 (秒)**
（数据面零字节流动多久算卡死，对应 `relay_stall_timeout`）与
**等待空闲槽位 (秒)**（池里没有空机器的等待上限，对应
`relay_slot_wait_seconds`）。

**何时该配基线**：不限时意味着一个"存储侧真的卡住"的派生卷会让这台 VM 一直
等下去。如果希望"宁可失败也别等"，给上面两个环境变量一个明确的估算上限；
运维侧的观察手段是日志里每 60 秒的打点行与作业详情页的「已等待」时长。

长等待期间台账会跟着 60 秒打点刷新时间戳（页面上的「已等待」也因此持续更新），
这样对账清理不会把"正在派生"的卷当成残留删掉；对账窗口另有一个 1 小时的下限，
不会被不限时配置带成 0。挂载 / 卸载步骤仍在
`MIGRATION_ATTACH_READY_TIMEOUT`（默认 1800 秒）内失败回滚。

### 对账的安全边界（谁在删卷）

对账（`relay-watchdog` 每 60 秒一次 + 页面「立即对账清理」）只回收**没有活作业
认领**的台账记录：作业还在本进程运行时，它的卷记录一律跳过，哪怕几小时没刷新
时间戳——同一 VM 的盘是串行传的，排在队里的卷要等前一块拷完才轮到，按"静默
时长"判残留会把在用的派生卷删掉，轮到挂载时就会报
`404 ... Volume <id> could not be found`。判断不出活跃作业列表时对账整体跳过
（宁可多留一小时残留，也不误删在用卷）。「立即对账清理」在作业运行中会被后端
以 409 拒绝、页面上按钮也是禁用的；作业收尾走的 `reconcile_job` 不受此限制，
它就是来清自己作业的。真要回收时日志会留下
`[MIGRATION] 对账回收孤儿卷 job=... volume=... derived=... 静默 Ns`，云上少盘可
直接对日志。

等待期间后端每 60 秒打一条 `[MIGRATION] 快照 ... 已等待 Ns` /
`[MIGRATION] 卷 ... 已等待 Ns`；作业详情的「中转机」页签也会显示每个卷
当前阶段与「已等待 Xm Ys」，打快照/派生卷阶段不再因为没有字节流动而显示
「暂无在途卷」。

### 单卷数据面拷贝的墙钟上限

拷贝阶段（挂载 → 传输 → 校验 → 清理）真正判断"卡死"的依据是字节进度：超过
`MIGRATION_COPY_STALL_SECONDS`（页面「停滞判死」，默认 300 秒）没有新字节才
失败。墙钟上限 `MIGRATION_RELAY_RESULT_TIMEOUT` 默认 `0` = 不限时，这样
1TiB 盘在 50MB/s 下约 5.8 小时、或者业务 VM 整体超过 6 小时都不会被误判失败。
校验、等待 agent 监听这类没有字节信号的子步骤另有 6 小时兜底，不受不限时影响。

### 多盘 VM 的快照/派生并发

单卷流程拆成了「准备（打快照 → 派生拷贝源 → 建目标空白卷）」与「传输」
两段：准备段只等存储侧拷贝、不占数据面带宽，因此按**每台 VM 的盘**并发
执行，多盘 VM 的等待时间不再随盘数线性叠加。并发度沿用页面上的
**单台卷拷贝并发**（`volume_concurrency`，默认 1 = 与原来的串行行为一致），
RBD 直连通道的语义不变。

- 调大后同时压在 Cinder 上的快照/派生数量会成倍增加，注意 `gigabytes`
  配额与存储侧并发上限；
- 一块盘准备失败时，同批已准备好的派生卷与快照会被立即回收（台账里标记
  `failed`），不会留下占配额的半成品；正在跑的准备任务会先结束再抛错，
  避免后台线程继续建资源；
- 多线程写台账（准备阶段）已加锁，页面轮询读取不会读到写一半的 JSON。

## 中转机空洞跳过（sparse copy）

中转机通道默认开启空洞跳过：源端按 4 MiB 块判零，全零块只发一个
`RLYH` 空洞帧（不占带宽），目标端按"空洞模式"落地。空盘占比高的迁移场景
传输量可下降一个数量级，目标存储也不会再被写入大段零数据。

- 页面"空洞模式"可选 `skip`（默认，最快，依赖目标卷是本次新建的空白卷）、
  `zero`（目标端 `BLKZEROOUT` 写零，对已有数据的卷也安全）、`off`（关闭）；
- `MIGRATION_RELAY_HOLE_MODE=off`：平台级强制关闭，覆盖所有作业表单值；
- 只有源、目标两端 agent 版本均 ≥ 1.1.0 才会启用；任一端是 1.0.0 时自动
  回退为整盘搬移，不会因为新帧导致作业失败；
- 升级方式：新作业按新镜像创建中转机即自动使用 1.1.0；常驻节点需要
  在资源页"重建"一次，或用新参数扩容出的新节点；
- 校验：`relay_full_verify`（整盘 sha256）读的是逻辑内容，跳过区域读出应为
  全零，因此开启后可直接证明没有漏写；
- 判零需要先读出来，因此源端读路径本身是瓶颈时耗时下降幅度小于传输量
  下降幅度；页面会同时显示"已处理 / 已发送 / 跳过"字节便于定位。

## 上传治理参数

- `MIGRATION_MAX_UPLOAD_MB`：单次请求上传上限（MB），默认 20，超限返回 413；
- `MIGRATION_UPLOAD_RETENTION_DAYS`：任务上传目录保留天数，默认 7，仅清理
  已结束/失败且早于保留期的任务目录，不删除 `jobs_state.json`。

源/目标 Ceph conf 与 Excel 保存后收紧为 `0600`；运行中任务的上传目录不会被
清理。同一源网络下多个固定 IP 会逐 IP 创建目标端口；行/网络覆盖以
`server_id` 为准（无 ID 时回退 vm_name）。

## 访问控制与密码参数

- `MIGRATION_API_TOKEN`：启用后，浏览器/管理类 `/api/*` 必须携带该令牌
  （`X-API-Token` 头、`Authorization: Bearer`，或 `?token=`），否则返回 401。
  页面首次可访问 `/?token=<令牌>` 自动记住（存入 localStorage）；留空则
  `/api/*` 无鉴权，启动日志会明确告警，建议在入口层或反向代理启用认证。
  agent 的 `/api/relay/*` 数据面使用自带签名令牌，不受该开关影响。
- `MIGRATION_DEFAULT_VM_PASSWORD`：迁移目标 VM 的默认管理员密码；未设置时按
  进程随机生成，不再使用内置弱口令。
- `MIGRATION_DELETE_SNAPSHOT_BUDGET_SECONDS`：删除任务前回收其增量迁移快照的
  时间预算（秒），默认 60；超预算的部分交给下一轮 GC。

## API

- `POST /api/migrate`：提交 OpenStack 凭据、Ceph conf、并发、单卷限速与
  每台 VM 的网卡规划；VM 清单支持 `selected_rows` JSON（列表勾选）或
  `excel_file`（高级导入），逐台 VM 的覆盖项（`row_overrides`）里可带
  `start_target` 控制迁移后是否开机（缺省 true），返回 `job_id`；
- `GET /api/jobs/<job_id>`：任务状态（完整 VM/卷明细）；
- `GET /api/jobs/<job_id>/diagnose`：只读卡点诊断。一次返回每台 VM 的
  当前阶段与停留时长、逐盘完成/失败计数、RBD 拷贝名额占用者、中转机节点与
  在途卷台账（各阶段已等待多久），以及与该作业相关的日志尾部和服务最近日志；
  `hints` 字段把“在排队等名额 / 卡在打快照派生 / 中转机异常”等结论直接写成
  人读的句子。作业详情的「诊断卡点」按钮就是这个接口的渲染结果；
- `GET /api/jobs`：作业列表，只返回摘要（`vm_count` + `vm_status_counts`），
  避免历史作业一多列表响应体线性膨胀；需要完整展开时用 `?full=1`；
- `POST /api/preview`：上传 Excel 返回行预览；
- `POST /api/projects`：使用当前凭据列出账号可访问项目（name + domain + UUID）；
- `POST /api/diag`：目标环境诊断（连接作用域、卷/计算接口探测、可手执行的
  openstack 命令）；
- `POST /api/source-vms`：列出当前源项目下 VM（search/limit/marker）；
- `POST /api/source-vm-networks`：批量返回勾选 VM 的网卡/卷摘要。清单较大时
  走「整体拉取一次再按 `device_id` 分组」，请求次数与 VM 台数无关；台数少时
  退回逐台定向查询，单台失败只影响该台；
- `POST /api/catalog`：列出目标云 images/flavors/networks/subnets/availability_zones；
- `GET /healthz`：健康检查。

页面“环境诊断”按钮会给出与迁移相同的连接作用域信息；设置环境变量
`MIGRATION_HTTP_DEBUG=1` 并重启后可输出 openstacksdk 的 HTTP 请求/响应日志。
需要复现“数据卷/系统盘配额”问题时，可在容器内直接运行：

```bash
python repro_create.py --auth-url <目标keystone> --username admin \
  --password '<密码>' --project-id <目标项目UUID> \
  --image <目标镜像ID> --flavor <目标flavor ID> \
  --network <网络ID> --subnet <子网ID> --az <AZ> --volume-type <卷类型，与页面一致>
```

其中 `--az` 只作用于 BFV 建机（Nova），数据卷固定建在 Cinder 的 `default-az`。

服务代码打包在镜像里（Deployment `openstack-vm-migration-deployment` 只挂
`/app/uploads`），更新后需要重新构建镜像、推送并滚动重启。构建走
`.dockerignore` 裁剪上下文：`uploads/`（含 `relay-master-key` /
`relay-secret` 主密钥与租户凭据密文）、`__pycache__/`、`tests/`、`docs/`、
`deploy/` 都不会进镜像，请勿删除该文件。镜像同时支持
**amd64 与 arm64**，两种架构共用一个 tag（多架构 manifest），ARM 集群与
x86 集群可以拉同一个 tag：

```bash
# 方式一：buildx 容器驱动一次出两个平台（推荐，构建机装了 binfmt 即可）
docker buildx create --name multiarch --driver docker-container --use
docker buildx build --platform linux/amd64,linux/arm64 \
  --build-arg CEPH_DEB_REPO=https://mirrors.tuna.tsinghua.edu.cn/ceph/debian-nautilus \
  --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
  -t hub.ecns.io/migration/openstack-vm-migration:v1.6.1 --push .

# 方式二：默认 docker 驱动不支持多平台导出（报 Multi-platform build is not
# supported for the docker driver），改成逐平台推送再用 manifest 合并；自签
# 证书的 registry 要带 --insecure
R=hub.ecns.io/migration/openstack-vm-migration
for p in amd64 arm64; do
  docker buildx build --platform linux/$p \
    --build-arg CEPH_DEB_REPO=https://mirrors.tuna.tsinghua.edu.cn/ceph/debian-nautilus \
    --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
    -t $R:v1.6.1-$p --push .
done
docker manifest create --insecure --amend $R:v1.6.1 $R:v1.6.1-amd64 $R:v1.6.1-arm64
docker manifest push --insecure $R:v1.6.1

# 只在本机构建（单架构）时仍可用 docker build
docker build -t hub.ecns.io/migration/openstack-vm-migration:v1.6.1 .

kubectl -n migrate set image deployment/openstack-vm-migration-deployment \
  openstack-vm-migration-container=hub.ecns.io/migration/openstack-vm-migration:v1.6.1
kubectl -n migrate rollout status deployment/openstack-vm-migration-deployment --timeout=180s
```

跨架构构建的前置条件：构建机需要注册 binfmt 才能跑 arm64 的构建步骤
（`uname -m` 就是 arm64 的构建机不需要）：

```bash
docker run --privileged --rm tonistiigi/binfmt --install arm64
```

Dockerfile 按 buildx 注入的 `TARGETARCH` 选择 bionic 的 amd64/arm64 ceph 包与
对应的多架构库目录（`x86_64-linux-gnu` / `aarch64-linux-gnu`），两套 SHA256
都固定；老式构建器没有 `TARGETARCH` 时退回 `dpkg --print-architecture`，
因此 `docker build` 仍然可用。只提供 amd64/arm64 两种架构，
`--platform linux/riscv64` 之类不支持的架构会在构建早期直接报错退出。

镜像把数据面的 `rbd` 固定为 Ceph **Nautilus 14.2.22**，与源/目标集群同代。
不能用 bookworm 自带的 `ceph-common`（16.2 Pacific）：Pacific 客户端解析 Nautilus
的 OSDMap 增量会命中
`./src/osd/OSDMap.cc: FAILED ceph_assert(q != removed_snaps_queue.end())` 并直接
SIGABRT，表现为增量迁移时 `export-diff` 与 `import-diff` 在同一秒一起崩溃。
构建按 SHA256 固定下载 deb，慢链路可用镜像站覆盖（ceph 包与 pip 依赖各一个
build arg，默认分别是官方源 `download.ceph.com` 与 `pypi.org`）：

```bash
docker build \
  --build-arg CEPH_DEB_REPO=https://mirrors.tuna.tsinghua.edu.cn/ceph/debian-nautilus \
  --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple \
  -t hub.ecns.io/migration/openstack-vm-migration:v1.6.1 .
```

发布后确认客户端版本（应为 `ceph version 14.2.22 ... nautilus`），两个架构
都要看（`--platform` 缺省取本机架构）：

```bash
kubectl -n migrate exec deploy/openstack-vm-migration-deployment -- rbd --version
docker run --rm --platform linux/arm64 \
  hub.ecns.io/migration/openstack-vm-migration:v1.6.1 rbd --version
docker manifest inspect --insecure \
  hub.ecns.io/migration/openstack-vm-migration:v1.6.1 | grep architecture
```

镜像内不含 `tests/`、`docs/` 与 `uploads/`（见 `.dockerignore`），`uploads/`
是 hostPath，重启不会丢作业状态与中转机台账。

### 常驻中转机（persistent 模式）额外要求

常驻节点池需要主密钥来加密云凭据。**不配置也能跑**：平台会在首次使用时
自动生成 `uploads/relay-master-key`（0600）并一直复用。生产环境建议改为
注入环境变量，把密钥与密文分开存放：

```bash
kubectl -n migrate create secret generic migrate-relay-secrets \
  --from-literal=relay-secret="$(head -c 32 /dev/urandom | base64)" \
  --from-literal=master-key="$(head -c 32 /dev/urandom | base64)" \
  --dry-run=client -o yaml | kubectl apply -f -
```

Deployment 注入（两个值都必须是 32 字节的 base64/hex）：

```yaml
env:
  - name: MIGRATION_RELAY_SECRET
    valueFrom:
      secretKeyRef:
        name: migrate-relay-secrets
        key: relay-secret
  - name: MIGRATION_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: migrate-relay-secrets
        key: master-key
  # 可选：中转机回连平台地址，缺省时用「中转机资源」页里的池参数
  - name: MIGRATION_RELAY_PLATFORM_URL
    value: "https://migrate.example.com:19099"
```

`MIGRATION_SECRET_KEY` 丢失会导致全部云凭据不可解密、节点令牌校验失败，
必须纳入备份；同理 `uploads/relay-master-key`（自动生成时）要随 uploads
一起持久化。`MIGRATION_RELAY_SECRET` / `uploads/relay-secret` 是节点令牌
签名密钥，规则完全一样：未配置时首次启动自动生成 0600 文件。

建机失败的处置规则（避免中途卡死与资源堆积）：

- 中转机起来后 cloud-init 会带节点令牌请求 `/api/relay/bootstrap`。该接口
  必须返回 200；返回 401 说明平台侧令牌校验没通过（例如清单/凭据没装配
  上），此时 agent 装不上、节点永远不会注册。
- 作业提交时的池预热：建机后等待 agent 注册，注册不上会删除这台机器并重试
  （默认 2 次，`ensure_pool(attempts=...)`），仍失败则**作业直接失败**，
  不会继续关机、建卷、拷贝。
- 作业中途扩容：同一规则，`SchedulerConfig.node_create_attempts`（默认 2）
  控制重试次数，失败的中转机立即从清单和云上删除，不占用建机额度。
- 巡检会回收 `provisioning` 且从未注册成功、创建超过
  `MIGRATION_RELAY_REBUILD_SECONDS`（默认 600s）的残留节点。

### 中转机资源页

入口在侧边栏「配置 / 中转机资源」。日常迁移不需要进这一页：提交一次
`中转机 · 全量`（常驻模式）作业就会自动加密保存租户凭据、写入池建机参数并
创建节点，之后按槽位自动扩缩容。这一页只有两个动作，且都不需要手填参数——
池与节点的身份、AZ、槽位/节点数、底线全部由 `/api/relay/pools` 与
`/api/relay/nodes` 带出，租户 key（`<auth_url>|<project>`）在页面上只显示成
短名（如 `opsl2`）：

- **扩容**：池行点「＋ 扩容 1 台」，按该池已保存的建机参数新建一台并等它注册
  （节点数到 `max_nodes` 时按钮自动禁用）；
- **清理**：池行点「－ 缩容 1 台」删掉池里最新创建的空闲节点；个别异常节点
  可在节点行点「删除」（硬删云主机 + 启动卷 + 浮动 IP + 端口）。在途任务没
  结束时会返回 409「节点仍有在途任务，请先 drain」，等任务收尾再试。

缩容到池底线（`min_nodes`）以下时页面先弹确认，再自动把底线一并降到目标值
（`PUT /api/relay/pools/profile` → `POST /api/relay/pools/scale`），所以不需要
手工填 `min_nodes`。注意底线只决定"巡检不再自动重建"：作业表单里的
「常驻节点数（缩容底线）」仍决定提交常驻作业时的预热台数，要彻底不再重建就
把它也填 0。要连同建机参数/租户凭据一起清掉，仍需走 API：
`DELETE /api/relay/pools/profile`、`DELETE /api/relay/tenants`。

> 平台重启会把未完成作业判失败，但槽位租约只有 `RelayRuntime.finish()` 会释放，
> 残留的 active 记录会让节点永远 busy（删不掉、也调度不出去）。巡检会按
> `MIGRATION_RELAY_LEASE_GRACE_SECONDS`（默认 300s）兜底回收"作业已不在运行"
> 的租约并按租约台账回算槽位，不需要人工干预。

## 增量迁移故障排查（rbd）

排查前先记住两件事：**RBD 映像名不是虚拟机名**，而是 Cinder 卷 UUID
（`volume-<volume-uuid>`，暂存卷再加 `-mig-stage` 后缀）；**cron 之外的
`rbd --conf` 必须写成 Pod 内的绝对路径**，否则报
`global_init: unable to open config file from search list ...`。

```bash
POD=$(kubectl -n migrate get pod -l app=openstack-vm-migration -o name | head -1)
JOB=<job_id>                      # 形如 cb53115c5c35
D=/app/uploads/$JOB               # 每个 job 一份 source/target_ceph.conf
SRC=volumes/volume-<源卷uuid>      # 日志里「卷映射」的第一个元素
STAGE=volumes/volume-<目标卷uuid>-mig-stage
DST=volumes/volume-<目标卷uuid>

# 0. 数据面客户端必须与集群同代（14.2.22 nautilus）
kubectl -n migrate exec $POD -- rbd --version
# 1. 源端快照是否还在（迁移中的 mig-* 应存在，跑完会被清理）
kubectl -n migrate exec $POD -- rbd --conf $D/source_ceph.conf snap ls $SRC
# 2. 暂存卷布局与快照：增量跑完基线后 snapshot_count 必须 >0，且包含 mig-<job>-0
kubectl -n migrate exec $POD -- rbd --conf $D/target_ceph.conf info $STAGE
kubectl -n migrate exec $POD -- rbd --conf $D/target_ceph.conf snap ls $STAGE
# 3. 目标卷信息（换入成功后只剩一个无快照的卷）
kubectl -n migrate exec $POD -- rbd --conf $D/target_ceph.conf info $DST
# 4. 源/目标是否指向同一个 Ceph（跨云时必须不同 fsid/mon_host）
kubectl -n migrate exec $POD -- md5sum $D/source_ceph.conf $D/target_ceph.conf
```

判读要点：第 0 步若显示 `16.2.x pacific`，说明镜像里的客户端与 Nautilus 集群跨代，
一旦 mon 推送带 `removed_snaps` 的 OSDMap 增量就会
`FAILED ceph_assert(q != removed_snaps_queue.end())`；第 2 步 `snapshot_count: 0`
就是增量失败的直接原因（见下一条注意事项）；
第 2 步的 `order/stripe unit/stripe count` 必须与第 1 步对应的源卷一致，否则
换入前会被布局校验拒绝。

**先看退出码再下结论**：`rc=110` 是命令超时（集群慢），`rc=16` 是 EBUSY
（映像还有 watcher，说明正被别的进程用着），`rc=2` 才是"映像/快照不存在"，
负退出码表示被信号打死（`-6` = SIGABRT，Ceph 客户端命中断言时就是这样）。
因此 `rbd info` 这类探测命令一旦超时或 EBUSY，**不能**当成"卷不存在"：曾经
把所有探测失败都当成"目标暂存卷不存在，无法应用增量 diff"，集群抖动一次就把
本来能续跑的增量任务判死。现在的实现只对真正的不存在放行，其余情况记 WARNING
并把 rbd 的 stderr 一起打出来。

## 注意事项

- **RBD 拷贝失败看哪一行**：两端都会打印
  `rbd 拷贝失败 export_rc=.. import_rc=.. import 先退出=.. export_stderr=.. import_stderr=..`。
  `import 先退出=True` 时 `export_rc=-15` 是本进程 `terminate()` export 造成的
  假象，真实原因在 `import_stderr`；若 export 先自己失败（例如源快照不存在），
  则先报 export，此时 import 的报错多半只是读到截断的流。报错时抛出的
  `CalledProcessError` 指向的也是判定出来的根因那一端；
- **增量迁移的基线快照**：`rbd import-diff` 只会把增量合并进目标，并校验 diff
  头的起始快照必须已存在于目标卷。全量 `rbd export | rbd import` 建出来的暂存
  卷没有任何快照，因此基线拷贝完成后必须补一条同名快照
  （`rbd snap create <pool>/<stage>@<mig-<job>-0>`），否则第一轮增量会立刻以
  `start snapshot '...' does not exist in the image` 失败，源端只能看到
  `Broken pipe`/`rc=-15`。代码已在基线拷贝后自动补建，并在每轮增量前预检；
  换入目标卷前会清空暂存卷上的 `mig-*` 快照，最终卷仍保持无快照。
  完整的链路是 `...@mig-<job>-0` 为基线、之后每轮 `--from-snap mig-<job>-N`
  → `mig-<job>-N+1`，日志里的 `增量预估 mig-…-1..mig-…-2` 就是这个推进顺序；
  任何一环缺失（源端快照被 GC 回收、暂存卷被误删）都会让这一轮的
  `export-diff`/`import-diff` 直接失败；
- **残留暂存镜像由谁清**：`<target>-mig-stage` 只在校验通过后被 `rename` 换入，
  而失败/取消的任务走不到这一步；它的名字又由目标卷 UUID 决定，每次迁移都会
  新建目标卷，所以旧名字再也匹配不到，只能靠两处显式回收——卷级失败清理
  （`on_failure` 删源快照 + 删暂存镜像）与服务启动时的后台 GC
  （`sweep_orphan_stages`，删除目标池里不属于运行中任务的 `*-mig-stage`）。
  判断"是否运行中"不能只看任务状态：出现过任务已标成 `completed`、而某个卷还停在
  `pending`/`copying` 的窗口——基线全备期间卷状态本来就还是 `pending`
  （`seed_volume` 跑完才置 `copying`）——此时删掉暂存镜像会让迁移随即报
  "目标暂存卷 … 不存在"。所以只要任务里还有卷没落终态（`success`/`failed`）
  或工作线程还活着，该任务的暂存镜像与源端 `mig-*` 快照一律不回收（GC 下一轮
  再看）。相反，VM 失败清理会把卷落成 `failed`，进程重启时也会把残留的
  `pending`/`copying` 卷落成 `failed`，保证真正的残留最终能被回收。
  删除暂存镜像前会先 `snap purge`，因为带快照的 `rbd rm` 会以 rc=39 失败；
  也正因为会先 purge，更不能对在飞的暂存镜像动手——purge 会把它这一轮的
  基线快照删掉，后续增量立刻缺起始快照；
  回收动作里的 EBUSY（rc=16）与信号退出（rc<0）都按"稍后再试"降级为 WARNING，
  不再刷 ERROR 掩盖真正的原因；
- OpenStack 项目名在域内唯一、跨域可同名，因此页面提供“载入项目”：
  选择后自动填入项目 UUID，后续创建卷/VM 都按 UUID 定位项目，避免同名
  `admin` 落到错误项目导致“配额为 0”的问题；后端在仅填项目名时也会尝试
  用 `GET /v3/auth/projects` 自动解析为 UUID，同名无法唯一确定时会报错并
  列出候选 UUID，不会静默选错；
- 源与目标 OpenStack/Ceph 必须能被运行本服务的节点访问；
- 源卷 RBD 名假定为 `volume-<uuid>`，执行前会通过 `rbd info` 校验；
- RBD 导入后不迁移快照与克隆关系，仅迁移当前卷数据；
- Job 状态保存在进程内，进程/容器重启后运行中任务会中断；
- 源 VM 成功后不会被自动删除，请人工确认后再下线。

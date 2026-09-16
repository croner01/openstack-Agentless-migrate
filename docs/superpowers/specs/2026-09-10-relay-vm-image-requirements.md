# 中转机镜像与操作系统要求（OpenStack VM）

日期：2026-09-10
状态：待审阅
适用设计：`2026-09-10-relay-vm-full-copy-migration-design.md`

## 1. 结论

- 中转机是**通过 OpenStack 创建的虚拟机**：由 Glance 镜像启动 instance，
  挂上端口与卷即可，不是容器镜像、不涉及 Dockerfile。
- agent 是**纯 Python 标准库实现，没有任何第三方依赖**，因此镜像侧只需要
  `python3` 与 `systemd` 两样东西，不需要 pip 安装、不需要本地源。
- **主路径是平台自动下发**：平台按页面配置的镜像与 flavor 调 Nova 建 instance，
  cloud-init 注入注册令牌并拉起 `relay-agent`，agent 主动注册后进入池子。
  这部分代码已经实现：`RelayPool.provision()` → `create_relay_server(user_data=...)`
  → agent 注册 → `RelayPool.wait_ready()` 绑定会话。
- 需要提前准备的只有**镜像来源**：agent 代码得先进到 VM 里。
  两种办法见第 6 节（预烘焙镜像 / cloud-init 自助安装）。
- 预置 VM + SSH 安装是可选路径，不属于主流程，见第 7 节。

## 2. 镜像与操作系统基线

| 项目 | 要求 |
| --- | --- |
| 架构 | x86_64 |
| 发行版 | RHEL/CentOS 7.9+、Rocky Linux 8/9、Ubuntu 20.04/22.04 等主流服务器版 |
| 内核 | ≥ 3.10，需自带 `virtio-blk` / `virtio-scsi` 驱动 |
| 必备组件 | `python3` ≥ 3.8、`systemd` |
| 运行身份 | root（要直接读写 `/dev/vdX`） |
| 不需要 | Docker/容器运行时、`qemu-guest-agent`、`multipath-tools` |

关于多路径：iSCSI/FC 的多路径与登录由**计算节点**上的 os-brick 完成，
guest 内看到的是一块普通 virtio 盘（`/dev/vdb` 这类），所以中转机里
不需要装多路径工具，也不需要 HBA 或 NPIV。

## 3. 网络与安全组

- **管理面**：中转机需能出方向访问平台地址（HTTPS/HTTP）。
  agent 只发起出向请求，平台不主动连中转机。
- **数据面**：两台中转机之间需 IP 互通，目标端监听端口默认 `9200`，
  入方向只放通对端中转机网段，不要暴露到公网。
- 建议两块网卡：一块管理网（到平台），一块存储/数据网（到对端中转机）。
- 源卷、目标卷由 Cinder attach 到中转机，卷必须与中转机在**同一个 AZ**。

## 4. 资源规格建议

- 起步规格：2 vCPU / 4 GiB 内存 / 20 GiB 系统盘。
- 每台中转机同一时刻只服务**一个卷对**（串行复用），因此不需要大规格，
  需要的是存储网带宽。
- 池大小按带宽估算：单流块拷贝实测通常 100–300 MB/s，
  N 台中转机的理论上限约为 `N × 单流速率`，且受限于同一张存储网卡。

### 命名规则

| 位置 | 取值 | 说明 |
| --- | --- | --- |
| Nova instance 名 | `relay-source-<i>` / `relay-target-<i>` | `i` 从 0 开始，等于池大小 |
| guest 主机名 | 同上 | cloud-init `hostname` 写入，与 instance 名一致 |
| `/etc/relay/agent.env` 的 `RELAY_NAME` | 同上 | agent 注册时上报，平台按它绑定池节点 |
| `RELAY_DATA_ADDR` | agent 自动探测 | 未显式配置时，取到平台方向所用网卡的地址 |

重建中转机时会复用同一个 slot 名（IP 与 instance id 都会变），
因此运维按名字排查时看到的主机名始终稳定。

## 5. 登录方式与互信

页面上的「中转机池」面板提供两项：

- **中转机 root 密码**：作为 Nova `admin_password` 下发，cloud-init 里再用
  `chpasswd` + `ssh_pwauth` 显式设一次，两条都做是为了兼容不同镜像。
  留空则只允许密钥登录。
- **平台 SSH 公钥**：通过 cloud-init 写进中转机 root 的 `authorized_keys`，
  平台/运维可以免密 SSH 进去排查。

反向互信由 cloud-init 在首次启动时执行
`ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519` 生成中转机自己的密钥对，
agent 注册时把公钥上报给平台（`/api/relay/register` 的 `ssh_public_key`），
平台存在该节点的记录里。需要中转机反向登录平台时，运维把这条公钥加到
平台的 `authorized_keys` 即可；平台不会自动改写宿主机的 SSH 配置。

注意：密码与令牌都会出现在 cloud-init 的 user_data 里，而 user_data 在
OpenStack 中可被有权限的用户通过 metadata 读到，因此中转机的密码不要复用
任何生产口令。

## 5.1 镜像内必须做的两件事（否则会写脏源数据）

1. **禁止自动挂载**：不要在 `/etc/fstab` 里写任何数据盘条目，
   不要安装 `udisks` 这类会自动挂载块设备的组件。挂上来的派生卷是
   源数据的完整副本，一旦被自动 mount 并写入，源侧快照就失去意义。
2. **屏蔽 LVM 自动激活**：派生卷里往往含源 VM 的 LVM 卷组，
   若与本机卷组同名会冲突，且会以读写方式激活。建议在
   `/etc/lvm/lvm.conf` 中关闭 `obtain_device_list_from_udev`，
   或用 `global_filter` 把 `/dev/vd*`（系统盘除外）排除。

另外若发行版默认开启 SELinux，服务以 root + `unconfined_service_t` 运行即可
读写块设备；若做了自定义策略，需要给 `relay-agent` 放行块设备访问。

## 6. 镜像来源：两种让 agent 进入 VM 的办法

平台自动下发时，唯一的缺口是"agent 代码怎么进到 VM 里"。两条路：

### A1. 预烘焙镜像

把 `relay_protocol.py`、`relay_transfer.py`、`relay_agent.py` 三个文件
和 systemd unit 烤进一个 Glance 镜像（`/opt/relay` + `/etc/systemd/system/relay-agent.service`），
平台直接用这个镜像建机，cloud-init 只注入 `/etc/relay/agent.env` 并启动服务。

- 优点：建机后启动快，不依赖外网。
- 缺点：每次改 agent 都要重新烤镜像并同步到两侧云。

### A2. cloud-init 自助安装（推荐）

平台自己提供 agent 包的下载端点，cloud-init 里把文件拉下来装好再启动服务：

```yaml
#cloud-config
write_files:
  - path: /etc/relay/agent.env
    permissions: '0600'
    content: |
      RELAY_PLATFORM_URL=https://<platform-host>:19099
      RELAY_TOKEN=<一次性作业令牌>
      RELAY_JOB_ID=<job_id>
      RELAY_ROLE=source
      RELAY_NAME=relay-source-0
      RELAY_DATA_PORT=9200
runcmd:
  - curl -fsSL "https://<platform-host>:19099/api/relay/bootstrap?token=<一次性作业令牌>" | bash
```

`/api/relay/bootstrap` 返回一段安装脚本：下载三个 py 文件到 `/opt/relay`、
写 systemd unit、`systemctl enable --now relay-agent`。

- 优点：**任何标准镜像**（python3 + systemd）都能直接用，不需要定制 Glance 镜像，
  agent 升级只需换平台侧文件。
- 安全：安装脚本以 root 执行，必须走 HTTPS；端点用一次性令牌鉴权，
  且只白名单下发这三个文件，不接受任意路径。
- **现状：这个端点还没实现**，是"平台自动下发 + 免定制镜像"闭环的最后一块。

## 7. 可选：预置节点模式（暂不实现）

如果某个云不允许平台创建实例，或希望节点常驻、按需分配，可以走预置节点：
运维先建好 VM，agent 带**节点级令牌**注册为 `idle` 节点，作业开始时平台从
`idle` 节点按 role + AZ 取用，再下发短时效作业票据。

这需要 `RelayState.register` 区分节点注册与作业注册、`RelayPool` 支持
"外部节点池"，页面增加"平台创建 / 使用预置节点"的选择。
当前设计的注册令牌绑定 `(job_id, role)`，只支持平台建机，
因此该模式作为后续增强，不在本期范围。

## 8. 验收清单

- [ ] VM 能从 Glance 镜像正常启动，`python3 --version` ≥ 3.8，`systemctl` 可用；
- [ ] `curl -k https://<platform>/api/jobs` 能通（管理面出方向 OK）；
- [ ] 两台中转机互相 `nc -vz <peer> 9200` 能通（数据面 OK）；
- [ ] `/etc/fstab` 无数据盘条目，`lvm.conf` 已屏蔽自动激活；
- [ ] `systemctl enable --now relay-agent` 后服务 active，日志无异常；
- [ ] 平台页面"中转机池"能看到该节点，状态为 ready/idle。

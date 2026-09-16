"""作业级中转机池：建机、预热、调度与销毁。

池按 (云, AZ) 分组；每台机器同一时刻只服务一个卷对，
agent 负责数据面，卷的挂载卸载由平台完成。
"""
from __future__ import annotations

import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from relay_registry import RelayState
from relay_wait import wait_for

CLOUD_INIT_HEADER = """#cloud-config
hostname: {name}
preserve_hostname: false
ssh_pwauth: true
disable_root: false
{password_block}{ssh_block}write_files:
  - path: /etc/relay/agent.env
    permissions: '0600'
    content: |
      RELAY_PLATFORM_URL={platform_url}
      RELAY_TOKEN={token}
      RELAY_JOB_ID={job_id}
      RELAY_NODE_ID={node_id}
      RELAY_ROLE={role}
      RELAY_NAME={name}
      RELAY_DATA_PORT={data_port}
      RELAY_SLOTS={slots_total}
{data_addr_block}
"""

# 镜像里已预装 agent：cloud-init 只启动服务。
CLOUD_INIT_BAKED_RUNCMD = """runcmd:
  - [ systemctl, enable, --now, relay-agent ]
"""

# 自举模式：镜像里没有 agent，cloud-init 从平台拉安装脚本现场安装。
CLOUD_INIT_BOOTSTRAP_RUNCMD = """runcmd:
  - bash -lc "curl -fsSL '{platform_url}/api/relay/bootstrap?token={token}' -o /tmp/relay-bootstrap.sh && bash /tmp/relay-bootstrap.sh"
"""

# 生成中转机自己的密钥对，供 agent 上报公钥、建立反向互信。
CLOUD_INIT_KEYGEN_RUNCMD = (
    "  - bash -lc \"test -f /root/.ssh/id_ed25519 || ssh-keygen -t ed25519 -N '' "
    "-f /root/.ssh/id_ed25519 -C relay@{name}\"\n"
)

#: 中转机名只允许这些字符：它会进 hostname、shell 命令与 YAML 标量，
#: 放行任意字符等于把命令注入/配置注入的入口交给上游 AZ/角色名。
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _reject_newlines(field: str, value: str) -> str:
    """拒绝会把 YAML 块标量/命令撑破的换行与控制字符。"""
    text = str(value or "")
    if any(ch in text for ch in "\r\n\x00"):
        raise ValueError(f"{field} 不能包含换行或空字符")
    return text


def _safe_name(name: str) -> str:
    text = str(name or "")
    if not SAFE_NAME_RE.fullmatch(text):
        raise ValueError(f"非法中转机名: {text!r}")
    return text


def _password_block(admin_password: str) -> str:
    if not admin_password:
        return ""
    password = _reject_newlines("admin_password", admin_password)
    return (
        "chpasswd:\n"
        "  expire: false\n"
        "  list: |\n"
        f"    root:{password}\n"
    )


def _ssh_block(ssh_public_key: str) -> str:
    if not ssh_public_key:
        return ""
    lines = [line.strip() for line in ssh_public_key.splitlines() if line.strip()]
    entries = "\n".join(f"      - {line}" for line in lines)
    return f"users:\n  - name: root\n    ssh_authorized_keys:\n{entries}\n"


def _data_addr_block(data_addr: str) -> str:
    """数据面地址：跨云迁移时必须是对端可达的浮动 IP。"""
    value = str(data_addr or "").strip()
    return f"      RELAY_DATA_ADDR={value}\n" if value else ""


def build_cloud_init(
    *,
    platform_url: str,
    token: str,
    job_id: str,
    role: str,
    name: str,
    data_port: int = 9200,
    node_id: str = "",
    slots_total: int = 1,
    bootstrap: bool = True,
    admin_password: str = "",
    ssh_public_key: str = "",
    data_addr: str = "",
) -> str:
    """生成注入注册令牌与角色的 cloud-init。

    bootstrap=True 时由平台下发 agent 包并现场安装，标准镜像即可；
    False 时假定镜像里已预装 /opt/relay 与 relay-agent 服务。
    admin_password 设置 root 密码登录，ssh_public_key 建立平台→中转机互信。
    """
    # 所有插值都落在 YAML 标量或 shell 命令里，输入必须先做注入校验。
    name = _safe_name(name)
    role = _reject_newlines("role", role)
    node_id = _reject_newlines("node_id", node_id)
    job_id = _reject_newlines("job_id", job_id)
    token = _reject_newlines("token", token)
    platform_url = _reject_newlines("platform_url", platform_url)
    data_addr = _reject_newlines("data_addr", data_addr)
    prefix = CLOUD_INIT_HEADER.format(
        password_block=_password_block(admin_password),
        ssh_block=_ssh_block(ssh_public_key),
        platform_url=platform_url,
        token=token,
        job_id=job_id,
        node_id=node_id,
        slots_total=int(slots_total or 1),
        role=role,
        name=name,
        data_port=data_port,
        data_addr_block=_data_addr_block(data_addr),
    )
    body = (
        CLOUD_INIT_BOOTSTRAP_RUNCMD if bootstrap else CLOUD_INIT_BAKED_RUNCMD
    ).format(
        platform_url=platform_url,
        token=token,
    )
    return prefix + body + CLOUD_INIT_KEYGEN_RUNCMD.format(name=name)


@dataclass
class RelayNode:
    """中转机在控制面的视图。

    按作业建机（RelayPool）与常驻池（RelayScheduler）都返回这个类型：
    RelayVolumeMover 是按属性鸭子类型取值的，两边若各自维护一份字段，
    漏一个字段就要到线上拷贝时才炸。槽位模式下额外带 lease_id。
    """

    node_id: str
    name: str
    role: str
    az: str
    server_id: str
    session_id: str = ""
    data_address: str = ""
    data_port: int = 9200
    ssh_public_key: str = ""
    agent_version: str = ""
    state: str = "provisioning"
    current_task_id: str = ""
    root_volume_id: str = ""
    lease_id: str = ""


class RelayPool:
    """一台中转机同一时刻只服务一个卷对，复用方式为串行。"""

    def __init__(
        self,
        *,
        role: str,
        job_id: str,
        az: str,
        size: int,
        image_id: str,
        flavor_id: str,
        port_ids: list[str],
        admin_password: str,
        platform_url: str,
        token_factory: Callable[[str, str], str],
        os_utils: Any,
        state: RelayState,
        data_port: int = 9200,
        bootstrap: bool = True,
        ssh_public_key: str = "",
        heartbeat_timeout: float = 30.0,
        subnet_id: str = "",
        fixed_ips: list[str] | None = None,
        system_volume_type: str = "",
        data_floating_network: str = "",
    ):
        self.role = role
        self.job_id = job_id
        self.az = az
        self.size = size
        self.image_id = image_id
        self.flavor_id = flavor_id
        self.port_ids = list(port_ids)
        self.admin_password = admin_password
        self.platform_url = platform_url
        self.token_factory = token_factory
        self.os_utils = os_utils
        self.state = state
        self.data_port = data_port
        self.bootstrap = bootstrap
        self.ssh_public_key = ssh_public_key
        self.heartbeat_timeout = heartbeat_timeout
        self.subnet_id = subnet_id
        self.fixed_ips = list(fixed_ips or [])
        self.system_volume_type = system_volume_type
        self.data_floating_network = data_floating_network
        # 数据面浮动 IP：按 port 顺序记录，供 cloud-init 注入 RELAY_DATA_ADDR。
        self.data_fips: list[dict[str, str]] = []
        self.data_addresses: list[str] = []
        self.nodes: list[RelayNode] = []
        # acquire/release/sweep 都是"读-改-写"节点状态，必须串行化，否则
        # 两个拷贝任务会同时抢到同一台中转机（同一数据端口互相踩踏）。
        self._lock = threading.Lock()

    def provision(self) -> None:
        """按池大小创建中转机，cloud-init 注入注册令牌。"""
        if self.port_ids and len(self.port_ids) != self.size:
            raise ValueError(
                f"中转机端口数与池大小不一致: {len(self.port_ids)} != {self.size}"
            )
        for index in range(self.size):
            self.provision_one(index)
        logging.info(
            "[MIGRATION] 中转机池 role=%s az=%s 创建 %s 台",
            self.role,
            self.az,
            len(self.nodes),
        )

    def provision_one(self, index: int) -> RelayNode:
        """创建单台中转机；重建时复用同一个 slot 名字。"""
        name = f"relay-{self.role}-{index}"
        token = self.token_factory(self.job_id, self.role)
        logging.info(
            "[MIGRATION] 中转机 cloud-init 平台地址=%s name=%s",
            self.platform_url,
            name,
        )
        user_data = build_cloud_init(
            platform_url=self.platform_url,
            token=token,
            job_id=self.job_id,
            role=self.role,
            name=name,
            data_port=self.data_port,
            bootstrap=self.bootstrap,
            admin_password=self.admin_password,
            ssh_public_key=self.ssh_public_key,
            data_addr=(
                self.data_addresses[index]
                if index < len(self.data_addresses)
                else ""
            ),
        )
        port_id = self.port_ids[index] if index < len(self.port_ids) else None
        server = self.os_utils.create_relay_server(
            name=name,
            image_id=self.image_id,
            flavor_id=self.flavor_id,
            port_ids=[port_id] if port_id else [],
            availability_zone=self.az,
            user_data=user_data,
            admin_password=self.admin_password or None,
            system_volume_type=self.system_volume_type,
        )
        node = RelayNode(
            node_id=uuid.uuid4().hex,
            name=name,
            role=self.role,
            az=self.az,
            server_id=server.id,
            root_volume_id=str(getattr(server, "root_volume_id", "") or ""),
        )
        self.nodes.append(node)
        return node

    def rebuild(self, node_id: str) -> RelayNode | None:
        """销毁并重建一台机器：同一 slot 名字，旧 agent 会话作废。"""
        for index, node in enumerate(self.nodes):
            if node.node_id != node_id:
                continue
            try:
                self.os_utils.delete_server(node.server_id, force=True)
            except Exception:  # noqa: BLE001 - 删不掉就放弃重建，避免端口被两台机器争用
                logging.exception(
                    "[MIGRATION] 重建前删除中转机失败，放弃重建 server=%s",
                    node.server_id,
                )
                return None
            self._delete_root_volume(node)
            self.nodes.pop(index)
            self.state.drop_by_name(node.name)
            return self.provision_one(index)
        return None

    def wait_ready(
        self,
        *,
        timeout: float = 300.0,
        poll_interval: float = 2.0,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        """等待池内每台机器的 agent 完成注册。"""
        pending = list(self.nodes)

        def all_registered() -> bool:
            """返回 True 表示全部就绪；否则把还没注册的节点留在 pending。"""
            still_pending = []
            for node in pending:
                agent = self.state.find_by_name(node.name)
                if agent is None:
                    still_pending.append(node)
                    continue
                node.session_id = agent.session_id
                node.data_address = agent.data_address
                node.data_port = agent.data_port
                node.ssh_public_key = agent.ssh_public_key
                node.agent_version = getattr(agent, "version", "") or ""
                node.state = "ready"
                self._log_host(node)
            pending[:] = still_pending
            return not pending

        def log_progress(waited: float) -> None:
            # 定期记录等待中的节点，避免只看到最后一条超时。
            if int(waited) % 30 == 0:
                logging.info(
                    "[MIGRATION] 等待 agent 注册中 %.0fs: %s",
                    waited,
                    ", ".join(node.name for node in pending),
                )

        try:
            wait_for(
                all_registered,
                timeout=timeout,
                interval=poll_interval,
                sleeper=sleeper,
                on_tick=log_progress,
                message="中转机 agent 注册超时",
            )
        except TimeoutError as exc:
            raise TimeoutError(
                "中转机 agent 注册超时: " + "; ".join(self._describe_pending(pending))
            ) from exc

    def _describe_pending(self, pending: list[RelayNode]) -> list[str]:
        """注册超时时给出 Nova 侧现状，便于判断是没起来还是连不上平台。"""
        described: list[str] = []
        for node in pending:
            status = host = "?"
            try:
                server = self.os_utils.get_server_detail(node.server_id)
                status = str(getattr(server, "status", "?") or "?")
                host = str(server.to_dict().get("OS-EXT-SRV-ATTR:host") or "?")
            except Exception:  # noqa: BLE001 - 纯诊断信息
                status = "查询失败"
            described.append(
                f"{node.name}(server={node.server_id} status={status} host={host})"
            )
        return described

    def _log_host(self, node: RelayNode) -> None:
        """记录中转机落在哪台宿主机：挂载失败时用来核对存储主机组映射。"""
        try:
            server = self.os_utils.get_server_detail(node.server_id)
            host = str(server.to_dict().get("OS-EXT-SRV-ATTR:host") or "")
        except Exception:  # noqa: BLE001 - 纯诊断信息，取不到不影响流程
            return
        logging.info(
            "[MIGRATION] 中转机就绪 name=%s server=%s host=%s",
            node.name,
            node.server_id,
            host or "unknown",
        )

    def acquire(self, task_id: str) -> RelayNode | None:
        with self._lock:
            for node in self.nodes:
                # unhealthy 只是瞬时判定：agent 心跳恢复后应当能重新被调度，
                # 否则一次网络抖动就会把节点在这个作业里永久拉黑。
                if node.state in {"ready", "unhealthy"} and self.agent_alive(node):
                    node.state = "busy"
                    node.current_task_id = task_id
                    return node
            return None

    def agent_alive(self, node: RelayNode) -> bool:
        agent = self.state.find_by_name(node.name)
        if agent is None:
            return False
        if getattr(agent, "state", "") == "unhealthy":
            return False
        last = float(getattr(agent, "last_heartbeat", 0.0) or 0.0)
        return (time.time() - last) <= self.heartbeat_timeout

    def describe(self) -> list[str]:
        """给"没有空闲机器"这类报错补上可定位的现场。"""
        return [
            f"{node.name}={node.state}"
            f"{'' if self.agent_alive(node) else '(agent-unreachable)'}"
            for node in self.nodes
        ]

    def release(self, node_id: str, *, lease_id: str = "") -> None:
        """按作业建机的池一台机器只有一个整机槽位，lease_id 仅作接口兼容。"""
        with self._lock:
            for node in self.nodes:
                if node.node_id == node_id:
                    node.state = "ready"
                    node.current_task_id = ""
                    return

    def sweep(self, *, now: float, timeout: float | None = None) -> list[str]:
        """心跳超时标记 unhealthy；心跳恢复后重新放回 ready，避免永久拉黑。"""
        limit = self.heartbeat_timeout if timeout is None else timeout
        with self._lock:
            changed: list[str] = []
            for node in self.nodes:
                agent = self.state.find_by_name(node.name)
                alive = agent is not None and (now - agent.last_heartbeat) <= limit
                if alive and node.state == "unhealthy":
                    node.state = "busy" if node.current_task_id else "ready"
                    changed.append(node.node_id)
                    continue
                if not alive and node.state != "unhealthy":
                    node.state = "unhealthy"
                    changed.append(node.node_id)
            return changed

    def destroy(self) -> None:
        for node in list(self.nodes):
            try:
                self.os_utils.delete_server(node.server_id, force=True)
            except Exception:  # noqa: BLE001 - 清理失败不阻塞其余机器
                logging.exception(
                    "[MIGRATION] 删除中转机失败 server=%s", node.server_id
                )
            self._delete_root_volume(node)
        self.nodes = []
        self.release_data_floating_ips()

    def _delete_root_volume(self, node: RelayNode) -> None:
        """启动卷随实例删除失败时显式回收，避免占卷配额。"""
        if not node.root_volume_id:
            return
        try:
            self.os_utils.delete_volume_wait(node.root_volume_id)
        except Exception:  # noqa: BLE001 - 可能已随实例删除
            logging.warning(
                "[MIGRATION] 删除中转机启动卷失败（可能已随实例回收）volume=%s",
                node.root_volume_id,
            )

    def release_data_floating_ips(self) -> None:
        """释放本池绑定的数据面浮动 IP，避免占用 FIP 池。"""
        for item in list(self.data_fips):
            floating_ip_id = str(item.get("id") or "")
            if not floating_ip_id:
                continue
            try:
                self.os_utils.delete_relay_floating_ip(floating_ip_id)
                logging.info(
                    "[MIGRATION] 已释放中转机浮动 IP fip=%s address=%s",
                    floating_ip_id,
                    item.get("address", ""),
                )
            except Exception:  # noqa: BLE001 - 已释放/网络异常不阻塞收尾
                logging.warning(
                    "[MIGRATION] 释放中转机浮动 IP 失败（可能已释放）fip=%s",
                    floating_ip_id,
                )
        self.data_fips = []
        self.data_addresses = []

"""中转机通道的运行时装配：表单参数 → 池 / 搬运器 / 对账器。"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

from env_utils import env_float

from relay_orchestrator import RelayVolumeMover
from relay_pool import RelayPool
from relay_protocol import DEFAULT_CHUNK, issue_token
from relay_reaper import TERMINAL_PHASES, RelayReaper
from relay_volumes import VolumeLifecycle

#: 台账里已完结记录的保留时长；超过后由 finish() 机会性回收，避免无限增长。
LEDGER_RETENTION_SECONDS = 7 * 24 * 3600.0

#: 单卷数据面拷贝的墙钟上限（秒），0 = 不限时。真正防卡死的是 stall_timeout 的
#: 字节进度看门狗：1TiB 盘按 50MB/s 要 5.8 小时，按 6 小时墙钟判死会把"只是慢"
#: 的拷贝判失败。用 MIGRATION_RELAY_RESULT_TIMEOUT 配置上限。
RELAY_RESULT_TIMEOUT_SECONDS = 0.0

#: 对账清理"残留"记录的最短静默窗口（秒）。等待建盘/派生卷期间台账会按 60s
#: 打点刷新 updated_at，所以这个窗口只要覆盖"挂载"这类无打点的步骤即可。
RELAY_REAPER_MIN_STALE_SECONDS = 3600.0


def relay_result_timeout_default() -> float:
    """单卷拷贝的墙钟上限；0 = 不限时（默认）。"""
    return env_float(
        "MIGRATION_RELAY_RESULT_TIMEOUT", RELAY_RESULT_TIMEOUT_SECONDS, minimum=0.0
    )


RELAY_PHASE_LABELS = {
    "queued": "排队中",
    "snapshotting": "打快照",
    "cloning": "派生中转卷",
    "attaching_source": "挂载源卷",
    "attaching_target": "挂载目标卷",
    "copying": "全量传输",
    "verifying": "校验数据",
    "detaching": "卸载卷",
    "cleaning": "收尾清理",
    "done": "完成",
    "cleaned": "已清理",
    "failed": "失败",
}

# 采样窗口太短时（页面高频轮询、其他接口也在取快照）差分噪声极大，
# 这种时候沿用上一次的速率，避免页面出现 0 或几十 GiB/s 的抖动。
MIN_THROUGHPUT_WINDOW = 0.5


def relay_phase_label(phase: str) -> str:
    """把台账里的英文阶段映射成与 RBD 路径同风格的中文标签。"""
    return RELAY_PHASE_LABELS.get(phase or "", phase or "")


@dataclass
class PoolConfig:
    size: int
    image: str
    flavor: str
    az: str
    network: str
    subnet: str = ""
    volume_type: str = ""
    system_volume_type: str = ""
    data_floating_network: str = ""
    fixed_ips: list[str] = field(default_factory=list)
    port_ids: list[str] = field(default_factory=list)


@dataclass
class RelayChannelConfig:
    platform_url: str
    source: PoolConfig
    target: PoolConfig
    token_ttl: int = 3600
    agent_install: str = "bootstrap"
    data_port: int = 9200
    admin_password: str = ""
    ssh_public_key: str = ""
    copy_retries: int = 3
    full_verify: bool = False
    heartbeat_interval: int = 10
    heartbeat_timeout: int = 30
    stall_timeout: float = 300.0
    slot_wait_timeout: float = 1800.0
    #: 「打快照 / 快照派生卷」的等待超时（秒）。0 = 不限时（默认）：商业存储
    #: 上这一步常是存储侧全量拷贝，200GiB 也能超过 1 小时，按时间判死会让
    #: 平台在云上还在建盘时就判失败并回收资源。给了正数则按下面 env 的
    #: 基线一同参与"按卷大小自适应"。
    volume_ready_timeout: float = 0.0
    snapshot_ready_timeout: float = 0.0
    hole_mode: str = "skip"
    source_cloud: str = ""
    target_cloud: str = ""
    rate_limit_bytes_per_sec: float = 0.0
    chunk_size: int = DEFAULT_CHUNK
    ready_timeout: float = 180.0
    #: 单卷拷贝的墙钟上限（秒），0 = 不限时；防卡死靠 stall_timeout。
    result_timeout: float = 0.0
    node_mode: str = "persistent"
    slots_per_node: int = 5
    max_nodes: int = 6
    min_nodes: int = 1
    idle_scale_down_seconds: float = 86400.0
    queue_timeout_seconds: float = 1800.0


_REQUIRED_FIELDS = (
    ("relay_platform_url", "中转机回连平台地址"),
    ("relay_source_image", "源端中转机镜像"),
    ("relay_source_flavor", "源端中转机 flavor"),
    ("relay_source_az", "源端中转机可用区"),
    ("relay_source_system_volume_type", "源端中转机系统盘类型"),
    ("relay_target_image", "目标端中转机镜像"),
    ("relay_target_flavor", "目标端中转机 flavor"),
    ("relay_target_az", "目标端中转机可用区"),
    ("relay_target_system_volume_type", "目标端中转机系统盘类型"),
)

_FORM_SCALAR_FIELDS = (
    "relay_platform_url",
    "relay_source_image",
    "relay_source_flavor",
    "relay_source_az",
    "relay_source_network",
    "relay_source_subnet",
    "relay_source_ips",
    "relay_source_volume_type",
    "relay_source_system_volume_type",
    "relay_source_data_floating_network",
    "relay_source_size",
    "relay_target_image",
    "relay_target_flavor",
    "relay_target_az",
    "relay_target_network",
    "relay_target_subnet",
    "relay_target_ips",
    "relay_target_volume_type",
    "relay_target_system_volume_type",
    "relay_target_data_floating_network",
    "relay_target_size",
    "relay_token_ttl",
    "relay_agent_install",
    "relay_data_port",
    "relay_admin_password",
    "relay_ssh_public_key",
    "relay_ready_timeout",
    "relay_heartbeat_interval",
    "relay_heartbeat_timeout",
    "relay_copy_retries",
    "relay_stall_timeout",
    "relay_slot_wait_seconds",
    "relay_hole_mode",
    "relay_full_verify",
    "relay_node_mode",
    "relay_slots_per_node",
    "relay_max_nodes",
    "relay_min_nodes",
    "relay_idle_scale_down_hours",
)

_FORM_LIST_FIELDS = ("relay_source_ports", "relay_target_ports")


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value in (None, ""):
        return []
    return [str(value)]


def _split_ips(value: Any) -> list[str]:
    """解析固定 IP 输入：支持逗号、分号、空白分隔，多台按顺序取用。"""
    if isinstance(value, list):
        raw = value
    else:
        raw = re.split(r"[,;\s]+", str(value or ""))
    return [item.strip() for item in raw if str(item).strip()]


def relay_options_from_form(form: Any) -> dict[str, Any]:
    """把表单字段收敛成 options 片段，便于单测与复用。"""
    options: dict[str, Any] = {
        "data_channel": str(form.get("data_channel") or "rbd").strip().lower()
    }
    for key in _FORM_SCALAR_FIELDS:
        options[key] = str(form.get(key) or "").strip()
    for key in _FORM_LIST_FIELDS:
        getlist = getattr(form, "getlist", None)
        options[key] = list(getlist(key)) if callable(getlist) else []
    return options


def parse_relay_options(options: dict[str, Any]) -> RelayChannelConfig | None:
    """解析中转机通道配置；未启用该通道时返回 None。"""
    if str(options.get("data_channel") or "rbd").strip().lower() != "relay":
        return None

    node_mode = (
        "persistent"
        if str(options.get("relay_node_mode") or "").strip().lower() == "persistent"
        else "ephemeral"
    )
    # 常驻模式的镜像/flavor/网络/卷类型来自资源页的池参数，作业表单只需选 AZ。
    required = (
        (
            ("relay_source_az", "源端中转机可用区"),
            ("relay_target_az", "目标端中转机可用区"),
        )
        if node_mode == "persistent"
        else _REQUIRED_FIELDS
    )
    missing = [
        label
        for key, label in required
        if not str(options.get(key) or "").strip()
    ]
    if missing:
        raise ValueError("中转机通道缺少必填项: " + "、".join(missing))

    source_size = int(options.get("relay_source_size") or 2)
    target_size = int(options.get("relay_target_size") or 2)
    if source_size < 1 or target_size < 1:
        raise ValueError("中转机池大小必须大于 0")

    rate_limit_mb = float(options.get("rate_limit_mb_s") or 0)
    hole_mode = str(options.get("relay_hole_mode") or "").strip().lower() or "skip"
    if hole_mode not in {"skip", "zero", "off"}:
        hole_mode = "skip"
    return RelayChannelConfig(
        platform_url=str(options["relay_platform_url"]).strip().rstrip("/"),
        source=PoolConfig(
            size=source_size,
            image=str(options.get("relay_source_image") or "").strip(),
            flavor=str(options.get("relay_source_flavor") or "").strip(),
            az=str(options.get("relay_source_az") or "").strip(),
            network=str(options.get("relay_source_network") or "").strip(),
            subnet=str(options.get("relay_source_subnet") or "").strip(),
            volume_type=str(options.get("relay_source_volume_type") or "").strip(),
            system_volume_type=(
                str(options.get("relay_source_system_volume_type") or "").strip()
            ),
            data_floating_network=(
                str(options.get("relay_source_data_floating_network") or "").strip()
            ),
            fixed_ips=_split_ips(options.get("relay_source_ips")),
            port_ids=_as_list(options.get("relay_source_ports")),
        ),
        target=PoolConfig(
            size=target_size,
            image=str(options.get("relay_target_image") or "").strip(),
            flavor=str(options.get("relay_target_flavor") or "").strip(),
            az=str(options.get("relay_target_az") or "").strip(),
            network=str(options.get("relay_target_network") or "").strip(),
            subnet=str(options.get("relay_target_subnet") or "").strip(),
                    volume_type=(
                        str(options.get("relay_target_volume_type") or "").strip()
                        or str(options.get("target_volume_type") or "").strip()
                    ),
            system_volume_type=(
                str(options.get("relay_target_system_volume_type") or "").strip()
            ),
            data_floating_network=(
                str(options.get("relay_target_data_floating_network") or "").strip()
            ),
            fixed_ips=_split_ips(options.get("relay_target_ips")),
            port_ids=_as_list(options.get("relay_target_ports")),
        ),
        token_ttl=int(options.get("relay_token_ttl") or 3600),
        agent_install=(
            "baked"
            if str(options.get("relay_agent_install") or "").strip().lower()
            == "baked"
            else "bootstrap"
        ),
        data_port=int(options.get("relay_data_port") or 9200),
        admin_password=str(options.get("relay_admin_password") or ""),
        ssh_public_key=str(options.get("relay_ssh_public_key") or "").strip(),
        copy_retries=max(int(options.get("relay_copy_retries") or 3), 0),
        full_verify=str(options.get("relay_full_verify") or "").strip().lower()
        in {"1", "true", "yes", "on"},
        heartbeat_interval=max(int(options.get("relay_heartbeat_interval") or 10), 1),
        heartbeat_timeout=max(int(options.get("relay_heartbeat_timeout") or 30), 1),
        stall_timeout=max(float(options.get("relay_stall_timeout") or 300.0), 30.0),
        # 0 = 不限时：拷多慢都不判死，卡住由 stall_timeout 的进度看门狗兜底。
        result_timeout=max(
            float(
                options.get("relay_result_timeout")
                or relay_result_timeout_default()
                or 0.0
            ),
            0.0,
        ),
        slot_wait_timeout=max(float(options.get("relay_slot_wait_seconds") or 1800.0), 0.0),
        volume_ready_timeout=max(float(options.get("volume_ready_timeout") or 0.0), 0.0),
        # 页面只有一个「卷/快照就绪超时」输入；未单独给快照超时时沿用它。
        snapshot_ready_timeout=max(
            float(
                options.get("snapshot_ready_timeout")
                or options.get("volume_ready_timeout")
                or 0.0
            ),
            0.0,
        ),
        hole_mode=hole_mode,
        ready_timeout=max(float(options.get("relay_ready_timeout") or 180), 1.0),
        source_cloud=str(options.get("source_cloud") or ""),
        target_cloud=str(options.get("target_cloud") or ""),
        rate_limit_bytes_per_sec=rate_limit_mb * 1024 * 1024,
        # 常驻模式在 Task 14 完成前不可用，因此表单默认仍走 ephemeral；
        # 只有显式传 relay_node_mode=persistent 才启用常驻池。
        node_mode=node_mode,
        slots_per_node=max(int(options.get("relay_slots_per_node") or 5), 1),
        max_nodes=max(int(options.get("relay_max_nodes") or 6), 1),
        min_nodes=max(int(options.get("relay_min_nodes") or 1), 0),
        idle_scale_down_seconds=(
            max(float(options.get("relay_idle_scale_down_hours") or 24), 0.0) * 3600.0
        ),
    )


class SchedulerPoolAdapter:
    """把 RelayScheduler 适配成 RelayVolumeMover 期望的池接口。"""

    def __init__(self, scheduler: Any, *, job_id: str, role: str, tenant_key: str, az: str):
        self.scheduler = scheduler
        self.job_id = job_id
        self.role = role
        self.tenant_key = tenant_key
        self.az = az

    def acquire(self, task_id: str) -> Any:
        return self.scheduler.acquire(
            task_id,
            job_id=self.job_id,
            role=self.role,
            tenant_key=self.tenant_key,
            az=self.az,
            vm_id=task_id,
        )

    def release(self, node_id: str, *, lease_id: str = "") -> None:
        # 带上 job_id / lease_id：同一常驻节点会被同租户的多个作业共享，
        # 串作业释放会把别人在拷的卷判成空闲。
        self.scheduler.release(node_id, job_id=self.job_id, lease_id=lease_id)

    def describe(self) -> list[str]:
        pool = getattr(self.scheduler, "describe", None)
        if callable(pool):
            return pool(self.tenant_key, self.role, self.az)
        return []


class RelayRuntime:
    """一次作业共享一套池与搬运器；作业结束统一收尾。"""

    def __init__(
        self,
        *,
        config: RelayChannelConfig,
        source_os: Any,
        target_os: Any,
        state: Any,
        ledger: Any,
        job_id: str,
        admin_password: str = "",
        inventory: Any = None,
        leases: Any = None,
        registry: Any = None,
        scheduler: Any = None,
        scheduler_factory: Any = None,
        should_stop: Callable[[], bool] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.config = config
        self.state = state
        self.ledger = ledger
        self.job_id = job_id
        self.inventory = inventory
        self.leases = leases
        self.registry = registry or state
        self.scheduler = scheduler
        self.scheduler_factory = scheduler_factory
        # 卷/快照等待默认不限时，必须能感知用户取消，否则工作线程会一直挂着。
        self.should_stop = should_stop or (lambda: False)
        self.clock = clock
        self._progress_samples: dict[str, tuple[float, int, float | None]] = {}
        self._progress_percent: dict[str, float] = {}
        self._created_ports: dict[str, list[str]] = {"source": [], "target": []}
        self.lifecycle = VolumeLifecycle(
            source_os,
            target_os,
            ready_timeout=config.volume_ready_timeout or None,
            snapshot_timeout=config.snapshot_ready_timeout or None,
            should_stop=self.should_stop,
        )
        self.source_pool = RelayPool(
            role="source",
            job_id=job_id,
            az=config.source.az,
            size=config.source.size,
            image_id=config.source.image,
            flavor_id=config.source.flavor,
            port_ids=config.source.port_ids,
            admin_password=config.admin_password or admin_password,
            platform_url=config.platform_url,
            token_factory=self._token,
            os_utils=source_os,
            state=state,
            data_port=config.data_port,
            bootstrap=config.agent_install == "bootstrap",
            ssh_public_key=config.ssh_public_key,
            heartbeat_timeout=config.heartbeat_timeout,
            subnet_id=config.source.subnet,
            fixed_ips=config.source.fixed_ips,
            system_volume_type=config.source.system_volume_type,
            data_floating_network=config.source.data_floating_network,
        )
        self.target_pool = RelayPool(
            role="target",
            job_id=job_id,
            az=config.target.az,
            size=config.target.size,
            image_id=config.target.image,
            flavor_id=config.target.flavor,
            port_ids=config.target.port_ids,
            admin_password=config.admin_password or admin_password,
            platform_url=config.platform_url,
            token_factory=self._token,
            os_utils=target_os,
            state=state,
            data_port=config.data_port,
            bootstrap=config.agent_install == "bootstrap",
            ssh_public_key=config.ssh_public_key,
            heartbeat_timeout=config.heartbeat_timeout,
            subnet_id=config.target.subnet,
            fixed_ips=config.target.fixed_ips,
            system_volume_type=config.target.system_volume_type,
            data_floating_network=config.target.data_floating_network,
        )
        self.reaper = RelayReaper(
            ledger=ledger,
            lifecycle=self.lifecycle,
            pools={"source": self.source_pool, "target": self.target_pool},
            # 对账窗口不能跟着 result_timeout 一起被调小到 0：它必须覆盖
            # 「挂载」等没有 60s 打点的步骤，否则会把在途卷当残留删掉。
            stale_seconds=max(
                float(config.result_timeout or 0.0),
                RELAY_REAPER_MIN_STALE_SECONDS,
            ),
            # 对账只回收"没有活作业认领"的残留：本进程里还在跑的作业，它的
            # 卷就算几小时不刷新 updated_at 也归它自己的线程收拾，绝不能被
            # 巡检当孤儿删掉（否则后续挂载会 404 Volume not found）。
            active_jobs=lambda: set(_RUNTIMES),
        )

    def _token(self, job_id: str, role: str) -> str:
        return issue_token(
            self.state.secret, job_id, role, now=time.time(), ttl=self.config.token_ttl
        )

    def start(self) -> None:
        """建池并等待 agent 注册；失败时抛错由调用方决定是否降级。"""
        if self.config.node_mode == "persistent":
            if self.inventory is None or self.leases is None:
                raise ValueError("persistent 模式缺少平台级节点清单或租约存储")
            # 常驻节点由调度器按需扩容，作业启动不建机、不删机。
            self.state.heartbeat_interval = self.config.heartbeat_interval
            self.state.heartbeat_timeout = self.config.heartbeat_timeout
            return

        # 用当前这对云的凭据，先把上一轮遗留的中间产物收掉（平台重启后尤其重要）。
        if self.config.source_cloud or self.config.target_cloud:
            cleaned = self.reaper.sweep(
                cloud_filter=(self.config.source_cloud, self.config.target_cloud)
            )
            if cleaned:
                logging.info("[MIGRATION] 启动对账清理遗留资源: %s", cleaned)
        # 心跳间隔下发给 agent（注册响应里带回），单作业场景下取最新一次配置。
        self.state.heartbeat_interval = self.config.heartbeat_interval
        self.state.heartbeat_timeout = self.config.heartbeat_timeout
        self._created_ports["source"] = self._ensure_ports(
            self.source_pool, self.config.source, self.source_pool.os_utils
        )
        self._created_ports["target"] = self._ensure_ports(
            self.target_pool, self.config.target, self.target_pool.os_utils
        )
        # 跨云迁移：两侧中转机各自绑浮动 IP，agent 用 FIP 作为对端可连地址。
        self._bind_data_floating_ips(
            self.source_pool, self.config.source, self.source_pool.os_utils
        )
        self._bind_data_floating_ips(
            self.target_pool, self.config.target, self.target_pool.os_utils
        )
        self.source_pool.provision()
        self.target_pool.provision()
        self.source_pool.wait_ready(timeout=self.config.ready_timeout)
        self.target_pool.wait_ready(timeout=self.config.ready_timeout)

    @staticmethod
    def _ensure_ports(pool: Any, pool_config: PoolConfig, os_utils: Any) -> list[str]:
        """池未显式给端口时，按配置的网络为每台中转机建一个端口。"""
        if pool.port_ids:
            return []
        if not pool_config.network:
            raise ValueError("中转机池必须指定网络，或显式提供端口")
        created: list[str] = []
        for index in range(pool_config.size):
            fixed_ip = (
                pool_config.fixed_ips[index]
                if index < len(pool_config.fixed_ips)
                else None
            )
            port = os_utils.create_relay_port(
                pool_config.network,
                subnet_id=pool_config.subnet or None,
                fixed_ip=fixed_ip,
            )
            created.append(str(port.id))
        pool.port_ids = created
        return created

    @staticmethod
    def _bind_data_floating_ips(
        pool: Any, pool_config: PoolConfig, os_utils: Any
    ) -> list[str]:
        """给池内每个端口绑一个数据面浮动 IP；失败时回收已绑定的。"""
        network = str(
            getattr(pool_config, "data_floating_network", "") or ""
        ).strip()
        if not network:
            return []
        bound: list[dict[str, str]] = []
        try:
            for port_id in list(pool.port_ids):
                floating_ip = os_utils.create_relay_floating_ip(port_id, network)
                bound.append(
                    {
                        "id": str(getattr(floating_ip, "id", "") or ""),
                        "address": str(
                            getattr(floating_ip, "floating_ip_address", "") or ""
                        ),
                        "port_id": str(port_id),
                    }
                )
        except Exception:
            for item in bound:
                try:
                    os_utils.delete_relay_floating_ip(item["id"])
                except Exception:  # noqa: BLE001 - 回收失败不覆盖原始错误
                    logging.exception(
                        "[MIGRATION] 回收浮动 IP 失败 fip=%s", item.get("id")
                    )
            raise
        pool.data_fips = bound
        pool.data_addresses = [item["address"] for item in bound]
        logging.info(
            "[MIGRATION] 中转机数据面浮动 IP 就绪 role=%s addresses=%s",
            getattr(pool, "role", ""),
            ", ".join(pool.data_addresses) or "无",
        )
        return pool.data_addresses

    def mover_factory(self, vm: Any, options: dict[str, Any]) -> RelayVolumeMover:
        if self.config.node_mode == "persistent":
            source_scheduler = self._scheduler_for("source", self.config.source.az)
            target_scheduler = self._scheduler_for("target", self.config.target.az)
            source_pool = SchedulerPoolAdapter(
                source_scheduler,
                job_id=self.job_id,
                role="source",
                tenant_key=self.config.source_cloud,
                az=self.config.source.az,
            )
            target_pool = SchedulerPoolAdapter(
                target_scheduler,
                job_id=self.job_id,
                role="target",
                tenant_key=self.config.target_cloud,
                az=self.config.target.az,
            )
        else:
            source_pool, target_pool = self.source_pool, self.target_pool
        return RelayVolumeMover(
            source_pool=source_pool,
            target_pool=target_pool,
            lifecycle=self.lifecycle,
            state=self.state,
            ledger=self.ledger,
            job_id=self.job_id,
            chunk_size=self.config.chunk_size,
            rate_limit_bytes_per_sec=self.config.rate_limit_bytes_per_sec,
            ready_timeout=self.config.ready_timeout,
            result_timeout=self.config.result_timeout,
            copy_retries=self.config.copy_retries,
            full_verify=self.config.full_verify,
            stall_timeout=self.config.stall_timeout,
            slot_wait_timeout=self.config.slot_wait_timeout,
            hole_mode=self.config.hole_mode,
            clock=self.clock,
            source_cloud=self.config.source_cloud,
            target_cloud=self.config.target_cloud,
            source_volume_type=self.config.source.volume_type,
            target_volume_type=self.config.target.volume_type,
        )

    def _scheduler_for(self, role: str, az: str) -> Any:
        """常驻模式下按池取调度器：容量参数来自该池的建机参数。"""
        tenant_key = (
            self.config.source_cloud if role == "source" else self.config.target_cloud
        )
        if self.scheduler_factory is not None:
            scheduler = self.scheduler_factory(tenant_key, role, az)
            if scheduler is not None:
                return scheduler
        if self.scheduler is None:
            raise ValueError("persistent 模式缺少 RelayScheduler")
        return self.scheduler

    def finish(self) -> None:
        """作业收尾：对账清理中间产物；persistent 模式只归还租约。"""
        try:
            self.reaper.reconcile_job(self.job_id)
        except Exception:  # noqa: BLE001 - 收尾失败不能阻止销毁中转机
            logging.exception("[MIGRATION] 作业 %s 对账清理失败", self.job_id)
        self.seal_ledger()
        # 已完结的台账记录不会再被对账/界面引用，定期回收防止文件无限增长。
        if self.ledger.prune(
            prunable_phases={"done", "cleaned"},
            max_age_seconds=LEDGER_RETENTION_SECONDS,
        ):
            self.ledger.save()

        if self.config.node_mode == "persistent":
            # 槽位租约由调度器在 acquire/release 上维护：
            # 作业收尾按其 job_id 统一归还，绝不删除或重建节点。
            if self.scheduler is not None:
                self.scheduler.release_job(self.job_id)
            elif self.leases is not None:
                self.leases.release_job(self.job_id)
                self.leases.save()
            return

        self.source_pool.destroy()
        self.target_pool.destroy()
        self._delete_created_ports("source", self.source_pool.os_utils)
        self._delete_created_ports("target", self.target_pool.os_utils)

    def seal_ledger(self) -> list[str]:
        """作业结束后把仍非终态的卷记录标成 failed。

        否则一条中断的拷贝会让台账一直停在 "copying"，页面永远显示"在途卷"。
        cleanup_failed 例外：它还要留给对账重试。
        """
        sealed: list[str] = []
        for record in self.ledger.all():
            if record.job_id != self.job_id:
                continue
            if record.phase in TERMINAL_PHASES or record.phase == "cleanup_failed":
                continue
            record.phase = "failed"
            record.updated_at = time.time()
            self.ledger.upsert(record)
            sealed.append(record.key)
        if sealed:
            self.ledger.save()
        return sealed

    def _delete_created_ports(self, role: str, os_utils: Any) -> None:
        for port_id in self._created_ports.pop(role, []):
            try:
                os_utils.delete_port(port_id)
            except Exception:  # noqa: BLE001 - 端口清理失败不影响作业结果
                logging.exception("[MIGRATION] 删除中转机端口失败 port=%s", port_id)

    def snapshot(self) -> dict[str, Any]:
        """供页面轮询的池状态。"""
        return {
            "job_id": self.job_id,
            "source": [asdict(node) for node in self.source_pool.nodes],
            "target": [asdict(node) for node in self.target_pool.nodes],
            "ledger": self.ledger_summary(),
            "volumes": self.volume_progress(),
            # 用服务端时钟做基准：浏览器与服务器时间不同步时，页面算出来的
            # "已等待" 才不会凭空多出几小时。
            "now": time.time(),
        }

    def volume_progress(self) -> list[dict[str, Any]]:
        """卷级进度：中转机通道不写 vm.volumes，页面靠这份数据看百分比与速率。"""
        now = self.clock()
        items: list[dict[str, Any]] = []
        for record in self.ledger.all():
            if record.job_id != self.job_id:
                continue
            copied = int(record.copied_bytes or 0)
            total = int(getattr(record, "total_bytes", 0) or 0)
            if total > 0:
                percent = round(min(100.0, copied / total * 100.0), 1)
                if record.phase == "done":
                    percent = 100.0
            else:
                percent = 0.0
            # 重试会按偏移续传、total 也可能在建记录后被修正，进度只许前进，
            # 否则页面上的进度条会莫名回退，和 RBD 路径的观感不一致。
            percent = max(percent, self._progress_percent.get(record.volume_id, 0.0))
            self._progress_percent[record.volume_id] = percent
            previous = self._progress_samples.get(record.volume_id)
            throughput: float | None = None
            last_throughput: float | None = None
            if previous is not None:
                prev_ts, prev_bytes, last_throughput = previous
                elapsed = now - prev_ts
                if elapsed >= MIN_THROUGHPUT_WINDOW and copied >= prev_bytes:
                    throughput = round(
                        (copied - prev_bytes) / elapsed / (1024 * 1024), 1
                    )
                else:
                    throughput = last_throughput
            self._progress_samples[record.volume_id] = (now, copied, throughput)
            items.append(
                {
                    "volume_id": record.volume_id,
                    "vm_id": record.vm_id,
                    "target_volume_id": record.target_volume_id,
                    "phase": record.phase,
                    "progress_label": relay_phase_label(record.phase),
                    "copied_bytes": copied,
                    "total_bytes": total,
                    "skipped_bytes": int(getattr(record, "skipped_bytes", 0) or 0),
                    "sent_bytes": max(
                        copied - int(getattr(record, "skipped_bytes", 0) or 0), 0
                    ),
                    "progress_percent": percent,
                    "throughput_mb_s": throughput,
                    # 打快照/派生卷这类阶段没有字节流动，页面靠 updated_at
                    # 显示"已等待多久"，否则只能看到"暂无在途卷"。
                    "updated_at": float(getattr(record, "updated_at", 0.0) or 0.0),
                }
            )
        return items

    def ledger_summary(self) -> dict[str, int]:
        """在途与已清理计数，页面用它证明"没有堆积"。"""
        summary = {"total": 0, "in_flight": 0, "done": 0, "cleaned": 0, "failed": 0}
        for record in self.ledger.all():
            if record.job_id != self.job_id:
                continue
            summary["total"] += 1
            if record.phase == "done":
                summary["done"] += 1
            elif record.phase == "cleaned":
                summary["cleaned"] += 1
            elif record.phase in {"failed", "failed_retained"}:
                summary["failed"] += 1
            else:
                summary["in_flight"] += 1
        return summary

    def sweep(self, *, now: float) -> list[str]:
        """周期巡检：标记心跳超时的中转机，并清理超时未完成的卷任务。"""
        unhealthy: list[str] = []
        for pool in (self.source_pool, self.target_pool):
            unhealthy.extend(
                pool.sweep(now=now, timeout=self.config.heartbeat_timeout)
            )
        cleaned = self.reaper.sweep(
            now=now,
            cloud_filter=(self.config.source_cloud, self.config.target_cloud),
        )
        if unhealthy:
            logging.warning("[MIGRATION] 中转机心跳超时: %s", unhealthy)
        return unhealthy + cleaned

    def rebuild_node(self, node_id: str) -> dict[str, Any] | None:
        """重建一台中转机；返回新节点信息，找不到时返回 None。"""
        for pool in (self.source_pool, self.target_pool):
            node = pool.rebuild(node_id)
            if node is not None:
                return asdict(node)
        return None


_RUNTIMES: dict[str, RelayRuntime] = {}


def register_runtime(job_id: str, runtime: RelayRuntime) -> None:
    _RUNTIMES[job_id] = runtime


def get_runtime(job_id: str) -> RelayRuntime | None:
    return _RUNTIMES.get(job_id)


def drop_runtime(job_id: str) -> None:
    _RUNTIMES.pop(job_id, None)


def sweep_all_runtimes(*, now: float | None = None) -> list[str]:
    """周期巡检入口：遍历所有活跃作业的运行时做健康检查与对账。"""
    current = time.time() if now is None else now
    results: list[str] = []
    for job_id, runtime in list(_RUNTIMES.items()):
        try:
            results.extend(runtime.sweep(now=current))
        except Exception:  # noqa: BLE001 - 单个作业巡检失败不影响其它作业
            logging.exception("[MIGRATION] 中转机巡检失败 job=%s", job_id)
    return results

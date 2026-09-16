"""常驻中转机资源层装配：清单 / 租约 / 池参数 / 凭据 / 调度器 / 节点管理器。

主密钥优先取 MIGRATION_SECRET_KEY，缺省时自动生成 uploads/relay-master-key（0600）；
只有密钥文件不可写等真实错误才会让 ensure() 返回 False。
RBD 通道与 relay ephemeral 通道完全不受影响。
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable

from relay_credentials import CredentialError, CredentialStore, Sealer
from relay_inventory import NodeInventory
from relay_lease import LeaseStore
from relay_node_manager import RelayNodeManager
from relay_pool_profile import PoolProfileStore
from relay_reaper import NodeReconciler
from relay_scheduler import NodeNotReadyError, RelayScheduler, SchedulerConfig
from relay_secret import SecretError, load_or_create_secret


def load_or_create_credentials(
    credentials_path: Any,
    master_key_path: Any,
    *,
    env: Any = None,
) -> CredentialStore:
    """凭据存储工厂：主密钥优先取 MIGRATION_SECRET_KEY，缺省则生成并落盘。

    密钥文件（0600）必须随 uploads 一起持久化，否则重启后旧凭据无法解密。
    """
    key = load_or_create_secret(
        master_key_path, env_var="MIGRATION_SECRET_KEY", env=env
    )
    return CredentialStore.load(credentials_path, Sealer(key))


class RelayResourceLayer:
    def __init__(
        self,
        *,
        inventory: NodeInventory,
        leases: LeaseStore,
        state: Any,
        secret: bytes,
        platform_url: str,
        profiles_path: Any,
        credentials_path: Any,
        master_key_path: Any = None,
        os_utils_factory: Callable[[dict[str, Any]], Any],
        ledger: Any = None,
        credentials_factory: Callable[[], CredentialStore] | None = None,
        scheduler_config: SchedulerConfig | None = None,
        data_port: int = 9200,
        bootstrap: bool = True,
        clock: Callable[[], float] = time.time,
    ):
        self.inventory = inventory
        self.leases = leases
        self.state = state
        self.secret = secret
        self.platform_url = platform_url
        self.profiles = PoolProfileStore.load(profiles_path)
        self.credentials_path = credentials_path
        self.master_key_path = (
            master_key_path
            if master_key_path is not None
            else Path(str(credentials_path)).with_name("relay-master-key")
        )
        self.os_utils_factory = os_utils_factory
        self.ledger = ledger
        self.credentials_factory = credentials_factory
        self.scheduler_config = scheduler_config or SchedulerConfig()
        self.data_port = data_port
        self.bootstrap = bootstrap
        self._clock = clock
        self.credentials: CredentialStore | None = None
        self.node_manager: RelayNodeManager | None = None
        self.scheduler: RelayScheduler | None = None
        self.reconciler: NodeReconciler | None = None
        self.error = ""

    @property
    def ready(self) -> bool:
        return self.scheduler is not None and self.node_manager is not None

    def ensure(self) -> bool:
        """按需构建依赖主密钥的部分；返回 False 时 error 里是原因。"""
        if self.ready:
            return True
        try:
            if self.credentials is None:
                self.credentials = (
                    self.credentials_factory()
                    if self.credentials_factory is not None
                    else load_or_create_credentials(
                        self.credentials_path, self.master_key_path
                    )
                )
        except (CredentialError, SecretError, OSError) as exc:
            self.error = str(exc)
            return False

        self.node_manager = RelayNodeManager(
            inventory=self.inventory,
            profiles=self.profiles,
            credentials=self.credentials,
            registry=self.state,
            secret=self.secret,
            platform_url=self.platform_url,
            os_utils_factory=self.os_utils_factory,
            data_port=self.data_port,
            bootstrap=self.bootstrap,
            clock=self._clock,
        )
        self.scheduler = RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=self.state,
            config=self.scheduler_config,
            node_manager=self.node_manager,
            clock=self._clock,
        )
        if self.ledger is not None:
            self.reconciler = NodeReconciler(
                inventory=self.inventory,
                leases=self.leases,
                ledger=self.ledger,
                os_utils_factory=self.os_utils_factory,
                credentials=self.credentials,
            )
        self.error = ""
        logging.info("[MIGRATION] 常驻中转机资源层已就绪")
        return True

    def scheduler_for_pool(self, tenant_key: str, role: str, az: str) -> RelayScheduler | None:
        """按池参数返回独立调度器；池未配置时返回 None。

        每个池一份 SchedulerConfig，避免多个作业/池共享同一个可变配置。
        """
        profile = self.profiles.get(tenant_key, role, az)
        if profile is None or self.scheduler is None:
            return None
        return self._build_scheduler(
            SchedulerConfig(
                slots_per_node=profile.slots_per_node,
                max_nodes=profile.max_nodes,
                min_nodes=profile.min_nodes,
                idle_scale_down_seconds=profile.idle_scale_down_seconds,
                scale_up_wait_seconds=self.scheduler_config.scale_up_wait_seconds,
                queue_timeout_seconds=self.scheduler_config.queue_timeout_seconds,
            )
        )

    def _build_scheduler(self, config: SchedulerConfig) -> RelayScheduler:
        return RelayScheduler(
            inventory=self.inventory,
            leases=self.leases,
            registry=self.state,
            config=config,
            node_manager=self.node_manager,
            clock=self._clock,
        )

    def ensure_pool(
        self,
        *,
        tenant_key: str,
        role: str,
        az: str,
        auth: dict[str, Any],
        profile_defaults: dict[str, Any],
        min_nodes: int,
        ready_timeout: float = 300.0,
        attempts: int = 2,
    ) -> dict[str, Any]:
        """作业提交时的常驻池准备：存凭据 → 首次写池参数 → 预热 min_nodes。

        已有池沿用原有建机参数，作业表单只在池首次创建时生效。
        建机成功但 agent 注册不上时必须重试；重试仍失败则删掉失败的机器并
        抛错让作业直接失败——静默放行会让作业继续关源机、建卷、拷贝，
        最后卡在拿不到槽位上，同时白占建机额度。
        """
        if not self.ensure():
            raise ValueError("常驻中转机未就绪：" + (self.error or "缺少主密钥"))

        self.credentials.save(tenant_key, auth)
        self.credentials.flush()

        profile = self.profiles.get(tenant_key, role, az)
        created_profile = False
        if profile is None:
            payload = dict(profile_defaults)
            payload.update({"tenant_key": tenant_key, "role": role, "az": az})
            profile = self.profiles.upsert_profile(payload)
            self.profiles.save()
            created_profile = True

        if not str(getattr(profile, "system_volume_type", "") or "").strip():
            raise ValueError(
                f"常驻中转机池未配置系统盘类型（role={role} az={az}）："
                "中转机走云硬盘启动，必须先选好云盘类型。"
                "请在「中转机资源 / 环境配置」页更新该池的系统盘类型后重试。"
            )

        live = [
            record
            for record in self.inventory.nodes_in_pool(tenant_key, role, az)
            if record.state in {"ready", "busy", "provisioning"}
        ]
        created_nodes: list[str] = []
        for _ in range(max(int(min_nodes) - len(live), 0)):
            created_nodes.append(
                self._create_ready_node(
                    tenant_key=tenant_key,
                    role=role,
                    az=az,
                    ready_timeout=ready_timeout,
                    attempts=attempts,
                )
            )

        logging.info(
            "[MIGRATION] 常驻池就绪 tenant=%s role=%s az=%s 新建池参数=%s 新建节点=%s",
            tenant_key,
            role,
            az,
            created_profile,
            created_nodes,
        )
        return {
            "tenant_key": tenant_key,
            "created_profile": created_profile,
            "profile": profile,
            "created_nodes": created_nodes,
        }

    def _create_ready_node(
        self,
        *,
        tenant_key: str,
        role: str,
        az: str,
        ready_timeout: float,
        attempts: int,
    ) -> str:
        """建一台能注册的节点；失败的中转机立即删除，重试到上限后抛错。"""
        limit = max(int(attempts or 1), 1)
        last_name = last_server = ""
        for attempt in range(1, limit + 1):
            record = self.node_manager.create_node(
                tenant_key=tenant_key, role=role, az=az
            )
            last_name, last_server = record.name, record.server_id
            if self.node_manager.wait_ready(record, timeout=ready_timeout):
                return record.node_id
            # 注册不上多半是平台地址不可达、cloud-init 没跑或镜像缺依赖；
            # 留着只会白占建机额度，先删干净再重试。
            logging.warning(
                "[MIGRATION] 常驻中转机未在 %.0fs 内注册：%s（第 %s/%s 次）",
                ready_timeout,
                record.name,
                attempt,
                limit,
            )
            try:
                self.node_manager.delete_node(record.node_id)
            except Exception:  # noqa: BLE001 - 删不掉也要继续报错
                logging.exception(
                    "[MIGRATION] 删除未注册的中转机失败 node=%s", record.node_id
                )
        raise NodeNotReadyError(
            f"新建中转机 {last_name} 在 {ready_timeout:.0f}s 内未注册成功"
            f"（server={last_server}，role={role}，az={az}，已尝试 {limit} 次）"
            "。请检查中转机能否访问平台地址、cloud-init 是否执行、"
            "镜像是否带 curl/python3，以及 /api/relay/bootstrap 是否返回 401。"
        )

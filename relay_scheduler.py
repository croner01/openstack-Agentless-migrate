"""中转机槽位调度：把 VM 迁移槽位分配到常驻节点的空闲槽位上。

容量单位是槽位：1 台 VM 迁移占用 1 个槽位，该 VM 的多块盘在槽位内串行。
本模块负责"有没有槽位、分给谁"与扩缩容判定，建机与删机在 node_manager。
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_lease import LeaseStore
from relay_pool import RelayNode

READY_STATES = {"ready", "busy"}


class NoCapacityError(RuntimeError):
    """池内没有可用槽位。"""


class QueueTimeoutError(NoCapacityError):
    """池已满且等待超过 queue_timeout_seconds。"""


class NodeNotReadyError(NoCapacityError):
    """新建的中转机在超时前没有完成 agent 注册。"""


@dataclass
class SchedulerConfig:
    slots_per_node: int = 5
    max_nodes: int = 6
    min_nodes: int = 1
    idle_scale_down_seconds: float = 86400.0
    scale_up_wait_seconds: float = 30.0
    queue_timeout_seconds: float = 1800.0
    ready_timeout_seconds: float = 300.0
    node_create_attempts: int = 2


#: 常驻池与按作业建机共用同一份节点视图，避免字段漂移。
ScheduledNode = RelayNode


class RelayScheduler:
    def __init__(
        self,
        *,
        inventory: NodeInventory,
        leases: LeaseStore,
        registry: Any,
        config: SchedulerConfig | None = None,
        node_manager: Any = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.inventory = inventory
        self.leases = leases
        self.registry = registry
        self.config = config or SchedulerConfig()
        self.node_manager = node_manager
        self._clock = clock
        self._sleeper = sleeper
        # 槽位分配是"挑节点 -> 写租约 -> 加计数"的读-改-写，必须原子化：
        # 否则并发 acquire 会给同一个 1 槽节点发两条租约。扩容（grow）会长时间
        # 等待/建机，故意放在锁外执行，落位时再回到锁内复核。
        self._lock = threading.Lock()

    def agent_for(self, record: RelayNodeRecord) -> Any:
        if self.registry is None:
            return None
        return self.registry.find_by_name(record.name)

    def has_free_slot(self, record: RelayNodeRecord) -> bool:
        if record.slots_used >= record.slots_total:
            return False
        agent = self.agent_for(record)
        if agent is None:
            return False
        # 心跳超时会把节点标成 unhealthy，但心跳恢复后 RelayState 会把 agent
        # 自身状态复位；此时允许重新调度，避免一次抖动永久占用额度。
        agent_state = getattr(agent, "state", "")
        if agent_state and agent_state not in READY_STATES:
            return False
        if record.state in READY_STATES:
            return True
        return record.state == "unhealthy"

    def pick_node(self, tenant_key: str, role: str, az: str) -> RelayNodeRecord | None:
        """最早创建的节点优先，天然配合"缩容保留最老节点"的策略。"""
        candidates = [
            record
            for record in self.inventory.nodes_in_pool(tenant_key, role, az)
            if self.has_free_slot(record)
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item.created_at, item.node_id))
        return candidates[0]

    def describe(self, tenant_key: str, role: str, az: str) -> list[str]:
        """给"没有可用槽位"这类报错补上现场：每个节点的状态与是否在线。"""
        described: list[str] = []
        for record in self.inventory.nodes_in_pool(tenant_key, role, az):
            agent = self.agent_for(record)
            agent_state = getattr(agent, "state", "") if agent is not None else ""
            online = agent is not None and agent_state in READY_STATES | {""}
            described.append(
                f"{record.name}={record.state} slot={record.slots_used}/"
                f"{record.slots_total}{'' if online else '(agent-unreachable)'}"
            )
        return described

    def view(
        self,
        record: RelayNodeRecord,
        *,
        data_port: int | None = None,
        lease_id: str = "",
    ) -> ScheduledNode:
        agent = self.agent_for(record)
        return ScheduledNode(
            node_id=record.node_id,
            name=record.name,
            role=record.role,
            az=record.az,
            server_id=record.server_id,
            session_id=getattr(agent, "session_id", "") or "",
            data_address=getattr(agent, "data_address", "") or record.fixed_ip,
            data_port=int(
                data_port
                or getattr(agent, "data_port", 0)
                or record.data_port_base
                or 9200
            ),
            ssh_public_key=getattr(agent, "ssh_public_key", "") or "",
            agent_version=(
                getattr(agent, "version", "") or record.agent_version or ""
            ),
            state="busy" if record.slots_used else "ready",
            current_task_id=getattr(agent, "current_task_id", "") or "",
            lease_id=lease_id,
        )

    def acquire(
        self,
        task_id: str,
        *,
        job_id: str,
        role: str,
        tenant_key: str,
        az: str,
        vm_id: str = "",
        volume_id: str = "",
    ) -> ScheduledNode:
        claim_args = {
            "job_id": job_id,
            "role": role,
            "tenant_key": tenant_key,
            "vm_id": vm_id,
            "volume_id": volume_id,
        }
        view = self._try_claim(task_id, az=az, **claim_args)
        if view is not None:
            return view
        # 没有空闲槽位才扩容：grow 会 sleep/建机，绝不能持锁执行。
        for _ in range(4):
            record = self.grow(tenant_key, role, az, needed=1)
            if record is None:
                break
            view = self._claim_record(record, task_id, **claim_args)
            if view is not None:
                return view
            # 建机/挑中的槽位被别的线程先抢走：继续下一轮，由 grow 重新决策。
        raise NoCapacityError(
            f"中转机池无可用槽位: tenant={tenant_key} role={role} az={az}"
        )

    def _try_claim(
        self,
        task_id: str,
        *,
        job_id: str,
        role: str,
        tenant_key: str,
        az: str,
        vm_id: str = "",
        volume_id: str = "",
    ) -> ScheduledNode | None:
        with self._lock:
            record = self.pick_node(tenant_key, role, az)
            if record is None:
                return None
            return self._claim_locked(
                record,
                task_id,
                job_id=job_id,
                role=role,
                tenant_key=tenant_key,
                vm_id=vm_id,
                volume_id=volume_id,
            )

    def _claim_record(
        self,
        record: RelayNodeRecord,
        task_id: str,
        *,
        job_id: str,
        role: str,
        tenant_key: str,
        vm_id: str = "",
        volume_id: str = "",
    ) -> ScheduledNode | None:
        with self._lock:
            if not self.has_free_slot(record):
                return None
            return self._claim_locked(
                record,
                task_id,
                job_id=job_id,
                role=role,
                tenant_key=tenant_key,
                vm_id=vm_id,
                volume_id=volume_id,
            )

    def _claim_locked(
        self,
        record: RelayNodeRecord,
        task_id: str,
        *,
        job_id: str,
        role: str,
        tenant_key: str,
        vm_id: str = "",
        volume_id: str = "",
    ) -> ScheduledNode:
        """调用方必须持有 self._lock。"""
        now = self._clock()
        lease = self.leases.acquire(
            job_id=job_id,
            node_id=record.node_id,
            role=role,
            tenant_key=tenant_key,
            now=now,
            vm_id=vm_id,
            volume_id=volume_id,
            data_port=self._next_data_port(record),
        )
        record.slots_used += 1
        record.state = "busy"
        record.updated_at = now
        self.inventory.upsert(record)
        self._persist()
        # 同一节点上的不同槽位必须使用不同监听端口，端口随租约下发。
        return self.view(
            record, data_port=lease.data_port or None, lease_id=lease.lease_id
        )

    def _next_data_port(self, record: RelayNodeRecord) -> int:
        """挑一个当前没被占用的数据面端口。

        不能再用 ``data_port_base + slots_used`` 推算：slots_used 只是计数，
        槽位乱序释放后会把仍被占用的端口再发一遍（0 号先释放、计数回到 1，
        下一个租约又拿到正被 1 号任务监听的 9201），目标端 bind 直接失败。
        改按当前活动租约实际占用的端口取最小空闲值。
        """
        base = int(record.data_port_base or 9200)
        used = {
            int(lease.data_port)
            for lease in self.leases.active_for_node(record.node_id)
            if lease.data_port
        }
        span = max(int(record.slots_total or 0), 1)
        for offset in range(span + 1):
            if base + offset not in used:
                return base + offset
        return base + span

    def release(
        self, node_id: str, *, job_id: str = "", lease_id: str = ""
    ) -> None:
        """释放该节点上一条活动租约。

        优先按 lease_id 精确释放（同一作业可能在一台机器上占多个槽位）；
        退化为"本作业最早那条（FIFO）"。

        必须区分作业：常驻节点会被同一租户下的多个作业共享，旧写法无条件
        释放"最早一条"租约，作业 A 收尾时会把作业 B 正在用的租约销掉，
        租约台账少一条后 NodeReconciler 的 live 集合跟着缩水，
        就可能把 B 还在拷贝的卷当孤儿卸载掉。
        """
        with self._lock:
            active = [
                lease
                for lease in self.leases.active_for_node(node_id)
                if not job_id or lease.job_id == job_id
            ]
            target = None
            if lease_id:
                target = next(
                    (lease for lease in active if lease.lease_id == lease_id), None
                )
            elif active:
                target = min(active, key=lambda lease: lease.acquired_at)
            if target is not None:
                self.leases.release(target.lease_id, now=self._clock())
            record = self.inventory.get(node_id)
            if record is not None:
                # 以租约台账回算，释放错租约/重复释放都不会让计数漂移。
                record.slots_used = len(self.leases.active_for_node(node_id))
                record.state = "busy" if record.slots_used else "ready"
                if not record.slots_used:
                    record.idle_since = self._clock()
                record.updated_at = self._clock()
                self.inventory.upsert(record)
            self._persist()

    def release_job(self, job_id: str) -> int:
        with self._lock:
            released = self.leases.release_job(job_id, now=self._clock())
            # 一台节点可能释放多条租约：按节点去重后以租约台账回算计数，
            # 避免逐条 -- 造成负值或与其他作业的占用互相漂移。
            touched = {lease.node_id for lease in released}
            for node_id in touched:
                record = self.inventory.get(node_id)
                if record is None:
                    continue
                record.slots_used = len(self.leases.active_for_node(node_id))
                record.state = "busy" if record.slots_used else "ready"
                if not record.slots_used:
                    record.idle_since = self._clock()
                record.updated_at = self._clock()
                self.inventory.upsert(record)
            self._persist()
            return len(released)

    def grow(
        self, tenant_key: str, role: str, az: str, *, needed: int
    ) -> RelayNodeRecord | None:
        """优先复用空闲槽位；不足则扩容，额度满则先跨 AZ 再平衡，最后排队。"""
        if self.node_manager is None:
            return None

        deadline = self._clock() + self.config.queue_timeout_seconds
        while True:
            record = self.pick_node(tenant_key, role, az)
            if record is not None:
                return record

            if (
                self.inventory.count_for_role(tenant_key, role)
                < self.config.max_nodes
            ):
                # 先等其他作业释放槽位，再决定是否真的建机。
                self._sleeper(self.config.scale_up_wait_seconds)
                record = self.pick_node(tenant_key, role, az)
                if record is not None:
                    return record
                record = self._create_ready_node(tenant_key, role, az)
                if record is not None:
                    return record
                # 建机后槽位仍被抢走/建机失败：回到外层循环继续等待，
                # 不能落到下面的 rebalance 把别的 AZ 空闲节点删掉。
                continue

            if self._rebalance(tenant_key, role, az):
                continue

            if self._clock() >= deadline:
                raise QueueTimeoutError(
                    "中转机池已满且等待超时: "
                    f"tenant={tenant_key} role={role} az={az}"
                )
            self._sleeper(self.config.scale_up_wait_seconds)

    def _create_ready_node(
        self, tenant_key: str, role: str, az: str
    ) -> RelayNodeRecord | None:
        """建机并等注册；注册不上就删掉重试，重试到上限仍失败则报错。

        失败的中转机必须从清单删掉：留着它既占建机额度又拿不到槽位，
        会让后续作业一直卡在"没有可用槽位"上。
        """
        limit = max(int(getattr(self.config, "node_create_attempts", 1) or 1), 1)
        last_name = last_server = ""
        for attempt in range(1, limit + 1):
            created = self.node_manager.create_node(
                tenant_key=tenant_key,
                role=role,
                az=az,
                slots_total=self.config.slots_per_node,
            )
            if created is None:
                return None
            last_name, last_server = created.name, created.server_id
            ready = self.node_manager.wait_ready(
                created, timeout=self.config.ready_timeout_seconds
            )
            record = self.pick_node(tenant_key, role, az)
            if record is not None:
                return record
            if ready:
                # 已注册但槽位被其它作业抢走：不删机，回到外层循环继续等待。
                return None
            logging.warning(
                "[MIGRATION] 中转机 %s 未在 %.0fs 内注册（第 %s/%s 次），删除后重试",
                created.name,
                self.config.ready_timeout_seconds,
                attempt,
                limit,
            )
            self.node_manager.delete_node(created.node_id)
            if attempt < limit:
                self._sleeper(self.config.scale_up_wait_seconds)
        raise NodeNotReadyError(
            f"新建中转机 {last_name} 在 "
            f"{self.config.ready_timeout_seconds:.0f}s 内未注册成功"
            f"（server={last_server}，role={role}，az={az}，已尝试 {limit} 次）。"
            "请检查中转机能否访问平台地址、cloud-init 是否执行、"
            "镜像是否带 curl/python3，以及 /api/relay/bootstrap 是否返回 401。"
        )

    def _rebalance(self, tenant_key: str, role: str, target_az: str) -> bool:
        """额度被其他 AZ 的空闲节点占用时，回收一台释放额度。"""
        if self.node_manager is None:
            return False
        for record in sorted(
            self.inventory.nodes_for_role(tenant_key, role),
            key=lambda item: (item.created_at, item.node_id),
            reverse=True,
        ):
            if record.az == target_az:
                continue
            if record.slots_used or self.leases.active_for_node(record.node_id):
                continue
            same_az = self.inventory.nodes_in_pool(tenant_key, role, record.az)
            if len(same_az) <= self.config.min_nodes:
                continue
            if self.node_manager.delete_node(record.node_id):
                return True
        return False

    def _persist(self) -> None:
        self.inventory.save()
        self.leases.save()

    def idle_seconds(
        self, record: RelayNodeRecord, *, now: float | None = None
    ) -> float:
        """节点连续空闲时长；从未使用过的节点从创建时间起算。"""
        current = self._clock() if now is None else now
        since = (
            record.idle_since
            or self.leases.idle_since(record.node_id)
            or record.created_at
        )
        if not since:
            return 0.0
        return max(current - since, 0.0)

    def scale_down(self, *, now: float | None = None) -> list[str]:
        """回收空闲超过阈值且高于 min_nodes 的节点，由新到旧删除。"""
        if self.node_manager is None:
            return []
        current = self._clock() if now is None else now
        removed: list[str] = []
        pools: dict[tuple[str, str, str], list[RelayNodeRecord]] = {}
        for record in self.inventory.all():
            pools.setdefault(record.pool_key, []).append(record)

        for records in pools.values():
            if len(records) <= self.config.min_nodes:
                continue
            removable = len(records) - self.config.min_nodes
            candidates = [
                record
                for record in records
                if record.slots_used == 0
                and not self.leases.active_for_node(record.node_id)
                and self.idle_seconds(record, now=current)
                >= self.config.idle_scale_down_seconds
            ]
            candidates.sort(
                key=lambda item: (item.created_at, item.node_id), reverse=True
            )
            for record in candidates[:removable]:
                if self.node_manager.delete_node(record.node_id):
                    removed.append(record.node_id)
        if removed:
            self.inventory.save()
        return removed

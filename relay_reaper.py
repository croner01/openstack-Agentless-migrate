"""周期对账：把超过阈值仍未完成的卷任务清理掉，避免资源堆积。

agent 崩溃或平台重启后控制面的请求可能丢失，只有按台账对账才能把孤儿资源
收回来，因此这一层是"资源不堆积"的最后一道保险。
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from relay_volumes import SourceCopy

#: 明确完结、对账不再碰的阶段。failed_retained 不是终态：它带着保留期，
#: 到期后由对账回收，作业收尾/取消/删除时立即回收。
TERMINAL_PHASES = {"done", "cleaned"}

#: 失败保留阶段：派生卷/快照在保留期内留给人工重试复用。
RETAINED_PHASE = "failed_retained"

#: Nova 拒绝卸载实例根设备时的报错片段（400 Client Error）。
ROOT_DEVICE_REFUSAL = "cannot detach a root device volume"


def _is_root_device_refusal(exc: Exception) -> bool:
    """根设备被 Nova 拒绝不是对账孤儿，属于清单缺 root_volume_id 时的兜底。"""
    return ROOT_DEVICE_REFUSAL in str(exc).lower()

# 清理失败不是终态：等一小段时间再重试，避免残留卷长期占用配额。
CLEANUP_FAILED_PHASE = "cleanup_failed"
RETRY_SECONDS = 300.0


class RelayReaper:
    def __init__(
        self,
        *,
        ledger: Any,
        lifecycle: Any,
        pools: dict[str, Any],
        stale_seconds: float = 600.0,
        now: Callable[[], float] = time.time,
        active_jobs: Callable[[], set[str]] | None = None,
    ):
        self.ledger = ledger
        self.lifecycle = lifecycle
        self.pools = pools
        self.stale_seconds = stale_seconds
        self._now = now
        #: 本进程里仍在跑的作业 id 提供者；None 表示调用方不做"活作业"保护
        #: （仅老测试/单次脚本会这样传）。
        self.active_jobs = active_jobs

    def _live_jobs(self) -> set[str] | None:
        """正在运行的作业 id；返回 None 表示判断不了，此时本轮不清理任何东西。

        删掉一个正在被拷贝的派生卷的代价远大于多留一小时孤儿卷，所以读不出
        活跃作业列表时宁可什么都不删。
        """
        if self.active_jobs is None:
            return set()
        try:
            return {str(item) for item in (self.active_jobs() or ())}
        except Exception:  # noqa: BLE001 - 判断不了就不清理
            logging.exception("[MIGRATION] 读取活跃作业失败，本轮对账跳过")
            return None

    def sweep(
        self,
        *,
        now: float | None = None,
        cloud_filter: tuple[str, str] | None = None,
    ) -> list[str]:
        """清理超时未完成的卷任务，返回被清理的 key 列表。

        cloud_filter 用于只清理属于当前这对云的记录，避免拿 A 云的凭据
        去清理 B 云遗留的资源。
        """
        current = self._now() if now is None else now
        live_jobs = self._live_jobs()
        if live_jobs is None:
            return []
        cleaned: list[str] = []
        for record in list(self.ledger.all()):
            if record.phase in TERMINAL_PHASES:
                continue
            # 作业还在跑：它自己的线程负责回收，对账只清"没人认领"的残留。
            # updated_at 只在阶段切换/进度上报时刷新，而"已派生但排在队里等
            # 传输"的卷可能几小时不刷新（同一 VM 的盘是串行传的），按时间判
            # 残留会把正在用的派生卷删掉，轮到它挂载时就 404 Volume not found。
            if record.job_id in live_jobs:
                continue
            if cloud_filter is not None and (
                record.source_cloud != cloud_filter[0]
                or record.target_cloud != cloud_filter[1]
            ):
                continue
            # 失败保留：只在保留期过后回收；保留期内即使作业已经不在跑也先留着，
            # 这正是"默认保留 N 小时，重试可直接复用派生卷"的兑现方式。
            if record.phase == RETAINED_PHASE and not self._retention_expired(
                record, current
            ):
                continue
            # 清理失败的记录用更短的间隔重试，避免残留卷长期占配额。
            stale_limit = (
                RETRY_SECONDS
                if record.phase == CLEANUP_FAILED_PHASE
                else self.stale_seconds
            )
            if record.phase != RETAINED_PHASE and (
                current - float(record.updated_at or 0.0) < stale_limit
            ):
                continue
            # 先记下回收前的阶段与静默时长：下面会覆盖 phase/updated_at，
            # 覆盖后再打日志就变成"phase=cleaned 静默 0s"，等于没记。
            stale_phase = record.phase
            silent_seconds = max(current - float(record.updated_at or 0.0), 0.0)
            ok = self._cleanup(record)
            record.phase = "cleaned" if ok else CLEANUP_FAILED_PHASE
            record.retained_until = 0.0
            record.updated_at = current
            self.ledger.upsert(record)
            if ok:
                # 明确记录"谁删了哪个卷"：否则云上少了一块盘只能靠猜。
                logging.warning(
                    "[MIGRATION] 对账回收孤儿卷 job=%s vm=%s volume=%s phase=%s "
                    "derived=%s snapshot=%s target=%s 静默 %.0fs",
                    record.job_id,
                    record.vm_id,
                    record.volume_id,
                    stale_phase,
                    record.derived_volume_id or "-",
                    record.snapshot_id or "-",
                    record.target_volume_id or "-",
                    silent_seconds,
                )
                cleaned.append(record.key)
        if cleaned:
            self.ledger.save()
        return cleaned

    @staticmethod
    def _retention_expired(record: Any, current: float) -> bool:
        """保留期是否已过；没有写保留期的老记录按"已过期"回收。"""
        deadline = float(getattr(record, "retained_until", 0.0) or 0.0)
        if deadline <= 0:
            return True
        return current >= deadline

    def release(self, records: list[Any], *, reason: str = "manual") -> list[str]:
        """立即回收指定记录（不等保留期）：取消/删除/人工释放走这条路。

        与 sweep 的区别是不看时间窗口，调用方已经确认这些资源可以回收。
        返回真正回收成功的 key 列表；清理失败会落到 cleanup_failed 等下一轮重试。
        """
        released: list[str] = []
        current = self._now()
        for record in records:
            ok = self._cleanup(record)
            record.phase = "cleaned" if ok else CLEANUP_FAILED_PHASE
            record.retained_until = 0.0
            record.updated_at = current
            self.ledger.upsert(record)
            if not ok:
                continue
            released.append(record.key)
            logging.warning(
                "[MIGRATION] 立即回收中间卷 job=%s vm=%s volume=%s reason=%s "
                "derived=%s snapshot=%s target=%s",
                record.job_id,
                record.vm_id,
                record.volume_id,
                reason,
                record.derived_volume_id or "-",
                record.snapshot_id or "-",
                record.target_volume_id or "-",
            )
        if released:
            self.ledger.save()
        return released

    def reconcile_job(self, job_id: str) -> list[str]:
        """作业收尾：不管是否超时，清理该作业所有未完成记录。

        作业已经结束（正常结束/取消/删除）就不会再有人点重试，保留期的意义
        不复存在，因此 failed_retained 也在这里立即回收。
        """
        cleaned: list[str] = []
        current = self._now()
        for record in list(self.ledger.all()):
            if record.job_id != job_id or record.phase in TERMINAL_PHASES:
                continue
            ok = self._cleanup(record)
            record.phase = "cleaned" if ok else CLEANUP_FAILED_PHASE
            record.retained_until = 0.0
            record.updated_at = current
            self.ledger.upsert(record)
            if ok:
                cleaned.append(record.key)
        if cleaned:
            self.ledger.save()
        return cleaned

    def _cleanup(self, record: Any) -> bool:
        """清理中间产物；返回 False 表示有资源没清掉，需要后续重试。"""
        ok = True
        try:
            if record.derived_volume_id:
                self.lifecycle.cleanup_source_copy(
                    SourceCopy(
                        snapshot_id=record.snapshot_id,
                        derived_volume_id=record.derived_volume_id,
                    ),
                    record.source_relay_id,
                )
        except Exception:  # noqa: BLE001 - 清理失败不能中断其它记录
            ok = False
            logging.exception("[MIGRATION] 对账清理派生卷失败 key=%s", record.key)
        try:
            if record.target_volume_id and record.target_relay_id:
                self.lifecycle.detach(
                    role="target",
                    server_id=record.target_relay_id,
                    volume_id=record.target_volume_id,
                )
        except Exception:  # noqa: BLE001
            ok = False
            logging.exception("[MIGRATION] 对账卸载目标卷失败 key=%s", record.key)
        return ok


class NodeReconciler:
    """节点维度对账：不属于任何活跃租约/台账的 attachment 一律卸载。

    常驻节点会被多作业反复使用，只有按节点实际挂载对账才能防止中间产物堆积。
    """

    def __init__(
        self,
        *,
        inventory: Any,
        leases: Any,
        ledger: Any,
        os_utils_factory: Callable[[dict[str, Any]], Any],
        credentials: Any,
    ):
        self.inventory = inventory
        self.leases = leases
        self.ledger = ledger
        self.os_utils_factory = os_utils_factory
        self.credentials = credentials

    def _live_volumes(self, node_id: str, server_id: str) -> set[str]:
        live = {
            lease.volume_id
            for lease in self.leases.active_for_node(node_id)
            if lease.volume_id
        }
        for record in self.ledger.all():
            if record.phase in TERMINAL_PHASES:
                continue
            if server_id and server_id in {
                getattr(record, "source_relay_id", ""),
                getattr(record, "target_relay_id", ""),
            }:
                for field in ("derived_volume_id", "target_volume_id"):
                    value = getattr(record, field, "")
                    if value:
                        live.add(value)
        return live

    def sweep(self) -> list[str]:
        detached: list[str] = []
        for record in self.inventory.all():
            if not record.server_id:
                continue
            auth = self.credentials.get(record.tenant_key)
            if not auth:
                continue
            try:
                os_utils = self.os_utils_factory(auth)
                live = self._live_volumes(record.node_id, record.server_id)
                attachments = os_utils.list_server_volume_attachments(
                    record.server_id
                )
            except Exception:  # noqa: BLE001 - 单节点查询失败不阻塞其它节点
                logging.exception(
                    "[MIGRATION] 节点孤儿对账失败 node=%s", record.node_id
                )
                continue
            for attachment in attachments:
                volume_id = str(attachment.get("volume_id") or "")
                if not volume_id or volume_id in live:
                    continue
                # 节点自身是卷启动，系统盘永远不会出现在租约/台账里，
                # 但它不是孤儿；卸它会被 Nova 以 400 拒绝。
                if record.root_volume_id and volume_id == record.root_volume_id:
                    continue
                try:
                    os_utils.detach_volume(record.server_id, volume_id)
                except Exception as exc:  # noqa: BLE001 - 卸载失败保留到下一轮
                    if _is_root_device_refusal(exc):
                        logging.warning(
                            "[MIGRATION] 跳过根设备 attachment（非孤儿）"
                            " node=%s volume=%s",
                            record.node_id,
                            volume_id,
                        )
                        continue
                    logging.exception(
                        "[MIGRATION] 节点孤儿 attachment 卸载失败 node=%s volume=%s",
                        record.node_id,
                        volume_id,
                    )
                    continue
                detached.append(volume_id)
                logging.warning(
                    "[MIGRATION] 节点孤儿 attachment 已卸载 node=%s volume=%s",
                    record.node_id,
                    volume_id,
                )
        return detached

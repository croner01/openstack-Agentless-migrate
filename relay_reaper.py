"""周期对账：把超过阈值仍未完成的卷任务清理掉，避免资源堆积。

agent 崩溃或平台重启后控制面的请求可能丢失，只有按台账对账才能把孤儿资源
收回来，因此这一层是"资源不堆积"的最后一道保险。
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from relay_volumes import SourceCopy

TERMINAL_PHASES = {"done", "cleaned", "failed_retained"}

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
    ):
        self.ledger = ledger
        self.lifecycle = lifecycle
        self.pools = pools
        self.stale_seconds = stale_seconds
        self._now = now

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
        cleaned: list[str] = []
        for record in list(self.ledger.all()):
            if record.phase in TERMINAL_PHASES:
                continue
            if cloud_filter is not None and (
                record.source_cloud != cloud_filter[0]
                or record.target_cloud != cloud_filter[1]
            ):
                continue
            # 清理失败的记录用更短的间隔重试，避免残留卷长期占配额。
            stale_limit = (
                RETRY_SECONDS
                if record.phase == CLEANUP_FAILED_PHASE
                else self.stale_seconds
            )
            if current - float(record.updated_at or 0.0) < stale_limit:
                continue
            ok = self._cleanup(record)
            record.phase = "cleaned" if ok else CLEANUP_FAILED_PHASE
            record.updated_at = current
            self.ledger.upsert(record)
            if ok:
                cleaned.append(record.key)
        if cleaned:
            self.ledger.save()
        return cleaned

    def reconcile_job(self, job_id: str) -> list[str]:
        """作业收尾：不管是否超时，清理该作业所有未完成记录。"""
        cleaned: list[str] = []
        current = self._now()
        for record in list(self.ledger.all()):
            if record.job_id != job_id or record.phase in TERMINAL_PHASES:
                continue
            ok = self._cleanup(record)
            record.phase = "cleaned" if ok else CLEANUP_FAILED_PHASE
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

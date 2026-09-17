"""卷生命周期：源快照、派生卷、挂载卸载与清理。

源卷全程不 detach，派生卷是唯一拷贝源；所有资源都以台账为准登记与回收。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from openstack_utils import (
    derive_ready_timeout,
    snapshot_ready_timeout_default,
    volume_ready_timeout_default,
)


@dataclass
class SourceCopy:
    snapshot_id: str
    derived_volume_id: str


def _raise_quota_hint(exc: Exception, *, side: str, size: int, volume_type: str) -> None:
    """把 Cinder 配额拒绝翻译成可操作的提示，其它异常原样抛出。"""
    text = str(exc)
    if "quota" not in text.lower():
        raise exc
    used = volume_type or "__DEFAULT__（未指定卷类型）"
    raise RuntimeError(
        f"{side}卷创建被配额拒绝：请求 {size}G，卷类型 {used}。"
        "请在页面为该侧指定一个有配额的卷类型，或让云管理员调整 "
        f"gigabytes_{volume_type or '__DEFAULT__'} 配额。原始错误：{text}"
    ) from exc


class VolumeLifecycle:
    """卷生命周期：源快照、派生卷、挂载卸载与清理。

    ``ready_timeout`` / ``snapshot_timeout`` 是作业级覆盖（秒）：给了就原样用，
    留空则按环境变量基线 + 卷容量自适应。商业存储上「打快照 → 派生卷」多半是
    存储侧全量拷贝，耗时与卷大小成正比，固定阈值会把只是慢的大卷判成失败。
    """

    def __init__(
        self,
        source_os: Any,
        target_os: Any,
        *,
        ready_timeout: float | None = None,
        snapshot_timeout: float | None = None,
    ):
        self.source_os = source_os
        self.target_os = target_os
        self.ready_timeout = float(ready_timeout) if ready_timeout else None
        self.snapshot_timeout = float(snapshot_timeout) if snapshot_timeout else None

    def _derive_timeouts(self, size: int) -> tuple[float, float]:
        """返回 (快照等待, 派生卷等待)，单位秒。"""
        snapshot = self.snapshot_timeout or derive_ready_timeout(
            snapshot_ready_timeout_default(), size
        )
        volume = self.ready_timeout or derive_ready_timeout(
            volume_ready_timeout_default(), size
        )
        return float(snapshot), float(volume)

    def _os_for(self, role: str):
        if role == "source":
            return self.source_os
        if role == "target":
            return self.target_os
        raise ValueError(f"unknown role: {role!r}")

    def _source_volume_type(self, volume_id: str) -> str | None:
        """派生卷默认继承源卷的卷类型：同一阵列、同一配额池，最不容易撞配额。"""
        try:
            volume = self.source_os.get_volume(volume_id)
        except Exception as exc:  # noqa: BLE001 - 读不到就按未指定处理
            logging.debug("[MIGRATION] 读取源卷类型失败 volume=%s: %s", volume_id, exc)
            return None
        value = str(getattr(volume, "volume_type", "") or "").strip()
        return value or None

    def create_source_copy(
        self,
        *,
        volume_id: str,
        vm_name: str,
        index: int,
        size: int = 0,
        volume_type: str | None = None,
    ) -> SourceCopy:
        """对源卷打快照并派生一份可挂载的拷贝源。"""
        name = f"mig-{vm_name}-{index}"
        resolved_type = volume_type or self._source_volume_type(volume_id)
        snapshot_timeout, volume_timeout = self._derive_timeouts(size)
        context = f"（源卷派生 {name}，{int(size or 0)}GiB）"
        logging.info(
            "[MIGRATION] 源卷 %s 开始快照派生 %s，等待上限 快照 %.0fs / 派生 %.0fs",
            volume_id,
            context,
            snapshot_timeout,
            volume_timeout,
        )
        snapshot = self.source_os.create_volume_snapshot(volume_id=volume_id, name=name)
        self.source_os.wait_snapshot_status(
            snapshot.id, timeout=snapshot_timeout, context=context
        )
        try:
            derived = self.source_os.create_volume_from_snapshot(
                name=f"{name}-copy",
                snapshot_id=snapshot.id,
                size=size,
                volume_type=resolved_type,
            )
        except Exception as exc:  # noqa: BLE001 - 配额类错误给出可操作提示
            _raise_quota_hint(
                exc, side="源端派生", size=size, volume_type=resolved_type or ""
            )
        self.source_os.wait_volume_status(
            derived.id, timeout=volume_timeout, context=context
        )
        logging.info(
            "[MIGRATION] 源卷派生完成 volume=%s snapshot=%s derived=%s",
            volume_id,
            snapshot.id,
            derived.id,
        )
        return SourceCopy(snapshot_id=snapshot.id, derived_volume_id=derived.id)

    def create_target_volume(
        self,
        *,
        name: str,
        size: int,
        volume_type: str | None = None,
    ) -> str:
        """在目标云建空白卷，内容由中转机全量拷贝写入。"""
        try:
            target = self.target_os.create_blank_volume(
                name=name,
                size=size,
                volume_type=volume_type,
            )
        except Exception as exc:  # noqa: BLE001 - 配额类错误给出可操作提示
            _raise_quota_hint(exc, side="目标", size=size, volume_type=volume_type or "")
        self.target_os.wait_volume_status(target.id)
        return target.id

    def mark_bootable(self, *, volume_id: str, role: str = "target") -> None:
        """把拷完的系统盘标记为可启动，否则 Nova 用 boot_index=0 建机会被拒。"""
        self._os_for(role).set_volume_bootable(volume_id, True)

    def attach(self, *, role: str, server_id: str, volume_id: str) -> str:
        os_utils = self._os_for(role)
        attachment_id = os_utils.attach_volume(
            server_id=server_id, volume_id=volume_id
        )
        context = f"（{role} 卷挂载到中转机 {server_id}）"
        try:
            # Cinder 的挂载完成状态是 "in-use"（连字符），不是 "in_use"。
            os_utils.wait_volume_status(
                volume_id, target="in-use", context=context
            )
        except Exception:
            # 挂载失败不能把卷留在 attaching：尽力卸载，否则后续删除会被 Cinder 拒绝。
            try:
                os_utils.detach_volume(server_id=server_id, volume_id=volume_id)
            except Exception:  # noqa: BLE001 - 清理是尽力而为
                logging.exception(
                    "[MIGRATION] 挂载失败后卸载卷也失败 volume=%s server=%s",
                    volume_id,
                    server_id,
                )
            raise
        return attachment_id

    def detach(
        self, *, role: str, server_id: str, volume_id: str, force: bool = False
    ) -> None:
        os_utils = self._os_for(role)
        if not force and os_utils.find_volume_attachment(server_id, volume_id) is None:
            return
        try:
            os_utils.detach_volume(server_id=server_id, volume_id=volume_id)
        except Exception:
            if not force:
                raise
            logging.exception(
                "[MIGRATION] 强制卸载卷失败 volume=%s server=%s", volume_id, server_id
            )
            return
        os_utils.wait_volume_status(volume_id, target="available")

    def cleanup_source_copy(self, copy: SourceCopy, relay_server_id: str) -> None:
        """清理派生卷与快照；任一步失败只记录，不阻塞后续清理。"""
        if relay_server_id:
            self._detach_best_effort(relay_server_id, copy.derived_volume_id)
        if not self._delete_volume(copy.derived_volume_id) and relay_server_id:
            # Cinder 报 "must not be attached" 说明还有残留挂载：强制卸载后再删一次。
            self._detach_best_effort(relay_server_id, copy.derived_volume_id, force=True)
            self._delete_volume(copy.derived_volume_id)
        try:
            self.source_os.delete_volume_snapshot(copy.snapshot_id)
        except Exception:  # noqa: BLE001
            logging.exception("[MIGRATION] 删除快照失败 snapshot=%s", copy.snapshot_id)

    def _detach_best_effort(
        self, server_id: str, volume_id: str, *, force: bool = False
    ) -> None:
        try:
            if force or self.source_os.find_volume_attachment(server_id, volume_id):
                self.detach(
                    role="source",
                    server_id=server_id,
                    volume_id=volume_id,
                    force=force,
                )
        except Exception:  # noqa: BLE001 - 清理是尽力而为
            logging.exception("[MIGRATION] 卸载派生卷失败 volume=%s", volume_id)

    def _delete_volume(self, volume_id: str) -> bool:
        try:
            self.source_os.delete_volume(volume_id)
            return True
        except Exception:  # noqa: BLE001
            logging.exception("[MIGRATION] 删除派生卷失败 volume=%s", volume_id)
            return False

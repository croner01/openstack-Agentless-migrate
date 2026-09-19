"""单卷中转编排：快照派生、挂载、下发任务、等待完成、清理。"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from env_utils import env_float

from relay_protocol import DEFAULT_CHUNK, agent_supports_sparse
from relay_transfer import TAIL_WINDOW
from relay_volumes import SourceCopy, VolumeLifecycle
from relay_wait import wait_for

GIB = 1024 ** 3

#: 传输失败后保留派生卷/快照的默认时长（小时）。派生一份大盘在商业存储上
#: 常要几小时，失败就删掉会让每次重试都从头再来；保留期内人工重试可直接
#: 复用，过期由对账回收。0 = 失败即回收（旧行为），用
#: MIGRATION_DERIVED_RETENTION_HOURS 覆盖。
DERIVED_RETENTION_HOURS = 24.0

#: 保留原因截断长度，避免把整段堆栈写进台账把文件撑大。
RETAIN_REASON_MAX = 200


def derived_retention_seconds() -> float:
    """失败中间产物保留时长（秒）；0 表示失败立即回收。"""
    return (
        env_float(
            "MIGRATION_DERIVED_RETENTION_HOURS",
            DERIVED_RETENTION_HOURS,
            minimum=0.0,
        )
        * 3600.0
    )


def volume_bytes(volume: Any) -> int:
    """卷的字节长度。

    Cinder 的 ``volume.size`` 单位是 GiB；调用方若已知精确字节数
    （size_bytes）则以它为准，避免再乘一次 1024^3。
    """
    explicit = int(getattr(volume, "size_bytes", 0) or 0)
    if explicit:
        return explicit
    return int(getattr(volume, "size", 0) or 0) * GIB


class CopyCancelled(RuntimeError):
    """拷贝被平台下发取消，不做重试。"""


def sparse_decision(
    source_version: str, target_version: str, hole_mode: str
) -> tuple[bool, str]:
    """返回 (是否启用空洞跳过, 原因)；原因写日志，出问题时有据可查。"""
    if hole_mode == "off":
        return False, "hole_mode=off"
    if not agent_supports_sparse(source_version):
        return False, f"source agent {source_version or 'unknown'} 不支持空洞帧"
    if not agent_supports_sparse(target_version):
        return False, f"target agent {target_version or 'unknown'} 不支持空洞帧"
    return True, f"both agents >= 1.1.0, hole_mode={hole_mode}"


def _describe(pool: Any) -> list[str]:
    """取池内节点状态用于报错；两种池实现都支持 describe()。"""
    describe = getattr(pool, "describe", None)
    if not callable(describe):
        return []
    try:
        return list(describe())
    except Exception:  # noqa: BLE001 - 诊断信息取不到不影响主流程
        return []


class _FifoSlotQueue:
    """跨线程的池级排队闸门：让等待最久的作业/盘先拿到释放出来的槽位。

    槽位只有 ``pool.acquire`` 一个入口，天然是"谁先抢到算谁的"；并发准备
    多块盘时，后启动的线程可能反复插队，把先来的饿死。这里给每个池维护
    一条票号队列，只有队首能尝试 acquire，成功或退出时把票销掉。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queues: dict[int, list[int]] = {}
        self._counter = 0

    def ticket(self, pool: Any) -> tuple[int, int]:
        key = id(pool)
        with self._lock:
            self._counter += 1
            token = self._counter
            self._queues.setdefault(key, []).append(token)
            return key, token

    def is_front(self, key: int, token: int) -> bool:
        with self._lock:
            queue = self._queues.get(key)
            return bool(queue) and queue[0] == token

    def drop(self, key: int, token: int) -> None:
        with self._lock:
            queue = self._queues.get(key)
            if not queue:
                return
            if token in queue:
                queue.remove(token)
            if not queue:
                self._queues.pop(key, None)


_SLOT_QUEUE = _FifoSlotQueue()


def _pool_has_live_holder(pool: Any) -> bool:
    """池里是否还有"迟早会释放"的持有者；取不到判断时按 True（继续等）。"""
    probe = getattr(pool, "has_live_holder", None)
    if callable(probe):
        try:
            return bool(probe())
        except Exception:  # noqa: BLE001 - 诊断失败不改变等待语义
            return True
    described = _describe(pool)
    if not described:
        # describe 不可用/为空时无法判断，保守继续等，避免误判。
        return not callable(getattr(pool, "describe", None))
    return any("(agent-unreachable)" not in item for item in described)


@dataclass
class PreparedVolume:
    """「打快照 → 派生拷贝源 → 建目标空白卷」的产物。

    这一段只等存储侧拷贝，没有数据面流量，因此可以按卷并发准备；准备好
    之后再逐卷走 attach/传输。拆开是为了让多盘 VM 的等待时间不再线性叠加。
    """

    volume: Any
    record: Any
    copy: Any
    target_volume_id: str
    index: int


class RelayVolumeMover:
    """按设计文档 8.1 的单条流程搬运一个卷。"""

    def __init__(
        self,
        *,
        source_pool: Any,
        target_pool: Any,
        lifecycle: VolumeLifecycle,
        state: Any,
        ledger: Any,
        job_id: str,
        chunk_size: int = DEFAULT_CHUNK,
        rate_limit_bytes_per_sec: float = 0.0,
        ready_timeout: float = 180.0,
        #: 单卷拷贝的墙钟上限（秒），0 = 不限时。真正防卡死的是 stall_timeout 的
    #: 字节进度看门狗；这个上限只是兜底，1TiB 盘在 50MB/s 下要 5.8 小时，
    #: 按 6 小时判死会把"只是慢"的拷贝判失败。
    result_timeout: float = 0.0,
        poll_interval: float = 1.0,
        sleeper: Callable[[float], None] = time.sleep,
        copy_retries: int = 3,
        full_verify: bool = False,
        source_cloud: str = "",
        target_cloud: str = "",
        source_volume_type: str = "",
        target_volume_type: str = "",
        hole_mode: str = "skip",
        stall_timeout: float = 300.0,
        slot_wait_timeout: float = 1800.0,
        #: 取消/停机回调：不限时排队期间必须能被用户取消，否则线程会一直挂着。
        should_stop: Callable[[], bool] | None = None,
        #: 失败后保留派生卷/快照的时长（秒）；None 时读
        #: MIGRATION_DERIVED_RETENTION_HOURS（默认 24h），0 = 失败即回收。
        derived_retention_window: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.source_pool = source_pool
        self.target_pool = target_pool
        self.lifecycle = lifecycle
        self.state = state
        self.ledger = ledger
        self.job_id = job_id
        self.chunk_size = chunk_size
        self.rate_limit_bytes_per_sec = rate_limit_bytes_per_sec
        self.ready_timeout = ready_timeout
        self.result_timeout = result_timeout
        self.poll_interval = poll_interval
        self.sleeper = sleeper
        self.copy_retries = max(int(copy_retries), 0)
        self.full_verify = full_verify
        self.source_cloud = source_cloud
        self.target_cloud = target_cloud
        self.source_volume_type = source_volume_type
        self.target_volume_type = target_volume_type
        self.hole_mode = hole_mode
        self.stall_timeout = stall_timeout
        self.slot_wait_timeout = slot_wait_timeout
        self.should_stop = should_stop or (lambda: False)
        self.derived_retention_window = (
            derived_retention_seconds()
            if derived_retention_window is None
            else max(float(derived_retention_window), 0.0)
        )
        self.clock = clock

    def _bounded_timeout(self) -> float:
        """给没有进度看门狗的子步骤用的兜底上限。

        ``result_timeout <= 0``（不限时）只针对带字节进度检测的拷贝等待；
        校验、等待 agent 监听这类步骤一旦卡住没有任何信号，必须有墙钟兜底。
        """
        timeout = float(self.result_timeout or 0.0)
        return timeout if timeout > 0 else 6 * 3600.0

    def record(self, volume: Any) -> Any:
        from relay_ledger import VolumeTaskRecord

        record = self.ledger.get(self.job_id, volume.source_volume_id)
        if record is None:
            record = VolumeTaskRecord(
                job_id=self.job_id,
                vm_id="",
                volume_id=volume.source_volume_id,
                source_cloud=self.source_cloud,
                target_cloud=self.target_cloud,
            )
        elif not record.source_cloud:
            record.source_cloud = self.source_cloud
            record.target_cloud = self.target_cloud
        return record

    def _save(self, record: Any, **fields: Any) -> None:
        for key, value in fields.items():
            setattr(record, key, value)
        record.updated_at = time.time()
        self.ledger.upsert(record)
        self.ledger.save()

    def move(
        self,
        *,
        volume: Any,
        vm_name: str,
        index: int,
        source_volume_type: str | None = None,
        target_volume_type: str | None = None,
    ) -> dict[str, Any]:
        """准备 + 传输一条卷，保持原有单卷调用语义。"""
        return self.transfer(
            self.prepare(
                volume=volume,
                vm_name=vm_name,
                index=index,
                source_volume_type=source_volume_type,
                target_volume_type=target_volume_type,
            )
        )

    def prepare(
        self,
        *,
        volume: Any,
        vm_name: str,
        index: int,
        source_volume_type: str | None = None,
        target_volume_type: str | None = None,
        reuse_target_volume_id: str = "",
        reuse_source_copy: bool = True,
    ) -> PreparedVolume:
        """只做「打快照 → 派生拷贝源 → 建目标空白卷」。

        没有数据面流量，多块盘可以并发准备；失败时把已经建出来的快照与
        派生卷回收，不留占配额的空壳。

        ``reuse_target_volume_id`` 供失败盘重试使用：目标空白卷是这台 VM 的
        数据盘、里面可能已经写进了部分字节，重试时要沿用而不是再建一块；
        但云侧已删掉/状态异常/容量不符时校验不过，回退成新建一块。

        ``reuse_source_copy`` 供失败盘重试使用：上一次传输失败时派生卷/快照
        会按保留期留下（phase=failed_retained），校验通过就直接复用，省掉
        几小时的存储侧打快照 + 派生；校验不过（卷被删/类型不符/保留期已过）
        自动回退到重新派生。
        """
        record = self.record(volume)
        if not record.vm_id:
            # 台账里的 vm_id 就是页面展示用的 VM 名（relay 通道此前一直是空串）。
            record.vm_id = vm_name
        copy = None
        reused = False
        resolved_source_type = source_volume_type or self.source_volume_type or None
        self._save(record, phase="snapshotting")

        def touch(_waited: float = 0.0) -> None:
            """等待期间刷新台账时间戳。

            这一步现在默认不限时，一块盘准备几小时很正常；对账清理只看
            ``updated_at``，不刷新的话准备到一半的派生卷会被当成残留删掉。
            """
            self._save(record, phase=record.phase)

        try:
            if reuse_source_copy:
                copy = self._reuse_source_copy(
                    record,
                    volume_id=volume.source_volume_id,
                    size=int(volume.size or 0),
                    volume_type=resolved_source_type,
                )
            if copy is None:
                # 记录里可能还留着已失效的派生指针（云侧已删、类型不符等），
                # 重新派生之前先把残留清掉，避免快照越积越多。
                self._release_record_copy(record)
                copy = self.lifecycle.create_source_copy(
                    volume_id=volume.source_volume_id,
                    vm_name=vm_name,
                    index=index,
                    size=int(volume.size or 0),
                    volume_type=resolved_source_type,
                    on_wait=touch,
                )
            else:
                reused = True
                logging.info(
                    "[MIGRATION] 复用失败保留的派生卷 volume=%s derived=%s snapshot=%s",
                    volume.source_volume_id,
                    copy.derived_volume_id,
                    copy.snapshot_id,
                )
            self._save(
                record,
                phase="cloning",
                snapshot_id=copy.snapshot_id,
                derived_volume_id=copy.derived_volume_id,
                retained_until=0.0,
                retain_reason="",
            )
            target_volume_id = str(reuse_target_volume_id or "").strip()
            if target_volume_id and not self._target_volume_reusable(
                target_volume_id, int(volume.size or 0)
            ):
                target_volume_id = ""
            if target_volume_id:
                logging.info(
                    "[MIGRATION] 失败盘重试沿用已有目标卷 volume=%s target=%s",
                    volume.source_volume_id,
                    target_volume_id,
                )
            else:
                target_volume_id = self.lifecycle.create_target_volume(
                    name=f"{vm_name}-vol-{index}",
                    size=int(volume.size or 0),
                    volume_type=target_volume_type or self.target_volume_type or None,
                    on_wait=touch,
                )
            self._save(
                record, phase="attaching_target", target_volume_id=target_volume_id
            )
        except Exception as exc:
            retained = False
            if reused and copy is not None:
                # 复用的派生卷仍然有效：保留它，别让"建目标卷失败"把几小时的
                # 派生结果一起删掉，下次重试还能直接用。
                retained = self._retain_copy(
                    record,
                    copy,
                    reason=f"准备阶段失败：{type(exc).__name__}: {exc}",
                )
            if not retained:
                self._save(record, phase="failed")
                self.discard(
                    PreparedVolume(
                        volume=volume,
                        record=record,
                        copy=copy,
                        target_volume_id="",
                        index=index,
                    )
                )
            raise
        return PreparedVolume(
            volume=volume,
            record=record,
            copy=copy,
            target_volume_id=target_volume_id,
            index=index,
        )

    def discard(self, prepared: PreparedVolume) -> None:
        """回收准备阶段建出来的派生卷与快照（该卷不再继续传输）。"""
        if prepared is None or prepared.copy is None:
            return
        try:
            self.lifecycle.cleanup_source_copy(prepared.copy, "")
        except Exception:  # noqa: BLE001 - 清理失败不覆盖原始错误
            logging.exception("[MIGRATION] 回收派生卷失败")

    def _reuse_source_copy(
        self,
        record: Any,
        *,
        volume_id: str,
        size: int,
        volume_type: str | None,
    ) -> Any | None:
        """复用上次失败保留下来的派生卷/快照；任一检查不过返回 None。

        返回 None 时调用方会走"清理残留 → 重新派生"，因此这里只负责判断，
        不做任何删除动作。
        """
        snapshot_id = str(getattr(record, "snapshot_id", "") or "")
        derived_volume_id = str(getattr(record, "derived_volume_id", "") or "")
        if not snapshot_id or not derived_volume_id:
            return None
        if (
            record.source_cloud
            and self.source_cloud
            and record.source_cloud != self.source_cloud
        ):
            logging.warning(
                "[MIGRATION] 台账记录的云与本次不一致，不复用派生卷 volume=%s", volume_id
            )
            return None
        deadline = float(getattr(record, "retained_until", 0.0) or 0.0)
        if deadline and time.time() > deadline:
            logging.warning(
                "[MIGRATION] 派生卷保留期已过（%s），改为重新派生 volume=%s",
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(deadline)),
                volume_id,
            )
            return None
        validate = getattr(self.lifecycle, "validate_source_copy", None)
        if not callable(validate):
            return None
        copy = SourceCopy(
            snapshot_id=snapshot_id, derived_volume_id=derived_volume_id
        )
        try:
            ok, detail = validate(
                copy, volume_id=volume_id, size=size, volume_type=volume_type
            )
        except Exception:  # noqa: BLE001 - 校验异常按不可复用处理
            logging.exception(
                "[MIGRATION] 派生卷复用校验异常，改为重新派生 volume=%s", volume_id
            )
            return None
        if not ok:
            logging.warning(
                "[MIGRATION] 派生卷复用校验失败，改为重新派生 volume=%s：%s",
                volume_id,
                detail,
            )
            return None
        logging.info(
            "[MIGRATION] 派生卷复用校验通过 volume=%s derived=%s（%s）",
            volume_id,
            derived_volume_id,
            detail,
        )
        # 复用的卷必须先从中转机上卸干净：重试会重新 attach，残留挂载会被
        # Nova 以 409 拒绝。已经 available 时 park_volume 是空操作。
        self.lifecycle.park_volume(
            role="source",
            server_id=str(getattr(record, "source_relay_id", "") or ""),
            volume_id=derived_volume_id,
        )
        return copy

    def _release_record_copy(self, record: Any) -> None:
        """清掉台账里已失效的派生卷/快照，并清空对应指针。"""
        snapshot_id = str(getattr(record, "snapshot_id", "") or "")
        derived_volume_id = str(getattr(record, "derived_volume_id", "") or "")
        if not snapshot_id and not derived_volume_id:
            return
        try:
            self.lifecycle.cleanup_source_copy(
                SourceCopy(
                    snapshot_id=snapshot_id, derived_volume_id=derived_volume_id
                ),
                str(getattr(record, "source_relay_id", "") or ""),
            )
        except Exception:  # noqa: BLE001 - 清理失败由对账兜底
            logging.exception(
                "[MIGRATION] 回收失效派生卷失败 volume=%s", record.volume_id
            )
        self._save(
            record,
            snapshot_id="",
            derived_volume_id="",
            retained_until=0.0,
            retain_reason="",
        )

    def _target_volume_reusable(self, volume_id: str, size: int) -> bool:
        """重试前核对上次建好的目标卷还能不能续传。

        目标卷里可能已经写进部分字节，沿用能省一次全量拷贝；但云上被删掉后
        继续沿用会每次重试都 404，所以查不到/状态异常/容量不符时回退成新建
        一块（调用方会记日志，方便定位被放弃的那块盘）。
        """
        try:
            volume = self.lifecycle.target_os.get_volume(volume_id)
        except Exception as exc:  # noqa: BLE001 - 查不到就重建
            logging.warning(
                "[MIGRATION] 目标卷不可用（%s），改为新建一块 target=%s", exc, volume_id
            )
            return False
        if volume is None:
            logging.warning(
                "[MIGRATION] 目标卷不存在，改为新建一块 target=%s", volume_id
            )
            return False
        status = str(getattr(volume, "status", "") or "").lower()
        if status in {"error", "error_deleting", "deleting"}:
            logging.warning(
                "[MIGRATION] 目标卷状态 %s，改为新建一块 target=%s", status, volume_id
            )
            return False
        actual = int(getattr(volume, "size", 0) or 0)
        if size and actual and actual != int(size):
            logging.warning(
                "[MIGRATION] 目标卷容量 %sGiB 与本次 %sGiB 不一致，"
                "改为新建一块 target=%s",
                actual,
                int(size),
                volume_id,
            )
            return False
        return True

    def _retain_copy(
        self,
        record: Any,
        copy: Any,
        *,
        reason: str,
        source_relay_id: str = "",
    ) -> bool:
        """失败保留派生卷/快照：卸载但不删除，写保留期与原因。

        返回 False 表示没有保留（未建出派生卷或保留期关闭），调用方按原逻辑
        立即回收。
        """
        window = float(self.derived_retention_window or 0.0)
        if copy is None or window <= 0:
            return False
        self.lifecycle.park_volume(
            role="source",
            server_id=str(source_relay_id or record.source_relay_id or ""),
            volume_id=copy.derived_volume_id,
        )
        deadline = time.time() + window
        self._save(
            record,
            phase="failed_retained",
            retained_until=deadline,
            retain_reason=(reason or "")[:RETAIN_REASON_MAX],
        )
        logging.warning(
            "[MIGRATION] 盘 %s 失败，保留派生卷 %s/快照 %s 供重试复用，保留至 %s",
            record.volume_id,
            copy.derived_volume_id,
            copy.snapshot_id,
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(deadline)),
        )
        return True

    def retained_info(self, volume_id: str) -> dict[str, Any]:
        """失败保留信息，供作业层落到 relay_disks 给前端提示/释放入口。

        没有保留（正常失败、已完成）时返回空 dict。
        """
        record = self.ledger.get(self.job_id, volume_id)
        if record is None or getattr(record, "phase", "") != "failed_retained":
            return {}
        return {
            "retained_until": float(getattr(record, "retained_until", 0.0) or 0.0),
            "retain_reason": str(getattr(record, "retain_reason", "") or ""),
            "snapshot_id": str(getattr(record, "snapshot_id", "") or ""),
            "derived_volume_id": str(getattr(record, "derived_volume_id", "") or ""),
        }

    def transfer(self, prepared: PreparedVolume) -> dict[str, Any]:
        """挂载两侧卷、搬数据、清理，返回目标卷信息。"""
        volume = prepared.volume
        record = prepared.record
        copy = prepared.copy
        target_volume_id = prepared.target_volume_id
        source_node = target_node = None
        cleaned = False
        retained = False
        started_bytes = max(int(record.copied_bytes or 0), 0)
        try:
            source_node = self._acquire_with_wait(
                self.source_pool, volume.source_volume_id
            )
            if source_node is None:
                # 源端都拿不到就不要再抢目标端：既白等一轮，也会把"自己刚
                # 标记的 busy"写进报错现场，误导排查方向。
                source_state = ", ".join(_describe(self.source_pool)) or "无节点"
                raise RuntimeError(
                    "中转机池没有空闲机器，无法继续拷贝（源端: "
                    f"{source_state}）"
                )
            target_node = self._acquire_with_wait(
                self.target_pool, volume.source_volume_id
            )
            if target_node is None:
                target_state = ", ".join(_describe(self.target_pool)) or "无节点"
                raise RuntimeError(
                    "中转机池没有空闲机器，无法继续拷贝（目标端: "
                    f"{target_state}）"
                )
            self._save(
                record,
                source_relay_id=source_node.server_id,
                target_relay_id=target_node.server_id,
                phase="attaching_source",
            )

            self.lifecycle.attach(
                role="target",
                server_id=target_node.server_id,
                volume_id=target_volume_id,
            )
            self.lifecycle.attach(
                role="source",
                server_id=source_node.server_id,
                volume_id=copy.derived_volume_id,
            )
            source_device = self.lifecycle.source_os.wait_attachment_device(
                source_node.server_id, copy.derived_volume_id
            )
            target_device = self.lifecycle.target_os.wait_attachment_device(
                target_node.server_id, target_volume_id
            )
            self._save(
                record,
                phase="copying",
                copied_bytes=started_bytes,
                total_bytes=volume_bytes(volume),
            )
            self._copy_with_retries(
                volume=volume,
                record=record,
                source_node=source_node,
                target_node=target_node,
                source_device=source_device,
                target_device=target_device,
                offset=started_bytes,
            )
            self._verify(
                record=record,
                source_node=source_node,
                target_node=target_node,
                source_device=source_device,
                target_device=target_device,
            )
            self._save(
                record,
                phase="detaching",
                copied_bytes=volume_bytes(volume),
            )
            self.lifecycle.detach(
                role="target",
                server_id=target_node.server_id,
                volume_id=target_volume_id,
            )
            self._save(record, phase="cleaning")
            self.lifecycle.cleanup_source_copy(copy, source_node.server_id)
            cleaned = True
            self._save(
                record, phase="done", retained_until=0.0, retain_reason=""
            )
            return {
                "target_volume_id": target_volume_id,
                "source_volume_id": volume.source_volume_id,
                "size": int(volume.size or 0),
            }
        except Exception as exc:
            if isinstance(exc, CopyCancelled):
                # 取消（用户/停机）走"立即回收"：不占用保留配额。
                self._save(record, phase="failed")
            else:
                retained = self._retain_copy(
                    record,
                    copy,
                    reason=f"{type(exc).__name__}: {exc}",
                    source_relay_id=(
                        source_node.server_id if source_node is not None else ""
                    ),
                )
                if not retained:
                    self._save(record, phase="failed")
            # 目标卷里可能已经写进了部分字节；先把挂载卸掉再保留，
            # 否则重试时 attach 会被 Nova 拒绝。
            self.lifecycle.park_volume(
                role="target",
                server_id=str(
                    getattr(target_node, "server_id", "")
                    or getattr(record, "target_relay_id", "")
                    or ""
                ),
                volume_id=str(target_volume_id or ""),
            )
            raise
        finally:
            # copy 一旦建出来就必须回收：取不到中转机槽位时 source_node 为 None，
            # 旧写法会跳过清理，把派生卷和快照留在源云里等作业收尾。
            # 命中保留策略时例外：派生卷/快照留给重试复用，过期由对账回收。
            if copy is not None and not cleaned and not retained:
                try:
                    self.lifecycle.cleanup_source_copy(
                        copy, source_node.server_id if source_node is not None else ""
                    )
                except Exception:  # noqa: BLE001 - 清理失败不覆盖原始错误
                    logging.exception("[MIGRATION] 兜底清理派生卷失败")
            if source_node is not None:
                # 常驻池按 lease_id 精确释放：同一作业可能在一台机器上占多个槽位。
                self.source_pool.release(
                    source_node.node_id,
                    lease_id=str(getattr(source_node, "lease_id", "") or ""),
                )
            if target_node is not None:
                self.target_pool.release(
                    target_node.node_id,
                    lease_id=str(getattr(target_node, "lease_id", "") or ""),
                )

    def _transfer(
        self,
        *,
        volume: Any,
        record: Any,
        source_node: Any,
        target_node: Any,
        source_device: str,
        target_device: str,
        offset: int,
    ) -> None:
        # Cinder 的卷大小单位是 GiB，数据面按字节传，必须显式换算。
        total = volume_bytes(volume)
        if offset >= total:
            # 上一轮已经拷完，无需再传（否则 length 会被兜底成 1 反而越界）。
            return
        ticket = uuid.uuid4().hex
        target_task_id = f"{ticket}-t"
        source_task_id = f"{ticket}-s"
        common: dict[str, Any] = {
            "job_id": self.job_id,
            "volume_id": volume.source_volume_id,
            "vm_id": getattr(record, "vm_id", ""),
            "ticket": ticket,
            "offset": offset,
            "length": max(total - offset, 1),
            "chunk_size": self.chunk_size,
            # 续传时上一轮已跳过的字节由平台带回来，保证 skipped 也是绝对口径。
            "skipped_base": int(getattr(record, "skipped_bytes", 0) or 0),
        }
        sparse, reason = self._sparse_enabled(source_node, target_node)
        logging.info(
            "[MIGRATION] 卷 %s 空洞跳过 sparse=%s（%s）",
            volume.source_volume_id,
            sparse,
            reason,
        )
        if sparse:
            common["sparse"] = True
            common["hole_mode"] = self.hole_mode
        self.state.enqueue(
            dict(
                common,
                task_id=target_task_id,
                role="target",
                agent_name=target_node.name,
                dst_path=target_device,
                listen_host="0.0.0.0",
                listen_port=target_node.data_port,
                # 目标端不能无限期卡在 accept：对端失败时它还占着槽位，
                # 后续重试会因为没有空闲 agent 而连监听都起不来。
                accept_timeout=min(max(float(self.ready_timeout), 60.0), 120.0),
            )
        )
        wait_for(
            lambda: self.state.task_ready(target_task_id),
            timeout=self.ready_timeout,
            interval=self.poll_interval,
            sleeper=self.sleeper,
            message="目标端中转机未能在超时前进入监听状态",
        )
        self.state.enqueue(
            dict(
                common,
                task_id=source_task_id,
                role="source",
                agent_name=source_node.name,
                src_path=source_device,
                peer_host=target_node.data_address,
                peer_port=target_node.data_port,
                rate_limit_bytes_per_sec=self.rate_limit_bytes_per_sec,
            )
        )
        task_ids = (target_task_id, source_task_id)
        try:
            self._wait_for_results(record=record, task_ids=task_ids)
        except TimeoutError:
            # 停滞时两端可能还挂在 accept/发送上，先撤销再向上抛（重试会换新票据）。
            self._cancel_pending(task_ids)
            raise
        # 一端失败后，另一端可能还挂在 accept/发送上；下发取消并等它收尾。
        self._cancel_pending(task_ids)

        # 先报真正失败的那一端：另一端可能只是还在监听、根本没有结果，
        # 按固定顺序取第一个会报成 "失败: None"，把真正的错误掩盖掉。
        pending: list[str] = []
        for task_id in (target_task_id, source_task_id):
            result = self.state.task_result(task_id)
            if result is None:
                pending.append(task_id)
                continue
            status = result.get("status")
            if status == "done":
                continue
            if status == "cancelled":
                raise CopyCancelled(f"块拷贝任务 {task_id} 被取消")
            detail = result.get("error") or "未上报错误详情"
            raise RuntimeError(f"块拷贝任务 {task_id} 失败: {status}（{detail}）")
        if pending:
            raise RuntimeError(
                "块拷贝任务未返回结果: " + "、".join(pending)
            )

    def _cancel_pending(self, task_ids: tuple[str, str]) -> None:
        """只撤销还没返回结果的那一端；已完成的任务不需要再取消。"""
        request_cancel = getattr(self.state, "request_cancel", None)
        if not callable(request_cancel):
            return
        for task_id in task_ids:
            if self.state.task_result(task_id) is None:
                request_cancel(task_id)

    def _acquire_with_wait(self, pool: Any, task_id: str) -> Any:
        """取中转机槽位；池满时排队等待，避免把并发作业直接判失败。

        ephemeral 池按作业固定创建 N 台机器，池大小小于并发数时立刻报
        "没有空闲机器"会把其它 VM 白白判失败；作业本身是串行消费槽位的，
        等前一个卷做完就能继续。

        ``slot_wait_timeout`` 是作业表单「等待空闲槽位」的统一口径，两种池
        模式共用：``0`` = 不限时，正数 = 等满该秒数仍拿不到才失败。等待按
        FIFO 交接，且能被取消/停机信号打断。

        只有 ``0`` = 不限时 时才额外检查"池里确实无人会释放"（池为空、全部
        agent 失联）：否则无限排队会永久挂起。配了有限超时时不能提前判死——
        一次心跳抖动会让整池暂时显示 agent-unreachable，但作业表单既然给了
        等待上限，就应当等满再失败。
        """
        node = pool.acquire(task_id)
        if node is not None:
            return node
        limit = float(self.slot_wait_timeout or 0.0)
        if limit <= 0 and not _pool_has_live_holder(pool):
            logging.warning(
                "[MIGRATION] 不限时排队但池内无人会释放（agent 全部失联），"
                "立即失败（%s）",
                "、".join(_describe(pool)) or "无节点",
            )
            return None
        deadline = None if limit <= 0 else self.clock() + limit
        waited = 0.0
        key, token = _SLOT_QUEUE.ticket(pool)
        try:
            while True:
                if self.should_stop():
                    raise CopyCancelled("等待中转机空闲槽位被取消")
                if _SLOT_QUEUE.is_front(key, token):
                    node = pool.acquire(task_id)
                    if node is not None:
                        return node
                if deadline is not None and self.clock() >= deadline:
                    return None
                if int(waited) % 60 == 0:
                    logging.info(
                        "[MIGRATION] 中转机池已满，等待空闲槽位 %.0fs/%s（%s）",
                        waited,
                        "不限时" if deadline is None else f"{limit:.0f}s",
                        "、".join(_describe(pool)) or "无节点",
                    )
                self.sleeper(self.poll_interval)
                waited += self.poll_interval
        finally:
            _SLOT_QUEUE.drop(key, token)

    def _sparse_enabled(self, source_node: Any, target_node: Any) -> tuple[bool, str]:
        """两端 agent 都支持空洞帧、且模式不为 off 时才启用。

        老 agent 不认识 HOLE_MAGIC，收到空洞帧会直接报错，因此必须双端门控。
        """
        return sparse_decision(
            getattr(source_node, "agent_version", ""),
            getattr(target_node, "agent_version", ""),
            self.hole_mode,
        )

    def _wait_for_results(self, *, record: Any, task_ids: tuple[str, str]) -> None:
        """等两端结果，同时盯住字节进度。

        墙钟上限（``result_timeout``，0 = 不限时）只是兜底：真正判断"卡住"靠
        字节进度——要求 copied_bytes 每 stall_timeout 秒至少涨一次，否则页面和
        平台都会一直显示"拷贝中"。任一端出结果（失败也算）立即收尾。
        """

        def finished() -> bool:
            # 任一端失败就立刻收尾：否则另一端可能永远卡在 accept/发送。
            for task_id in task_ids:
                result = self.state.task_result(task_id)
                if result is not None and result.get("status") != "done":
                    return True
            return all(self.state.task_result(task_id) is not None for task_id in task_ids)

        timeout = float(self.result_timeout or 0.0)
        deadline = self.clock() + timeout if timeout > 0 else None
        last_bytes = int(getattr(record, "copied_bytes", 0) or 0)
        last_change = self.clock()
        while True:
            if finished():
                return
            now = self.clock()
            if deadline is not None and now >= deadline:
                raise TimeoutError(
                    f"块拷贝任务超过 {timeout:.0f}s 未返回结果"
                    "（可用 MIGRATION_RELAY_RESULT_TIMEOUT 调大或设为 0 不限时）"
                )
            current = int(getattr(record, "copied_bytes", 0) or 0)
            if current != last_bytes:
                last_bytes = current
                last_change = now
            elif self.stall_timeout and now - last_change >= self.stall_timeout:
                raise TimeoutError(
                    f"块拷贝停滞：{int(self.stall_timeout)} 秒内 copied_bytes "
                    f"没有增长（仍为 {current} 字节），已判定卡死"
                )
            self.sleeper(self.poll_interval)

    def _copy_with_retries(self, **kwargs: Any) -> None:
        """拷贝中断时按台账记录的最新偏移续传，最多重试 copy_retries 次。"""
        record = kwargs["record"]
        attempt = 0
        while True:
            try:
                self._transfer(**kwargs)
                return
            except CopyCancelled:
                raise
            except Exception as exc:  # noqa: BLE001 - 失败后按偏移续传
                attempt += 1
                record.retry_count = attempt
                self._save(record, phase="copying")
                if self.is_data_path_error(str(exc)):
                    # 数据面不通是环境问题（跨云浮动 IP / 网络隔离），重试只是白等。
                    target_node = kwargs.get("target_node")
                    raise RuntimeError(
                        self.data_path_hint(
                            getattr(target_node, "data_address", ""),
                            getattr(target_node, "data_port", 0),
                        )
                    ) from exc
                if attempt > self.copy_retries:
                    raise
                logging.warning(
                    "[MIGRATION] 块拷贝失败，第 %s/%s 次重试: %s",
                    attempt,
                    self.copy_retries,
                    exc,
                )
                latest = self.ledger.get(self.job_id, record.volume_id)
                if latest is not None:
                    kwargs["offset"] = max(
                        int(latest.copied_bytes or 0), int(kwargs["offset"])
                    )

    # 数据面连不通的报错特征：Errno 113/111、DNS 解析失败、连接超时。
    _DATA_PATH_PATTERNS = (
        "no route to host",
        "errno 113",
        "connection refused",
        "errno 111",
        "name or service not known",
        "no address associated with hostname",
        "connection timed out",
        "errno 110",
    )

    @classmethod
    def is_data_path_error(cls, detail: str) -> bool:
        """判断失败是否属于"两台中转机之间连不通"。"""
        text = str(detail or "").lower()
        return any(pattern in text for pattern in cls._DATA_PATH_PATTERNS)

    @staticmethod
    def data_path_hint(address: str, port: int) -> str:
        """给运维可执行的提示，而不是一串 socket 报错。"""
        return (
            f"源端中转机无法连接目标端中转机 {address}:{port}（数据面不通）。"
            "两侧中转机不在同一二层时会报 No route to host：跨云迁移请为两侧中转机"
            "配置「数据面浮动 IP 网络」，并确认两侧外部网络之间三层可达、"
            "安全组放通该端口；确认后重跑作业。"
        )

    def _verify(
        self,
        *,
        record: Any,
        source_node: Any,
        target_node: Any,
        source_device: str,
        target_device: str,
    ) -> None:
        """两端各算一次摘要并比对；整卷校验由 full_verify 打开。"""
        window = 0 if self.full_verify else TAIL_WINDOW
        self._save(record, phase="verifying")
        ticket = uuid.uuid4().hex
        source_task_id = f"{ticket}-vs"
        target_task_id = f"{ticket}-vt"
        common: dict[str, Any] = {
            "kind": "verify",
            "job_id": self.job_id,
            "volume_id": record.volume_id,
            "window": window,
        }
        self.state.enqueue(
            dict(
                common,
                task_id=source_task_id,
                role="source",
                agent_name=source_node.name,
                path=source_device,
            )
        )
        self.state.enqueue(
            dict(
                common,
                task_id=target_task_id,
                role="target",
                agent_name=target_node.name,
                path=target_device,
            )
        )
        wait_for(
            lambda: self.state.task_result(source_task_id) is not None
            and self.state.task_result(target_task_id) is not None,
            timeout=self._bounded_timeout(),
            interval=self.poll_interval,
            sleeper=self.sleeper,
            message="完成校验任务超时未返回结果",
        )
        source_result = self.state.task_result(source_task_id) or {}
        target_result = self.state.task_result(target_task_id) or {}
        if source_result.get("status") != "done" or target_result.get("status") != "done":
            raise RuntimeError(
                "完成校验失败: "
                f"source={source_result.get('status')} target={target_result.get('status')}"
            )
        if int(target_result.get("size") or 0) < int(source_result.get("size") or 0):
            raise RuntimeError("目标卷容量小于源卷，拷贝不完整")
        if source_result.get("digest") != target_result.get("digest"):
            raise RuntimeError("源/目标端到端校验摘要不一致，目标数据不可信")

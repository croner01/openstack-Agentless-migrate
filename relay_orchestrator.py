"""单卷中转编排：快照派生、挂载、下发任务、等待完成、清理。"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from relay_protocol import DEFAULT_CHUNK, agent_supports_sparse
from relay_transfer import TAIL_WINDOW
from relay_volumes import VolumeLifecycle
from relay_wait import wait_for

GIB = 1024 ** 3


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
        result_timeout: float = 6 * 3600.0,
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
        self.clock = clock

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
    ) -> PreparedVolume:
        """只做「打快照 → 派生拷贝源 → 建目标空白卷」。

        没有数据面流量，多块盘可以并发准备；失败时把已经建出来的快照与
        派生卷回收，不留占配额的空壳。
        """
        record = self.record(volume)
        if not record.vm_id:
            # 台账里的 vm_id 就是页面展示用的 VM 名（relay 通道此前一直是空串）。
            record.vm_id = vm_name
        copy = None
        self._save(record, phase="snapshotting")
        try:
            copy = self.lifecycle.create_source_copy(
                volume_id=volume.source_volume_id,
                vm_name=vm_name,
                index=index,
                size=int(volume.size or 0),
                volume_type=source_volume_type or self.source_volume_type or None,
            )
            self._save(
                record,
                phase="cloning",
                snapshot_id=copy.snapshot_id,
                derived_volume_id=copy.derived_volume_id,
            )
            target_volume_id = self.lifecycle.create_target_volume(
                name=f"{vm_name}-vol-{index}",
                size=int(volume.size or 0),
                volume_type=target_volume_type or self.target_volume_type or None,
            )
            self._save(
                record, phase="attaching_target", target_volume_id=target_volume_id
            )
        except Exception:
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

    def transfer(self, prepared: PreparedVolume) -> dict[str, Any]:
        """挂载两侧卷、搬数据、清理，返回目标卷信息。"""
        volume = prepared.volume
        record = prepared.record
        copy = prepared.copy
        target_volume_id = prepared.target_volume_id
        source_node = target_node = None
        cleaned = False
        started_bytes = max(int(record.copied_bytes or 0), 0)
        try:
            source_node = self._acquire_with_wait(
                self.source_pool, volume.source_volume_id
            )
            target_node = self._acquire_with_wait(
                self.target_pool, volume.source_volume_id
            )
            if source_node is None or target_node is None:
                source_state = ", ".join(_describe(self.source_pool)) or "无节点"
                target_state = ", ".join(_describe(self.target_pool)) or "无节点"
                raise RuntimeError(
                    "中转机池没有空闲机器，无法继续拷贝"
                    f"（源端: {source_state}；目标端: {target_state}）"
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
            self._save(record, phase="done")
            return {
                "target_volume_id": target_volume_id,
                "source_volume_id": volume.source_volume_id,
                "size": int(volume.size or 0),
            }
        except Exception:
            self._save(record, phase="failed")
            raise
        finally:
            # copy 一旦建出来就必须回收：取不到中转机槽位时 source_node 为 None，
            # 旧写法会跳过清理，把派生卷和快照留在源云里等作业收尾。
            if copy is not None and not cleaned:
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
        等前一个卷做完就能继续。slot_wait_timeout=0 时保持旧的立即失败行为。
        """
        node = pool.acquire(task_id)
        if node is not None or self.slot_wait_timeout <= 0:
            return node
        deadline = self.clock() + float(self.slot_wait_timeout)
        waited = 0.0
        while node is None and self.clock() < deadline:
            if int(waited) % 60 == 0:
                logging.info(
                    "[MIGRATION] 中转机池已满，等待空闲槽位 %.0fs/%s（%s）",
                    waited,
                    f"{self.slot_wait_timeout:.0f}s",
                    "、".join(_describe(pool)) or "无节点",
                )
            self.sleeper(self.poll_interval)
            waited += self.poll_interval
            node = pool.acquire(task_id)
        return node

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

        只看 result_timeout（默认 6 小时）不够：数据面真卡住时页面和平台都会
        一直显示"拷贝中"。这里要求 copied_bytes 每 stall_timeout 秒至少涨一次，
        任一端出结果（失败也算）立即收尾。
        """

        def finished() -> bool:
            # 任一端失败就立刻收尾：否则另一端可能永远卡在 accept/发送。
            for task_id in task_ids:
                result = self.state.task_result(task_id)
                if result is not None and result.get("status") != "done":
                    return True
            return all(self.state.task_result(task_id) is not None for task_id in task_ids)

        deadline = self.clock() + self.result_timeout
        last_bytes = int(getattr(record, "copied_bytes", 0) or 0)
        last_change = self.clock()
        while True:
            if finished():
                return
            now = self.clock()
            if now >= deadline:
                raise TimeoutError("块拷贝任务超时未返回结果")
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
            timeout=self.result_timeout,
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

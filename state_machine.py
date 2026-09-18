from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class JobStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class VmStatus(str, Enum):
    QUEUED = "queued"
    PREFLIGHT_FAILED = "preflight_failed"
    CREATING_TARGET_BFV = "creating_target_bfv"
    STOPPING_TARGET = "stopping_target"
    STOPPING_SOURCE = "stopping_source"
    COLLECTING_VOLUMES = "collecting_volumes"
    PRECOPYING = "precopying"
    AWAITING_CUTOVER = "awaiting_cutover"
    COPYING_VOLUMES = "copying_volumes"
    #: 中转机通道专用：其余盘都传完了，只剩失败盘在等人工点「重试」。
    AWAITING_DISK_RETRY = "awaiting_disk_retry"
    STARTING_TARGET = "starting_target"
    VERIFYING = "verifying"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    AWAITING_FLAVOR = "awaiting_flavor"


class VolumeStatus(str, Enum):
    PENDING = "pending"
    COPYING = "copying"
    # 基像或某一轮增量已经写完、暂存卷数据可用，正在等下一步动作
    # （用户点「同步一次」或「开始切换」）；与 SUCCESS（已换入目标卷）区分开。
    READY = "ready"
    SUCCESS = "success"
    FAILED = "failed"


#: 卷级迁移快照回收状态（`VolumeTask.cleanup_status`）。
CLEANUP_NONE = "none"
CLEANUP_PENDING = "pending"
CLEANUP_CLEANED = "cleaned"
#: 源卷已被删除（迁移完成后清理源机是常规操作）：它上面的 mig-* 快照随卷
#: 消失，无从回收，再扫多少次也不会有新结论，GC 视同终态跳过。
CLEANUP_GONE = "gone"
#: 再扫也不会有新结论的终态集合。
TERMINAL_CLEANUP_STATUSES = frozenset({CLEANUP_CLEANED, CLEANUP_GONE})


class MigrationMode(str, Enum):
    """迁移功能项：全量（既有）与增量（新增），两者相互独立。"""

    FULL = "full"
    INCREMENTAL = "incremental"

    @classmethod
    def parse(cls, raw: object) -> "MigrationMode":
        try:
            return cls(str(raw or cls.FULL.value).strip().lower())
        except ValueError:
            return cls.FULL


@dataclass
class VolumeTask:
    source_volume_id: str
    source_rbd_name: str
    target_volume_id: str | None = None
    target_rbd_name: str | None = None
    size: int = 0
    status: VolumeStatus = VolumeStatus.PENDING
    error: str | None = None
    progress_percent: float = 0.0
    throughput_mb_s: float | None = None
    progress_label: str = ""
    source_pool: str = ""
    target_pool: str = ""
    mode: MigrationMode = MigrationMode.FULL
    layout: dict[str, Any] | None = None
    snapshots: list[dict[str, Any]] = field(default_factory=list)
    cleanup_status: str = CLEANUP_NONE

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_volume_id": self.source_volume_id,
            "source_rbd_name": self.source_rbd_name,
            "target_volume_id": self.target_volume_id,
            "target_rbd_name": self.target_rbd_name,
            "size": self.size,
            "status": self.status.value,
            "error": self.error,
            "progress_percent": self.progress_percent,
            "throughput_mb_s": self.throughput_mb_s,
            "progress_label": self.progress_label,
            "source_pool": self.source_pool,
            "target_pool": self.target_pool,
            "mode": self.mode.value,
            "layout": self.layout,
            "snapshots": self.snapshots,
            "cleanup_status": self.cleanup_status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VolumeTask":
        return cls(
            source_volume_id=data["source_volume_id"],
            source_rbd_name=data["source_rbd_name"],
            target_volume_id=data.get("target_volume_id"),
            target_rbd_name=data.get("target_rbd_name"),
            size=int(data.get("size") or 0),
            status=VolumeStatus(data.get("status") or VolumeStatus.PENDING.value),
            error=data.get("error"),
            progress_percent=float(data.get("progress_percent") or 0.0),
            throughput_mb_s=(
                float(data["throughput_mb_s"])
                if data.get("throughput_mb_s") is not None
                else None
            ),
            progress_label=data.get("progress_label") or "",
            source_pool=data.get("source_pool") or "",
            target_pool=data.get("target_pool") or "",
            mode=MigrationMode.parse(data.get("mode")),
            layout=data.get("layout"),
            snapshots=data.get("snapshots") or [],
            cleanup_status=data.get("cleanup_status") or "none",
        )


@dataclass
class VmTask:
    name: str
    target_az: str
    mode: MigrationMode = MigrationMode.FULL
    target_image: str | None = None
    target_flavor: str | None = None
    status: VmStatus = VmStatus.QUEUED
    phase: str = "queued"
    #: 进入当前阶段的时间（ISO8601）。诊断页据此算"卡在这个阶段多久了"；
    #: 老作业反序列化时没有这个字段，前端按 None 处理即可。
    phase_since: str | None = None
    error: str | None = None
    source_server_id: str | None = None
    target_server_id: str | None = None
    target_network_ports: list = field(default_factory=list)
    created_resources: list = field(default_factory=list)
    volumes: list[VolumeTask] = field(default_factory=list)
    #: 中转机通道逐块盘的迁移结果（``volume_id``/``target_volume_id``/``size``/
    #: ``status``/``error``）。中转机的运行时在作业结束后会被回收，卷信息只存在
    #: 台账里会被 7 天保留期清掉；挂在这里才能让"这台 VM 几块盘完成、几块失败"
    #: 跟着作业一起留存并展示。
    relay_disks: list[dict[str, Any]] = field(default_factory=list)
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float | None = None
    target_floating_ips: list = field(default_factory=list)
    source_ips: list = field(default_factory=list)
    target_ips: list = field(default_factory=list)
    cutover_requested: bool = False
    sync_requested: bool = False
    #: 中转机通道：用户在页面点了「重试失败盘」。volume_ids 为空表示重试全部失败盘。
    disk_retry_requested: bool = False
    disk_retry_volume_ids: list[str] = field(default_factory=list)
    delta_rounds_done: int = 0
    delta_rounds_max: int = 0
    delta_last_bytes: int | None = None
    delta_threshold_bytes: int = 0
    delta_interval_seconds: float = 0.0
    delta_cutover_mode: str = ""
    #: 迁移收尾后是否把目标机开起来。关掉时数据照常迁移、状态照常成功，
    #: 但目标机保持 SHUTOFF，等人工开机（默认开机，与历史行为一致）。
    start_target: bool = True

    def can_start_target(self) -> bool:
        return bool(self.volumes) and all(
            volume.status == VolumeStatus.SUCCESS for volume in self.volumes
        )

    def set_phase(self, phase: str) -> None:
        """记录阶段与进入时间：诊断页据此判断卡在某个阶段多久了。"""
        self.phase = phase
        self.phase_since = datetime.now(timezone.utc).isoformat()

    def mark_failed(self, message: str) -> None:
        self.status = VmStatus.FAILED
        self.set_phase("failed")
        self.error = message
        self.finish(interrupted=True)

    def mark_success(self) -> None:
        self.status = VmStatus.SUCCESS
        self.set_phase("success")
        self.error = None
        self.finish(interrupted=False)

    def mark_cancelled(self, message: str = "任务被用户取消") -> None:
        self.status = VmStatus.CANCELLED
        self.set_phase("cancelled")
        self.error = message
        self.finish(interrupted=True)

    def start(self) -> None:
        if not self.started_at:
            self.started_at = datetime.now(timezone.utc).isoformat()

    def finish(self, interrupted: bool = False) -> None:
        now = datetime.now(timezone.utc)
        self.finished_at = now.isoformat()
        if self.started_at:
            try:
                started = datetime.fromisoformat(self.started_at)
                self.duration_seconds = round((now - started).total_seconds(), 1)
            except (TypeError, ValueError):
                self.duration_seconds = None
        elif interrupted:
            self.started_at = now.isoformat()
            self.duration_seconds = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "target_az": self.target_az,
            "mode": self.mode.value,
            "target_image": self.target_image,
            "target_flavor": self.target_flavor,
            "status": self.status.value,
            "phase": self.phase,
            "phase_since": self.phase_since,
            "error": self.error,
            "source_server_id": self.source_server_id,
            "target_server_id": self.target_server_id,
            "target_network_ports": self.target_network_ports,
            "created_resources": self.created_resources,
            "volumes": [volume.to_dict() for volume in self.volumes],
            "relay_disks": self.relay_disks,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "target_floating_ips": self.target_floating_ips,
            "source_ips": self.source_ips,
            "target_ips": self.target_ips,
            "cutover_requested": self.cutover_requested,
            "sync_requested": self.sync_requested,
            "disk_retry_requested": self.disk_retry_requested,
            "disk_retry_volume_ids": self.disk_retry_volume_ids,
            "delta_rounds_done": self.delta_rounds_done,
            "delta_rounds_max": self.delta_rounds_max,
            "delta_last_bytes": self.delta_last_bytes,
            "delta_threshold_bytes": self.delta_threshold_bytes,
            "delta_interval_seconds": self.delta_interval_seconds,
            "delta_cutover_mode": self.delta_cutover_mode,
            "start_target": self.start_target,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VmTask":
        return cls(
            name=data["name"],
            target_az=data["target_az"],
            mode=MigrationMode.parse(data.get("mode")),
            target_image=data.get("target_image"),
            target_flavor=data.get("target_flavor"),
            status=VmStatus(data.get("status") or VmStatus.QUEUED.value),
            phase=data.get("phase") or "queued",
            phase_since=data.get("phase_since"),
            error=data.get("error"),
            source_server_id=data.get("source_server_id"),
            target_server_id=data.get("target_server_id"),
            target_network_ports=data.get("target_network_ports") or [],
            created_resources=data.get("created_resources") or [],
            volumes=[
                VolumeTask.from_dict(volume) for volume in data.get("volumes") or []
            ],
            relay_disks=data.get("relay_disks") or [],
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            duration_seconds=data.get("duration_seconds"),
            target_floating_ips=data.get("target_floating_ips") or [],
            source_ips=data.get("source_ips") or [],
            target_ips=data.get("target_ips") or [],
            cutover_requested=bool(data.get("cutover_requested")),
            sync_requested=bool(data.get("sync_requested")),
            disk_retry_requested=bool(data.get("disk_retry_requested")),
            disk_retry_volume_ids=[
                str(item) for item in data.get("disk_retry_volume_ids") or []
            ],
            delta_rounds_done=int(data.get("delta_rounds_done") or 0),
            delta_rounds_max=int(data.get("delta_rounds_max") or 0),
            delta_last_bytes=(
                int(data["delta_last_bytes"])
                if data.get("delta_last_bytes") is not None
                else None
            ),
            delta_threshold_bytes=int(data.get("delta_threshold_bytes") or 0),
            delta_interval_seconds=float(data.get("delta_interval_seconds") or 0.0),
            delta_cutover_mode=str(data.get("delta_cutover_mode") or ""),
            start_target=bool(data.get("start_target", True)),
        )


@dataclass
class MigrationJob:
    id: str
    status: JobStatus = JobStatus.RUNNING
    error: str | None = None
    cancelled: bool = False
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    vms: list[VmTask] = field(default_factory=list)

    def refresh_status(self) -> str:
        terminal = {
            VmStatus.SUCCESS,
            VmStatus.FAILED,
            VmStatus.PREFLIGHT_FAILED,
            VmStatus.CANCELLED,
        }
        if self.vms and all(vm.status in terminal for vm in self.vms):
            self.status = (
                JobStatus.CANCELLED
                if any(vm.status == VmStatus.CANCELLED for vm in self.vms)
                else JobStatus.COMPLETED
            )
        return self.status.value

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status.value,
            "error": self.error,
            "cancelled": self.cancelled,
            "created_at": self.created_at,
            "vms": [vm.to_dict() for vm in self.vms],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MigrationJob":
        return cls(
            id=data["id"],
            status=JobStatus(data.get("status") or JobStatus.RUNNING.value),
            error=data.get("error"),
            cancelled=bool(data.get("cancelled")),
            created_at=data.get("created_at") or "",
            vms=[VmTask.from_dict(vm) for vm in data.get("vms") or []],
        )

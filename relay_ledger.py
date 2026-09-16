"""中转机迁移台账：卷任务记录的持久化。

台账是"资源不堆积"的前提：凡由平台创建的资源都必须先登记再操作，
服务启动时用台账与云上实际资源对账。
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

from json_store import (
    atomic_write_json,
    dump_dataclass_records,
    load_dataclass_records,
)


@dataclass
class VolumeTaskRecord:
    job_id: str
    vm_id: str
    volume_id: str
    phase: str = "queued"
    snapshot_id: str = ""
    derived_volume_id: str = ""
    target_volume_id: str = ""
    source_relay_id: str = ""
    target_relay_id: str = ""
    copied_bytes: int = 0
    total_bytes: int = 0
    skipped_bytes: int = 0
    attach_state: str = ""
    retry_count: int = 0
    created_at: float = 0.0
    updated_at: float = 0.0
    source_cloud: str = ""
    target_cloud: str = ""

    @property
    def key(self) -> str:
        return f"{self.job_id}:{self.volume_id}"


class Ledger:
    """JSON 落盘的卷任务台账，路径形如 uploads/relay-ledger-<job>.json。"""

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._records: dict[str, VolumeTaskRecord] = {}

    @classmethod
    def load(cls, path: Path | str) -> "Ledger":
        ledger = cls(path)
        for record in load_dataclass_records(
            ledger.path, key="records", cls=VolumeTaskRecord
        ):
            ledger._records[record.key] = record
        return ledger

    def get(self, job_id: str, volume_id: str) -> VolumeTaskRecord | None:
        return self._records.get(f"{job_id}:{volume_id}")

    def all(self) -> list[VolumeTaskRecord]:
        return list(self._records.values())

    def prune(
        self,
        *,
        prunable_phases: set[str],
        max_age_seconds: float,
        now: float | None = None,
    ) -> int:
        """回收已完结且超过保留期的台账记录，避免台账文件无限增长。

        只清理明确完结的阶段（done/cleaned）；"failed_retained" 是留给对账
        重试的指针，不能在这里丢掉。
        """
        current = time.time() if now is None else now
        cutoff = current - max_age_seconds
        stale = [
            key
            for key, record in self._records.items()
            if record.phase in prunable_phases
            and (record.updated_at or record.created_at or 0.0) < cutoff
        ]
        for key in stale:
            self._records.pop(key, None)
        return len(stale)

    def upsert(self, record: VolumeTaskRecord) -> VolumeTaskRecord:
        existing = self._records.get(record.key)
        if existing is not None and not record.created_at:
            record.created_at = existing.created_at
        self._records[record.key] = record
        return record

    def save(self) -> None:
        atomic_write_json(
            self.path,
            dump_dataclass_records(list(self._records.values()), key="records"),
            prefix=".relay-ledger-",
        )

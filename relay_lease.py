"""中转机租约：作业对槽位的占用记录，落盘 uploads/relay-leases.json（0600）。

租约是缩容判定与孤儿识别的依据：有未释放租约的节点永不回收。
"""
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from json_store import (
    atomic_write_json,
    dump_dataclass_records,
    load_dataclass_records,
)


@dataclass
class LeaseRecord:
    lease_id: str
    job_id: str
    node_id: str
    role: str
    tenant_key: str
    vm_id: str = ""
    volume_id: str = ""
    data_port: int = 0
    acquired_at: float = 0.0
    released_at: float = 0.0

    @property
    def active(self) -> bool:
        return not self.released_at


class LeaseStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._records: dict[str, LeaseRecord] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "LeaseStore":
        store = cls(path)
        for record in load_dataclass_records(
            store.path, key="leases", cls=LeaseRecord
        ):
            store._records[record.lease_id] = record
        return store

    def get(self, lease_id: str) -> LeaseRecord | None:
        return self._records.get(lease_id)

    def all(self) -> list[LeaseRecord]:
        return list(self._records.values())

    def acquire(
        self,
        *,
        job_id: str,
        node_id: str,
        role: str,
        tenant_key: str,
        now: float | None = None,
        vm_id: str = "",
        volume_id: str = "",
        data_port: int = 0,
    ) -> LeaseRecord:
        record = LeaseRecord(
            lease_id=uuid.uuid4().hex,
            job_id=job_id,
            node_id=node_id,
            role=role,
            tenant_key=tenant_key,
            vm_id=vm_id,
            volume_id=volume_id,
            data_port=int(data_port or 0),
            acquired_at=time.time() if now is None else now,
        )
        self._records[record.lease_id] = record
        return record

    def release(self, lease_id: str, *, now: float | None = None) -> LeaseRecord | None:
        record = self._records.get(lease_id)
        if record is None:
            return None
        record.released_at = time.time() if now is None else now
        return record

    def release_job(self, job_id: str, *, now: float | None = None) -> list[LeaseRecord]:
        released: list[LeaseRecord] = []
        for record in self.active_for_job(job_id):
            self.release(record.lease_id, now=now)
            released.append(record)
        return released

    def active_for_node(self, node_id: str) -> list[LeaseRecord]:
        return [
            record
            for record in self._records.values()
            if record.node_id == node_id and record.active
        ]

    def active_for_job(self, job_id: str) -> list[LeaseRecord]:
        return [
            record
            for record in self._records.values()
            if record.job_id == job_id and record.active
        ]

    def active_count_for_role(self, tenant_key: str, role: str) -> int:
        return len(
            [
                record
                for record in self._records.values()
                if record.tenant_key == tenant_key
                and record.role == role
                and record.active
            ]
        )

    def idle_since(self, node_id: str) -> float:
        """返回节点最近一次释放时间；仍有活跃租约时返回 0。"""
        if self.active_for_node(node_id):
            return 0.0
        timestamps = [
            record.released_at
            for record in self._records.values()
            if record.node_id == node_id and record.released_at
        ]
        return max(timestamps) if timestamps else 0.0

    def save(self) -> None:
        atomic_write_json(
            self.path,
            dump_dataclass_records(list(self._records.values()), key="leases"),
            prefix=".relay-leases-",
        )

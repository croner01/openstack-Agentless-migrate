"""池级建机参数：扩容时新建节点所需的镜像、flavor、网络与容量策略。

不保存 admin_password：SSH 密码按节点加密存储，扩容时从池内既有节点解密复用，
池为空且未提供密码时由 API 层拒绝建机。
"""
from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from json_store import (
    atomic_write_json,
    known_field_names,
    load_dataclass_records,
)


@dataclass
class PoolProfile:
    tenant_key: str
    role: str
    az: str
    image: str
    flavor: str
    network: str
    subnet: str = ""
    system_volume_type: str = ""
    data_floating_network: str = ""
    slots_per_node: int = 5
    max_nodes: int = 6
    min_nodes: int = 1
    idle_scale_down_seconds: float = 86400.0
    data_port: int = 9200
    platform_url: str = ""
    ssh_public_key: str = ""
    # 仅内存中使用：SSH 密码按节点加密存储，绝不落盘/不回显（见 save()）。
    admin_password: str = field(default="", repr=False)
    updated_at: float = 0.0

    @property
    def pool_key(self) -> tuple[str, str, str]:
        return (self.tenant_key, self.role, self.az)


class PoolProfileStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._records: dict[tuple[str, str, str], PoolProfile] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "PoolProfileStore":
        store = cls(path)
        for profile in load_dataclass_records(
            store.path, key="pools", cls=PoolProfile
        ):
            store._records[profile.pool_key] = profile
        return store

    def get(self, tenant_key: str, role: str, az: str) -> PoolProfile | None:
        return self._records.get((tenant_key, role, az))

    def all(self) -> list[PoolProfile]:
        return list(self._records.values())

    def for_tenant(self, tenant_key: str) -> list[PoolProfile]:
        return [item for item in self._records.values() if item.tenant_key == tenant_key]

    def upsert(self, profile: PoolProfile) -> PoolProfile:
        self._records[profile.pool_key] = profile
        return profile

    def upsert_profile(self, payload: dict[str, Any]) -> PoolProfile:
        """从 API 表单字典构建/更新池参数，忽略未知字段。"""
        known = known_field_names(PoolProfile)
        data = {key: value for key, value in payload.items() if key in known}
        for key in (
            "tenant_key",
            "role",
            "az",
            "image",
            "flavor",
            "network",
        ):
            data[key] = str(data.get(key) or "").strip()
        data["system_volume_type"] = str(data.get("system_volume_type") or "").strip()
        if not all(
            data.get(key)
            for key in (
                "tenant_key",
                "role",
                "az",
                "image",
                "flavor",
                "network",
                "system_volume_type",
            )
        ):
            raise ValueError(
                "tenant_key/role/az/image/flavor/network/system_volume_type 必填"
                "（中转机走云硬盘启动，系统盘类型必选）"
            )
        for key in ("slots_per_node", "max_nodes", "min_nodes", "data_port"):
            if data.get(key) not in (None, ""):
                data[key] = int(data[key])
        if data.get("idle_scale_down_seconds") not in (None, ""):
            data["idle_scale_down_seconds"] = float(data["idle_scale_down_seconds"])
        return self.upsert(PoolProfile(**data))

    def remove(self, tenant_key: str, role: str, az: str) -> bool:
        return self._records.pop((tenant_key, role, az), None) is not None

    def save(self) -> None:
        # admin_password 只在建机那一刻需要，落盘会变成本地明文口令。
        # 节点建好后密码已加密存进节点记录，扩容时从既有节点解密复用。
        records = []
        for profile in self._records.values():
            item = asdict(profile)
            item.pop("admin_password", None)
            records.append(item)
        atomic_write_json(
            self.path,
            {"pools": records},
            prefix=".relay-pools-",
        )

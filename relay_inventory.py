"""常驻中转机节点清单：平台级资源，落盘 uploads/relay-nodes.json（0600）。

清单是跨重启恢复的唯一依据，所有字段必须可 JSON 序列化。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from json_store import (
    atomic_write_json,
    dump_dataclass_records,
    load_dataclass_records,
)


@dataclass
class RelayNodeRecord:
    node_id: str
    name: str
    role: str
    tenant_key: str
    az: str
    server_id: str = ""
    port_id: str = ""
    network: str = ""
    subnet: str = ""
    fixed_ip: str = ""
    flavor: str = ""
    image: str = ""
    system_volume_type: str = ""
    root_volume_id: str = ""
    floating_ip_id: str = ""
    floating_ip: str = ""
    slots_total: int = 5
    slots_used: int = 0
    agent_version: str = ""
    token_enc: str = ""
    ssh_password_enc: str = ""
    state: str = "provisioning"
    last_seen: float = 0.0
    idle_since: float = 0.0
    data_port_base: int = 9200
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def pool_key(self) -> tuple[str, str, str]:
        return (self.tenant_key, self.role, self.az)


class NodeInventory:
    """节点清单的内存视图 + 原子落盘。"""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._records: dict[str, RelayNodeRecord] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "NodeInventory":
        inventory = cls(path)
        for record in load_dataclass_records(
            inventory.path, key="nodes", cls=RelayNodeRecord
        ):
            inventory._records[record.node_id] = record
        return inventory

    def get(self, node_id: str) -> RelayNodeRecord | None:
        return self._records.get(node_id)

    def all(self) -> list[RelayNodeRecord]:
        return list(self._records.values())

    def upsert(self, record: RelayNodeRecord) -> RelayNodeRecord:
        existing = self._records.get(record.node_id)
        if existing is not None and not record.created_at:
            record.created_at = existing.created_at
        self._records[record.node_id] = record
        return record

    def remove(self, node_id: str) -> RelayNodeRecord | None:
        return self._records.pop(node_id, None)

    def nodes_in_pool(
        self, tenant_key: str, role: str, az: str
    ) -> list[RelayNodeRecord]:
        return [
            record
            for record in self._records.values()
            if record.pool_key == (tenant_key, role, az)
        ]

    def nodes_for_role(self, tenant_key: str, role: str) -> list[RelayNodeRecord]:
        return [
            record
            for record in self._records.values()
            if record.tenant_key == tenant_key and record.role == role
        ]

    def count_for_role(self, tenant_key: str, role: str) -> int:
        return len(self.nodes_for_role(tenant_key, role))

    def save(self) -> None:
        atomic_write_json(
            self.path,
            dump_dataclass_records(list(self._records.values()), key="nodes"),
            prefix=".relay-nodes-",
        )

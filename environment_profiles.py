"""环境档案：源/目标 OpenStack 凭据与 Ceph conf 的加密存储。

明文只在内存出现，落盘字段一律是密文。复用中转机已有的 Sealer 与主密钥
体系（``MIGRATION_SECRET_KEY`` / ``uploads/relay-master-key``），因此不新增
必填环境变量；密码与 conf 留空表示"沿用旧值"，避免前端回显密钥。
"""
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from json_store import (
    atomic_write_json,
    dump_dataclass_records,
    load_dataclass_records,
)
from relay_credentials import Sealer

SECRET_FIELDS = (
    "source_password",
    "target_password",
    "source_ceph_conf",
    "target_ceph_conf",
)

#: 新建档案时允许显式设置的非密文字段。
PLAIN_FIELDS = (
    "source_auth_url",
    "source_project_name",
    "source_username",
    "source_user_domain_name",
    "source_project_domain_name",
    "source_project_id",
    "target_auth_url",
    "target_project_name",
    "target_username",
    "target_user_domain_name",
    "target_project_domain_name",
    "target_project_id",
    "source_ceph_pool",
    "target_ceph_pool",
    "target_volume_type",
)


@dataclass
class EnvironmentProfile:
    profile_id: str
    name: str
    source_auth_url: str = ""
    source_project_name: str = ""
    source_username: str = ""
    source_user_domain_name: str = "Default"
    source_project_domain_name: str = "Default"
    source_project_id: str = ""
    source_password: str = ""
    target_auth_url: str = ""
    target_project_name: str = ""
    target_username: str = ""
    target_user_domain_name: str = "Default"
    target_project_domain_name: str = "Default"
    target_project_id: str = ""
    target_password: str = ""
    source_ceph_conf: str = ""
    target_ceph_conf: str = ""
    source_ceph_pool: str = "volumes"
    target_ceph_pool: str = "volumes"
    target_volume_type: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0


class EnvironmentProfileStore:
    """按 profile_id 保存环境档案；密文字段用 AAD 绑定到具体档案与字段名。"""

    def __init__(self, path: str | os.PathLike[str], sealer: Sealer):
        self.path = Path(path)
        self.sealer = sealer
        self._records: dict[str, EnvironmentProfile] = {}

    @classmethod
    def load(
        cls, path: str | os.PathLike[str], sealer: Sealer
    ) -> "EnvironmentProfileStore":
        store = cls(path, sealer)
        for profile in load_dataclass_records(
            store.path, key="profiles", cls=EnvironmentProfile
        ):
            store._records[profile.profile_id] = profile
        return store

    def list_public(self) -> list[dict[str, Any]]:
        return [
            self._public(item)
            for item in sorted(self._records.values(), key=lambda p: p.name)
        ]

    def get(self, profile_id: str) -> EnvironmentProfile | None:
        return self._records.get(str(profile_id))

    def reveal(self, profile: EnvironmentProfile, field: str) -> str:
        """解密单个密文字段；空值返回空串，密文损坏抛 CredentialError。"""
        token = getattr(profile, field, "")
        if not token:
            return ""
        return self.sealer.unseal(token, aad=self._aad(profile.profile_id, field))

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        profile_id = str(payload.get("profile_id") or "").strip()
        existing = self._records.get(profile_id) if profile_id else None
        if profile_id and existing is None:
            raise ValueError(f"档案不存在：{profile_id}")
        if not profile_id:
            profile_id = uuid.uuid4().hex

        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("档案名称不能为空")
        if any(
            item.name == name and item.profile_id != profile_id
            for item in self._records.values()
        ):
            raise ValueError(f"已存在同名档案：{name}")

        profile = existing or EnvironmentProfile(profile_id=profile_id, name=name)
        profile.name = name
        for field in PLAIN_FIELDS:
            if field in payload:
                setattr(profile, field, str(payload.get(field) or ""))
        for field in SECRET_FIELDS:
            text = str(payload.get(field) or "")
            if text:
                setattr(profile, field, self._seal(profile_id, field, text))

        now = time.time()
        if not profile.created_at:
            profile.created_at = now
        profile.updated_at = now
        self._records[profile_id] = profile
        return self._public(profile)

    def delete(self, profile_id: str) -> bool:
        return self._records.pop(str(profile_id), None) is not None

    def flush(self) -> None:
        atomic_write_json(
            self.path,
            dump_dataclass_records(list(self._records.values()), key="profiles"),
            prefix=".environment-profiles-",
        )

    def _seal(self, profile_id: str, field: str, plaintext: str) -> str:
        return self.sealer.seal(plaintext, aad=self._aad(profile_id, field))

    @staticmethod
    def _aad(profile_id: str, field: str) -> str:
        return f"{profile_id}:{field}"

    def _public(self, profile: EnvironmentProfile) -> dict[str, Any]:
        result = {
            key: value
            for key, value in vars(profile).items()
            if key not in SECRET_FIELDS
        }
        for prefix in ("source", "target"):
            result[f"has_{prefix}_password"] = bool(
                getattr(profile, f"{prefix}_password")
            )
            result[f"has_{prefix}_ceph_conf"] = bool(
                getattr(profile, f"{prefix}_ceph_conf")
            )
        return result

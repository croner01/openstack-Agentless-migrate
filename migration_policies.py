"""迁移策略模板：把一整套批量迁移参数命名保存，下次一键套用。

批量迁移真正费时的不是"点开始"，而是每次都要重填几十个参数（并发、限速、
超时、中转机池、通道）并逐台选 AZ/镜像/网络。这里按 HCX profile / AWS MGN
template 的做法，把参数快照与"规则映射"一起存成可复用的策略：

* ``params`` 只收白名单里的作业级表单字段，避免把凭据/令牌等敏感字段写盘；
* ``rules`` 是"源 VM 特征 → 目标参数"的映射规则，由前端做 dry-run 预览后
  再落到每台 VM 的逐台覆盖上，服务端只负责校验与持久化。

落盘与其它存储一致：原子写 + 0600（复用 ``json_store``）。
"""
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from json_store import (
    atomic_write_json,
    dump_dataclass_records,
    load_dataclass_records,
)

#: 策略允许携带的作业级表单字段。凭据类字段（密码/令牌/ceph conf）绝不入库。
POLICY_PARAM_FIELDS = (
    "data_channel",
    "vm_concurrency",
    "volume_concurrency",
    "rate_limit_mb_s",
    "target_volume_type",
    "delta_rounds",
    "delta_threshold_mb",
    "delta_interval_seconds",
    "cutover_mode",
    "volume_ready_timeout",
    "relay_node_mode",
    "relay_platform_url",
    "relay_agent_install",
    "relay_data_port",
    "relay_slots_per_node",
    "relay_max_nodes",
    "relay_min_nodes",
    "relay_idle_scale_down_hours",
    "relay_ready_timeout",
    "relay_heartbeat_interval",
    "relay_heartbeat_timeout",
    "relay_copy_retries",
    "relay_stall_timeout",
    "relay_slot_wait_seconds",
    "relay_attach_ready_timeout",
    "relay_transfer_concurrency",
    "relay_disk_retry_wait_seconds",
    "relay_full_verify",
    "relay_hole_mode",
    "offpeak_window",
    "offpeak_rate_limit_mb_s",
    "relay_source_size",
    "relay_target_size",
    "relay_source_image",
    "relay_source_flavor",
    "relay_source_az",
    "relay_source_network",
    "relay_source_subnet",
    "relay_source_volume_type",
    "relay_source_system_volume_type",
    "relay_source_data_floating_network",
    "relay_target_image",
    "relay_target_flavor",
    "relay_target_az",
    "relay_target_network",
    "relay_target_subnet",
    "relay_target_volume_type",
    "relay_target_system_volume_type",
    "relay_target_data_floating_network",
)

#: 规则里允许出现的键；多出来的键直接丢掉，避免策略文件里塞进奇怪的东西。
RULE_MATCH_FIELDS = ("name_regex", "az", "status", "image", "flavor", "min_disks")
RULE_TARGET_FIELDS = ("az", "image", "flavor", "network", "channel")

MAX_RULES = 50
MAX_RULE_TEXT = 200


@dataclass
class MigrationPolicy:
    policy_id: str
    name: str
    description: str = ""
    #: 表单字段名 -> 值（字符串/布尔）；只允许 POLICY_PARAM_FIELDS 里的键。
    params: dict[str, Any] = field(default_factory=dict)
    #: 规则映射：{"match": {...}, "target": {...}, "enabled": bool}
    rules: list[dict[str, Any]] = field(default_factory=list)
    created_at: float = 0.0
    updated_at: float = 0.0


def _clean_text(value: Any, *, limit: int = MAX_RULE_TEXT) -> str:
    return str(value or "").strip()[:limit]


def normalize_params(payload: Any) -> dict[str, Any]:
    """只保留白名单字段，并统一成字符串/布尔，防止策略里带进敏感或未知键。"""
    if not isinstance(payload, dict):
        return {}
    params: dict[str, Any] = {}
    for key in POLICY_PARAM_FIELDS:
        if key not in payload:
            continue
        value = payload[key]
        if isinstance(value, bool):
            params[key] = value
            continue
        text = "" if value is None else str(value).strip()
        if text:
            params[key] = text
    return params


def normalize_rules(payload: Any) -> list[dict[str, Any]]:
    """校验并规整规则列表；非法规则整条丢弃（宁少不错）。"""
    if not isinstance(payload, list):
        return []
    rules: list[dict[str, Any]] = []
    for item in payload[:MAX_RULES]:
        if not isinstance(item, dict):
            continue
        raw_match = item.get("match")
        raw_target = item.get("target")
        if not isinstance(raw_match, dict) or not isinstance(raw_target, dict):
            continue
        match = {
            key: _clean_text(raw_match.get(key))
            for key in RULE_MATCH_FIELDS
            if _clean_text(raw_match.get(key))
        }
        target = {
            key: _clean_text(raw_target.get(key))
            for key in RULE_TARGET_FIELDS
            if _clean_text(raw_target.get(key))
        }
        if not match or not target:
            # 空匹配会命中全部 VM、空目标什么也改不了，两种都是误配置。
            continue
        rules.append(
            {
                "name": _clean_text(item.get("name")),
                "enabled": item.get("enabled") is not False,
                "match": match,
                "target": target,
            }
        )
    return rules


class MigrationPolicyStore:
    """迁移策略模板的持久化：一个 JSON 文件存全部策略。"""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._records: dict[str, MigrationPolicy] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> "MigrationPolicyStore":
        store = cls(path)
        for policy in load_dataclass_records(
            store.path, key="policies", cls=MigrationPolicy
        ):
            store._records[policy.policy_id] = policy
        return store

    def list_public(self) -> list[dict[str, Any]]:
        return [
            self._public(item)
            for item in sorted(self._records.values(), key=lambda p: p.name)
        ]

    def get(self, policy_id: str) -> MigrationPolicy | None:
        return self._records.get(str(policy_id))

    @staticmethod
    def _public(policy: MigrationPolicy) -> dict[str, Any]:
        return {
            "policy_id": policy.policy_id,
            "name": policy.name,
            "description": policy.description,
            "params": dict(policy.params or {}),
            "rules": list(policy.rules or []),
            "created_at": float(policy.created_at or 0.0),
            "updated_at": float(policy.updated_at or 0.0),
        }

    def save(self, payload: dict[str, Any]) -> MigrationPolicy:
        policy_id = _clean_text(payload.get("policy_id"))
        existing = self._records.get(policy_id) if policy_id else None
        if policy_id and existing is None:
            raise ValueError(f"策略不存在：{policy_id}")
        if not policy_id:
            policy_id = uuid.uuid4().hex

        name = _clean_text(payload.get("name"), limit=60)
        if not name:
            raise ValueError("策略名称不能为空")
        if any(
            item.name == name and item.policy_id != policy_id
            for item in self._records.values()
        ):
            raise ValueError(f"已存在同名策略：{name}")

        now = time.time()
        policy = existing or MigrationPolicy(
            policy_id=policy_id, name=name, created_at=now
        )
        policy.name = name
        policy.description = _clean_text(payload.get("description"))
        policy.params = normalize_params(payload.get("params"))
        policy.rules = normalize_rules(payload.get("rules"))
        policy.updated_at = now
        if not policy.created_at:
            policy.created_at = now
        self._records[policy_id] = policy
        return policy

    def delete(self, policy_id: str) -> bool:
        return self._records.pop(str(policy_id), None) is not None

    def flush(self) -> None:
        atomic_write_json(
            self.path,
            dump_dataclass_records(list(self._records.values()), key="policies"),
            indent=2,
        )

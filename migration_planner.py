import ipaddress
from typing import Any


def vm_channel(
    options: dict[str, Any],
    vm_name: str,
    server_id: str | None = None,
) -> str:
    """逐台数据通道优先，其次取作业级 data_channel，默认走 RBD 直连。

    前端 row_overrides 的键是 ``server_id || vm_name``，这里必须用同一规则，
    否则逐台覆盖会取不到、静默回退到作业级通道。
    """
    overrides = options.get("row_overrides") or {}
    row = (
        overrides.get(override_key(server_id or "", vm_name))
        or overrides.get(vm_name)
        or {}
    )
    return str(row.get("data_channel") or options.get("data_channel") or "rbd")


def match_flavor(
    source: dict[str, Any],
    flavors: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Return the target flavor that best matches a source flavor.

    Matching is first by identical vcpus/ram/disk. Among candidates, a flavor
    with the same name as the source wins; otherwise the lexicographically
    first name is selected for deterministic behavior.
    """
    source_vcpus = int(source.get("vcpus") or 0)
    source_ram = int(source.get("ram") or 0)
    source_disk = int(source.get("disk") or 0)
    candidates = [
        flavor
        for flavor in flavors
        if int(flavor.get("vcpus") or 0) == source_vcpus
        and int(flavor.get("ram") or 0) == source_ram
        and int(flavor.get("disk") or 0) == source_disk
    ]
    if not candidates:
        return None
    same_name = [
        flavor
        for flavor in candidates
        if flavor.get("name") == source.get("name")
    ]
    pool = same_name or candidates
    return sorted(pool, key=lambda flavor: flavor.get("name") or "")[0]


def order_system_and_data(
    volumes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Order volumes with bootable/system volumes first, then by device name."""
    return sorted(
        volumes,
        key=lambda volume: (
            not bool(volume.get("is_bootable")),
            volume.get("device") or volume.get("id") or "",
        ),
    )


def ip_belongs_to_subnet(ip: str, cidr: str) -> bool:
    """Return True when ``ip`` is inside the IPv4/IPv6 network ``cidr``."""
    return ipaddress.ip_address(ip) in ipaddress.ip_network(cidr, strict=False)


def override_key(server_id: str, vm_name: str) -> str:
    """Return the stable key for row/network overrides."""
    return (server_id or "").strip() or (vm_name or "").strip()


def resolve_port_ip(
    source_ip: str,
    target_cidr: str,
    target_ip: str,
) -> str | None:
    """Decide which fixed IP to use for the target port.

    - An explicit ``target_ip`` must belong to the target subnet.
    - Otherwise the source IP is reused when it belongs to the target subnet.
    - Otherwise ``None`` means Neutron should auto-allocate from the subnet.
    """
    if target_ip:
        if not ip_belongs_to_subnet(target_ip, target_cidr):
            raise ValueError(
                f"目标 IP {target_ip} 不属于目标子网 {target_cidr}"
            )
        return target_ip
    if ip_belongs_to_subnet(source_ip, target_cidr):
        return source_ip
    return None


SNAPSHOT_PREFIX = "mig-"


def snapshot_name(job_id: str, seq: int) -> str:
    """迁移快照统一命名：mig-<job_id>-<seq>。"""
    if not job_id:
        raise ValueError("job_id 不能为空")
    if seq < 0:
        raise ValueError("seq 不能为负数")
    return f"{SNAPSHOT_PREFIX}{job_id}-{seq}"


def is_migration_snapshot(name: str) -> bool:
    return bool(name) and str(name).startswith(SNAPSHOT_PREFIX)


def snapshot_job_id(name: str) -> str | None:
    """解析迁移快照的归属 job_id；非迁移快照或格式不符返回 None。"""
    if not is_migration_snapshot(name):
        return None
    head, sep, tail = str(name).rpartition("-")
    if not sep or not tail.isdigit():
        return None
    job_id = head[len(SNAPSHOT_PREFIX):]
    return job_id or None


def _snapshot_seq(name: str) -> int:
    return int(str(name).rpartition("-")[2])


def snapshots_to_prune(names: list[str], job_id: str, keep: int) -> list[str]:
    """返回本 job 该删的迁移快照，按 seq 升序保留最新 keep 个。"""
    mine = sorted(
        (name for name in names if snapshot_job_id(name) == job_id),
        key=_snapshot_seq,
    )
    if keep <= 0:
        return mine
    return mine[:-keep]


def orphan_snapshots(names: list[str], active_job_ids: set[str]) -> list[str]:
    """返回归属 job 已结束/不存在的迁移快照；非迁移快照一律不动。"""
    return sorted(
        name
        for name in names
        if (job_id := snapshot_job_id(name)) and job_id not in active_job_ids
    )

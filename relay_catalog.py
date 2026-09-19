"""中转机池配置所需的两侧云目录，以及提交前的预检。"""
from __future__ import annotations

import logging
from typing import Any

from migration_planner import ip_belongs_to_subnet


def _resources(os_utils: Any) -> dict[str, Any]:
    def as_int(value: Any) -> int:
        """目录里的数值字段容错：取不到/不是数字一律按 0，别让整段目录失败。"""
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    result: dict[str, Any] = {
        "images": [],
        "flavors": [],
        "azs": [],
        "networks": [],
        "subnets": [],
        "volume_types": [],
        "external_networks": [],
        "errors": {},
    }

    def load(name: str, loader) -> None:
        try:
            result[name] = loader()
        except Exception as exc:  # noqa: BLE001 - 单段失败不影响其它下拉
            logging.exception("[MIGRATION] 目录加载失败 section=%s", name)
            result["errors"][name] = str(exc)

    load(
        "images",
        lambda: [
            {"id": str(image.id), "name": str(getattr(image, "name", "") or image.id)}
            for image in os_utils.list_images()
        ],
    )
    load(
        "flavors",
        lambda: [
            {
                "id": str(flavor.id),
                "name": str(getattr(flavor, "name", "") or flavor.id),
                "vcpus": as_int(getattr(flavor, "vcpus", 0)),
                "ram": as_int(getattr(flavor, "ram", 0)),
                "disk": as_int(getattr(flavor, "disk", 0)),
            }
            for flavor in os_utils.list_flavors()
        ],
    )
    load(
        "azs",
        lambda: [
            str(zone.get("name"))
            for zone in os_utils.list_availability_zones()
            if zone.get("name")
        ],
    )
    load("networks", lambda: list(os_utils.list_networks()))
    load("subnets", lambda: list(os_utils.list_subnets()))
    load("volume_types", lambda: list(os_utils.list_volume_types()))
    load("external_networks", lambda: list(os_utils.list_external_networks()))
    return result


def build_catalog(source_os: Any, target_os: Any) -> dict[str, Any]:
    """返回 {'source': {...}, 'target': {...}} 两侧目录。"""
    return {
        "source": _resources(source_os),
        "target": _resources(target_os),
    }


def _names(items: list[dict[str, str]]) -> set[str]:
    names: set[str] = set()
    for item in items:
        names.add(str(item.get("id", "")))
        names.add(str(item.get("name", "")))
    return names


def _find_subnet(subnets: list[dict[str, str]], value: str) -> dict[str, str] | None:
    if not value:
        return None
    for subnet in subnets:
        if value in (subnet.get("id"), subnet.get("name")):
            return subnet
    return None


def _quota_warnings(config: Any, source_os: Any, target_os: Any) -> list[str]:
    """选了卷类型但该类型配额为 0 时提前告警，避免提交后撞 413。"""
    warnings: list[str] = []
    for side, os_utils, pool in (
        ("源端", source_os, config.source),
        ("目标端", target_os, config.target),
    ):
        if not pool.volume_type:
            continue
        try:
            limits = os_utils.get_volume_quota() or {}
        except Exception:  # noqa: BLE001 - 配额查询失败不应让预检整体失败
            logging.debug("[MIGRATION] 配额查询失败，跳过 %s 卷类型校验", side)
            continue
        key = f"gigabytes_{pool.volume_type}"
        if limits.get(key) == 0:
            warnings.append(
                f"{side}卷类型 {pool.volume_type} 的配额为 0（{key}），建卷会被拒绝，"
                "请换一个卷类型或请管理员调整配额。"
            )
    return warnings


def _flavor_facts(section: dict[str, Any], value: str) -> dict[str, int]:
    """按名称/ID 找 flavor 的 vCPU/RAM；找不到返回空。"""
    for flavor in section.get("flavors") or []:
        if value in (flavor.get("id"), flavor.get("name")):
            return {
                "vcpus": int(flavor.get("vcpus", 0) or 0),
                "ram": int(flavor.get("ram", 0) or 0),
            }
    return {}


def _remaining(limits: dict[str, int], max_key: str, used_key: str) -> int | None:
    if max_key not in limits or used_key not in limits:
        return None
    return int(limits[max_key]) - int(limits[used_key])


def _capacity_warnings(
    config: Any, catalog: dict[str, Any], source_os: Any, target_os: Any
) -> list[str]:
    """建机容量预检：建 2×并发 台前先核对 vCPU/RAM/实例数/端口/浮动 IP 配额。

    读不到配额时给"请自行确认"的提示，而不是静默通过——中转机建到一半撞
    QuotaExceeded 会把已经建好的机器和设备一起回滚，代价比提前提示大得多。
    """
    warnings: list[str] = []
    mode = str(getattr(config, "node_mode", "ephemeral") or "ephemeral")
    plan = {"源端": config.source, "目标端": config.target}
    total = sum(int(getattr(pool, "size", 0) or 0) for pool in plan.values())
    if total:
        warnings.append(
            f"容量口径：本次将创建 {total} 台中转机（"
            + " + ".join(
                f"{side} {int(getattr(pool, 'size', 0) or 0)} 台"
                for side, pool in plan.items()
            )
            + ("，临时池已按「传输并发」扩池" if mode == "ephemeral" else "，常驻池按需扩缩容")
            + "）。"
        )
    for side, os_utils, pool in (
        ("源端", source_os, config.source),
        ("目标端", target_os, config.target),
    ):
        size = int(getattr(pool, "size", 0) or 0)
        if size <= 0:
            continue
        facts = _flavor_facts(catalog["source" if side == "源端" else "target"], pool.flavor)
        need_cores = size * int(facts.get("vcpus", 0) or 0)
        need_ram = size * int(facts.get("ram", 0) or 0)
        try:
            limits = os_utils.get_compute_limits() or {}
        except Exception:  # noqa: BLE001 - 配额读失败不阻断预检
            limits = {}
        if not isinstance(limits, dict) or not limits:
            warnings.append(
                f"{side}读不到计算配额，无法确认能否建 {size} 台 flavor={pool.flavor} 的中转机"
                f"（需要 {need_cores} vCPU / {need_ram} MB 内存），请先确认实例/核数/内存配额。"
            )
        else:
            shortfalls: list[str] = []
            remain_instances = _remaining(limits, "maxTotalInstances", "totalInstancesUsed")
            if remain_instances is not None and remain_instances < size:
                shortfalls.append(f"实例数剩余 {remain_instances} < {size}")
            remain_cores = _remaining(limits, "maxTotalCores", "totalCoresUsed")
            if remain_cores is not None and remain_cores < need_cores:
                shortfalls.append(f"vCPU 剩余 {remain_cores} < {need_cores}")
            remain_ram = _remaining(limits, "maxTotalRAMSize", "totalRAMUsed")
            if remain_ram is not None and remain_ram < need_ram:
                shortfalls.append(f"内存剩余 {remain_ram}MB < {need_ram}MB")
            if shortfalls:
                warnings.append(
                    f"{side}计算配额可能不足（{ '；'.join(shortfalls) }）："
                    f"建 {size} 台 flavor={pool.flavor} 会失败，请下调「传输并发」/池大小或申请配额。"
                )
        if not getattr(pool, "port_ids", None):
            try:
                net_limits = os_utils.get_network_quota() or {}
            except Exception:  # noqa: BLE001
                net_limits = {}
            if isinstance(net_limits, dict) and net_limits:
                port_limit = net_limits.get("port")
                if port_limit is not None and int(port_limit) < size:
                    warnings.append(
                        f"{side}网络端口配额（{port_limit}）小于中转机台数 {size}，"
                        "建端口会失败，请申请配额或改用常驻池。"
                    )
        fip_network = str(getattr(pool, "data_floating_network", "") or "")
        if fip_network:
            try:
                net_limits = os_utils.get_network_quota() or {}
            except Exception:  # noqa: BLE001
                net_limits = {}
            fip_limit = net_limits.get("floatingip") if isinstance(net_limits, dict) else None
            if fip_limit is not None and int(fip_limit) < size:
                warnings.append(
                    f"{side}浮动 IP 配额（{fip_limit}）小于中转机台数 {size}，"
                    "数据面绑 FIP 会失败，请申请配额。"
                )
    return warnings


def preflight(config: Any, source_os: Any, target_os: Any) -> dict[str, Any]:
    """校验中转机池配置；无法自动校验的能力以 warning 返回。"""
    catalog = build_catalog(source_os, target_os)
    errors: list[str] = []
    warnings: list[str] = []
    for side, pool, section in (
        ("源端", config.source, catalog["source"]),
        ("目标端", config.target, catalog["target"]),
    ):
        if pool.image not in _names(section["images"]):
            errors.append(f"{side}中转机镜像不存在: {pool.image}")
        if pool.flavor not in _names(section["flavors"]):
            errors.append(f"{side}中转机 flavor 不存在: {pool.flavor}")
        if pool.az not in set(section["azs"]):
            errors.append(f"{side}中转机可用区不存在: {pool.az}")
        if not pool.network and not pool.port_ids:
            errors.append(f"{side}中转机必须指定网络或端口")
        elif pool.network and pool.network not in _names(section["networks"]):
            errors.append(f"{side}中转机网络不存在: {pool.network}")
        subnet = _find_subnet(section["subnets"], pool.subnet)
        if pool.subnet and subnet is None:
            errors.append(f"{side}中转机网段不存在: {pool.subnet}")
        elif subnet is not None and pool.network and subnet["network_id"] != pool.network:
            errors.append(f"{side}中转机网段 {subnet['name']} 不属于所选网络")
        if pool.fixed_ips:
            if len(pool.fixed_ips) != pool.size:
                errors.append(
                    f"{side}中转机固定 IP 数量 {len(pool.fixed_ips)} 与池大小 "
                    f"{pool.size} 不一致（多台请用逗号分隔）"
                )
            if not pool.subnet:
                errors.append(f"{side}中转机指定了固定 IP，必须同时选择网段")
            elif subnet is not None:
                for ip in pool.fixed_ips:
                    try:
                        inside = ip_belongs_to_subnet(ip, subnet["cidr"])
                    except ValueError:
                        inside = False
                    if not inside:
                        errors.append(
                            f"{side}中转机固定 IP {ip} 不属于网段 {subnet['cidr']}"
                        )
        if pool.volume_type and pool.volume_type not in _names(section["volume_types"]):
            errors.append(f"{side}中转机卷类型不存在: {pool.volume_type}")
        elif not pool.volume_type:
            warnings.append(
                f"{side}未指定卷类型，将使用 __DEFAULT__；若该项目对 __DEFAULT__ 的配额为 0，"
                "建卷会失败（VolumeSizeExceedsAvailableQuota）。"
            )
        system_volume_type = str(
            getattr(pool, "system_volume_type", "") or ""
        ).strip()
        if not system_volume_type:
            errors.append(
                f"{side}中转机系统盘类型未选择：中转机走云硬盘启动（镜像直起在这套环境下"
                "会因本地盘 etcd 锁失败），必须在页面上选择云盘类型"
            )
        elif system_volume_type not in _names(section["volume_types"]):
            errors.append(f"{side}中转机系统盘类型不存在: {system_volume_type}")
        floating_network = str(
            getattr(pool, "data_floating_network", "") or ""
        ).strip()
        external = _names(section.get("external_networks", []))
        if floating_network and floating_network not in external:
            errors.append(
                f"{side}数据面浮动 IP 网络不存在或不是外部网络: {floating_network}"
            )
        elif not floating_network:
            if side == "目标端":
                warnings.append(
                    "目标端未配置数据面浮动 IP 网络：两侧中转机不在同一二层时"
                    "（例如各自云里的 Geneve 网络），源端连不到目标端，块拷贝会报"
                    " No route to host。跨云迁移请为**目标端**选择该云的外部网络。"
                )
            else:
                warnings.append(
                    "源端未配置数据面浮动 IP 网络：源端主动连目标端，通常不需要；"
                    "仅当需要反向连通（诊断/回连）时才配置。"
                )
    warnings.extend(_quota_warnings(config, source_os, target_os))
    warnings.extend(_capacity_warnings(config, catalog, source_os, target_os))
    warnings.append(
        "无法自动校验源端 Cinder 驱动是否支持对 in-use 卷打快照，"
        "请确认后再执行；不支持时预检通过的配置仍会在拷贝阶段失败。"
    )
    return {"ok": not errors, "errors": errors, "warnings": warnings}

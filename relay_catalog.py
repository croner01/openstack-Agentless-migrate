"""中转机池配置所需的两侧云目录，以及提交前的预检。"""
from __future__ import annotations

import logging
from typing import Any

from migration_planner import ip_belongs_to_subnet


def _resources(os_utils: Any) -> dict[str, Any]:
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
    warnings.append(
        "无法自动校验源端 Cinder 驱动是否支持对 in-use 卷打快照，"
        "请确认后再执行；不支持时预检通过的配置仍会在拷贝阶段失败。"
    )
    return {"ok": not errors, "errors": errors, "warnings": warnings}

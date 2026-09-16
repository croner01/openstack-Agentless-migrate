import base64
import logging
import os
import time
from typing import Any

from env_utils import env_int


# Version marker: bump whenever the diagnostic path changes so a running
# deployment can be verified against the code that produced the fix.
DIAG_VERSION = "20260908-diagnostic-no-vars"


def _normalize_status(value: Any) -> str:
    """归一化 Cinder 状态字符串。

    Cinder 混用连字符与下划线：卷挂载完成是 ``in-use``，
    但错误态又是 ``error_deleting``。比较前统一成下划线，避免因写法差异误判。
    """
    return str(value or "").strip().lower().replace("-", "_")


def relay_boot_volume_size(image_bytes: int, *, extra_gib: int = 10) -> int:
    """中转机启动卷大小：镜像大小 + extra_gib，向上取整到 GiB。"""
    size = int(image_bytes or 0) + int(extra_gib) * 1024**3
    return -(-size // (1024**3))


def volume_ready_timeout_default() -> int:
    """卷等待 available 的默认超时。

    这套环境的 Cinder 建卷经常 3~5 分钟，并发时还会更慢；默认 600s 太紧，
    会把只是"慢"的卷判成失败。
    """
    return env_int("MIGRATION_VOLUME_READY_TIMEOUT", 1800, minimum=1)


#: Cinder 因卷仍被实例占用而拒绝删除时的报错特征。
_ATTACHMENT_BUSY_MARKERS = (
    "must not be attached",
    "in-use",
    "in use",
    "is attached",
    "attachment",
)


def _is_attachment_busy_error(exc: Exception) -> bool:
    """判断删除卷失败是否只是 attachment 未释放（值得重试）。"""
    text = str(exc).lower()
    return any(marker in text for marker in _ATTACHMENT_BUSY_MARKERS)


class OpenStackUtils:
    def __init__(self, auth_args: dict[str, Any] | None = None, conn=None):
        """Wrap an openstacksdk connection.

        A real connection is created lazily when ``auth_args`` is supplied.
        Tests can inject a fake ``conn`` object.
        """
        if conn is not None:
            self.conn = conn
        else:
            if not auth_args:
                raise ValueError("auth_args 或 conn 必须提供一个")
            import openstack

            resolved = self._resolve_project_id(auth_args)
            self.conn = openstack.connect(**resolved)

    @staticmethod
    def _probe_args_without_project_scope(
        auth_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Keep only user-scope credentials so Keystone issues an unscoped token."""
        return {
            key: value
            for key, value in auth_args.items()
            if key
            not in {
                "project_name",
                "project_domain_name",
                "project_id",
                "project_domain_id",
            }
        }

    @staticmethod
    def _fetch_accessible_projects(
        auth_args: dict[str, Any],
        probe_conn=None,
    ) -> list[dict[str, Any]]:
        """Return projects the user can access using an unscoped token."""
        probe_args = OpenStackUtils._probe_args_without_project_scope(auth_args)
        if not probe_args.get("auth_url"):
            raise ValueError("缺少 auth_url")

        import requests

        if probe_conn is None:
            import openstack

            probe_conn = openstack.connect(**probe_args)
        token = probe_conn.authorize()
        auth_url = str(probe_args["auth_url"]).rstrip("/")
        response = requests.get(
            f"{auth_url}/auth/projects",
            headers={"X-Auth-Token": token},
            timeout=30,
        )
        response.raise_for_status()
        return (response.json() or {}).get("projects", []) or []

    @staticmethod
    def fetch_token_scope(auth_args: dict[str, Any]) -> dict[str, Any]:
        """Authorize and return the scope Keystone actually granted."""
        probe_args = OpenStackUtils._probe_args_without_project_scope(auth_args)
        if not probe_args.get("auth_url"):
            raise ValueError("缺少 auth_url")

        import requests
        import openstack

        conn = openstack.connect(**probe_args)
        token = conn.authorize()
        auth_url = str(probe_args["auth_url"]).rstrip("/")
        response = requests.get(
            f"{auth_url}/auth/tokens",
            headers={
                "X-Auth-Token": token,
                "X-Subject-Token": token,
            },
            timeout=30,
        )
        response.raise_for_status()
        body = response.json() or {}
        token_body = body.get("token") or {}
        project = token_body.get("project") or {}
        domain = token_body.get("domain") or {}
        return {
            "scoped_project_id": (project or {}).get("id"),
            "scoped_project_name": (project or {}).get("name"),
            "scoped_domain_id": (domain or {}).get("id")
            or (project or {}).get("domain", {}).get("id"),
            "scoped_domain_name": (domain or {}).get("name")
            or (project or {}).get("domain", {}).get("name"),
            "scope_type": token_body.get("methods"),
            "expires_at": token_body.get("expires_at"),
        }

    @staticmethod
    def _resolve_project_id(
        auth_args: dict[str, Any],
        probe_conn=None,
    ) -> dict[str, Any]:
        """Resolve ``project_name`` to ``project_id`` when only a name is given."""
        if auth_args.get("project_id") or not auth_args.get("project_name"):
            return auth_args

        project_name = str(auth_args["project_name"]).strip()
        project_domain_name = str(
            auth_args.get("project_domain_name") or ""
        ).strip()
        projects = OpenStackUtils._fetch_accessible_projects(
            auth_args, probe_conn=probe_conn
        )
        matched = [
            project
            for project in projects
            if project.get("name") == project_name
            and (
                not project_domain_name
                or project.get("domain_name") == project_domain_name
                or project.get("domain_id") == project_domain_name
            )
        ]
        if not matched:
            accessible = ", ".join(
                f"{project.get('name')}@{project.get('domain_name') or project.get('domain_id') or '?'}"
                for project in projects
            )
            raise RuntimeError(
                f"当前用户可访问的项目中没有匹配到 {project_name}"
                f"{'@' + project_domain_name if project_domain_name else ''}"
                f"（可访问项目：{accessible or '无'}），"
                "请核对项目名/项目域，或直接在页面填写项目 UUID"
            )
        if len(matched) > 1:
            ids = ", ".join(project.get("id", "?") for project in matched)
            raise RuntimeError(
                f"存在多个名为 {project_name} 的项目（{ids}），"
                "请在页面填写目标项目 UUID"
            )

        resolved = {
            key: value
            for key, value in auth_args.items()
            if key
            not in {
                "project_name",
                "project_domain_name",
                "project_domain_id",
            }
        }
        resolved["project_id"] = matched[0]["id"]
        return resolved

    # ---------- source/target shared queries ----------

    def get_server_by_name(self, name: str):
        return self.conn.compute.find_server(name)

    def get_server_detail(self, server_id: str):
        return self.conn.compute.get_server(server_id)

    def get_volume(self, volume_id: str):
        return self.conn.block_storage.get_volume(volume_id)

    def get_server_volumes_with_device(self, server_id: str) -> list[dict[str, Any]]:
        server = self.conn.compute.get_server(server_id)
        server_dict = server.to_dict()
        attachments = server_dict.get("attached_volumes", []) or []
        entries = []
        for index, attachment in enumerate(attachments):
            volume_id = attachment["id"]
            volume = self.conn.block_storage.get_volume(volume_id)
            volume_dict = volume.to_dict()
            device = self._find_attachment_device(volume_dict, server_id)
            entries.append(
                {
                    "volume": volume,
                    "volume_id": volume_id,
                    "device": device or f"auto-{index}",
                    "is_bootable": bool(getattr(volume, "is_bootable", False)),
                    "size": int(getattr(volume, "size", 0) or 0),
                    "name": getattr(volume, "name", "") or "",
                }
            )
        return entries

    @staticmethod
    def _find_attachment_device(volume_dict: dict[str, Any], server_id: str) -> str:
        attachments = volume_dict.get("attachments") or []
        for attachment in attachments:
            if attachment.get("server_id") == server_id:
                return attachment.get("device") or ""
        return ""

    def get_server_addresses(self, server_id: str) -> dict[str, list[dict[str, Any]]]:
        server = self.conn.compute.get_server(server_id)
        addresses = server.to_dict().get("addresses", {}) or {}
        result = {}
        for network_name, entries in addresses.items():
            fixed = [
                entry
                for entry in entries
                if entry.get("type") != "floating"
            ]
            result[network_name] = fixed
        return result

    def get_server_port_details(self, server_id: str) -> list[dict[str, Any]]:
        """Return per-port fixed-IP info (IP + subnet cidr + network) for a VM."""
        details: list[dict[str, Any]] = []
        try:
            ports = self.conn.network.ports(device_id=server_id)
        except Exception as exc:  # noqa: BLE001 - Neutron 不可用时降级
            logging.warning("[MIGRATION] 查询端口失败（尝试由 server.addresses 降级）: %s", exc)
            ports = []
        for port in ports:
            network_name = ""
            try:
                network = self.conn.network.get_network(port.network_id)
                network_name = getattr(network, "name", "") or ""
            except Exception as exc:  # noqa: BLE001
                logging.debug("[MIGRATION] 网络名查询失败: %s", exc)
            for fixed_ip in (getattr(port, "fixed_ips", None) or []):
                ip = (fixed_ip or {}).get("ip_address") or ""
                subnet_id = (fixed_ip or {}).get("subnet_id") or ""
                cidr = ""
                if subnet_id:
                    try:
                        subnet = self.conn.network.get_subnet(subnet_id)
                        cidr = getattr(subnet, "cidr", "") or ""
                    except Exception as exc:  # noqa: BLE001
                        logging.debug("[MIGRATION] 子网查询失败: %s", exc)
                if ip:
                    details.append(
                        {
                            "network_id": port.network_id,
                            "network_name": network_name,
                            "ip": ip,
                            "subnet_id": subnet_id,
                            "cidr": cidr,
                            "mac": getattr(port, "mac_address", "") or "",
                        }
                    )
        return details

    def get_server_flavor_spec(self, server) -> dict[str, Any]:
        flavor_id = (server.to_dict().get("flavor") or {}).get("id")
        if not flavor_id:
            raise RuntimeError("无法从源 VM 获取 flavor id")
        flavor = self.conn.compute.get_flavor(flavor_id)
        return {
            "id": flavor.id,
            "name": flavor.name,
            "vcpus": int(flavor.vcpus or 0),
            "ram": int(flavor.ram or 0),
            "disk": int(flavor.disk or 0),
            "ephemeral": int(getattr(flavor, "ephemeral", 0) or 0),
            "swap": int(getattr(flavor, "swap", 0) or 0),
        }

    @staticmethod
    def _summarize_source_server(
        server,
        flavor_by_id,
        image_by_id,
        image_id_override: str | None = None,
        image_name_hint: str | None = None,
    ) -> dict[str, Any]:
        data = server.to_dict()
        flavor_id = (data.get("flavor") or {}).get("id") or ""
        image_id = image_id_override or (data.get("image") or {}).get("id") or ""
        flavor = flavor_by_id.get(flavor_id)
        return {
            "server_id": str(getattr(server, "id", "") or ""),
            "name": str(getattr(server, "name", "") or data.get("name") or ""),
            "status": str(getattr(server, "status", "") or data.get("status") or ""),
            "availability_zone": str(
                data.get("OS-EXT-AZ:availability_zone")
                or getattr(server, "availability_zone", None)
                or ""
            ),
            "flavor": {
                "id": flavor_id,
                "name": getattr(flavor, "name", None) or flavor_id,
                "vcpus": int(getattr(flavor, "vcpus", 0) or 0),
                "ram": int(getattr(flavor, "ram", 0) or 0),
                "disk": int(getattr(flavor, "disk", 0) or 0),
            },
            "image_id": image_id,
            "image_name": (
                image_by_id.get(image_id)
                or image_name_hint
                or (image_id if image_id else "")
                or "BFV（卷启动）"
            ),
        }

    @staticmethod
    def _boot_image_records_by_server(
        volumes: list[Any],
    ) -> dict[str, dict[str, str]]:
        """Return per-attached-server boot-volume image facts.

        Cinder keeps both ``image_id`` and ``image_name`` in
        ``volume_image_metadata`` from the moment the BFV volume was created,
        so the name can still be displayed after the Glance image is deleted
        or becomes invisible to the current user.
        """
        records: dict[str, dict[str, str]] = {}
        for volume in volumes:
            data = volume.to_dict()
            # openstacksdk 的 to_dict() 默认使用 Python 属性名 is_bootable；
            # 少数返回体/旧版本可能仍是服务端字段 bootable，这里兼容两种。
            raw_bootable = data.get("is_bootable", data.get("bootable"))
            bootable = str(raw_bootable or "false").lower()
            if bootable not in {"true", "1", "yes"}:
                continue
            metadata = data.get("volume_image_metadata") or {}
            image_id = str(metadata.get("image_id") or "").strip()
            image_name = str(metadata.get("image_name") or "").strip()
            if not image_id and not image_name:
                continue
            record = {
                "image_id": image_id,
                "image_name": image_name,
            }
            for attachment in data.get("attachments") or []:
                server_id = str(attachment.get("server_id") or "").strip()
                if server_id and server_id not in records:
                    records[server_id] = record
        return records

    def list_source_servers(
        self,
        search: str = "",
        limit: int = 100,
        marker: str | None = None,
    ) -> list[dict[str, Any]]:
        """List source-project VMs with flavor/image names for the picker."""
        filters: dict[str, Any] = {"limit": int(limit)}
        if search:
            filters["name"] = search
        if marker:
            filters["marker"] = marker
        servers = list(self.conn.compute.servers(**filters))
        flavor_ids = {
            (server.to_dict().get("flavor") or {}).get("id")
            for server in servers
        }
        flavor_by_id = {
            flavor.id: flavor
            for flavor in self.conn.compute.flavors()
            if flavor.id in flavor_ids
        }
        boot_records_by_server: dict[str, dict[str, str]] = {}
        missing_image_servers = [
            server
            for server in servers
            if not (server.to_dict().get("image") or {}).get("id")
        ]
        if missing_image_servers:
            try:
                volumes = list(self.conn.block_storage.volumes())
            except AttributeError:
                volumes = []
            boot_records_by_server = self._boot_image_records_by_server(
                volumes,
            )
        boot_image_by_server = {
            server_id: record["image_id"]
            for server_id, record in boot_records_by_server.items()
            if record["image_id"]
        }
        image_name_hint_by_server = {
            server_id: record["image_name"]
            for server_id, record in boot_records_by_server.items()
            if record["image_name"]
        }
        image_ids = {
            (server.to_dict().get("image") or {}).get("id")
            for server in servers
        }
        image_ids.update(boot_image_by_server.values())
        image_by_id = {
            image.id: getattr(image, "name", "") or ""
            for image in self.conn.image.images()
            if image.id in image_ids
        }
        summaries = []
        for server in servers:
            raw_image_id = (server.to_dict().get("image") or {}).get("id") or ""
            resolved_image_id = (
                raw_image_id or boot_image_by_server.get(server.id) or ""
            )
            summaries.append(
                self._summarize_source_server(
                    server,
                    flavor_by_id,
                    image_by_id,
                    image_id_override=resolved_image_id or None,
                    image_name_hint=image_name_hint_by_server.get(server.id),
                )
            )
        return summaries

    # ---------- target resource discovery ----------

    def list_images(self) -> list[Any]:
        return list(self.conn.image.images())

    def list_flavors(self) -> list[Any]:
        return list(self.conn.compute.flavors())

    def list_availability_zones(self) -> list[dict[str, Any]]:
        """Return available zones from the target compute service."""
        zones = []
        for method_name in ("availability_zones", "list_availability_zones"):
            method = getattr(self.conn.compute, method_name, None)
            if method is None:
                continue
            try:
                zones = list(method())
                break
            except (AttributeError, TypeError):
                zones = []
        normalized = []
        for zone in zones:
            if isinstance(zone, dict):
                raw_name = zone.get("zoneName") or zone.get("name") or ""
                state = zone.get("zoneState") or {}
            else:
                raw_name = (
                    getattr(zone, "zoneName", None)
                    or getattr(zone, "name", None)
                    or ""
                )
                state = getattr(zone, "zoneState", {}) or {}
            name = str(raw_name).strip()
            if not name:
                continue
            available = bool(
                state.get("available", True) if isinstance(state, dict) else True
            )
            normalized.append(
                {"id": name, "name": name, "available": available}
            )
        return normalized

    @staticmethod
    def list_accessible_projects(
        auth_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Return projects the authenticated user can access.

        Uses an unscoped token against ``GET /v3/auth/projects``.  When the
        account is a cloud/system admin the token may only carry role
        assignments for a single project, so we additionally enumerate all
        projects via the identity service and merge them.
        """
        projects = OpenStackUtils._fetch_accessible_projects(auth_args)
        all_projects = OpenStackUtils._list_all_projects(auth_args)
        by_id: dict[str, dict[str, Any]] = {}
        for project in [*projects, *all_projects]:
            project_id = project.get("id")
            if not project_id:
                continue
            current = by_id.get(project_id) or {}
            by_id[project_id] = {
                "id": project_id,
                "name": project.get("name") or current.get("name"),
                "domain_id": project.get("domain_id") or current.get("domain_id"),
                "domain_name": project.get("domain_name")
                or current.get("domain_name"),
            }
        merged = sorted(
            by_id.values(),
            key=lambda item: (
                item.get("domain_name") or "",
                item.get("name") or "",
            ),
        )
        return {
            "projects": merged,
            "summary": {
                "role_scoped_count": len(projects),
                "all_projects_count": len(all_projects),
                "total_count": len(merged),
            },
        }

    @staticmethod
    def _list_all_projects(auth_args: dict[str, Any]) -> list[dict[str, Any]]:
        """Best-effort full project list via the identity service.

        ``GET /v3/auth/projects`` only returns projects where the token has an
        explicit role assignment.  A cloud admin often has a system-level role
        instead; we first try a system-scoped token (``system_scope=all``) and
        fall back to a domain/project-scoped one.  Returns [] when the account
        lacks ``identity:list_projects`` permission.
        """
        probe_args = OpenStackUtils._probe_args_without_project_scope(auth_args)
        if not probe_args.get("auth_url"):
            return []
        import openstack

        def _flatten(conn) -> list[dict[str, Any]]:
            return [
                {
                    "id": project.id,
                    "name": getattr(project, "name", None),
                    "domain_id": getattr(project, "domain_id", None),
                    "domain_name": getattr(project, "domain_name", None),
                }
                for project in conn.identity.projects()
            ]

        # 系统级角色（云管理员）用 system_scope 能列出所有项目；
        # 普通账号无该角色时 Keystone 会拒绝，这里安静降级。
        try:
            conn = openstack.connect(**probe_args, system_scope="all")
            return _flatten(conn)
        except Exception as exc:  # noqa: BLE001
            logging.debug(
                "[MIGRATION] 全量项目列举不可用（非系统级账号）: %s", exc
            )
        return []

    @staticmethod
    def accessible_project_diagnostics(
        auth_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Human-readable explanation of what the project picker shows."""
        try:
            scope = OpenStackUtils.fetch_token_scope(auth_args)
        except Exception as exc:  # noqa: BLE001
            scope = {"error": str(exc)}
        return scope

    def list_networks(self) -> list[dict[str, Any]]:
        """列出网络，供中转机池的网络下拉使用。"""
        networks = []
        for network in self.conn.network.networks():
            network_id = str(getattr(network, "id", "") or "")
            name = str(getattr(network, "name", "") or "")
            entry: dict[str, Any] = {"id": network_id, "name": name or network_id}
            if getattr(network, "is_router_external", False):
                entry["external"] = True
            networks.append(entry)
        return networks

    def list_external_networks(self) -> list[dict[str, Any]]:
        """外部网络（浮动 IP 池），供跨云数据面 FIP 选择。"""
        return [
            {"id": item["id"], "name": item["name"]}
            for item in self.list_networks()
            if item.get("external")
        ]

    def find_subnet(self, name_or_id: str):
        return self.conn.network.find_subnet(name_or_id)

    def find_network(self, name_or_id: str):
        return self.conn.network.find_network(name_or_id)

    def find_image(self, name_or_id: str):
        return self.conn.image.find_image(name_or_id)

    def find_flavor(self, name_or_id: str):
        return self.conn.compute.find_flavor(name_or_id)

    # ---------- target resource creation ----------

    def create_port_with_fixed_ip(
        self,
        network_id: str,
        subnet_id: str,
        fixed_ip: str | None = None,
    ):
        fixed_ips = [{"subnet_id": subnet_id}]
        if fixed_ip:
            fixed_ips[0]["ip_address"] = fixed_ip
        return self.conn.network.create_port(
            network_id=network_id,
            fixed_ips=fixed_ips,
        )

    def create_blank_volume(
        self,
        name: str,
        size: int,
        volume_type: str | None = None,
        availability_zone: str | None = None,
    ):
        kwargs = {"name": name, "size": size}
        if volume_type:
            kwargs["volume_type"] = volume_type
        if availability_zone:
            kwargs["availability_zone"] = availability_zone
        logging.info(
            "[MIGRATION] 创建空白卷 name=%s size=%s type=%s az=%s (conn scope=%s)",
            name,
            size,
            volume_type,
            availability_zone,
            self._scope_hint(),
        )
        try:
            volume = self.conn.block_storage.create_volume(**kwargs)
            logging.info("[MIGRATION] 空白卷创建成功 volume_id=%s", volume.id)
            return volume
        except Exception as exc:  # noqa: BLE001
            logging.exception(
                "[MIGRATION] 空白卷创建失败 name=%s size=%s kwargs=%s",
                name,
                size,
                kwargs,
            )
            raise

    def get_image_size(self, image_id: str) -> int:
        """镜像大小（字节），用于推算中转机启动卷容量。"""
        image = self.conn.image.get_image(image_id)
        size = int(getattr(image, "size", 0) or 0)
        if size <= 0:
            raise ValueError(
                f"镜像 {image_id} 未返回 size，无法推算中转机启动卷大小；"
                "请改用其它镜像或在池参数中显式指定启动卷大小"
            )
        return size

    def create_boot_volume(
        self,
        name: str,
        image_id: str,
        size: int,
        volume_type: str,
        availability_zone: str | None = None,
    ):
        """用镜像生成一块可启动云硬盘（中转机系统盘）。"""
        kwargs: dict[str, Any] = {
            "name": name,
            "size": int(size),
            "image_id": image_id,
            "volume_type": volume_type,
        }
        if availability_zone:
            kwargs["availability_zone"] = availability_zone
        logging.info(
            "[MIGRATION] 创建中转机启动卷 name=%s size=%s type=%s image=%s az=%s",
            name,
            size,
            volume_type,
            image_id,
            availability_zone,
        )
        volume = self.conn.block_storage.create_volume(**kwargs)
        logging.info(
            "[MIGRATION] 中转机启动卷已提交 volume=%s status=%s",
            getattr(volume, "id", "?"),
            getattr(volume, "status", "?"),
        )
        return volume

    def _scope_hint(self) -> str:
        """Best-effort textual scope for log lines (never logs the password)."""
        auth = getattr(self.conn, "auth", None)
        auth_vars = self._collect_auth_attrs(auth)
        return "project_id=" + str(
            auth_vars.get("project_id")
            or auth_vars.get("project_name")
            or getattr(self.conn, "project_id", "")
        )

    @staticmethod
    def _collect_auth_attrs(auth: Any) -> dict[str, Any]:
        """Return safe auth-plugin attributes (works with __slots__ plugins)."""
        if auth is None:
            return {}
        attrs: dict[str, Any] = {}
        # keystoneauth plugins may store fields in __dict__ OR __slots__.
        # Never call vars() here: plugins without __dict__ raise TypeError.
        for name in (
            "auth_url",
            "username",
            "password",
            "user_domain_name",
            "user_domain_id",
            "project_name",
            "project_id",
            "project_domain_name",
            "project_domain_id",
            "system_scope",
            "interface",
            "region_name",
            "reauthenticate",
            "project_domain_id",
        ):
            try:
                value = getattr(auth, name)
            except (AttributeError, TypeError):
                continue
            if not callable(value):
                attrs[name] = value
        return attrs

    @staticmethod
    def _build_bfv_block_device_mapping(
        image_id: str,
        boot_volume_size: int,
        data_volume_ids: list[str],
        volume_type: str | None = None,
    ) -> list[dict[str, Any]]:
        boot_entry = {
            "uuid": image_id,
            "source_type": "image",
            "destination_type": "volume",
            "boot_index": 0,
            "volume_size": boot_volume_size,
            "delete_on_termination": False,
        }
        if volume_type:
            boot_entry["volume_type"] = volume_type
        bdm = [boot_entry]
        for volume_id in data_volume_ids:
            bdm.append(
                {
                    "uuid": volume_id,
                    "source_type": "volume",
                    "destination_type": "volume",
                    "boot_index": -1,
                    "delete_on_termination": False,
                }
            )
        return bdm

    def create_bfv_server(
        self,
        name: str,
        image_id: str,
        flavor_id: str,
        port_ids: list[str],
        boot_volume_size: int,
        data_volume_ids: list[str],
        availability_zone: str,
        admin_password: str,
        volume_type: str | None = None,
    ):
        bdm = self._build_bfv_block_device_mapping(
            image_id,
            boot_volume_size,
            data_volume_ids,
            volume_type,
        )
        logging.info(
            "[MIGRATION] 创建 BFV VM name=%s image_id=%s flavor_id=%s "
            "ports=%s az=%s boot_size=%s data_volumes=%s bdm=%s conn_scope=%s",
            name,
            image_id,
            flavor_id,
            port_ids,
            availability_zone,
            boot_volume_size,
            data_volume_ids,
            bdm,
            self._scope_hint(),
        )
        try:
            logging.info(
                "[MIGRATION] BFV VM admin_pass_set=%s",
                "yes" if admin_password else "no",
            )
            server = self.conn.compute.create_server(
                name=name,
                flavor_id=flavor_id,
                networks=[{"port": port_id} for port_id in port_ids],
                block_device_mapping_v2=bdm,
                availability_zone=availability_zone,
                # openstacksdk Server.admin_password -> Nova adminPass.
                # 不要使用 admin_pass：它不是 Server 的属性，会被 SDK 静默丢弃，
                # 导致 Nova 自动生成随机密码（nova show 可见随机 adminPass）。
                admin_password=admin_password,
            )
            logging.info("[MIGRATION] BFV VM 创建成功 server_id=%s", server.id)
            return server
        except Exception as exc:  # noqa: BLE001
            logging.exception(
                "[MIGRATION] BFV VM 创建失败 name=%s image_id=%s flavor_id=%s "
                "ports=%s az=%s boot_size=%s data_volumes=%s",
                name,
                image_id,
                flavor_id,
                port_ids,
                availability_zone,
                boot_volume_size,
                data_volume_ids,
            )
            raise

    def create_relay_server(
        self,
        name: str,
        image_id: str,
        flavor_id: str,
        port_ids: list[str],
        availability_zone: str,
        user_data: str,
        admin_password: str | None = None,
        system_volume_type: str = "",
        boot_volume_size: int | None = None,
        volume_ready_timeout: float = 900.0,
    ):
        """创建中转机：先从镜像生成启动云硬盘，再以云硬盘启动。

        必须走云硬盘启动：这套环境的 libvirt 对"镜像直起"的本地根盘做
        etcd 分布式锁时会在 resume 阶段失败
        （``virLockManagerEtcdAcquire: Failed to acquire lock`` →
        ``operation failed: resume operation failed``），而页面手工建机
        默认是云硬盘启动所以正常。配置仍由 cloud-init 注入。
        """
        volume_type = str(system_volume_type or "").strip()
        if not volume_type:
            raise ValueError(
                "未配置中转机系统盘类型：中转机走云硬盘启动，必须在页面上选择云盘类型"
            )
        size = int(boot_volume_size or 0) or relay_boot_volume_size(
            self.get_image_size(image_id)
        )
        logging.info(
            "[MIGRATION] 创建中转机 name=%s image=%s flavor=%s az=%s "
            "boot_volume=%sGiB type=%s admin_pass_set=%s",
            name,
            image_id,
            flavor_id,
            availability_zone,
            size,
            volume_type,
            "yes" if admin_password else "no",
        )
        volume = None
        volume_id = ""
        try:
            volume = self.create_boot_volume(
                name=f"{name}-root",
                image_id=image_id,
                size=size,
                volume_type=volume_type,
                availability_zone=availability_zone,
            )
            volume_id = str(getattr(volume, "id", "") or "")
            self.wait_volume_status(
                volume_id,
                "available",
                timeout=int(volume_ready_timeout),
                context=f"（中转机 {name} 启动卷）",
            )
            # Nova 要求 user_data 必须是 base64（openstacksdk 不会替我们编码，
            # 见 openstack/compute/v2/server.py 的 "Must be Base64 encoded"）。
            encoded_user_data = base64.b64encode(user_data.encode("utf-8")).decode(
                "ascii"
            )
            kwargs: dict[str, Any] = {
                "name": name,
                "flavor_id": flavor_id,
                "networks": [{"port": port_id} for port_id in port_ids],
                "block_device_mapping_v2": [
                    {
                        "uuid": volume_id,
                        "source_type": "volume",
                        "destination_type": "volume",
                        "boot_index": 0,
                        "delete_on_termination": True,
                    }
                ],
                "availability_zone": availability_zone,
                "user_data": encoded_user_data,
            }
            if admin_password:
                kwargs["admin_password"] = admin_password
            server = self.conn.compute.create_server(**kwargs)
        except Exception:  # noqa: BLE001 - 记录可定位的现场后原样抛出
            logging.exception(
                "[MIGRATION] 创建中转机失败 name=%s image=%s flavor=%s az=%s "
                "ports=%s user_data_bytes=%s volume_type=%s（密码不记录）",
                name,
                image_id,
                flavor_id,
                availability_zone,
                port_ids,
                len(user_data.encode("utf-8")),
                volume_type,
            )
            if volume_id:
                try:
                    self.conn.block_storage.delete_volume(volume_id)
                except Exception:  # noqa: BLE001 - 清理失败不覆盖原始错误
                    logging.exception(
                        "[MIGRATION] 清理中转机启动卷失败 volume=%s", volume_id
                    )
            raise
        logging.info(
            "[MIGRATION] 中转机已提交 name=%s server=%s boot_volume=%s，等待 agent 回连平台",
            name,
            getattr(server, "id", "?"),
            volume_id,
        )
        # 调用方（池/节点清单）要记录启动卷，删机时显式回收。
        try:
            server.root_volume_id = volume_id
        except Exception:  # noqa: BLE001 - 只影响清理路径，不影响建机
            logging.debug("[MIGRATION] 无法在实例对象上记录启动卷 id")
        return server

    def create_server_from_volumes(
        self,
        name: str,
        flavor_id: str,
        port_ids: list[str],
        boot_volume_id: str,
        data_volume_ids: list[str],
        availability_zone: str,
        admin_password: str,
    ):
        """用已建好的卷作启动盘建 VM（中转机路径的目标 VM）。"""
        bdm = [
            {
                "uuid": boot_volume_id,
                "source_type": "volume",
                "destination_type": "volume",
                "boot_index": 0,
                "delete_on_termination": False,
            }
        ]
        for volume_id in data_volume_ids:
            bdm.append(
                {
                    "uuid": volume_id,
                    "source_type": "volume",
                    "destination_type": "volume",
                    "boot_index": -1,
                    "delete_on_termination": False,
                }
            )
        logging.info(
            "[MIGRATION] 用目标卷建 VM name=%s boot=%s data=%s az=%s",
            name,
            boot_volume_id,
            data_volume_ids,
            availability_zone,
        )
        return self.conn.compute.create_server(
            name=name,
            flavor_id=flavor_id,
            networks=[{"port": port_id} for port_id in port_ids],
            block_device_mapping_v2=bdm,
            availability_zone=availability_zone,
            admin_password=admin_password,
        )

    def set_volume_bootable(self, volume_id: str, bootable: bool = True) -> None:
        """设置卷的启动标记。

        中转机通道的目标启动盘是空白卷，Cinder 默认 ``bootable=false``；
        Nova 在 BDM 里遇到 ``boot_index=0`` 的不可启动卷会直接 400 拒绝建机。
        """
        self.conn.block_storage.set_volume_bootable_status(volume_id, bootable)

    def create_volume_snapshot(self, volume_id: str, name: str, *, force: bool = True):
        """对卷打快照；force 允许对 in-use（仍挂在源 VM 上）的卷打快照。"""
        return self.conn.block_storage.create_snapshot(
            volume_id=volume_id,
            name=name,
            force=force,
        )

    def create_volume_from_snapshot(
        self,
        name: str,
        snapshot_id: str,
        size: int,
        volume_type: str | None = None,
        availability_zone: str | None = None,
    ):
        kwargs: dict[str, Any] = {
            "name": name,
            "snapshot_id": snapshot_id,
            "size": size,
        }
        if volume_type:
            kwargs["volume_type"] = volume_type
        if availability_zone:
            kwargs["availability_zone"] = availability_zone
        return self.conn.block_storage.create_volume(**kwargs)

    def attach_volume(self, server_id: str, volume_id: str) -> str:
        """把卷挂到实例上，返回 attachment id。

        openstacksdk 4.x 的方法是 create_volume_attachment(server, volume)；
        更老的版本用的是 create_server_volume(server_id, volume_id)，这里做兼容。
        """
        create = getattr(self.conn.compute, "create_volume_attachment", None)
        if callable(create):
            attachment = create(server=server_id, volume=volume_id)
        else:
            attachment = self.conn.compute.create_server_volume(
                server_id=server_id, volume_id=volume_id
            )
        return getattr(attachment, "id", "") or ""

    def detach_volume(self, server_id: str, volume_id: str) -> None:
        delete = getattr(self.conn.compute, "delete_volume_attachment", None)
        if callable(delete):
            delete(server=server_id, volume=volume_id)
            return
        self.conn.compute.delete_server_volume(
            server_id=server_id, volume_id=volume_id
        )

    def find_volume_attachment(self, server_id: str, volume_id: str) -> str | None:
        getter = getattr(self.conn.compute, "get_volume_attachment", None)
        if callable(getter):
            try:
                attachment = getter(server=server_id, volume=volume_id)
                return getattr(attachment, "id", None) if attachment else None
            except Exception:  # noqa: BLE001 - 查询失败时回退到列举接口
                pass
        for attachment in self.conn.compute.volume_attachments(server_id):
            if attachment.volume_id == volume_id:
                return attachment.id
        return None

    def list_server_volume_attachments(self, server_id: str) -> list[dict[str, str]]:
        """列出实例上的卷 attachment，供常驻节点孤儿对账使用。"""
        attachments: list[dict[str, str]] = []
        for attachment in self.conn.compute.volume_attachments(server_id):
            attachments.append(
                {
                    "volume_id": str(getattr(attachment, "volume_id", "") or ""),
                    "attachment_id": str(getattr(attachment, "id", "") or ""),
                    "device": str(getattr(attachment, "device", "") or ""),
                }
            )
        return attachments

    def wait_attachment_device(
        self,
        server_id: str,
        volume_id: str,
        *,
        timeout: float = 120.0,
        poll_interval: float = 2.0,
        sleeper=time.sleep,
    ) -> str:
        """等待 attachment 的 device 字段就绪，返回块设备路径。"""
        deadline = time.monotonic() + timeout
        while True:
            for attachment in self.conn.compute.volume_attachments(server_id):
                if attachment.volume_id == volume_id and attachment.device:
                    return attachment.device
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"等待卷 {volume_id} 在实例 {server_id} 上的设备路径超时"
                )
            sleeper(poll_interval)

    def delete_volume(self, volume_id: str) -> None:
        self.conn.block_storage.delete_volume(volume_id)

    def create_relay_floating_ip(self, port_id: str, floating_network_id: str):
        """给中转机端口绑一个浮动 IP，供跨云数据面互通使用。"""
        logging.info(
            "[MIGRATION] 为中转机端口绑定浮动 IP port=%s network=%s",
            port_id,
            floating_network_id,
        )
        floating_ip = self.conn.network.create_ip(
            floating_network_id=floating_network_id, port_id=port_id
        )
        logging.info(
            "[MIGRATION] 浮动 IP 已绑定 address=%s fip=%s",
            getattr(floating_ip, "floating_ip_address", "?"),
            getattr(floating_ip, "id", "?"),
        )
        return floating_ip

    def delete_relay_floating_ip(self, floating_ip_id: str) -> None:
        """释放浮动 IP；端口还挂着时 Neutron 会拒绝，交给调用方决定是否重试。"""
        self.conn.network.delete_ip(floating_ip_id)

    def delete_volume_wait(
        self,
        volume_id: str,
        *,
        timeout: float = 120.0,
        poll_interval: float = 5.0,
        sleeper=time.sleep,
        monotonic=time.monotonic,
    ) -> bool:
        """删除卷；实例刚删完时 attachment 可能还没释放，重试到超时。

        Nova 对实例的删除是异步的：即使实例已经查不到，卷可能还是 in-use，
        Cinder 会以 "must not be attached" 拒绝删除。
        """
        deadline = monotonic() + float(timeout)
        while True:
            try:
                self.conn.block_storage.delete_volume(volume_id)
                return True
            except Exception as exc:  # noqa: BLE001 - 可能只是 attachment 未释放
                # 只有"卷仍被实例占用"值得重试；鉴权失败/卷不存在之类重试到
                # 超时只会白等 120s 并掩盖真正原因。
                if not _is_attachment_busy_error(exc) or monotonic() >= deadline:
                    raise
                logging.info(
                    "[MIGRATION] 删除卷 %s 失败（attachment 可能未释放），%.0fs 后重试",
                    volume_id,
                    poll_interval,
                )
                sleeper(poll_interval)

    def delete_volume_snapshot(self, snapshot_id: str) -> None:
        self.conn.block_storage.delete_snapshot(snapshot_id)

    def create_relay_port(
        self,
        network_id: str,
        subnet_id: str | None = None,
        fixed_ip: str | None = None,
    ):
        """为中转机建端口；给了网段就固定在该网段，给了 IP 就固定该地址。"""
        kwargs: dict[str, Any] = {"network_id": network_id}
        if subnet_id:
            entry: dict[str, Any] = {"subnet_id": subnet_id}
            if fixed_ip:
                entry["ip_address"] = fixed_ip
            kwargs["fixed_ips"] = [entry]
        return self.conn.network.create_port(**kwargs)

    def list_subnets(self) -> list[dict[str, Any]]:
        """列出子网（含所属网络与网段），供中转机池的网段下拉使用。"""
        subnets = []
        for subnet in self.conn.network.subnets():
            subnet_id = str(getattr(subnet, "id", "") or "")
            name = str(getattr(subnet, "name", "") or "")
            subnets.append(
                {
                    "id": subnet_id,
                    "name": name or subnet_id,
                    "cidr": str(getattr(subnet, "cidr", "") or ""),
                    "network_id": str(getattr(subnet, "network_id", "") or ""),
                }
            )
        return subnets

    def list_volume_types(self) -> list[dict[str, str]]:
        """列出卷类型，供中转机池选择源/目标卷类型（影响配额与后端）。"""
        types = []
        for volume_type in self.conn.block_storage.types():
            type_id = str(getattr(volume_type, "id", "") or "")
            name = str(getattr(volume_type, "name", "") or "")
            types.append({"id": type_id, "name": name or type_id})
        return types

    def get_volume_quota(self, project_id: str | None = None) -> dict[str, int]:
        """尽力读取卷配额（含按卷类型的 gigabytes_<类型>）；读不到返回空字典。"""
        target = project_id or ""
        if not target:
            auth_vars = self._collect_auth_attrs(getattr(self.conn, "auth", None))
            target = str(auth_vars.get("project_id") or "")
        if not target:
            return {}
        try:
            quota = self.conn.block_storage.get_quota_set(target)
            raw = quota.to_dict() if hasattr(quota, "to_dict") else dict(quota)
        except Exception as exc:  # noqa: BLE001 - 配额读取失败不影响主流程
            logging.debug("[MIGRATION] 读取卷配额失败: %s", exc)
            return {}
        limits: dict[str, int] = {}
        for key, value in (raw or {}).items():
            try:
                limits[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
        return limits

    def delete_port(self, port_id: str) -> None:
        self.conn.network.delete_port(port_id)

    @staticmethod
    def enable_http_debug_logging() -> None:
        """Enable openstacksdk HTTP request/response logging for debugging."""
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        if root.handlers:
            for handler in root.handlers:
                handler.setLevel(logging.DEBUG)
        for logger_name in (
            "openstack",
            "openstack.sdk",
            "keystoneauth",
            "urllib3",
        ):
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.DEBUG)

    # ---------- lifecycle helpers ----------

    def stop_server(self, server_id: str):
        self.conn.compute.stop_server(server_id)

    def start_server(self, server_id: str):
        self.conn.compute.start_server(server_id)

    def delete_server(self, server_id: str, *, force: bool = False) -> None:
        """删除实例；force=True 走 Nova os-force_delete。

        这套环境配置了 ``reclaim_instance_interval=86400``，普通 delete 只是
        软删除，实例和它的启动卷会滞留 24 小时；中转机是一次性资源，必须硬删。
        """
        if force:
            self.conn.compute.delete_server(server_id, force=True)
            return
        self.conn.compute.delete_server(server_id)

    def wait_server_status(
        self,
        server_id: str,
        target: str,
        fail_states: set[str] | None = None,
        timeout: int = 900,
        poll_interval: int = 5,
        not_found_grace: float = 120.0,
    ):
        """等待实例达到目标状态，容忍 create 之后的短暂 404。

        实例刚创建时可能还没落到 Nova 的 cell DB，立刻查询会拿到 404
        "Instance could not be found"；把它当成立即失败会把一次成功的建机
        判死。这里在 not_found_grace 秒内继续轮询，超时才报错。
        """
        return self._wait_server_state(
            server_id,
            target=target,
            fail_states=fail_states,
            timeout=timeout,
            poll_interval=poll_interval,
            not_found_grace=not_found_grace,
            start_if_shutoff=False,
        )

    def wait_server_booted(
        self,
        server_id: str,
        *,
        target: str = "ACTIVE",
        fail_states: set[str] | None = None,
        timeout: int = 900,
        poll_interval: int = 5,
        not_found_grace: float = 120.0,
    ):
        """等待建机流程把实例带到目标状态；只有停在 SHUTOFF 才补一次 start。

        多数云在 create 后会自动开机，插队调 os-start 只会撞上 404/409。
        """
        return self._wait_server_state(
            server_id,
            target=target,
            fail_states=fail_states,
            timeout=timeout,
            poll_interval=poll_interval,
            not_found_grace=not_found_grace,
            start_if_shutoff=True,
        )

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        """识别 openstacksdk 的 NotFoundException，兼容测试里的鸭子类型。"""
        return (
            getattr(exc, "status_code", None) == 404
            or type(exc).__name__ == "NotFoundException"
        )

    def _wait_server_state(
        self,
        server_id: str,
        *,
        target: str,
        fail_states: set[str] | None,
        timeout: int,
        poll_interval: int,
        not_found_grace: float,
        start_if_shutoff: bool,
    ):
        fail_states = fail_states or {"ERROR", "PAUSED", "SUSPENDED"}
        deadline = time.time() + timeout
        visibility_deadline = time.time() + not_found_grace
        started = False
        while time.time() < deadline:
            try:
                server = self.conn.compute.get_server(server_id)
            except Exception as exc:  # noqa: BLE001 - 404 是建机后的可见性竞态
                if not self._is_not_found(exc):
                    raise
                if time.time() >= visibility_deadline:
                    raise RuntimeError(
                        f"实例 {server_id} 创建后仍不可见（Nova 返回 404），"
                        "可能是构建失败被云侧回收，请检查 nova-compute 日志"
                    ) from exc
                time.sleep(poll_interval)
                continue
            if server is None:
                if time.time() >= visibility_deadline:
                    raise RuntimeError(f"实例 {server_id} 创建后仍不可见")
                time.sleep(poll_interval)
                continue
            status = str(getattr(server, "status", "") or "")
            if status == target:
                return server
            if status in fail_states:
                raise RuntimeError(
                    f"VM {server_id} 状态异常: {status}（期望 {target}）"
                )
            if start_if_shutoff and status == "SHUTOFF" and not started:
                # 只有云侧确实没有自动开机时才兜底启动，避免创建后立刻插队。
                self.start_server(server_id)
                started = True
            time.sleep(poll_interval)
        raise TimeoutError(f"等待 VM {server_id} 达到 {target} 超时")

    def wait_snapshot_status(
        self,
        snapshot_id: str,
        target: str = "available",
        timeout: int = 600,
        poll_interval: int = 5,
    ):
        deadline = time.time() + timeout
        while time.time() < deadline:
            snapshot = self.conn.block_storage.get_snapshot(snapshot_id)
            if snapshot.status == target:
                return snapshot
            if snapshot.status == "error":
                raise RuntimeError(f"快照 {snapshot_id} 状态异常: {snapshot.status}")
            time.sleep(poll_interval)
        raise TimeoutError(f"等待快照 {snapshot_id} 达到 {target} 超时")

    def wait_volume_status(
        self,
        volume_id: str,
        target: str = "available",
        timeout: int | None = None,
        poll_interval: int = 5,
        *,
        context: str = "",
    ):
        if timeout is None:
            timeout = volume_ready_timeout_default()
        deadline = time.time() + timeout
        wanted = _normalize_status(target)
        last_status = "unknown"
        polls = 0
        seen_attaching = False
        while time.time() < deadline:
            volume = self.conn.block_storage.get_volume(volume_id)
            last_status = str(volume.status or "unknown")
            status = _normalize_status(last_status)
            if status == wanted:
                return volume
            if status.startswith("error"):
                raise RuntimeError(
                    f"卷 {volume_id} 状态异常: {last_status}{context}"
                )
            if wanted == "in_use":
                # attach 失败会被 Cinder 回滚：先 attaching 再回到 available。
                # 这通常意味着挂载目标所在宿主机连不上存储后端，不能干等到超时。
                if status == "attaching":
                    seen_attaching = True
                elif seen_attaching and status == "available":
                    raise RuntimeError(
                        f"卷 {volume_id} 挂载被回滚：先进入 attaching 又回到 available。"
                        "通常表示挂载目标的宿主机无法连接该卷的存储后端"
                        "（主机组/LUN 映射/iSCSI、FC 链路），请检查该宿主机与阵列的对接。"
                        f"{context}"
                    )
            polls += 1
            if polls % 12 == 0:
                logging.warning(
                    "[MIGRATION] 卷 %s 等待 %s，当前状态 %s，已等待 %ss%s",
                    volume_id,
                    target,
                    last_status,
                    polls * poll_interval,
                    context,
                )
            time.sleep(poll_interval)
        raise TimeoutError(
            f"等待卷 {volume_id} 达到 {target} 超时"
            f"（等待 {timeout}s，最后状态 {last_status}）{context}"
        )

    def log(self, message: str, level: str = "info") -> None:
        getattr(logging, level)(f"[MIGRATION] {message}")

    def diagnostic_info(self) -> dict[str, Any]:
        """Return facts about the connection scope for manual debugging."""
        conn = self.conn
        auth = getattr(conn, "auth", None)
        # Authorize first so the plugin finishes name/UUID resolution.
        try:
            conn.authorize()
        except Exception as exc:  # noqa: BLE001
            return {"authorize_error": str(exc)}
        auth_vars = self._collect_auth_attrs(auth)
        auth_vars.pop("password", None)

        def _primitive(value: Any) -> Any:
            if isinstance(value, (str, int, float, bool)) or value is None:
                return value
            return repr(value)[:240]

        info: dict[str, Any] = {
            "diag_version": DIAG_VERSION,
            "auth_url": getattr(auth, "auth_url", None),
            "auth_plugin": type(auth).__name__ if auth else None,
            # Full attribute dump (password removed) lets us verify the scope
            # that openstacksdk actually resolved for this account.
            "auth_attrs": {
                key: _primitive(value)
                for key, value in auth_vars.items()
                if "pass" not in key.lower()
                and "token" not in key.lower()
                and not callable(value)
            },
        }
        # Real API probe with the same connection exposes scope/quota problems.
        info["probe"] = {}
        try:
            volumes = list(conn.block_storage.volumes(limit=1))
            info["probe"]["volume_list_ok"] = True
            info["probe"]["volume_count_sample"] = len(volumes)
        except Exception as exc:  # noqa: BLE001
            info["probe"]["volume_list_ok"] = False
            info["probe"]["volume_list_error"] = str(exc)
        try:
            servers = list(conn.compute.servers(limit=1))
            info["probe"]["compute_list_ok"] = True
            info["probe"]["compute_count_sample"] = len(servers)
        except Exception as exc:  # noqa: BLE001
            info["probe"]["compute_list_ok"] = False
            info["probe"]["compute_list_error"] = str(exc)
        return info

"""常驻节点管理：建机、删机、重建、drain/resume。

建机参数来自 PoolProfileStore，云凭据来自加密 CredentialStore；
节点令牌与 SSH 密码都按 node_id 作为 AAD 加密后写入节点清单。
"""
from __future__ import annotations

import hashlib
import logging
import secrets
import time
import uuid
from typing import Any, Callable

from relay_inventory import NodeInventory, RelayNodeRecord
from relay_pool import build_cloud_init
from relay_wait import WaitTimeout, wait_for
from relay_protocol import issue_node_token


class NodeManagerError(RuntimeError):
    """建机前置条件不满足。"""


def _tenant_short(tenant_key: str) -> str:
    return hashlib.sha1(tenant_key.encode("utf-8")).hexdigest()[:8]


class RelayNodeManager:
    def __init__(
        self,
        *,
        inventory: NodeInventory,
        profiles: Any,
        credentials: Any,
        registry: Any,
        secret: bytes,
        platform_url: str,
        os_utils_factory: Callable[[dict[str, Any]], Any],
        data_port: int = 9200,
        bootstrap: bool = True,
        ready_timeout: float = 300.0,
        clock: Callable[[], float] = time.time,
    ):
        self.inventory = inventory
        self.profiles = profiles
        self.credentials = credentials
        self.registry = registry
        self.secret = secret
        self.platform_url = platform_url.rstrip("/")
        self.os_utils_factory = os_utils_factory
        self.data_port = data_port
        self.bootstrap = bootstrap
        self.ready_timeout = ready_timeout
        self._clock = clock

    def _next_index(self, tenant_key: str, role: str, az: str) -> int:
        used = {
            record.name.rsplit("-", 1)[-1]
            for record in self.inventory.nodes_in_pool(tenant_key, role, az)
        }
        index = 1
        while str(index) in used:
            index += 1
        return index

    def _password_for_pool(self, tenant_key: str, role: str, az: str) -> str:
        """扩容时复用池内既有节点的 SSH 密码，保证同池密码一致。"""
        for record in sorted(
            self.inventory.nodes_in_pool(tenant_key, role, az),
            key=lambda item: item.created_at,
        ):
            if record.ssh_password_enc:
                return self.credentials.unseal_text(
                    record.ssh_password_enc, aad=record.node_id
                )
        return ""

    def _os_utils(self, tenant_key: str) -> Any:
        auth = self.credentials.get(tenant_key)
        if not auth:
            raise NodeManagerError(f"未配置租户凭据: {tenant_key}")
        return self.os_utils_factory(auth)

    @staticmethod
    def _system_volume_type(profile: Any) -> str:
        """中转机走云硬盘启动，池参数里必须选好系统盘类型。"""
        volume_type = str(getattr(profile, "system_volume_type", "") or "").strip()
        if not volume_type:
            raise NodeManagerError(
                "池未配置中转机系统盘类型：中转机走云硬盘启动，"
                "请在「环境配置 / 中转机资源」页里选择云盘类型后重建池"
            )
        return volume_type

    @staticmethod
    def _release_floating_ip(os_utils: Any, floating_ip_id: str) -> None:
        """释放浮动 IP：先断开端口关联再删，避免残留占用 FIP 池。"""
        try:
            os_utils.delete_relay_floating_ip(floating_ip_id)
            logging.info("[MIGRATION] 已释放浮动 IP fip=%s", floating_ip_id)
        except Exception:  # noqa: BLE001 - 已释放/网络异常不阻塞删机
            logging.warning(
                "[MIGRATION] 释放浮动 IP 失败（可能已释放）fip=%s", floating_ip_id
            )

    @staticmethod
    def _release_port(os_utils: Any, port_id: str) -> None:
        """建机失败时回收已创建的端口，避免占用端口/固定 IP 配额。"""
        if not port_id:
            return
        try:
            os_utils.delete_port(port_id)
            logging.info("[MIGRATION] 已回收中转机端口 port=%s", port_id)
        except Exception:  # noqa: BLE001 - 端口可能已随实例删除
            logging.warning(
                "[MIGRATION] 回收中转机端口失败（可能已删除）port=%s", port_id
            )

    def create_node(
        self, *, tenant_key: str, role: str, az: str, slots_total: int | None = None
    ) -> RelayNodeRecord:
        profile = self.profiles.get(tenant_key, role, az)
        if profile is None:
            raise NodeManagerError(f"未配置池参数: {tenant_key}/{role}/{az}")
        os_utils = self._os_utils(tenant_key)
        node_id = uuid.uuid4().hex
        index = self._next_index(tenant_key, role, az)
        name = f"relay-{role}-{_tenant_short(tenant_key)}-{az}-{index}"
        token = issue_node_token(
            self.secret, node_id=node_id, role=role, tenant_key=tenant_key, az=az
        )
        # 密码优先级：池内既有节点 > 池参数 > 平台随机生成（保证 SSH 可登录）。
        password = (
            self._password_for_pool(tenant_key, role, az)
            or getattr(profile, "admin_password", "")
            or secrets.token_urlsafe(12)
        )
        port = os_utils.create_relay_port(
            profile.network, subnet_id=profile.subnet or None
        )
        # 跨云迁移时对端只能走浮动 IP 找到这台中转机，先绑 FIP 再注入 cloud-init。
        floating_ip_id = floating_ip = ""
        data_addr = ""
        floating_network = str(
            getattr(profile, "data_floating_network", "") or ""
        ).strip()
        try:
            if floating_network:
                fip = os_utils.create_relay_floating_ip(port.id, floating_network)
                floating_ip_id = str(getattr(fip, "id", "") or "")
                floating_ip = str(getattr(fip, "floating_ip_address", "") or "")
                data_addr = floating_ip
            user_data = build_cloud_init(
                platform_url=(
                    getattr(profile, "platform_url", "") or self.platform_url
                ),
                token=token,
                job_id="",
                role=role,
                name=name,
                data_port=profile.data_port or self.data_port,
                node_id=node_id,
                slots_total=int(slots_total or profile.slots_per_node),
                bootstrap=self.bootstrap,
                admin_password=password,
                ssh_public_key=profile.ssh_public_key,
                data_addr=data_addr,
            )
            server = os_utils.create_relay_server(
                name=name,
                image_id=profile.image,
                flavor_id=profile.flavor,
                port_ids=[port.id],
                availability_zone=az,
                user_data=user_data,
                admin_password=password or None,
                system_volume_type=self._system_volume_type(profile),
            )
        except Exception:
            if floating_ip_id:
                self._release_floating_ip(os_utils, floating_ip_id)
            # 端口在 try 之前创建，建机/绑 FIP 失败时必须一并回收。
            self._release_port(os_utils, str(getattr(port, "id", "") or ""))
            raise
        root_volume_id = str(getattr(server, "root_volume_id", "") or "")
        now = self._clock()
        record = RelayNodeRecord(
            node_id=node_id,
            name=name,
            role=role,
            tenant_key=tenant_key,
            az=az,
            server_id=server.id,
            port_id=port.id,
            network=profile.network,
            subnet=profile.subnet,
            flavor=profile.flavor,
            image=profile.image,
            system_volume_type=profile.system_volume_type,
            root_volume_id=root_volume_id,
            floating_ip_id=floating_ip_id,
            floating_ip=floating_ip,
            slots_total=int(slots_total or profile.slots_per_node),
            data_port_base=int(profile.data_port or self.data_port),
            token_enc=self.credentials.seal_text(token, aad=node_id),
            ssh_password_enc=(
                self.credentials.seal_text(password, aad=node_id) if password else ""
            ),
            state="provisioning",
            created_at=now,
            updated_at=now,
        )
        self.inventory.upsert(record)
        self.inventory.save()
        logging.info(
            "[MIGRATION] 创建常驻中转机 name=%s server=%s az=%s",
            name,
            server.id,
            az,
        )
        return record

    def wait_ready(
        self, record: RelayNodeRecord, *, timeout: float | None = None
    ) -> bool:
        """等待 agent 注册；超时返回 False，由调用方决定是否重建。"""
        def registered() -> bool:
            agent = self.registry.find_by_name(record.name)
            if agent is None:
                return False
            record.state = "ready"
            record.agent_version = getattr(agent, "version", "")
            record.fixed_ip = getattr(agent, "data_address", "") or record.fixed_ip
            record.updated_at = self._clock()
            self.inventory.upsert(record)
            self.inventory.save()
            return True

        try:
            wait_for(
                registered,
                timeout=float(timeout or self.ready_timeout),
                interval=1.0,
                clock=self._clock,
                message=f"中转机 {record.name} 未在超时前注册",
            )
        except WaitTimeout:
            return False
        return True

    def delete_node(self, node_id: str) -> bool:
        record = self.inventory.get(node_id)
        if record is None:
            return False
        try:
            os_utils = self._os_utils(record.tenant_key)
            if record.server_id:
                # 软删除会让中转机和启动卷滞留整个 reclaim 窗口，必须硬删。
                os_utils.delete_server(record.server_id, force=True)
            if record.root_volume_id:
                try:
                    os_utils.delete_volume_wait(record.root_volume_id)
                except Exception:  # noqa: BLE001 - 卷可能已随实例删除
                    logging.warning(
                        "[MIGRATION] 删除中转机启动卷失败（可能已随实例回收）volume=%s",
                        record.root_volume_id,
                    )
            if record.floating_ip_id:
                self._release_floating_ip(os_utils, record.floating_ip_id)
            if record.port_id:
                os_utils.delete_port(record.port_id)
        except Exception:  # noqa: BLE001 - 云上资源已不存在时仍要清清单
            logging.exception("[MIGRATION] 删除常驻中转机云上资源失败 node=%s", node_id)
        try:
            self.registry.drop_by_name(record.name)
        except AttributeError:
            pass
        self.inventory.remove(node_id)
        self.inventory.save()
        return True

    def rebuild(self, node_id: str) -> RelayNodeRecord | None:
        """重建一台常驻中转机：先建新机，成功后再删旧机。

        顺序不能反：删机成功后建机失败（配额不足、Nova 抖动）会让池子凭空
        少一台，而且旧记录已被删掉、连重试的锚点都没有。先建后删最坏情况是
        短暂多占一台的额度，旧机仍在、清单记录也还在，退避后可以再试。
        """
        record = self.inventory.get(node_id)
        if record is None:
            return None
        tenant_key, role, az = record.tenant_key, record.role, record.az
        slots_total = record.slots_total
        new_record = self.create_node(
            tenant_key=tenant_key, role=role, az=az, slots_total=slots_total
        )
        if new_record is None:
            return None
        logging.info(
            "[MIGRATION] 中转机重建完成 old=%s new=%s", record.name, new_record.name
        )
        self.delete_node(node_id)
        return new_record

    def drain(self, node_id: str) -> bool:
        return self._set_state(node_id, "draining")

    def resume(self, node_id: str) -> bool:
        return self._set_state(node_id, "ready")

    def rotate_token(self, node_id: str) -> str:
        record = self.inventory.get(node_id)
        if record is None:
            raise NodeManagerError("节点不存在")
        token = issue_node_token(
            self.secret,
            node_id=record.node_id,
            role=record.role,
            tenant_key=record.tenant_key,
            az=record.az,
        )
        record.token_enc = self.credentials.seal_text(token, aad=record.node_id)
        record.updated_at = self._clock()
        self.inventory.upsert(record)
        self.inventory.save()
        return token

    def list_orphans(self, node_id: str) -> list[dict[str, str]]:
        record = self.inventory.get(node_id)
        if record is None or not record.server_id:
            return []
        os_utils = self._os_utils(record.tenant_key)
        return os_utils.list_server_volume_attachments(record.server_id)

    def _set_state(self, node_id: str, state: str) -> bool:
        record = self.inventory.get(node_id)
        if record is None:
            return False
        record.state = state
        record.updated_at = self._clock()
        self.inventory.upsert(record)
        self.inventory.save()
        return True

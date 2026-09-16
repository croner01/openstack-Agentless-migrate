"""中转机资源管理 API：节点、池、租户凭据。

所有依赖云凭据的操作都先走 layer.ensure()；主密钥缺失时返回 503，
不影响 RBD 通道与 relay ephemeral 通道。
"""
from __future__ import annotations

import logging
from dataclasses import asdict

from flask import Blueprint, jsonify, request

from relay_credentials import CredentialError


def create_admin_blueprint(*, layer) -> Blueprint:
    blueprint = Blueprint("relay_admin", __name__, url_prefix="/api/relay")

    def _profile_public(profile) -> dict:
        """池参数对外视图：剥离只允许存在于内存的明文 SSH 密码。"""
        item = asdict(profile)
        item.pop("admin_password", None)
        return item

    def _require_ready():
        if layer.ensure():
            return None
        return (
            jsonify(
                {
                    "ok": False,
                    "error": layer.error
                    or "常驻中转机未就绪：请配置 MIGRATION_SECRET_KEY",
                }
            ),
            503,
        )

    @blueprint.get("/nodes")
    def list_nodes():
        tenant = request.args.get("tenant_key") or ""
        role = request.args.get("role") or ""
        az = request.args.get("az") or ""
        nodes = [
            record
            for record in layer.inventory.all()
            if (not tenant or record.tenant_key == tenant)
            and (not role or record.role == role)
            and (not az or record.az == az)
        ]
        return jsonify({"ok": True, "nodes": [asdict(record) for record in nodes]})

    @blueprint.get("/nodes/<node_id>")
    def get_node(node_id: str):
        record = layer.inventory.get(node_id)
        if record is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        return jsonify({"ok": True, "node": asdict(record)})

    @blueprint.delete("/nodes/<node_id>")
    def delete_node(node_id: str):
        record = layer.inventory.get(node_id)
        if record is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        if record.slots_used or layer.leases.active_for_node(node_id):
            return jsonify({"ok": False, "error": "节点仍有在途任务，请先 drain"}), 409
        missing = _require_ready()
        if missing:
            return missing
        layer.node_manager.delete_node(node_id)
        return jsonify({"ok": True})

    @blueprint.post("/nodes/<node_id>/rebuild")
    def rebuild_node(node_id: str):
        if layer.inventory.get(node_id) is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        missing = _require_ready()
        if missing:
            return missing
        record = layer.node_manager.rebuild(node_id)
        return jsonify({"ok": True, "node": asdict(record)})

    @blueprint.post("/nodes/<node_id>/drain")
    def drain_node(node_id: str):
        missing = _require_ready()
        if missing:
            return missing
        if not layer.node_manager.drain(node_id):
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        return jsonify({"ok": True})

    @blueprint.post("/nodes/<node_id>/resume")
    def resume_node(node_id: str):
        missing = _require_ready()
        if missing:
            return missing
        if not layer.node_manager.resume(node_id):
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        return jsonify({"ok": True})

    @blueprint.post("/nodes/<node_id>/rotate-token")
    def rotate_token(node_id: str):
        if layer.inventory.get(node_id) is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        missing = _require_ready()
        if missing:
            return missing
        token = layer.node_manager.rotate_token(node_id)
        return jsonify({"ok": True, "token": token})

    @blueprint.get("/nodes/<node_id>/password")
    def show_password(node_id: str):
        record = layer.inventory.get(node_id)
        if record is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        if not record.ssh_password_enc:
            return jsonify({"ok": False, "error": "该节点没有密码凭据"}), 404
        missing = _require_ready()
        if missing:
            return missing
        try:
            password = layer.credentials.unseal_text(
                record.ssh_password_enc, aad=node_id
            )
        except CredentialError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 500
        logging.warning(
            "[MIGRATION] 查看节点 SSH 密码 node=%s from=%s",
            node_id,
            request.remote_addr,
        )
        return jsonify({"ok": True, "password": password})

    @blueprint.get("/nodes/<node_id>/orphans")
    def node_orphans(node_id: str):
        if layer.inventory.get(node_id) is None:
            return jsonify({"ok": False, "error": "节点不存在"}), 404
        missing = _require_ready()
        if missing:
            return missing
        return jsonify({"ok": True, "orphans": layer.node_manager.list_orphans(node_id)})

    @blueprint.get("/pools")
    def list_pools():
        summary: dict[tuple, dict] = {}
        for record in layer.inventory.all():
            item = summary.setdefault(
                record.pool_key,
                {
                    "tenant_key": record.tenant_key,
                    "role": record.role,
                    "az": record.az,
                    "nodes": 0,
                    "slots_used": 0,
                    "slots_total": 0,
                },
            )
            item["nodes"] += 1
            item["slots_used"] += record.slots_used
            item["slots_total"] += record.slots_total
        pools = list(summary.values())
        for pool in pools:
            profile = layer.profiles.get(
                pool["tenant_key"], pool["role"], pool["az"]
            )
            pool["max_nodes"] = profile.max_nodes if profile else 6
            pool["min_nodes"] = profile.min_nodes if profile else 1
            pool["slots_per_node"] = profile.slots_per_node if profile else 5
        return jsonify({"ok": True, "pools": pools})

    @blueprint.get("/pools/profiles")
    def list_profiles():
        return jsonify(
            {"ok": True, "profiles": [_profile_public(item) for item in layer.profiles.all()]}
        )

    @blueprint.put("/pools/profile")
    def upsert_profile():
        body = request.get_json(force=True, silent=True) or {}
        try:
            profile = layer.profiles.upsert_profile(body)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        layer.profiles.save()
        return jsonify({"ok": True, "profile": _profile_public(profile)})

    @blueprint.delete("/pools/profile")
    def delete_profile():
        tenant_key = request.args.get("tenant_key") or ""
        role = request.args.get("role") or ""
        az = request.args.get("az") or ""
        if not (tenant_key and role and az):
            return jsonify({"ok": False, "error": "tenant_key/role/az 必填"}), 400
        if layer.profiles.get(tenant_key, role, az) is None:
            return jsonify({"ok": False, "error": "该池没有建机参数"}), 404
        # 池里还有节点时删掉参数会让节点失去扩容依据，先要求清空节点。
        if layer.inventory.nodes_in_pool(tenant_key, role, az):
            return (
                jsonify({"ok": False, "error": "该池仍有节点，请先删除节点"}),
                409,
            )
        layer.profiles.remove(tenant_key, role, az)
        layer.profiles.save()
        return jsonify({"ok": True})

    @blueprint.post("/pools/scale")
    def scale_pool():
        body = request.get_json(force=True, silent=True) or {}
        tenant_key = str(body.get("tenant_key") or "")
        role = str(body.get("role") or "")
        az = str(body.get("az") or "")
        target = int(body.get("target_nodes") or 0)
        profile = layer.profiles.get(tenant_key, role, az)
        if profile is None:
            return jsonify({"ok": False, "error": "该池未配置建机参数"}), 400
        if target < profile.min_nodes or target > profile.max_nodes:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": f"目标节点数必须在 {profile.min_nodes}-{profile.max_nodes} 之间",
                    }
                ),
                400,
            )
        missing = _require_ready()
        if missing:
            return missing

        changed: list[str] = []
        current = len(layer.inventory.nodes_in_pool(tenant_key, role, az))
        for _ in range(max(target - current, 0)):
            record = layer.node_manager.create_node(
                tenant_key=tenant_key, role=role, az=az
            )
            changed.append(f"+{record.node_id}")

        for _ in range(max(current - target, 0)):
            idle = [
                record
                for record in layer.inventory.nodes_in_pool(tenant_key, role, az)
                if not record.slots_used
                and not layer.leases.active_for_node(record.node_id)
            ]
            if not idle:
                return (
                    jsonify({"ok": False, "error": "剩余节点仍有在途任务，无法缩容"}),
                    409,
                )
            record = sorted(idle, key=lambda item: item.created_at, reverse=True)[0]
            layer.node_manager.delete_node(record.node_id)
            changed.append(f"-{record.node_id}")
        return jsonify({"ok": True, "changed": changed})

    @blueprint.get("/tenants")
    def list_tenants():
        missing = _require_ready()
        if missing:
            return missing
        return jsonify({"ok": True, "tenants": layer.credentials.tenants()})

    @blueprint.put("/tenants")
    def save_tenant():
        body = request.get_json(force=True, silent=True) or {}
        tenant_key = str(body.get("tenant_key") or "")
        auth = body.get("auth") or {}
        if not tenant_key or not isinstance(auth, dict) or not auth:
            return jsonify({"ok": False, "error": "tenant_key 与 auth 必填"}), 400
        missing = _require_ready()
        if missing:
            return missing
        layer.credentials.save(tenant_key, auth)
        layer.credentials.flush()
        return jsonify({"ok": True})

    @blueprint.delete("/tenants")
    def delete_tenant():
        tenant_key = request.args.get("tenant_key") or ""
        if layer.inventory.nodes_for_role(
            tenant_key, "source"
        ) or layer.inventory.nodes_for_role(tenant_key, "target"):
            return jsonify({"ok": False, "error": "该租户仍有节点，先删除节点"}), 409
        missing = _require_ready()
        if missing:
            return missing
        layer.credentials.delete(tenant_key)
        layer.credentials.flush()
        return jsonify({"ok": True})

    return blueprint

"""平台侧 /api/relay/* 接口：agent 注册、心跳、领任务与上报。

agent 只做出向请求，平台不主动连中转机；取消通过心跳响应下发。
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

from flask import Blueprint, Response, jsonify, request

from relay_ledger import Ledger, VolumeTaskRecord
from relay_protocol import ProtocolError, verify_node_token, verify_token
from relay_registry import RelayState

# 允许下发的文件白名单：中转机只需要这三个纯标准库模块。
AGENT_PACKAGE_FILES = (
    "relay_protocol.py",
    "relay_transfer.py",
    "relay_agent.py",
)

SYSTEMD_UNIT = """[Unit]
Description=Relay agent for cross-cloud volume migration
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/relay/agent.env
WorkingDirectory=/opt/relay
ExecStart=/usr/bin/python3 -u /opt/relay/relay_agent.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""


def _as_int(body: dict, key: str, default: int = 0) -> int:
    """安全解析 agent 上报的整数字段；畸形值返回 4xx 而不是 500。"""
    raw = body.get(key, default)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"{key} 必须是整数") from exc


def _session_owns_task(state: RelayState, task: dict, session_id: str) -> bool:
    """校验上报任务的会话确实是该任务的归属 agent。

    任务绑定了 agent_id/agent_name 时必须一致；未绑定（按角色自由领取）时
    只要求会话真实存在，避免任意人凭 task_id 伪造进度/结果。
    """
    agent = state.by_session(session_id) if session_id else None
    if agent is None:
        return False
    pinned_id = task.get("agent_id")
    if pinned_id and pinned_id != agent.agent_id:
        return False
    pinned_name = task.get("agent_name")
    if pinned_name and pinned_name != agent.name:
        return False
    return True


def build_bootstrap_script(platform_url: str) -> str:
    """生成中转机自举安装脚本；令牌从 /etc/relay/agent.env 读取。"""
    base = platform_url.rstrip("/")
    downloads = "\n".join(
        f'curl -fsSL "$BASE/api/relay/pkg/{name}?token=$TOKEN" -o "$DEST/{name}"'
        for name in AGENT_PACKAGE_FILES
    )
    return f"""#!/bin/bash
# 由迁移平台下发：安装 relay agent 并启动服务。需要 root 权限。
set -euo pipefail
BASE="{base}"
install -d -m 0755 /opt/relay /etc/relay
DEST=/opt/relay
TOKEN="$(sed -n 's/^RELAY_TOKEN=//p' /etc/relay/agent.env)"
if [ -z "$TOKEN" ]; then
  echo "缺少 /etc/relay/agent.env 或其中的 RELAY_TOKEN" >&2
  exit 1
fi
{downloads}
cat >/etc/systemd/system/relay-agent.service <<'UNIT'
{SYSTEMD_UNIT}UNIT
systemctl daemon-reload
systemctl enable --now relay-agent
systemctl status relay-agent --no-pager || true
"""


def create_blueprint(
    state: RelayState,
    *,
    inventory=None,
    credentials=None,
    credentials_provider=None,
) -> Blueprint:
    """relay 控制面蓝图。

    常驻模式的节点令牌必须拿清单里的 token_enc 做一致性校验，因此 inventory
    不允许缺省漏传；credentials 支持惰性获取（主密钥在进程启动后才可用）。
    """
    blueprint = Blueprint("relay", __name__, url_prefix="/api/relay")

    def _resolve_credentials():
        if credentials is not None:
            return credentials
        if credentials_provider is None:
            return None
        try:
            return credentials_provider()
        except Exception:  # noqa: BLE001 - 凭据不可用时按校验失败处理
            logging.exception("[MIGRATION] 加载中转机凭据失败，节点令牌将校验失败")
            return None

    def _verify_node_token_against_inventory(token: str) -> dict[str, str]:
        """节点长期令牌：签名有效 + 与节点记录里的当前令牌一致（轮换后旧令牌失效）。"""
        payload = verify_node_token(state.secret, token)
        record = inventory.get(payload["node_id"]) if inventory is not None else None
        if record is None:
            raise ProtocolError("unknown node")
        stored = ""
        store = _resolve_credentials()
        if store is not None and record.token_enc:
            stored = store.unseal_text(record.token_enc, aad=record.node_id)
        if stored != token:
            raise ProtocolError("node token revoked")
        return payload

    def _verify_any_token(token: str) -> None:
        """cloud-init 拉安装脚本/agent 包时用：作业令牌与节点令牌都接受。"""
        if (token or "").count(".") == 1 and inventory is not None:
            _verify_node_token_against_inventory(token)
            return
        verify_token(state.secret, token, now=time.time())

    @blueprint.post("/register")
    def register():
        body = request.get_json(force=True, silent=True) or {}
        token = str(body.get("token", ""))
        try:
            # 节点令牌是 "<b64url payload>.<sig>"，作业令牌是四段 "|" 分隔。
            if token.count(".") == 1 and inventory is not None:
                payload = _verify_node_token_against_inventory(token)
                job_id, role = "", payload["role"]
            else:
                job_id, role = verify_token(state.secret, token, now=time.time())
        except ProtocolError as exc:
            return jsonify({"error": str(exc)}), 401
        try:
            agent = state.register(
                job_id=job_id,
                role=role,
                name=str(body.get("name", "")),
                version=str(body.get("agent_version", "")),
                address=request.remote_addr or "",
                now=time.time(),
                data_address=str(body.get("data_address", "")),
                data_port=_as_int(body, "data_port", 9200) or 9200,
                ssh_public_key=str(body.get("ssh_public_key", "")),
                slots_total=_as_int(body, "slots_total", 1) or 1,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 409
        return jsonify(
            {
                "session_id": agent.session_id,
                "agent_id": agent.agent_id,
                "heartbeat_interval": state.heartbeat_interval,
            }
        )

    @blueprint.post("/heartbeat")
    def heartbeat():
        body = request.get_json(force=True, silent=True) or {}
        session_id = str(body.get("session_id", ""))
        agent = state.heartbeat(session_id, now=time.time())
        if agent is None:
            return jsonify({"error": "unknown session"}), 404
        cancels = state.cancels_for(session_id)
        return jsonify({"cancel": bool(cancels), "cancel_tasks": cancels})

    @blueprint.get("/tasks/next")
    def tasks_next():
        session_id = request.args.get("session_id", "")
        task = state.dispatch(session_id)
        if task is None:
            return ("", 204)
        return jsonify(task)

    @blueprint.post("/tasks/<task_id>/progress")
    def task_progress(task_id: str):
        body = request.get_json(force=True, silent=True) or {}
        task = state.task(task_id)
        if task is None:
            return jsonify({"error": "unknown task"}), 404
        session_id = str(body.get("session_id", ""))
        if not _session_owns_task(state, task, session_id):
            return jsonify({"error": "session does not own task"}), 403
        try:
            copied_bytes = _as_int(body, "copied_bytes")
            skipped_bytes = _as_int(body, "skipped_bytes")
        except ProtocolError as exc:
            return jsonify({"error": str(exc)}), 400
        _record_progress(
            state,
            task,
            copied_bytes,
            skipped_bytes,
        )
        _persist(state)
        # 进度响应顺带回传取消指令，agent 在传输过程中即可中止。
        cancelled = state.has_cancel(session_id, task_id)
        return jsonify(
            {
                "ok": True,
                "cancel": cancelled,
                "cancel_tasks": [task_id] if cancelled else [],
            }
        )

    @blueprint.post("/tasks/<task_id>/ready")
    def task_ready(task_id: str):
        body = request.get_json(force=True, silent=True) or {}
        task = state.task(task_id)
        session_id = str(body.get("session_id", ""))
        if task is not None and session_id and not _session_owns_task(
            state, task, session_id
        ):
            return jsonify({"error": "session does not own task"}), 403
        if not session_id:
            # 兼容不带 session_id 的旧版 agent；新版 agent 会带上并完成归属校验。
            logging.debug("[MIGRATION] task %s ready 未带 session_id", task_id)
        state.mark_task_ready(task_id)
        return jsonify({"ok": True})

    @blueprint.get("/bootstrap")
    def bootstrap():
        """返回自举安装脚本，供 cloud-init 拉取后执行。"""
        token = request.args.get("token", "")
        try:
            _verify_any_token(token)
        except ProtocolError as exc:
            return jsonify({"error": str(exc)}), 401
        script = build_bootstrap_script(request.host_url.rstrip("/"))
        return Response(script, mimetype="text/x-shellscript")

    @blueprint.get("/pkg/<name>")
    def package(name: str):
        """下发 agent 模块文件；只允许白名单内的三个文件。"""
        if name not in AGENT_PACKAGE_FILES:
            return jsonify({"error": "unknown package"}), 404
        try:
            _verify_any_token(request.args.get("token", ""))
        except ProtocolError as exc:
            return jsonify({"error": str(exc)}), 401
        path = Path(__file__).resolve().parent / name
        if not path.exists():
            return jsonify({"error": "package missing on server"}), 500
        return Response(path.read_text(encoding="utf-8"), mimetype="text/x-python")

    @blueprint.post("/tasks/<task_id>/result")
    def task_result(task_id: str):
        body = request.get_json(force=True, silent=True) or {}
        task = state.task(task_id)
        if task is None:
            return jsonify({"error": "unknown task"}), 404
        session_id = str(body.get("session_id", ""))
        if not _session_owns_task(state, task, session_id):
            return jsonify({"error": "session does not own task"}), 403
        status = str(body.get("status", ""))
        try:
            copied_bytes = _as_int(body, "copied_bytes")
            skipped_bytes = _as_int(body, "skipped_bytes")
        except ProtocolError as exc:
            return jsonify({"error": str(exc)}), 400
        state.record_result(
            task_id,
            status,
            copied_bytes,
            digest=str(body.get("digest", "")),
            error=str(body.get("error", "")),
        )
        if status == "done":
            _record_progress(
                state,
                task,
                copied_bytes,
                skipped_bytes,
            )
        state.complete_task(session_id, task_id)
        _persist(state)
        return jsonify({"ok": True})

    return blueprint


def _record_progress(
    state: RelayState, task: dict, copied_bytes: int, skipped_bytes: int = 0
) -> None:
    ledger = getattr(state, "ledger", None)
    if not isinstance(ledger, Ledger):
        return
    job_id = task.get("job_id")
    volume_id = task.get("volume_id")
    if not job_id or not volume_id:
        return
    # agent 上报的是本次会话已传字节；断点续传时要加上 task.offset 才是卷的绝对进度。
    copied_bytes = int(task.get("offset") or 0) + int(copied_bytes or 0)
    # 跳过字节同理：本轮之前已经跳过的量由平台放在 skipped_base 里带回来。
    skipped_base = int(task.get("skipped_base") or 0)
    record = ledger.get(job_id, volume_id)
    if record is None:
        record = VolumeTaskRecord(
            job_id=job_id, vm_id=task.get("vm_id", ""), volume_id=volume_id
        )
    record.copied_bytes = copied_bytes
    record.skipped_bytes = skipped_base + int(skipped_bytes or 0)
    record.updated_at = time.time()
    ledger.upsert(record)


def _persist(state: RelayState) -> None:
    ledger = getattr(state, "ledger", None)
    if isinstance(ledger, Ledger):
        ledger.save()

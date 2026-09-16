import json
import hmac
import logging
import os
import shutil
import threading
import time
import uuid
from logging.handlers import RotatingFileHandler
from typing import Any

import pandas as pd
from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import RequestEntityTooLarge

from ceph_utils import (
    COPY_GATE,
    MiB,
    MON_PROBE_TIMEOUT_SECONDS,
    CephUtils,
    any_mon_reachable,
    copy_stall_timeout_seconds,
    parse_mon_endpoints,
    rbd_cmd_timeout_seconds,
)
from config import LOG_FILE, UPLOAD_FOLDER, default_vm_pass
from env_utils import env_float, env_int, env_str
from environment_profiles import EnvironmentProfile, EnvironmentProfileStore
from excel_parser import parse_mode, parse_rows, parse_selected_rows
from graceful_shutdown import ShutdownCoordinator
from job_manager import JobManager
from migration_manager import MigrationManager
from migration_planner import override_key
from openstack_utils import OpenStackUtils
from relay_admin_api import create_admin_blueprint
from relay_api import create_blueprint
from relay_catalog import build_catalog, preflight
from relay_credentials import CredentialError, CredentialStore, Sealer
from relay_inventory import NodeInventory
from relay_ledger import Ledger
from relay_lease import LeaseStore
from relay_resources import RelayResourceLayer, load_or_create_credentials
from relay_registry import RelayState
from relay_runtime import (
    RelayRuntime,
    drop_runtime,
    get_runtime,
    parse_relay_options,
    register_runtime,
    relay_options_from_form,
    sweep_all_runtimes,
)
from relay_secret import load_or_create_secret
from state_machine import JobStatus
from openstack_utils import DIAG_VERSION


app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = (
    env_int("MIGRATION_MAX_UPLOAD_MB", 20, minimum=1) * 1024 * 1024
)

#: 统一访问令牌。设置 MIGRATION_API_TOKEN 后，浏览器/管理类 /api/* 接口都必须
#: 携带该令牌（X-API-Token / Authorization: Bearer / token 查询参数）；agent 的
#: /api/relay/* 数据面自带签名令牌，不受此开关约束。未设置时保持兼容放行，
#: 但启动日志会明确告警，避免"以为有鉴权"。
API_TOKEN = (os.environ.get("MIGRATION_API_TOKEN") or "").strip()
if not API_TOKEN:
    logging.warning(
        "[MIGRATION] 未设置 MIGRATION_API_TOKEN：/api/* 无鉴权，"
        "请在入口层启用认证或配置该环境变量"
    )


@app.before_request
def _require_api_token():
    if not API_TOKEN:
        return None
    if request.blueprint == "relay":
        # agent 数据面：register/heartbeat/progress 等自带签名令牌。
        return None
    if not (request.path or "").startswith("/api/"):
        # 首页与静态资源放行，令牌由前端从 localStorage 取并附在 fetch 上。
        return None
    bearer = request.headers.get("Authorization", "")
    if bearer.lower().startswith("bearer "):
        bearer = bearer[7:].strip()
    supplied = (
        request.headers.get("X-API-Token")
        or bearer
        or request.args.get("token")
        or request.form.get("token")
        or ""
    )
    if hmac.compare_digest(str(supplied), API_TOKEN):
        return None
    return jsonify({"ok": False, "error": "unauthorized"}), 401


job_manager = JobManager()
shutdown_coordinator = ShutdownCoordinator(job_manager)

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# 中转机 agent 控制面：签名密钥必须持久化，否则平台重启后常驻节点全部失联。
RELAY_SECRET_PATH = os.path.join(UPLOAD_FOLDER, "relay-secret")
RELAY_STATE = RelayState(secret=load_or_create_secret(RELAY_SECRET_PATH))


def _load_store_or_empty(store_cls, path: str, label: str):
    """加载 JSON 存储；文件损坏时按空状态启动，不能让进程起不来。"""
    try:
        return store_cls.load(path)
    except (OSError, ValueError) as exc:
        logging.error(
            "[MIGRATION] 读取%s失败，按空状态启动（原文件保留待人工修复）：%s",
            label,
            exc,
        )
        return store_cls(path)


RELAY_STATE.ledger = _load_store_or_empty(
    Ledger,
    os.path.join(UPLOAD_FOLDER, "relay-ledger-platform.json"),
    "平台台账",
)
RELAY_INVENTORY = _load_store_or_empty(
    NodeInventory, os.path.join(UPLOAD_FOLDER, "relay-nodes.json"), "中转机清单"
)
RELAY_LEASES = _load_store_or_empty(
    LeaseStore, os.path.join(UPLOAD_FOLDER, "relay-leases.json"), "中转机租约"
)


def _relay_agent_credentials():
    """agent 校验节点令牌用的凭据存储：按需构建，避免导入期依赖主密钥。"""
    if RELAY_RESOURCES.ensure():
        return RELAY_RESOURCES.credentials
    logging.warning(
        "[MIGRATION] 中转机凭据不可用，节点令牌校验将失败：%s", RELAY_RESOURCES.error
    )
    return None


app.register_blueprint(
    create_blueprint(
        RELAY_STATE,
        inventory=RELAY_INVENTORY,
        credentials_provider=_relay_agent_credentials,
    )
)

# 常驻节点资源层：主密钥缺失时不阻塞导入，管理接口按需 ensure() 并返回 503。
RELAY_MASTER_KEY_PATH = os.path.join(UPLOAD_FOLDER, "relay-master-key")
RELAY_RESOURCES = RelayResourceLayer(
    inventory=RELAY_INVENTORY,
    leases=RELAY_LEASES,
    state=RELAY_STATE,
    secret=RELAY_STATE.secret,
    platform_url=os.environ.get("MIGRATION_RELAY_PLATFORM_URL") or "",
    profiles_path=os.path.join(UPLOAD_FOLDER, "relay-pools.json"),
    credentials_path=os.path.join(UPLOAD_FOLDER, "relay-credentials.json"),
    master_key_path=RELAY_MASTER_KEY_PATH,
    os_utils_factory=OpenStackUtils,
    ledger=RELAY_STATE.ledger,
)
app.register_blueprint(create_admin_blueprint(layer=RELAY_RESOURCES))


# 环境档案：与中转机共用主密钥，缺省自动生成并落盘，不新增必填环境变量。
ENV_PROFILE_PATH = os.path.join(UPLOAD_FOLDER, "environment-profiles.json")


def _environment_profile_sealer() -> Sealer:
    return Sealer(
        load_or_create_secret(RELAY_MASTER_KEY_PATH, env_var="MIGRATION_SECRET_KEY")
    )


def _environment_profile_store() -> EnvironmentProfileStore:
    return EnvironmentProfileStore.load(ENV_PROFILE_PATH, _environment_profile_sealer())


def load_relay_credentials(
    *, env: dict[str, str] | None = None
) -> CredentialStore:
    """常驻模式专用凭据存储：主密钥可来自环境变量，缺省自动生成并落盘。"""
    return load_or_create_credentials(
        os.path.join(UPLOAD_FOLDER, "relay-credentials.json"),
        RELAY_MASTER_KEY_PATH,
        env=env,
    )


@app.errorhandler(RequestEntityTooLarge)
def _handle_upload_too_large(_exc):
    max_mb = app.config["MAX_CONTENT_LENGTH"] // (1024 * 1024)
    return (
        jsonify({"ok": False, "error": f"上传内容超过 {max_mb}MB 限制"}),
        413,
    )


def _setup_logging() -> None:
    """Write logs to both the log file and stdout (kubectl logs)."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    # 避免重复 handler（热加载/多次 import）
    for handler in root.handlers[:]:
        root.removeHandler(handler)
    handlers: list[logging.Handler] = []
    if LOG_FILE:
        # 日志落在 uploads（hostPath）下并轮转，Pod 重启后仍能查历史。
        os.makedirs(os.path.dirname(os.path.abspath(LOG_FILE)), exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                LOG_FILE,
                maxBytes=env_int("MIGRATION_LOG_MAX_MB", 20, minimum=1)
                * 1024
                * 1024,
                backupCount=env_int("MIGRATION_LOG_BACKUPS", 5, minimum=1),
                encoding="utf-8",
            )
        )
    handlers.append(logging.StreamHandler())
    for handler in handlers:
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)
    for name in (
        "werkzeug",
        "openstack",
        "keystoneauth",
        "urllib3",
    ):
        logging.getLogger(name).propagate = True


_setup_logging()
if os.environ.get("MIGRATION_HTTP_DEBUG") == "1":
    OpenStackUtils.enable_http_debug_logging()


def _auth_args(prefix: str, fallback: dict[str, str] | None = None) -> dict[str, str]:
    """表单优先、档案兜底；显式留空视为"沿用档案"。"""
    fallback = fallback or {}

    def pick(key: str) -> str:
        value = request.form.get(f"{prefix}_{key}")
        if value is None or value == "":
            return fallback.get(key, "")
        return value

    base = {
        "auth_url": pick("auth_url"),
        "username": pick("username"),
        "password": pick("password"),
        "user_domain_name": pick("user_domain_name"),
    }
    project_id = pick("project_id")
    project_name = pick("project_name")
    project_domain_name = pick("project_domain_name")
    if project_id:
        base["project_id"] = project_id
    else:
        base["project_name"] = project_name
        base["project_domain_name"] = project_domain_name
    return {key: value for key, value in base.items() if value}


def _profile_auth(profile: EnvironmentProfile | None, prefix: str) -> dict[str, str]:
    """把档案某一侧凭据还原为 auth 字典（显式表单为空时的兜底）。"""
    if profile is None:
        return {}
    store = _environment_profile_store()
    auth = {
        "auth_url": getattr(profile, f"{prefix}_auth_url", ""),
        "username": getattr(profile, f"{prefix}_username", ""),
        "user_domain_name": getattr(profile, f"{prefix}_user_domain_name", ""),
        "password": store.reveal(profile, f"{prefix}_password"),
    }
    project_id = getattr(profile, f"{prefix}_project_id", "")
    if project_id:
        auth["project_id"] = project_id
    else:
        auth["project_name"] = getattr(profile, f"{prefix}_project_name", "")
        auth["project_domain_name"] = getattr(
            profile, f"{prefix}_project_domain_name", ""
        )
    return {key: value for key, value in auth.items() if value}


def _cloud_fingerprint(auth: dict[str, str]) -> str:
    """云指纹：用于对账时区分不同云的遗留资源，不含任何口令。"""
    project = auth.get("project_id") or auth.get("project_name") or ""
    return f"{auth.get('auth_url', '')}|{project}"


def start_relay_watchdog(interval: float | None = None) -> threading.Thread:
    """周期巡检中转机池与台账；没有作业运行时它是空转，不会访问任何云。"""
    period = interval or env_float("MIGRATION_RELAY_WATCHDOG_SECONDS", 60.0, minimum=1.0)

    def loop() -> None:
        while True:
            time.sleep(period)
            try:
                sweep_all_runtimes()
            except Exception:  # noqa: BLE001 - 巡检失败不能让后台线程退出
                logging.exception("[MIGRATION] 中转机周期巡检异常")
            try:
                sweep_relay_resources()
            except Exception:  # noqa: BLE001
                logging.exception("[MIGRATION] 常驻节点周期巡检异常")

    thread = threading.Thread(target=loop, name="relay-watchdog", daemon=True)
    thread.start()
    return thread


def sweep_relay_resources(*, now: float | None = None) -> list[str]:
    """常驻节点巡检：心跳超时、自动重建、空闲缩容、节点维度孤儿对账。

    资源层未就绪（缺主密钥）时直接返回，不访问任何云。
    """
    if not RELAY_RESOURCES.ready:
        return []
    current = time.time() if now is None else now
    results: list[str] = []

    for agent_id in RELAY_STATE.sweep(now=current):
        agent = RELAY_STATE.by_id(agent_id)
        if agent is None:
            continue
        for record in RELAY_INVENTORY.all():
            if record.name != agent.name:
                continue
            record.state = "unhealthy"
            record.updated_at = current
            RELAY_INVENTORY.upsert(record)
            results.append(record.node_id)

    for record in list(RELAY_INVENTORY.all()):
        if record.state != "unhealthy":
            continue
        if current - float(record.updated_at or 0.0) < relay_rebuild_seconds():
            continue
        try:
            RELAY_RESOURCES.node_manager.rebuild(record.node_id)
            results.append(f"rebuild:{record.node_id}")
        except Exception:  # noqa: BLE001 - 重建失败只告警，等人工介入
            logging.exception(
                "[MIGRATION] 自动重建常驻中转机失败 node=%s", record.node_id
            )

    # 建机后从未注册成功的残留（cloud-init 失败、平台重启打断）不会自己变成
    # unhealthy，只能按创建时间回收，否则会永久占着建机额度。
    for record in list(RELAY_INVENTORY.all()):
        if record.state != "provisioning":
            continue
        if RELAY_STATE.find_by_name(record.name) is not None:
            continue
        created = float(record.created_at or record.updated_at or 0.0)
        if current - created < relay_rebuild_seconds():
            continue
        try:
            if RELAY_RESOURCES.node_manager.delete_node(record.node_id):
                logging.warning(
                    "[MIGRATION] 回收从未注册成功的中转机 name=%s created=%.0f",
                    record.name,
                    created,
                )
                results.append(f"reap:{record.node_id}")
        except Exception:  # noqa: BLE001 - 回收失败只告警，等下一轮
            logging.exception(
                "[MIGRATION] 回收未注册中转机失败 node=%s", record.node_id
            )

    try:
        results.extend(RELAY_RESOURCES.scheduler.scale_down(now=current))
    except Exception:  # noqa: BLE001
        logging.exception("[MIGRATION] 常驻节点缩容判定失败")

    if RELAY_RESOURCES.reconciler is not None:
        try:
            results.extend(RELAY_RESOURCES.reconciler.sweep())
        except Exception:  # noqa: BLE001
            logging.exception("[MIGRATION] 节点维度孤儿对账失败")

    RELAY_INVENTORY.save()
    return results


def relay_rebuild_seconds() -> float:
    return env_float("MIGRATION_RELAY_REBUILD_SECONDS", 600.0, minimum=1.0)


def relay_hole_mode_override(
    options: dict[str, Any], environ: dict[str, str] | None = None
) -> None:
    """平台级空洞模式开关：非空时强制覆盖作业表单里的值。

    给运维留一条"某次发现异常，全平台立刻退回全量搬移"的快速通道。
    """
    value = str(
        (environ if environ is not None else os.environ).get(
            "MIGRATION_RELAY_HOLE_MODE"
        )
        or ""
    ).strip().lower()
    if value:
        options["relay_hole_mode"] = value


def relay_persistent_warnings(relay_config) -> list[str]:
    """常驻模式的预检提示：池不存在会自动建，已存在则沿用原建机参数。"""
    if getattr(relay_config, "node_mode", "ephemeral") != "persistent":
        return []
    if not RELAY_RESOURCES.ensure():
        return [
            "常驻中转机未就绪："
            + (RELAY_RESOURCES.error or "请先配置 MIGRATION_SECRET_KEY")
        ]
    warnings: list[str] = []
    for label, role, tenant_key, pool in (
        ("源端", "source", relay_config.source_cloud, relay_config.source),
        ("目标端", "target", relay_config.target_cloud, relay_config.target),
    ):
        if not pool.az:
            warnings.append(f"{label}常驻池缺少可用区，无法定位池")
            continue
        profile = RELAY_RESOURCES.profiles.get(tenant_key, role, pool.az)
        if profile is None:
            warnings.append(
                f"{label}常驻池尚未创建：提交作业时会按本次表单参数自动建机并写为池参数"
            )
        else:
            warnings.append(
                f"{label}常驻池已存在，将沿用现有建机参数："
                f"镜像 {profile.image} / flavor {profile.flavor} / 网络 {profile.network}"
            )
    return warnings


def start_job_relay_runtime(
    relay_config,
    *,
    source_auth: dict[str, str],
    target_auth: dict[str, str],
    job_id: str,
    options: dict[str, Any],
):
    """作业级中转机运行时装配；relay_config 为 None 时表示不走中转机通道。"""
    if relay_config is None:
        return None
    scheduler = None
    scheduler_factory = None
    if getattr(relay_config, "node_mode", "ephemeral") == "persistent":
        # 常驻模式：作业提交即自动建池，不需要用户先去资源页手工配置。
        if not RELAY_RESOURCES.ensure():
            raise ValueError(
                "常驻中转机未就绪：" + (RELAY_RESOURCES.error or "请先配置 MIGRATION_SECRET_KEY")
            )
        admin_password = options.get("admin_password") or ""
        prepare_relay_pool(
            layer=RELAY_RESOURCES,
            side="source",
            auth=source_auth,
            pool_config=relay_config.source,
            config=relay_config,
            admin_password=admin_password,
        )
        prepare_relay_pool(
            layer=RELAY_RESOURCES,
            side="target",
            auth=target_auth,
            pool_config=relay_config.target,
            config=relay_config,
            admin_password=admin_password,
        )
        scheduler = RELAY_RESOURCES.scheduler
        scheduler_factory = RELAY_RESOURCES.scheduler_for_pool
    runtime = RelayRuntime(
        config=relay_config,
        source_os=OpenStackUtils(source_auth),
        target_os=OpenStackUtils(target_auth),
        state=RELAY_STATE,
        ledger=RELAY_STATE.ledger,
        job_id=job_id,
        admin_password=options.get("admin_password") or "",
        inventory=RELAY_INVENTORY,
        leases=RELAY_LEASES,
        registry=RELAY_STATE,
        scheduler=scheduler,
        scheduler_factory=scheduler_factory,
    )
    try:
        runtime.start()
    except Exception:
        # start() 中途失败时可能已经建了端口/中转机；此时 runtime 还没注册，
        # 调用方拿不到它，必须在这里走一次 finish() 回收，否则资源永久残留。
        try:
            runtime.finish()
        except Exception:  # noqa: BLE001 - 清理失败不能吞掉原始异常
            logging.exception("[MIGRATION] 中转机启动失败后的清理也失败 job=%s", job_id)
        raise
    register_runtime(job_id, runtime)
    options["relay_mover_factory"] = runtime.mover_factory
    return runtime


def prepare_relay_pool(
    *,
    layer,
    side: str,
    auth: dict[str, str],
    pool_config,
    config,
    admin_password: str,
) -> dict[str, Any]:
    """把作业表单里的中转机配置落成常驻池：加密存凭据 + 首次写池参数 + 预热。"""
    tenant_key = (
        _cloud_fingerprint(auth) or getattr(config, f"{side}_cloud", "") or side
    )
    return layer.ensure_pool(
        tenant_key=tenant_key,
        role=side,
        az=pool_config.az,
        auth=dict(auth),
        profile_defaults={
            "image": pool_config.image,
            "flavor": pool_config.flavor,
            "network": pool_config.network,
            "subnet": pool_config.subnet,
            "system_volume_type": pool_config.system_volume_type,
            "data_floating_network": pool_config.data_floating_network,
            "slots_per_node": config.slots_per_node,
            "max_nodes": config.max_nodes,
            "min_nodes": config.min_nodes,
            "idle_scale_down_seconds": config.idle_scale_down_seconds,
            "data_port": config.data_port,
            "platform_url": config.platform_url,
            "ssh_public_key": config.ssh_public_key,
            "admin_password": config.admin_password or admin_password,
        },
        min_nodes=config.min_nodes,
        ready_timeout=max(float(config.ready_timeout or 300.0), 30.0),
    )


def _auth_args_from_payload(
    payload: dict[str, Any], fallback: dict[str, str] | None = None
) -> dict[str, str]:
    """JSON 载荷 → auth 参数；显式留空视为"沿用档案"（与 _auth_args 同语义）。"""
    fallback = fallback or {}

    def pick(key: str) -> str:
        value = payload.get(key)
        if value is None or value == "":
            return fallback.get(key, "")
        return value

    base = {
        "auth_url": pick("auth_url"),
        "username": pick("username"),
        "password": pick("password"),
        "user_domain_name": pick("user_domain_name"),
    }
    project_id = pick("project_id")
    if project_id:
        base["project_id"] = project_id
    else:
        base["project_name"] = pick("project_name")
        base["project_domain_name"] = pick("project_domain_name")
    return {key: value for key, value in base.items() if value}


def _auth_args_from_request_payload(
    payload: dict[str, Any],
) -> tuple[dict[str, str], str | None]:
    """带环境档案兜底的 JSON 鉴权解析。

    档案里的密码不回显到表单，所以前端只回传 profile_id + side，这里补 password。
    返回 (auth_args, error_message)；错误由调用方转成 400 响应。
    """
    profile_id = str(payload.get("profile_id") or "").strip()
    side = str(payload.get("side") or "").strip().lower()
    fallback: dict[str, str] = {}
    if profile_id:
        profile = _environment_profile_store().get(profile_id)
        if profile is None:
            return {}, f"环境档案不存在：{profile_id}"
        if side in ("source", "target"):
            fallback = _profile_auth(profile, side)
    return _auth_args_from_payload(payload, fallback), None


def _positive_int(value: Any, default: int = 1, maximum: int = 20) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def _non_negative_float(
    value: Any,
    default: float = 0.0,
    maximum: float = 10240.0,
) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(0.0, min(parsed, maximum))


def _ensure_trailing_newline(path: str) -> None:
    """给 ceph conf 补一个结尾换行，否则 rbd 会静默丢掉最后一行配置。"""
    if not path:
        return
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except OSError:
        return
    if not data or data.endswith(b"\n"):
        return
    try:
        with open(path, "ab") as handle:
            handle.write(b"\n")
    except OSError:
        return


def _serialize_volume(volume) -> dict[str, Any]:
    return {
        "source_volume_id": volume.source_volume_id,
        "source_rbd_name": volume.source_rbd_name,
        "target_volume_id": volume.target_volume_id,
        "target_rbd_name": volume.target_rbd_name,
        "size": volume.size,
        "status": volume.status.value,
        "error": volume.error,
        "progress_percent": volume.progress_percent,
        "throughput_mb_s": volume.throughput_mb_s,
        "progress_label": volume.progress_label,
        # 前端要靠这两个字段区分"增量/全量"和"快照是否已清理"，
        # 少传时页面只能一律显示"全量"与"—"。
        "mode": volume.mode.value,
        "cleanup_status": volume.cleanup_status,
    }


def _serialize_vm(vm) -> dict[str, Any]:
    return {
        "name": vm.name,
        "target_az": vm.target_az,
        "target_image": vm.target_image,
        "target_flavor": vm.target_flavor,
        "status": vm.status.value,
        "phase": vm.phase,
        "error": vm.error,
        "source_server_id": vm.source_server_id,
        "target_server_id": vm.target_server_id,
        "created_resources": vm.created_resources,
        "volumes": [_serialize_volume(volume) for volume in vm.volumes],
        "started_at": vm.started_at,
        "finished_at": vm.finished_at,
        "duration_seconds": vm.duration_seconds,
        "source_ips": vm.source_ips,
        "target_ips": vm.target_ips,
        "target_network_ports": vm.target_network_ports,
        "cutover_requested": vm.cutover_requested,
    }


def _serialize_job(job) -> dict[str, Any]:
    return {
        "id": job.id,
        "status": job.status.value,
        "error": job.error,
        "cancelled": bool(getattr(job, "cancelled", False)),
        "created_at": job.created_at,
        "vms": [_serialize_vm(vm) for vm in job.vms],
    }


def _read_excel_rows(file_path: str):
    frame = pd.read_excel(file_path)
    return parse_rows(frame.to_dict(orient="records"))


def _json_form_field(raw: str | None, expected: type, label: str):
    """解析表单里的 JSON 字段：非法 JSON/类型返回 400，而不是 500 或后续崩溃。"""
    try:
        value = json.loads(
            raw if raw not in (None, "") else ("[]" if expected is list else "{}")
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} 不是合法 JSON: {exc}") from exc
    if not isinstance(value, expected):
        raise ValueError(f"{label} 必须是{'数组' if expected is list else '对象'}")
    return value


#: mon 端口探测的总预算：mon 列表再长也不能把提交请求拖成十几秒。
CEPH_PREFLIGHT_BUDGET_SECONDS = 6.0


def ceph_preflight_enabled() -> bool:
    """提交前的 mon 可达性探测开关，``MIGRATION_CEPH_PREFLIGHT=off`` 可关闭。"""
    return (env_str("MIGRATION_CEPH_PREFLIGHT", "on") or "on").lower() not in (
        "off",
        "0",
        "false",
        "no",
    )


def _ceph_conf_reachability_error(*conf_paths: str) -> str:
    """提交前探一次 mon 端口；可达或解析不出 mon 时返回空串。

    线上事故：目标 conf 的 mon_host 填成了一个没有 ceph 服务的地址，`rbd info`
    既不报错也不返回（客户端无限重连），线程永久占着拷贝名额，之后所有迁移
    任务都在排队。这里先把"根本连不上"的 conf 拦下来，比事后清场便宜得多。
    解析不出 mon（自定义写法、DNS-SD）时一律放行，不能因为"看不懂配置"就把
    正常提交挡回去。
    """
    if not ceph_preflight_enabled():
        return ""
    for label, path in zip(("源", "目标"), conf_paths):
        if not path:
            continue
        endpoints = parse_mon_endpoints(path)
        if not endpoints:
            continue
        if not any_mon_reachable(
            endpoints,
            timeout=MON_PROBE_TIMEOUT_SECONDS,
            budget_seconds=CEPH_PREFLIGHT_BUDGET_SECONDS,
        ):
            return (
                f"{label} Ceph 不可达：mon "
                + "、".join(f"{host}:{port}" for host, port in endpoints)
                + " 均无法建立连接，请检查 conf 里的 mon_host 是否写错、"
                "集群是否在线（确认无误可用 MIGRATION_CEPH_PREFLIGHT=off 跳过校验）"
            )
    return ""


def _create_job_and_files(
    *,
    require_ceph: bool = True,
    require_target_image: bool = True,
    profile: EnvironmentProfile | None = None,
    store: EnvironmentProfileStore | None = None,
) -> tuple:
    """Save uploads under a stable job directory and return the parsed rows.

    走中转机通道时不需要 Ceph 配置文件，因此 require_ceph=False。
    未上传 conf 但选了环境档案时，用档案里的 conf 文本兜底。
    """
    job_id = uuid.uuid4().hex[:12]
    job_dir = os.path.join(app.config["UPLOAD_FOLDER"], job_id)

    excel_file = request.files.get("excel_file")
    source_conf_file = request.files.get("source_ceph_conf_file")
    target_conf_file = request.files.get("target_ceph_conf_file")
    selected_rows = _json_form_field(
        request.form.get("selected_rows"), list, "selected_rows"
    )
    profile_conf: tuple[str, str] | None = None
    if profile is not None and store is not None and (
        not source_conf_file or not target_conf_file
    ):
        candidate = (
            store.reveal(profile, "source_ceph_conf"),
            store.reveal(profile, "target_ceph_conf"),
        )
        if all(candidate):
            profile_conf = candidate
    if require_ceph and (not source_conf_file or not target_conf_file) and not (
        profile_conf
    ):
        raise ValueError("缺少源/目标 Ceph conf 文件")
    if not excel_file and not selected_rows:
        raise ValueError("缺少 excel_file 或 selected_rows")

    # 校验全部通过后才建作业目录：失败的请求不该留下空目录占空间。
    os.makedirs(job_dir, exist_ok=True)

    source_conf_path = ""
    target_conf_path = ""
    if (source_conf_file and target_conf_file) or profile_conf:
        source_conf_path = os.path.join(job_dir, "source_ceph.conf")
        target_conf_path = os.path.join(job_dir, "target_ceph.conf")
        if source_conf_file and target_conf_file:
            source_conf_file.save(source_conf_path)
            target_conf_file.save(target_conf_path)
        else:
            with open(source_conf_path, "w", encoding="utf-8") as handle:
                handle.write(profile_conf[0])
            with open(target_conf_path, "w", encoding="utf-8") as handle:
                handle.write(profile_conf[1])
        # ceph 的配置文件解析器会丢掉"结尾没有换行"的那一行（stderr 里只留一句
        # read_conf: ignoring line N），最后一行恰好是 key/mon_host 时就静默失效。
        for conf_path in (source_conf_path, target_conf_path):
            _ensure_trailing_newline(conf_path)
        os.chmod(source_conf_path, 0o600)
        os.chmod(target_conf_path, 0o600)

    if excel_file:
        excel_path = os.path.join(job_dir, "migration.xlsx")
        excel_file.save(excel_path)
        os.chmod(excel_path, 0o600)
        rows = _read_excel_rows(excel_path)
    else:
        rows = parse_selected_rows(selected_rows)
    row_overrides = _json_form_field(
        request.form.get("row_overrides"), dict, "row_overrides"
    )
    global_image = (request.form.get("target_image") or "").strip() or None
    for row in rows:
        override = (
            row_overrides.get(override_key(row.source_server_id, row.vm_name))
            or row_overrides.get(row.vm_name)
            or {}
        )
        row.target_az = str(override.get("target_az") or row.target_az)
        row.target_image = str(override.get("target_image") or row.target_image or "")
        row.target_image = row.target_image or global_image
        row.target_flavor = str(override.get("target_flavor") or row.target_flavor or "") or None
        if override.get("mode"):
            row.mode = parse_mode(override["mode"])
        # 中转机通道用拷贝过来的目标卷启动 VM，不依赖目标镜像。
        if require_target_image and not row.target_image:
            raise ValueError(f"VM {row.vm_name} 缺少目标镜像，请在页面选择")
    return job_id, job_dir, rows, source_conf_path, target_conf_path


@app.get("/api/profiles")
def api_profiles_list():
    try:
        return jsonify(
            {"ok": True, "profiles": _environment_profile_store().list_public()}
        )
    except Exception as exc:  # noqa: BLE001 - 读取失败不应 500 暴露堆栈
        logging.exception("[MIGRATION] 环境档案列表失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/profiles")
def api_profiles_save():
    payload = request.get_json(silent=True) or {}
    try:
        store = _environment_profile_store()
        profile = store.save(payload)
        store.flush()
        return jsonify({"ok": True, "profile": profile})
    except (ValueError, CredentialError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.delete("/api/profiles/<profile_id>")
def api_profiles_delete(profile_id: str):
    try:
        store = _environment_profile_store()
        if not store.delete(profile_id):
            return jsonify({"ok": False, "error": "档案不存在"}), 404
        store.flush()
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 环境档案删除失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/api/preview")
def api_preview():
    """Parse an uploaded Excel into rows for the frontend preview table."""
    try:
        excel_file = request.files.get("excel_file")
        if not excel_file:
            raise ValueError("缺少 excel_file")
        frame = pd.read_excel(excel_file)
        rows = parse_rows(frame.to_dict(orient="records"))
        return jsonify(
            {
                "ok": True,
                "rows": [
                    {
                        "vm_name": row.vm_name,
                        "target_az": row.target_az,
                        "target_image": row.target_image,
                        "target_flavor": row.target_flavor,
                    }
                    for row in rows
                ],
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] Excel 预览失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/migrate")
def api_migrate():
    try:
        if shutdown_coordinator.stopping:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": shutdown_coordinator.reject_new_message,
                    }
                ),
                503,
            )
        early_channel = (request.form.get("data_channel") or "rbd").strip().lower()
        profile_id = (request.form.get("profile_id") or "").strip()
        profile: EnvironmentProfile | None = None
        profile_store: EnvironmentProfileStore | None = None
        if profile_id:
            profile_store = _environment_profile_store()
            profile = profile_store.get(profile_id)
            if profile is None:
                return (
                    jsonify({"ok": False, "error": f"环境档案不存在：{profile_id}"}),
                    400,
                )
        (
            job_id,
            _job_dir,
            rows,
            source_conf_path,
            target_conf_path,
        ) = _create_job_and_files(
            require_ceph=early_channel != "relay",
            require_target_image=early_channel != "relay",
            profile=profile,
            store=profile_store,
        )
        reachability_error = _ceph_conf_reachability_error(
            source_conf_path, target_conf_path
        )
        if reachability_error:
            # 校验没过就不该留下空作业目录：失败的请求不该占空间。
            shutil.rmtree(_job_dir, ignore_errors=True)
            logging.warning(
                "[MIGRATION] 提交被 Ceph 可达性校验拦下: %s", reachability_error
            )
            return jsonify({"ok": False, "error": reachability_error}), 400
        source_auth = _auth_args("source", _profile_auth(profile, "source"))
        target_auth = _auth_args("target", _profile_auth(profile, "target"))

        job = job_manager.create_job(rows, job_id=job_id)
        options = {
            "job_id": job_id,
            "source_auth_args": source_auth,
            "target_auth_args": target_auth,
            "source_ceph_conf": source_conf_path,
            "target_ceph_conf": target_conf_path,
            "source_ceph_pool": request.form.get("source_ceph_pool")
            or (profile.source_ceph_pool if profile else None)
            or "volumes",
            "target_ceph_pool": request.form.get("target_ceph_pool")
            or (profile.target_ceph_pool if profile else None)
            or "volumes",
            "admin_password": (request.form.get("admin_password") or "").strip()
            or default_vm_pass,
            "vm_concurrency": _positive_int(
                request.form.get("vm_concurrency"), 1, 10
            ),
            "volume_concurrency": _positive_int(
                request.form.get("volume_concurrency"), 1, 8
            ),
            "rate_limit_mb_s": _non_negative_float(
                request.form.get("rate_limit_mb_s")
            ),
            "delta_rounds": _positive_int(
                request.form.get("delta_rounds"), 0, 10
            ),
            "delta_threshold_mb": _non_negative_float(
                request.form.get("delta_threshold_mb")
            ),
            "delta_interval_seconds": _non_negative_float(
                request.form.get("delta_interval_seconds")
            ),
            "cutover_mode": (
                "manual"
                if (request.form.get("cutover_mode") or "").strip() == "manual"
                else "auto"
            ),
            "target_volume_type": request.form.get("target_volume_type")
            or (profile.target_volume_type if profile else None)
            or None,
            "vm_network_overrides": _json_form_field(
                request.form.get("vm_network_overrides"),
                dict,
                "vm_network_overrides",
            ),
            "volume_overrides": _json_form_field(
                request.form.get("volume_overrides"), dict, "volume_overrides"
            ),
        }
        options.update(relay_options_from_form(request.form))
        relay_hole_mode_override(options)
        options["source_cloud"] = _cloud_fingerprint(source_auth)
        options["target_cloud"] = _cloud_fingerprint(target_auth)
        try:
            relay_config = parse_relay_options(options)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

        def worker():
            relay_runtime = None
            try:
                if relay_config is not None:
                    relay_runtime = start_job_relay_runtime(
                        relay_config,
                        source_auth=source_auth,
                        target_auth=target_auth,
                        job_id=job_id,
                        options=options,
                    )

                def run_vm(vm, _options):
                    source_os = OpenStackUtils(source_auth)
                    target_os = OpenStackUtils(target_auth)
                    ceph_utils = CephUtils(
                        source_conf=options["source_ceph_conf"],
                        source_pool=options["source_ceph_pool"],
                        target_conf=options["target_ceph_conf"],
                        target_pool=options["target_ceph_pool"],
                    )
                    manager = MigrationManager(
                        source_os,
                        target_os,
                        ceph_utils,
                        can_proceed=lambda: (
                            not shutdown_coordinator.stopping
                            and not job_manager.is_cancelled(job.id)
                        ),
                        cancelled=lambda: job_manager.is_cancelled(job.id),
                        cutover_requested=lambda: job_manager.is_cutover_requested(
                            job.id, vm.name
                        ),
                        sync_requested=lambda: job_manager.is_sync_requested(
                            job.id, vm.name
                        ),
                        persist=lambda: job_manager.save_now(),
                    )
                    manager.migrate_vm(vm, options)

                job_manager.execute(
                    job,
                    run_vm,
                    options,
                    should_stop=lambda: (
                        shutdown_coordinator.stopping
                        or job_manager.is_cancelled(job.id)
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                logging.exception("[MIGRATION] Job %s 后台执行异常", job.id)
                job.status = JobStatus.FAILED
                job.error = str(exc)
            finally:
                if relay_runtime is not None:
                    try:
                        relay_runtime.finish()
                    finally:
                        drop_runtime(job_id)
                # 作业结束后回收该作业在内存注册表里的任务/结果缓存，
                # 否则长跑进程里这些字典只增不减。
                try:
                    RELAY_STATE.forget_job(job_id)
                except Exception:  # noqa: BLE001 - 缓存清理失败不影响任务结果
                    logging.exception("[MIGRATION] 清理中转机会话缓存失败 job=%s", job_id)
                job_manager.unregister_worker(job_id)
                job_manager.sweep_best_effort()

        thread = threading.Thread(
            target=worker,
            # Non-daemon so a graceful shutdown waits for the running volume
            # copy to finish instead of killing it mid-import.
            daemon=False,
            name=f"job-{job.id}",
        )
        job_manager.register_worker(job_id, thread)
        thread.start()
        return jsonify({"ok": True, "job_id": job.id})
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 创建任务失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/api/jobs/<job_id>")
def api_job(job_id: str):
    job = job_manager.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    return jsonify({"ok": True, "job": _serialize_job(job)})


@app.get("/api/jobs")
def api_jobs():
    jobs = job_manager.list_jobs()
    return jsonify({"ok": True, "jobs": [_serialize_job(job) for job in jobs]})


@app.post("/api/jobs/<job_id>/cancel")
def api_job_cancel(job_id: str):
    """取消任务：标记作业停止，并通知其中转机 agent 中止正在进行的拷贝。"""
    if job_manager.get(job_id) is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    cancelled = job_manager.cancel(job_id)
    notified = 0
    for task in RELAY_STATE.tasks_for_job(job_id):
        RELAY_STATE.request_cancel(task["task_id"])
        notified += 1
    return jsonify({"ok": True, "cancelled": cancelled, "relay_agents": notified})


@app.post("/api/jobs/<job_id>/vms/<vm_name>/cutover")
def api_vm_cutover(job_id: str, vm_name: str):
    """人工确认切换：停源 → 末轮增量 → 换入 → 起目标机（仅增量·手动模式）。"""
    if job_manager.get(job_id) is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    if not job_manager.request_cutover(job_id, vm_name):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "该 VM 不在等待切换状态（可能仍在预拷贝或已结束）",
                }
            ),
            409,
        )
    return jsonify({"ok": True})


@app.post("/api/jobs/<job_id>/vms/<vm_name>/sync")
def api_vm_sync(job_id: str, vm_name: str):
    """手动同步一轮增量（仅增量·手动模式）：源机不停机，只补一轮 diff。"""
    if job_manager.get(job_id) is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    if not job_manager.request_sync(job_id, vm_name):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "该 VM 当前不能同步（可能已有同步在进行、仍在预拷贝或已结束）",
                }
            ),
            409,
        )
    return jsonify({"ok": True})


@app.delete("/api/jobs/<job_id>")
def api_job_delete(job_id: str):
    """删除任务记录与上传目录；运行中的任务必须先取消。"""
    error = job_manager.delete(job_id)
    if error:
        status = 404 if error == "任务不存在" else 409
        return jsonify({"ok": False, "error": error}), status
    drop_runtime(job_id)
    RELAY_STATE.forget_job(job_id)
    return jsonify({"ok": True})


@app.get("/api/jobs/<job_id>/relay")
def api_job_relay(job_id: str):
    runtime = get_runtime(job_id)
    if runtime is None:
        return jsonify({"ok": True, "relay": None})
    return jsonify({"ok": True, "relay": runtime.snapshot()})


@app.post("/api/jobs/<job_id>/relay/reconcile")
def api_job_relay_reconcile(job_id: str):
    runtime = get_runtime(job_id)
    if runtime is None:
        return jsonify({"ok": False, "error": "该任务没有中转机运行时"}), 404
    cleaned = runtime.reaper.reconcile_job(job_id)
    return jsonify({"ok": True, "cleaned": cleaned})


@app.post("/api/jobs/<job_id>/relay/nodes/<node_id>/rebuild")
def api_job_relay_rebuild(job_id: str, node_id: str):
    runtime = get_runtime(job_id)
    if runtime is None:
        return jsonify({"ok": False, "error": "该任务没有中转机运行时"}), 404
    node = runtime.rebuild_node(node_id)
    if node is None:
        return jsonify({"ok": False, "error": "中转机不存在"}), 404
    return jsonify({"ok": True, "node": node})


@app.post("/api/relay/catalog")
def api_relay_catalog():
    """拉取两侧云的镜像/flavor/AZ/网络，供中转机池下拉使用。"""
    try:
        source_os = OpenStackUtils(_auth_args("source"))
        target_os = OpenStackUtils(_auth_args("target"))
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 中转机目录鉴权失败")
        return jsonify({"ok": False, "error": str(exc)}), 400
    try:
        catalog = build_catalog(source_os, target_os)
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 中转机目录加载失败")
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, "catalog": catalog})


@app.post("/api/relay/preflight")
def api_relay_preflight():
    """校验中转机池配置，避免拷到一半才发现镜像/flavor/AZ 不存在。"""
    source_auth = _auth_args("source")
    target_auth = _auth_args("target")
    options = relay_options_from_form(request.form)
    # 必须和 api_migrate 用同一套覆盖规则，否则预检按表单模式校验、
    # 真正迁移却按平台强制模式执行，出现"预检通过但行为不一致"。
    relay_hole_mode_override(options)
    options["source_cloud"] = _cloud_fingerprint(source_auth)
    options["target_cloud"] = _cloud_fingerprint(target_auth)
    try:
        config = parse_relay_options(options)
    except ValueError as exc:
        return jsonify({"ok": False, "errors": [str(exc)], "warnings": []}), 400
    if config is None:
        return jsonify({"ok": True, "errors": [], "warnings": []})
    try:
        source_os = OpenStackUtils(source_auth)
        target_os = OpenStackUtils(target_auth)
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 中转机预检鉴权失败")
        return jsonify({"ok": False, "errors": [str(exc)], "warnings": []}), 400
    result = preflight(config, source_os, target_os)
    result.setdefault("warnings", []).extend(relay_persistent_warnings(config))
    return jsonify(result)


@app.post("/api/projects")
def api_projects():
    """List projects the given account can access (name -> UUID resolver)."""
    try:
        payload = request.get_json(force=True)
        auth_args, profile_error = _auth_args_from_request_payload(payload)
        if profile_error:
            return jsonify({"ok": False, "error": profile_error}), 400
        result = OpenStackUtils.list_accessible_projects(auth_args)
        scope = OpenStackUtils.accessible_project_diagnostics(auth_args)
        logging.info(
            "[MIGRATION] 载入项目: 角色可见=%s 全量=%s 合并=%s scope=%s",
            result["summary"].get("role_scoped_count"),
            result["summary"].get("all_projects_count"),
            result["summary"].get("total_count"),
            scope,
        )
        return jsonify(
            {
                "ok": True,
                "projects": result["projects"],
                "summary": result["summary"],
                "scope": scope,
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 查询可访问项目失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/diag")
def api_diag():
    """Diagnose auth scope / quota before starting a migration."""
    try:
        payload = request.get_json(force=True)
        auth_args, profile_error = _auth_args_from_request_payload(payload)
        if profile_error:
            return jsonify({"ok": False, "error": profile_error}), 400
        target_os = OpenStackUtils(auth_args)
        info = target_os.diagnostic_info()

        # Prefer the project scope Keystone actually granted.
        auth_attrs = info.get("auth_attrs", {})
        effective_project_id = (
            auth_attrs.get("project_id")
            or auth_attrs.get("_project_id")
            or auth_args.get("project_id", "")
        )
        auth_url = auth_attrs.get("auth_url") or auth_args.get("auth_url", "")
        domain = auth_attrs.get(
            "user_domain_name", auth_args.get("user_domain_name", "")
        )
        project_domain = auth_attrs.get(
            "project_domain_name", auth_args.get("project_domain_name", "")
        )
        cli = {
            "openrc": "\n".join(
                [
                    f"export OS_AUTH_URL={auth_url}",
                    f"export OS_USERNAME={auth_attrs.get('username') or auth_args.get('username', '')}",
                    f"export OS_USER_DOMAIN_NAME={domain}",
                    f"export OS_PROJECT_ID={effective_project_id}",
                    f"export OS_PASSWORD=<页面密码>",
                    f"export OS_PROJECT_DOMAIN_NAME={project_domain}",
                ]
            ),
            "commands": [
                "openstack token issue -f json",
                "openstack project show $OS_PROJECT_ID -f json",
                "openstack quota show --detail -f json",
                "openstack volume type list -f json",
                "openstack volume create --size 1 --name diag-vol-mig",
                "# 若配额正常，再复现 BFV 建 VM（把 <> 换成目标目录里的值）：",
                "openstack server create --flavor <flavor_id> \\",
                "  --image <image_id> --boot-from-volume <系统盘G> \\",
                "  --nic port-id=<port_id> --availability-zone <az> \\",
                "  --admin-pass '<页面设置的密码>' mig-diag-bfv",
            ],
        }
        return jsonify(
            {
                "ok": True,
                "diag": info,
                "cli": cli,
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 诊断失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/catalog")
def api_catalog():
    """Return target images/flavors/networks for UI selection."""
    try:
        payload = request.get_json(force=True)
        auth_args, profile_error = _auth_args_from_request_payload(payload)
        if profile_error:
            return jsonify({"ok": False, "error": profile_error}), 400
        target_os = OpenStackUtils(auth_args)
        images = [
            {"id": image.id, "name": image.name}
            for image in target_os.list_images()
        ]
        flavors = [
            {
                "id": flavor.id,
                "name": flavor.name,
                "vcpus": flavor.vcpus,
                "ram": flavor.ram,
                "disk": flavor.disk,
            }
            for flavor in target_os.list_flavors()
        ]
        networks = []
        for network in target_os.conn.network.networks():
            networks.append(
                {
                    "id": network.id,
                    "name": network.name,
                    "subnets": getattr(network, "subnet_ids", []) or [],
                }
            )
        subnets = [
            {
                "id": subnet.id,
                "name": subnet.name,
                "cidr": subnet.cidr,
                "network_id": subnet.network_id,
            }
            for subnet in target_os.conn.network.subnets()
        ]
        availability_zones = target_os.list_availability_zones()
        try:
            volume_types = target_os.list_volume_types()
        except Exception:  # noqa: BLE001 - 卷类型拿不到不影响其它下拉
            logging.exception("[MIGRATION] 查询目标卷类型失败")
            volume_types = []
        return jsonify(
            {
                "ok": True,
                "images": images,
                "flavors": flavors,
                "networks": networks,
                "subnets": subnets,
                "volume_types": volume_types,
                "availability_zones": availability_zones,
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 查询目标目录失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/source-vms")
def api_source_vms():
    """List source-project VMs for the checklist picker."""
    try:
        payload = request.get_json(force=True)
        auth_args, profile_error = _auth_args_from_request_payload(payload)
        if profile_error:
            return jsonify({"ok": False, "error": profile_error}), 400
        source_os = OpenStackUtils(auth_args)
        search = str(payload.get("search") or "").strip()
        try:
            limit = max(1, min(int(payload.get("limit") or 100), 500))
        except (TypeError, ValueError):
            limit = 100
        marker = str(payload.get("marker") or "").strip() or None
        servers = source_os.list_source_servers(
            search=search,
            limit=limit,
            marker=marker,
        )
        next_marker = servers[-1]["server_id"] if len(servers) == limit else None
        return jsonify(
            {
                "ok": True,
                "servers": servers,
                "next_marker": next_marker,
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 查询源 VM 列表失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


def _source_volume_brief(source_os, server_id: str) -> list[dict[str, Any]]:
    """源 VM 的卷清单（供页面逐卷选目标卷类型）。查不到时返回空列表。"""
    try:
        entries = source_os.get_server_volumes_with_device(server_id)
    except Exception as exc:  # noqa: BLE001 - 卷清单拿不到不阻塞网络映射
        logging.debug("[MIGRATION] 查询源 VM 卷清单失败 server=%s: %s", server_id, exc)
        return []
    return [
        {
            "volume_id": str(entry.get("volume_id") or ""),
            "size": int(entry.get("size") or 0),
            "device": str(entry.get("device") or ""),
            "is_bootable": bool(entry.get("is_bootable")),
        }
        for entry in entries
    ]


@app.post("/api/source-vm-networks")
def api_source_vm_networks():
    """Return source fixed IP / subnet info per VM for the checklist preview."""
    try:
        payload = request.get_json(force=True)
        auth_args, profile_error = _auth_args_from_request_payload(payload)
        if profile_error:
            return jsonify({"ok": False, "error": profile_error}), 400
        vm_names = payload.get("vm_names") or []
        server_ids = payload.get("server_ids") or []
        if not isinstance(vm_names, list) or not vm_names:
            raise ValueError("缺少 vm_names")
        if not isinstance(server_ids, list):
            raise ValueError("server_ids 必须是数组")
        source_os = OpenStackUtils(auth_args)
        result = {}
        for index, vm_name in enumerate(vm_names):
            server_id = (
                str(server_ids[index]).strip()
                if len(server_ids) == len(vm_names)
                else ""
            )
            server = (
                source_os.get_server_detail(server_id)
                if server_id
                else source_os.get_server_by_name(str(vm_name))
            )
            if not server:
                result[vm_name] = {"error": "源 VM 不存在"}
                continue
            try:
                ports = source_os.get_server_port_details(server.id)
            except Exception as exc:  # noqa: BLE001
                logging.exception("[MIGRATION] VM %s 端口信息查询失败", vm_name)
                result[vm_name] = {"error": str(exc)}
                continue
            result[vm_name] = {
                "server_id": server.id,
                "ports": ports,
                "volumes": _source_volume_brief(source_os, server.id),
            }
        try:
            source_volume_types = source_os.list_volume_types()
        except Exception:  # noqa: BLE001 - 卷类型拿不到不影响网络映射
            logging.exception("[MIGRATION] 查询源端卷类型失败")
            source_volume_types = []
        return jsonify(
            {
                "ok": True,
                "servers": result,
                "volume_types": source_volume_types,
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 查询源 VM 网络信息失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/healthz")
def healthz():
    stopping = shutdown_coordinator.stopping
    return (
        jsonify(
            {
                "status": "OK" if not stopping else "STOPPING",
                "diag_version": DIAG_VERSION,
                "stopping": stopping,
            }
        ),
        200,
    )


@app.get("/api/runtime")
def api_runtime():
    """只读运行参数：设置页据此核对并发/超时等生效值，省掉上机器查环境变量。

    只回传调优数值与当前名额占用，不含任何凭据。
    """
    return jsonify(
        {
            "ok": True,
            "runtime": {
                "diag_version": DIAG_VERSION,
                "stopping": shutdown_coordinator.stopping,
                "api_token_enabled": bool(API_TOKEN),
                "rbd_copy_concurrency": COPY_GATE.max_active_copies,
                "rbd_copy_active": COPY_GATE.active_copies,
                "copy_holders": [
                    {"owner": owner, "seconds": round(seconds)}
                    for owner, seconds in COPY_GATE.holders()
                ],
                "memory_high_water": COPY_GATE.high_water,
                "copy_reserve_mb": int(COPY_GATE.reserve_bytes / MiB),
                "rbd_cmd_timeout_seconds": rbd_cmd_timeout_seconds(),
                "copy_stall_timeout_seconds": copy_stall_timeout_seconds(),
                "max_upload_mb": env_int("MIGRATION_MAX_UPLOAD_MB", 20, minimum=1),
                "upload_retention_days": env_int(
                    "MIGRATION_UPLOAD_RETENTION_DAYS", 7, minimum=1
                ),
                "ceph_preflight": ceph_preflight_enabled(),
            },
        }
    )


if __name__ == "__main__":
    from werkzeug.serving import make_server

    # Register SIGTERM/SIGINT first so werkzeug does not install its own
    # immediate-exit handler and never gets a chance to override ours.
    shutdown_coordinator.install()
    http_server = make_server(host="0.0.0.0", port=19099, app=app, threaded=True)
    server_thread = threading.Thread(
        target=http_server.serve_forever,
        name="http-server",
        daemon=True,
    )
    server_thread.start()
    logging.info("[MIGRATION] 服务启动 http://0.0.0.0:19099，等待退出信号...")
    job_manager.start_persistent_saver()
    job_manager.start_storage_gc()
    start_relay_watchdog()
    shutdown_coordinator.main_loop(
        http_server,
        on_exit=lambda: (job_manager.save_now(), job_manager.stop_persistent_saver()),
    )
    logging.info("[MIGRATION] 服务已优雅退出")

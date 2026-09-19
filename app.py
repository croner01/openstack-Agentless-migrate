import io
import json
import hmac
import logging
import os
import shutil
import threading
import time
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file
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
from excel_parser import (
    parse_mode,
    parse_rows,
    parse_rows_tolerant,
    parse_selected_rows,
    parse_start_target,
)
from graceful_shutdown import ShutdownCoordinator
from job_manager import JobManager
from json_store import atomic_write_json
from migration_manager import MigrationManager
from migration_policies import MigrationPolicyStore
from migration_planner import override_key
from openstack_utils import OpenStackUtils
from relay_admin_api import create_admin_blueprint
from relay_api import create_blueprint
from relay_catalog import build_catalog, preflight
from relay_credentials import CredentialError, CredentialStore, Sealer
from relay_inventory import NodeInventory
from relay_ledger import Ledger
from relay_lease import LeaseStore
from relay_orchestrator import derived_retention_seconds
from relay_resources import RelayResourceLayer, load_or_create_credentials
from relay_registry import RelayState
from relay_runtime import (
    RelayRuntime,
    drop_runtime,
    get_runtime,
    parse_relay_options,
    register_runtime,
    relay_options_from_form,
    relay_phase_label,
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


def relay_heartbeat_settings() -> tuple[int, int]:
    """常驻中转机的心跳间隔/超时（平台级，不受单次作业表单影响）。

    timeout 至少留 3 个心跳周期：只容一次抖动的参数会把正常网络波动判成
    节点死亡，10 分钟后触发自动重建。改这两个值需要重启进程（agent 的
    心跳间隔是注册响应下发的，只在那时生效）。
    """
    interval = env_int("MIGRATION_RELAY_HEARTBEAT_INTERVAL", 10, minimum=1)
    timeout = env_int("MIGRATION_RELAY_HEARTBEAT_TIMEOUT", 30, minimum=1)
    return interval, max(timeout, interval * 3)


_RELAY_HB_INTERVAL, _RELAY_HB_TIMEOUT = relay_heartbeat_settings()
RELAY_STATE = RelayState(
    secret=load_or_create_secret(RELAY_SECRET_PATH),
    heartbeat_interval=_RELAY_HB_INTERVAL,
    heartbeat_timeout=_RELAY_HB_TIMEOUT,
)


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

# 迁移策略模板（参数快照 + 规则映射）：不存凭据，明文 JSON 即可。
MIGRATION_POLICY_PATH = os.path.join(UPLOAD_FOLDER, "migration-policies.json")

#: 「一次拉全量清单」的上限：再大就退回 marker 分页，避免单请求把内存吃满。
SOURCE_VM_ALL_LIMIT = env_int("MIGRATION_SOURCE_VM_ALL_LIMIT", 2000, minimum=1)


def _migration_policy_store() -> MigrationPolicyStore:
    return MigrationPolicyStore.load(MIGRATION_POLICY_PATH)


def busy_source_vm_names() -> set[str]:
    """仍在运行/等待中的作业已经占用的源 VM 名，用于清单里提示"已在迁移中"。"""
    names: set[str] = set()
    try:
        jobs = job_manager.list_jobs(limit=100)
    except Exception:  # noqa: BLE001 - 只是提示信息，读不到就不标
        return names
    for job in jobs:
        if job.status != JobStatus.RUNNING:
            continue
        for vm in job.vms:
            if vm.name:
                names.add(vm.name)
    return names


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


#: 迁移自身之外最吵的第三方 logger：它们的 INFO 基本是请求流水，
#: 调试迁移时会把 `[MIGRATION]` 行淹掉。
THIRD_PARTY_LOGGERS = (
    "werkzeug",
    "openstack",
    "keystoneauth",
    "urllib3",
    "requests",
    "novaclient",
    "cinderclient",
    "glanceclient",
    "neutronclient",
)


def log_only_migration() -> bool:
    """``MIGRATION_LOG_ONLY``：默认只输出迁移相关日志，``off`` 恢复全量。

    「迁移相关」= 迁移服务自己的记录（走 root logger）加上 `[MIGRATION]` /
    `[SHUTDOWN]` 前缀；ERROR 及以上一律放行，避免降噪把真实报错一起吞掉。
    """
    raw = (env_str("MIGRATION_LOG_ONLY", "on") or "on").strip().lower()
    return raw not in {"off", "0", "false", "no"}


class MigrationOnlyFilter(logging.Filter):
    """按来源过滤：第三方库的 INFO/WARNING 只当噪声，ERROR 起仍会输出。"""

    _PREFIXES = ("[MIGRATION]", "[SHUTDOWN]")

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return True
        if record.name in {"root", ""}:
            return True
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - 格式化失败不能连日志一起丢
            return True
        return message.startswith(self._PREFIXES)


#: 复用同一个实例：_setup_logging 会被重复调用，handler 每次都是新的。
MIGRATION_ONLY_FILTER = MigrationOnlyFilter()

#: 过滤是否真正生效：``MIGRATION_HTTP_DEBUG=1`` 时会让位给 HTTP 调试日志，
#: 设置页据此显示"生效值"而不是环境变量里的期望值。
_LOG_FILTER_ACTIVE = False


def _setup_logging() -> None:
    """Write logs to both the log file and stdout (kubectl logs)."""
    global _LOG_FILTER_ACTIVE
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    # 避免重复 handler（热加载/多次 import）；旧 handler 要 close，
    # 否则文件句柄一直挂着（测试里会看到 ResourceWarning）。
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()
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

    only_migration = log_only_migration()
    if only_migration and os.environ.get("MIGRATION_HTTP_DEBUG") == "1":
        # 显式要看 HTTP 请求/响应时不能过滤，否则调试开关等于失效。
        logging.info(
            "[MIGRATION] MIGRATION_HTTP_DEBUG=1：本次不做迁移日志过滤（"
            "MIGRATION_LOG_ONLY 暂不生效）"
        )
        only_migration = False
    _LOG_FILTER_ACTIVE = only_migration
    if only_migration:
        for handler in handlers:
            handler.addFilter(MIGRATION_ONLY_FILTER)
    for name in THIRD_PARTY_LOGGERS:
        logger = logging.getLogger(name)
        logger.propagate = True
        # 关闭过滤时恢复继承 root（NOTSET），否则 _setup_logging 重复调用
        # 会把这几个 logger 永久压在 WARNING。
        logger.setLevel(logging.WARNING if only_migration else logging.NOTSET)


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
    current = time.time() if now is None else now
    # 租约回收只读本地台账、不碰云，所以放在 ready 判断之前：它正是"重启后
    # 节点删不掉"的兜底，缺主密钥时也不该被跳过。
    results: list[str] = reap_relay_leases(now=current)
    if not RELAY_RESOURCES.ready:
        return results

    # 兜底清理：agent 进程消失后会话不该只增不减（同名旧会话已在注册时作废）。
    stale_agents = RELAY_STATE.prune_stale(
        now=current, max_age=RELAY_STATE.heartbeat_timeout * 10
    )
    if stale_agents:
        logging.warning(
            "[MIGRATION] 清理 %s 条陈旧 agent 会话", len(stale_agents)
        )

    for agent_id in RELAY_STATE.sweep(now=current):
        agent = RELAY_STATE.by_id(agent_id)
        if agent is None:
            continue
        for record in _inventory_records_for_agent(agent):
            record.state = "unhealthy"
            record.updated_at = current
            RELAY_INVENTORY.upsert(record)
            results.append(record.node_id)
            # 现场必须留下"谁被判死、什么时候"：否则只能靠代码猜。
            logging.warning(
                "[MIGRATION] 常驻中转机心跳超时 node=%s name=%s agent=%s"
                "（%.0fs 内仍不健康才自动重建，期间恢复即撤销）",
                record.node_id,
                record.name,
                agent.agent_id,
                relay_rebuild_seconds(),
            )

    for record in list(RELAY_INVENTORY.all()):
        if record.state != "unhealthy":
            continue
        due = max(
            float(record.updated_at or 0.0) + relay_rebuild_seconds(),
            float(getattr(record, "rebuild_backoff_until", 0.0) or 0.0),
        )
        if current < due:
            continue
        # 二次确认"此刻仍不健康"：标记之后心跳可能已经恢复（agent 自愈、
        # 网络恢复、controller 抖动）。这时撤销误判即可，绝不能删一台正在
        # 健康服务的机器——这正是"中转机经常自己删掉重建"的主因之一。
        if _live_agent_for(record, now=current) is not None:
            record.state = "busy" if int(record.slots_used or 0) else "ready"
            record.updated_at = current
            record.rebuild_backoff_until = 0.0
            RELAY_INVENTORY.upsert(record)
            logging.warning(
                "[MIGRATION] 常驻中转机心跳已恢复，撤销重建 node=%s name=%s",
                record.node_id,
                record.name,
            )
            continue
        # 正在服务（有槽位占用或活动租约）的机器绝不能删：删了会打断在跑的
        # 迁移，还会让后续挂载/派生报 404。等它空下来再重建。
        if _relay_node_busy(record):
            logging.warning(
                "[MIGRATION] 常驻中转机不健康但仍在服务，暂不重建 node=%s slots=%s",
                record.node_id,
                record.slots_used,
            )
            continue
        try:
            RELAY_RESOURCES.node_manager.rebuild(record.node_id)
            results.append(f"rebuild:{record.node_id}")
            logging.warning(
                "[MIGRATION] 已自动重建常驻中转机 node=%s name=%s",
                record.node_id,
                record.name,
            )
        except Exception:  # noqa: BLE001 - 重建失败按退避重试，不每分钟打云
            logging.exception(
                "[MIGRATION] 自动重建常驻中转机失败 node=%s（%.0fs 后重试）",
                record.node_id,
                relay_rebuild_seconds(),
            )
            failed = RELAY_INVENTORY.get(record.node_id)
            if failed is not None:
                failed.rebuild_backoff_until = current + relay_rebuild_seconds()
                RELAY_INVENTORY.upsert(failed)

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


def _inventory_records_for_agent(agent: Any) -> list[Any]:
    """把心跳超时的会话映射到库存记录。

    优先按 node_id 精确匹配（agent 会带 RELAY_NODE_ID）：按名字匹配时，
    agent 重新注册留下的僵尸会话会把同名那台在跑的中转机一起判死。
    老版本 agent 不上报 node_id，只能退回按名字匹配。
    """
    node_id = str(getattr(agent, "node_id", "") or "")
    if node_id:
        record = RELAY_INVENTORY.get(node_id)
        return [record] if record is not None else []
    name = str(getattr(agent, "name", "") or "")
    return [record for record in RELAY_INVENTORY.all() if record.name == name]


def _live_agent_for(record: Any, *, now: float) -> Any | None:
    """该清单记录对应的 agent 会话此刻是否还活着（心跳在超时窗口内）。

    只用于"救回来"：返回非 None 表示不重建。所以这里按名字兜底查询是安全的
    —— 误判只会推迟一次重建，不会删掉在服务的机器。
    """
    try:
        agent = RELAY_STATE.find_by_name(record.name)
    except Exception:  # noqa: BLE001 - 查不到就按没恢复处理
        logging.exception(
            "[MIGRATION] 查询 agent 会话失败 node=%s", getattr(record, "node_id", "")
        )
        return None
    if agent is None or getattr(agent, "state", "") == "unhealthy":
        return None
    timeout = float(getattr(RELAY_STATE, "heartbeat_timeout", 0.0) or 0.0)
    last = float(getattr(agent, "last_heartbeat", 0.0) or 0.0)
    return agent if now - last <= timeout else None


def _relay_node_busy(record: Any) -> bool:
    """节点是否正在服务：有槽位占用或活动租约就不能删。"""
    if int(getattr(record, "slots_used", 0) or 0) > 0:
        return True
    try:
        return bool(RELAY_LEASES.active_for_node(record.node_id))
    except Exception:  # noqa: BLE001 - 判断不了就当占用，宁可晚点重建
        logging.exception(
            "[MIGRATION] 读取节点租约失败，按占用处理 node=%s", record.node_id
        )
        return True


def relay_rebuild_seconds() -> float:
    return env_float("MIGRATION_RELAY_REBUILD_SECONDS", 600.0, minimum=1.0)


def relay_lease_grace_seconds() -> float:
    """残留租约的宽限窗口：让刚收尾的作业自己释放，避免和 finish() 抢。"""
    return env_float("MIGRATION_RELAY_LEASE_GRACE_SECONDS", 300.0, minimum=0.0)


def reap_relay_leases(*, now: float | None = None) -> list[str]:
    """回收"作业已经不在跑"的槽位租约，解开被残留槽位卡住的常驻节点。

    常驻节点的槽位计数完全由租约推导，而租约只有 ``RelayRuntime.finish()``
    会释放：平台重启会把未完成作业判失败、运行时就没了，残留的 active 记录
    会让节点永远 busy——既调度不出去，删也删不掉（DELETE 直接 409）。
    """
    current = time.time() if now is None else now
    grace = relay_lease_grace_seconds()
    stale: set[str] = set()
    for lease in RELAY_LEASES.all():
        if not lease.active:
            continue
        if current - float(lease.acquired_at or 0.0) < grace:
            continue
        job = job_manager.get(lease.job_id)
        if job is not None and job.status == JobStatus.RUNNING:
            continue
        stale.add(lease.job_id)

    results: list[str] = []
    for job_id in sorted(stale):
        scheduler = RELAY_RESOURCES.scheduler
        if scheduler is not None:
            # 复用调度器的释放逻辑：按租约台账回算 slots_used/state 并落盘。
            released = scheduler.release_job(job_id)
        else:
            released = len(RELAY_LEASES.release_job(job_id, now=current))
            if released:
                RELAY_LEASES.save()
        if released:
            logging.warning(
                "[MIGRATION] 回收残留中转机槽位租约 job=%s count=%s（作业已不在运行）",
                job_id,
                released,
            )
            results.append(f"lease-reap:{job_id}")
    return results


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


#: 提交参数快照的体积上限：VM 行可能上千条，超过就只记日志不落盘。
SUBMIT_PARAMS_MAX_BYTES = 4 * 1024 * 1024

#: 落盘时要剔除的字段名片段（小写匹配），避免把口令写进磁盘。
_SUBMIT_SECRET_MARKERS = ("password", "passwd", "secret", "token")


def _submit_params_dir() -> str:
    """提交参数快照目录（每个作业一个文件）。

    「调整参数重新提交」此前只认浏览器内存里的 lastSubmit：刷新页面、换浏览器
    或提交下一个作业后按钮就消失了。把快照落盘后，任何会话都能在作业详情页
    重新载入参数。口令类字段一律不落盘（见 _sanitize_submit_params）。
    """
    return os.path.join(
        app.config.get("UPLOAD_FOLDER") or UPLOAD_FOLDER, "job_params"
    )


def _submit_params_path(job_id: str) -> str:
    """快照文件路径；job_id 会被清洗成安全文件名。"""
    safe = "".join(ch for ch in str(job_id or "") if ch.isalnum() or ch in "-_")
    return os.path.join(_submit_params_dir(), f"{safe}.json")


def _sanitize_submit_params(value: Any) -> Any:
    """递归剔除口令类字段，其余原样保留。"""
    if isinstance(value, dict):
        return {
            key: _sanitize_submit_params(item)
            for key, item in value.items()
            if not any(marker in str(key).lower() for marker in _SUBMIT_SECRET_MARKERS)
        }
    if isinstance(value, list):
        return [_sanitize_submit_params(item) for item in value]
    return value


def _save_submit_params(job_id: str, raw: str) -> None:
    """保存提交参数快照；解析/写盘失败只记日志，不影响作业提交。"""
    text = str(raw or "").strip()
    if not text:
        return
    if len(text) > SUBMIT_PARAMS_MAX_BYTES:
        logging.warning(
            "[MIGRATION] 提交参数快照过大（%s 字节），跳过保存 job=%s",
            len(text),
            job_id,
        )
        return
    try:
        payload = json.loads(text)
    except ValueError as exc:
        logging.warning("[MIGRATION] 提交参数快照不是合法 JSON job=%s: %s", job_id, exc)
        return
    if not isinstance(payload, dict):
        logging.warning("[MIGRATION] 提交参数快照不是对象，跳过保存 job=%s", job_id)
        return
    path = _submit_params_path(job_id)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        atomic_write_json(path, _sanitize_submit_params(payload))
    except Exception:  # noqa: BLE001 - 快照落盘失败不能拦住迁移
        logging.exception("[MIGRATION] 保存提交参数快照失败 job=%s", job_id)


def _load_submit_params(job_id: str) -> dict[str, Any] | None:
    path = _submit_params_path(job_id)
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        logging.warning("[MIGRATION] 读取提交参数快照失败 job=%s: %s", job_id, exc)
        return None
    return payload if isinstance(payload, dict) else None


def start_job_relay_runtime(
    relay_config,
    *,
    source_auth: dict[str, str],
    target_auth: dict[str, str],
    job_id: str,
    options: dict[str, Any],
    should_stop=None,
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
        # 卷/快照等待默认不限时，取消作业时必须能把等待中的轮询一并打断。
        should_stop=should_stop,
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
        "phase_since": getattr(vm, "phase_since", None),
        "error": vm.error,
        "source_server_id": vm.source_server_id,
        "target_server_id": vm.target_server_id,
        "created_resources": vm.created_resources,
        "volumes": [_serialize_volume(volume) for volume in vm.volumes],
        # 中转机通道的逐盘结果（运行结束后运行时会被回收，只能靠这里留存）。
        "relay_disks": getattr(vm, "relay_disks", []) or [],
        "started_at": vm.started_at,
        "finished_at": vm.finished_at,
        "duration_seconds": vm.duration_seconds,
        "source_ips": vm.source_ips,
        "target_ips": vm.target_ips,
        "target_network_ports": vm.target_network_ports,
        "cutover_requested": vm.cutover_requested,
        "start_target": vm.start_target,
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


def _serialize_job_summary(job) -> dict[str, Any]:
    """作业列表用的摘要。

    列表页只用到状态与各状态台数，``_serialize_job`` 会把每个作业的
    VM 与卷明细全部展开，历史作业一多响应体就线性膨胀。详情接口
    （``/api/jobs/<id>``）仍返回完整数据。
    """
    counts: dict[str, int] = {}
    for vm in job.vms:
        key = vm.status.value
        counts[key] = counts.get(key, 0) + 1
    return {
        "id": job.id,
        "status": job.status.value,
        "error": job.error,
        "cancelled": bool(getattr(job, "cancelled", False)),
        "created_at": job.created_at,
        "vm_count": len(job.vms),
        "vm_status_counts": counts,
    }


#: 诊断文本里的阶段名，与 templates/index.html 的 phaseLabel 保持一致；只用于
#: 拼人读的提示，接口同时回传原始 phase，前端仍按自己的映射渲染。
_PHASE_TEXT = {
    "queued": "等待",
    "preparing_source": "准备源环境",
    "collecting_volumes": "收集卷信息",
    "stopping_target": "停止目标机",
    "stopping_source": "停止源机",
    "copying_volumes": "拷贝卷",
    "starting_target": "启动目标机",
    "verifying": "校验中",
    "precopying": "增量预拷贝",
    "syncing": "增量同步",
    "awaiting_cutover": "待切换",
    "awaiting_disk_retry": "等待重试失败盘",
    "relay_stopping_source": "中转机·停源",
    "relay_copying": "中转机·拷贝",
    "relay_creating_target_vm": "中转机·建目标机",
    "success": "成功",
    "failed": "失败",
    "cancelled": "已取消",
}


#: 已经落终态的 VM 状态：诊断时只有非终态才值得提示"停留了多久"。
_VM_TERMINAL_STATUSES = frozenset(
    {"success", "failed", "cancelled", "preflight_failed"}
)


def _phase_text(phase: str) -> str:
    return _PHASE_TEXT.get(phase or "", phase or "")


def _iso_age_seconds(iso: str | None) -> float | None:
    """ISO 时间戳距离现在的秒数；解析不了返回 None。"""
    if not iso:
        return None
    try:
        parsed = datetime.fromisoformat(str(iso))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - parsed).total_seconds(), 1)


def _humanize_seconds(seconds: float) -> str:
    total = int(max(0.0, float(seconds or 0.0)))
    if total >= 3600:
        return f"{total // 3600}h{(total % 3600) // 60}m"
    if total >= 60:
        return f"{total // 60}m{total % 60}s"
    return f"{total}s"


def _vm_disk_progress(vm) -> dict[str, Any]:
    """逐盘完成/失败计数：中转机看 relay_disks，RBD 直连看 volumes。"""
    relay_disks = list(getattr(vm, "relay_disks", None) or [])
    if relay_disks:
        done = sum(1 for disk in relay_disks if disk.get("status") == "success")
        failed = sum(1 for disk in relay_disks if disk.get("status") == "failed")
        in_flight = [
            disk
            for disk in relay_disks
            if disk.get("status") not in ("success", "failed")
        ]
        current = in_flight[0] if in_flight else None
        return {
            "channel": "relay",
            "total": len(relay_disks),
            "done": done,
            "failed": failed,
            "in_flight": len(in_flight),
            "current": (
                {
                    "volume_id": current.get("volume_id"),
                    "role": current.get("role"),
                    "status": current.get("status"),
                }
                if current
                else None
            ),
        }
    volumes = list(getattr(vm, "volumes", None) or [])
    if not volumes:
        return {
            "channel": "",
            "total": 0,
            "done": 0,
            "failed": 0,
            "in_flight": 0,
            "current": None,
        }
    statuses = [volume.status.value for volume in volumes]
    in_flight = [
        volume
        for volume in volumes
        if volume.status.value in ("pending", "copying", "ready")
    ]
    current = next(
        (volume for volume in volumes if volume.status.value in ("copying", "ready")),
        None,
    )
    return {
        "channel": "rbd",
        "total": len(volumes),
        "done": statuses.count("success"),
        "failed": statuses.count("failed"),
        "in_flight": len(in_flight),
        "current": (
            {
                "volume_id": current.source_volume_id,
                "rbd": current.source_rbd_name,
                "status": current.status.value,
                "progress_percent": round(float(current.progress_percent or 0.0), 1),
                "progress_label": current.progress_label,
            }
            if current
            else None
        ),
    }


def _relay_diagnose(job_id: str) -> dict[str, Any] | None:
    """中转机通道的卡点视图：节点状态 + 在途卷台账（含各阶段停留时长）。"""
    runtime = get_runtime(job_id)
    if runtime is None:
        return None
    now = time.time()
    nodes: list[dict[str, Any]] = []
    for role, pool in (("source", runtime.source_pool), ("target", runtime.target_pool)):
        for node in pool.nodes:
            nodes.append(
                {
                    "role": role,
                    "node_id": node.node_id,
                    "state": node.state,
                    "server_id": node.server_id,
                    "current_task_id": node.current_task_id,
                }
            )
    volumes: list[dict[str, Any]] = []
    for record in runtime.ledger.all():
        if record.job_id != job_id:
            continue
        volumes.append(
            {
                "volume_id": record.volume_id,
                "vm_id": record.vm_id,
                "phase": record.phase,
                "phase_label": relay_phase_label(record.phase),
                "waited_seconds": round(
                    now - float(record.updated_at or record.created_at or now), 1
                ),
                "snapshot_id": record.snapshot_id,
                "derived_volume_id": record.derived_volume_id,
                "target_volume_id": record.target_volume_id,
                "copied_bytes": int(record.copied_bytes or 0),
                "total_bytes": int(record.total_bytes or 0),
                "retry_count": int(record.retry_count or 0),
            }
        )
    volumes.sort(
        key=lambda item: (item["phase"] in ("done", "cleaned"), -item["waited_seconds"])
    )
    return {"nodes": nodes, "volumes": volumes, "ledger": runtime.ledger_summary()}


def _job_log_tail(
    job, limit: int = 80, recent: int = 20
) -> tuple[list[str], list[str]]:
    """日志尾部：``(与本作业相关的行, 服务最近若干行)``。

    并发排队这类日志（"等待 RBD 拷贝名额"）不写 job id，只有服务最近行
    里才看得到，所以两种都回传，省掉上机器 grep。
    """
    if not LOG_FILE or not os.path.isfile(LOG_FILE):
        return [], []
    try:
        size = os.path.getsize(LOG_FILE)
        with open(LOG_FILE, "rb") as handle:
            handle.seek(max(0, size - 2 * 1024 * 1024))
            text = handle.read().decode("utf-8", errors="replace")
    except OSError:
        return [], []
    lines = text.splitlines()
    keys = [job.id] + [vm.name for vm in job.vms if vm.name]
    matched = [line for line in lines if any(key in line for key in keys)]
    return matched[-limit:], lines[-recent:]


def _diagnose_hints(
    job,
    vms: list[dict[str, Any]],
    gate: dict[str, Any],
    relay: dict[str, Any] | None,
    log_lines: list[str],
    worker_alive: bool = True,
) -> list[str]:
    """把"卡在哪"翻译成几条可执行结论，省得对着原始数据猜。"""
    hints: list[str] = []
    if job.status == JobStatus.RUNNING and not worker_alive:
        hints.append(
            "作业状态是「进行中」，但没有执行线程：提交时参数校验失败、作业却已注册"
            "（历史遗留的僵尸作业）。它不会自己推进，请取消该作业后重新提交。"
        )
    for line in reversed(log_lines):
        if "等待 RBD 拷贝名额" in line:
            hints.append(
                "日志显示任务在排队等 RBD 拷贝名额（不是卡死）：" + line.strip()
            )
            break
    if job.status == JobStatus.RUNNING and gate["active"] >= gate["concurrency"]:
        holders = (
            "、".join(
                f"{holder['owner']}({holder['seconds']:.0f}s)"
                for holder in gate["holders"]
            )
            or "无"
        )
        hints.append(
            f"RBD 拷贝名额已满 {gate['active']}/{gate['concurrency']}，"
            f"当前占用：{holders}；后面的卷要等前面的卷释放名额。"
        )
    for vm in vms:
        if vm["status"] == "awaiting_disk_retry":
            failed = int((vm.get("disks") or {}).get("failed") or 0)
            hints.append(
                f"VM {vm['name']} 有 {failed} 块盘失败、其余盘已完成，正在等待人工"
                "在作业详情点「重试失败盘」（源机在等待期间保持关机）。"
            )
            continue
        if vm["status"] in _VM_TERMINAL_STATUSES or not vm["phase_seconds"]:
            continue
        if vm["phase_seconds"] >= 600:
            hints.append(
                f"VM {vm['name']} 停留在「{_phase_text(vm['phase'])}」已 "
                f"{_humanize_seconds(vm['phase_seconds'])}。"
            )
    if relay:
        for volume in relay["volumes"][:5]:
            if volume["phase"] in ("done", "cleaned", "failed", "failed_retained"):
                continue
            if volume["waited_seconds"] >= 600:
                hints.append(
                    f"中转卷 {volume['volume_id']}（VM {volume['vm_id']}）处于"
                    f"「{volume['phase_label']}」已 "
                    f"{_humanize_seconds(volume['waited_seconds'])}；"
                    "打快照/派生大盘没有字节流动，慢是常见现象，不是死锁。"
                )
        for node in relay["nodes"]:
            if node["state"] in ("unhealthy", "error"):
                hints.append(
                    f"中转机 {node['node_id']} 状态异常（{node['state']}），"
                    "可在「中转机通道」页重建。"
                )
    if job.status != JobStatus.RUNNING:
        hints.append(f"任务已不是运行中状态（{job.status.value}），无需继续等待。")
    if not hints:
        hints.append("没有发现明显的排队或异常等待，请结合下方日志尾部与在途卷判断。")
    return hints


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
        if "start_target" in override:
            row.start_target = parse_start_target(override["start_target"])
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
    """解析 Excel 成预览行；坏行只报行号不整份失败。

    批量清单里一行写错就整份提交不了，用户得反复试；这里改成"能用的行照常
    返回 + errors 列出问题行"，页面上按行号改完再提交。
    """
    try:
        excel_file = request.files.get("excel_file")
        if not excel_file:
            raise ValueError("缺少 excel_file")
        frame = pd.read_excel(excel_file)
        rows, errors = parse_rows_tolerant(frame.to_dict(orient="records"))
        return jsonify(
            {
                "ok": True,
                "errors": errors,
                "rows": [
                    {
                        "vm_name": row.vm_name,
                        "target_az": row.target_az,
                        "target_image": row.target_image,
                        "target_flavor": row.target_flavor,
                        "mode": row.mode,
                        "start_target": row.start_target,
                        "channel": row.channel,
                        "target_network": row.target_network,
                        "target_volume_type": row.target_volume_type,
                        "rate_limit_mb_s": row.rate_limit_mb_s,
                        "row_number": row.row_number,
                    }
                    for row in rows
                ],
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] Excel 预览失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


#: Excel 模板列与示例行：列名必须与 excel_parser 的字段名一致。
EXCEL_TEMPLATE_COLUMNS = [
    "vm_name",
    "target_az",
    "target_image",
    "target_flavor",
    "mode",
    "start_target",
    "channel",
    "target_network",
    "target_volume_type",
    "rate_limit_mb_s",
]
EXCEL_TEMPLATE_EXAMPLE = {
    "vm_name": "web-01",
    "target_az": "az1",
    "target_image": "（留空按源镜像匹配）",
    "target_flavor": "（留空自动匹配）",
    "mode": "full",
    "start_target": "是",
    "channel": "relay",
    "target_network": "（留空按源网段自动映射）",
    "target_volume_type": "（留空用作业默认）",
    "rate_limit_mb_s": 0,
}


@app.get("/api/excel-template")
def api_excel_template():
    """下载批量迁移清单模板（含一行示例），避免用户猜列名。"""
    try:
        frame = pd.DataFrame([EXCEL_TEMPLATE_EXAMPLE], columns=EXCEL_TEMPLATE_COLUMNS)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            frame.to_excel(writer, index=False, sheet_name="migration")
        buffer.seek(0)
        return send_file(
            buffer,
            mimetype=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
            as_attachment=True,
            download_name="migration-list-template.xlsx",
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 生成 Excel 模板失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/migrate")
def api_migrate():
    # 校验失败时要知道作业/目录是否已经建出来，才能正确清理，别留僵尸作业。
    job = None
    job_dir: str | None = None
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
        job_dir = _job_dir
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

        # 先把 options 全部解析、校验完再注册作业。否则像 parse_relay_options
        # 这类校验失败时，作业已经注册却没有执行线程，会永远停在 running。
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
            # 「快照 / 快照派生卷」等待上限（秒）：0 = 不限时（默认）。
            # 商业存储上这一步常是存储侧全量拷贝，200GiB 实测能超过 1 小时，
            # 1TiB 按 20s/GiB 要 5 小时以上；按时间判死会让平台在云上还在建盘
            # 时判失败并回收资源。填正数时上限放宽到 7 天，且会按卷大小自适应。
            "volume_ready_timeout": _non_negative_float(
                request.form.get("volume_ready_timeout"),
                maximum=7 * 24 * 3600.0,
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
            # 作业还没注册，只留下 _create_job_and_files 建的空目录，一并清掉。
            shutil.rmtree(job_dir, ignore_errors=True)
            return jsonify({"ok": False, "error": str(exc)}), 400

        job = job_manager.create_job(rows, job_id=job_id)
        # 表单自带的提交参数快照：作业详情页后续可用它「调整参数重新提交」。
        _save_submit_params(job_id, request.form.get("submit_snapshot"))

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
                        should_stop=lambda: (
                            shutdown_coordinator.stopping
                            or job_manager.is_cancelled(job.id)
                        ),
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
                        disk_retry_take=lambda: job_manager.take_disk_retry_request(
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
                # 运行时可回收后，盘上的保留信息已经没有对应的中间卷了：
                # 清掉标记，避免页面继续显示"中间卷保留中"。
                for vm in getattr(job, "vms", []) or []:
                    for disk in getattr(vm, "relay_disks", []) or []:
                        if disk.get("retained_until"):
                            disk["retained_until"] = 0.0
                            disk["retain_reason"] = ""
                job_manager.save_now()
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
        if job is not None:
            # 作业已注册但执行线程没起来：标记失败，别让它永远停在 running。
            job.status = JobStatus.FAILED
            job.error = f"提交失败：{exc}"
            job_manager.save_now()
        elif job_dir:
            shutil.rmtree(job_dir, ignore_errors=True)
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/api/jobs/<job_id>")
def api_job(job_id: str):
    job = job_manager.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    return jsonify({"ok": True, "job": _serialize_job(job)})


@app.get("/api/jobs")
def api_jobs():
    """作业列表默认返回摘要；``?full=1`` 保留旧的完整展开行为。"""
    jobs = job_manager.list_jobs()
    if (request.args.get("full") or "").strip() in {"1", "true", "yes"}:
        return jsonify({"ok": True, "jobs": [_serialize_job(job) for job in jobs]})
    return jsonify({"ok": True, "jobs": [_serialize_job_summary(job) for job in jobs]})


def _release_job_retained(job_id: str, *, reason: str) -> list[str]:
    """立即回收某作业失败保留的中间卷；没有活跃运行时就交给后续对账。

    只动 phase=failed_retained 的记录（已卸载、不在传输中），因此取消/删除
    时提前回收不会和正在跑的拷贝抢卷。
    """
    runtime = get_runtime(job_id)
    if runtime is None:
        logging.warning(
            "[MIGRATION] 作业 %s 没有活跃运行时（%s），保留的中间卷留给对账回收",
            job_id,
            reason,
        )
        return []
    try:
        return runtime.release_retained(reason=reason)
    except Exception:  # noqa: BLE001 - 回收失败不能影响取消/删除本身
        logging.exception("[MIGRATION] 立即回收中间卷失败 job=%s", job_id)
        return []


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
    released = _release_job_retained(job_id, reason="cancel")
    return jsonify(
        {
            "ok": True,
            "cancelled": cancelled,
            "relay_agents": notified,
            "retained_released": released,
        }
    )


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


@app.post("/api/jobs/<job_id>/vms/<vm_name>/disks/retry")
def api_vm_disk_retry(job_id: str, vm_name: str):
    """中转机通道：重试该 VM 的失败盘（其余盘已经在正常迁移）。

    请求体可选 ``{"volume_ids": [...]}``：只重试指定的失败盘，省略/空表示
    重试全部失败盘。重试会尽量沿用已建好的目标卷（按已拷字节续传），
    派生卷/快照若还在保留期内（phase=failed_retained）则校验后直接复用，
    只有校验不过（已被删/类型不符/保留期已过）才重新打快照派生。
    """
    job = job_manager.get(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    vm = next((item for item in job.vms if item.name == vm_name), None)
    if vm is None:
        return jsonify({"ok": False, "error": "VM 不存在"}), 404
    payload = request.get_json(silent=True) or {}
    raw_ids = payload.get("volume_ids") or []
    if not isinstance(raw_ids, list):
        return jsonify({"ok": False, "error": "volume_ids 必须是数组"}), 400
    failed_ids = {
        str(disk.get("volume_id") or "")
        for disk in vm.relay_disks or []
        if disk.get("status") == "failed"
    }
    wanted = [str(item) for item in raw_ids if str(item)]
    unknown = [item for item in wanted if item not in failed_ids]
    if unknown:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "这些盘不在失败列表中：" + "、".join(unknown),
                }
            ),
            400,
        )
    if not job_manager.request_disk_retry(job_id, vm_name, wanted):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "该 VM 当前不在等待失败盘重试（可能正在重试或已结束）",
                }
            ),
            409,
        )
    return jsonify({"ok": True})


@app.post("/api/jobs/<job_id>/relay/disks/retry-all")
def api_relay_disk_retry_all(job_id: str):
    """一键重试本作业所有"等待失败盘重试"的 VM。

    大批量作业里几十台 VM 各自散在「中转机通道」页签，逐台点重试太慢；
    这里按 VM 维度把它们的全部失败盘排队，返回实际受理/跳过的清单。
    只影响已经在等待重试的 VM，不会去动正在拷贝或已结束的盘。
    """
    job = job_manager.get(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    requested: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for vm in job.vms:
        failed = [
            str(disk.get("volume_id") or "")
            for disk in vm.relay_disks or []
            if disk.get("status") == "failed" and str(disk.get("volume_id") or "")
        ]
        if not failed:
            continue
        if job_manager.request_disk_retry(job_id, vm.name, failed):
            requested.append({"vm": vm.name, "volume_ids": failed})
        else:
            skipped.append(
                {
                    "vm": vm.name,
                    "reason": "当前不在等待失败盘重试（可能正在重试或已结束）",
                }
            )
    if not requested and not skipped:
        return jsonify({"ok": True, "requested": [], "skipped": [], "message": "没有失败盘需要重试"})
    return jsonify({"ok": True, "requested": requested, "skipped": skipped})


@app.post("/api/jobs/<job_id>/vms/<vm_name>/disks/release")
def api_vm_disk_release(job_id: str, vm_name: str):
    """中转机通道：立即释放失败盘保留的中间卷（派生卷 + 快照）。

    请求体可选 ``{"volume_ids": [...]}``：只释放指定盘，省略/空表示释放该
    VM 全部保留卷。释放只删源端中间产物、目标卷里已写入的字节保留；之后再
    重试会重新打快照派生（大盘会慢几小时），因此这是"马上归还存储配额"与
    "保留快速重试能力"之间的取舍。
    """
    job = job_manager.get(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    vm = next((item for item in job.vms if item.name == vm_name), None)
    if vm is None:
        return jsonify({"ok": False, "error": "VM 不存在"}), 404
    payload = request.get_json(silent=True) or {}
    raw_ids = payload.get("volume_ids") or []
    if not isinstance(raw_ids, list):
        return jsonify({"ok": False, "error": "volume_ids 必须是数组"}), 400
    retained_ids = {
        str(disk.get("volume_id") or "")
        for disk in vm.relay_disks or []
        if float(disk.get("retained_until") or 0) > 0
    }
    wanted = [str(item) for item in raw_ids if str(item)]
    unknown = [item for item in wanted if item not in retained_ids]
    if unknown:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "这些盘没有可释放的中间卷：" + "、".join(unknown),
                }
            ),
            400,
        )
    runtime = get_runtime(job_id)
    if runtime is None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "任务已结束，中间卷已随任务回收，无需释放",
                }
            ),
            409,
        )
    try:
        released = runtime.release_retained(wanted, reason="manual")
    except Exception as exc:  # noqa: BLE001 - 释放失败要给出可读原因
        logging.exception("[MIGRATION] 释放中间卷失败 job=%s", job_id)
        return jsonify({"ok": False, "error": f"释放失败：{exc}"}), 500
    released_set = {str(item) for item in released}
    for disk in vm.relay_disks or []:
        if str(disk.get("volume_id") or "") in released_set:
            disk["retained_until"] = 0.0
            disk["retain_reason"] = ""
            disk["released"] = True
    if released:
        job_manager.save_now()
    targets = wanted or sorted(retained_ids)
    pending = [item for item in targets if item not in released_set]
    return jsonify(
        {"ok": True, "released": sorted(released_set), "pending": pending}
    )


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
    # 运行中的作业删不掉（job_manager 会返回 409），这里回收的是作业已结束
    # 但运行时还没来得及收尾的保留卷，避免删除后配额一直被占。
    if job_manager.get(job_id) is not None:
        _release_job_retained(job_id, reason="delete")
    error = job_manager.delete(job_id)
    if error:
        status = 404 if error == "任务不存在" else 409
        return jsonify({"ok": False, "error": error}), status
    drop_runtime(job_id)
    RELAY_STATE.forget_job(job_id)
    # 参数快照跟着作业一起删，避免作业目录删了快照还留在磁盘上。
    try:
        os.unlink(_submit_params_path(job_id))
    except FileNotFoundError:
        pass
    except OSError:
        logging.exception("[MIGRATION] 删除提交参数快照失败 job=%s", job_id)
    return jsonify({"ok": True})


@app.get("/api/jobs/<job_id>/params")
def api_job_params(job_id: str):
    """作业提交时的向导参数快照（口令字段已剔除），用于「调整参数重新提交」。"""
    if job_manager.get(job_id) is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    params = _load_submit_params(job_id)
    if params is None:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "服务器没有保存这个作业的提交参数，请重新在向导里填写",
                }
            ),
            404,
        )
    return jsonify({"ok": True, "params": params})


@app.get("/api/jobs/<job_id>/diagnose")
def api_job_diagnose(job_id: str):
    """只读卡点诊断：一次拿到"停在哪个阶段、多久、在等谁"。

    不返回任何凭据；日志只回传与该作业/VM 相关的尾部行。
    """
    job = job_manager.get(job_id)
    if job is None:
        return jsonify({"ok": False, "error": "job 不存在"}), 404
    vms: list[dict[str, Any]] = []
    for vm in job.vms:
        phase_seconds = _iso_age_seconds(getattr(vm, "phase_since", None))
        if phase_seconds is None:
            # 老作业没有 phase_since，退回 started_at，至少能看出开始多久了。
            phase_seconds = _iso_age_seconds(vm.started_at)
        vms.append(
            {
                "name": vm.name,
                "status": vm.status.value,
                "phase": vm.phase,
                "phase_since": getattr(vm, "phase_since", None),
                "phase_seconds": phase_seconds,
                "error": vm.error,
                "target_server_id": vm.target_server_id,
                "disks": _vm_disk_progress(vm),
            }
        )
    holders = [
        {"owner": owner, "seconds": round(seconds)}
        for owner, seconds in COPY_GATE.holders()
    ]
    gate = {
        "concurrency": COPY_GATE.max_active_copies,
        "active": COPY_GATE.active_copies,
        "holders": holders,
        "memory_high_water": COPY_GATE.high_water,
        "reserve_mb": int(COPY_GATE.reserve_bytes / MiB),
        "rbd_cmd_timeout_seconds": rbd_cmd_timeout_seconds(),
        "copy_stall_timeout_seconds": copy_stall_timeout_seconds(),
    }
    relay = _relay_diagnose(job_id)
    log_lines, log_recent = _job_log_tail(job)
    worker_alive = job_manager.is_worker_alive(job_id)
    return jsonify(
        {
            "ok": True,
            "diag_version": DIAG_VERSION,
            "worker_alive": worker_alive,
            "job": {
                "id": job.id,
                "status": job.status.value,
                "error": job.error,
                "created_at": job.created_at,
                "elapsed_seconds": _iso_age_seconds(job.created_at),
                "vm_count": len(job.vms),
            },
            "vms": vms,
            "copy_gate": gate,
            "relay": relay,
            "hints": _diagnose_hints(
                job, vms, gate, relay, log_lines + log_recent, worker_alive
            ),
            "log_file": LOG_FILE,
            "log_lines": log_lines,
            "log_recent": log_recent,
        }
    )


@app.get("/api/jobs/<job_id>/relay")
def api_job_relay(job_id: str):
    runtime = get_runtime(job_id)
    if runtime is None:
        return jsonify({"ok": True, "relay": None})
    return jsonify({"ok": True, "relay": runtime.snapshot()})


@app.post("/api/jobs/<job_id>/relay/reconcile")
def api_job_relay_reconcile(job_id: str):
    """手工对账：只回收"没人认领"的残留，运行中的卷由任务自己清理。

    运行中的作业绝不能对账：该作业的派生卷/目标卷正在被拷贝线程使用，
    回收会把卷删掉，轮到挂载时就变成 404 Volume ... could not be found。
    """
    runtime = get_runtime(job_id)
    if runtime is None:
        return jsonify({"ok": False, "error": "该任务没有中转机运行时"}), 404
    job = job_manager.get(job_id)
    if job is not None and job.status == JobStatus.RUNNING:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "任务仍在运行：运行中的卷由任务自己回收，"
                    "此时对账会删掉正在拷贝的卷。请等任务结束后再对账。",
                }
            ),
            409,
        )
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


def _existing_relay_pools(auth: dict[str, Any], role: str) -> list[dict[str, Any]]:
    """该凭据下已有的常驻池参数，供作业表单直接沿用。

    常驻池一旦建好，建机参数就已经存在池参数里（``PoolProfile``），作业表单
    再填一遍既多余又容易跟池里不一致。这里按 ``source/target`` 侧的云指纹
    查出已有池，前端据此隐藏建机参数、只留"选哪个 AZ 的池"。
    """
    layer = RELAY_RESOURCES
    try:
        if not layer.ensure():
            return []
    except Exception:  # noqa: BLE001 - 查不到已有池不影响目录加载
        logging.exception("[MIGRATION] 读取常驻中转机池失败")
        return []
    tenant_key = _cloud_fingerprint(auth)
    pools: list[dict[str, Any]] = []
    try:
        profiles = [item for item in layer.profiles.for_tenant(tenant_key) if item.role == role]
        for profile in profiles:
            nodes = layer.inventory.nodes_in_pool(tenant_key, role, profile.az)
            pools.append(
                {
                    "az": profile.az,
                    "image": profile.image,
                    "flavor": profile.flavor,
                    "network": profile.network,
                    "subnet": profile.subnet,
                    "system_volume_type": profile.system_volume_type,
                    "data_floating_network": profile.data_floating_network,
                    "slots_per_node": profile.slots_per_node,
                    "max_nodes": profile.max_nodes,
                    "min_nodes": profile.min_nodes,
                    "data_port": profile.data_port,
                    "platform_url": profile.platform_url,
                    "ssh_public_key": profile.ssh_public_key,
                    "nodes": len(nodes),
                    "slots_total": sum(
                        int(getattr(node, "slots_total", 0) or 0) for node in nodes
                    ),
                    "slots_used": sum(
                        int(getattr(node, "slots_used", 0) or 0) for node in nodes
                    ),
                }
            )
    except Exception:  # noqa: BLE001 - 单个池读不出来不影响其它池
        logging.exception("[MIGRATION] 汇总常驻中转机池失败 tenant=%s", tenant_key)
        return []
    return sorted(pools, key=lambda item: item["az"])


@app.post("/api/relay/catalog")
def api_relay_catalog():
    """拉取两侧云的镜像/flavor/AZ/网络，供中转机池下拉使用。

    顺带回一份"已有常驻池"清单：常驻模式下已有池的建机参数直接沿用，
    作业表单不该再要求用户填一遍。
    """
    # 与 api_migrate 一样支持环境档案兜底：池的 tenant_key 是云指纹，
    # 凭据来源不同（表单/档案）会算出不同指纹，必须用同一套解析规则。
    profile_id = (request.form.get("profile_id") or "").strip()
    profile: EnvironmentProfile | None = None
    if profile_id:
        profile = _environment_profile_store().get(profile_id)
        if profile is None:
            return (
                jsonify({"ok": False, "error": f"环境档案不存在：{profile_id}"}),
                400,
            )
    source_auth = _auth_args("source", _profile_auth(profile, "source"))
    target_auth = _auth_args("target", _profile_auth(profile, "target"))
    try:
        source_os = OpenStackUtils(source_auth)
        target_os = OpenStackUtils(target_auth)
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 中转机目录鉴权失败")
        return jsonify({"ok": False, "error": str(exc)}), 400
    try:
        catalog = build_catalog(source_os, target_os)
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 中转机目录加载失败")
        return jsonify({"ok": False, "error": str(exc)}), 400
    pools = {
        "source": _existing_relay_pools(source_auth, "source"),
        "target": _existing_relay_pools(target_auth, "target"),
    }
    return jsonify({"ok": True, "catalog": catalog, "pools": pools})


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
    """List source-project VMs for the checklist picker.

    批量迁移要一次拿全项目清单（前端"加载更多"点几十次太慢）：``all=true``
    时内部按 marker 翻页，最多 ``SOURCE_VM_ALL_LIMIT`` 台，返回的
    ``next_marker`` 非空表示还有剩余，可以继续加载。
    """
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
        fetch_all = bool(payload.get("all"))
        bundle = source_os.list_source_servers_bundle(
            search=search,
            limit=limit,
            marker=marker,
            fetch_all=fetch_all,
            max_results=SOURCE_VM_ALL_LIMIT if fetch_all else 0,
        )
        servers = bundle["servers"]
        busy = busy_source_vm_names()
        for item in servers:
            item["busy_in_job"] = item.get("name") in busy
        return jsonify(
            {
                "ok": True,
                "servers": servers,
                "next_marker": bundle["next_marker"],
                "busy_names": sorted(busy),
            }
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 查询源 VM 列表失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/api/policies")
def api_policies_list():
    """迁移策略模板列表（参数快照 + 规则映射，不含任何凭据）。"""
    try:
        return jsonify(
            {"ok": True, "policies": _migration_policy_store().list_public()}
        )
    except Exception as exc:  # noqa: BLE001 - 读失败不该 500 暴露堆栈
        logging.exception("[MIGRATION] 迁移策略列表失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/policies")
def api_policies_save():
    payload = request.get_json(silent=True) or {}
    try:
        store = _migration_policy_store()
        policy = store.save(payload)
        store.flush()
        return jsonify({"ok": True, "policy": MigrationPolicyStore._public(policy)})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 迁移策略保存失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.delete("/api/policies/<policy_id>")
def api_policies_delete(policy_id: str):
    try:
        store = _migration_policy_store()
        if not store.delete(policy_id):
            return jsonify({"ok": False, "error": "策略不存在"}), 404
        store.flush()
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 迁移策略删除失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


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
        # 逐台查询会放大成 O(N × (端口 + 卷)) 次串行请求，清单一大就超时；
        # 交给 OpenStackUtils 做批量拉取 + 分组，请求次数与 VM 台数无关。
        result = source_os.collect_vm_network_briefs(
            [str(name) for name in vm_names],
            [str(item) for item in server_ids],
        )
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
                # 失败盘中间卷保留时长（小时），0 = 失败立即回收。
                "derived_retention_hours": round(
                    derived_retention_seconds() / 3600.0, 2
                ),
                "ceph_preflight": ceph_preflight_enabled(),
                "relay_heartbeat_interval": RELAY_STATE.heartbeat_interval,
                "relay_heartbeat_timeout": RELAY_STATE.heartbeat_timeout,
                "relay_rebuild_seconds": relay_rebuild_seconds(),
                "log_only_migration": _LOG_FILTER_ACTIVE,
                "log_only_migration_configured": log_only_migration(),
                "log_file": LOG_FILE,
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

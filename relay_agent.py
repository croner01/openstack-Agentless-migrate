"""中转机 agent：注册、心跳、领任务、执行块拷贝并上报。

agent 只做出向请求；取消通过心跳响应下发。agent 不执行任何 attach/detach，
卷的挂载与卸载一律由平台通过 OpenStack API 完成。
"""
from __future__ import annotations

import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from threading import Event, Thread
from typing import Any, Callable

from relay_protocol import DEFAULT_CHUNK
from relay_transfer import (
    TAIL_WINDOW,
    TransferError,
    TransferStats,
    hash_device,
    receive_device,
    send_device,
)

AGENT_VERSION = "1.1.0"


class AgentClient:
    """agent 面向平台的客户端；测试时可传入鸭子类型的假实现。"""

    def __init__(self, platform: Any, session_id: str):
        self.platform = platform
        self.session_id = session_id

    def heartbeat(self) -> dict[str, Any]:
        return self.platform.heartbeat(self.session_id)

    def next_task(self) -> dict[str, Any] | None:
        return self.platform.next_task(self.session_id)

    def report_progress(
        self, task_id: str, copied_bytes: int, skipped_bytes: int = 0
    ) -> dict[str, Any]:
        return (
            self.platform.report_progress(task_id, copied_bytes, skipped_bytes) or {}
        )

    def report_result(
        self,
        task_id: str,
        status: str,
        copied_bytes: int,
        *,
        digest: str = "",
        error: str = "",
        skipped_bytes: int = 0,
    ) -> None:
        self.platform.report_result(
            task_id,
            status,
            copied_bytes,
            digest=digest,
            error=error,
            skipped_bytes=skipped_bytes,
        )

    def report_ready(self, task_id: str) -> None:
        self.platform.report_ready(task_id)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


def local_public_key(path: str = "/root/.ssh/id_ed25519.pub") -> str:
    """读取本机公钥用于上报；不存在时返回空串（不影响注册）。"""
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def resolve_device(path: str, volume_id: str = "") -> str:
    """把平台给的设备路径解析成 guest 内真实存在的块设备。

    Nova 汇报的 device（如 /dev/vdb）不一定等于 guest 里的名字——virtio-scsi
    场景下常见 /dev/sdb。直接 stat 平台给的路径会立刻 FileNotFoundError，
    因此优先按卷 id 在 /dev/disk/by-* 下找对应的稳定符号链接。
    """
    if path and Path(path).exists():
        return path
    if volume_id:
        for root in ("/dev/disk/by-id", "/dev/disk/by-path", "/dev/disk/by-uuid"):
            base = Path(root)
            if not base.is_dir():
                continue
            try:
                entries = sorted(base.iterdir())
            except OSError:
                continue
            for entry in entries:
                if volume_id in entry.name:
                    try:
                        return str(entry.resolve())
                    except OSError:
                        continue
    available: list[str] = []
    for root in ("/dev/disk/by-path", "/dev"):
        base = Path(root)
        if base.is_dir():
            try:
                available.extend(sorted(item.name for item in base.iterdir())[:20])
            except OSError:
                pass
        if available:
            break
    raise TransferError(
        f"设备路径不可用: {path or '(空)'}；按卷 {volume_id or '-'} 也未找到块设备。"
        f"可见设备: {', '.join(available) or '无'}"
    )


def resolve_data_address(
    probe_host: str,
    *,
    probe_port: int = 80,
    sock_factory: Callable[..., Any] = socket.socket,
) -> str:
    """探测本机到平台方向使用的地址，供对端 agent 连接；失败返回空串。"""
    if not probe_host:
        return ""
    try:
        with sock_factory(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((probe_host, probe_port))
            return str(sock.getsockname()[0])
    except OSError:
        return ""


def execute_task(
    task: dict[str, Any],
    client: AgentClient,
    *,
    ready_event: Event | None = None,
) -> dict[str, Any]:
    """执行单个块拷贝任务，返回带 status 与 copied_bytes 的结果。"""
    role = task.get("role")
    if role not in {"source", "target"}:
        raise ValueError(f"unknown role: {role!r}")

    task_id = str(task.get("task_id", ""))
    chunk_size = int(task.get("chunk_size") or DEFAULT_CHUNK)
    length = int(task.get("length", 0))
    offset = int(task.get("offset", 0))
    ticket = str(task.get("ticket", ""))
    sparse = bool(task.get("sparse"))
    # 平台未启用 sparse 时把空洞模式收紧成 off：收到空洞帧就是协议错误，
    # 宁可失败也不要静默留下未写区域。
    hole_mode = str(task.get("hole_mode") or "skip") if sparse else "off"
    stats = TransferStats()
    report_state = {"at": 0.0}
    cancel_flag = {"requested": False}

    def progress(copied: int) -> None:
        now = time.monotonic()
        if copied < length and now - report_state["at"] < 1.0:
            return
        report_state["at"] = now
        # 进度响应顺带回传取消指令，长拷贝因此可以在中途被打断。
        reply = client.report_progress(task_id, copied, stats.skipped_bytes)
        if reply.get("cancel"):
            cancel_flag["requested"] = True

    try:
        if task.get("kind") == "verify":
            digest, size = hash_device(
                resolve_device(
                    str(task["path"]), str(task.get("volume_id") or "")
                ),
                int(task.get("window") or TAIL_WINDOW),
            )
            client.report_result(task_id, "done", size, digest=digest)
            return {"status": "done", "digest": digest, "size": size}

        device_path = resolve_device(
            str(task.get("src_path") or task.get("dst_path") or ""),
            str(task.get("volume_id") or ""),
        )
        if role == "target":
            port = int(task.get("listen_port") or 0) or _free_port()
            copied = receive_device(
                device_path,
                listen_host=str(task.get("listen_host") or "0.0.0.0"),
                listen_port=port,
                expected_ticket=ticket,
                offset=offset,
                length=length,
                chunk_size=chunk_size,
                progress_cb=progress,
                ready_event=ready_event,
                accept_timeout=float(task.get("accept_timeout") or 0) or None,
                on_listening=lambda: client.report_ready(task_id),
                hole_mode=hole_mode,
                stats=stats,
            )
        else:
            copied = send_device(
                device_path,
                peer_host=str(task["peer_host"]),
                peer_port=int(task["peer_port"]),
                ticket=ticket,
                offset=offset,
                length=length,
                chunk_size=chunk_size,
                rate_limit_bytes_per_sec=task.get("rate_limit_bytes_per_sec") or None,
                progress_cb=progress,
                is_cancelled=lambda: cancel_flag["requested"],
                skip_zero=sparse,
                stats=stats,
            )
    except (TransferError, OSError, ValueError) as exc:
        status = "cancelled" if cancel_flag["requested"] else "failed"
        # 把失败原因带回平台：否则平台只能看到 "failed"，无法定位。
        client.report_result(
            task_id,
            status,
            0,
            error=f"{type(exc).__name__}: {exc}",
            skipped_bytes=stats.skipped_bytes,
        )
        return {"status": status, "error": str(exc), "copied_bytes": 0}

    client.report_result(
        task_id, "done", copied, skipped_bytes=stats.skipped_bytes
    )
    return {
        "status": "done",
        "copied_bytes": copied,
        "skipped_bytes": stats.skipped_bytes,
    }


def run_once(client: AgentClient) -> dict[str, Any] | None:
    """心跳一次并尝试领取并执行一个任务。"""
    client.heartbeat()
    task = client.next_task()
    if task is None:
        return None
    return execute_task(task, client)


class AgentRunner:
    """注册 + N 个 worker + 心跳重注册。

    HTTP 细节通过注入的函数隔离，便于单测；未注入时走真实平台接口。
    worker 数等于节点槽位数：一台常驻中转机可同时服务多个迁移任务。
    """

    def __init__(
        self,
        *,
        platform_url: str,
        token: str,
        name: str,
        slots_total: int = 1,
        data_address: str = "",
        data_port: int = 9200,
        node_id: str = "",
        register_fn: Callable[[str, int], str] | None = None,
        heartbeat_fn: Callable[[str], dict[str, Any]] | None = None,
        worker_fn: Callable[[str, int], None] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.platform_url = platform_url.rstrip("/")
        self.token = token
        self.name = name
        self.slots_total = max(int(slots_total or 1), 1)
        self.data_address = data_address
        self.data_port = int(data_port or 9200)
        self.node_id = node_id
        self._register_fn = register_fn
        self._heartbeat_fn = heartbeat_fn
        self._worker_fn = worker_fn
        self._sleep = sleeper
        self.session_id = ""
        self.heartbeat_interval = 5.0

    def _register(self, name: str, slots_total: int) -> str:
        payload = {
            "token": self.token,
            "name": name,
            "node_id": self.node_id,
            "agent_version": AGENT_VERSION,
            "data_address": self.data_address,
            "data_port": self.data_port,
            "slots_total": slots_total,
            "ssh_public_key": local_public_key(),
        }
        response = _http_json(f"{self.platform_url}/api/relay/register", payload)
        interval = float(response.get("heartbeat_interval") or 5.0)
        self.heartbeat_interval = interval
        return str(response["session_id"])

    def _heartbeat(self, session_id: str) -> dict[str, Any]:
        return _http_json(
            f"{self.platform_url}/api/relay/heartbeat", {"session_id": session_id}
        )

    def _worker(self) -> None:
        session = self.session_id
        platform = _HttpPlatform(self.platform_url, session)
        client = AgentClient(platform, session)
        task = client.next_task()
        if task is None:
            return
        execute_task(task, client)

    def register(self) -> str:
        """注册并换取会话；网络错误时指数退避重试，上限 60s。"""
        delay = 1.0
        while True:
            try:
                self.session_id = (
                    self._register_fn(self.name, self.slots_total)
                    if self._register_fn is not None
                    else self._register(self.name, self.slots_total)
                )
                return self.session_id
            except OSError:
                self._sleep(delay)
                delay = min(delay * 2, 60.0)

    def run(self, *, max_heartbeats: int | None = None) -> None:
        self.register()
        stop = Event()
        threads = [
            Thread(target=self._run_worker, args=(slot, stop), daemon=True)
            for slot in range(self.slots_total)
        ]
        for thread in threads:
            thread.start()
        beats = 0
        while max_heartbeats is None or beats < max_heartbeats:
            try:
                if self._heartbeat_fn is not None:
                    self._heartbeat_fn(self.session_id)
                else:
                    self._heartbeat(self.session_id)
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 404):
                    # 会话失效（平台重启或令牌轮换）：重新注册。
                    self.register()
            except urllib.error.URLError as exc:
                # 平台/网络短暂不可用：退避后继续心跳，不能因为一次抖动退出进程。
                logging.warning(
                    "[MIGRATION] relay 心跳失败（网络不可达），稍后重试: %s",
                    getattr(exc, "reason", exc),
                )
            beats += 1
            self._sleep(self.heartbeat_interval)
        stop.set()
        for thread in threads:
            thread.join(timeout=1.0)

    def _run_worker(self, slot: int, stop: Event) -> None:
        while not stop.is_set():
            try:
                if self._worker_fn is not None:
                    self._worker_fn(self.session_id, slot)
                else:
                    self._worker()
            except urllib.error.HTTPError as exc:
                if exc.code in (401, 404):
                    continue
            except urllib.error.URLError as exc:
                # 网络不可达：避免每秒刷一条完整堆栈，退避后重试。
                logging.warning(
                    "[MIGRATION] relay worker 无法连接平台 slot=%s: %s",
                    slot,
                    getattr(exc, "reason", exc),
                )
                self._sleep(5.0)
                continue
            except Exception:  # noqa: BLE001 - 单个任务失败不能拖垮 worker
                logging.exception("[MIGRATION] relay worker 异常 slot=%s", slot)
            self._sleep(1.0)


def _http_json(url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
        body = response.read()
    return json.loads(body) if body else {}


class _HttpPlatform:
    """把平台 HTTP 接口适配成 AgentClient 期望的鸭子类型。"""

    def __init__(self, base_url: str, session_id: str):
        self.base_url = base_url.rstrip("/")
        self.session_id = session_id

    def heartbeat(self, session_id: str) -> dict[str, Any]:
        return _http_json(
            f"{self.base_url}/api/relay/heartbeat", {"session_id": session_id}
        )

    def next_task(self, session_id: str) -> dict[str, Any] | None:
        url = f"{self.base_url}/api/relay/tasks/next?session_id={session_id}"
        request = urllib.request.Request(url)
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            if response.status == 204:
                return None
            body = response.read()
        return json.loads(body) if body else None

    def report_progress(
        self, task_id: str, copied_bytes: int, skipped_bytes: int = 0
    ) -> None:
        return _http_json(
            f"{self.base_url}/api/relay/tasks/{task_id}/progress",
            {
                "session_id": self.session_id,
                "copied_bytes": copied_bytes,
                "skipped_bytes": skipped_bytes,
            },
        )

    def report_result(
        self,
        task_id: str,
        status: str,
        copied_bytes: int,
        *,
        digest: str = "",
        error: str = "",
        skipped_bytes: int = 0,
    ) -> None:
        _http_json(
            f"{self.base_url}/api/relay/tasks/{task_id}/result",
            {
                "session_id": self.session_id,
                "status": status,
                "copied_bytes": copied_bytes,
                "skipped_bytes": skipped_bytes,
                "digest": digest,
                "error": error,
            },
        )

    def report_ready(self, task_id: str) -> None:
        # 带上 session_id，平台据此校验任务归属；旧版平台忽略该字段也兼容。
        _http_json(
            f"{self.base_url}/api/relay/tasks/{task_id}/ready",
            {"session_id": self.session_id},
        )


def main(platform_url: str, token: str, name: str, *, interval: float = 5.0) -> None:
    """agent 常驻入口：注册后启动 N 个 worker 并发领任务，主线程只做心跳。"""
    runner = AgentRunner(
        platform_url=platform_url,
        token=token,
        name=name,
        slots_total=int(os.environ.get("RELAY_SLOTS") or 1),
        data_address=os.environ.get("RELAY_DATA_ADDR")
        or resolve_data_address(urllib.parse.urlsplit(platform_url).hostname or ""),
        data_port=int(os.environ.get("RELAY_DATA_PORT") or 9200),
        node_id=os.environ.get("RELAY_NODE_ID", ""),
        sleeper=time.sleep,
    )
    runner.heartbeat_interval = float(interval)
    runner.run()


if __name__ == "__main__":
    main(
        os.environ["RELAY_PLATFORM_URL"],
        os.environ["RELAY_TOKEN"],
        os.environ.get("RELAY_NAME", socket.gethostname()),
    )

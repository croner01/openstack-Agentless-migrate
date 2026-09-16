import json
import logging
import os
import re
import socket
import shlex
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from env_utils import env_float, env_int
from migration_planner import snapshot_name, snapshots_to_prune


IDENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
CHUNK_SIZE = 256 * 1024
PROGRESS_INTERVAL_SECONDS = 0.2

ProgressCallback = Callable[[int, Optional[int], float], None]


class ValidationError(ValueError):
    """Raised when a RBD pool/image identifier is not safe to use."""


LAYOUT_EQ_FIELDS = ("order", "stripe_unit", "stripe_count", "data_pool", "size")

#: 决定"目标映像能否承载源数据"的特性集合：缺任何一个都拒绝换入。
#: 这是 Nautilus 及以上版本共通的全集，目标端用 `--image-feature` 就能建出来。
LAYOUT_FEATURES = frozenset(
    {
        "layering",
        "striping",
        "exclusive-lock",
        "object-map",
        "fast-diff",
        "deep-flatten",
        "journaling",
        "data-pool",
    }
)

STAGE_SUFFIX = "-mig-stage"

#: `rbd info` 判定"映像不存在"的特征。退出码 2 = ENOENT，其余措辞兼容各版本。
_MISSING_IMAGE_MARKERS = (
    "no such file or directory",
    "no such image",
    "does not exist",
)


def describe_process_error(
    exc: subprocess.CalledProcessError, limit: int = 400
) -> str:
    """把子进程 stderr 带进错误信息。

    `CalledProcessError.__str__` 只有 "returned non-zero exit status N"，
    而 rbd 的真实原因（超时 110 / 占用 16 / 缺快照 39）全在 stderr 里，
    不带上就没法判断是环境抖动还是真的缺东西。
    """
    stderr = exc.stderr if isinstance(exc.stderr, str) else ""
    detail = " ".join(stderr.split())
    if len(detail) > limit:
        detail = detail[:limit] + "..."
    return f"rc={exc.returncode} stderr={detail or '<空>'}"


def is_missing_image_error(exc: subprocess.CalledProcessError) -> bool:
    """True 仅当 rbd 明确报告"映像不存在"。"""
    if exc.returncode == 2:
        return True
    stderr = exc.stderr if isinstance(exc.stderr, str) else ""
    lowered = stderr.lower()
    return any(marker in lowered for marker in _MISSING_IMAGE_MARKERS)


#: mon 端口探测的默认超时：只用来判断"这个集群还联不联得上"。
MON_PROBE_TIMEOUT_SECONDS = 2.0
_MON_LINE_RE = re.compile(r"^\s*mon[ _]hosts?\s*=\s*(.+)$", re.IGNORECASE)
_MON_TOKEN_RE = re.compile(r"(?:v[12]:)?(\[[^\]]+\]|[^,\s\[\]]+?)(?::(\d+))?(?:/\d+)?$")


def parse_mon_endpoints(source: str) -> list[tuple[str, int]]:
    """从 ceph.conf 文本（或路径）里解析 `mon_host` 的 (host, port) 列表。

    兼容 `host`、`host:port`、`a,b`、`[v2:h:3300/0,v1:h:6789/0]` 这几种
    写法和多行 `mon host`。解析不出任何东西时返回空列表，调用方应当照旧
    执行原逻辑，绝不能因为"解析不了"就跳过。
    """
    text = source
    if os.path.exists(source):
        try:
            with open(source, encoding="utf-8", errors="replace") as handle:
                text = handle.read()
        except OSError:
            return []
    endpoints: list[tuple[str, int]] = []
    for raw_line in text.splitlines():
        match = _MON_LINE_RE.match(raw_line)
        if not match:
            continue
        value = match.group(1).strip()
        if value.startswith("[") and value.endswith("]"):
            value = value[1:-1]
        for token in re.split(r"[,\s]+", value):
            if not token:
                continue
            token = re.sub(r"^v[12]:", "", token)
            token_match = _MON_TOKEN_RE.match(token)
            if not token_match:
                continue
            host = token_match.group(1).strip("[]")
            port = int(token_match.group(2) or 6789)
            if host and (host, port) not in endpoints:
                endpoints.append((host, port))
    return endpoints


def any_mon_reachable(
    endpoints: list[tuple[str, int]],
    timeout: float = MON_PROBE_TIMEOUT_SECONDS,
    budget_seconds: float | None = None,
) -> bool:
    """任一 mon 能建立 TCP 连接即认为集群可达。

    `budget_seconds` 限制整轮探测的总耗时：mon 列表很长时逐个等到超时会把
    提交请求拖成十几秒，超预算即按"不可达"处理。
    """
    deadline = (
        None
        if not budget_seconds or budget_seconds <= 0
        else time.monotonic() + budget_seconds
    )
    for host, port in endpoints:
        if deadline is not None and time.monotonic() >= deadline:
            return False
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            continue
    return False


#: 单条 rbd 控制命令（info/ls/snap/rm）的默认超时。mon 不可达时 ceph 客户端
#: 不报错而是无限重连（线上实测 20 分钟以上仍无输出），线程会一直握着拷贝
#: 名额，后面的任务全部排队。0 = 不限制。
RBD_CMD_TIMEOUT_SECONDS = 120.0

#: 拷贝管道允许的"零字节"时长：超过即认为这条管道已经卡死。
COPY_STALL_TIMEOUT_SECONDS = 300.0

#: 超时统一按 Ceph 的 ETIMEDOUT(110) 上报，和 rbd 自己的超时退出码对齐。
RBD_TIMEOUT_RC = 110


def rbd_cmd_timeout_seconds() -> float:
    """rbd 控制命令的默认超时，可用 MIGRATION_RBD_CMD_TIMEOUT_SECONDS 调整。"""
    return env_float(
        "MIGRATION_RBD_CMD_TIMEOUT_SECONDS",
        RBD_CMD_TIMEOUT_SECONDS,
        minimum=0.0,
    )


def copy_stall_timeout_seconds() -> float:
    """拷贝停滞判定时长，MIGRATION_COPY_STALL_SECONDS 调整，0 = 关闭看门狗。"""
    return env_float(
        "MIGRATION_COPY_STALL_SECONDS",
        COPY_STALL_TIMEOUT_SECONDS,
        minimum=0.0,
    )


def describe_timeout(command, seconds: float) -> str:
    """超时命令的可读描述，塞进 CalledProcessError.stderr 一起落日志。"""
    return (
        f"rbd 命令超过 {seconds:.0f}s 未返回（mon/OSD 不可达或集群不可用）: "
        + " ".join(shlex.quote(str(part)) for part in command)
    )


def _terminate_process(proc, *, force: bool = False) -> None:
    """尽力结束子进程；kill/terminate 缺失或报错都静默跳过。"""
    for name in (("kill", "terminate") if force else ("terminate", "kill")):
        action = getattr(proc, name, None)
        if action is None:
            continue
        try:
            action()
        except OSError:
            pass
        return


class _CopyWatchdog:
    """拷贝管道停滞看门狗：超过 limit 秒没有字节流动就杀掉两端子进程。

    没有它时，mon/OSD 不可达会让 `rbd export` 一直阻塞（客户端无限重连），
    线程握着拷贝名额不放，整个服务的迁移任务全部排队——线上就是这么被卡住的。
    """

    def __init__(self, limit: float, processes, probe_interval: float = 1.0):
        self.limit = limit
        self._processes = tuple(processes)
        self._probe_interval = max(0.05, min(probe_interval, limit / 4 or 1.0))
        self._last_progress = time.monotonic()
        self._stalled = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def stalled(self) -> bool:
        return self._stalled.is_set()

    def touch(self) -> None:
        """收到一个数据块就刷新存活时间。"""
        self._last_progress = time.monotonic()

    def start(self) -> "_CopyWatchdog":
        if self.limit and self.limit > 0:
            self._thread = threading.Thread(
                target=self._run, name="copy-stall-watchdog", daemon=True
            )
            self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.wait(self._probe_interval):
            if time.monotonic() - self._last_progress < self.limit:
                continue
            self._stalled.set()
            logging.error(
                "[MIGRATION] 拷贝停滞：%.1fs 内没有任何字节流动，终止 rbd "
                "export/import（通常是源/目标 Ceph 的 mon/OSD 不可达）",
                self.limit,
            )
            for proc in self._processes:
                _terminate_process(proc, force=True)
            return

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


class LayoutMismatchError(ValueError):
    """Raised when a target image cannot host the source image layout."""

    def __init__(self, mismatches: list[str]):
        self.mismatches = list(mismatches)
        super().__init__("RBD 布局不一致: " + "; ".join(self.mismatches))


class StageImageMissingError(ValueError):
    """目标暂存卷在基线全备之后消失，增量 diff 无从落盘。

    正常流程里暂存卷只在基线全备时创建、换入后删除，中途不该消失；真消失了
    说明它被外部清理（后台 GC 误回收、人工 `rbd rm`），此时唯一正确的补救是
    对该卷重做一次基线全备。继承 ValueError 是为了兼容调用方原有的捕获逻辑。
    """


class CopyStalledError(subprocess.CalledProcessError):
    """拷贝管道长时间没有任何字节流动（mon/OSD 不可达或对端卡死）。

    刻意继承 `CalledProcessError`：上游到处按 rbd 退出码判定失败原因
    （ENOENT=2 / EBUSY=16 / ETIMEDOUT=110），复用它才能让停滞走同一条失败
    路径——卷被标失败、拷贝名额在 `finally` 里释放，而不是线程永久挂住。
    """

    def __str__(self) -> str:
        # 上游用 str(exc) 直接落前端错误文案，默认的 "returned non-zero exit
        # status 110" 看不出根因，这里换成带 mon/OSD 提示的说明。
        return self.stderr or super().__str__()


def _normalize_features(raw: Any) -> list[str]:
    if not raw:
        return []
    parts = raw.split(",") if isinstance(raw, str) else list(raw)
    return sorted(str(part).strip() for part in parts if str(part).strip())


def parse_layout(info: dict[str, Any]) -> dict[str, Any]:
    """提取必须与源保持一致的布局字段。"""
    layout = {field: (info or {}).get(field) for field in LAYOUT_EQ_FIELDS}
    layout["features"] = _normalize_features((info or {}).get("features"))
    return layout


def layout_mismatches(source: dict[str, Any], target: dict[str, Any]) -> list[str]:
    """返回源与目标的布局差异清单；目标 features 只需覆盖源的布局特性。

    只有 LAYOUT_FEATURES 里的特性缺失才算不一致。源集群较新时会带上
    `operations` 这类"镜像操作能力"特性位（由 op_features 如 snap-trash
    触发），目标端 librbd 建像时会按自己的 op_features 归一化掉，天然复制
    不过来，也和数据布局无关——把它当失败会让一台已经在拷贝的 VM 白跑一遍
    全量才报错。
    """
    src = parse_layout(source)
    dst = parse_layout(target)
    diffs = [
        f"{field}: source={src[field]!r} target={dst[field]!r}"
        for field in LAYOUT_EQ_FIELDS
        if src[field] != dst[field]
    ]
    missing = [
        feature
        for feature in src["features"]
        if feature not in dst["features"] and feature in LAYOUT_FEATURES
    ]
    if missing:
        diffs.append(f"features 缺失: {', '.join(missing)}")
    ignored = [
        feature
        for feature in src["features"]
        if feature not in dst["features"] and feature not in LAYOUT_FEATURES
    ]
    if ignored:
        logging.info(
            "[MIGRATION] 目标映像缺少非布局特性 %s（只影响镜像操作能力，"
            "不参与数据布局），不阻断迁移",
            ", ".join(ignored),
        )
    return diffs


def assert_layout_matches(source: dict[str, Any], target: dict[str, Any]) -> None:
    mismatches = layout_mismatches(source, target)
    if mismatches:
        raise LayoutMismatchError(mismatches)


CINDER_REQUIRED_FEATURES = ("layering", "exclusive-lock")


def build_import_args(
    layout: dict[str, Any],
    extra_features: tuple[str, ...] = (),
) -> list[str]:
    """生成让目标映像复现源布局的 rbd import 参数。"""
    args: list[str] = []
    if layout.get("order") is not None:
        args += ["--order", str(layout["order"])]
    if layout.get("stripe_count"):
        args += ["--stripe-count", str(layout["stripe_count"])]
    if layout.get("stripe_unit"):
        args += ["--stripe-unit", str(layout["stripe_unit"])]
    if layout.get("data_pool"):
        args += ["--data-pool", str(layout["data_pool"])]
    features = sorted(
        set(layout.get("features") or ())
        | set(CINDER_REQUIRED_FEATURES)
        | set(extra_features)
    )
    if features:
        args += ["--image-feature", ",".join(features)]
    return args


def parse_rbd_info_json(output: str) -> dict[str, Any]:
    return json.loads(output)


def watchers_present(status: dict[str, Any]) -> bool:
    return bool(status.get("watchers"))


@dataclass
class IncrementalSession:
    """一次增量预拷贝的上下文，供停机后的最终切换复用。"""

    job_id: str
    source_rbd_name: str
    target_rbd_name: str
    stage_name: str
    layout: dict[str, Any]
    size: int
    last_snapshot: str | None = None
    next_seq: int = 0


def pump_stream(
    reader,
    writer,
    *,
    total_bytes: Optional[int] = None,
    progress_cb: Optional[ProgressCallback] = None,
    rate_limit_bytes_per_sec: Optional[float] = None,
    chunk_size: int = CHUNK_SIZE,
    progress_interval: float = PROGRESS_INTERVAL_SECONDS,
) -> int:
    """Forward a binary stream chunk by chunk with optional throttling.

    The shim lives entirely in memory (no temporary export file), so disk
    space is never consumed. ``progress_cb`` receives
    ``(bytes_copied, total_bytes, bytes_per_sec)`` at most every
    ``progress_interval`` seconds, plus one final call at EOF.
    """
    started = time.monotonic()
    copied = 0
    deadline = 0.0
    last_report = 0.0

    def report() -> None:
        elapsed = time.monotonic() - started
        if not progress_cb:
            return
        bytes_per_sec = copied / elapsed if elapsed > 0 else 0.0
        progress_cb(copied, total_bytes, bytes_per_sec)

    while True:
        chunk = reader.read(chunk_size)
        if not chunk:
            break
        writer.write(chunk)
        copied += len(chunk)

        if rate_limit_bytes_per_sec and rate_limit_bytes_per_sec > 0:
            now = time.monotonic()
            if deadline < now:
                deadline = now
            deadline += len(chunk) / rate_limit_bytes_per_sec
            delay = deadline - time.monotonic()
            if delay > 0:
                time.sleep(delay)

        if progress_cb:
            now = time.monotonic()
            if now - last_report >= progress_interval:
                last_report = now
                report()

    report()
    return copied


def rss_mb_from_status(status_text: str) -> int | None:
    """Parse ``VmRSS`` (kB) from a /proc/<pid>/status dump into MiB."""
    for line in (status_text or "").splitlines():
        if line.startswith("VmRSS:"):
            try:
                return int(line.split()[1]) // 1024
            except (IndexError, ValueError):
                return None
    return None


def process_rss_mb(pid: int | None = None) -> int | None:
    if not pid:
        return None
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as handle:
            return rss_mb_from_status(handle.read())
    except OSError:
        return None


class CephUtils:
    def __init__(
        self,
        source_conf: str,
        source_pool: str,
        target_conf: str,
        target_pool: str,
        runner=None,
    ):
        self.source_conf = source_conf
        self.source_pool = source_pool
        self.target_conf = target_conf
        self.target_pool = target_pool
        self._runner = runner or subprocess.run
        self._validate_identifier(source_pool, "source_pool")
        self._validate_identifier(target_pool, "target_pool")

    @staticmethod
    def _validate_identifier(value: str, field: str = "value") -> None:
        if not IDENT_RE.match(value):
            raise ValidationError(f"非法 {field}: {value}")

    def _run(
        self,
        command,
        shell: bool = False,
        capture: bool = False,
        capture_stderr: bool = True,
        timeout: float | None = None,
    ):
        """Run a subprocess; `capture` selects whether stdout is returned.

        `timeout` 只给"读扫描"这类随时可以重来的调用使用：超时抛
        `subprocess.TimeoutExpired`，由调用方降级成"下轮再试"，避免一次
        GC 扫描把整轮清扫拖成几十分钟的陈旧视图。

        调用方没给超时时套用 `MIGRATION_RBD_CMD_TIMEOUT_SECONDS`：没有兜底
        超时，一个不可达的 mon 就能让 `rbd info` 无限重连并永久占住拷贝名额，
        超时按 rbd 自己的 ETIMEDOUT(rc=110) 上报，走既有失败路径。
        """
        kwargs = {
            "check": True,
            "text": True,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE if capture_stderr else None,
        }
        if shell:
            kwargs["shell"] = True
        default_timeout = timeout is None
        limit = rbd_cmd_timeout_seconds() if default_timeout else timeout
        if limit and limit > 0:
            kwargs["timeout"] = limit
        try:
            result = self._runner(command, **kwargs)
        except subprocess.TimeoutExpired as exc:
            if not default_timeout:
                raise
            raise subprocess.CalledProcessError(
                RBD_TIMEOUT_RC,
                command,
                output="",
                stderr=describe_timeout(command, limit),
            ) from exc
        if capture:
            return result.stdout
        return result

    def _rbd_spec(self, pool: str, name: str) -> str:
        return f"{pool}/{name}"

    def _export_import(
        self,
        export_cmd: list[str],
        import_cmd: list[str],
        *,
        total_bytes: Optional[int] = None,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
        stall_timeout: float | None = None,
    ) -> None:
        """Stream rbd export -> rbd import without a shell pipeline.

        The container image's /bin/sh is dash which rejects ``set -o
        pipefail``; using subprocess.Popen directly also lets us capture both
        commands' stderr for diagnosis. Bytes are forwarded through a small
        in-memory pump so we can report progress and enforce a rate limit
        without writing a temporary export file.

        `stall_timeout` 秒内没有任何字节流动就杀掉两端并抛 `CopyStalledError`：
        没有这道闸，Ceph 不可达时管道会永久阻塞，拷贝名额再也放不出来。
        """
        limit = (
            copy_stall_timeout_seconds()
            if stall_timeout is None
            else stall_timeout
        )
        with tempfile.TemporaryDirectory(
            prefix="mig-rbd-"
        ) as tmp_dir:
            export_err_path = os.path.join(tmp_dir, "export.err")
            import_err_path = os.path.join(tmp_dir, "import.err")
            with open(export_err_path, "w") as export_err_file, open(
                import_err_path, "w"
            ) as import_err_file:
                importer = subprocess.Popen(
                    import_cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=import_err_file,
                )
                exporter = subprocess.Popen(
                    export_cmd,
                    stdout=subprocess.PIPE,
                    stderr=export_err_file,
                )
                last_memory_log = [0.0]
                watchdog = _CopyWatchdog(limit, (exporter, importer)).start()

                def wrapped_progress(
                    copied: int,
                    total: int | None,
                    bytes_per_sec: float,
                ) -> None:
                    now = time.time()
                    watchdog.touch()
                    if now - last_memory_log[0] >= 10:
                        last_memory_log[0] = now
                        logging.info(
                            "[MIGRATION] RBD 拷贝内存监控: "
                            "export(pid=%s) RSS=%sMiB import(pid=%s) RSS=%sMiB "
                            "已复制=%s",
                            exporter.pid,
                            process_rss_mb(exporter.pid),
                            importer.pid,
                            process_rss_mb(importer.pid),
                            copied,
                        )
                    if progress_cb:
                        progress_cb(copied, total, bytes_per_sec)

                importer_exited_early = False
                try:
                    copied = pump_stream(
                        exporter.stdout,
                        importer.stdin,
                        total_bytes=total_bytes,
                        progress_cb=wrapped_progress,
                        rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
                    )
                except BrokenPipeError:
                    # stdin 断开只可能是 import 端已经退出（例如 diff 头校验失败、
                    # 目标卷状态不匹配）。源端继续导出没有意义；注意随后 export
                    # 的 rc=-15 是本进程 terminate() 造成的，不是真实原因。
                    importer_exited_early = True
                    logging.warning(
                        "[MIGRATION] rbd import 提前退出，终止 rbd export"
                        "（export 的 rc=-15 由本进程终止造成，根因见 import stderr）"
                    )
                    _terminate_process(exporter)
                except Exception:
                    # 回调/转发异常：尽快终止两端，避免残留孤儿进程。
                    for proc in (exporter, importer):
                        _terminate_process(proc)
                    raise
                finally:
                    watchdog.close()
                    try:
                        if exporter.stdout:
                            exporter.stdout.close()
                        if importer.stdin:
                            importer.stdin.close()
                    except BrokenPipeError:
                        # 缓冲里最后一块数据也写不进去，同样说明 import 端先退出；
                        # 不能让它盖掉下面更有信息量的 CalledProcessError。
                        importer_exited_early = True
                export_rc = exporter.wait()
                import_rc = importer.wait()

            with open(export_err_path, encoding="utf-8", errors="replace") as handle:
                export_err = handle.read()
            with open(import_err_path, encoding="utf-8", errors="replace") as handle:
                import_err = handle.read()
            if export_rc != 0 or import_rc != 0:
                # 两端 stderr 一起落日志：只报先判定的那个会把真实原因吞掉。
                logging.error(
                    "[MIGRATION] rbd 拷贝失败 export_rc=%s import_rc=%s "
                    "import 先退出=%s export_stderr=%s import_stderr=%s",
                    export_rc,
                    import_rc,
                    importer_exited_early,
                    export_err.strip(),
                    import_err.strip(),
                )
            if watchdog.stalled:
                # 停滞是根因，退出码只是我们 kill 出来的 -9/-15，先报它。
                raise CopyStalledError(
                    RBD_TIMEOUT_RC,
                    export_cmd,
                    output="",
                    stderr=(
                        f"拷贝停滞超过 {limit:.1f}s：期间 export->import 没有任何"
                        f"字节流动，已终止两端（通常是源/目标 Ceph 的 mon/OSD "
                        f"不可达）。export_stderr={export_err.strip()[:200] or '<空>'} "
                        f"import_stderr={import_err.strip()[:200] or '<空>'}"
                    ),
                )
            # import 先退出时 export 的 rc 必然来自上面的 terminate()，先报 import
            # 才拿得到真实原因；反之 export 先失败会让 import 读到截断的流，
            # 此时先报 export 才对（保持原判定顺序）。
            if import_rc != 0 and importer_exited_early:
                self._raise_rbd_failure("import", import_rc, import_cmd, import_err)
            if export_rc != 0:
                self._raise_rbd_failure("export", export_rc, export_cmd, export_err)
            if import_rc != 0:
                self._raise_rbd_failure("import", import_rc, import_cmd, import_err)
            return copied

    @staticmethod
    def _raise_rbd_failure(
        role: str, rc: int, cmd: list[str], stderr: str
    ) -> None:
        if rc == -9:
            logging.error(
                "[MIGRATION] rbd %s 被 SIGKILL(rc=-9)：疑似 cgroup "
                "OOM 或外部 kill -9，请检查 memory.events 与 kubectl top",
                role,
            )
        logging.error(
            "[MIGRATION] rbd %s 失败 rc=%s stderr=%s", role, rc, stderr.strip()
        )
        raise subprocess.CalledProcessError(rc, cmd, output="", stderr=stderr)

    def _snap_spec(self, pool: str, name: str, snap: str) -> str:
        return f"{pool}/{name}@{snap}"

    def create_source_snapshot(self, rbd_name: str, snap_name: str) -> None:
        self._validate_identifier(rbd_name, "rbd_name")
        self._validate_identifier(snap_name, "snap_name")
        spec = self._snap_spec(self.source_pool, rbd_name, snap_name)
        self._run(["rbd", "--conf", self.source_conf, "snap", "create", spec])
        logging.info("[MIGRATION] 已创建源快照 %s", spec)

    def remove_source_snapshot(self, rbd_name: str, snap_name: str) -> None:
        self._validate_identifier(rbd_name, "rbd_name")
        self._validate_identifier(snap_name, "snap_name")
        spec = self._snap_spec(self.source_pool, rbd_name, snap_name)
        self._run(["rbd", "--conf", self.source_conf, "snap", "rm", spec])
        logging.info("[MIGRATION] 已删除迁移快照 %s", spec)

    def list_source_snapshots(
        self, rbd_name: str, timeout: float | None = None
    ) -> list[str]:
        self._validate_identifier(rbd_name, "rbd_name")
        output = self._run(
            [
                "rbd",
                "--conf",
                self.source_conf,
                "snap",
                "ls",
                "--format",
                "json",
                self._rbd_spec(self.source_pool, rbd_name),
            ],
            capture=True,
            timeout=timeout,
        )
        return [entry.get("name") for entry in json.loads(output or "[]")]

    def prune_source_snapshots(
        self,
        rbd_name: str,
        job_id: str,
        keep: int,
        on_removed=None,
    ) -> list[str]:
        """删除本 job 的旧迁移快照；keep=0 表示清空本 job 全部快照。"""
        doomed = snapshots_to_prune(
            self.list_source_snapshots(rbd_name), job_id, keep
        )
        for snap_name in doomed:
            self.remove_source_snapshot(rbd_name, snap_name)
            if on_removed:
                on_removed(snap_name)
        return doomed

    def list_target_snapshots(self, rbd_name: str) -> list[str]:
        self._validate_identifier(rbd_name, "rbd_name")
        output = self._run(
            [
                "rbd",
                "--conf",
                self.target_conf,
                "snap",
                "ls",
                "--format",
                "json",
                self._rbd_spec(self.target_pool, rbd_name),
            ],
            capture=True,
        )
        return [entry.get("name") for entry in json.loads(output or "[]")]

    def create_target_snapshot(self, rbd_name: str, snap_name: str) -> None:
        self._validate_identifier(rbd_name, "rbd_name")
        self._validate_identifier(snap_name, "snap_name")
        spec = self._snap_spec(self.target_pool, rbd_name, snap_name)
        self._run(["rbd", "--conf", self.target_conf, "snap", "create", spec])
        logging.info("[MIGRATION] 已创建目标基线快照 %s", spec)

    def remove_target_snapshot(self, rbd_name: str, snap_name: str) -> None:
        self._validate_identifier(rbd_name, "rbd_name")
        self._validate_identifier(snap_name, "snap_name")
        spec = self._snap_spec(self.target_pool, rbd_name, snap_name)
        self._run(["rbd", "--conf", self.target_conf, "snap", "rm", spec])
        logging.info("[MIGRATION] 已删除暂存卷旧基线快照 %s", spec)

    def prune_target_snapshots(
        self, rbd_name: str, job_id: str, keep: int
    ) -> list[str]:
        """只保留暂存卷上最新的 keep 个基线快照。

        `import-diff` 只要求"上一轮那个快照"还在，再早的快照对后续增量没有
        意义；不清的话每同步一轮都会在暂存卷上多堆一条快照。
        """
        doomed = snapshots_to_prune(
            self.list_target_snapshots(rbd_name), job_id, keep
        )
        for snap_name in doomed:
            self.remove_target_snapshot(rbd_name, snap_name)
        return doomed

    def _remove_image(self, conf: str, pool: str, name: str) -> None:
        """删除映像前先清空快照：带快照时 rbd rm 会以 rc=39 直接失败。"""
        spec = self._rbd_spec(pool, name)
        try:
            self._run(["rbd", "--conf", conf, "snap", "purge", spec])
        except subprocess.CalledProcessError:
            # 无快照时 purge 也会返回非 0，最终以 rm 的结果为准。
            logging.debug("[MIGRATION] snap purge %s 未成功，继续尝试删除", spec)
        self._run(["rbd", "--conf", conf, "rm", spec])

    @staticmethod
    def stage_image_name(target_rbd_name: str) -> str:
        return f"{target_rbd_name}{STAGE_SUFFIX}"

    def list_target_images(self, timeout: float | None = None) -> list[str]:
        output = self._run(
            [
                "rbd",
                "--conf",
                self.target_conf,
                "ls",
                "--format",
                "json",
                self.target_pool,
            ],
            capture=True,
            timeout=timeout,
        )
        return list(json.loads(output or "[]"))

    def remove_stage_image(self, target_rbd_name: str) -> bool:
        """删除迁移暂存镜像（连同它在增量轮次中建立的 mig-* 快照）。

        失败/取消的任务走不到换入，暂存镜像只能显式清理；而它的名字由目标卷
        UUID 决定，重试会新建目标卷，因此旧名字再也匹配不上，必须由失败清理
        与后台 GC 兜底，否则会永久残留在目标池里。
        """
        self._validate_identifier(target_rbd_name, "target_rbd_name")
        stage_name = self.stage_image_name(target_rbd_name)
        if not self._image_exists(self.target_conf, self.target_pool, stage_name):
            return False
        logging.info(
            "[MIGRATION] 清理暂存镜像 %s",
            self._rbd_spec(self.target_pool, stage_name),
        )
        self._remove_image(self.target_conf, self.target_pool, stage_name)
        return True

    def _ensure_target_snapshot(self, target_rbd_name: str, snap_name: str) -> None:
        """增量导入前确认暂存卷存在 diff 头的起始快照。

        rbd import-diff 只做增量合并：目标卷缺少起始快照时会立即退出
        （start snapshot '...' does not exist in the image），源端 export 看到
        的只是 Broken pipe，报错有强烈误导性，因此在这里提前给出明确原因。
        """
        target_spec = self._rbd_spec(self.target_pool, target_rbd_name)
        try:
            if not self._image_exists(
                self.target_conf, self.target_pool, target_rbd_name
            ):
                raise StageImageMissingError(
                    f"目标暂存卷 {target_spec} 不存在，无法应用增量 diff；"
                    "该卷的基线全备产物已被清理，需要重做基线全备"
                )
            existing = self.list_target_snapshots(target_rbd_name)
        except subprocess.CalledProcessError as exc:
            # 预检本身是"为了报错更清楚"的辅助手段，不能变成新的失败点：
            # 集群慢到超时（rc=110）时直接放行 import-diff，它自己会给出
            # rbd 原生错误；否则一次探测抖动就会让整台 VM 白白失败。
            logging.warning(
                "[MIGRATION] 暂存卷 %s 预检失败，跳过预检继续 import-diff（%s）",
                target_spec,
                describe_process_error(exc),
            )
            return
        if snap_name not in existing:
            raise ValueError(
                f"目标暂存卷 {target_spec} 缺少起始快照 {snap_name}"
                f"（现有快照: {', '.join(existing) or '无'}），无法应用增量 diff；"
                "该卷需重新执行全量基线拷贝"
            )

    def export_import_diff(
        self,
        source_rbd_name: str,
        target_rbd_name: str,
        *,
        from_snap: str,
        to_snap: str,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
        total_bytes: Optional[int] = None,
    ) -> int:
        if total_bytes is None:
            # 增量没有天然的"总量"：不预扫的话进度只能按卷容量估算，
            # 大卷上会永远停在 0.x%，与全量拷贝的观感不一致。
            total_bytes = self.diff_bytes(
                source_rbd_name, from_snap=from_snap, to_snap=to_snap
            )
        export_cmd = [
            "rbd",
            "--conf",
            self.source_conf,
            "export-diff",
            "--from-snap",
            from_snap,
            self._snap_spec(self.source_pool, source_rbd_name, to_snap),
            "-",
        ]
        import_cmd = [
            "rbd",
            "--conf",
            self.target_conf,
            "import-diff",
            "-",
            self._rbd_spec(self.target_pool, target_rbd_name),
        ]
        self._ensure_target_snapshot(target_rbd_name, from_snap)
        logging.info(
            "[MIGRATION] 增量 diff %s..%s -> %s",
            from_snap,
            to_snap,
            target_rbd_name,
        )
        return self._export_import(
            export_cmd,
            import_cmd,
            total_bytes=total_bytes,
            progress_cb=progress_cb,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )

    def diff_bytes(
        self,
        rbd_name: str,
        *,
        from_snap: str,
        to_snap: str,
    ) -> Optional[int]:
        """预扫描一轮增量要传输的字节数，仅用于进度展示。

        ``rbd diff`` 只读对象映射（fast-diff），不搬数据，因此几秒即可返回；
        它拿到的 extent 总长与 export-diff 的载荷基本一致。任何失败都返回
        None，让调用方按"总量未知"降级，绝不因此中断迁移。
        """
        self._validate_identifier(rbd_name, "rbd_name")
        self._validate_identifier(from_snap, "from_snap")
        self._validate_identifier(to_snap, "to_snap")
        try:
            output = self._run(
                [
                    "rbd",
                    "--conf",
                    self.source_conf,
                    "diff",
                    "--from-snap",
                    from_snap,
                    "--format",
                    "json",
                    self._snap_spec(self.source_pool, rbd_name, to_snap),
                ],
                capture=True,
            )
            entries = json.loads(output or "[]")
        except (subprocess.CalledProcessError, ValueError) as exc:
            # 必须带上 stderr：`rbd diff` 被信号打死（rc=-6，客户端命中
            # ceph_assert）时，str(exc) 只有 "died with <Signals.SIGABRT>"，
            # 真正的原因全在 stderr 里，不打印就没法定位。
            detail = (
                describe_process_error(exc)
                if isinstance(exc, subprocess.CalledProcessError)
                else exc
            )
            logging.warning(
                "[MIGRATION] 增量大小预估失败，进度按卷容量估算 %s..%s: %s",
                from_snap,
                to_snap,
                detail,
            )
            return None
        total = sum(int(entry.get("length") or 0) for entry in entries)
        if total <= 0:
            return None
        logging.info(
            "[MIGRATION] 增量预估 %s..%s 约 %.2f GiB（%s 个 extent）",
            from_snap,
            to_snap,
            total / (1024**3),
            len(entries),
        )
        return total

    def _info(self, conf: str, pool: str, name: str) -> dict[str, Any]:
        output = self._run(
            [
                "rbd",
                "--conf",
                conf,
                "info",
                "--format",
                "json",
                self._rbd_spec(pool, name),
            ],
            capture=True,
        )
        return parse_rbd_info_json(output)

    def _image_exists(self, conf: str, pool: str, name: str) -> bool:
        """探测 RBD 映像是否存在：只有"确实不存在"才返回 False。

        这里绝不能用"rbd info 失败就当不存在"：集群慢到超时（rc=110）、
        映像被占用（rc=16）、认证/网络抖动都会失败，一旦被当成不存在，
        上层就会报出"目标暂存卷不存在，无法应用增量 diff"这种误导性结论，
        把一次本来能续上的增量迁移直接判死。
        """
        try:
            self._info(conf, pool, name)
            return True
        except subprocess.CalledProcessError as exc:
            if is_missing_image_error(exc):
                # 探测不存在的镜像会打印 rbd 错误，仅记录调试级别，避免误报。
                logging.debug(
                    "[MIGRATION] rbd info %s/%s 不存在: %s",
                    pool,
                    name,
                    describe_process_error(exc),
                )
                return False
            logging.warning(
                "[MIGRATION] rbd info %s/%s 探测失败（非不存在）: %s",
                pool,
                name,
                describe_process_error(exc),
            )
            raise

    def replace_rbd_data(
        self,
        source_rbd_name: str,
        target_rbd_name: str,
        *,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
        align_layout: bool = True,
        on_phase=None,
    ) -> bool:
        try:
            self._validate_identifier(source_rbd_name, "source_rbd_name")
            self._validate_identifier(target_rbd_name, "target_rbd_name")

            # Staging image lets us import to the target pool without ever
            # leaving the target volume in a "deleted, half-written" state if
            # the process is interrupted (pod shutdown / crash / SIGKILL).
            stage_name = self.stage_image_name(target_rbd_name)
            stage_spec = self._rbd_spec(self.target_pool, stage_name)
            target_spec = self._rbd_spec(self.target_pool, target_rbd_name)

            source_info = self._info(
                self.source_conf, self.source_pool, source_rbd_name
            )
            target_info = self._info(
                self.target_conf, self.target_pool, target_rbd_name
            )
            if source_info.get("size") != target_info.get("size"):
                logging.error(
                    "[MIGRATION] RBD 大小不一致: source=%s target=%s",
                    source_info.get("size"),
                    target_info.get("size"),
                )
                return False

            # Clean up a stage image left by a previous interrupted attempt,
            # otherwise the import below fails because the image already exists.
            if self._image_exists(self.target_conf, self.target_pool, stage_name):
                logging.info("[MIGRATION] 清理上次中断遗留的暂存镜像 %s", stage_spec)
                self._remove_image(self.target_conf, self.target_pool, stage_name)

            export_cmd = [
                "rbd",
                "--conf",
                self.source_conf,
                "export",
                self._rbd_spec(self.source_pool, source_rbd_name),
                "-",
            ]
            import_cmd = (
                ["rbd", "--conf", self.target_conf, "import"]
                + (
                    build_import_args(parse_layout(source_info))
                    if align_layout
                    else []
                )
                + ["-", stage_spec]
            )
            logging.info(
                "[MIGRATION] 开始导出/导入 RBD:\n  export: %s\n  import: %s",
                " ".join(shlex.quote(part) for part in export_cmd),
                " ".join(shlex.quote(part) for part in import_cmd),
            )
            self._export_import(
                export_cmd,
                import_cmd,
                total_bytes=source_info.get("size") or None,
                progress_cb=progress_cb,
                rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            )
            if on_phase:
                # 数据拷完不等于结束：后面还有布局校验与 rename 换入。
                on_phase("收尾：校验与换入", True)

            stage_info = self._info(self.target_conf, self.target_pool, stage_name)
            if stage_info.get("size") != source_info.get("size"):
                logging.error(
                    "[MIGRATION] 暂存 RBD 大小不一致: source=%s stage=%s",
                    source_info.get("size"),
                    stage_info.get("size"),
                )
                self._discard_stage_image(stage_name)
                return False

            if align_layout:
                mismatches = layout_mismatches(source_info, stage_info)
                if mismatches:
                    logging.error(
                        "[MIGRATION] 暂存 RBD 布局与源不一致，拒绝换入: %s",
                        "; ".join(mismatches),
                    )
                    self._discard_stage_image(stage_name)
                    return False

            if not self._swap_stage_into_place(target_rbd_name, stage_name):
                self._discard_stage_image(stage_name)
                return False

            final_info = self._info(
                self.target_conf, self.target_pool, target_rbd_name
            )
            if final_info.get("size") != source_info.get("size"):
                logging.error("[MIGRATION] RBD 导入后大小校验失败")
                self._discard_stage_image(stage_name)
                return False

            logging.info(
                "[MIGRATION] RBD %s -> %s -> %s 替换完成",
                self._rbd_spec(self.source_pool, source_rbd_name),
                stage_spec,
                target_spec,
            )
            return True
        except ValidationError:
            raise
        except CopyStalledError:
            # 停滞不是"这个卷坏了"而是集群不可达：清理暂存镜像后把真实原因抛上去，
            # 否则前端只会看到"RBD 替换失败"，看不出根因。
            try:
                self._discard_stage_image(stage_name)
            except Exception:  # noqa: BLE001 - 清理失败不能掩盖停滞根因
                logging.warning(
                    "[MIGRATION] 停滞清理暂存镜像失败 %s/%s",
                    self.target_pool,
                    stage_name,
                )
            raise
        except (subprocess.CalledProcessError, OSError) as exc:
            stderr_tail = getattr(exc, "stderr", "") or ""
            if isinstance(stderr_tail, bytes):
                stderr_tail = stderr_tail.decode(errors="replace")
            logging.error(
                "[MIGRATION] RBD 替换失败: %s%s",
                exc,
                f"\n  stderr: {stderr_tail.strip()[:2000]}" if stderr_tail.strip() else "",
            )
            self._discard_stage_image(stage_name)
            return False
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            logging.error("[MIGRATION] RBD 信息解析失败: %s", exc)
            self._discard_stage_image(stage_name)
            return False

    def _discard_stage_image(self, stage_name: str) -> None:
        """失败路径清理暂存镜像：残留既占容量，也会让下一次重试多踩一次坑。"""
        if not stage_name:
            return
        try:
            if self._image_exists(self.target_conf, self.target_pool, stage_name):
                self._remove_image(self.target_conf, self.target_pool, stage_name)
        except Exception:  # noqa: BLE001 - 清理失败留给下一轮预清理兜底
            logging.warning(
                "[MIGRATION] 清理暂存镜像失败，下一轮重试会重试 %s/%s",
                self.target_pool,
                stage_name,
            )

    def _swap_stage_into_place(self, target_rbd_name: str, stage_name: str) -> bool:
        """校验 watcher/快照后删除同名目标映像并 rename 换入暂存映像。"""
        stage_spec = self._rbd_spec(self.target_pool, stage_name)
        target_spec = self._rbd_spec(self.target_pool, target_rbd_name)
        status_output = self._run(
            [
                "rbd",
                "--conf",
                self.target_conf,
                "status",
                "--format",
                "json",
                target_spec,
            ],
            capture=True,
        )
        if watchers_present(parse_rbd_info_json(status_output)):
            logging.error(
                "[MIGRATION] 目标 RBD %s 仍有 watcher，拒绝换入", target_rbd_name
            )
            return False
        snap_output = self._run(
            [
                "rbd",
                "--conf",
                self.target_conf,
                "snap",
                "ls",
                "--format",
                "json",
                target_spec,
            ],
            capture=True,
        )
        if json.loads(snap_output):
            logging.error(
                "[MIGRATION] 目标 RBD %s 存在快照，拒绝换入", target_rbd_name
            )
            return False
        stage_snaps = self._run(
            [
                "rbd",
                "--conf",
                self.target_conf,
                "snap",
                "ls",
                "--format",
                "json",
                stage_spec,
            ],
            capture=True,
        )
        self._remove_image(self.target_conf, self.target_pool, target_rbd_name)
        if json.loads(stage_snaps):
            # 换入前清掉迁移过程中在暂存卷上积累的 mig-* 快照，避免把迁移
            # 中间态留在最终的 Cinder 卷上；放在删除目标卷之后，失败也不影响
            # 重试（重试仍需要这些快照做最后一轮增量），且清不掉只告警不中断。
            logging.info("[MIGRATION] 清理暂存镜像迁移快照 %s", stage_spec)
            try:
                self._run(
                    ["rbd", "--conf", self.target_conf, "snap", "purge", stage_spec]
                )
            except subprocess.CalledProcessError as exc:
                logging.warning(
                    "[MIGRATION] 暂存镜像快照清理失败（不影响迁移结果）: %s", exc
                )
        self._run(
            ["rbd", "--conf", self.target_conf, "rename", stage_spec, target_spec]
        )
        return True

    def _next_source_snapshot(self, session: "IncrementalSession", on_snapshot=None) -> str:
        snap_name = snapshot_name(session.job_id, session.next_seq)
        session.next_seq += 1
        self.create_source_snapshot(session.source_rbd_name, snap_name)
        if on_snapshot:
            on_snapshot(snap_name)
        return snap_name

    def seed_volume(
        self,
        source_rbd_name: str,
        target_rbd_name: str,
        job_id: str,
        *,
        on_snapshot=None,
        on_phase=None,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
    ) -> "IncrementalSession":
        """基像全量（seed）：建快照 → 全量拷贝到暂存卷 → 建同名基线快照。

        只做一次全备，不跑增量轮次：编排层要求先让"所有盘"都完成全备，
        再统一进入增量同步，否则后面的盘要等前面的盘跑完所有轮次才开始。
        """
        self._validate_identifier(source_rbd_name, "source_rbd_name")
        self._validate_identifier(target_rbd_name, "target_rbd_name")

        def announce_phase(label: str, keep_percent: bool = False) -> None:
            if on_phase:
                on_phase(label, keep_percent)

        source_info = self._info(
            self.source_conf, self.source_pool, source_rbd_name
        )
        layout = parse_layout(source_info)
        stage_name = self.stage_image_name(target_rbd_name)
        stage_spec = self._rbd_spec(self.target_pool, stage_name)
        if self._image_exists(self.target_conf, self.target_pool, stage_name):
            logging.info("[MIGRATION] 清理上次遗留的暂存镜像 %s", stage_spec)
            self._remove_image(self.target_conf, self.target_pool, stage_name)

        session = IncrementalSession(
            job_id=job_id,
            source_rbd_name=source_rbd_name,
            target_rbd_name=target_rbd_name,
            stage_name=stage_name,
            layout=layout,
            size=int(source_info.get("size") or 0),
        )
        base_snap = self._next_source_snapshot(session, on_snapshot)
        announce_phase("基线全量")
        export_cmd = [
            "rbd",
            "--conf",
            self.source_conf,
            "export",
            self._snap_spec(self.source_pool, source_rbd_name, base_snap),
            "-",
        ]
        import_cmd = (
            ["rbd", "--conf", self.target_conf, "import"]
            + build_import_args(layout)
            + ["-", stage_spec]
        )
        self._export_import(
            export_cmd,
            import_cmd,
            total_bytes=source_info.get("size") or None,
            progress_cb=progress_cb,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )
        # 数据已经拷完，进度条停在 100%；下面两步（校验布局、补建基线快照）
        # 在大卷 + 慢集群上要花上一段时间，必须让页面知道在等什么。
        announce_phase("收尾：校验布局与基线快照", keep_percent=True)
        stage_info = self._info(self.target_conf, self.target_pool, stage_name)
        assert_layout_matches(source_info, stage_info)
        # 目标端必须存在与源端同名的基线快照，后续 import-diff 才能把增量
        # 应用到该基线之上；rbd import（全量）本身不会建立任何快照。
        self.create_target_snapshot(stage_name, base_snap)
        session.last_snapshot = base_snap
        return session

    def sync_volume_round(
        self,
        session: "IncrementalSession",
        *,
        on_snapshot=None,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
    ) -> int:
        """单卷一轮增量：打源快照 → import-diff 到暂存卷 → 清理旧快照。"""
        next_snap = self._next_source_snapshot(session, on_snapshot)
        copied = self.export_import_diff(
            session.source_rbd_name,
            session.stage_name,
            from_snap=session.last_snapshot,
            to_snap=next_snap,
            progress_cb=progress_cb,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )
        session.last_snapshot = next_snap
        self.prune_source_snapshots(session.source_rbd_name, session.job_id, keep=2)
        self.prune_target_snapshots(session.stage_name, session.job_id, keep=1)
        return copied

    def finalize_incremental_volume(
        self,
        session: "IncrementalSession",
        *,
        on_snapshot=None,
        on_removed=None,
        on_phase=None,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
    ) -> bool:
        """停机后打最终快照、补齐最后一轮增量、换入并清空本 job 快照。"""
        try:
            if on_phase:
                on_phase("末轮增量")
            final_snap = self._next_source_snapshot(session, on_snapshot)
            self.export_import_diff(
                session.source_rbd_name,
                session.stage_name,
                from_snap=session.last_snapshot,
                to_snap=final_snap,
                progress_cb=progress_cb,
                rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            )
            source_info = self._info(
                self.source_conf, self.source_pool, session.source_rbd_name
            )
            if on_phase:
                on_phase("收尾：校验与换入", True)
            stage_info = self._info(
                self.target_conf, self.target_pool, session.stage_name
            )
            if stage_info.get("size") != source_info.get("size"):
                logging.error("[MIGRATION] 增量后大小校验失败")
                return False
            assert_layout_matches(source_info, stage_info)
            if not self._swap_stage_into_place(
                session.target_rbd_name, session.stage_name
            ):
                return False
            self.prune_source_snapshots(
                session.source_rbd_name,
                session.job_id,
                keep=0,
                on_removed=on_removed,
            )
            logging.info(
                "[MIGRATION] 增量迁移完成 %s -> %s",
                session.source_rbd_name,
                session.target_rbd_name,
            )
            return True
        except LayoutMismatchError as exc:
            logging.error("[MIGRATION] 增量后布局校验失败: %s", exc)
            return False
        except StageImageMissingError as exc:
            # 停机之后才发现暂存卷没了：这里不能偷偷重做全量（源机已停，全量
            # 拷贝会把停机时间拖到不可控），只能明确失败，让人决定重做还是换机。
            logging.error(
                "[MIGRATION] 换入前暂存卷已丢失，无法完成末轮增量: %s", exc
            )
            return False
        except (subprocess.CalledProcessError, OSError) as exc:
            logging.error("[MIGRATION] 增量迁移失败: %s", exc)
            return False


# ---------------------------------------------------------------------------
# RBD 拷贝全局门控：限制全服务同时运行的 export/import 对数，并按 Pod 内存
# 水位决定是否放行新的拷贝，避免 cgroup OOM 把其中一个 rbd 进程 SIGKILL。
# ---------------------------------------------------------------------------

MiB = 1024 * 1024
HUGE_MEMORY = 1 << 62

CURRENT_MEMORY_PATHS = (
    "/sys/fs/cgroup/memory.current",
    "/sys/fs/cgroup/memory/memory.usage_in_bytes",
)
LIMIT_MEMORY_PATHS = (
    "/sys/fs/cgroup/memory.max",
    "/sys/fs/cgroup/memory/memory.limit_in_bytes",
)
OOM_KILL_PATHS = (
    "/sys/fs/cgroup/memory.events",
    "/sys/fs/cgroup/memory/memory.oom_control",
)


def parse_memory_value(raw: str) -> Optional[int]:
    """Parse cgroup memory byte counts; treat ``max``/v1 huge as unlimited."""
    text = (raw or "").strip()
    if not text or text == "max":
        return None
    try:
        value = int(text)
    except (TypeError, ValueError):
        return None
    if value >= HUGE_MEMORY:
        return None
    return value


def parse_oom_count(text: str) -> int:
    for line in (text or "").splitlines():
        if line.startswith("oom_kill "):
            try:
                return int(line.split()[-1])
            except (IndexError, ValueError):
                return 0
    return 0


def _read_first_file(paths) -> Optional[str]:
    for path in paths:
        try:
            with open(path, encoding="utf-8") as handle:
                return handle.read().strip()
        except OSError:
            continue
    return None


def _proc_rss_bytes() -> int:
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0


def current_memory_bytes() -> int:
    raw = _read_first_file(CURRENT_MEMORY_PATHS)
    if raw is None:
        return _proc_rss_bytes()
    return parse_memory_value(raw) or _proc_rss_bytes()


def memory_limit_bytes() -> Optional[int]:
    raw = _read_first_file(LIMIT_MEMORY_PATHS)
    return parse_memory_value(raw) if raw is not None else None


def oom_kill_count() -> int:
    raw = _read_first_file(OOM_KILL_PATHS)
    return parse_oom_count(raw) if raw is not None else 0


def _env_float(name: str, default: float) -> float:
    return env_float(name, default)


def _env_int(name: str, default: int) -> int:
    return env_int(name, default, minimum=1)


class CopyGate:
    """Thread-safe, service-wide gate shared by all migration VM workers."""

    def __init__(
        self,
        *,
        max_active_copies: Optional[int] = None,
        high_water: Optional[float] = None,
        reserve_bytes: Optional[int] = None,
        memory_provider: Optional[Callable[[], tuple[int, Optional[int]]]] = None,
        poll_interval_seconds: float = 2.0,
        log_interval_seconds: float = 10.0,
    ):
        self.max_active_copies = max_active_copies or _env_int(
            "MIGRATION_MAX_RBD_COPIES", 2
        )
        self.high_water = (
            high_water
            if high_water is not None
            else _env_float("MIGRATION_MEMORY_HIGH_WATER", 0.85)
        )
        if reserve_bytes is not None:
            self.reserve_bytes = reserve_bytes
        else:
            reserve_mb = _env_int("MIGRATION_COPY_RESERVE_MB", 1024)
            self.reserve_bytes = reserve_mb * MiB
        self._memory_provider = memory_provider or (
            lambda: (current_memory_bytes(), memory_limit_bytes())
        )
        self._poll_interval_seconds = poll_interval_seconds
        self._log_interval_seconds = log_interval_seconds
        self._lock = threading.Lock()
        self._active_copies = 0
        # 线程 ident -> (描述, 拿到的时刻)，用来回答"名额到底被谁占着"。
        self._holders: dict[int, tuple[str, float]] = {}
        logging.info(
            "[MIGRATION] 拷贝内存门控: max_active=%s high_water=%.0f%% "
            "reserve=%sMiB",
            self.max_active_copies,
            self.high_water * 100,
            int(self.reserve_bytes / MiB),
        )

    @property
    def active_copies(self) -> int:
        with self._lock:
            return self._active_copies

    def holders(self) -> list[tuple[str, float]]:
        """当前占用名额的 (描述, 已持有秒数)，按持有时长倒序。"""
        with self._lock:
            now = time.monotonic()
            entries = [
                (owner or "未知", now - since)
                for owner, since in self._holders.values()
            ]
        return sorted(entries, key=lambda item: item[1], reverse=True)

    def holder_summary(self) -> str:
        """一行描述占用者，直接拼进排队日志。"""
        entries = self.holders()
        if not entries:
            return "无"
        return "、".join(f"{owner}({seconds:.0f}s)" for owner, seconds in entries)

    def try_acquire(self, owner: str = "") -> bool:
        """Try to claim one copy slot without waiting."""
        with self._lock:
            if self._active_copies >= self.max_active_copies:
                return False
            current, limit = self._memory_provider()
            if limit is not None and self._active_copies > 0:
                usable = int(limit * self.high_water)
                if current + self.reserve_bytes > usable:
                    return False
            self._active_copies += 1
            if owner:
                self._holders[threading.get_ident()] = (owner, time.monotonic())
            return True

    def acquire(
        self,
        can_proceed: Optional[Callable[[], bool]] = None,
        owner: str = "",
    ) -> None:
        """Wait until a slot is available; raises on a stop signal."""
        started = time.monotonic()
        next_log = 0.0
        while not self.try_acquire(owner):
            if can_proceed and not can_proceed():
                raise RuntimeError("迁移被停机信号中断，未获得拷贝并发名额")
            time.sleep(self._poll_interval_seconds)
            now = time.monotonic()
            if now >= next_log:
                next_log = now + self._log_interval_seconds
                current, limit = self._memory_provider()
                limit_text = f"{limit / MiB:.0f}MiB" if limit else "未限制"
                logging.info(
                    "[MIGRATION] 等待 RBD 拷贝名额/内存余量: "
                    "active=%s/%s current=%sMiB limit=%s 已等待 %.0fs 占用中: %s",
                    self._active_copies,
                    self.max_active_copies,
                    int(current / MiB),
                    limit_text,
                    now - started,
                    self.holder_summary(),
                )

    def release(self) -> None:
        with self._lock:
            self._active_copies = max(0, self._active_copies - 1)
            self._holders.pop(threading.get_ident(), None)


# 所有 MigrationManager 实例默认共享同一个门控实例。
COPY_GATE = CopyGate()

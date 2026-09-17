import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
import concurrent.futures
from typing import Any, Callable

from config import UPLOAD_FOLDER
from env_utils import env_int
from excel_parser import MigrationRow
from ceph_utils import (
    STAGE_SUFFIX,
    any_mon_reachable,
    describe_process_error,
    is_missing_image_error,
    parse_mon_endpoints,
)
from json_store import atomic_write_json
from migration_planner import orphan_snapshots
from state_machine import (
    CLEANUP_GONE,
    TERMINAL_CLEANUP_STATUSES,
    JobStatus,
    MigrationJob,
    MigrationMode,
    VolumeStatus,
    VolumeTask,
    VmStatus,
    VmTask,
)


#: GC 单次 rbd 读扫描（`rbd ls` / `rbd snap ls`）的超时。集群慢时一次 `rbd ls`
#: 实测能挂几十分钟，期间 GC 手里的"哪些资源还在用"的认知会过期，必须给它
#: 一个上限：超时就放弃这轮扫描，下一轮重来。
GC_SCAN_TIMEOUT_SECONDS = env_int("MIGRATION_GC_SCAN_TIMEOUT_SECONDS", 120, minimum=10)

#: 单轮 GC 的总预算：扫描 + 删除的总时长上限，超了就停手交给下一轮，
#: 避免一次清扫长期占着陈旧的任务视图。
GC_BUDGET_SECONDS = env_int("MIGRATION_GC_BUDGET_SECONDS", 900, minimum=60)

#: 删除作业时回收其迁移快照的预算。删除是人工触发的接口，不能无限期阻塞，
#: 超过预算就留给下一轮 GC（作业目录已被删，这里只做尽力而为的兜底）。
DELETE_SNAPSHOT_BUDGET_SECONDS = env_int(
    "MIGRATION_DELETE_SNAPSHOT_BUDGET_SECONDS", 60, minimum=5
)


def _conf_fingerprint(path: str) -> str:
    """conf 文件的内容指纹；读不出来时退化成路径，保证"不同 conf 不同键"。"""
    try:
        with open(path, "rb") as handle:
            return hashlib.sha256(handle.read()).hexdigest()
    except OSError:
        return os.path.abspath(path)


class JobManager:
    """Registry and executor for migration jobs with file-backed state.

    State is snapshotted to ``uploads/jobs_state.json`` so an in-flight batch
    survives a pod restart/rollout: after restart the API still reports
    per-VM progress and the operator can re-submit only the unfinished VMs.
    """

    def __init__(self, state_file: str | None = None):
        self._jobs: dict[str, MigrationJob] = {}
        self._workers: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()
        self._state_file = state_file or os.path.join(UPLOAD_FOLDER, "jobs_state.json")
        self._last_save = 0.0
        self._last_cleanup = 0.0
        self._saver: threading.Thread | None = None
        self._saver_stop = threading.Event()
        self._load()

    # ---------- persistence ----------

    def _load(self) -> None:
        try:
            with open(self._state_file, encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            return
        except (OSError, ValueError) as exc:
            logging.warning("[MIGRATION] 读取任务状态文件失败: %s", exc)
            return
        changed = False
        with self._lock:
            for job in payload.get("jobs") or []:
                try:
                    restored = MigrationJob.from_dict(job)
                except (KeyError, ValueError) as exc:
                    logging.warning("[MIGRATION] 忽略损坏的任务记录: %s", exc)
                    continue
                changed = self._mark_interrupted(restored) or changed
                changed = self._settle_stale_volumes(restored) or changed
                self._jobs[restored.id] = restored
        # 重启收敛必须落盘：否则内存里已判 FAILED/COMPLETED，磁盘仍是 RUNNING，
        # 下一次重启又要重跑一遍收敛，且外部读状态文件会看到不一致的旧值。
        if changed:
            self._save()

    @staticmethod
    def _settle_stale_volumes(job: MigrationJob) -> bool:
        """重启后不可能再有在跑的 rbd，把没落终态的卷标成 FAILED。

        GC 用"卷是否落终态"判断暂存镜像/源端快照能不能回收。若进程被杀导致卷
        永远停在 `pending`/`copying`，GC 会一直认为它还在飞，残留越积越多。
        返回是否发生了改动，供调用方决定要不要落盘。
        """
        changed = False
        for vm in job.vms:
            for volume in vm.volumes:
                if volume.status not in (VolumeStatus.SUCCESS, VolumeStatus.FAILED):
                    volume.status = VolumeStatus.FAILED
                    volume.error = (
                        volume.error or "进程中断，暂存卷与迁移快照可回收"
                    )
                    changed = True
        return changed

    def _settle_volume_cleanup(self, volume: VolumeTask, status: str) -> None:
        """把卷的快照回收状态落到终态并落盘（GC 只改内存对象，必须自己保存）。"""
        if volume.cleanup_status == status:
            return
        volume.cleanup_status = status
        self._save()

    @staticmethod
    def _mark_interrupted(job: MigrationJob) -> bool:
        """把上次崩溃留下的 RUNNING 任务收敛到终态。

        崩溃可能发生在"所有 VM 都已落终态、但任务状态还没写成 COMPLETED"之间，
        旧实现此时直接 return，任务会永远停在 RUNNING：既不能被 cancel，也不能
        delete。因此没有未完成 VM 时也要按 refresh_status() 收敛（空任务按完成
        处理），避免永久卡死。返回是否发生了改动。
        """
        if job.status != JobStatus.RUNNING:
            return False
        terminal = (
            VmStatus.SUCCESS,
            VmStatus.FAILED,
            VmStatus.PREFLIGHT_FAILED,
            VmStatus.CANCELLED,
        )
        unfinished = [vm for vm in job.vms if vm.status not in terminal]
        if not unfinished:
            job.refresh_status()
            if job.status == JobStatus.RUNNING:
                # 空任务或状态未能自动推导：按已完成收敛。
                job.status = JobStatus.COMPLETED
            return True
        job.status = JobStatus.FAILED
        job.error = "上次运行被中断（进程退出/重启），未完成 VM 请核对后重新发起"
        for vm in unfinished:
            vm.mark_failed("进程中断，任务需重新发起")
        return True

    def _save(self) -> None:
        with self._lock:
            payload = {
                "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "jobs": [job.to_dict() for job in self._jobs.values()],
            }
        try:
            # 并发保存必须用唯一临时名：固定写 `jobs_state.json.tmp` 时，
            # 两个线程会互相抢同一个文件，后到的那次 os.replace 直接
            # ENOENT 报"保存任务状态失败"。
            atomic_write_json(self._state_file, payload, indent=1)
            self._last_save = time.time()
        except OSError as exc:
            logging.warning("[MIGRATION] 保存任务状态失败: %s", exc)

    def save_now(self) -> None:
        self._save()

    def cleanup_old_uploads(
        self,
        max_age_days: int | None = None,
        now: float | None = None,
    ) -> None:
        """Remove old per-job upload directories while keeping jobs_state."""
        if max_age_days is None:
            max_age_days = env_int("MIGRATION_UPLOAD_RETENTION_DAYS", 7, minimum=1)
        max_age_days = max(1, max_age_days)
        now = now if now is not None else time.time()
        cutoff = now - max_age_days * 24 * 3600
        upload_root = os.path.dirname(self._state_file) or UPLOAD_FOLDER
        try:
            entries = os.listdir(upload_root)
        except OSError:
            return
        for name in entries:
            if not re.fullmatch(r"[0-9a-fA-F]{12}", name):
                continue
            path = os.path.join(upload_root, name)
            if not os.path.isdir(path):
                continue
            with self._lock:
                job = self._jobs.get(name)
                running = job is not None and job.status == JobStatus.RUNNING
            if running:
                continue
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if mtime < cutoff:
                logging.info("[MIGRATION] 清理过期上传目录 %s", path)
                shutil.rmtree(path, ignore_errors=True)

    def start_persistent_saver(self, interval: float = 5.0) -> None:
        """Snapshot running jobs every ``interval`` seconds in the background."""
        if self._saver and self._saver.is_alive():
            return
        self.cleanup_old_uploads()
        self._last_cleanup = time.time()

        def loop() -> None:
            while not self._saver_stop.is_set():
                self._saver_stop.wait(interval)
                if not self._saver_stop.is_set():
                    self._save()
                if time.time() - self._last_cleanup >= 3600:
                    self.cleanup_old_uploads()
                    self._last_cleanup = time.time()

        self._saver_stop.clear()
        self._saver = threading.Thread(target=loop, name="job-state-saver", daemon=True)
        self._saver.start()

    def stop_persistent_saver(self) -> None:
        self._saver_stop.set()
        if self._saver:
            self._saver.join(timeout=3)

    def create_job(
        self,
        rows: list[MigrationRow],
        job_id: str | None = None,
    ) -> MigrationJob:
        job_id = job_id or uuid.uuid4().hex[:12]
        vms = [
            VmTask(
                name=row.vm_name,
                target_az=row.target_az,
                mode=MigrationMode.parse(row.mode),
                target_image=row.target_image,
                target_flavor=row.target_flavor,
                source_server_id=row.source_server_id,
                start_target=row.start_target,
            )
            for row in rows
        ]
        job = MigrationJob(id=job_id, vms=vms)
        with self._lock:
            self._jobs[job.id] = job
        self._save()
        return job

    def get(self, job_id: str) -> MigrationJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def cancel(self, job_id: str) -> bool:
        """标记任务为取消；已结束或不存在时返回 False。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.status != JobStatus.RUNNING:
                return False
            job.cancelled = True
        for vm in job.vms:
            if vm.status not in {
                VmStatus.SUCCESS,
                VmStatus.FAILED,
                VmStatus.PREFLIGHT_FAILED,
                VmStatus.CANCELLED,
            }:
                vm.mark_cancelled()
        self._save()
        logging.info("[MIGRATION] 任务 %s 已标记取消", job_id)
        return True

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job.cancelled)

    def request_cutover(self, job_id: str, vm_name: str) -> bool:
        """把某台 VM 标记为"可以切换"：由预拷贝线程在下一个轮次边界接管。"""
        found = False
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.RUNNING:
                return False
            vm = next((item for item in job.vms if item.name == vm_name), None)
            if vm is not None and vm.status == VmStatus.AWAITING_CUTOVER:
                vm.cutover_requested = True
                found = True
        if not found:
            return False
        # _save() 自己取锁，必须在锁外调用，否则死锁。
        self._save()
        logging.info("[MIGRATION] 任务 %s 的 VM %s 收到切换指令", job_id, vm_name)
        return True

    def is_cutover_requested(self, job_id: str, vm_name: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            return any(
                vm.name == vm_name and vm.cutover_requested for vm in job.vms
            )

    def request_sync(self, job_id: str, vm_name: str) -> bool:
        """请求"只同步一轮增量"：由预拷贝线程在待命循环里接管。"""
        found = False
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.RUNNING:
                return False
            vm = next((item for item in job.vms if item.name == vm_name), None)
            if vm is not None and vm.status == VmStatus.AWAITING_CUTOVER:
                if vm.sync_requested:
                    return False
                vm.sync_requested = True
                found = True
        if not found:
            return False
        # _save() 自己取锁，必须在锁外调用，否则死锁。
        self._save()
        logging.info("[MIGRATION] 任务 %s 的 VM %s 收到同步指令", job_id, vm_name)
        return True

    def is_sync_requested(self, job_id: str, vm_name: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            return any(
                vm.name == vm_name and vm.sync_requested for vm in job.vms
            )

    def delete(self, job_id: str) -> str | None:
        """删除任务记录与其上传目录；返回错误信息，成功返回 None。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return "任务不存在"
            if job.status == JobStatus.RUNNING:
                return "任务仍在进行中，请先取消并等待结束"
            if job_id in self._workers and self._workers[job_id].is_alive():
                return "任务线程仍在收尾，请稍后重试"
            self._jobs.pop(job_id, None)
        self._save()
        upload_root = os.path.dirname(self._state_file) or UPLOAD_FOLDER
        job_dir = os.path.join(upload_root, job_id)
        # 作业记录已删，它的 mig-* 快照此刻才算孤儿。必须在删目录前尽力回收，
        # 否则源卷/池信息随目录一起丢失，快照只能永远留在集群里。
        if self._has_unsettled_snapshots(job):
            try:
                self._sweep_job_snapshots(
                    job,
                    upload_root,
                    None,
                    [],
                    time.monotonic() + DELETE_SNAPSHOT_BUDGET_SECONDS,
                )
            except Exception:  # noqa: BLE001 - 回收失败不应阻塞删除
                logging.exception(
                    "[MIGRATION] 删除前回收迁移快照失败 job=%s", job_id
                )
        if os.path.isdir(job_dir):
            shutil.rmtree(job_dir, ignore_errors=True)
        logging.info("[MIGRATION] 任务 %s 已删除", job_id)
        return None

    @staticmethod
    def _has_unsettled_snapshots(job: MigrationJob) -> bool:
        """是否有还没落终态的增量快照需要回收（全量作业直接跳过）。"""
        return any(
            volume.mode == MigrationMode.INCREMENTAL
            and volume.cleanup_status not in TERMINAL_CLEANUP_STATUSES
            for vm in job.vms
            for volume in vm.volumes
        )

    @staticmethod
    def _job_still_working(job: MigrationJob, live_workers: set[str]) -> bool:
        """任务是否可能还有 rbd 进程在读写源/目标卷。

        只按 `job.status` 判断是不够的：出现过任务已经标成 completed、但某个卷
        仍停在 `copying` 的窗口，此时无论把它的 `-mig-stage` 暂存镜像还是源端
        `mig-*` 快照当成"孤儿"回收，都会打断正在跑的 export / import-diff。
        注意基线全备期间卷状态还是 `pending`（`seed_volume` 跑完才置 `copying`），
        而暂存镜像与源端快照那时已经在用了，所以判定按"是否落终态"而不是
        "是否等于 copying"。宁可下轮再回收。
        """
        if job.status == JobStatus.RUNNING or job.id in live_workers:
            return True
        return any(
            volume.status not in (VolumeStatus.SUCCESS, VolumeStatus.FAILED)
            for vm in job.vms
            for volume in vm.volumes
        )

    @staticmethod
    def _is_retryable_remove_error(exc: subprocess.CalledProcessError) -> bool:
        """EBUSY(16) 与信号退出(负退出码) 都表示"稍后再试"，不是真失败。"""
        return exc.returncode == 16 or exc.returncode < 0

    def _still_working_job_ids(self) -> set[str]:
        """当前可能还有 rbd 进程在读写卷的 job id；每次调用都重新计算。"""
        with self._lock:
            jobs = list(self._jobs.values())
            live_workers = {
                job_id
                for job_id, thread in self._workers.items()
                if thread.is_alive()
            }
        return {
            job.id for job in jobs if self._job_still_working(job, live_workers)
        }

    def _protected_stage_images(self) -> set[str]:
        """此刻绝对不能回收的暂存镜像名（删除前现算，而不是开扫时算一次）。

        扫描本身可能跑很久（集群慢时 `rbd ls` 几十分钟才返回），期间完全可能
        有新任务开始基线全备、写下自己的 `-mig-stage` 镜像。如果保护集是开扫
        时的快照，这些新镜像就会被当成孤儿删掉，紧接着任务报
        "目标暂存卷不存在，无法应用增量 diff" 直接失败，对应的 `rbd rm`
        还会和正在写的 rbd 进程撞在一起。因此这里每次调用都按当前任务状态
        重算。
        """
        from ceph_utils import CephUtils  # 延迟导入，避免模块级循环依赖

        still_working = self._still_working_job_ids()
        with self._lock:
            jobs = list(self._jobs.values())
        protected: set[str] = set()
        for job in jobs:
            if job.id not in still_working:
                continue
            for vm in job.vms:
                for volume in vm.volumes:
                    if volume.target_rbd_name:
                        protected.add(
                            CephUtils.stage_image_name(volume.target_rbd_name)
                        )
        return protected

    def sweep_orphan_snapshots(self, ceph_factory=None) -> list[str]:
        """删除归属已结束 job 的迁移快照；非 mig- 前缀一律不动。"""
        from ceph_utils import CephUtils  # 延迟导入，避免模块级循环依赖

        factory = ceph_factory or CephUtils
        upload_root = os.path.dirname(self._state_file) or UPLOAD_FOLDER
        with self._lock:
            jobs = list(self._jobs.values())
        removed: list[str] = []
        deadline = time.monotonic() + GC_BUDGET_SECONDS
        for job in jobs:
            if not self._sweep_job_snapshots(
                job, upload_root, factory, removed, deadline
            ):
                logging.warning(
                    "[MIGRATION] 迁移快照清扫超过 %ss 预算，剩余目标留到下一轮",
                    GC_BUDGET_SECONDS,
                )
                break
        return removed

    def _sweep_job_snapshots(
        self,
        job: MigrationJob,
        upload_root: str,
        factory: Any,
        removed: list[str],
        deadline: float,
    ) -> bool:
        """清扫单个作业的迁移快照；超过预算返回 False。"""
        if factory is None:
            from ceph_utils import CephUtils  # 延迟导入，避免模块级循环依赖

            factory = CephUtils
        if time.monotonic() > deadline:
            return False
        job_dir = os.path.join(upload_root, job.id)
        source_conf = os.path.join(job_dir, "source_ceph.conf")
        target_conf = os.path.join(job_dir, "target_ceph.conf")
        if not (os.path.exists(source_conf) and os.path.exists(target_conf)):
            return True
        client = None
        for vm in job.vms:
            for volume in vm.volumes:
                if volume.mode != MigrationMode.INCREMENTAL:
                    continue
                if volume.cleanup_status in TERMINAL_CLEANUP_STATUSES:
                    # 这一卷的迁移快照已经处理完（或源卷早已消失）：再扫一次
                    # 只是白跑一趟 `rbd snap ls`，历史任务多了就是成片的噪声。
                    continue
                if client is None:
                    client = factory(
                        source_conf=source_conf,
                        source_pool=volume.source_pool or "volumes",
                        target_conf=target_conf,
                        target_pool=volume.target_pool or "volumes",
                    )
                try:
                    names = client.list_source_snapshots(
                        volume.source_rbd_name,
                        timeout=GC_SCAN_TIMEOUT_SECONDS,
                    )
                except subprocess.TimeoutExpired:
                    # 扫描超时说明集群/网络此刻很慢，本轮结论不可信：
                    # 直接放弃这个卷，别拿陈旧结果去删快照。
                    logging.warning(
                        "[MIGRATION] 扫描迁移快照超时 %s（>%ss），下轮重试",
                        volume.source_rbd_name,
                        GC_SCAN_TIMEOUT_SECONDS,
                    )
                    continue
                except subprocess.CalledProcessError as exc:
                    if is_missing_image_error(exc):
                        # rc=2 = ENOENT：源卷本身已经不在了（迁移成功后源机
                        # 被清理是常规操作），它上面的 mig-* 快照随之消失，
                        # 没有任何东西可回收。旧实现把它当"环境抖动"每轮重
                        # 试，于是同一条 WARNING 永远刷下去、cleanup_status
                        # 也永远落不了地。
                        logging.warning(
                            "[MIGRATION] 源卷已不存在，无需回收迁移快照 %s（%s）",
                            volume.source_rbd_name,
                            describe_process_error(exc),
                        )
                        self._settle_volume_cleanup(volume, CLEANUP_GONE)
                        continue
                    # 集群慢时 `rbd snap ls` 会超时（rc=110），属于环境抖动：
                    # 不是任务失败，下一轮 GC 会重试，别刷 ERROR 吓人。
                    logging.warning(
                        "[MIGRATION] 扫描迁移快照失败 %s（%s），下轮重试",
                        volume.source_rbd_name,
                        describe_process_error(exc),
                    )
                    continue
                except Exception:  # noqa: BLE001 - GC 失败不影响服务
                    logging.exception(
                        "[MIGRATION] 扫描迁移快照失败 %s",
                        volume.source_rbd_name,
                    )
                    continue
                # 哪些 job 还在飞必须现算：开扫时的快照会把"扫描期间新起的
                # 任务"当成已结束，把它的 mig-* 快照删掉。
                for snap_name in orphan_snapshots(
                    names, self._still_working_job_ids()
                ):
                    try:
                        client.remove_source_snapshot(
                            volume.source_rbd_name, snap_name
                        )
                        removed.append(snap_name)
                    except subprocess.CalledProcessError as exc:
                        if self._is_retryable_remove_error(exc):
                            logging.warning(
                                "[MIGRATION] 迁移快照正被占用或删除被中断，"
                                "跳过待下轮回收 %s（%s）",
                                snap_name,
                                describe_process_error(exc),
                            )
                        else:
                            logging.error(
                                "[MIGRATION] 删除孤儿快照失败 %s（%s）",
                                snap_name,
                                describe_process_error(exc),
                            )
                    except Exception:  # noqa: BLE001
                        logging.exception(
                            "[MIGRATION] 删除孤儿快照失败 %s", snap_name
                        )
        return True

    def sweep_best_effort(self) -> None:
        for sweep, label in (
            (self.sweep_orphan_snapshots, "迁移快照"),
            (self.sweep_orphan_stages, "暂存镜像"),
        ):
            try:
                sweep()
            except Exception:  # noqa: BLE001 - 清扫失败不影响任务结果
                logging.exception("[MIGRATION] %s清扫失败", label)

    def sweep_orphan_stages(self, ceph_factory=None) -> list[str]:
        """删除目标池里不属于运行中任务的 *-mig-stage 暂存镜像。

        暂存镜像名由目标卷 UUID 决定，而每次迁移都会新建目标卷，所以历史失败
        任务留下的暂存镜像没有任何后续步骤会再引用它，只能靠这里兜底回收。
        若任务目录已被删除，其所在池仍会被其他任务的 conf 覆盖到。
        """
        from ceph_utils import CephUtils  # 延迟导入，避免模块级循环依赖

        factory = ceph_factory or CephUtils
        upload_root = os.path.dirname(self._state_file) or UPLOAD_FOLDER
        with self._lock:
            jobs = list(self._jobs.values())
        # 扫描目标（conf + 池）只影响"这轮扫不扫得到"，不影响删除安全性：
        # 删之前一律按当前任务状态重算保护集，见 _protected_stage_images()。
        # 每个任务都有自己的 conf 文件，内容是同一份 profile：按"conf 内容 +
        # 池"去重，否则同一个集群会被逐任务各扫一遍（线上一轮 12 次 rbd ls）。
        targets: dict[tuple[str, str], tuple[str, str]] = {}
        for job in jobs:
            job_dir = os.path.join(upload_root, job.id)
            target_conf = os.path.join(job_dir, "target_ceph.conf")
            if not os.path.exists(target_conf):
                continue
            for vm in job.vms:
                for volume in vm.volumes:
                    pool = volume.target_pool or "volumes"
                    targets.setdefault(
                        (_conf_fingerprint(target_conf), pool),
                        (target_conf, pool),
                    )
        removed: list[str] = []
        deadline = time.monotonic() + GC_BUDGET_SECONDS
        for (target_conf, target_pool) in targets.values():
            if time.monotonic() > deadline:
                logging.warning(
                    "[MIGRATION] 暂存镜像清扫超过 %ss 预算，剩余目标留到下一轮",
                    GC_BUDGET_SECONDS,
                )
                break
            mons = parse_mon_endpoints(target_conf)
            if mons and not any_mon_reachable(mons):
                # 历史任务的 conf 可能指向早已下线的集群：这时 `rbd ls` 只会
                # 挂到超时（实测 180s 仍无输出），既扫不出结果又挡着后面真正
                # 需要清扫的池。先探一次 mon 端口，联不上就直接跳过并说清原因。
                logging.warning(
                    "[MIGRATION] 目标集群不可达（mon=%s），跳过暂存镜像扫描 pool=%s："
                    "通常是历史任务指向的旧集群已下线，需要人工确认该池是否还有残留",
                    ",".join(f"{host}:{port}" for host, port in mons),
                    target_pool,
                )
                continue
            client = factory(
                source_conf=target_conf,
                source_pool=target_pool,
                target_conf=target_conf,
                target_pool=target_pool,
            )
            try:
                names = client.list_target_images(
                    timeout=GC_SCAN_TIMEOUT_SECONDS
                )
            except subprocess.TimeoutExpired:
                # 扫描超时说明集群此刻很慢：本轮结果不可信，直接放弃这个池。
                logging.warning(
                    "[MIGRATION] 扫描暂存镜像超时 pool=%s mon=%s（>%ss），下轮重试",
                    target_pool,
                    ",".join(f"{host}:{port}" for host, port in mons),
                    GC_SCAN_TIMEOUT_SECONDS,
                )
                continue
            except subprocess.CalledProcessError as exc:
                # 池很大 + 集群慢时 `rbd ls` 会超时（rc=110），属于环境抖动：
                # 这不是任务失败，下一轮 GC 会重试，别刷 ERROR 吓人。
                logging.warning(
                    "[MIGRATION] 扫描暂存镜像失败 pool=%s mon=%s（%s），下轮重试",
                    target_pool,
                    ",".join(f"{host}:{port}" for host, port in mons),
                    describe_process_error(exc),
                )
                continue
            except Exception:  # noqa: BLE001 - GC 失败不影响服务
                logging.exception("[MIGRATION] 扫描暂存镜像失败 pool=%s", target_pool)
                continue
            for name in names:
                if not name.endswith(STAGE_SUFFIX):
                    continue
                # 只要卷还没落终态，就说明（可能）有 rbd 进程正在写它。曾经只按
                # job.status 判断，结果把一台正在迁移的 VM 的暂存镜像当成孤儿
                # 删掉，VM 随后报"暂存卷不存在"直接失败。这里必须逐个删除前重算：
                # 这次 `rbd ls` 可能已经跑了几十分钟，期间新起的任务不在开扫时的
                # 任务列表里，用旧列表判断就会误删它正在写的暂存镜像。
                if name in self._protected_stage_images():
                    continue
                try:
                    if client.remove_stage_image(name[: -len(STAGE_SUFFIX)]):
                        removed.append(name)
                except subprocess.CalledProcessError as exc:
                    if self._is_retryable_remove_error(exc):
                        # EBUSY：镜像还被迁移中的 rbd 进程占着，说明它其实
                        # 不是孤儿。负数退出码 = 被信号打死（rbd 客户端命中
                        # ceph_assert 时就是 SIGABRT）。两种都按"稍后再试"处理，
                        # 否则一串 ERROR 会盖掉真正的原因。
                        logging.warning(
                            "[MIGRATION] 暂存镜像正被占用或删除被中断，跳过待下轮回收 %s（%s）",
                            name,
                            describe_process_error(exc),
                        )
                    else:
                        logging.error(
                            "[MIGRATION] 删除孤儿暂存镜像失败 %s（%s）",
                            name,
                            describe_process_error(exc),
                        )
                except Exception:  # noqa: BLE001
                    logging.exception("[MIGRATION] 删除孤儿暂存镜像失败 %s", name)
        return removed

    def start_storage_gc(self) -> None:
        """服务启动时后台清扫上次运行遗留的迁移快照与暂存镜像。"""
        threading.Thread(
            target=self.sweep_best_effort,
            name="storage-gc",
            daemon=True,
        ).start()

    def register_worker(self, job_id: str, thread: threading.Thread) -> None:
        with self._lock:
            self._workers[job_id] = thread

    def unregister_worker(self, job_id: str) -> None:
        with self._lock:
            self._workers.pop(job_id, None)

    def active_worker_threads(self) -> list[threading.Thread]:
        with self._lock:
            return [
                thread
                for job_id, thread in self._workers.items()
                if thread.is_alive()
            ]

    def has_active_workers(self) -> bool:
        return bool(self.active_worker_threads())

    def list_jobs(self, limit: int = 20) -> list[MigrationJob]:
        with self._lock:
            return list(self._jobs.values())[-limit:]

    def execute(
        self,
        job: MigrationJob,
        run_vm: Callable[[VmTask, dict[str, Any]], None],
        options: dict[str, Any],
        should_stop: Callable[[], bool] | None = None,
    ) -> None:
        """Run every queued VM; a VM failure must not stop the batch."""
        terminal = {
            VmStatus.SUCCESS,
            VmStatus.FAILED,
            VmStatus.PREFLIGHT_FAILED,
            VmStatus.CANCELLED,
        }
        pending_vms = [vm for vm in job.vms if vm.status not in terminal]
        should_stop = should_stop or (lambda: False)

        def run_one(vm: VmTask) -> None:
            if vm.status in terminal:
                return
            try:
                run_vm(vm, options)
            except Exception as exc:  # noqa: BLE001 - batch isolation
                logging.exception("[MIGRATION] Job %s VM %s 异常", job.id, vm.name)
                vm.mark_failed(str(exc))
            self._save()

        max_vms = int(options.get("vm_concurrency") or 1)
        def stop_unstarted() -> None:
            if job.cancelled:
                vm.mark_cancelled("任务被用户取消，未执行")
            else:
                vm.mark_failed("任务被停机信号中断，未执行")

        if max_vms <= 1 or len(pending_vms) <= 1:
            for vm in pending_vms:
                if should_stop():
                    stop_unstarted()
                    continue
                run_one(vm)
        else:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_vms
            ) as executor:
                futures = []
                for vm in pending_vms:
                    if should_stop():
                        stop_unstarted()
                        continue
                    futures.append(executor.submit(run_one, vm))
                for future in concurrent.futures.as_completed(futures):
                    # 逐个吞掉异常：任何一个 future.result() 抛错都不该让
                    # execute() 提前返回，否则其余 VM 还在后台拷贝，任务却已
                    # 被标成 COMPLETED/FAILED 并落盘。
                    try:
                        future.result()
                    except Exception:  # noqa: BLE001 - run_one 已兜底，这里只防漏网
                        logging.exception(
                            "[MIGRATION] Job %s 存在未捕获的 VM 异常", job.id
                        )
        if should_stop():
            if job.cancelled:
                job.status = JobStatus.CANCELLED
                job.error = "任务被用户取消，部分 VM 未执行"
                logging.warning("[MIGRATION] Job %s 已按用户取消停止", job.id)
            else:
                job.status = JobStatus.FAILED
                job.error = "迁移任务被停机信号中断，部分 VM 未完成（可重新发起剩余 VM）"
                logging.warning("[MIGRATION] Job %s 已按停机信号停止", job.id)
            self._save()
            return
        job.status = JobStatus.COMPLETED
        job.refresh_status()
        self._save()

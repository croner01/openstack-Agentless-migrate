"""Graceful shutdown coordination for the migration web service.

The migration web app runs two kinds of work in the same process:

* short HTTP requests (Flask), and
* long-running migration jobs (background threads that may be in the middle
  of an ``rbd export | rbd import`` stream).

Kubernetes stops a pod by sending SIGTERM, then waits
``terminationGracePeriodSeconds`` before SIGKILL.  This module makes SIGTERM
graceful: we stop accepting new migrations, let the currently active volume
copy reach its next safe boundary, and only then exit the process.
"""

import logging
import os
import signal
import threading
import time
from typing import Any


class ShutdownCoordinator:
    def __init__(
        self,
        job_manager,
        grace_seconds: int | None = None,
        reject_new_message: str | None = None,
    ):
        self.job_manager = job_manager
        env_grace = os.environ.get("MIGRATION_SHUTDOWN_GRACE_SECONDS", "")
        try:
            self.grace_seconds = (
                grace_seconds
                if grace_seconds is not None
                else int(env_grace or "60")
            )
        except (TypeError, ValueError):
            self.grace_seconds = 60
        self.reject_new_message = reject_new_message or "服务正在关闭，暂不接受新的迁移任务"
        self._stop_event = threading.Event()
        self._requested_at = 0.0

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def request_shutdown(self, signum: int | None = None) -> None:
        if self.stopping:
            return
        self._requested_at = time.time()
        self._stop_event.set()
        logging.warning(
            "[SHUTDOWN] 收到退出信号（%s），停止接收新任务，宽限期 %ss",
            signum,
            self.grace_seconds,
        )

    def install(self) -> None:
        for signum in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(
                    signum,
                    lambda _signum, _frame: self.request_shutdown(_signum),
                )
            except (ValueError, OSError):
                logging.warning("[SHUTDOWN] 无法注册信号 %s", signum)

    def _active_worker_names(self) -> str:
        workers = self.job_manager.active_worker_threads()
        if not workers:
            return ""
        return ", ".join(getattr(worker, "name", "?") for worker in workers)

    def wait_for_active_workers(self) -> list[threading.Thread]:
        """Wait until active volume copies finish (or grace expires)."""
        deadline = time.time() + max(1, self.grace_seconds)
        while time.time() < deadline:
            workers = self.job_manager.active_worker_threads()
            if not workers:
                return []
            time.sleep(1)
        logging.error(
            "[SHUTDOWN] 宽限期 %ss 已到，仍有任务未结束：%s；将强制退出，"
            "在跑卷可能未完成（下次运行会重新替换）。",
            self.grace_seconds,
            self._active_worker_names(),
        )
        return self.job_manager.active_worker_threads()

    def stop_flask(self, server: Any) -> None:
        """Stop the werkzeug dev server from another thread."""
        if server is None:
            return
        try:
            server.shutdown()
        except Exception as exc:  # noqa: BLE001
            logging.warning("[SHUTDOWN] 关闭 HTTP server 失败: %s", exc)

    def main_loop(
        self,
        server: Any,
        on_exit=None,
    ) -> None:
        """Block the main thread until a stop is requested, then drain.

        Returns normally only after active workers finished inside the grace
        window.  Callers should then return from ``__main__`` so the process
        exits.  When workers are still running we log and return anyway; the
        kubelet will SIGKILL after ``terminationGracePeriodSeconds``.
        """
        while not self._stop_event.wait(timeout=1.0):
            pass
        if self.job_manager.has_active_workers():
            logging.info("[SHUTDOWN] 等待在跑迁移任务收尾...")
            remaining = self.wait_for_active_workers()
            if remaining:
                logging.warning(
                    "[SHUTDOWN] %s 个任务未在宽限期内结束，将强制退出；"
                    "已使用暂存镜像+rename，目标卷不会被写坏，"
                    "未完成卷下次运行会重新替换。",
                    len(remaining),
                )
                self.stop_flask(server)
                if on_exit:
                    on_exit()
                import os

                os._exit(1)
        self.stop_flask(server)
        if on_exit:
            on_exit()

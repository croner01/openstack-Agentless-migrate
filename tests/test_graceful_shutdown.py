import threading
import time
import unittest

from graceful_shutdown import ShutdownCoordinator


class FakeJobManager:
    def __init__(self):
        self.workers: list[threading.Thread] = []

    def active_worker_threads(self):
        return [worker for worker in self.workers if worker.is_alive()]

    def has_active_workers(self):
        return bool(self.active_worker_threads())


class FakeServer:
    def __init__(self):
        self.shutdown_called = False

    def shutdown(self):
        self.shutdown_called = True


class ShutdownCoordinatorTest(unittest.TestCase):
    def test_request_shutdown_marks_stopping_and_rejects_new_work(self):
        coordinator = ShutdownCoordinator(FakeJobManager(), grace_seconds=1)
        self.assertFalse(coordinator.stopping)
        coordinator.request_shutdown()
        self.assertTrue(coordinator.stopping)

    def test_wait_returns_when_no_worker(self):
        coordinator = ShutdownCoordinator(FakeJobManager(), grace_seconds=1)
        coordinator.request_shutdown()
        self.assertEqual(coordinator.wait_for_active_workers(), [])

    def test_wait_waits_for_active_worker_to_finish(self):
        release = threading.Event()

        def worker():
            release.wait(timeout=5)

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        job_manager = FakeJobManager()
        job_manager.workers.append(thread)

        coordinator = ShutdownCoordinator(job_manager, grace_seconds=5)
        coordinator.request_shutdown()

        timer = threading.Timer(0.3, release.set)
        timer.start()
        remaining = coordinator.wait_for_active_workers()
        timer.join()
        self.assertEqual(remaining, [])
        self.assertFalse(thread.is_alive())

    def test_main_loop_stops_server_and_drains(self):
        job_manager = FakeJobManager()
        server = FakeServer()
        coordinator = ShutdownCoordinator(job_manager, grace_seconds=1)
        result = {}

        def run():
            coordinator.main_loop(server)
            result["done"] = True

        main = threading.Thread(target=run, daemon=True)
        main.start()
        time.sleep(0.1)
        self.assertFalse(result.get("done"))

        coordinator.request_shutdown()
        main.join(timeout=3)
        self.assertTrue(result.get("done"))
        self.assertTrue(server.shutdown_called)


if __name__ == "__main__":
    unittest.main()

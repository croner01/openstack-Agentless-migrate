"""RelayState 被多线程共享：领任务必须原子，不能抛异常也不能重复派发。"""
import threading
import unittest

from relay_registry import RelayState


class RelayStateConcurrencyTest(unittest.TestCase):
    def _state_with_agents(self, agent_count: int, task_count: int):
        state = RelayState(secret=b"s")
        agents = [
            state.register(
                job_id="job-1",
                role="source",
                name=f"relay-source-{index}",
                version="1.1.0",
                address="10.0.0.1",
                now=0.0,
            )
            for index in range(agent_count)
        ]
        for index in range(task_count):
            state.enqueue(
                {"task_id": f"task-{index}", "role": "source", "job_id": "job-1"}
            )
        return state, agents

    def test_concurrent_dispatch_never_raises_nor_double_assigns(self):
        state, agents = self._state_with_agents(agent_count=6, task_count=60)
        barrier = threading.Barrier(len(agents))
        dispatched: list[str] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def poll(agent):
            got: list[str] = []
            try:
                barrier.wait(timeout=5)
                for _ in range(400):
                    task = state.dispatch(agent.session_id)
                    if task is not None:
                        got.append(task["task_id"])
                        # 单槽 agent：拿到就立刻归还槽位，让它能继续领下一个。
                        state.complete_task(agent.session_id, task["task_id"])
            except BaseException as exc:  # noqa: BLE001 - 记录后断言
                errors.append(exc)
            with lock:
                dispatched.extend(got)

        threads = [threading.Thread(target=poll, args=(agent,)) for agent in agents]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(errors, [])
        # 关键：同一个任务不能派给两个 agent（队列 remove 的竞争）。
        self.assertEqual(len(dispatched), len(set(dispatched)))
        self.assertEqual(sorted(dispatched), sorted(f"task-{i}" for i in range(60)))

    def test_sweep_and_register_can_run_concurrently(self):
        """巡检遍历 agent 表时另一线程注册，不能触发 dict changed size。"""
        state = RelayState(secret=b"s")
        state.register(
            job_id="job-1", role="source", name="relay-source-0",
            version="1.1.0", address="10.0.0.1", now=0.0,
        )
        errors: list[BaseException] = []
        stop = threading.Event()

        def register_many():
            try:
                for index in range(300):
                    state.register(
                        job_id="job-1", role="source",
                        name=f"relay-source-{index + 1}", version="1.1.0",
                        address="10.0.0.2", now=0.0,
                    )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                stop.set()

        def sweep_many():
            try:
                while not stop.is_set():
                    state.sweep(now=1.0)
                    state.agents()
                    state.find_by_name("relay-source-0")
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            threading.Thread(target=register_many),
            threading.Thread(target=sweep_many),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()

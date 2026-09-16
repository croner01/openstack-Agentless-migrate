import unittest

from relay_registry import RelayState


class RelayStateSlotTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(secret=b"s" * 32)
        self.agent = self.state.register(
            job_id="",
            role="source",
            name="relay-source-1",
            version="1.0.0",
            address="10.0.0.5",
            now=1.0,
            slots_total=3,
        )

    def _enqueue(self, task_id, *, job_id=""):
        self.state.enqueue(
            {
                "task_id": task_id,
                "role": "source",
                "agent_id": self.agent.agent_id,
                "job_id": job_id,
            }
        )

    def test_dispatch_fills_all_slots_then_stops(self):
        dispatched = []
        for index in range(4):
            task_id = f"task-{index}"
            self._enqueue(task_id)
            task = self.state.dispatch(self.agent.session_id)
            dispatched.append(task["task_id"] if task else None)

        self.assertEqual(dispatched, ["task-0", "task-1", "task-2", None])

    def test_complete_task_frees_one_slot(self):
        self._enqueue("task-0")
        task = self.state.dispatch(self.agent.session_id)
        self._enqueue("task-1")

        self.state.complete_task(self.agent.session_id, task["task_id"])

        self.assertEqual(
            self.state.dispatch(self.agent.session_id)["task_id"], "task-1"
        )

    def test_cancel_is_per_task_not_per_agent(self):
        self._enqueue("task-0")
        self._enqueue("task-1")
        first = self.state.dispatch(self.agent.session_id)
        second = self.state.dispatch(self.agent.session_id)

        self.state.request_cancel(first["task_id"])

        self.assertEqual(
            self.state.cancels_for(self.agent.session_id), [first["task_id"]]
        )
        self.assertFalse(self.state.has_cancel(self.agent.session_id, second["task_id"]))

    def test_heartbeat_keeps_agent_busy_while_slots_remain(self):
        self._enqueue("task-0")
        self.state.dispatch(self.agent.session_id)

        record = self.state.heartbeat(self.agent.session_id, now=2.0)

        self.assertEqual(record.state, "busy")
        self.assertEqual(len(record.running_tasks), 1)

    def test_tasks_for_job_filters_by_job(self):
        self._enqueue("task-0", job_id="job-1")
        self._enqueue("task-1", job_id="job-2")

        tasks = self.state.tasks_for_job("job-1")

        self.assertEqual([task["task_id"] for task in tasks], ["task-0"])


class TaskPinningTest(unittest.TestCase):
    """任务绑定：生产环境里节点 id 与 agent_id 不同，必须能按节点名绑定。"""

    def setUp(self):
        self.state = RelayState(secret=b"s" * 32)
        self.agent = self.state.register(
            job_id="job-1",
            role="target",
            name="relay-target-0",
            version="1.0.0",
            address="10.0.0.5",
            now=1.0,
        )

    def test_task_pinned_by_name_is_dispatched_even_if_ids_differ(self):
        # 池里的 node_id 与注册产生的 agent_id 本来就不是同一个值
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "target",
                "job_id": "job-1",
                "agent_name": "relay-target-0",
            }
        )

        task = self.state.dispatch(self.agent.session_id)

        self.assertIsNotNone(task)
        self.assertEqual(task["task_id"], "t-1")

    def test_task_pinned_to_other_name_is_not_dispatched(self):
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "target",
                "agent_name": "relay-target-1",
            }
        )

        self.assertIsNone(self.state.dispatch(self.agent.session_id))

    def test_task_pinned_to_other_job_is_not_dispatched(self):
        """并发作业时同名节点（relay-source-0）不能互相领走对方的任务。"""
        self.state.enqueue(
            {"task_id": "t-1", "role": "target", "job_id": "job-2"}
        )

        self.assertIsNone(self.state.dispatch(self.agent.session_id))

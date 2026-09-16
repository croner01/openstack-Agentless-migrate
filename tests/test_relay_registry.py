import unittest

from relay_registry import RelayState


class RelayStateTest(unittest.TestCase):
    def setUp(self):
        self.state = RelayState(
            secret=b"secret", heartbeat_interval=10, heartbeat_timeout=30
        )

    def _register(self, role="source", name="relay-a", now=1000.0):
        return self.state.register(
            job_id="job-1",
            role=role,
            name=name,
            version="1.0.0",
            address="10.0.0.5",
            now=now,
        )

    def test_register_creates_ready_agent(self):
        agent = self._register()

        self.assertEqual(agent.state, "ready")
        self.assertTrue(agent.session_id)
        self.assertEqual(self.state.by_session(agent.session_id).name, "relay-a")

    def test_register_requires_supported_protocol_version(self):
        with self.assertRaises(ValueError):
            self.state.register(
                job_id="job-1",
                role="source",
                name="relay-a",
                version="0.9.0",
                address="10.0.0.5",
                now=1000.0,
            )

    def test_register_accepts_sparse_capable_agent(self):
        agent = self.state.register(
            job_id="job-1",
            role="source",
            name="relay-a",
            version="1.1.0",
            address="10.0.0.5",
            now=1000.0,
        )

        self.assertEqual(agent.version, "1.1.0")

    def test_heartbeat_updates_timestamp(self):
        agent = self._register()

        self.state.heartbeat(agent.session_id, now=1001.0)

        self.assertEqual(self.state.by_session(agent.session_id).last_heartbeat, 1001.0)

    def test_heartbeat_unknown_session_returns_none(self):
        self.assertIsNone(self.state.heartbeat("missing", now=1001.0))

    def test_sweep_marks_stale_agent_unhealthy(self):
        agent = self._register()

        changed = self.state.sweep(now=1100.0)

        self.assertEqual(changed, [agent.agent_id])
        self.assertEqual(self.state.by_session(agent.session_id).state, "unhealthy")

    def test_sweep_keeps_fresh_agent_ready(self):
        agent = self._register(now=1000.0)

        self.assertEqual(self.state.sweep(now=1010.0), [])
        self.assertEqual(self.state.by_session(agent.session_id).state, "ready")

    def test_dispatch_marks_agent_busy_and_returns_task(self):
        agent = self._register()
        self.state.enqueue({"task_id": "t-1", "role": "source"})

        task = self.state.dispatch(agent.session_id)

        self.assertEqual(task["task_id"], "t-1")
        self.assertEqual(self.state.by_session(agent.session_id).state, "busy")
        self.assertEqual(self.state.by_session(agent.session_id).current_task_id, "t-1")

    def test_dispatch_returns_none_when_queue_empty(self):
        agent = self._register()

        self.assertIsNone(self.state.dispatch(agent.session_id))
        self.assertEqual(self.state.by_session(agent.session_id).state, "ready")

    def test_dispatch_returns_none_for_mismatched_role(self):
        agent = self._register(role="target", name="relay-b")
        self.state.enqueue({"task_id": "t-1", "role": "source"})

        self.assertIsNone(self.state.dispatch(agent.session_id))

    def test_cancel_flag_is_set_and_cleared(self):
        agent = self._register()
        self.state.enqueue(
            {"task_id": "t-1", "role": "source", "agent_id": agent.agent_id}
        )
        self.state.dispatch(agent.session_id)

        self.assertFalse(self.state.has_cancel(agent.session_id))
        self.state.request_cancel("t-1")
        self.assertTrue(self.state.has_cancel(agent.session_id, "t-1"))
        self.assertEqual(self.state.cancels_for(agent.session_id), ["t-1"])
        self.state.complete_task(agent.session_id, "t-1")
        self.assertFalse(self.state.has_cancel(agent.session_id))

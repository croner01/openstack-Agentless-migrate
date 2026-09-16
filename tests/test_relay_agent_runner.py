import unittest

from relay_agent import AgentRunner


class AgentRunnerTest(unittest.TestCase):
    def test_register_retries_with_backoff_then_succeeds(self):
        attempts = []

        def register(name, slots_total):
            attempts.append(name)
            if len(attempts) < 3:
                raise OSError("connection refused")
            return "session-ok"

        slept = []
        runner = AgentRunner(
            platform_url="https://migrate.example.com",
            token="tok",
            name="relay-source-1",
            slots_total=5,
            register_fn=register,
            sleeper=slept.append,
        )

        session = runner.register()

        self.assertEqual(session, "session-ok")
        self.assertEqual(slept, [1.0, 2.0])

    def test_run_starts_one_worker_per_slot(self):
        calls = []
        runner = AgentRunner(
            platform_url="https://migrate.example.com",
            token="tok",
            name="relay-source-1",
            slots_total=3,
            register_fn=lambda name, slots_total: "session-1",
            heartbeat_fn=lambda session: {"cancel_tasks": []},
            worker_fn=lambda session, slot: calls.append(slot),
            sleeper=lambda seconds: None,
        )

        runner.run(max_heartbeats=1)

        # worker 会一直循环领任务，因此断言"三个槽位都启动过"而不是调用次数。
        self.assertEqual(set(calls), {0, 1, 2})

    def test_heartbeat_404_triggers_reregister(self):
        import urllib.error

        registrations = []

        def register(name, slots_total):
            registrations.append(name)
            return f"session-{len(registrations)}"

        def heartbeat(session):
            if session == "session-1":
                raise urllib.error.HTTPError("u", 404, "not found", {}, None)
            return {"cancel_tasks": []}

        runner = AgentRunner(
            platform_url="https://migrate.example.com",
            token="tok",
            name="relay-source-1",
            slots_total=1,
            register_fn=register,
            heartbeat_fn=heartbeat,
            worker_fn=lambda session, slot: None,
            sleeper=lambda seconds: None,
        )

        runner.run(max_heartbeats=2)

        self.assertEqual(len(registrations), 2)
        self.assertEqual(runner.session_id, "session-2")

import tempfile
import unittest
from pathlib import Path

from flask import Flask

from relay_api import create_blueprint
from relay_ledger import Ledger
from relay_protocol import issue_token
from relay_registry import RelayState


class RelayApiTest(unittest.TestCase):
    SECRET = b"api-test-secret"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state = RelayState(secret=self.SECRET)
        self.state.ledger = Ledger.load(Path(self.tmp.name) / "ledger.json")
        self.app = Flask(__name__)
        self.app.register_blueprint(create_blueprint(self.state))
        self.client = self.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def _token(self, role="source", job_id="job-1", ttl=300):
        return issue_token(self.SECRET, job_id, role, now=self.state.now(), ttl=ttl)

    def _register(self, role="source", name="relay-a"):
        return self.client.post(
            "/api/relay/register",
            json={
                "token": self._token(role),
                "name": name,
                "agent_version": "1.0.0",
            },
        )

    def test_register_returns_session_id(self):
        response = self._register()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["session_id"])

    def test_register_rejects_bad_token(self):
        response = self.client.post(
            "/api/relay/register",
            json={"token": "bogus", "name": "relay-a", "agent_version": "1.0.0"},
        )

        self.assertEqual(response.status_code, 401)

    def test_register_rejects_unsupported_version(self):
        response = self.client.post(
            "/api/relay/register",
            json={
                "token": self._token(),
                "name": "relay-a",
                "agent_version": "0.0.1",
            },
        )

        self.assertEqual(response.status_code, 409)

    def test_heartbeat_reports_no_cancel_by_default(self):
        session = self._register().get_json()["session_id"]

        response = self.client.post(
            "/api/relay/heartbeat", json={"session_id": session}
        )

        self.assertFalse(response.get_json()["cancel"])

    def test_heartbeat_reports_cancel_after_request(self):
        session = self._register().get_json()["session_id"]
        agent = self.state.by_session(session)
        self.state.enqueue(
            {"task_id": "t-cancel", "role": "source", "agent_id": agent.agent_id}
        )
        self.state.dispatch(session)
        self.state.request_cancel("t-cancel")

        response = self.client.post(
            "/api/relay/heartbeat", json={"session_id": session}
        )

        self.assertTrue(response.get_json()["cancel"])
        self.assertEqual(response.get_json()["cancel_tasks"], ["t-cancel"])

    def test_heartbeat_rejects_unknown_session(self):
        response = self.client.post(
            "/api/relay/heartbeat", json={"session_id": "nope"}
        )

        self.assertEqual(response.status_code, 404)

    def test_tasks_next_returns_204_when_queue_empty(self):
        session = self._register().get_json()["session_id"]

        response = self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.assertEqual(response.status_code, 204)

    def test_tasks_next_returns_enqueued_task(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue({"task_id": "t-1", "role": "source", "length": 1024})

        response = self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.assertEqual(response.get_json()["task_id"], "t-1")

    def test_progress_updates_ledger_copied_bytes(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "source",
                "length": 4096,
                "job_id": "job-1",
                "volume_id": "vol-1",
            }
        )
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        response = self.client.post(
            "/api/relay/tasks/t-1/progress",
            json={"session_id": session, "copied_bytes": 2048},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.state.ledger.get("job-1", "vol-1").copied_bytes, 2048)

    def test_progress_creates_missing_ledger_record(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "source",
                "job_id": "job-1",
                "vm_id": "vm-1",
                "volume_id": "vol-1",
            }
        )
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.client.post(
            "/api/relay/tasks/t-1/progress",
            json={"session_id": session, "copied_bytes": 512},
        )

        record = self.state.ledger.get("job-1", "vol-1")
        self.assertEqual(record.vm_id, "vm-1")
        self.assertEqual(record.copied_bytes, 512)

    def test_progress_reports_absolute_bytes_for_resumed_copy(self):
        """断点续传的 task.offset>0：台账要记绝对字节，否则进度和续传偏移都会错。"""
        session = self._register().get_json()["session_id"]
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "source",
                "job_id": "job-1",
                "volume_id": "vol-1",
                "offset": 1024,
                "length": 3072,
            }
        )
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.client.post(
            "/api/relay/tasks/t-1/progress",
            json={"session_id": session, "copied_bytes": 2048},
        )

        self.assertEqual(
            self.state.ledger.get("job-1", "vol-1").copied_bytes, 3072
        )

    def test_progress_records_skipped_bytes_with_base(self):
        """续传时 skipped 也要按 skipped_base 累加，保证 sent=copied-skipped 正确。"""
        session = self._register().get_json()["session_id"]
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "source",
                "job_id": "job-1",
                "volume_id": "vol-1",
                "offset": 1024,
                "length": 3072,
                "skipped_base": 512,
            }
        )
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.client.post(
            "/api/relay/tasks/t-1/progress",
            json={"session_id": session, "copied_bytes": 2048, "skipped_bytes": 1024},
        )

        record = self.state.ledger.get("job-1", "vol-1")
        self.assertEqual(record.copied_bytes, 3072)
        self.assertEqual(record.skipped_bytes, 1536)

    def test_result_records_skipped_bytes(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue(
            {
                "task_id": "t-1",
                "role": "target",
                "job_id": "job-1",
                "volume_id": "vol-1",
                "offset": 0,
                "length": 4096,
            }
        )
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        self.client.post(
            "/api/relay/tasks/t-1/result",
            json={
                "session_id": session,
                "status": "done",
                "copied_bytes": 4096,
                "skipped_bytes": 3072,
            },
        )

        self.assertEqual(
            self.state.ledger.get("job-1", "vol-1").skipped_bytes, 3072
        )

    def test_progress_rejects_unknown_task(self):
        session = self._register().get_json()["session_id"]

        response = self.client.post(
            "/api/relay/tasks/missing/progress",
            json={"session_id": session, "copied_bytes": 1},
        )

        self.assertEqual(response.status_code, 404)

    def test_result_marks_agent_ready_again(self):
        session = self._register().get_json()["session_id"]
        self.state.enqueue({"task_id": "t-1", "role": "source", "length": 4096})
        self.client.get(f"/api/relay/tasks/next?session_id={session}")

        response = self.client.post(
            "/api/relay/tasks/t-1/result",
            json={"session_id": session, "status": "done", "copied_bytes": 4096},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.state.by_session(session).state, "ready")

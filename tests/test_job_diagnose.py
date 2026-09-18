"""作业卡点诊断：一次给出阶段停留时长、并发名额、在途卷与相关日志。"""
import os
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import app as app_module
from excel_parser import MigrationRow
from relay_ledger import VolumeTaskRecord
from state_machine import JobStatus, VmStatus


def _row(name: str) -> MigrationRow:
    return MigrationRow(vm_name=name, target_az="az-1", mode="full")


class _FakeNode:
    def __init__(self, node_id: str, state: str):
        self.node_id = node_id
        self.state = state
        self.server_id = "srv-" + node_id
        self.current_task_id = ""


class _FakePool:
    def __init__(self, nodes):
        self.nodes = nodes


class _FakeLedger:
    def __init__(self, records):
        self._records = records

    def all(self):
        return list(self._records)


class _FakeRuntime:
    def __init__(self, records):
        self.source_pool = _FakePool([_FakeNode("src-1", "busy")])
        self.target_pool = _FakePool([_FakeNode("tgt-1", "unhealthy")])
        self.ledger = _FakeLedger(records)

    def ledger_summary(self):
        return {
            "total": len(self.ledger.all()),
            "in_flight": len(self.ledger.all()),
            "done": 0,
            "cleaned": 0,
            "failed": 0,
        }


class _FakeGate:
    """把 COPY_GATE 换成确定性数据，避免诊断结果随环境变量抖动。"""

    max_active_copies = 2
    active_copies = 2
    high_water = 16384
    reserve_bytes = 4096 * 1024 * 1024

    def holders(self):
        return [("job-a/vm-1#0", 322.0), ("job-a/vm-2#0", 12.0)]


class JobDiagnoseApiTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        self.tmp = tempfile.TemporaryDirectory()
        self.log_path = os.path.join(self.tmp.name, "vm.log")
        with open(self.log_path, "w", encoding="utf-8") as handle:
            handle.write("[MIGRATION] 无关日志\n")
            handle.write(
                "[MIGRATION] 等待 RBD 拷贝名额/内存余量: active=2 current=113MiB "
                "limit=16384MiB 已等待 322s\n"
            )
            handle.write("[MIGRATION] VM vm-1 阶段 -> copying_volumes (copying_volumes)\n")
        app_module.job_manager.delete("diag-job")

    def tearDown(self):
        app_module.job_manager.delete("diag-job")
        self.tmp.cleanup()

    def _seed(self) -> None:
        job = app_module.job_manager.create_job([_row("vm-1")], job_id="diag-job")
        vm = job.vms[0]
        vm.status = VmStatus.COPYING_VOLUMES
        vm.phase = "copying_volumes"
        vm.phase_since = (
            datetime.now(timezone.utc) - timedelta(minutes=11)
        ).isoformat()
        vm.relay_disks = [
            {"volume_id": "vol-1", "status": "success"},
            {"volume_id": "vol-2", "status": "copying"},
        ]

    def test_unknown_job_returns_404(self):
        resp = self.client.get("/api/jobs/does-not-exist/diagnose")

        self.assertEqual(resp.status_code, 404)
        self.assertFalse(resp.get_json()["ok"])

    def test_reports_phase_age_disks_and_actionable_hints(self):
        self._seed()
        record = VolumeTaskRecord(
            job_id="diag-job",
            vm_id="vm-1",
            volume_id="vol-2",
            phase="cloning",
            updated_at=time.time() - 900,
        )
        with (
            mock.patch.object(app_module, "get_runtime", return_value=_FakeRuntime([record])),
            mock.patch.object(app_module, "COPY_GATE", _FakeGate()),
            mock.patch.object(app_module, "LOG_FILE", self.log_path),
        ):
            payload = self.client.get("/api/jobs/diag-job/diagnose").get_json()

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["job"]["id"], "diag-job")
        self.assertEqual(payload["job"]["status"], JobStatus.RUNNING.value)

        vm = payload["vms"][0]
        self.assertEqual(vm["phase"], "copying_volumes")
        self.assertGreaterEqual(vm["phase_seconds"], 600)
        self.assertEqual(vm["disks"]["total"], 2)
        self.assertEqual(vm["disks"]["done"], 1)
        self.assertEqual(vm["disks"]["in_flight"], 1)
        self.assertEqual(vm["disks"]["channel"], "relay")

        gate = payload["copy_gate"]
        self.assertEqual(gate["active"], 2)
        self.assertEqual(gate["concurrency"], 2)
        self.assertEqual(gate["holders"][0]["owner"], "job-a/vm-1#0")

        relay = payload["relay"]
        self.assertEqual(relay["volumes"][0]["volume_id"], "vol-2")
        self.assertEqual(relay["volumes"][0]["phase_label"], "派生中转卷")
        self.assertGreaterEqual(relay["volumes"][0]["waited_seconds"], 850)

        hints = "\n".join(payload["hints"])
        self.assertIn("拷贝卷", hints)
        self.assertIn("等待 RBD 拷贝名额", hints)
        self.assertIn("名额已满 2/2", hints)
        self.assertIn("派生中转卷", hints)
        self.assertIn("tgt-1", hints)

        self.assertEqual(payload["log_file"], self.log_path)
        # 与本作业相关的行按 job id / VM 名过滤；排队这类通用日志只出现在最近行里。
        self.assertTrue(any("阶段 -> copying_volumes" in line for line in payload["log_lines"]))
        self.assertFalse(any("无关日志" in line for line in payload["log_lines"]))
        self.assertTrue(any("等待 RBD 拷贝名额" in line for line in payload["log_recent"]))

    def test_finished_job_reports_no_need_to_wait(self):
        job = app_module.job_manager.create_job([_row("vm-1")], job_id="diag-job")
        job.vms[0].mark_success()
        job.refresh_status()

        with (
            mock.patch.object(app_module, "get_runtime", return_value=None),
            mock.patch.object(app_module, "LOG_FILE", self.log_path),
        ):
            payload = self.client.get("/api/jobs/diag-job/diagnose").get_json()

        self.assertIsNone(payload["relay"])
        self.assertTrue(any("无需继续等待" in hint for hint in payload["hints"]))


class JobDiagnoseRenderTest(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()

    def _html(self) -> str:
        return self.client.get("/").get_data(as_text=True)

    def test_job_detail_has_diagnose_entry(self):
        html = self._html()

        self.assertIn('id="job-diag-btn"', html)
        self.assertIn('id="job-diag-result"', html)
        self.assertIn("$('#job-diag-btn').addEventListener('click', jobDiagnose)", html)

    def test_diagnose_calls_readonly_endpoint(self):
        html = self._html()

        self.assertIn("/api/jobs/${state.jobId}/diagnose", html)
        self.assertIn("function renderJobDiagnose(data)", html)
        self.assertIn("data.hints", html)
        self.assertIn("data.relay.volumes", html)

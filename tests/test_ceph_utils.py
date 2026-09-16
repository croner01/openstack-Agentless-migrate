import json
import logging
import os
import subprocess
import tempfile
import time
import unittest
from io import BytesIO
from unittest import mock

import ceph_utils
from ceph_utils import (
    any_mon_reachable,
    CephUtils,
    IncrementalSession,
    LayoutMismatchError,
    StageImageMissingError,
    ValidationError,
    assert_layout_matches,
    describe_process_error,
    is_missing_image_error,
    build_import_args,
    layout_mismatches,
    parse_layout,
    parse_mon_endpoints,
    parse_rbd_info_json,
    pump_stream,
    watchers_present,
    rss_mb_from_status,
)


class CompletedFake:
    def __init__(self, stdout: str = ""):
        self.stdout = stdout


class FakeRunner:
    """Records commands and returns sensible stdout for each rbd verb.

    Scripted values may be supplied per parsed call (info/status/snap); when
    absent, sensible defaults are returned so tests focus on the call
    sequence rather than script alignment.
    """

    def __init__(
        self,
        stdout_by_index=None,
        raise_on_command=None,
        status_watchers: list | None = None,
        snap_list: list | None = None,
        info_by_spec: dict | None = None,
        missing_once: list | None = None,
        snaps_by_image: dict | None = None,
        diff_output: str | None = None,
    ):
        self.stdout_by_index = list(stdout_by_index or [])
        self.raise_on_command = raise_on_command or []
        self.status_watchers = status_watchers if status_watchers is not None else []
        self.snap_list = snap_list if snap_list is not None else []
        self.info_by_spec = dict(info_by_spec or {})
        self.missing_once = list(missing_once or [])
        self.snaps_by_image = dict(snaps_by_image or {})
        self.diff_output = diff_output
        self.calls = []

    @staticmethod
    def _match(text: str, table: dict):
        # 长键优先，"volume-dst-1-mig-stage" 必须先于 "volume-dst-1" 命中
        for key in sorted(table, key=len, reverse=True):
            if key and key in text:
                return table[key]
        return None

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        command_text = command if isinstance(command, str) else " ".join(command)
        if any(tag in command_text for tag in self.raise_on_command):
            import subprocess

            raise subprocess.CalledProcessError(2, command)
        for spec in list(self.missing_once):
            if spec in command_text:
                self.missing_once.remove(spec)
                import subprocess

                raise subprocess.CalledProcessError(2, command)
        if "info --format json" in command_text:
            scripted = self._match(command_text, self.info_by_spec)
            if scripted is not None:
                return CompletedFake(scripted)
            if self.stdout_by_index:
                return CompletedFake(self.stdout_by_index.pop(0))
            return CompletedFake(json.dumps({"size": 10}))
        if "status --format json" in command_text:
            return CompletedFake(json.dumps({"watchers": self.status_watchers}))
        if "snap ls" in command_text:
            scripted = self._match(command_text, self.snaps_by_image)
            if scripted is not None:
                return CompletedFake(json.dumps(scripted))
            return CompletedFake(json.dumps(self.snap_list))
        if "diff --from-snap" in command_text:
            return CompletedFake(self.diff_output or "[]")
        # 让 snap create / snap purge 真实影响后续 snap ls 的结果，
        # 这样测试能覆盖"基线快照建立后才能 import-diff"的完整链路。
        if "snap create" in command_text:
            self._apply_snap_change(command_text.split()[-1], "create")
            return CompletedFake("")
        if "snap rm" in command_text:
            self._apply_snap_change(command_text.split()[-1], "rm")
            return CompletedFake("")
        if "snap purge" in command_text:
            self._apply_snap_change(command_text.split()[-1], "purge")
            return CompletedFake("")
        return CompletedFake("")

    def _apply_snap_change(self, spec: str, mode: str) -> None:
        """模拟 snap create / rm / purge 对后续 snap ls 结果的影响。"""
        image = spec.partition("@")[0]
        name = spec.partition("@")[2]
        key = next(
            (
                k
                for k in sorted(self.snaps_by_image, key=len, reverse=True)
                if k in image
            ),
            None,
        )
        if key is None:
            return
        snaps = list(self.snaps_by_image[key])
        if mode == "create":
            if name and all(entry.get("name") != name for entry in snaps):
                snaps.append({"id": len(snaps) + 1, "name": name})
        elif mode == "rm":
            snaps = [entry for entry in snaps if entry.get("name") != name]
        elif mode == "purge":
            snaps = []
        self.snaps_by_image[key] = snaps

    def fake_export_import(self, export_cmd, import_cmd, **kwargs):
        """Stand-in for CephUtils._export_import used by replace_rbd_data."""
        self.calls.append(export_cmd)
        self.calls.append(import_cmd)
        if "import-diff" not in import_cmd:
            return
        # import-diff 成功后会按 diff 头在目标卷上建出结束快照，
        # 模拟这一行为才能覆盖多轮增量。
        to_spec = next((part for part in export_cmd if "@" in part), "")
        if not to_spec:
            return
        self._apply_snap_change(
            f"{import_cmd[-1]}@{to_spec.split('@', 1)[1]}", "create"
        )


class _FakePipe:
    def __init__(self, data: bytes = b""):
        self._data = data

    def read(self, _size: int = -1) -> bytes:
        return self._data

    def close(self) -> None:
        pass


class _FakeProc:
    """Minimal Popen stand-in for _export_import failure-path tests."""

    def __init__(self, rc: int, stderr_text: str = "", stdout=None):
        self.rc = rc
        self.stderr_text = stderr_text
        self.stdout = stdout
        self.stdin = _FakePipe()
        self.pid = 4321
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True

    def wait(self) -> int:
        return self.rc


class ImageProbeSemanticsTest(unittest.TestCase):
    """`rbd info` 失败不能一律当成"映像不存在"。"""

    @staticmethod
    def _ceph(runner):
        return CephUtils(
            source_conf="s.conf",
            source_pool="src",
            target_conf="t.conf",
            target_pool="volumes",
            runner=runner,
        )

    @staticmethod
    def _raiser(returncode: int, stderr: str):
        def runner(command, **kwargs):
            raise subprocess.CalledProcessError(
                returncode, command, stderr=stderr
            )

        return runner

    def test_missing_image_reports_false(self):
        ceph = self._ceph(
            self._raiser(
                2,
                "rbd: error opening image foo: (2) No such file or directory",
            )
        )
        self.assertFalse(ceph._image_exists("t.conf", "volumes", "foo"))

    def test_transient_probe_failure_is_raised_not_swallowed(self):
        # rc=110 = ETIMEDOUT：集群慢，不代表卷不存在。
        ceph = self._ceph(self._raiser(110, "timed out"))
        with self.assertRaises(subprocess.CalledProcessError):
            ceph._image_exists("t.conf", "volumes", "foo")

    def test_snapshot_preflight_is_advisory_when_probe_times_out(self):
        # 预检只是为了让报错更清楚，不能变成新的失败点：放行 import-diff。
        ceph = self._ceph(self._raiser(110, "timed out"))
        ceph._ensure_target_snapshot("volume-t1-mig-stage", "mig-j1-0")

    def test_snapshot_preflight_still_reports_missing_image(self):
        ceph = self._ceph(self._raiser(2, "No such file or directory"))
        with self.assertRaises(ValueError) as ctx:
            ceph._ensure_target_snapshot("volume-t1-mig-stage", "mig-j1-0")
        self.assertIn("不存在", str(ctx.exception))

    def test_snapshot_preflight_flags_recycled_stage_image(self):
        """暂存卷被回收后必须报出可识别的类型，编排层才能就地重做基线。

        线上表现是 `目标暂存卷 ... 不存在，无法应用增量 diff` 之后 VM 直接失败，
        用户只能整台重来；这里把它标成 StageImageMissingError 以便自动补救。
        """
        ceph = self._ceph(self._raiser(2, "No such file or directory"))
        with self.assertRaises(StageImageMissingError) as ctx:
            ceph._ensure_target_snapshot("volume-t1-mig-stage", "mig-j1-0")
        self.assertIsInstance(ctx.exception, ValueError)
        self.assertIn("需要重做基线全备", str(ctx.exception))

    def test_snapshot_preflight_still_reports_missing_snapshot(self):
        def runner(command, **kwargs):
            text = " ".join(command)
            if "snap ls" in text:
                return CompletedFake(json.dumps([{"name": "mig-j1-9"}]))
            return CompletedFake(json.dumps({"size": 1024, "order": 22}))

        ceph = self._ceph(runner)
        with self.assertRaises(ValueError) as ctx:
            ceph._ensure_target_snapshot("volume-t1-mig-stage", "mig-j1-0")
        self.assertIn("缺少起始快照", str(ctx.exception))

    def test_is_missing_image_error_matches_only_not_found(self):
        self.assertTrue(
            is_missing_image_error(
                subprocess.CalledProcessError(2, ["rbd"], stderr="")
            )
        )
        self.assertTrue(
            is_missing_image_error(
                subprocess.CalledProcessError(
                    1, ["rbd"], stderr="rbd: error opening image x: (2) No such file or directory"
                )
            )
        )
        self.assertFalse(
            is_missing_image_error(
                subprocess.CalledProcessError(110, ["rbd"], stderr="timed out")
            )
        )

    def test_parse_mon_endpoints_handles_common_forms(self):
        """mon_host 的几种写法都要认得，解析不出来时返回空列表让调用方走原逻辑。"""
        self.assertEqual(
            parse_mon_endpoints("mon_host = 10.6.10.4"), [("10.6.10.4", 6789)]
        )
        self.assertEqual(
            parse_mon_endpoints("mon host = 1.2.3.4,5.6.7.8"),
            [("1.2.3.4", 6789), ("5.6.7.8", 6789)],
        )
        self.assertEqual(
            parse_mon_endpoints(
                "mon_host = [v2:1.2.3.4:3300/0,v1:1.2.3.4:6789/0]"
            ),
            [("1.2.3.4", 3300), ("1.2.3.4", 6789)],
        )
        self.assertEqual(
            parse_mon_endpoints("mon_host = 10.0.0.1:3300\nmon_host = 10.0.0.2"),
            [("10.0.0.1", 3300), ("10.0.0.2", 6789)],
        )
        # 注释行与没有 mon 的 conf 都不该被当成地址。
        self.assertEqual(parse_mon_endpoints("# mon_host = 9.9.9.9"), [])
        self.assertEqual(parse_mon_endpoints("[global]\nfsid = abc"), [])

    def test_parse_mon_endpoints_reads_conf_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ceph.conf")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("[global]\nmon_host = 100.100.13.21\n")

            self.assertEqual(parse_mon_endpoints(path), [("100.100.13.21", 6789)])

    def test_any_mon_reachable_stops_at_first_success(self):
        with mock.patch.object(
            ceph_utils.socket,
            "create_connection",
            return_value=mock.MagicMock(),
        ) as patched:
            self.assertTrue(
                any_mon_reachable([("10.6.10.4", 6789), ("10.6.10.5", 6789)])
            )

        patched.assert_called_once_with(("10.6.10.4", 6789), timeout=mock.ANY)

    def test_any_mon_reachable_reports_unreachable_cluster(self):
        with mock.patch.object(
            ceph_utils.socket,
            "create_connection",
            side_effect=OSError("timed out"),
        ):
            self.assertFalse(any_mon_reachable([("100.100.13.21", 6789)]))
        self.assertFalse(any_mon_reachable([]))

    def test_describe_process_error_includes_stderr(self):
        exc = subprocess.CalledProcessError(
            16, ["rbd", "rm"], stderr="rbd: error: image has watchers\n"
        )
        detail = describe_process_error(exc)
        self.assertIn("rc=16", detail)
        self.assertIn("image has watchers", detail)


class ExportImportAttributionTest(unittest.TestCase):
    """export/import 同时非 0 时必须报出真正先失败的那一端。

    回归：import 先退出会让 export 收到本进程的 SIGTERM(rc=-15)，旧实现先判
    export，于是日志里只剩 "rbd export 失败 rc=-15 stderr="，真实原因
    （import 的 stderr）被丢弃。
    """

    def setUp(self):
        self.utils = CephUtils("s.conf", "sp", "t.conf", "tp")
        self.export_cmd = ["rbd", "export-diff", "sp/x@snap", "-"]
        self.import_cmd = ["rbd", "import-diff", "-", "tp/x"]

    def _run(self, *, exporter, importer, pump):
        def fake_popen(command, **kwargs):
            stderr_file = kwargs.get("stderr")
            proc = importer if "import-diff" in command else exporter
            if stderr_file is not None and proc.stderr_text:
                stderr_file.write(proc.stderr_text)
                stderr_file.flush()
            return proc

        with mock.patch("ceph_utils.subprocess.Popen", side_effect=fake_popen), \
                mock.patch("ceph_utils.pump_stream", side_effect=pump):
            return self.utils._export_import(self.export_cmd, self.import_cmd)

    def test_broken_pipe_reports_import_as_root_cause(self):
        exporter = _FakeProc(-15, stdout=_FakePipe(b"x"))
        importer = _FakeProc(
            2, stderr_text="rbd: import-diff failed: (22) Invalid argument"
        )

        with self.assertRaises(subprocess.CalledProcessError) as ctx:
            self._run(
                exporter=exporter,
                importer=importer,
                pump=BrokenPipeError("broken pipe"),
            )

        self.assertEqual(ctx.exception.cmd, self.import_cmd)
        self.assertEqual(ctx.exception.returncode, 2)
        self.assertIn("Invalid argument", ctx.exception.stderr)
        self.assertTrue(exporter.terminated)

    def test_clean_export_failure_still_reports_export(self):
        """export 自己失败（stdout 正常 EOF）时不能被 import 的连带失败盖掉。"""
        exporter = _FakeProc(2, stderr_text="rbd: error opening snapshot mig-1")
        importer = _FakeProc(1, stderr_text="rbd: failed to decode diff banner")

        with self.assertRaises(subprocess.CalledProcessError) as ctx:
            self._run(exporter=exporter, importer=importer, pump=lambda *a, **k: 0)

        self.assertEqual(ctx.exception.cmd, self.export_cmd)
        self.assertIn("error opening snapshot", ctx.exception.stderr)

    def test_success_returns_copied_bytes(self):
        exporter = _FakeProc(0, stdout=_FakePipe(b"x"))
        importer = _FakeProc(0)

        copied = self._run(
            exporter=exporter, importer=importer, pump=lambda *a, **k: 7
        )

        self.assertEqual(copied, 7)


class ParserTest(unittest.TestCase):
    def test_parse_rbd_info_json_reads_size(self):
        parsed = parse_rbd_info_json(json.dumps({"size": 10737418240}))
        self.assertEqual(parsed["size"], 10737418240)

    def test_watchers_present_detects_entries(self):
        self.assertFalse(watchers_present({"watchers": []}))
        self.assertTrue(
            watchers_present(
                {"watchers": [{"watcher": "1.2.3.4:0/1 client.1 cookie=1"}]}
            )
        )


class StreamPumpTest(unittest.TestCase):
    def test_progress_callback_reports_copied_bytes_and_rate(self):
        payload = b"x" * (4 * 256 * 1024)
        events = []

        def on_progress(copied, total, bytes_per_sec):
            events.append((copied, total, bytes_per_sec))

        pump_stream(
            BytesIO(payload),
            BytesIO(),
            total_bytes=len(payload),
            progress_cb=on_progress,
        )
        self.assertGreaterEqual(events[-1][0], len(payload))
        self.assertEqual(events[-1][1], len(payload))
        self.assertGreater(events[-1][2], 0)

    def test_rate_limit_throttles_bytes_per_second(self):
        payload = b"a" * (1024 * 1024)
        started = time.monotonic()
        pump_stream(
            BytesIO(payload),
            BytesIO(),
            total_bytes=len(payload),
            rate_limit_bytes_per_sec=1024 * 1024,
        )
        self.assertGreaterEqual(time.monotonic() - started, 0.65)


class ProcStatusTest(unittest.TestCase):
    def test_rss_parser_reads_megabytes(self):
        status = "Name:\trbd\nVmPeak:\t1048576 kB\nVmRSS:\t524288 kB\n"
        self.assertEqual(rss_mb_from_status(status), 512)

    def test_rss_parser_returns_none_without_vmrss(self):
        self.assertIsNone(rss_mb_from_status("Name:\tpython\n"))


class ReplaceRbdTest(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.utils = CephUtils(
            source_conf="/src/ceph.conf",
            source_pool="volumes",
            target_conf="/dst/ceph.conf",
            target_pool="volumes",
        )

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _size_json(self, size: int) -> str:
        return json.dumps({"size": size})

    def test_validation_rejects_unsafe_name(self):
        with self.assertRaises(ValidationError):
            CephUtils("/a", "rbd", "/b", "rbd").replace_rbd_data(
                "x; rm -rf /", "y"
            )

    def test_successful_replace_imports_stage_then_renames(self):
        # 调用顺序:
        # info(src) info(dst) exists(stage) rm(stage)
        # export/import(stage) info(stage) status(dst) snap(dst)
        # rm(dst) rename(stage->dst) info(dst)
        runner = FakeRunner()
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertTrue(
            self.utils.replace_rbd_data("volume-src-1", "volume-dst-1")
        )
        self.assertTrue(
            any(
                isinstance(call, list)
                and "rename" in call
                and "volume-dst-1-mig-stage" in str(call)
                for call in runner.calls
            )
        )
        self.assertTrue(
            any(
                isinstance(call, list)
                and "import" in call
                and "volume-dst-1-mig-stage" in str(call)
                for call in runner.calls
            )
        )

    def test_pipeline_does_not_depend_on_bash_pipefail(self):
        # 验证导出与导入命令都被发出，而不是依赖 bash 的 pipefail
        runner = FakeRunner()
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertTrue(
            self.utils.replace_rbd_data("volume-src-1", "volume-dst-1")
        )
        combined = " ".join(str(call) for call in runner.calls)
        self.assertNotIn("pipefail", combined)
        self.assertIn("volume-src-1", combined)
        self.assertIn("volume-dst-1-mig-stage", combined)

    def test_interrupted_stage_is_cleaned_before_retry(self):
        # 上个周期中断留下 stage：info(stage) 返回成功，须先 rm(stage) 再导入
        runner = FakeRunner()
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertTrue(
            self.utils.replace_rbd_data("volume-src-1", "volume-dst-1")
        )
        self.assertTrue(
            any(
                isinstance(call, list)
                and "rm" in call
                and "volume-dst-1-mig-stage" in " ".join(call)
                for call in runner.calls
            )
        )

    def test_size_mismatch_aborts_before_any_mutation(self):
        runner = FakeRunner(
            [
                self._size_json(20),
                self._size_json(10),
            ]
        )
        self.utils._runner = runner
        self.assertFalse(
            self.utils.replace_rbd_data("volume-src-1", "volume-dst-1")
        )
        self.assertEqual(len(runner.calls), 2)

    def test_replace_passes_total_bytes_progress_and_rate_to_export_import(self):
        class RecordingExporter:
            def __init__(self):
                self.kwargs = None

            def __call__(self, export_cmd, import_cmd, **kwargs):
                self.kwargs = kwargs

        exporter = RecordingExporter()
        runner = FakeRunner()
        self.utils._runner = runner
        self.utils._export_import = exporter
        callback = lambda copied, total, rate: None  # noqa: E731
        self.assertTrue(
            self.utils.replace_rbd_data(
                "volume-src-1",
                "volume-dst-1",
                progress_cb=callback,
                rate_limit_bytes_per_sec=8 * 1024 * 1024,
            )
        )
        self.assertEqual(exporter.kwargs["total_bytes"], 10)
        self.assertIs(exporter.kwargs["progress_cb"], callback)
        self.assertEqual(
            exporter.kwargs["rate_limit_bytes_per_sec"], 8 * 1024 * 1024
        )

    def test_watcher_prevents_swap(self):
        runner = FakeRunner(status_watchers=[{"watcher": "x"}])
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertFalse(
            self.utils.replace_rbd_data("volume-src-1", "volume-dst-1")
        )
        self.assertFalse(
            any("rename" in str(call) for call in runner.calls)
        )

    def test_snapshot_prevents_swap(self):
        runner = FakeRunner(snap_list=[{"id": "1", "name": "snap1"}])
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertFalse(
            self.utils.replace_rbd_data("volume-src-1", "volume-dst-1")
        )
        self.assertFalse(
            any("rename" in str(call) for call in runner.calls)
        )


class LayoutCompareTest(unittest.TestCase):
    def test_features_accepts_string_and_list(self):
        as_list = parse_layout({"features": ["fast-diff", "layering"]})
        as_str = parse_layout({"features": "layering, fast-diff"})
        self.assertEqual(as_list["features"], ["fast-diff", "layering"])
        self.assertEqual(as_str["features"], ["fast-diff", "layering"])

    def test_identical_layout_has_no_mismatch(self):
        info = {
            "size": 100,
            "order": 20,
            "stripe_unit": 4096,
            "stripe_count": 2,
            "features": ["layering"],
        }
        self.assertEqual(layout_mismatches(info, dict(info)), [])

    def test_order_difference_is_reported(self):
        source = {"size": 100, "order": 20}
        target = {"size": 100, "order": 22}
        self.assertEqual(
            layout_mismatches(source, target), ["order: source=20 target=22"]
        )

    def test_size_difference_is_reported(self):
        self.assertEqual(
            layout_mismatches({"size": 100}, {"size": 200}),
            ["size: source=100 target=200"],
        )

    def test_target_missing_source_feature_is_reported(self):
        source = {"size": 100, "features": ["layering", "exclusive-lock"]}
        target = {"size": 100, "features": ["layering"]}
        self.assertEqual(
            layout_mismatches(source, target),
            ["features 缺失: exclusive-lock"],
        )

    def test_target_extra_feature_is_accepted(self):
        source = {"size": 100, "features": ["layering"]}
        target = {"size": 100, "features": ["layering", "fast-diff"]}
        self.assertEqual(layout_mismatches(source, target), [])

    def test_non_layout_feature_missing_is_tolerated(self):
        """源集群较新时带上 operations（op_features 触发），目标端复制不过来。"""
        source = {"size": 100, "features": ["layering", "operations"]}
        target = {"size": 100, "features": ["layering"]}
        with self.assertLogs(level="INFO") as captured:
            self.assertEqual(layout_mismatches(source, target), [])
        self.assertIn("operations", "\n".join(captured.output))
        assert_layout_matches(source, target)


class BuildImportArgsTest(unittest.TestCase):
    def test_carries_order_and_striping(self):
        args = build_import_args(
            {"order": 20, "stripe_unit": 4096, "stripe_count": 2, "features": []}
        )
        self.assertEqual(args[args.index("--order") + 1], "20")
        self.assertEqual(args[args.index("--stripe-unit") + 1], "4096")
        self.assertEqual(args[args.index("--stripe-count") + 1], "2")

    def test_adds_cinder_required_features_to_source_features(self):
        args = build_import_args({"features": ["object-map"]})
        features = args[args.index("--image-feature") + 1].split(",")
        self.assertIn("object-map", features)
        self.assertIn("layering", features)
        self.assertIn("exclusive-lock", features)

    def test_omits_unset_layout_flags_but_keeps_required_features(self):
        args = build_import_args({"order": None, "features": []})
        self.assertNotIn("--order", args)
        self.assertNotIn("--stripe-unit", args)
        self.assertNotIn("--stripe-count", args)
        self.assertEqual(args, ["--image-feature", "exclusive-lock,layering"])


class FullPathLayoutAlignmentTest(ReplaceRbdTest):
    def test_import_command_carries_source_layout(self):
        source = json.dumps({"size": 10, "order": 20, "features": ["layering"]})
        runner = FakeRunner(
            info_by_spec={"volume-src-1": source, "volume-dst-1-mig-stage": source},
            missing_once=["volume-dst-1-mig-stage"],
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertTrue(self.utils.replace_rbd_data("volume-src-1", "volume-dst-1"))
        import_cmd = next(
            call
            for call in runner.calls
            if isinstance(call, list) and "import" in call
        )
        self.assertIn("--order", import_cmd)
        self.assertEqual(import_cmd[import_cmd.index("--order") + 1], "20")

    def test_stage_layout_mismatch_aborts_swap(self):
        runner = FakeRunner(
            info_by_spec={
                "volume-src-1": json.dumps({"size": 10, "order": 20}),
                "volume-dst-1-mig-stage": json.dumps({"size": 10, "order": 22}),
            },
            missing_once=["volume-dst-1-mig-stage"],
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertFalse(self.utils.replace_rbd_data("volume-src-1", "volume-dst-1"))
        self.assertFalse(
            any(isinstance(call, list) and "rename" in call for call in runner.calls)
        )

    def test_align_layout_false_keeps_legacy_behaviour(self):
        runner = FakeRunner(
            info_by_spec={
                "volume-src-1": json.dumps({"size": 10, "order": 20}),
                "volume-dst-1-mig-stage": json.dumps({"size": 10, "order": 22}),
            },
            missing_once=["volume-dst-1-mig-stage"],
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertTrue(
            self.utils.replace_rbd_data(
                "volume-src-1", "volume-dst-1", align_layout=False
            )
        )


class SnapshotPrimitiveTest(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.utils = CephUtils(
            "/src/ceph.conf", "volumes", "/dst/ceph.conf", "volumes"
        )

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _runner(self, snaps=None, stage_snaps=None):
        runner = FakeRunner(
            snaps_by_image={
                "volume-src-1": snaps or [],
                "volume-dst-1-mig-stage": stage_snaps or [],
            }
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        return runner

    def test_create_snapshot_uses_source_conf_and_snap_spec(self):
        runner = self._runner()
        self.utils.create_source_snapshot("volume-src-1", "mig-j1-0")
        self.assertEqual(
            runner.calls[0],
            [
                "rbd",
                "--conf",
                "/src/ceph.conf",
                "snap",
                "create",
                "volumes/volume-src-1@mig-j1-0",
            ],
        )

    def test_list_source_snapshots_returns_names(self):
        self._runner(
            [{"id": 1, "name": "mig-j1-0"}, {"id": 2, "name": "daily"}]
        )
        self.assertEqual(
            self.utils.list_source_snapshots("volume-src-1"),
            ["mig-j1-0", "daily"],
        )

    def test_remove_snapshot_uses_source_conf(self):
        runner = self._runner()
        self.utils.remove_source_snapshot("volume-src-1", "mig-j1-0")
        self.assertEqual(
            runner.calls[0],
            [
                "rbd",
                "--conf",
                "/src/ceph.conf",
                "snap",
                "rm",
                "volumes/volume-src-1@mig-j1-0",
            ],
        )

    def test_export_import_diff_uses_from_snap_and_import_diff(self):
        runner = self._runner(stage_snaps=[{"id": 1, "name": "mig-j1-0"}])
        self.utils.export_import_diff(
            "volume-src-1",
            "volume-dst-1-mig-stage",
            from_snap="mig-j1-0",
            to_snap="mig-j1-1",
        )
        export_cmd, import_cmd = runner.calls[-2], runner.calls[-1]
        self.assertIn("export-diff", export_cmd)
        self.assertEqual(
            export_cmd[export_cmd.index("--from-snap") + 1], "mig-j1-0"
        )
        self.assertIn("volumes/volume-src-1@mig-j1-1", export_cmd)
        self.assertIn("import-diff", import_cmd)
        self.assertIn("volumes/volume-dst-1-mig-stage", import_cmd)

    def test_export_import_diff_rejects_stage_without_base_snapshot(self):
        # 回归：暂存卷由全量 rbd import 建立时没有任何快照，import-diff 会以
        # "start snapshot ... does not exist" 立即失败，必须在发起拷贝前拦下。
        runner = self._runner()
        with self.assertRaises(ValueError) as ctx:
            self.utils.export_import_diff(
                "volume-src-1",
                "volume-dst-1-mig-stage",
                from_snap="mig-j1-0",
                to_snap="mig-j1-1",
            )
        self.assertIn("缺少起始快照 mig-j1-0", str(ctx.exception))
        flat = " ".join(
            " ".join(call) if isinstance(call, list) else str(call)
            for call in runner.calls
        )
        self.assertNotIn("import-diff", flat)

    def test_export_import_diff_probes_round_total_for_progress(self):
        # 增量没有天然总量：导出前用 rbd diff 预扫，进度条才不会永远停在 0.x%。
        runner = self._runner(stage_snaps=[{"id": 1, "name": "mig-j1-0"}])
        runner.diff_output = json.dumps(
            [
                {"offset": 0, "length": 1024},
                {"offset": 4096, "length": 3072},
            ]
        )
        captured: dict = {}
        self.utils._export_import = (
            lambda export_cmd, import_cmd, **kwargs: captured.update(kwargs)
        )

        self.utils.export_import_diff(
            "volume-src-1",
            "volume-dst-1-mig-stage",
            from_snap="mig-j1-0",
            to_snap="mig-j1-1",
        )

        probe = [
            call
            for call in runner.calls
            if isinstance(call, list) and "diff" in call and "--from-snap" in call
        ]
        self.assertEqual(len(probe), 1)
        self.assertEqual(captured["total_bytes"], 4096)

    def test_diff_bytes_returns_none_when_probe_fails(self):
        runner = self._runner()
        runner.raise_on_command = ["diff"]
        self.assertIsNone(
            self.utils.diff_bytes(
                "volume-src-1", from_snap="mig-j1-0", to_snap="mig-j1-1"
            )
        )

    def test_diff_bytes_failure_logs_rbd_stderr(self):
        """预估失败必须带上 stderr：SIGABRT 的 str(exc) 只有信号名，没别处可查。"""
        utils = CephUtils(
            "/src/ceph.conf", "volumes", "/dst/ceph.conf", "volumes"
        )
        utils._runner = mock.Mock(
            side_effect=subprocess.CalledProcessError(
                -6,
                ["rbd", "diff"],
                stderr="FAILED ceph_assert(q != removed_snaps_queue.end())",
            )
        )
        logging.disable(logging.NOTSET)
        try:
            with self.assertLogs(level="WARNING") as captured:
                self.assertIsNone(
                    utils.diff_bytes(
                        "volume-src-1", from_snap="mig-j1-0", to_snap="mig-j1-1"
                    )
                )
        finally:
            logging.disable(logging.CRITICAL)
        joined = "\n".join(captured.output)
        self.assertIn("rc=-6", joined)
        self.assertIn("ceph_assert", joined)

    def test_prune_snapshots_only_removes_own_job(self):
        self._runner(
            [
                {"id": 1, "name": "mig-j1-0"},
                {"id": 2, "name": "mig-j1-1"},
                {"id": 3, "name": "daily"},
            ]
        )
        removed = self.utils.prune_source_snapshots("volume-src-1", "j1", keep=1)
        self.assertEqual(removed, ["mig-j1-0"])


class IncrementalCopyTest(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.utils = CephUtils(
            "/src/ceph.conf", "volumes", "/dst/ceph.conf", "volumes"
        )

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _runner(
        self,
        source_info,
        stage_info=None,
        snaps=None,
        missing_stage=True,
        stage_snaps=None,
    ):
        runner = FakeRunner(
            info_by_spec={
                "volume-src-1": json.dumps(source_info),
                "volume-dst-1-mig-stage": json.dumps(stage_info or source_info),
            },
            missing_once=(
                ["volume-dst-1-mig-stage"] if missing_stage else []
            ),
            snaps_by_image={
                "volume-src-1": snaps or [],
                "volume-dst-1-mig-stage": stage_snaps or [],
            },
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        return runner

    @staticmethod
    def _commands(runner):
        return " ".join(
            " ".join(call) if isinstance(call, list) else str(call)
            for call in runner.calls
        )

    def test_seed_creates_base_snapshot_with_source_layout(self):
        runner = self._runner({"size": 10, "order": 20})
        session = self.utils.seed_volume("volume-src-1", "volume-dst-1", "j1")
        self.assertEqual(session.last_snapshot, "mig-j1-0")
        commands = self._commands(runner)
        self.assertIn("volumes/volume-src-1@mig-j1-0", commands)
        self.assertIn("--order 20", commands)
        self.assertIn("volumes/volume-dst-1-mig-stage", commands)
        # 回归：全量 rbd import 不会在目标端留下快照，必须补建同名基线快照，
        # 否则后续 import-diff 会以 "start snapshot ... does not exist" 失败。
        self.assertIn(
            "snap create volumes/volume-dst-1-mig-stage@mig-j1-0", commands
        )

    def test_seed_removes_leftover_stage_with_snapshots(self):
        runner = self._runner(
            {"size": 10, "order": 20}, missing_stage=False
        )
        self.utils.seed_volume("volume-src-1", "volume-dst-1", "j1")
        commands = self._commands(runner)
        # 带快照的遗留暂存卷必须先 purge 再 rm（rbd rm 遇快照返回 rc=39）。
        self.assertIn("snap purge volumes/volume-dst-1-mig-stage", commands)
        self.assertLess(
            commands.index("snap purge volumes/volume-dst-1-mig-stage"),
            commands.index("rm volumes/volume-dst-1-mig-stage"),
        )

    def test_seed_rejects_stage_with_wrong_layout(self):
        runner = self._runner(
            {"size": 10, "order": 20}, stage_info={"size": 10, "order": 22}
        )
        with self.assertRaises(LayoutMismatchError):
            self.utils.seed_volume("volume-src-1", "volume-dst-1", "j1")
        self.assertNotIn("rename", self._commands(runner))

    def test_seed_tolerates_operations_feature_on_source_only(self):
        """真实故障回归：源卷带 operations、暂存卷没有，基线全备不该判失败。"""
        runner = self._runner(
            {
                "size": 10,
                "order": 20,
                "features": ["layering", "exclusive-lock", "operations"],
            },
            stage_info={
                "size": 10,
                "order": 20,
                "features": ["layering", "exclusive-lock"],
            },
        )
        session = self.utils.seed_volume("volume-src-1", "volume-dst-1", "j1")
        self.assertEqual(session.last_snapshot, "mig-j1-0")
        self.assertIn(
            "snap create volumes/volume-dst-1-mig-stage@mig-j1-0",
            self._commands(runner),
        )

    def test_delta_rounds_prune_older_source_snapshots(self):
        snaps = [
            {"id": 1, "name": "mig-j1-0"},
            {"id": 2, "name": "mig-j1-1"},
            {"id": 3, "name": "mig-j1-2"},
        ]
        runner = self._runner({"size": 10, "order": 20}, snaps=snaps)
        session = self.utils.seed_volume("volume-src-1", "volume-dst-1", "j1")
        self.utils.sync_volume_round(session)
        self.utils.sync_volume_round(session)
        self.assertEqual(session.last_snapshot, "mig-j1-2")
        commands = self._commands(runner)
        self.assertIn("export-diff --from-snap mig-j1-0", commands)
        self.assertIn("export-diff --from-snap mig-j1-1", commands)
        self.assertIn("snap rm volumes/volume-src-1@mig-j1-0", commands)
        self.assertNotIn("snap rm volumes/volume-src-1@mig-j1-1", commands)

    def test_delta_round_keeps_only_latest_stage_snapshot(self):
        """每轮增量都会在暂存卷上留一条快照，只保留最新一条即可，否则越堆越多。"""
        runner = self._runner(
            {"size": 10, "order": 20},
            missing_stage=False,
            snaps=[{"id": 1, "name": "mig-j1-0"}, {"id": 2, "name": "mig-j1-1"}],
            stage_snaps=[{"id": 1, "name": "mig-j1-0"}],
        )
        session = self.utils.seed_volume("volume-src-1", "volume-dst-1", "j1")
        session.last_snapshot = "mig-j1-0"

        self.utils.sync_volume_round(session)

        commands = self._commands(runner)
        # 新一轮的结束快照 mig-j1-1 保留，上一轮的 mig-j1-0 回收。
        self.assertIn(
            "snap rm volumes/volume-dst-1-mig-stage@mig-j1-0", commands
        )
        self.assertNotIn(
            "snap rm volumes/volume-dst-1-mig-stage@mig-j1-1", commands
        )

    def test_baseline_tail_phase_keeps_progress_at_100(self):
        """数据拷完后还有校验与建基线快照，必须报出阶段且不把进度打回 0。"""
        runner = self._runner(
            {"size": 10, "order": 20}, missing_stage=False
        )
        phases: list[tuple[str, bool]] = []

        self.utils.seed_volume(
            "volume-src-1",
            "volume-dst-1",
            "j1",
            on_phase=lambda label, keep=False: phases.append((label, keep)),
        )

        self.assertIn(("收尾：校验布局与基线快照", True), phases)
        self.assertEqual(phases[0], ("基线全量", False))

    def test_finalize_swaps_and_removes_all_job_snapshots(self):
        source = {"size": 10, "order": 20}
        runner = self._runner(
            source,
            missing_stage=False,
            snaps=[
                {"id": 1, "name": "mig-j1-0"},
                {"id": 2, "name": "mig-j1-1"},
                {"id": 3, "name": "daily"},
            ],
            stage_snaps=[{"id": 9, "name": "mig-j1-0"}],
        )
        session = IncrementalSession(
            job_id="j1",
            source_rbd_name="volume-src-1",
            target_rbd_name="volume-dst-1",
            stage_name="volume-dst-1-mig-stage",
            layout=parse_layout(source),
            size=10,
            last_snapshot="mig-j1-0",
        )
        self.assertTrue(self.utils.finalize_incremental_volume(session))
        commands = self._commands(runner)
        self.assertIn("export-diff --from-snap mig-j1-0", commands)
        self.assertIn(
            "rename volumes/volume-dst-1-mig-stage volumes/volume-dst-1",
            commands,
        )
        self.assertIn("snap rm volumes/volume-src-1@mig-j1-0", commands)
        self.assertIn("snap rm volumes/volume-src-1@mig-j1-1", commands)
        self.assertNotIn("snap rm volumes/volume-src-1@daily", commands)
        # 暂存卷上的迁移快照在换入前清空，避免污染最终 Cinder 卷。
        self.assertIn("snap purge volumes/volume-dst-1-mig-stage", commands)

    def test_finalize_reports_missing_stage_before_cutover(self):
        """停机后才发现暂存卷没了：明确失败并给出原因，绝不静默换入坏卷。

        这条路径不做自动重做全量：源机已经停机，全量拷贝会把停机窗口拖到不可控。
        """
        source = {"size": 10, "order": 20}
        runner = self._runner(source, missing_stage=True)
        session = IncrementalSession(
            job_id="j1",
            source_rbd_name="volume-src-1",
            target_rbd_name="volume-dst-1",
            stage_name="volume-dst-1-mig-stage",
            layout=parse_layout(source),
            size=10,
            last_snapshot="mig-j1-0",
        )
        logging.disable(logging.NOTSET)
        try:
            with self.assertLogs(level="ERROR") as captured:
                self.assertFalse(self.utils.finalize_incremental_volume(session))
        finally:
            logging.disable(logging.CRITICAL)
        self.assertIn("暂存卷已丢失", "\n".join(captured.output))
        self.assertNotIn("rename", self._commands(runner))


if __name__ == "__main__":
    unittest.main()

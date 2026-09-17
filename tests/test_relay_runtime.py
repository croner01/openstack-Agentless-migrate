import unittest
from unittest import mock

from relay_runtime import (
    PoolConfig,
    RelayRuntime,
    drop_runtime,
    get_runtime,
    parse_relay_options,
    register_runtime,
    relay_options_from_form,
)


class _Form(dict):
    def getlist(self, key):
        value = self.get(key)
        if value is None:
            return []
        return [value] if isinstance(value, str) else list(value)


class ParseRelayOptionsTest(unittest.TestCase):
    def _options(self, **overrides):
        options = {
            "data_channel": "relay",
            "relay_platform_url": "https://platform.example.com",
            "relay_source_image": "img-src",
            "relay_source_flavor": "flv-src",
            "relay_source_az": "az1",
            "relay_source_network": "net-src",
            "relay_source_system_volume_type": "vt-src",
            "relay_source_ports": ["port-s"],
            "relay_source_size": "2",
            "relay_target_image": "img-tgt",
            "relay_target_flavor": "flv-tgt",
            "relay_target_az": "az2",
            "relay_target_network": "net-tgt",
            "relay_target_system_volume_type": "vt-tgt",
            "relay_target_ports": ["port-t"],
            "relay_target_size": "2",
            "rate_limit_mb_s": "0",
        }
        options.update(overrides)
        return options

    def test_returns_none_when_channel_is_rbd(self):
        self.assertIsNone(parse_relay_options(self._options(data_channel="rbd")))

    def test_parses_pool_configs(self):
        config = parse_relay_options(self._options())

        self.assertEqual(config.platform_url, "https://platform.example.com")
        self.assertEqual(config.source.image, "img-src")
        self.assertEqual(config.source.size, 2)
        self.assertEqual(config.source.port_ids, ["port-s"])
        self.assertEqual(config.target.az, "az2")

    def test_rejects_missing_platform_url(self):
        with self.assertRaises(ValueError) as ctx:
            parse_relay_options(self._options(relay_platform_url=""))

        self.assertIn("回连平台地址", str(ctx.exception))

    def test_rejects_missing_pool_field(self):
        with self.assertRaises(ValueError) as ctx:
            parse_relay_options(self._options(relay_target_image=""))

        self.assertIn("目标端中转机镜像", str(ctx.exception))

    def test_rejects_non_positive_size(self):
        with self.assertRaises(ValueError):
            parse_relay_options(self._options(relay_source_size="0"))

    def test_converts_rate_limit_to_bytes(self):
        config = parse_relay_options(self._options(rate_limit_mb_s="8"))

        self.assertEqual(config.rate_limit_bytes_per_sec, 8 * 1024 * 1024)

    def test_parses_subnet_and_fixed_ips(self):
        config = parse_relay_options(
            self._options(
                relay_source_subnet="sub-s",
                relay_source_ips="10.0.0.11, 10.0.0.12",
                relay_target_subnet="sub-t",
                relay_target_ips="",
            )
        )

        self.assertEqual(config.source.subnet, "sub-s")
        self.assertEqual(config.source.fixed_ips, ["10.0.0.11", "10.0.0.12"])
        self.assertEqual(config.target.subnet, "sub-t")
        self.assertEqual(config.target.fixed_ips, [])

    def test_parses_volume_types(self):
        config = parse_relay_options(
            self._options(
                relay_source_volume_type="src-ssd",
                relay_target_volume_type="tgt-ssd",
            )
        )

        self.assertEqual(config.source.volume_type, "src-ssd")
        self.assertEqual(config.target.volume_type, "tgt-ssd")

    def test_target_volume_type_falls_back_to_job_level(self):
        config = parse_relay_options(
            self._options(relay_target_volume_type="", target_volume_type="ssd")
        )

        self.assertEqual(config.target.volume_type, "ssd")

    def test_derive_timeouts_default_to_zero_meaning_unlimited(self):
        """作业表单不填超时 ⇒ 0，即"不限制等待"（云上还在建盘就不能判死）。"""
        with mock.patch.dict("os.environ", {}, clear=True):
            config = parse_relay_options(self._options())

        self.assertEqual(config.volume_ready_timeout, 0.0)
        self.assertEqual(config.snapshot_ready_timeout, 0.0)

    def test_result_timeout_defaults_to_unlimited(self):
        """单卷拷贝默认也没有墙钟上限：防卡死靠 stall_timeout 的字节看门狗。"""
        with mock.patch.dict("os.environ", {}, clear=True):
            config = parse_relay_options(self._options())

        self.assertEqual(config.result_timeout, 0.0)

    def test_result_timeout_env_override(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_RELAY_RESULT_TIMEOUT": "43200"}, clear=True
        ):
            config = parse_relay_options(self._options())

        self.assertEqual(config.result_timeout, 43200.0)

    def test_result_timeout_job_option_wins(self):
        with mock.patch.dict(
            "os.environ", {"MIGRATION_RELAY_RESULT_TIMEOUT": "43200"}, clear=True
        ):
            config = parse_relay_options(self._options(relay_result_timeout="3600"))

        self.assertEqual(config.result_timeout, 3600.0)

    def test_derive_timeouts_come_from_job_options(self):
        config = parse_relay_options(
            self._options(volume_ready_timeout="7200", snapshot_ready_timeout="5400")
        )

        self.assertEqual(config.volume_ready_timeout, 7200.0)
        self.assertEqual(config.snapshot_ready_timeout, 5400.0)

    def test_snapshot_timeout_follows_volume_timeout_when_unset(self):
        """页面只有一个超时输入框，没单独填快照超时就沿用它。"""
        config = parse_relay_options(self._options(volume_ready_timeout="7200"))

        self.assertEqual(config.volume_ready_timeout, 7200.0)
        self.assertEqual(config.snapshot_ready_timeout, 7200.0)

    def test_fixed_ips_accept_space_and_semicolon_separators(self):
        config = parse_relay_options(
            self._options(relay_source_ips="10.0.0.11 10.0.0.12;10.0.0.13")
        )

        self.assertEqual(
            config.source.fixed_ips, ["10.0.0.11", "10.0.0.12", "10.0.0.13"]
        )

    def test_agent_install_defaults_to_bootstrap(self):
        self.assertEqual(parse_relay_options(self._options()).agent_install, "bootstrap")

    def test_agent_install_accepts_baked(self):
        config = parse_relay_options(self._options(relay_agent_install="baked"))

        self.assertEqual(config.agent_install, "baked")

    def test_unknown_agent_install_falls_back_to_bootstrap(self):
        config = parse_relay_options(self._options(relay_agent_install="weird"))

        self.assertEqual(config.agent_install, "bootstrap")

    def test_data_port_defaults_and_override(self):
        self.assertEqual(parse_relay_options(self._options()).data_port, 9200)
        self.assertEqual(
            parse_relay_options(self._options(relay_data_port="9300")).data_port, 9300
        )

    def test_stall_timeout_defaults_to_five_minutes(self):
        self.assertEqual(parse_relay_options(self._options()).stall_timeout, 300.0)

    def test_stall_timeout_accepts_override(self):
        config = parse_relay_options(self._options(relay_stall_timeout="60"))

        self.assertEqual(config.stall_timeout, 60.0)

    def test_hole_mode_defaults_to_skip(self):
        self.assertEqual(parse_relay_options(self._options()).hole_mode, "skip")

    def test_hole_mode_accepts_zero_and_rejects_unknown(self):
        self.assertEqual(
            parse_relay_options(self._options(relay_hole_mode="zero")).hole_mode,
            "zero",
        )
        self.assertEqual(
            parse_relay_options(self._options(relay_hole_mode="off")).hole_mode,
            "off",
        )
        self.assertEqual(
            parse_relay_options(self._options(relay_hole_mode="bogus")).hole_mode,
            "skip",
        )


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class RelayRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.state = mock.MagicMock()
        self.state.secret = b"secret"
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.ledger = mock.MagicMock()
        self.config = mock.MagicMock()
        self.config.platform_url = "https://platform.example.com"
        self.config.token_ttl = 3600
        self.config.source = PoolConfig(
            size=2, image="img-src", flavor="flv-src", az="az1",
            network="net-src", port_ids=["port-s1", "port-s2"],
        )
        self.config.target = PoolConfig(
            size=1, image="img-tgt", flavor="flv-tgt", az="az2",
            network="net-tgt", port_ids=["port-t1"],
        )
        self.config.rate_limit_bytes_per_sec = 0.0
        self.config.chunk_size = 4 * 1024 * 1024
        self.config.ready_timeout = 5.0
        self.config.result_timeout = 60.0
        self.runtime = RelayRuntime(
            config=self.config,
            source_os=self.source_os,
            target_os=self.target_os,
            state=self.state,
            ledger=self.ledger,
            job_id="job-1",
        )

    def _patch_lifecycle(self):
        src_patch = mock.patch.multiple(
            self.runtime.source_pool,
            provision=mock.DEFAULT,
            wait_ready=mock.DEFAULT,
        )
        tgt_patch = mock.patch.multiple(
            self.runtime.target_pool,
            provision=mock.DEFAULT,
            wait_ready=mock.DEFAULT,
        )
        return src_patch, tgt_patch

    def test_start_provisions_and_waits_for_both_pools(self):
        src_patch, tgt_patch = self._patch_lifecycle()
        with src_patch as src, tgt_patch as tgt:
            self.runtime.start()

        src["provision"].assert_called_once()
        tgt["provision"].assert_called_once()
        src["wait_ready"].assert_called_once()
        tgt["wait_ready"].assert_called_once()

    def test_pools_use_configured_az_and_network(self):
        self.assertEqual(self.runtime.source_pool.az, "az1")
        self.assertEqual(self.runtime.source_pool.port_ids, ["port-s1", "port-s2"])
        self.assertEqual(self.runtime.target_pool.az, "az2")
        self.assertEqual(self.runtime.target_pool.role, "target")

    def test_pools_inherit_agent_install_and_data_port(self):
        self.config.agent_install = "baked"
        self.config.data_port = 9300
        runtime = RelayRuntime(
            config=self.config,
            source_os=self.source_os,
            target_os=self.target_os,
            state=self.state,
            ledger=self.ledger,
            job_id="job-1",
        )

        self.assertFalse(runtime.source_pool.bootstrap)
        self.assertFalse(runtime.target_pool.bootstrap)
        self.assertEqual(runtime.source_pool.data_port, 9300)

    def test_token_factory_binds_job_and_role(self):
        token = self.runtime.source_pool.token_factory("job-1", "source")

        self.assertTrue(token.startswith("job-1|source|"))

    def test_mover_factory_shares_pools_and_lifecycle(self):
        mover = self.runtime.mover_factory(mock.MagicMock(), {})

        self.assertIs(mover.source_pool, self.runtime.source_pool)
        self.assertIs(mover.target_pool, self.runtime.target_pool)
        self.assertIs(mover.ledger, self.ledger)
        self.assertEqual(mover.job_id, "job-1")

    def test_mover_factory_passes_volume_types(self):
        self.config.source.volume_type = "src-ssd"
        self.config.target.volume_type = "tgt-ssd"
        runtime = RelayRuntime(
            config=self.config,
            source_os=self.source_os,
            target_os=self.target_os,
            state=self.state,
            ledger=self.ledger,
            job_id="job-1",
        )

        mover = runtime.mover_factory(mock.MagicMock(), {})

        self.assertEqual(mover.source_volume_type, "src-ssd")
        self.assertEqual(mover.target_volume_type, "tgt-ssd")

    def test_finish_reconciles_then_destroys_pools(self):
        with mock.patch.object(self.runtime.reaper, "reconcile_job") as reconcile, \
                mock.patch.object(self.runtime.source_pool, "destroy") as src_destroy, \
                mock.patch.object(self.runtime.target_pool, "destroy") as tgt_destroy:
            self.runtime.finish()

        reconcile.assert_called_once_with("job-1")
        src_destroy.assert_called_once()
        tgt_destroy.assert_called_once()

    def test_snapshot_reports_in_flight_volume_progress(self):
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="copying",
            copied_bytes=50,
            total_bytes=200,
        )
        self.ledger.all.return_value = [record]

        volumes = self.runtime.snapshot()["volumes"]

        self.assertEqual(len(volumes), 1)
        self.assertEqual(volumes[0]["volume_id"], "vol-s1")
        self.assertEqual(volumes[0]["vm_id"], "vm-1")
        self.assertEqual(volumes[0]["progress_percent"], 25.0)
        # 与 RBD 直连一致：带上中文阶段标签，页面才有「标签 · 百分比 · 速率」。
        self.assertEqual(volumes[0]["progress_label"], "全量传输")

    def test_snapshot_exposes_phase_age_for_waiting_volumes(self):
        """打快照/派生卷阶段没有字节流动，页面靠 now - updated_at 显示已等待多久。"""
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="snapshotting",
            total_bytes=0,
            copied_bytes=0,
        )
        record.updated_at = 1000.0
        self.ledger.all.return_value = [record]

        snapshot = self.runtime.snapshot()

        self.assertEqual(snapshot["volumes"][0]["updated_at"], 1000.0)
        self.assertIn("now", snapshot)

    def test_volume_progress_percent_never_goes_backwards(self):
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="copying",
            copied_bytes=150,
            total_bytes=200,
        )
        self.ledger.all.return_value = [record]
        self.assertEqual(
            self.runtime.snapshot()["volumes"][0]["progress_percent"], 75.0
        )

        # 重试后总量被修正（例如换了大盘）不能让进度条倒退。
        record.total_bytes = 400
        self.assertEqual(
            self.runtime.snapshot()["volumes"][0]["progress_percent"], 75.0
        )

    def test_volume_progress_labels_follow_phase(self):
        from relay_ledger import VolumeTaskRecord
        from relay_runtime import relay_phase_label

        self.assertEqual(relay_phase_label("attaching_source"), "挂载源卷")
        self.assertEqual(relay_phase_label("done"), "完成")
        self.assertEqual(relay_phase_label("unknown-phase"), "unknown-phase")

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="snapshotting",
            total_bytes=100,
        )
        self.ledger.all.return_value = [record]
        volumes = self.runtime.snapshot()["volumes"]
        self.assertEqual(volumes[0]["progress_label"], "打快照")

    def test_snapshot_computes_throughput_between_polls(self):
        from relay_ledger import VolumeTaskRecord

        clock = _FakeClock()
        self.runtime.clock = clock
        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="copying",
            copied_bytes=0,
            total_bytes=100 * 1024 * 1024,
        )
        self.ledger.all.return_value = [record]

        self.runtime.snapshot()
        record.copied_bytes = 50 * 1024 * 1024
        clock.advance(5.0)

        volumes = self.runtime.snapshot()["volumes"]

        self.assertEqual(volumes[0]["throughput_mb_s"], 10.0)

    def test_snapshot_reports_skipped_and_sent_bytes(self):
        from relay_ledger import VolumeTaskRecord

        record = VolumeTaskRecord(
            job_id="job-1",
            vm_id="vm-1",
            volume_id="vol-s1",
            phase="copying",
            copied_bytes=3072,
            total_bytes=4096,
            skipped_bytes=1024,
        )
        self.ledger.all.return_value = [record]

        volume = self.runtime.snapshot()["volumes"][0]

        self.assertEqual(volume["skipped_bytes"], 1024)
        self.assertEqual(volume["sent_bytes"], 2048)

    def test_finish_marks_unfinished_records_failed(self):
        """作业收尾后台账不能继续显示"在途卷"，否则页面永远停在拷贝中。"""
        from relay_ledger import VolumeTaskRecord

        running = VolumeTaskRecord(
            job_id="job-1", vm_id="vm-1", volume_id="vol-s1", phase="copying"
        )
        finished = VolumeTaskRecord(
            job_id="job-1", vm_id="vm-1", volume_id="vol-s2", phase="done"
        )
        other_job = VolumeTaskRecord(
            job_id="job-2", vm_id="vm-1", volume_id="vol-s3", phase="copying"
        )
        self.ledger.all.return_value = [running, finished, other_job]

        with mock.patch.object(self.runtime.reaper, "reconcile_job"), \
                mock.patch.object(self.runtime.source_pool, "destroy"), \
                mock.patch.object(self.runtime.target_pool, "destroy"):
            self.runtime.finish()

        self.assertEqual(running.phase, "failed")
        self.assertEqual(finished.phase, "done")
        self.assertEqual(other_job.phase, "copying")

    def test_start_creates_one_port_per_node_when_not_configured(self):
        self.runtime.source_pool.port_ids = []
        self.source_os.create_relay_port.side_effect = [
            mock.Mock(id="port-new-1"),
            mock.Mock(id="port-new-2"),
        ]
        src_patch, tgt_patch = self._patch_lifecycle()
        with src_patch, tgt_patch:
            self.runtime.start()

        self.assertEqual(
            self.runtime.source_pool.port_ids, ["port-new-1", "port-new-2"]
        )
        self.assertEqual(self.source_os.create_relay_port.call_count, 2)

    def test_ports_carry_subnet_and_fixed_ips(self):
        self.runtime.source_pool.port_ids = []
        self.config.source.subnet = "sub-s"
        self.config.source.fixed_ips = ["10.0.0.11", "10.0.0.12"]
        src_patch, tgt_patch = self._patch_lifecycle()
        with src_patch, tgt_patch:
            self.runtime.start()

        calls = self.source_os.create_relay_port.call_args_list
        self.assertEqual(calls[0].kwargs["subnet_id"], "sub-s")
        self.assertEqual(calls[0].kwargs["fixed_ip"], "10.0.0.11")
        self.assertEqual(calls[1].kwargs["fixed_ip"], "10.0.0.12")

    def test_ports_without_fixed_ip_use_auto_allocation(self):
        self.runtime.source_pool.port_ids = []
        self.config.source.subnet = "sub-s"
        self.config.source.fixed_ips = []
        src_patch, tgt_patch = self._patch_lifecycle()
        with src_patch, tgt_patch:
            self.runtime.start()

        calls = self.source_os.create_relay_port.call_args_list
        self.assertEqual(calls[0].kwargs["subnet_id"], "sub-s")
        self.assertIsNone(calls[0].kwargs["fixed_ip"])

    def test_start_requires_network_or_ports(self):
        self.runtime.source_pool.port_ids = []
        self.config.source.network = ""

        with self.assertRaises(ValueError):
            self.runtime.start()

    def test_finish_deletes_ports_created_by_runtime(self):
        self.runtime.source_pool.port_ids = []
        self.source_os.create_relay_port.side_effect = [
            mock.Mock(id="port-new-1"),
            mock.Mock(id="port-new-2"),
        ]
        src_patch, tgt_patch = self._patch_lifecycle()
        with src_patch, tgt_patch:
            self.runtime.start()
        with mock.patch.object(self.runtime.reaper, "reconcile_job"), \
                mock.patch.object(self.runtime.source_pool, "destroy"), \
                mock.patch.object(self.runtime.target_pool, "destroy"):
            self.runtime.finish()

        self.assertEqual(self.source_os.delete_port.call_count, 2)
        self.assertEqual(
            self.source_os.delete_port.call_args_list[0].args, ("port-new-1",)
        )

    def test_snapshot_lists_both_pools(self):
        self.runtime.source_pool.nodes = []
        self.runtime.target_pool.nodes = []

        snapshot = self.runtime.snapshot()

        self.assertEqual(snapshot["job_id"], "job-1")
        self.assertEqual(snapshot["source"], [])
        self.assertEqual(snapshot["target"], [])
        self.assertIn("ledger", snapshot)

    def test_ledger_summary_counts_by_phase(self):
        from relay_ledger import VolumeTaskRecord

        self.ledger.all.return_value = [
            VolumeTaskRecord(job_id="job-1", vm_id="v", volume_id="a", phase="copying"),
            VolumeTaskRecord(job_id="job-1", vm_id="v", volume_id="b", phase="done"),
            VolumeTaskRecord(job_id="job-1", vm_id="v", volume_id="c", phase="cleaned"),
            VolumeTaskRecord(job_id="job-1", vm_id="v", volume_id="d", phase="failed"),
            VolumeTaskRecord(job_id="job-2", vm_id="v", volume_id="e", phase="copying"),
        ]

        summary = self.runtime.ledger_summary()

        self.assertEqual(
            summary,
            {"total": 4, "in_flight": 1, "done": 1, "cleaned": 1, "failed": 1},
        )

    def test_sweep_marks_stale_nodes_and_cleans_ledger(self):
        self.runtime.source_pool.sweep = mock.Mock(return_value=["n-1"])
        self.runtime.target_pool.sweep = mock.Mock(return_value=[])
        self.runtime.reaper.sweep = mock.Mock(return_value=["job-1:vol-1"])

        result = self.runtime.sweep(now=1234.0)

        self.assertEqual(result, ["n-1", "job-1:vol-1"])
        self.runtime.reaper.sweep.assert_called_with(
            now=1234.0, cloud_filter=(self.config.source_cloud, self.config.target_cloud)
        )

    def test_rebuild_node_returns_none_for_unknown_id(self):
        self.assertIsNone(self.runtime.rebuild_node("nope"))


class RelayOptionsFromFormTest(unittest.TestCase):
    def test_extracts_channel_and_pool_fields(self):
        form = _Form(
            {
                "data_channel": "relay",
                "relay_platform_url": "https://platform.example.com",
                "relay_source_image": "img-src",
                "relay_source_flavor": "flv-src",
                "relay_source_az": "az1",
                "relay_source_network": "net-src",
                "relay_source_system_volume_type": "vt-src",
                "relay_source_ports": ["port-s1", "port-s2"],
                "relay_target_image": "img-tgt",
                "relay_target_flavor": "flv-tgt",
                "relay_target_az": "az2",
                "relay_target_network": "net-tgt",
                "relay_target_system_volume_type": "vt-tgt",
                "relay_target_ports": ["port-t1"],
            }
        )

        options = relay_options_from_form(form)

        self.assertEqual(options["data_channel"], "relay")
        self.assertEqual(options["relay_source_ports"], ["port-s1", "port-s2"])
        self.assertEqual(options["relay_target_ports"], ["port-t1"])
        config = parse_relay_options(options)
        self.assertEqual(config.target.network, "net-tgt")

    def test_defaults_to_rbd_when_field_absent(self):
        options = relay_options_from_form(_Form({}))

        self.assertEqual(options["data_channel"], "rbd")
        self.assertIsNone(parse_relay_options(options))


class RuntimeRegistryTest(unittest.TestCase):
    def test_register_get_and_drop(self):
        runtime = mock.MagicMock()

        register_runtime("job-9", runtime)
        self.assertIs(get_runtime("job-9"), runtime)
        drop_runtime("job-9")
        self.assertIsNone(get_runtime("job-9"))

import json
import os
import tempfile
import unittest
from unittest import mock

import app as app_module
from relay_protocol import issue_node_token


class RelayModuleWiringTest(unittest.TestCase):
    def test_relay_state_symbol_exists(self):
        """作业线程引用 RELAY_STATE，符号改名会在这里被抓住。"""
        self.assertTrue(hasattr(app_module, "RELAY_STATE"))
        self.assertTrue(hasattr(app_module.RELAY_STATE, "ledger"))

    def test_relay_secret_is_persisted_on_disk(self):
        import os

        self.assertTrue(hasattr(app_module, "RELAY_SECRET_PATH"))
        self.assertTrue(os.path.exists(app_module.RELAY_SECRET_PATH))
        self.assertEqual(len(app_module.RELAY_STATE.secret), 32)

    def test_inventory_and_lease_stores_exist(self):
        from relay_inventory import NodeInventory
        from relay_lease import LeaseStore

        self.assertIsInstance(app_module.RELAY_INVENTORY, NodeInventory)
        self.assertIsInstance(app_module.RELAY_LEASES, LeaseStore)

    def test_relay_secret_is_stable_within_process(self):
        from relay_secret import load_or_create_secret

        again = load_or_create_secret(app_module.RELAY_SECRET_PATH)
        self.assertEqual(again, app_module.RELAY_STATE.secret)

    def test_credentials_loader_creates_master_key_when_env_missing(self):
        import os

        # 未注入 MIGRATION_SECRET_KEY 时自动生成并落盘，不再要求人工配置。
        store = app_module.load_relay_credentials(env={})

        self.assertEqual(store.tenants(), [])
        self.assertTrue(os.path.exists(app_module.RELAY_MASTER_KEY_PATH))

    def test_credentials_loader_env_var_takes_precedence(self):
        import base64
        import os

        raw = base64.b64encode(b"m" * 32).decode("ascii")
        before = os.path.getmtime(app_module.RELAY_MASTER_KEY_PATH)

        app_module.load_relay_credentials(env={"MIGRATION_SECRET_KEY": raw})

        # 环境变量优先，不会改写磁盘上的密钥文件
        self.assertEqual(
            os.path.getmtime(app_module.RELAY_MASTER_KEY_PATH), before
        )

    def test_sweep_relay_resources_is_noop_when_layer_not_ready(self):
        with mock.patch.object(
            app_module.RELAY_RESOURCES, "scheduler", None
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "node_manager", None
        ), mock.patch.object(app_module.RELAY_STATE, "sweep") as sweep:
            result = app_module.sweep_relay_resources(now=100.0)

        self.assertEqual(result, [])
        sweep.assert_not_called()

    def test_sweep_relay_resources_marks_unhealthy_and_scales_down(self):
        # 注意：Mock(name=...) 的 name 是保留参数，必须构造后再赋值。
        record = mock.Mock(node_id="n1", state="ready", updated_at=0.0)
        record.name = "relay-source-1"
        agent = mock.Mock(name="agent")
        scheduler = mock.MagicMock()
        scheduler.scale_down.return_value = ["n2"]
        reconciler = mock.MagicMock()
        reconciler.sweep.return_value = ["vol-orphan"]
        with mock.patch.object(
            app_module.RELAY_RESOURCES, "scheduler", scheduler
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "reconciler", reconciler
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "node_manager", mock.MagicMock()
        ), mock.patch.object(
            app_module.RELAY_STATE, "sweep", return_value=["a1"]
        ), mock.patch.object(
            app_module.RELAY_STATE, "by_id", return_value=agent
        ), mock.patch.object(
            app_module.RELAY_INVENTORY, "all", return_value=[record]
        ), mock.patch.object(
            app_module.RELAY_INVENTORY, "upsert"
        ) as upsert, mock.patch.object(
            app_module.RELAY_INVENTORY, "save"
        ):
            agent.name = "relay-source-1"
            result = app_module.sweep_relay_resources(now=100.0)

        self.assertEqual(record.state, "unhealthy")
        upsert.assert_called_once()
        self.assertIn("n2", result)
        self.assertIn("vol-orphan", result)

    def test_sweep_reaps_nodes_stuck_in_provisioning(self):
        """从未注册成功的建机残留不会自己变 unhealthy，必须按创建时间回收。"""
        stale = mock.Mock(
            node_id="stale-1",
            name="relay-target-t1-nova-1-1",
            role="target",
            state="provisioning",
            server_id="srv-stale",
            created_at=0.0,
            updated_at=0.0,
        )
        fake_inventory = mock.MagicMock()
        fake_inventory.all.return_value = [stale]
        node_manager = mock.MagicMock()
        node_manager.delete_node.return_value = True

        with mock.patch.object(
            app_module, "RELAY_INVENTORY", fake_inventory
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "scheduler", mock.MagicMock()
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "reconciler", None
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "node_manager", node_manager
        ), mock.patch.object(
            app_module.RELAY_STATE, "sweep", return_value=[]
        ):
            result = app_module.sweep_relay_resources(now=10000.0)

        node_manager.delete_node.assert_called_once_with("stale-1")
        self.assertIn("reap:stale-1", result)

    def test_sweep_keeps_fresh_provisioning_nodes(self):
        """刚提交、还在开机的节点不能被误删。"""
        fresh = mock.Mock(
            node_id="fresh-1",
            name="relay-source-t1-nova-1-1",
            role="source",
            state="provisioning",
            server_id="srv-fresh",
            created_at=9_990.0,
            updated_at=9_990.0,
        )
        fake_inventory = mock.MagicMock()
        fake_inventory.all.return_value = [fresh]
        node_manager = mock.MagicMock()

        with mock.patch.object(
            app_module, "RELAY_INVENTORY", fake_inventory
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "scheduler", mock.MagicMock()
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "reconciler", None
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "node_manager", node_manager
        ), mock.patch.object(
            app_module.RELAY_STATE, "sweep", return_value=[]
        ), mock.patch.object(
            app_module.RELAY_STATE, "find_by_name", return_value=None
        ):
            result = app_module.sweep_relay_resources(now=10000.0)

        node_manager.delete_node.assert_not_called()
        self.assertEqual(result, [])

    def test_start_job_relay_runtime_returns_none_without_config(self):
        options = {}

        runtime = app_module.start_job_relay_runtime(
            None,
            source_auth={"auth_url": "http://src"},
            target_auth={"auth_url": "http://dst"},
            job_id="job-1",
            options=options,
        )

        self.assertIsNone(runtime)
        self.assertNotIn("relay_mover_factory", options)

    def test_start_job_relay_runtime_starts_pool_and_wires_factory(self):
        fake_runtime = mock.MagicMock()
        options = {}

        with mock.patch.object(app_module, "RelayRuntime", return_value=fake_runtime) as ctor, \
                mock.patch.object(app_module, "OpenStackUtils") as os_cls, \
                mock.patch.object(app_module, "register_runtime") as register:
            runtime = app_module.start_job_relay_runtime(
                mock.MagicMock(),
                source_auth={"auth_url": "http://src"},
                target_auth={"auth_url": "http://dst"},
                job_id="job-1",
                options=options,
            )

        self.assertIs(runtime, fake_runtime)
        fake_runtime.start.assert_called_once()
        register.assert_called_once_with("job-1", fake_runtime)
        self.assertIs(options["relay_mover_factory"], fake_runtime.mover_factory)
        self.assertEqual(os_cls.call_count, 2)
        kwargs = ctor.call_args.kwargs
        self.assertIs(kwargs["state"], app_module.RELAY_STATE)
        self.assertIs(kwargs["ledger"], app_module.RELAY_STATE.ledger)

    def test_persistent_mode_requires_ready_resource_layer(self):
        config = mock.MagicMock(node_mode="persistent")
        with mock.patch.object(
            app_module.RELAY_RESOURCES, "ensure", return_value=False
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "error", "缺少主密钥"
        ), mock.patch.object(app_module, "OpenStackUtils"):
            with self.assertRaises(ValueError):
                app_module.start_job_relay_runtime(
                    config,
                    source_auth={"auth_url": "http://src"},
                    target_auth={"auth_url": "http://dst"},
                    job_id="job-1",
                    options={},
                )

    def test_persistent_mode_passes_scheduler_to_runtime(self):
        config = mock.MagicMock(node_mode="persistent")
        fake_runtime = mock.MagicMock()
        scheduler = mock.MagicMock()
        with mock.patch.object(
            app_module.RELAY_RESOURCES, "ensure", return_value=True
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "ensure_pool"
        ) as ensure_pool, mock.patch.object(
            app_module.RELAY_RESOURCES, "scheduler_for_pool"
        ) as pool_factory, mock.patch.object(
            app_module.RELAY_RESOURCES, "scheduler", scheduler
        ), mock.patch.object(
            app_module, "RelayRuntime", return_value=fake_runtime
        ) as ctor, mock.patch.object(app_module, "OpenStackUtils"), \
                mock.patch.object(app_module, "register_runtime"):
            app_module.start_job_relay_runtime(
                config,
                source_auth={"auth_url": "http://src"},
                target_auth={"auth_url": "http://dst"},
                job_id="job-1",
                options={},
            )

        self.assertIs(ctor.call_args.kwargs["scheduler"], scheduler)
        self.assertIs(ctor.call_args.kwargs["scheduler_factory"], pool_factory)
        # 源、目标两侧各准备一次池（自动建机 + 存凭据）
        self.assertEqual(ensure_pool.call_count, 2)

    def test_prepare_relay_pool_maps_form_values_to_profile(self):
        layer = mock.MagicMock()
        layer.ensure_pool.return_value = {"created_nodes": ["n1"]}
        pool_config = mock.MagicMock(
            az="nova-1", image="img", flavor="flv", network="net",
            subnet="sub", volume_type="vt", fixed_ips=[], port_ids=[],
        )
        config = mock.MagicMock(
            slots_per_node=5, max_nodes=6, min_nodes=1,
            idle_scale_down_seconds=86400.0, data_port=9200,
            platform_url="https://m", ssh_public_key="key",
            admin_password="relay-pass", ready_timeout=300.0,
        )

        app_module.prepare_relay_pool(
            layer=layer,
            side="source",
            auth={"auth_url": "http://keystone", "project_id": "p1"},
            pool_config=pool_config,
            config=config,
            admin_password="vm-pass",
        )

        kwargs = layer.ensure_pool.call_args.kwargs
        self.assertEqual(kwargs["tenant_key"], "http://keystone|p1")
        self.assertEqual(kwargs["role"], "source")
        self.assertEqual(kwargs["az"], "nova-1")
        self.assertEqual(kwargs["min_nodes"], 1)
        self.assertEqual(kwargs["profile_defaults"]["image"], "img")
        self.assertEqual(kwargs["profile_defaults"]["slots_per_node"], 5)
        self.assertEqual(kwargs["profile_defaults"]["admin_password"], "relay-pass")

    def _persistent_config(self):
        return mock.MagicMock(
            node_mode="persistent",
            source_cloud="src|p1",
            target_cloud="dst|p2",
            source=mock.MagicMock(az="nova-1"),
            target=mock.MagicMock(az="nova-2"),
        )

    def test_persistent_warnings_report_new_pool(self):
        config = self._persistent_config()
        with mock.patch.object(
            app_module.RELAY_RESOURCES, "ensure", return_value=True
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "profiles"
        ) as profiles:
            profiles.get.return_value = None
            warnings = app_module.relay_persistent_warnings(config)

        self.assertEqual(len(warnings), 2)
        self.assertTrue(all("尚未创建" in item for item in warnings))

    def test_persistent_warnings_report_existing_pool_parameters(self):
        config = self._persistent_config()
        profile = mock.MagicMock(image="img-1", flavor="flv-1", network="net-1")
        with mock.patch.object(
            app_module.RELAY_RESOURCES, "ensure", return_value=True
        ), mock.patch.object(
            app_module.RELAY_RESOURCES, "profiles"
        ) as profiles:
            profiles.get.return_value = profile
            warnings = app_module.relay_persistent_warnings(config)

        self.assertTrue(all("沿用现有建机参数" in item for item in warnings))
        self.assertTrue(all("img-1" in item for item in warnings))

    def test_persistent_warnings_empty_for_ephemeral(self):
        config = mock.MagicMock(node_mode="ephemeral")

        self.assertEqual(app_module.relay_persistent_warnings(config), [])


class RelayAgentTokenWiringTest(unittest.TestCase):
    """常驻中转机的节点令牌必须交给清单校验。

    平台一度在注册 relay blueprint 时漏传 inventory，节点令牌被当成作业令牌
    走 verify_token，直接回 malformed token 401，导致所有常驻中转机
    cloud-init 拉不到安装脚本、永远注册不上。
    """

    def setUp(self):
        self.client = app_module.app.test_client()
        self.token = issue_node_token(
            app_module.RELAY_STATE.secret,
            node_id="missing-node",
            role="source",
            tenant_key="t1",
            az="nova-1",
        )

    def test_bootstrap_verifies_node_token_against_inventory(self):
        response = self.client.get(f"/api/relay/bootstrap?token={self.token}")

        self.assertEqual(response.status_code, 401)
        # 必须走到清单校验（unknown node），而不是被当成畸形作业令牌。
        self.assertEqual(response.get_json()["error"], "unknown node")

    def test_register_verifies_node_token_against_inventory(self):
        response = self.client.post(
            "/api/relay/register",
            json={"token": self.token, "name": "relay-source-1"},
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "unknown node")


class LogPersistenceTest(unittest.TestCase):
    """日志必须写在持久化的 uploads 目录，否则 Pod 重启后就查不到历史。"""

    def test_log_file_lives_under_upload_folder(self):
        upload = os.path.abspath(app_module.UPLOAD_FOLDER)

        self.assertTrue(
            os.path.abspath(app_module.LOG_FILE).startswith(upload + os.sep),
            app_module.LOG_FILE,
        )

    def test_setup_logging_installs_rotating_file_handler(self):
        import logging
        from logging.handlers import RotatingFileHandler

        app_module._setup_logging()

        files = [
            handler
            for handler in logging.getLogger().handlers
            if isinstance(handler, RotatingFileHandler)
        ]
        self.assertEqual(len(files), 1)
        self.assertEqual(
            os.path.abspath(files[0].baseFilename),
            os.path.abspath(app_module.LOG_FILE),
        )
        self.assertGreaterEqual(files[0].maxBytes, 1024 * 1024)


class HoleModeOverrideTest(unittest.TestCase):
    def test_env_forces_hole_mode(self):
        options = {"relay_hole_mode": "skip"}

        app_module.relay_hole_mode_override(
            options, {"MIGRATION_RELAY_HOLE_MODE": "off"}
        )

        self.assertEqual(options["relay_hole_mode"], "off")

    def test_empty_env_keeps_form_value(self):
        options = {"relay_hole_mode": "zero"}

        app_module.relay_hole_mode_override(options, {})

        self.assertEqual(options["relay_hole_mode"], "zero")


class JobCancelDeleteApiTest(unittest.TestCase):
    def test_cutover_unknown_job_returns_404(self):
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.get.return_value = None
            response = self.client.post("/api/jobs/missing/vms/vm-1/cutover")

        self.assertEqual(response.status_code, 404)

    def test_cutover_rejects_vm_not_awaiting_cutover(self):
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.get.return_value = mock.Mock()
            manager.request_cutover.return_value = False
            response = self.client.post("/api/jobs/job-1/vms/vm-1/cutover")

        self.assertEqual(response.status_code, 409)
        manager.request_cutover.assert_called_once_with("job-1", "vm-1")

    def test_cutover_accepts_awaiting_vm(self):
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.get.return_value = mock.Mock()
            manager.request_cutover.return_value = True
            response = self.client.post("/api/jobs/job-1/vms/vm-1/cutover")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

    def setUp(self):
        self.client = app_module.app.test_client()

    def test_cancel_unknown_job_returns_404(self):
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.get.return_value = None
            response = self.client.post("/api/jobs/missing/cancel")

        self.assertEqual(response.status_code, 404)

    def test_cancel_marks_job_and_notifies_relay_agents(self):
        task = {"task_id": "t-1", "job_id": "job-1"}
        with mock.patch.object(app_module, "job_manager") as manager, \
                mock.patch.object(
                    app_module.RELAY_STATE, "tasks_for_job", return_value=[task]
                ), mock.patch.object(
                    app_module.RELAY_STATE, "request_cancel"
                ) as request_cancel:
            manager.get.return_value = mock.Mock()
            manager.cancel.return_value = True

            response = self.client.post("/api/jobs/job-1/cancel")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["cancelled"])
        self.assertEqual(body["relay_agents"], 1)
        request_cancel.assert_called_once_with("t-1")

    def test_delete_running_job_returns_409(self):
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.delete.return_value = "任务仍在进行中，请先取消并等待结束"

            response = self.client.delete("/api/jobs/job-1")

        self.assertEqual(response.status_code, 409)

    def test_delete_unknown_job_returns_404(self):
        with mock.patch.object(app_module, "job_manager") as manager:
            manager.delete.return_value = "任务不存在"

            response = self.client.delete("/api/jobs/missing")

        self.assertEqual(response.status_code, 404)

    def test_delete_removes_job_and_runtime(self):
        with mock.patch.object(app_module, "job_manager") as manager, \
                mock.patch.object(app_module, "drop_runtime") as drop:
            manager.delete.return_value = None

            response = self.client.delete("/api/jobs/job-1")

        self.assertEqual(response.status_code, 200)
        drop.assert_called_once_with("job-1")


class CephConfRequirementTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig_upload = app_module.app.config["UPLOAD_FOLDER"]
        app_module.app.config["UPLOAD_FOLDER"] = self.tmp.name

    def tearDown(self):
        app_module.app.config["UPLOAD_FOLDER"] = self._orig_upload
        self.tmp.cleanup()

    def _call(
        self, *, require_ceph: bool, channel: str, require_target_image: bool = True
    ):
        with app_module.app.test_request_context(
            "/api/migrate",
            method="POST",
            data={
                "data_channel": channel,
                "selected_rows": json.dumps(
                    [
                        {
                            "vm_name": "vm-1",
                            "server_id": "srv-1",
                            "target_az": "az1",
                        }
                    ]
                ),
            },
        ):
            return app_module._create_job_and_files(
                require_ceph=require_ceph,
                require_target_image=require_target_image,
            )

    def test_relay_mode_does_not_require_ceph_conf(self):
        _job_id, _job_dir, rows, source_conf, target_conf = self._call(
            require_ceph=False, channel="relay", require_target_image=False
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(source_conf, "")
        self.assertEqual(target_conf, "")

    def test_rbd_mode_still_requires_ceph_conf(self):
        with self.assertRaises(ValueError) as ctx:
            self._call(require_ceph=True, channel="rbd")

        self.assertIn("Ceph conf", str(ctx.exception))

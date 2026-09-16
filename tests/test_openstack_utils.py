import unittest
from unittest import mock
from types import SimpleNamespace

from openstack_utils import CINDER_VOLUME_AZ, OpenStackUtils


class _NotFound(Exception):
    """模拟 openstacksdk 的 NotFoundException（带 status_code=404）。"""

    status_code = 404


class WaitServerBootedTest(unittest.TestCase):
    """建机后的可见性竞态与启停时序：立即 start 会撞 404，把成功的建机判失败。"""

    def setUp(self):
        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)

    def test_wait_server_status_tolerates_transient_not_found(self):
        self.conn.compute.get_server.side_effect = [
            _NotFound("could not be found"),
            SimpleNamespace(status="ACTIVE", id="srv-1"),
        ]

        server = self.os_utils.wait_server_status(
            "srv-1", target="ACTIVE", timeout=5, poll_interval=0, not_found_grace=5
        )

        self.assertEqual(server.status, "ACTIVE")

    def test_wait_server_booted_starts_after_shutoff(self):
        self.conn.compute.get_server.side_effect = [
            SimpleNamespace(status="BUILD", id="srv-1"),
            SimpleNamespace(status="SHUTOFF", id="srv-1"),
            SimpleNamespace(status="ACTIVE", id="srv-1"),
        ]

        server = self.os_utils.wait_server_booted(
            "srv-1", timeout=5, poll_interval=0, not_found_grace=5
        )

        self.assertEqual(server.status, "ACTIVE")
        self.conn.compute.start_server.assert_called_once_with("srv-1")

    def test_wait_server_booted_does_not_start_building_instance(self):
        self.conn.compute.get_server.side_effect = [
            SimpleNamespace(status="BUILD", id="srv-1"),
            SimpleNamespace(status="BUILD", id="srv-1"),
            SimpleNamespace(status="ACTIVE", id="srv-1"),
        ]

        self.os_utils.wait_server_booted(
            "srv-1", timeout=5, poll_interval=0, not_found_grace=5
        )

        self.conn.compute.start_server.assert_not_called()

    def test_wait_server_booted_reports_instance_never_visible(self):
        self.conn.compute.get_server.side_effect = _NotFound("could not be found")

        with self.assertRaises(RuntimeError) as ctx:
            self.os_utils.wait_server_booted(
                "srv-1", timeout=5, poll_interval=0, not_found_grace=0
            )

        self.assertIn("srv-1", str(ctx.exception))
        self.assertIn("不可见", str(ctx.exception))

    def test_wait_server_booted_raises_on_error_state(self):
        self.conn.compute.get_server.return_value = SimpleNamespace(
            status="ERROR", id="srv-1"
        )

        with self.assertRaises(RuntimeError):
            self.os_utils.wait_server_booted(
                "srv-1", timeout=5, poll_interval=0, not_found_grace=5
            )


class BdmBuildTest(unittest.TestCase):
    def test_bfv_bdm_puts_system_disk_first(self):
        bdm = OpenStackUtils._build_bfv_block_device_mapping(
            image_id="img-1",
            boot_volume_size=40,
            data_volume_ids=["vol-data-1", "vol-data-2"],
        )
        self.assertEqual(bdm[0]["uuid"], "img-1")
        self.assertEqual(bdm[0]["source_type"], "image")
        self.assertEqual(bdm[0]["destination_type"], "volume")
        self.assertEqual(bdm[0]["boot_index"], 0)
        self.assertEqual(bdm[0]["volume_size"], 40)
        self.assertEqual(
            [item["uuid"] for item in bdm[1:]],
            ["vol-data-1", "vol-data-2"],
        )
        self.assertTrue(all(item["boot_index"] == -1 for item in bdm[1:]))

    def test_bfv_bdm_carries_volume_type_for_boot_volume(self):
        bdm = OpenStackUtils._build_bfv_block_device_mapping(
            image_id="img-1",
            boot_volume_size=40,
            data_volume_ids=[],
            volume_type="rbd",
        )
        self.assertEqual(bdm[0]["volume_type"], "rbd")

    def test_bfv_bdm_omits_volume_type_when_unset(self):
        bdm = OpenStackUtils._build_bfv_block_device_mapping(
            image_id="img-1",
            boot_volume_size=40,
            data_volume_ids=[],
        )
        self.assertNotIn("volume_type", bdm[0])


class CreateBfvServerPasswordTest(unittest.TestCase):
    """Nova needs adminPass; openstacksdk only maps the Server attribute
    ``admin_password`` to that JSON key. ``admin_pass`` is silently dropped
    and Nova then generates a random password.
    """

    def _make_utils(self):
        conn = mock.Mock()
        created = SimpleNamespace(id="server-1")
        conn.compute.create_server.return_value = created
        utils = OpenStackUtils(conn=conn)
        return utils, created

    def test_password_is_sent_as_admin_password_attribute(self):
        utils, created = self._make_utils()
        result = utils.create_bfv_server(
            name="vm-test",
            image_id="image-1",
            flavor_id="flavor-1",
            port_ids=["port-1", "port-2"],
            boot_volume_size=40,
            data_volume_ids=["vol-data-1"],
            availability_zone="az-1",
            admin_password="P@ssw0rd",
        )
        self.assertIs(result, created)
        _, kwargs = utils.conn.compute.create_server.call_args
        self.assertEqual(kwargs["admin_password"], "P@ssw0rd")
        self.assertNotIn("admin_pass", kwargs)
        self.assertEqual(
            kwargs["networks"], [{"port": "port-1"}, {"port": "port-2"}]
        )
        self.assertEqual(kwargs["block_device_mapping_v2"][0]["boot_index"], 0)

    def test_empty_password_is_allowed_so_nova_generates_one(self):
        utils, _ = self._make_utils()
        utils.create_bfv_server(
            name="vm-test",
            image_id="image-1",
            flavor_id="flavor-1",
            port_ids=["port-1"],
            boot_volume_size=40,
            data_volume_ids=[],
            availability_zone="az-1",
            admin_password="",
        )
        _, kwargs = utils.conn.compute.create_server.call_args
        self.assertEqual(kwargs["admin_password"], "")
        self.assertNotIn("admin_pass", kwargs)

    def test_blank_volume_creation_does_not_touch_admin_password(self):
        conn = mock.Mock()
        conn.block_storage.create_volume.return_value = SimpleNamespace(
            id="volume-1"
        )
        utils = OpenStackUtils(conn=conn)
        volume = utils.create_blank_volume("blank-1", 10)
        self.assertEqual(volume.id, "volume-1")
        conn.block_storage.create_volume.assert_called_once_with(
            name="blank-1", size=10, availability_zone=CINDER_VOLUME_AZ
        )


class FakeSourceServer:
    def __init__(self, server_id, name, status, az, image_id="image-1"):
        self.id = server_id
        self.name = name
        self.status = status
        self._az = az
        self._image_id = image_id

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "OS-EXT-AZ:availability_zone": self._az,
            "flavor": {"id": "flavor-1"},
            "image": {"id": self._image_id},
        }


class FakeSourceFlavor:
    def __init__(self):
        self.id = "flavor-1"
        self.name = "m1.small"
        self.vcpus = 1
        self.ram = 2048
        self.disk = 20


class FakeBootVolume:
    def __init__(self, server_id, image_id, image_name=None):
        self._server_id = server_id
        self._image_id = image_id
        self._image_name = image_name

    def to_dict(self):
        metadata = {"image_id": self._image_id}
        if self._image_name:
            metadata["image_name"] = self._image_name
        return {
            # openstacksdk Volume.to_dict() 返回的是 Python 属性名，
            # 而不是 Cinder 服务端字段名 bootable。
            "is_bootable": True,
            "volume_image_metadata": metadata,
            "attachments": [{"server_id": self._server_id}],
        }


class SourceServerListTest(unittest.TestCase):
    def test_summary_enriches_flavor_and_image(self):
        server = FakeSourceServer("srv-1", "web-1", "ACTIVE", "az1")
        summary = OpenStackUtils._summarize_source_server(
            server,
            flavor_by_id={"flavor-1": FakeSourceFlavor()},
            image_by_id={"image-1": "cirros"},
        )
        self.assertEqual(summary["server_id"], "srv-1")
        self.assertEqual(summary["availability_zone"], "az1")
        self.assertEqual(summary["flavor"]["name"], "m1.small")
        self.assertEqual(summary["image_name"], "cirros")

    def test_list_source_servers_passes_paging_and_search(self):
        conn = mock.Mock()
        conn.compute.servers.return_value = [
            FakeSourceServer("srv-1", "web-1", "ACTIVE", "az1")
        ]
        conn.compute.flavors.return_value = [FakeSourceFlavor()]
        conn.image.images.return_value = [
            SimpleNamespace(id="image-1", name="cirros")
        ]
        utils = OpenStackUtils(conn=conn)
        result = utils.list_source_servers(search="web", marker="old", limit=20)
        conn.compute.servers.assert_called_once_with(
            name="web", marker="old", limit=20
        )
        self.assertEqual(result[0]["server_id"], "srv-1")

    def test_image_name_hint_used_when_glance_image_is_gone(self):
        server = FakeSourceServer("srv-3", "web-3", "ACTIVE", "az1", image_id="")
        conn = mock.Mock()
        conn.compute.servers.return_value = [server]
        conn.compute.flavors.return_value = [FakeSourceFlavor()]
        # 源镜像已删除/不可见时 Glance 查不到名字，Cinder 卷元数据仍可提供。
        conn.image.images.return_value = []
        conn.block_storage.volumes.return_value = [
            FakeBootVolume("srv-3", "deleted-img", image_name="ubuntu-20.04")
        ]
        utils = OpenStackUtils(conn=conn)
        result = utils.list_source_servers()
        self.assertEqual(result[0]["image_id"], "deleted-img")
        self.assertEqual(result[0]["image_name"], "ubuntu-20.04")

    def test_list_servers_falls_back_to_boot_volume_image(self):
        server = FakeSourceServer("srv-2", "web-2", "ACTIVE", "az1", image_id="")
        conn = mock.Mock()
        conn.compute.servers.return_value = [server]
        conn.compute.flavors.return_value = [FakeSourceFlavor()]
        conn.image.images.return_value = [
            SimpleNamespace(id="boot-image-1", name="cirros-uec")
        ]
        conn.block_storage.volumes.return_value = [
            FakeBootVolume("srv-2", "boot-image-1")
        ]
        utils = OpenStackUtils(conn=conn)
        result = utils.list_source_servers()
        self.assertEqual(result[0]["image_name"], "cirros-uec")
        self.assertEqual(result[0]["image_id"], "boot-image-1")


class FakeZone:
    def __init__(self, name, state):
        self.zoneName = name
        self.zoneState = state


class AvailabilityZoneTest(unittest.TestCase):
    def test_availability_zones_are_normalised(self):
        conn = mock.Mock()
        conn.compute.availability_zones.return_value = [
            FakeZone("az1", {"available": True}),
            FakeZone("az2", {"available": False}),
        ]
        utils = OpenStackUtils(conn=conn)
        result = utils.list_availability_zones()
        self.assertEqual(
            result,
            [
                {"id": "az1", "name": "az1", "available": True},
                {"id": "az2", "name": "az2", "available": False},
            ],
        )


class FakeAuthorizeConn:
    """Minimal stand-in for an openstacksdk connection used by the resolver."""

    def __init__(self, token: str = "FAKE_TOKEN"):
        self._token = token

    def authorize(self) -> str:
        return self._token


class ResolveProjectIdTest(unittest.TestCase):
    AUTH = {
        "auth_url": "https://keystone.example.com/v3",
        "username": "admin",
        "password": "secret",
        "user_domain_name": "Default",
    }

    def _resolve(self, auth_args, projects):
        response = mock.Mock()
        response.json.return_value = {"projects": projects}
        with mock.patch("requests.get", return_value=response) as get_mock:
            result = OpenStackUtils._resolve_project_id(
                auth_args, probe_conn=FakeAuthorizeConn()
            )
        get_mock.assert_called_once_with(
            f"{auth_args['auth_url'].rstrip('/')}/auth/projects",
            headers={"X-Auth-Token": "FAKE_TOKEN"},
            timeout=30,
        )
        return result

    def test_project_id_is_kept_as_is(self):
        args = {**self.AUTH, "project_name": "admin", "project_id": "uuid-123"}
        self.assertIs(
            OpenStackUtils._resolve_project_id(args, probe_conn=object()),
            args,
        )

    def test_name_without_project_scope_is_untouched(self):
        args = {**self.AUTH}
        self.assertIs(
            OpenStackUtils._resolve_project_id(args, probe_conn=object()),
            args,
        )

    def test_unique_project_by_name_is_resolved_to_uuid(self):
        args = {**self.AUTH, "project_name": "admin", "project_domain_name": "Default"}
        result = self._resolve(
            args,
            [
                {
                    "id": "uuid-admin-1",
                    "name": "admin",
                    "domain_id": "domain-default",
                    "domain_name": "Default",
                }
            ],
        )
        self.assertEqual(result["project_id"], "uuid-admin-1")
        self.assertNotIn("project_name", result)
        self.assertNotIn("project_domain_name", result)
        self.assertEqual(result["username"], "admin")

    def test_domain_mismatch_is_rejected(self):
        args = {**self.AUTH, "project_name": "admin", "project_domain_name": "Other"}
        with self.assertRaises(RuntimeError):
            self._resolve(
                args,
                [
                    {
                        "id": "uuid-admin-1",
                        "name": "admin",
                        "domain_name": "Default",
                    }
                ],
            )

    def test_ambiguous_name_requires_explicit_uuid(self):
        args = {**self.AUTH, "project_name": "admin"}
        with self.assertRaisesRegex(RuntimeError, "存在多个名为 admin"):
            self._resolve(
                args,
                [
                    {
                        "id": "uuid-admin-1",
                        "name": "admin",
                        "domain_name": "Default",
                    },
                    {
                        "id": "uuid-admin-2",
                        "name": "admin",
                        "domain_name": "Ops",
                    },
                ],
            )

    def test_missing_project_includes_accessible_names_in_error(self):
        args = {**self.AUTH, "project_name": "service"}
        with self.assertRaisesRegex(RuntimeError, "service|可访问项目"):
            self._resolve(
                args,
                [{"id": "uuid-admin-1", "name": "admin", "domain_name": "Default"}],
            )


class DiagnosticSafeTest(unittest.TestCase):
    """The debug helpers must never blow up when auth internals are minimal."""

    def test_scope_hint_works_without_password_exposure(self):
        conn = mock.Mock()
        conn.auth = mock.Mock()
        conn.auth.project_id = "uuid-123"
        os_utils = OpenStackUtils(conn=conn)
        hint = os_utils._scope_hint()
        self.assertIn("uuid-123", hint)
        self.assertNotIn("password", hint.lower())

    def test_collect_auth_attrs_supports_slots_plugin(self):
        class SlotsAuth:
            __slots__ = ("auth_url", "username", "password", "project_id")

            def __init__(self):
                self.auth_url = "https://keystone:5000/v3"
                self.username = "admin"
                self.password = "secret"
                self.project_id = "uuid-123"

        attrs = OpenStackUtils._collect_auth_attrs(SlotsAuth())
        self.assertEqual(attrs["project_id"], "uuid-123")
        self.assertEqual(attrs["username"], "admin")
        self.assertEqual(attrs["password"], "secret")
        conn = mock.Mock()
        conn.auth = SlotsAuth()
        conn.authorize.return_value = None
        conn.block_storage.volumes.side_effect = RuntimeError("no volumes")
        conn.compute.servers.side_effect = RuntimeError("no servers")
        os_utils = OpenStackUtils(conn=conn)
        info = os_utils.diagnostic_info()
        self.assertNotIn("password", info["auth_attrs"])
        self.assertEqual(info["auth_attrs"]["project_id"], "uuid-123")

    def test_diagnostic_info_is_serialisable_for_minimal_connection(self):
        conn = mock.Mock()
        conn.auth = mock.Mock()
        conn.auth.project_id = "uuid-123"
        conn.authorize.side_effect = RuntimeError("denied")
        os_utils = OpenStackUtils(conn=conn)
        info = os_utils.diagnostic_info()
        self.assertIn("authorize_error", info)


class AccessibleProjectsTest(unittest.TestCase):
    def test_list_merges_role_and_all_projects(self):
        fake = mock.Mock()
        project_a = mock.Mock()
        project_a.id, project_a.name, project_a.domain_id, project_a.domain_name = (
            "uuid-a",
            "project-a",
            "domain-1",
            "Default",
        )
        project_b = mock.Mock()
        project_b.id, project_b.name, project_b.domain_id, project_b.domain_name = (
            "uuid-b",
            "project-b",
            "domain-1",
            "Default",
        )
        fake.identity.projects.return_value = [project_a, project_b]
        auth_args = {"auth_url": "http://keystone/v3", "username": "u"}
        with mock.patch.object(
            OpenStackUtils,
            "_fetch_accessible_projects",
            return_value=[
                {"id": "uuid-a", "name": "project-a", "domain_name": "Default"}
            ],
        ):
            with mock.patch.object(
                OpenStackUtils,
                "_list_all_projects",
                return_value=[
                    {
                        "id": "uuid-b",
                        "name": "project-b",
                        "domain_name": "Default",
                    }
                ],
            ):
                result = OpenStackUtils.list_accessible_projects(auth_args)
        self.assertEqual(
            [project["name"] for project in result["projects"]],
            ["project-a", "project-b"],
        )
        self.assertEqual(result["summary"]["total_count"], 2)


class FindVolumeAttachmentTest(unittest.TestCase):
    """getter 抛错时不能当成"没挂载"，否则清理会跳过卸载导致删卷被拒。"""

    def setUp(self):
        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)

    def test_falls_back_to_listing_when_getter_raises(self):
        self.conn.compute.get_volume_attachment.side_effect = RuntimeError("boom")
        attachment = mock.Mock(volume_id="vol-1", id="att-1")
        self.conn.compute.volume_attachments.return_value = [attachment]

        self.assertEqual(
            self.os_utils.find_volume_attachment("srv-1", "vol-1"), "att-1"
        )

    def test_returns_none_when_listing_has_no_match(self):
        self.conn.compute.get_volume_attachment.side_effect = RuntimeError("boom")
        self.conn.compute.volume_attachments.return_value = []

        self.assertIsNone(self.os_utils.find_volume_attachment("srv-1", "vol-1"))


class WaitVolumeStatusTest(unittest.TestCase):
    """卷挂载失败会被 Cinder 回滚（attaching → available），必须快速失败。"""

    def setUp(self):
        self.conn = mock.MagicMock()
        self.os_utils = OpenStackUtils(conn=self.conn)

    def _volume(self, status):
        return mock.Mock(status=status)

    def test_returns_when_target_reached(self):
        self.conn.block_storage.get_volume.side_effect = [
            self._volume("creating"),
            self._volume("in_use"),
        ]

        with mock.patch("openstack_utils.time.sleep"):
            volume = self.os_utils.wait_volume_status(
                "vol-1", target="in_use", timeout=30, poll_interval=1
            )

        self.assertEqual(volume.status, "in_use")

    def test_hyphen_status_matches_underscore_target(self):
        """Cinder 实际返回 "in-use"，此前按 "in_use" 比较导致每次 attach 白等 600s。"""
        self.conn.block_storage.get_volume.side_effect = [self._volume("in-use")]

        volume = self.os_utils.wait_volume_status(
            "vol-1", target="in_use", timeout=30, poll_interval=1
        )

        self.assertEqual(volume.status, "in-use")
        self.assertEqual(self.conn.block_storage.get_volume.call_count, 1)

    def test_underscore_status_matches_hyphen_target(self):
        self.conn.block_storage.get_volume.side_effect = [self._volume("in_use")]

        volume = self.os_utils.wait_volume_status(
            "vol-1", target="in-use", timeout=30, poll_interval=1
        )

        self.assertEqual(volume.status, "in_use")

    def test_rollback_to_available_raises_immediately(self):
        self.conn.block_storage.get_volume.side_effect = [
            self._volume("attaching"),
            self._volume("available"),
        ]

        with mock.patch("openstack_utils.time.sleep"):
            with self.assertRaises(RuntimeError) as ctx:
                self.os_utils.wait_volume_status(
                    "vol-1",
                    target="in_use",
                    timeout=600,
                    poll_interval=1,
                    context="（target 卷挂载到中转机 srv-1）",
                )

        self.assertIn("挂载被回滚", str(ctx.exception))
        self.assertIn("srv-1", str(ctx.exception))
        # 只轮询了两次，没有干等到 600s
        self.assertEqual(self.conn.block_storage.get_volume.call_count, 2)

    def test_available_before_attaching_is_not_treated_as_rollback(self):
        self.conn.block_storage.get_volume.side_effect = [
            self._volume("available"),
            self._volume("attaching"),
            self._volume("in_use"),
        ]

        with mock.patch("openstack_utils.time.sleep"):
            volume = self.os_utils.wait_volume_status(
                "vol-1", target="in_use", timeout=30, poll_interval=1
            )

        self.assertEqual(volume.status, "in_use")

    def test_error_attaching_is_terminal(self):
        self.conn.block_storage.get_volume.side_effect = [
            self._volume("error_attaching")
        ]

        with self.assertRaises(RuntimeError) as ctx:
            self.os_utils.wait_volume_status("vol-1", target="in_use", timeout=30)

        self.assertIn("error_attaching", str(ctx.exception))

    def test_timeout_reports_last_status(self):
        self.conn.block_storage.get_volume.return_value = self._volume("downloading")

        with mock.patch("openstack_utils.time.time", side_effect=[0, 0, 999]):
            with self.assertRaises(TimeoutError) as ctx:
                self.os_utils.wait_volume_status(
                    "vol-1", target="available", timeout=10, poll_interval=1
                )

        self.assertIn("最后状态 downloading", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

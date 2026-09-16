import unittest
from unittest import mock

from openstack_utils import OpenStackUtils
from relay_catalog import build_catalog, preflight
from relay_runtime import PoolConfig, RelayChannelConfig


def _named(identifier: str, name: str, *, external: bool = False) -> mock.Mock:
    """Mock 的 name 是保留参数，必须构造后再赋值。"""
    item = mock.Mock(id=identifier)
    item.name = name
    item.is_router_external = external
    return item


class ListNetworksTest(unittest.TestCase):
    def test_list_networks_returns_id_and_name(self):
        conn = mock.MagicMock()
        conn.network.networks.return_value = [
            _named("net-1", "storage"),
            _named("net-2", ""),
        ]
        os_utils = OpenStackUtils(conn=conn)

        networks = os_utils.list_networks()

        self.assertEqual(
            networks,
            [{"id": "net-1", "name": "storage"}, {"id": "net-2", "name": "net-2"}],
        )

    def test_list_external_networks_keeps_only_external(self):
        conn = mock.MagicMock()
        conn.network.networks.return_value = [
            _named("net-1", "internal"),
            _named("net-2", "public", external=True),
        ]
        os_utils = OpenStackUtils(conn=conn)

        networks = os_utils.list_external_networks()

        self.assertEqual(networks, [{"id": "net-2", "name": "public"}])


class BuildCatalogTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.list_images.return_value = [_named("img-s", "web")]
        self.target_os.list_images.return_value = [_named("img-t", "web")]
        self.source_os.list_flavors.return_value = [_named("flv-s", "m1")]
        self.target_os.list_flavors.return_value = [_named("flv-t", "m1")]
        self.source_os.list_availability_zones.return_value = [
            {"name": "az1", "state": "available"}
        ]
        self.target_os.list_availability_zones.return_value = [
            {"name": "az2", "state": "available"}
        ]
        self.source_os.list_networks.return_value = [{"id": "net-s", "name": "s"}]
        self.target_os.list_networks.return_value = [{"id": "net-t", "name": "t"}]
        self.source_os.list_subnets.return_value = [
            {"id": "sub-s", "name": "s", "cidr": "10.0.0.0/24", "network_id": "net-s"}
        ]
        self.target_os.list_subnets.return_value = [
            {"id": "sub-t", "name": "t", "cidr": "10.1.0.0/24", "network_id": "net-t"}
        ]
        self.source_os.list_volume_types.return_value = [
            {"id": "vt-s", "name": "src-ssd"}
        ]
        self.target_os.list_volume_types.return_value = [
            {"id": "vt-t", "name": "tgt-ssd"}
        ]

    def test_catalog_contains_both_sides(self):
        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(
            catalog["source"]["images"], [{"id": "img-s", "name": "web"}]
        )
        self.assertEqual(
            catalog["target"]["flavors"], [{"id": "flv-t", "name": "m1"}]
        )
        self.assertEqual(
            catalog["source"]["networks"], [{"id": "net-s", "name": "s"}]
        )

    def test_catalog_azs_are_plain_names(self):
        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(catalog["source"]["azs"], ["az1"])
        self.assertEqual(catalog["target"]["azs"], ["az2"])

    def test_catalog_contains_subnets_with_cidr(self):
        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(
            catalog["source"]["subnets"],
            [
                {
                    "id": "sub-s",
                    "name": "s",
                    "cidr": "10.0.0.0/24",
                    "network_id": "net-s",
                }
            ],
        )

    def test_catalog_contains_volume_types(self):
        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(
            catalog["source"]["volume_types"], [{"id": "vt-s", "name": "src-ssd"}]
        )
        self.assertEqual(
            catalog["target"]["volume_types"], [{"id": "vt-t", "name": "tgt-ssd"}]
        )

    def test_catalog_tolerates_one_section_failure(self):
        self.target_os.list_images.side_effect = RuntimeError("boom")

        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(catalog["target"]["images"], [])
        self.assertIn("images", catalog["target"]["errors"])
        self.assertEqual(catalog["target"]["networks"], [{"id": "net-t", "name": "t"}])

    def test_catalog_reports_error_per_section(self):
        self.source_os.list_networks.side_effect = RuntimeError("no network api")

        catalog = build_catalog(self.source_os, self.target_os)

        self.assertEqual(catalog["source"]["networks"], [])
        self.assertIn("networks", catalog["source"]["errors"])
        self.assertEqual(catalog["source"]["images"][0]["id"], "img-s")


class PreflightTest(unittest.TestCase):
    def setUp(self):
        self.source_os = mock.MagicMock()
        self.target_os = mock.MagicMock()
        self.source_os.list_images.return_value = [_named("img-s", "web")]
        self.target_os.list_images.return_value = [_named("img-t", "web")]
        self.source_os.list_flavors.return_value = [_named("flv-s", "m1")]
        self.target_os.list_flavors.return_value = [_named("flv-t", "m1")]
        self.source_os.list_availability_zones.return_value = [{"name": "az1"}]
        self.target_os.list_availability_zones.return_value = [{"name": "az2"}]
        self.source_os.list_networks.return_value = [{"id": "net-s", "name": "s"}]
        self.target_os.list_networks.return_value = [{"id": "net-t", "name": "t"}]
        self.source_os.list_subnets.return_value = [
            {"id": "sub-s", "name": "s", "cidr": "10.0.0.0/24", "network_id": "net-s"}
        ]
        self.target_os.list_subnets.return_value = [
            {"id": "sub-t", "name": "t", "cidr": "10.1.0.0/24", "network_id": "net-t"}
        ]
        self.source_os.list_volume_types.return_value = [
            {"id": "vt-s", "name": "src-ssd"}
        ]
        self.target_os.list_volume_types.return_value = [
            {"id": "vt-t", "name": "tgt-ssd"}
        ]
        self.config = RelayChannelConfig(
            platform_url="https://platform.example.com",
            source=PoolConfig(
                size=1, image="img-s", flavor="flv-s", az="az1",
                network="net-s", port_ids=["port-s"], system_volume_type="vt-s",
            ),
            target=PoolConfig(
                size=1, image="img-t", flavor="flv-t", az="az2",
                network="net-t", port_ids=["port-t"], system_volume_type="vt-t",
            ),
        )

    def test_preflight_passes_for_valid_config(self):
        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(result["ok"])
        self.assertEqual(result["errors"], [])

    def test_preflight_reports_missing_image(self):
        self.config.source.image = "img-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("镜像" in item for item in result["errors"]))

    def test_preflight_reports_missing_flavor(self):
        self.config.target.flavor = "flv-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("flavor" in item for item in result["errors"]))

    def test_preflight_reports_missing_az(self):
        self.config.target.az = "az-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("可用区" in item for item in result["errors"]))

    def test_preflight_reports_missing_network(self):
        self.config.source.network = "net-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("网络" in item for item in result["errors"]))

    def test_preflight_requires_network_or_ports(self):
        self.config.source.network = ""
        self.config.source.port_ids = []

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("网络或端口" in item for item in result["errors"]))

    def test_preflight_warns_about_unverifiable_snapshot_support(self):
        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(any("in-use" in item for item in result["warnings"]))

    def test_preflight_warns_when_volume_type_is_default(self):
        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(any("__DEFAULT__" in item for item in result["warnings"]))

    def test_preflight_does_not_warn_default_when_type_chosen(self):
        self.config.source.volume_type = "src-ssd"
        self.config.target.volume_type = "tgt-ssd"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(
            any("将使用 __DEFAULT__" in item for item in result["warnings"])
        )

    def test_preflight_warns_when_type_quota_is_zero(self):
        self.config.source.volume_type = "src-ssd"
        self.source_os.get_volume_quota.return_value = {"gigabytes_src-ssd": 0}

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(any("配额为 0" in item for item in result["warnings"]))

    def test_quota_lookup_failure_does_not_break_preflight(self):
        self.config.source.volume_type = "src-ssd"
        self.source_os.get_volume_quota.side_effect = RuntimeError("no quota api")

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(result["ok"])

    def test_preflight_rejects_subnet_from_other_network(self):
        # 网段存在，但属于另一个网络。
        self.source_os.list_subnets.return_value = [
            {"id": "sub-s", "name": "s", "cidr": "10.0.0.0/24", "network_id": "net-s"},
            {
                "id": "sub-other",
                "name": "other",
                "cidr": "10.2.0.0/24",
                "network_id": "net-other",
            },
        ]
        self.config.source.subnet = "sub-other"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("不属于所选网络" in item for item in result["errors"]))

    def test_preflight_rejects_unknown_subnet(self):
        self.config.source.subnet = "sub-missing"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("网段不存在" in item for item in result["errors"]))

    def test_preflight_accepts_subnet_of_same_network(self):
        self.config.source.subnet = "sub-s"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(result["ok"], result["errors"])

    def test_preflight_rejects_ip_outside_subnet(self):
        self.config.source.subnet = "sub-s"
        self.config.source.fixed_ips = ["10.9.9.9"]
        self.config.source.size = 1

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("不属于网段" in item for item in result["errors"]))

    def test_preflight_rejects_ip_count_mismatch(self):
        self.config.source.subnet = "sub-s"
        self.config.source.fixed_ips = ["10.0.0.11", "10.0.0.12"]
        self.config.source.size = 3

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("数量" in item for item in result["errors"]))

    def test_preflight_rejects_ip_without_subnet(self):
        self.config.source.subnet = ""
        self.config.source.fixed_ips = ["10.0.0.11"]
        self.config.source.size = 1

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("必须同时选择网段" in item for item in result["errors"]))

    def test_preflight_accepts_valid_fixed_ips(self):
        self.config.source.subnet = "sub-s"
        self.config.source.fixed_ips = ["10.0.0.11"]
        self.config.source.size = 1

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(result["ok"], result["errors"])

    def test_preflight_rejects_unknown_volume_type(self):
        self.config.target.volume_type = "missing-type"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertFalse(result["ok"])
        self.assertTrue(any("卷类型不存在" in item for item in result["errors"]))

    def test_preflight_accepts_known_volume_type(self):
        self.config.target.volume_type = "tgt-ssd"

        result = preflight(self.config, self.source_os, self.target_os)

        self.assertTrue(result["ok"], result["errors"])

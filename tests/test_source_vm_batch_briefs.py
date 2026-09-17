"""源 VM 网卡/卷摘要：清单一大就不能再逐台查（N+1 会把请求数放大成几百次）。"""
import unittest
from types import SimpleNamespace
from unittest import mock

import app as app_module
from openstack_utils import OpenStackUtils


def _port(port_id: str, device_id: str, ip: str, network_id: str = "net-1",
          subnet_id: str = "sub-1") -> SimpleNamespace:
    return SimpleNamespace(
        id=port_id,
        device_id=device_id,
        network_id=network_id,
        mac_address="fa:16:3e:00:00:" + port_id[-2:],
        fixed_ips=[{"ip_address": ip, "subnet_id": subnet_id}],
    )


def _volume(volume_id: str, server_id: str, device: str, size: int,
            is_bootable: bool) -> SimpleNamespace:
    return SimpleNamespace(
        id=volume_id,
        size=size,
        is_bootable=is_bootable,
        name=volume_id,
        to_dict=lambda: {
            "id": volume_id,
            "attachments": [
                {"server_id": server_id, "device": device},
            ],
        },
    )


def _server(server_id: str, name: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=server_id,
        name=name,
        status="ACTIVE",
        to_dict=lambda: {
            "id": server_id,
            "name": name,
            "attached_volumes": [],
            "addresses": {},
        },
    )


class BulkBriefsTest(unittest.TestCase):
    """台数超过阈值时只允许"整体拉一次"，请求次数与 VM 台数无关。"""

    def setUp(self):
        self.conn = mock.MagicMock()
        self.utils = OpenStackUtils(conn=self.conn)
        self.vm_count = 20
        self.pairs = [(f"vm-{i}", f"srv-{i}") for i in range(self.vm_count)]

        self.conn.compute.servers.return_value = [
            _server(sid, name) for name, sid in self.pairs
        ]
        self.conn.network.ports.return_value = [
            _port(f"p{i}", sid, f"10.0.0.{i}")
            for i, (_name, sid) in enumerate(self.pairs)
        ]
        self.conn.network.networks.return_value = [
            SimpleNamespace(id="net-1", name="provider-net"),
        ]
        self.conn.network.subnets.return_value = [
            SimpleNamespace(id="sub-1", cidr="10.0.0.0/24"),
        ]
        self.conn.block_storage.volumes.return_value = [
            _volume(f"vol-{i}", sid, f"/dev/vdb", 40, i == 0)
            for i, (_name, sid) in enumerate(self.pairs)
        ]

    def _collect(self):
        return self.utils.collect_vm_network_briefs(
            [name for name, _sid in self.pairs],
            [sid for _name, sid in self.pairs],
        )

    def test_bulk_path_issues_constant_number_of_calls(self):
        result = self._collect()

        self.assertEqual(self.conn.compute.servers.call_count, 1)
        self.assertEqual(self.conn.network.ports.call_count, 1)
        self.assertEqual(self.conn.network.networks.call_count, 1)
        self.assertEqual(self.conn.network.subnets.call_count, 1)
        self.assertEqual(self.conn.block_storage.volumes.call_count, 1)
        # 逐台查询才会用到的调用必须一次都不发生
        self.assertEqual(self.conn.compute.get_server.call_count, 0)
        self.assertEqual(self.conn.network.get_network.call_count, 0)
        self.assertEqual(self.conn.network.get_subnet.call_count, 0)
        self.assertEqual(self.conn.block_storage.get_volume.call_count, 0)
        self.assertEqual(len(result), self.vm_count)

    def test_bulk_path_groups_ports_and_volumes_back_to_their_vm(self):
        result = self._collect()

        first = result["vm-0"]
        self.assertEqual(first["server_id"], "srv-0")
        self.assertEqual(first["ports"], [{
            "network_id": "net-1",
            "network_name": "provider-net",
            "ip": "10.0.0.0",
            "subnet_id": "sub-1",
            "cidr": "10.0.0.0/24",
            "mac": "fa:16:3e:00:00:p0",
        }])
        self.assertEqual(first["volumes"], [{
            "volume_id": "vol-0",
            "size": 40,
            "device": "/dev/vdb",
            "is_bootable": True,
        }])
        self.assertEqual(result["vm-7"]["ports"][0]["ip"], "10.0.0.7")
        self.assertEqual(result["vm-7"]["volumes"][0]["volume_id"], "vol-7")

    def test_bulk_path_isolates_missing_vm(self):
        self.pairs.append(("vm-ghost", "srv-ghost"))
        self.conn.compute.servers.return_value = [
            _server(sid, name) for name, sid in self.pairs if sid != "srv-ghost"
        ]

        result = self._collect()

        self.assertEqual(result["vm-ghost"], {"error": "源 VM 不存在"})
        # 其它 VM 不受影响
        self.assertEqual(result["vm-3"]["ports"][0]["ip"], "10.0.0.3")


class BulkFallbackTest(unittest.TestCase):
    """批量拉端口失败（Neutron 策略受限）时必须退回定向查询，不能静默丢网卡。"""

    def test_falls_back_to_per_vm_ports_when_listing_fails(self):
        conn = mock.MagicMock()
        utils = OpenStackUtils(conn=conn)
        pairs = [(f"vm-{i}", f"srv-{i}") for i in range(10)]
        conn.compute.servers.return_value = [
            _server(sid, name) for name, sid in pairs
        ]
        conn.network.ports.side_effect = RuntimeError("policy denied")
        conn.network.get_network.return_value = SimpleNamespace(id="net-1", name="net-a")
        conn.network.get_subnet.return_value = SimpleNamespace(id="sub-1", cidr="10.0.0.0/24")
        conn.block_storage.volumes.return_value = []

        result = utils.collect_vm_network_briefs(
            [name for name, _ in pairs], [sid for _, sid in pairs]
        )

        self.assertEqual(conn.network.get_network.call_count, 0)
        # 1 次整体列举失败 + 10 次逐台定向查询
        self.assertEqual(conn.network.ports.call_count, 11)
        for _name, sid in pairs:
            conn.network.ports.assert_any_call(device_id=sid)
        self.assertEqual(result["vm-2"]["ports"], [])


class SmallListBriefsTest(unittest.TestCase):
    """台数少时不该为了三台 VM 拉整个项目的端口与卷。"""

    def setUp(self):
        self.conn = mock.MagicMock()
        self.utils = OpenStackUtils(conn=self.conn)

    def test_small_list_uses_targeted_queries(self):
        server = _server("srv-a", "vm-a")
        server.to_dict = lambda: {
            "id": "srv-a",
            "name": "vm-a",
            "attached_volumes": [{"id": "vol-a"}],
            "addresses": {},
        }
        self.conn.compute.get_server.return_value = server
        self.conn.network.ports.return_value = [_port("p1", "srv-a", "10.0.0.5")]
        self.conn.network.get_network.return_value = SimpleNamespace(id="net-1", name="net-a")
        self.conn.network.get_subnet.return_value = SimpleNamespace(id="sub-1", cidr="10.0.0.0/24")
        self.conn.block_storage.get_volume.return_value = _volume(
            "vol-a", "srv-a", "/dev/vda", 20, True
        )

        result = self.utils.collect_vm_network_briefs(["vm-a"], ["srv-a"])

        self.assertEqual(self.conn.compute.servers.call_count, 0)
        self.assertEqual(self.conn.network.ports.call_count, 1)
        self.conn.network.ports.assert_called_once_with(device_id="srv-a")
        self.assertEqual(result["vm-a"]["ports"][0]["ip"], "10.0.0.5")
        self.assertEqual(result["vm-a"]["volumes"][0]["volume_id"], "vol-a")

    def test_small_list_fetches_server_once_per_vm(self):
        """卷清单不能再顺手 get_server 一次，否则每台 VM 白多一次 Nova 请求。"""
        server = _server("srv-a", "vm-a")
        server.to_dict = lambda: {
            "id": "srv-a",
            "name": "vm-a",
            "attached_volumes": [{"id": "vol-a"}],
            "addresses": {},
        }
        self.conn.compute.get_server.return_value = server
        self.conn.network.ports.return_value = []
        self.conn.block_storage.get_volume.return_value = _volume(
            "vol-a", "srv-a", "/dev/vda", 20, True
        )

        result = self.utils.collect_vm_network_briefs(["vm-a"], ["srv-a"])

        self.assertEqual(self.conn.compute.get_server.call_count, 1)
        self.assertEqual(result["vm-a"]["volumes"][0]["volume_id"], "vol-a")

    def test_small_list_reuses_network_lookup_for_same_network(self):
        """同一台 VM 的多个端口落在同一网络时，只该查一次网络名/网段。"""
        self.conn.compute.get_server.side_effect = lambda sid: _server(sid, sid)
        self.conn.network.ports.return_value = [
            _port("p1", "srv-a", "10.0.0.5"),
            _port("p2", "srv-a", "10.0.0.6"),
        ]
        self.conn.network.get_network.return_value = SimpleNamespace(id="net-1", name="net-a")
        self.conn.network.get_subnet.return_value = SimpleNamespace(id="sub-1", cidr="10.0.0.0/24")

        result = self.utils.collect_vm_network_briefs(["vm-a"], ["srv-a"])

        self.assertEqual(self.conn.network.get_network.call_count, 1)
        self.assertEqual(self.conn.network.get_subnet.call_count, 1)
        self.assertEqual(len(result["vm-a"]["ports"]), 2)

    def test_unmatched_ids_fall_back_to_name_lookup(self):
        """前端没带 id（Excel 清单）时按名字解析，不做索引错位配对。"""
        self.conn.compute.find_server.return_value = _server("srv-a", "vm-a")
        self.conn.compute.get_server.return_value = _server("srv-a", "vm-a")
        self.conn.network.ports.return_value = []

        result = self.utils.collect_vm_network_briefs(["vm-a"], [])

        self.conn.compute.find_server.assert_called_once_with("vm-a")
        self.assertEqual(result["vm-a"]["server_id"], "srv-a")

    def test_missing_vm_reports_error_without_breaking_others(self):
        self.conn.compute.find_server.return_value = None

        result = self.utils.collect_vm_network_briefs(["vm-ghost"], [])

        self.assertEqual(result["vm-ghost"], {"error": "源 VM 不存在"})


class SourceVmNetworksRouteTest(unittest.TestCase):
    """路由必须走批量接口，别退回逐台循环。"""

    def setUp(self):
        self.client = app_module.app.test_client()

    def _post(self, payload):
        fake = mock.MagicMock()
        fake.collect_vm_network_briefs.return_value = {
            "vm-a": {"server_id": "srv-a", "ports": [], "volumes": []},
        }
        fake.list_volume_types.return_value = [{"id": "t1", "name": "ceph"}]
        with mock.patch.object(app_module, "OpenStackUtils", return_value=fake):
            response = self.client.post("/api/source-vm-networks", json=payload)
        return response, fake

    def test_route_delegates_to_batch_collector(self):
        response, fake = self._post({
            "auth_url": "http://keystone/",
            "username": "u",
            "password": "p",
            "project_name": "proj",
            "vm_names": ["vm-a"],
            "server_ids": ["srv-a"],
        })

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["servers"]["vm-a"]["server_id"], "srv-a")
        self.assertEqual(body["volume_types"], [{"id": "t1", "name": "ceph"}])
        fake.collect_vm_network_briefs.assert_called_once_with(["vm-a"], ["srv-a"])
        # 逐台的老接口一次都不该被调到
        self.assertEqual(fake.get_server_port_details.call_count, 0)
        self.assertEqual(fake.get_server_detail.call_count, 0)


if __name__ == "__main__":
    unittest.main()

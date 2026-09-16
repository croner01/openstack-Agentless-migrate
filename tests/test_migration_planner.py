import unittest

from migration_planner import (
    ip_belongs_to_subnet,
    match_flavor,
    orphan_snapshots,
    order_system_and_data,
    override_key,
    resolve_port_ip,
    snapshot_job_id,
    snapshot_name,
    snapshots_to_prune,
    vm_channel,
)


class FlavorMatchTest(unittest.TestCase):
    def test_returns_none_when_spec_does_not_match(self):
        source = {"vcpus": 4, "ram": 8192, "disk": 80, "name": "m1.small"}
        flavors = [
            {
                "id": "f1",
                "name": "m1.medium",
                "vcpus": 8,
                "ram": 16384,
                "disk": 80,
            }
        ]
        self.assertIsNone(match_flavor(source, flavors))

    def test_prefers_same_name(self):
        source = {"vcpus": 4, "ram": 8192, "disk": 80, "name": "m1.x"}
        flavors = [
            {
                "id": "a",
                "name": "zzz",
                "vcpus": 4,
                "ram": 8192,
                "disk": 80,
            },
            {
                "id": "b",
                "name": "m1.x",
                "vcpus": 4,
                "ram": 8192,
                "disk": 80,
            },
        ]
        self.assertEqual(match_flavor(source, flavors)["id"], "b")

    def test_returns_lexicographically_first_candidate(self):
        source = {"vcpus": 2, "ram": 4096, "disk": 40, "name": "other"}
        flavors = [
            {"id": "z", "name": "b", "vcpus": 2, "ram": 4096, "disk": 40},
            {"id": "a", "name": "a", "vcpus": 2, "ram": 4096, "disk": 40},
        ]
        self.assertEqual(match_flavor(source, flavors)["id"], "a")


class VolumeOrderTest(unittest.TestCase):
    def test_system_disk_first(self):
        volumes = [
            {"id": "data", "is_bootable": False, "device": "vdb"},
            {"id": "system", "is_bootable": True, "device": "vda"},
        ]
        ordered = order_system_and_data(volumes)
        self.assertEqual(ordered[0]["id"], "system")
        self.assertEqual(ordered[1]["id"], "data")

    def test_orders_data_volumes_by_device(self):
        volumes = [
            {"id": "b", "is_bootable": False, "device": "vdc"},
            {"id": "a", "is_bootable": False, "device": "vdb"},
        ]
        ordered = order_system_and_data(volumes)
        self.assertEqual([volume["id"] for volume in ordered], ["a", "b"])


class SubnetTest(unittest.TestCase):
    def test_ip_in_subnet(self):
        self.assertTrue(ip_belongs_to_subnet("10.0.0.7", "10.0.0.0/24"))
        self.assertFalse(ip_belongs_to_subnet("10.0.1.7", "10.0.0.0/24"))

    def test_override_key_prefers_server_id(self):
        self.assertEqual(override_key("srv-abc", "web-1"), "srv-abc")
        self.assertEqual(override_key("", "web-1"), "web-1")


class ResolvePortIpTest(unittest.TestCase):
    def test_keeps_source_ip_when_it_belongs_to_target_subnet(self):
        self.assertEqual(
            resolve_port_ip(
                source_ip="192.168.111.223",
                target_cidr="192.168.111.0/24",
                target_ip="",
            ),
            "192.168.111.223",
        )

    def test_auto_allocates_when_source_ip_is_outside_target_subnet(self):
        self.assertIsNone(
            resolve_port_ip(
                source_ip="192.168.111.223",
                target_cidr="192.168.223.0/24",
                target_ip="",
            )
        )

    def test_uses_manual_target_ip_when_specified(self):
        self.assertEqual(
            resolve_port_ip(
                source_ip="192.168.111.223",
                target_cidr="192.168.223.0/24",
                target_ip="192.168.223.50",
            ),
            "192.168.223.50",
        )

    def test_manual_target_ip_must_be_in_target_subnet(self):
        with self.assertRaises(ValueError):
            resolve_port_ip(
                source_ip="192.168.111.223",
                target_cidr="192.168.223.0/24",
                target_ip="10.9.9.9",
            )


class SnapshotNamingTest(unittest.TestCase):
    def test_snapshot_name_uses_prefix_job_and_seq(self):
        self.assertEqual(snapshot_name("abc123", 0), "mig-abc123-0")

    def test_snapshot_name_rejects_empty_job_id(self):
        with self.assertRaises(ValueError):
            snapshot_name("", 0)

    def test_snapshot_job_id_parses_migration_snapshot(self):
        self.assertEqual(snapshot_job_id("mig-abc123-7"), "abc123")

    def test_snapshot_job_id_ignores_user_snapshot(self):
        self.assertIsNone(snapshot_job_id("daily-backup"))

    def test_snapshot_job_id_ignores_malformed_name(self):
        self.assertIsNone(snapshot_job_id("mig-abc123"))

    def test_snapshots_to_prune_keeps_newest_of_same_job(self):
        names = ["mig-j1-2", "mig-j1-0", "mig-j1-1"]
        self.assertEqual(snapshots_to_prune(names, "j1", keep=2), ["mig-j1-0"])

    def test_snapshots_to_prune_ignores_other_jobs_and_user_snaps(self):
        names = ["mig-j1-0", "mig-j2-5", "daily"]
        self.assertEqual(snapshots_to_prune(names, "j1", keep=1), [])

    def test_snapshots_to_prune_keep_zero_removes_all_of_job(self):
        names = ["mig-j1-1", "mig-j1-0"]
        self.assertEqual(
            snapshots_to_prune(names, "j1", keep=0), ["mig-j1-0", "mig-j1-1"]
        )

    def test_orphan_snapshots_only_returns_inactive_jobs(self):
        names = ["mig-j1-0", "mig-j2-0", "daily"]
        self.assertEqual(orphan_snapshots(names, {"j2"}), ["mig-j1-0"])

    def test_orphan_snapshots_never_touch_user_snapshots(self):
        self.assertEqual(orphan_snapshots(["daily", "snap-1"], set()), [])


class VmChannelTest(unittest.TestCase):
    def test_per_vm_override_wins(self):
        options = {
            "data_channel": "rbd",
            "row_overrides": {"vm-1": {"data_channel": "relay"}},
        }

        self.assertEqual(vm_channel(options, "vm-1"), "relay")

    def test_falls_back_to_job_level(self):
        self.assertEqual(vm_channel({"data_channel": "relay"}, "vm-1"), "relay")

    def test_defaults_to_rbd(self):
        self.assertEqual(vm_channel({}, "vm-1"), "rbd")

    def test_unknown_vm_uses_job_level(self):
        options = {
            "data_channel": "relay",
            "row_overrides": {"vm-2": {"data_channel": "rbd"}},
        }

        self.assertEqual(vm_channel(options, "vm-1"), "relay")

    def test_lookup_by_server_id_matches_frontend_key(self):
        options = {
            "data_channel": "rbd",
            "row_overrides": {"srv-1": {"data_channel": "relay"}},
        }

        self.assertEqual(vm_channel(options, "vm-1", "srv-1"), "relay")

    def test_falls_back_to_vm_name_key(self):
        options = {
            "data_channel": "rbd",
            "row_overrides": {"vm-1": {"data_channel": "relay"}},
        }

        self.assertEqual(vm_channel(options, "vm-1", ""), "relay")


if __name__ == "__main__":
    unittest.main()

import tempfile
import unittest
from pathlib import Path

from environment_profiles import EnvironmentProfileStore
from relay_credentials import CredentialError, Sealer

KEY = b"k" * 32


class EnvironmentProfileStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "environment-profiles.json"
        self.store = EnvironmentProfileStore.load(self.path, Sealer(KEY))

    def tearDown(self):
        self.tmp.cleanup()

    def _payload(self, name="客户 A"):
        return {
            "name": name,
            "source_auth_url": "http://src:5000/v3",
            "source_project_name": "admin",
            "source_username": "admin",
            "source_password": "s3cret",
            "source_ceph_conf": "[global]\nmon_host = 1.2.3.4",
            "source_ceph_pool": "volumes",
            "target_auth_url": "http://dst:5000/v3",
            "target_project_id": "pid-1",
            "target_username": "admin",
            "target_password": "t3cret",
            "target_ceph_conf": "[global]\nmon_host = 5.6.7.8",
            "target_ceph_pool": "volumes",
            "target_volume_type": "ssd",
        }

    def test_save_flush_reload_roundtrip(self):
        saved = self.store.save(self._payload())
        self.store.flush()
        reloaded = EnvironmentProfileStore.load(self.path, Sealer(KEY))
        profile = reloaded.get(saved["profile_id"])
        self.assertIsNotNone(profile)
        self.assertEqual(reloaded.reveal(profile, "source_password"), "s3cret")
        self.assertEqual(
            reloaded.reveal(profile, "target_ceph_conf"), "[global]\nmon_host = 5.6.7.8"
        )

    def test_public_list_masks_secrets(self):
        self.store.save(self._payload())
        item = self.store.list_public()[0]
        self.assertNotIn("source_password", item)
        self.assertNotIn("source_ceph_conf", item)
        self.assertTrue(item["has_source_password"])
        self.assertTrue(item["has_source_ceph_conf"])

    def test_empty_secret_keeps_previous_value(self):
        saved = self.store.save(self._payload())
        updated = self.store.save(
            {
                "profile_id": saved["profile_id"],
                "name": "客户 A",
                "source_password": "",
                "source_ceph_conf": "",
            }
        )
        profile = self.store.get(updated["profile_id"])
        self.assertEqual(self.store.reveal(profile, "source_password"), "s3cret")
        self.assertEqual(
            self.store.reveal(profile, "source_ceph_conf"), "[global]\nmon_host = 1.2.3.4"
        )

    def test_new_secret_replaces_previous(self):
        saved = self.store.save(self._payload())
        self.store.save(
            {
                "profile_id": saved["profile_id"],
                "name": "客户 A",
                "source_password": "newpass",
            }
        )
        profile = self.store.get(saved["profile_id"])
        self.assertEqual(self.store.reveal(profile, "source_password"), "newpass")

    def test_duplicate_name_rejected(self):
        self.store.save(self._payload())
        with self.assertRaises(ValueError):
            self.store.save(self._payload())

    def test_delete(self):
        saved = self.store.save(self._payload())
        self.assertTrue(self.store.delete(saved["profile_id"]))
        self.assertFalse(self.store.delete(saved["profile_id"]))

    def test_corrupt_ciphertext_raises(self):
        saved = self.store.save(self._payload())
        profile = self.store.get(saved["profile_id"])
        profile.source_password = "not-a-token"
        with self.assertRaises(CredentialError):
            self.store.reveal(profile, "source_password")


if __name__ == "__main__":
    unittest.main()

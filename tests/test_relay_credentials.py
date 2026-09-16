import tempfile
import unittest
from pathlib import Path

from relay_credentials import CredentialError, CredentialStore, Sealer

KEY = b"k" * 32


class SealerTest(unittest.TestCase):
    def test_roundtrip(self):
        sealer = Sealer(KEY)
        token = sealer.seal("s3cret", aad="t1")
        self.assertEqual(sealer.unseal(token, aad="t1"), "s3cret")

    def test_two_seals_of_same_plaintext_differ(self):
        sealer = Sealer(KEY)
        self.assertNotEqual(sealer.seal("x", aad="t1"), sealer.seal("x", aad="t1"))

    def test_wrong_key_fails(self):
        token = Sealer(KEY).seal("s3cret", aad="t1")
        with self.assertRaises(CredentialError):
            Sealer(b"x" * 32).unseal(token, aad="t1")

    def test_aad_mismatch_fails(self):
        token = Sealer(KEY).seal("s3cret", aad="t1")
        with self.assertRaises(CredentialError):
            Sealer(KEY).unseal(token, aad="t2")

    def test_rejects_wrong_key_length(self):
        with self.assertRaises(CredentialError):
            Sealer(b"short")


class CredentialStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-credentials.json"
        self.store = CredentialStore.load(self.path, Sealer(KEY))

    def tearDown(self):
        self.tmp.cleanup()

    def test_save_flush_get_roundtrip(self):
        auth = {"auth_url": "http://src", "username": "admin", "password": "p@ss"}
        self.store.save("t1", auth)
        self.store.flush()

        reloaded = CredentialStore.load(self.path, Sealer(KEY)).get("t1")

        self.assertEqual(reloaded, auth)

    def test_get_missing_returns_none(self):
        self.assertIsNone(self.store.get("t1"))

    def test_file_contains_no_plaintext(self):
        self.store.save("t1", {"password": "p@ss"})
        self.store.flush()
        self.assertNotIn("p@ss", self.path.read_text(encoding="utf-8"))

    def test_tenants_sorted_and_delete(self):
        self.store.save("t2", {"password": "b"})
        self.store.save("t1", {"password": "a"})

        self.assertEqual(self.store.tenants(), ["t1", "t2"])
        self.assertTrue(self.store.delete("t1"))
        self.assertEqual(self.store.tenants(), ["t2"])

    def test_node_password_helpers_use_node_aad(self):
        token = self.store.seal_text("root-pass", aad="node-1")

        self.assertEqual(self.store.unseal_text(token, aad="node-1"), "root-pass")
        with self.assertRaises(CredentialError):
            self.store.unseal_text(token, aad="node-2")

    def test_from_env_requires_key(self):
        with self.assertRaises(CredentialError):
            CredentialStore.from_env(self.path, env={})

    def test_from_env_accepts_base64_key(self):
        import base64

        raw = base64.b64encode(KEY).decode("ascii")
        store = CredentialStore.from_env(self.path, env={"MIGRATION_SECRET_KEY": raw})
        self.assertEqual(store.tenants(), [])

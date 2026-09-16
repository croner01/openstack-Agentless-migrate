import tempfile
import unittest
from pathlib import Path

from relay_pool_profile import PoolProfile, PoolProfileStore


def _profile(*, role="source", az="nova-1", tenant="t1", image="img-1"):
    return PoolProfile(
        tenant_key=tenant,
        role=role,
        az=az,
        image=image,
        flavor="flv-1",
        network="net-1",
        subnet="sub-1",
    )


class PoolProfileStoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "relay-pools.json"

    def tearDown(self):
        self.tmp.cleanup()

    def test_upsert_then_reload_roundtrips(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile())
        store.save()

        profile = PoolProfileStore.load(self.path).get("t1", "source", "nova-1")

        self.assertEqual(profile.image, "img-1")
        self.assertEqual(profile.network, "net-1")

    def test_get_missing_returns_none(self):
        self.assertIsNone(
            PoolProfileStore.load(self.path).get("t1", "source", "nova-1")
        )

    def test_tenant_key_with_pipe_roundtrips(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile(tenant="http://keystone|project-1"))
        store.save()

        profile = PoolProfileStore.load(self.path).get(
            "http://keystone|project-1", "source", "nova-1"
        )

        self.assertEqual(profile.tenant_key, "http://keystone|project-1")

    def test_profiles_are_unique_per_pool_key(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile(image="img-1"))
        store.upsert(_profile(image="img-2"))

        self.assertEqual(len(store.all()), 1)
        self.assertEqual(store.get("t1", "source", "nova-1").image, "img-2")

    def test_remove(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile())

        self.assertTrue(store.remove("t1", "source", "nova-1"))
        self.assertIsNone(store.get("t1", "source", "nova-1"))

    def test_for_tenant_filters(self):
        store = PoolProfileStore.load(self.path)
        store.upsert(_profile())
        store.upsert(_profile(role="target"))
        store.upsert(_profile(tenant="t2"))

        self.assertEqual(len(store.for_tenant("t1")), 2)

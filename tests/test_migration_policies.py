"""迁移策略模板：白名单过滤、规则校验、持久化与 HTTP 接口。"""
import json
import os
import tempfile
import unittest

from migration_policies import (
    POLICY_PARAM_FIELDS,
    MigrationPolicyStore,
    normalize_params,
    normalize_rules,
)


class NormalizeParamsTest(unittest.TestCase):
    def test_only_whitelisted_fields_survive(self):
        params = normalize_params({
            "vm_concurrency": "4",
            "relay_slot_wait_seconds": 600,
            "admin_password": "secret",
            "source_password": "secret",
            "relay_admin_password": "secret",
            "unknown_field": "x",
        })

        self.assertEqual(
            params,
            {"vm_concurrency": "4", "relay_slot_wait_seconds": "600"},
        )
        self.assertNotIn("admin_password", params)

    def test_non_mapping_payload_is_empty(self):
        self.assertEqual(normalize_params(None), {})
        self.assertEqual(normalize_params(["a"]), {})

    def test_policy_fields_do_not_contain_secrets(self):
        for name in POLICY_PARAM_FIELDS:
            self.assertNotIn("password", name)
            self.assertNotIn("token", name)
            self.assertNotIn("ceph_conf", name)


class NormalizeRulesTest(unittest.TestCase):
    def test_rule_keeps_match_and_target(self):
        rules = normalize_rules([{
            "name": "web 池",
            "match": {"name_regex": "^web-", "min_disks": 2, "junk": "x"},
            "target": {"az": "az1", "channel": "relay", "junk": "x"},
        }])

        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]["match"], {"name_regex": "^web-", "min_disks": "2"})
        self.assertEqual(rules[0]["target"], {"az": "az1", "channel": "relay"})
        self.assertTrue(rules[0]["enabled"])

    def test_rule_without_match_or_target_is_dropped(self):
        rules = normalize_rules([
            {"match": {}, "target": {"az": "az1"}},
            {"match": {"az": "az1"}, "target": {}},
            {"match": {"az": "az1"}, "target": {"az": "az2"}},
        ])

        self.assertEqual(len(rules), 1)

    def test_rules_are_capped(self):
        payload = [
            {"match": {"az": f"az{i}"}, "target": {"az": "az2"}} for i in range(80)
        ]

        self.assertEqual(len(normalize_rules(payload)), 50)


class MigrationPolicyStoreTest(unittest.TestCase):
    def setUp(self):
        handle, self.path = tempfile.mkstemp(suffix=".json")
        os.close(handle)
        os.unlink(self.path)

    def tearDown(self):
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_save_load_and_delete_round_trip(self):
        store = MigrationPolicyStore.load(self.path)
        policy = store.save({
            "name": "夜间限速",
            "params": {"rate_limit_mb_s": "50", "vm_concurrency": "2"},
            "rules": [{"match": {"name_regex": "^db-"}, "target": {"az": "az-db"}}],
        })
        store.flush()

        reloaded = MigrationPolicyStore.load(self.path)
        items = reloaded.list_public()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["name"], "夜间限速")
        self.assertEqual(items[0]["params"]["rate_limit_mb_s"], "50")
        self.assertEqual(len(items[0]["rules"]), 1)

        self.assertTrue(reloaded.delete(policy.policy_id))
        reloaded.flush()
        self.assertEqual(MigrationPolicyStore.load(self.path).list_public(), [])

    def test_duplicate_name_is_rejected(self):
        store = MigrationPolicyStore.load(self.path)
        store.save({"name": "A", "params": {}})

        with self.assertRaises(ValueError):
            store.save({"name": "A", "params": {}})

    def test_unknown_policy_id_is_rejected(self):
        store = MigrationPolicyStore.load(self.path)

        with self.assertRaises(ValueError):
            store.save({"policy_id": "nope", "name": "A"})

    def test_empty_name_is_rejected(self):
        store = MigrationPolicyStore.load(self.path)

        with self.assertRaises(ValueError):
            store.save({"name": "   "})

    def test_saved_file_is_owner_only(self):
        store = MigrationPolicyStore.load(self.path)
        store.save({"name": "A", "params": {}})
        store.flush()

        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)


class PolicyParamsSyncTest(unittest.TestCase):
    """前端套用模板时按同一份字段名回填，两边漂移会让参数静默丢失。"""

    def test_frontend_param_list_matches_backend(self):
        import app as app_module

        html = app_module.app.test_client().get("/").get_data(as_text=True)
        block = html.split("const POLICY_PARAM_FIELDS = [", 1)[1].split("];", 1)[0]
        names = [item.strip().strip("'") for item in block.replace("\n", "").split(",")]
        names = [name for name in names if name]

        self.assertEqual(names, list(POLICY_PARAM_FIELDS))

    def test_policy_api_round_trip(self):
        import app as app_module

        with tempfile.TemporaryDirectory() as tmp:
            original = app_module.MIGRATION_POLICY_PATH
            app_module.MIGRATION_POLICY_PATH = os.path.join(tmp, "policies.json")
            try:
                client = app_module.app.test_client()
                saved = client.post(
                    "/api/policies",
                    json={"name": "接口策略", "params": {"vm_concurrency": "3"}},
                )
                self.assertEqual(saved.status_code, 200)
                payload = saved.get_json()
                self.assertTrue(payload["ok"])
                policy_id = payload["policy"]["policy_id"]

                listed = client.get("/api/policies").get_json()
                self.assertTrue(any(
                    item["policy_id"] == policy_id for item in listed["policies"]
                ))

                self.assertEqual(
                    client.delete(f"/api/policies/{policy_id}").status_code, 200
                )
                self.assertEqual(
                    client.delete(f"/api/policies/{policy_id}").status_code, 404
                )
                self.assertTrue(json.loads(saved.get_data(as_text=True))["ok"])
            finally:
                app_module.MIGRATION_POLICY_PATH = original

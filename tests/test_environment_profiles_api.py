import tempfile
import unittest
from pathlib import Path
from unittest import mock

import app as app_module
from relay_credentials import Sealer

KEY = b"k" * 32


class EnvironmentProfileApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        app_module.ENV_PROFILE_PATH = str(Path(self.tmp.name) / "profiles.json")
        self._orig_upload = app_module.app.config["UPLOAD_FOLDER"]
        app_module.app.config["UPLOAD_FOLDER"] = self.tmp.name
        patcher = mock.patch.object(
            app_module, "_environment_profile_sealer", return_value=Sealer(KEY)
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.client = app_module.app.test_client()

    def tearDown(self):
        app_module.app.config["UPLOAD_FOLDER"] = self._orig_upload
        self.tmp.cleanup()

    def _seed_profile(self):
        return self.client.post(
            "/api/profiles",
            json={
                "name": "客户 A",
                "source_auth_url": "http://src:5000/v3",
                "source_project_name": "admin",
                "source_username": "admin",
                "source_password": "s3cret",
                "source_ceph_conf": "[global]\nmon_host = 1.2.3.4",
                "target_auth_url": "http://dst:5000/v3",
                "target_project_id": "pid-1",
                "target_username": "admin",
                "target_password": "t3cret",
                "target_ceph_conf": "[global]\nmon_host = 5.6.7.8",
            },
        ).get_json()["profile"]

    def test_save_list_delete_roundtrip(self):
        res = self.client.post(
            "/api/profiles",
            json={
                "name": "客户 A",
                "source_auth_url": "http://src",
                "source_password": "s3cret",
            },
        )
        self.assertEqual(res.status_code, 200)
        profile = res.get_json()["profile"]
        self.assertTrue(profile["has_source_password"])
        self.assertNotIn("source_password", profile)

        listed = self.client.get("/api/profiles").get_json()["profiles"]
        self.assertEqual([item["name"] for item in listed], ["客户 A"])

        deleted = self.client.delete(f"/api/profiles/{profile['profile_id']}")
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(self.client.get("/api/profiles").get_json()["profiles"], [])

    def test_save_without_name_rejected(self):
        res = self.client.post("/api/profiles", json={"source_auth_url": "http://src"})
        self.assertEqual(res.status_code, 400)

    def test_delete_missing_returns_404(self):
        self.assertEqual(self.client.delete("/api/profiles/nope").status_code, 404)

    def test_auth_falls_back_to_profile_when_form_empty(self):
        profile = self._seed_profile()
        store = app_module._environment_profile_store()
        fallback = app_module._profile_auth(
            store.get(profile["profile_id"]), "source"
        )
        with app_module.app.test_request_context(
            "/api/migrate", method="POST", data={"profile_id": profile["profile_id"]}
        ):
            auth = app_module._auth_args("source", fallback)
        self.assertEqual(auth["auth_url"], "http://src:5000/v3")
        self.assertEqual(auth["password"], "s3cret")
        self.assertEqual(auth["project_name"], "admin")

    def test_form_value_overrides_profile(self):
        profile = self._seed_profile()
        store = app_module._environment_profile_store()
        fallback = app_module._profile_auth(
            store.get(profile["profile_id"]), "source"
        )
        with app_module.app.test_request_context(
            "/api/migrate",
            method="POST",
            data={"profile_id": profile["profile_id"], "source_username": "override"},
        ):
            auth = app_module._auth_args("source", fallback)
        self.assertEqual(auth["username"], "override")
        self.assertEqual(auth["password"], "s3cret")

    def test_profile_conf_written_into_job_dir(self):
        profile = self._seed_profile()
        store = app_module._environment_profile_store()
        with app_module.app.test_request_context(
            "/api/migrate",
            method="POST",
            data={
                "profile_id": profile["profile_id"],
                "selected_rows": (
                    '[{"server_id":"s1","vm_name":"vm1",'
                    '"target_az":"az1","target_image":"img1"}]'
                ),
            },
        ):
            _job_id, _job_dir, _rows, src_conf, dst_conf = (
                app_module._create_job_and_files(
                    require_ceph=True,
                    require_target_image=True,
                    profile=store.get(profile["profile_id"]),
                    store=store,
                )
            )
        self.assertTrue(src_conf.endswith("source_ceph.conf"))
        # 落盘时会补结尾换行：ceph 会静默丢掉没有换行的最后一行配置。
        self.assertEqual(Path(src_conf).read_text(), "[global]\nmon_host = 1.2.3.4\n")
        self.assertEqual(Path(dst_conf).read_text(), "[global]\nmon_host = 5.6.7.8\n")


if __name__ == "__main__":
    unittest.main()

# 方案驱动向导与环境档案 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把迁移控制台改造成"先选方案、按方案显隐配置"的向导，新增可复用的加密环境档案，并把迁移清单瘦身为摘要 + 展开 + 批量。

**Architecture:** 后端新增 `environment_profiles.py`（复用 `json_store` 与 `relay_credentials.Sealer`）与 3 个 `/api/profiles` 路由；`_create_job_and_files` / `_auth_args` 在缺省时回填档案。前端在同一个 `templates/index.html` 内新增方案选择器与分组显隐、档案读写、清单摘要化，并保持既有 DOM id/name 稳定，兼容现有渲染测试与提交协议。

**Tech Stack:** Python 3.10 / Flask / unittest / 原生 JS + 现有 CSS 变量 / Bootstrap / IBM Plex。

**Note:** 本仓库无 git 历史，且遵循"未经明确要求不提交"的约定，因此计划中的"提交"步骤替换为"运行验证"。

---

## File Structure

- Create `environment_profiles.py` — 档案 dataclass、加密读写、脱敏列表
- Create `tests/test_environment_profiles.py` — 存储层单测
- Create `tests/test_environment_profiles_api.py` — 路由单测
- Modify `app.py` — `/api/profiles` 三路由；`_auth_args` 回填；`_create_job_and_files`/`api_migrate` 支持 `profile_id`
- Modify `templates/index.html` — 方案选择器、分组显隐、档案 UI、清单摘要化、中转机入口
- Modify `tests/test_relay_ui_render.py` — 补充/更新 UI 断言
- Modify `README.md` — 方案矩阵与环境档案说明

---

### Task 1: 环境档案存储层

**Files:**
- Create: `environment_profiles.py`
- Test: `tests/test_environment_profiles.py`

- [ ] **Step 1: Write the failing test**

```python
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
            {"profile_id": saved["profile_id"], "name": "客户 A",
             "source_password": "", "source_ceph_conf": ""}
        )
        profile = self.store.get(updated["profile_id"])
        self.assertEqual(self.store.reveal(profile, "source_password"), "s3cret")
        self.assertEqual(self.store.reveal(profile, "source_ceph_conf"),
                         "[global]\nmon_host = 1.2.3.4")

    def test_new_secret_replaces_previous(self):
        saved = self.store.save(self._payload())
        self.store.save({"profile_id": saved["profile_id"], "name": "客户 A",
                         "source_password": "newpass"})
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_environment_profiles -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'environment_profiles'`

- [ ] **Step 3: Write minimal implementation**

```python
"""环境档案：源/目标 OpenStack 凭据与 Ceph conf 的加密存储。

明文只在内存出现；落盘字段是密文。复用中转机已有的 Sealer 与主密钥体系
（MIGRATION_SECRET_KEY / uploads/relay-master-key），不新增必填环境变量。
"""
from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from json_store import (
    atomic_write_json,
    dump_dataclass_records,
    load_dataclass_records,
)
from relay_credentials import CredentialError, Sealer

SECRET_FIELDS = (
    "source_password",
    "target_password",
    "source_ceph_conf",
    "target_ceph_conf",
)


@dataclass
class EnvironmentProfile:
    profile_id: str
    name: str
    source_auth_url: str = ""
    source_project_name: str = ""
    source_username: str = ""
    source_user_domain_name: str = "Default"
    source_project_domain_name: str = "Default"
    source_project_id: str = ""
    source_password: str = ""
    target_auth_url: str = ""
    target_project_name: str = ""
    target_username: str = ""
    target_user_domain_name: str = "Default"
    target_project_domain_name: str = "Default"
    target_project_id: str = ""
    target_password: str = ""
    source_ceph_conf: str = ""
    target_ceph_conf: str = ""
    source_ceph_pool: str = "volumes"
    target_ceph_pool: str = "volumes"
    target_volume_type: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0


class EnvironmentProfileStore:
    def __init__(self, path: str | os.PathLike[str], sealer: Sealer):
        self.path = Path(path)
        self.sealer = sealer
        self._records: dict[str, EnvironmentProfile] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str], sealer: Sealer) -> "EnvironmentProfileStore":
        store = cls(path, sealer)
        for profile in load_dataclass_records(
            store.path, key="profiles", cls=EnvironmentProfile
        ):
            store._records[profile.profile_id] = profile
        return store

    def list_public(self) -> list[dict[str, Any]]:
        return [self._public(item) for item in sorted(
            self._records.values(), key=lambda p: p.name
        )]

    def get(self, profile_id: str) -> EnvironmentProfile | None:
        return self._records.get(str(profile_id))

    def reveal(self, profile: EnvironmentProfile, field: str) -> str:
        token = getattr(profile, field, "")
        if not token:
            return ""
        return self.sealer.unseal(token, aad=self._aad(profile.profile_id, field))

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        profile_id = str(payload.get("profile_id") or "").strip()
        existing = self._records.get(profile_id) if profile_id else None
        if not profile_id:
            profile_id = uuid.uuid4().hex
        name = str(payload.get("name") or "").strip()
        if not name:
            raise ValueError("档案名称不能为空")
        if any(p.name == name and p.profile_id != profile_id
               for p in self._records.values()):
            raise ValueError(f"已存在同名档案：{name}")
        profile = existing or EnvironmentProfile(profile_id=profile_id, name=name)
        profile.name = name
        for field, value in payload.items():
            if field in {"profile_id"} or not hasattr(profile, field):
                continue
            if field in SECRET_FIELDS:
                text = str(value or "")
                if text:
                    setattr(profile, field, self.sealer.seal(
                        text, aad=self._aad(profile_id, field)
                    ))
                continue
            setattr(profile, field, str(value or ""))
        now = time.time()
        if not profile.created_at:
            profile.created_at = now
        profile.updated_at = now
        self._records[profile_id] = profile
        return self._public(profile)

    def delete(self, profile_id: str) -> bool:
        return self._records.pop(str(profile_id), None) is not None

    def flush(self) -> None:
        atomic_write_json(
            self.path,
            dump_dataclass_records(list(self._records.values()), key="profiles"),
            prefix=".environment-profiles-",
        )

    @staticmethod
    def _aad(profile_id: str, field: str) -> str:
        return f"{profile_id}:{field}"

    def _public(self, profile: EnvironmentProfile) -> dict[str, Any]:
        result = {
            key: value
            for key, value in vars(profile).items()
            if key not in SECRET_FIELDS
        }
        result["has_source_password"] = bool(profile.source_password)
        result["has_target_password"] = bool(profile.target_password)
        result["has_source_ceph_conf"] = bool(profile.source_ceph_conf)
        result["has_target_ceph_conf"] = bool(profile.target_ceph_conf)
        return result
```

**已核对：** `json_store.load_dataclass_records(path, *, key, cls)` 与 `dump_dataclass_records(records, *, key)` 均为关键字参数（见 `relay_pool_profile.py:115`）。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_environment_profiles -v`
Expected: PASS（7 项）

- [ ] **Step 5: Verify no regression**

Run: `python3 -m unittest tests.test_json_store tests.test_relay_credentials -v`
Expected: PASS

---

### Task 2: `/api/profiles` 路由

**Files:**
- Modify: `app.py`（import 区、`_environment_profile_store` 帮助函数、路由）
- Test: `tests/test_environment_profiles_api.py`

- [ ] **Step 1: Write the failing test**

```python
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
        patcher = mock.patch.object(
            app_module, "_environment_profile_sealer", return_value=Sealer(KEY)
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.client = app_module.app.test_client()

    def tearDown(self):
        self.tmp.cleanup()

    def test_save_list_delete_roundtrip(self):
        res = self.client.post("/api/profiles", json={
            "name": "客户 A",
            "source_auth_url": "http://src",
            "source_password": "s3cret",
        })
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
        self.assertEqual(
            self.client.delete("/api/profiles/nope").status_code, 404
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_environment_profiles_api -v`
Expected: FAIL（`ENV_PROFILE_PATH` 不存在 / 404）

- [ ] **Step 3: Implement routes**

在 `app.py` import 区新增：

```python
from environment_profiles import EnvironmentProfileStore
from relay_credentials import CredentialError, Sealer
from relay_secret import load_or_create_secret
```

（`relay_secret` 已 import `load_or_create_secret`，若重复则合并。）

在 `app.py` 的 `RELAY_MASTER_KEY_PATH` 定义之后新增：

```python
ENV_PROFILE_PATH = os.path.join(UPLOAD_FOLDER, "environment-profiles.json")


def _environment_profile_sealer() -> Sealer:
    return Sealer(
        load_or_create_secret(RELAY_MASTER_KEY_PATH, env_var="MIGRATION_SECRET_KEY")
    )


def _environment_profile_store() -> EnvironmentProfileStore:
    return EnvironmentProfileStore.load(ENV_PROFILE_PATH, _environment_profile_sealer())
```

在 `@app.route("/")` 之前新增：

```python
@app.get("/api/profiles")
def api_profiles_list():
    try:
        return jsonify({"ok": True, "profiles": _environment_profile_store().list_public()})
    except Exception as exc:  # noqa: BLE001 - 档案读取失败不应 500 暴露堆栈
        logging.exception("[MIGRATION] 环境档案列表失败")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/profiles")
def api_profiles_save():
    payload = request.get_json(silent=True) or {}
    try:
        store = _environment_profile_store()
        profile = store.save(payload)
        store.flush()
        return jsonify({"ok": True, "profile": profile})
    except (ValueError, CredentialError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.delete("/api/profiles/<profile_id>")
def api_profiles_delete(profile_id: str):
    try:
        store = _environment_profile_store()
        if not store.delete(profile_id):
            return jsonify({"ok": False, "error": "档案不存在"}), 404
        store.flush()
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        logging.exception("[MIGRATION] 环境档案删除失败")
        return jsonify({"ok": False, "error": str(exc)}), 400
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_environment_profiles_api -v`
Expected: PASS（3 项）

---

### Task 3: 作业提交支持 `profile_id`

**Files:**
- Modify: `app.py:165-181`（`_auth_args`）、`app.py:519-580`（`_create_job_and_files`）、`app.py:615-700`（`api_migrate`）
- Test: `tests/test_environment_profiles_api.py`（追加用例）

- [ ] **Step 1: Write the failing test**

```python
    def _seed_profile(self):
        return self.client.post("/api/profiles", json={
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
        }).get_json()["profile"]

    def test_auth_falls_back_to_profile_when_form_empty(self):
        profile = self._seed_profile()
        with app_module.app.test_request_context(
            "/api/migrate", method="POST", data={"profile_id": profile["profile_id"]}
        ):
            auth = app_module._auth_args("source", app_module._profile_auth(profile, "source"))
        self.assertEqual(auth["auth_url"], "http://src:5000/v3")
        self.assertEqual(auth["password"], "s3cret")

    def test_form_value_overrides_profile(self):
        profile = self._seed_profile()
        with app_module.app.test_request_context(
            "/api/migrate", method="POST",
            data={"profile_id": profile["profile_id"], "source_username": "override"},
        ):
            auth = app_module._auth_args(
                "source", app_module._profile_auth(profile, "source")
            )
        self.assertEqual(auth["username"], "override")
```

说明：`test_request_context` 里 `request.form` 可读；`_profile_auth` 接收 `list_public()` 的脱敏 dict 即可推导 auth（密码需从 store 解密，见 Step 3 实现——因此测试改为先 `get` 再 reveal，见下）。

```python
    def test_profile_conf_written_into_job_dir(self):
        profile = self._seed_profile()
        store = app_module._environment_profile_store()
        with app_module.app.test_request_context(
            "/api/migrate", method="POST",
            data={
                "profile_id": profile["profile_id"],
                "selected_rows": '[{"server_id":"s1","vm_name":"vm1","target_az":"az1","target_image":"img1"}]',
            },
        ):
            job_id, job_dir, rows, src_conf, dst_conf = app_module._create_job_and_files(
                require_ceph=True, require_target_image=True,
                profile=store.get(profile["profile_id"]), store=store,
            )
        self.assertTrue(src_conf.endswith("source_ceph.conf"))
        self.assertEqual(
            Path(src_conf).read_text(), "[global]\nmon_host = 1.2.3.4"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_environment_profiles_api -v`
Expected: FAIL（`_profile_auth` 未定义 / `_create_job_and_files` 不接受 `profile`）

- [ ] **Step 3: Implement**

3a. `_auth_args` 增加回填参数（保持原行为）：

```python
def _auth_args(prefix: str, fallback: dict[str, str] | None = None) -> dict[str, str]:
    fallback = fallback or {}

    def pick(key: str) -> str:
        value = request.form.get(f"{prefix}_{key}")
        if value is None or value == "":
            return fallback.get(key, "")
        return value

    base = {
        "auth_url": pick("auth_url"),
        "username": pick("username"),
        "password": pick("password"),
        "user_domain_name": pick("user_domain_name"),
    }
    project_id = pick("project_id")
    if project_id:
        base["project_id"] = project_id
    else:
        base["project_name"] = pick("project_name")
        base["project_domain_name"] = pick("project_domain_name")
    return {key: value for key, value in base.items() if value}
```

3b. 新增档案取用 helper：

```python
def _profile_auth(profile: EnvironmentProfile, prefix: str) -> dict[str, str]:
    """把档案里某一侧的凭据还原为 auth 字典（显式表单为空时的兜底）。"""
    store = _environment_profile_store()
    auth = {
        "auth_url": getattr(profile, f"{prefix}_auth_url", ""),
        "username": getattr(profile, f"{prefix}_username", ""),
        "user_domain_name": getattr(profile, f"{prefix}_user_domain_name", ""),
        "password": store.reveal(profile, f"{prefix}_password"),
    }
    project_id = getattr(profile, f"{prefix}_project_id", "")
    if project_id:
        auth["project_id"] = project_id
    else:
        auth["project_name"] = getattr(profile, f"{prefix}_project_name", "")
        auth["project_domain_name"] = getattr(
            profile, f"{prefix}_project_domain_name", ""
        )
    return {key: value for key, value in auth.items() if value}
```

3c. `_create_job_and_files` 签名与 conf 回填：

```python
def _create_job_and_files(
    *,
    require_ceph: bool = True,
    require_target_image: bool = True,
    profile: EnvironmentProfile | None = None,
    store: EnvironmentProfileStore | None = None,
) -> tuple:
```

把 `if require_ceph and (not source_conf_file or not target_conf_file)` 改为：

```python
    profile_conf = None
    if profile is not None and store is not None:
        if not source_conf_file or not target_conf_file:
            profile_conf = (
                store.reveal(profile, "source_ceph_conf"),
                store.reveal(profile, "target_ceph_conf"),
            )
    if require_ceph and (not source_conf_file or not target_conf_file) and not (
        profile_conf and all(profile_conf)
    ):
        raise ValueError("缺少源/目标 Ceph conf 文件")
```

文件写入部分改为（上传优先、档案兜底）：

```python
    source_conf_path = ""
    target_conf_path = ""
    if (source_conf_file and target_conf_file) or (profile_conf and all(profile_conf)):
        source_conf_path = os.path.join(job_dir, "source_ceph.conf")
        target_conf_path = os.path.join(job_dir, "target_ceph.conf")
        if source_conf_file and target_conf_file:
            source_conf_file.save(source_conf_path)
            target_conf_file.save(target_conf_path)
        else:
            Path(source_conf_path).write_text(profile_conf[0], encoding="utf-8")
            Path(target_conf_path).write_text(profile_conf[1], encoding="utf-8")
        os.chmod(source_conf_path, 0o600)
        os.chmod(target_conf_path, 0o600)
```

（`Path` 已在 `app.py` 使用？确认 import；若无则用 `open()` 写。）

3d. `api_migrate` 先取档案再建作业：

```python
        early_channel = (request.form.get("data_channel") or "rbd").strip().lower()
        profile_id = (request.form.get("profile_id") or "").strip()
        profile = None
        profile_store = None
        if profile_id:
            profile_store = _environment_profile_store()
            profile = profile_store.get(profile_id)
            if profile is None:
                return jsonify({"ok": False, "error": f"环境档案不存在：{profile_id}"}), 400
        (
            job_id,
            _job_dir,
            rows,
            source_conf_path,
            target_conf_path,
        ) = _create_job_and_files(
            require_ceph=early_channel != "relay",
            require_target_image=early_channel != "relay",
            profile=profile,
            store=profile_store,
        )
        source_auth = _auth_args(
            "source", _profile_auth(profile, "source") if profile else None
        )
        target_auth = _auth_args(
            "target", _profile_auth(profile, "target") if profile else None
        )
```

`options` 里两处 pool 与卷类型加档案兜底：

```python
            "source_ceph_pool": request.form.get("source_ceph_pool")
            or (profile.source_ceph_pool if profile else None)
            or "volumes",
            "target_ceph_pool": request.form.get("target_ceph_pool")
            or (profile.target_ceph_pool if profile else None)
            or "volumes",
            ...
            "target_volume_type": request.form.get("target_volume_type")
            or (profile.target_volume_type if profile else None)
            or None,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_environment_profiles_api tests.test_relay_app_wiring -v`
Expected: PASS

- [ ] **Step 5: Verify no regression**

Run: `python3 -m unittest discover -s tests 2>&1 | tail -5`
Expected: `OK`

---

### Task 4: 方案选择器与分组显隐

**Files:**
- Modify: `templates/index.html`（`<style>` 尾部 ~833、`.stepper` before、JS `state` ~1458、`switchStep` ~1797）
- Modify: `tests/test_relay_ui_render.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_scheme_picker_exists_with_three_schemes(self):
        html = self._html()
        self.assertIn('id="scheme-picker"', html)
        for scheme in ("rbd_full", "rbd_incremental", "relay_full"):
            self.assertIn(f'data-scheme="{scheme}"', html)
        self.assertIn("const SCHEMES", html)
        self.assertIn("function applyScheme", html)

    def test_config_groups_are_marked_for_visibility(self):
        html = self._html()
        for group in ("cfg-group-ceph", "cfg-group-delta", "cfg-group-relay"):
            self.assertIn(f'id="{group}"', html)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_ui_render.RelayPageRenderTest.test_scheme_picker_exists_with_three_schemes -v`
Expected: FAIL（未找到 `id="scheme-picker"`）

- [ ] **Step 3: Implement HTML**

在 `.topbar` 之后、`<nav class="stepper">` 之前插入：

```html
    <section class="scheme-picker" id="scheme-picker" aria-label="迁移方案">
        <button type="button" class="scheme-card active" data-scheme="rbd_full">
            <span class="scheme-head"><span class="scheme-name">RBD 直连 · 全量</span><span class="scheme-tag">推荐</span></span>
            <span class="scheme-desc">Ceph → Ceph，一次性导出导入</span>
        </button>
        <button type="button" class="scheme-card" data-scheme="rbd_incremental">
            <span class="scheme-head"><span class="scheme-name">RBD 直连 · 增量预拷贝</span><span class="scheme-tag warn">实验</span></span>
            <span class="scheme-desc">停机前多轮增量，缩短停机窗口</span>
        </button>
        <button type="button" class="scheme-card" data-scheme="relay_full">
            <span class="scheme-head"><span class="scheme-name">中转机 · 全量</span><span class="scheme-tag muted">商业存储</span></span>
            <span class="scheme-desc">iSCSI/FC，无 Ceph 对象</span>
        </button>
    </section>
    <div class="scheme-needs" id="scheme-needs"></div>
```

给现有分组容器加 id（只加属性，不动内部结构）：

- Ceph 凭据面板：`<div class="panel">`（Step 1 第一个 panel 内 Ceph conf 部分）→ 用 `<div class="panel" id="cfg-group-ceph">` 包住 Ceph conf + pool 字段；
- 增量参数：`id="cfg-group-delta"` 包住 `delta_rounds/delta_threshold_mb/delta_interval_seconds`；
- 中转机池：`<div class="panel hidden" id="cfg-group-relay">`（原 `id="relay-pool-panel"` 保留在同元素上：`<div class="panel hidden" id="relay-pool-panel">` 即可，追加 `data-group="relay"`）；
- 作业参数：`id="cfg-group-job"`（默认 `hidden`）。

**关键约束：** `id="relay-pool-panel"`、`name="relay_*"`、`id="data-channel"`、`id="global-mode"` 必须保留，`test_relay_ui_render.py` 依赖它们。

- [ ] **Step 4: Implement CSS**

在 `.relay-pool-grid` 定义之前追加：

```css
        .scheme-picker {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
            margin-bottom: 10px;
        }
        .scheme-card {
            text-align: left;
            padding: 12px 16px;
            border-radius: 12px;
            border: 1px solid var(--line);
            background: rgba(17, 26, 44, .75);
            color: var(--text);
            cursor: pointer;
            transition: border-color .15s, background .15s, transform .15s;
        }
        .scheme-card:hover { border-color: var(--line-strong); transform: translateY(-1px); }
        .scheme-card.active {
            border-color: var(--accent);
            background: linear-gradient(135deg, rgba(56,189,248,.14), rgba(17,26,44,.85));
        }
        .scheme-head { display: flex; align-items: center; gap: 8px; }
        .scheme-name { font-weight: 600; }
        .scheme-tag {
            font-size: 11px; padding: 1px 7px; border-radius: 999px;
            border: 1px solid rgba(56,189,248,.5); color: var(--accent);
        }
        .scheme-tag.warn { border-color: rgba(251,191,36,.5); color: var(--warn); }
        .scheme-tag.muted { border-color: var(--line-strong); color: var(--muted); }
        .scheme-desc { display: block; margin-top: 6px; color: var(--muted); font-size: 12px; }
        .scheme-needs {
            margin-bottom: 18px; color: var(--muted); font-size: 12px;
        }
        @media (max-width: 900px) { .scheme-picker { grid-template-columns: 1fr; } }
```

- [ ] **Step 5: Implement JS**

在 `state` 对象里加 `scheme: 'rbd_full'`。新增：

```js
const SCHEMES = {
    rbd_full: {
        channel: 'rbd', mode: 'full', groups: ['ceph', 'job'],
        needs: '源/目标凭据 · 源/目标 Ceph conf · 目标镜像 · 逐台 AZ/Flavor/网络'
    },
    rbd_incremental: {
        channel: 'rbd', mode: 'incremental', groups: ['ceph', 'delta', 'job'],
        needs: 'RBD 全量所需 + 增量轮次/阈值/间隔'
    },
    relay_full: {
        channel: 'relay', mode: 'full', groups: ['relay'],
        needs: '源/目标凭据 · 中转机池（常驻池只需两侧 AZ）· 逐台 AZ/Flavor/网络'
    }
};

function applyScheme(scheme) {
    const config = SCHEMES[scheme] || SCHEMES.rbd_full;
    state.scheme = scheme;
    $$('.scheme-card').forEach(card =>
        card.classList.toggle('active', card.dataset.scheme === scheme)
    );
    ['ceph', 'delta', 'relay', 'job'].forEach(group => {
        const node = document.getElementById('cfg-group-' + group);
        if (node) node.classList.toggle('hidden', !config.groups.includes(group));
    });
    const needs = $('#scheme-needs');
    if (needs) needs.textContent = '本方案需要填写：' + config.needs;
    const channel = $('#data-channel');
    if (channel) channel.value = config.channel;
    const mode = $('#global-mode');
    if (mode) mode.value = config.mode;
    const relayConfig = $('#relay-pool-config');
    if (relayConfig) relayConfig.classList.toggle('hidden', config.channel !== 'relay');
}

function initSchemePicker() {
    $$('.scheme-card').forEach(card => {
        card.addEventListener('click', () => applyScheme(card.dataset.scheme));
    });
    applyScheme(state.scheme);
}
```

在 DOMContentLoaded 初始化链（搜索 `initRelayPanel()` 的调用处）追加 `initSchemePicker();`，并让 `initRelayPanel` 的 `#data-channel` 监听改为调用 `applyScheme` 对应的可见性同步（保留监听以防外部改值）：

```js
    $('#data-channel').addEventListener('change', event => {
        $('#relay-pool-config').classList.toggle('hidden', event.target.value !== 'relay');
    });
```

- [ ] **Step 6: Run tests**

Run: `python3 -m unittest tests.test_relay_ui_render -v`
Expected: PASS（含新增 2 项）

---

### Task 5: 环境档案前端（载入/保存/删除/提交）

**Files:**
- Modify: `templates/index.html`（Step 1 顶部新增档案区块、JS 新增函数、`startMigration` 追加 `profile_id`、`applyScheme` 后初始化）
- Modify: `tests/test_relay_ui_render.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_profile_panel_exists(self):
        html = self._html()
        for element_id in ("profile-panel", "profile-select", "profile-save", "profile-delete"):
            self.assertIn(f'id="{element_id}"', html)
        self.assertIn("async function loadProfiles", html)
        self.assertIn("async function saveProfile", html)
        self.assertIn("data.append('profile_id'", html)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_ui_render.RelayPageRenderTest.test_profile_panel_exists -v`
Expected: FAIL

- [ ] **Step 3: Implement HTML**

在 Step 1 第一个凭据 panel 之前插入：

```html
        <div class="panel" id="profile-panel">
            <div class="d-flex align-items-center justify-content-between">
                <div>
                    <div class="panel-title mb-0">环境档案</div>
                    <div class="text-muted small">保存源/目标凭据与 Ceph conf，换一次迁移直接载入，密码不回显。</div>
                </div>
                <div class="d-flex align-items-center gap-2">
                    <select class="form-control" id="profile-select" style="min-width:220px">
                        <option value="">（不使用档案，手工填写）</option>
                    </select>
                    <button type="button" class="btn btn-ghost btn-sm" id="profile-save">保存为新档案</button>
                    <button type="button" class="btn btn-ghost btn-sm" id="profile-delete">删除</button>
                </div>
            </div>
            <div class="form-group mt-3 mb-0">
                <label class="form-label" for="profile-name">档案名称</label>
                <input class="form-control" id="profile-name" placeholder="例如 客户A-生产迁移">
            </div>
        </div>
```

- [ ] **Step 4: Implement JS**

```js
async function loadProfiles(selectedId) {
    const select = $('#profile-select');
    if (!select) return;
    try {
        const res = await fetch('/api/profiles');
        const json = await res.json();
        if (!json.ok) throw new Error(json.error || '加载失败');
        select.innerHTML = '';
        select.appendChild(new Option('（不使用档案，手工填写）', ''));
        (json.profiles || []).forEach(profile => {
            select.appendChild(new Option(profile.name, profile.profile_id));
        });
        if (selectedId) select.value = selectedId;
    } catch (err) {
        toast('环境档案加载失败：' + err.message, 'error');
    }
}

function fillProfileForm(profile) {
    if (!profile) return;
    ['source', 'target'].forEach(prefix => {
        ['auth_url', 'project_name', 'username', 'user_domain_name',
         'project_domain_name', 'project_id'].forEach(field => {
            const el = $(`[name="${prefix}_${field}"]`);
            if (el) el.value = profile[`${prefix}_${field}`] || '';
        });
        const password = $(`[name="${prefix}_password"]`);
        if (password) {
            password.value = '';
            password.placeholder = profile[`has_${prefix}_password`]
                ? '已配置（留空沿用档案）' : '';
        }
    });
    ['source_ceph_pool', 'target_ceph_pool', 'target_volume_type'].forEach(name => {
        const el = $(`[name="${name}"]`);
        if (el && profile[name]) el.value = profile[name];
    });
    const confHint = $('#profile-ceph-hint');
    if (confHint) {
        confHint.textContent = profile.has_source_ceph_conf && profile.has_target_ceph_conf
            ? '档案已含 Ceph conf，无需再上传文件' : '档案未含 Ceph conf，请上传文件';
    }
}

async function saveProfile() {
    const name = ($('#profile-name').value || '').trim();
    if (!name) { toast('请填写档案名称', 'error'); return; }
    const payload = { name };
    ['source', 'target'].forEach(prefix =>
        Object.assign(payload, readAuth(prefix))
    );
    payload.source_password = $('[name="source_password"]').value;
    payload.target_password = $('[name="target_password"]').value;
    ['source_ceph_pool', 'target_ceph_pool', 'target_volume_type'].forEach(item => {
        payload[item] = $(`[name="${item}"]`).value.trim();
    });
    const sourceConf = $('[name="source_ceph_conf_file"]').files[0];
    const targetConf = $('[name="target_ceph_conf_file"]').files[0];
    if (sourceConf) payload.source_ceph_conf = await sourceConf.text();
    if (targetConf) payload.target_ceph_conf = await targetConf.text();
    const selected = $('#profile-select').value;
    if (selected) payload.profile_id = selected;
    try {
        const json = await postJson('/api/profiles', payload);
        await loadProfiles(json.profile.profile_id);
        $('#profile-name').value = json.profile.name;
        toast('档案已保存', 'success');
    } catch (err) {
        toast('档案保存失败：' + err.message, 'error');
    }
}

async function deleteProfile() {
    const selected = $('#profile-select').value;
    if (!selected) { toast('请先选择一个档案', 'error'); return; }
    try {
        await postJson('/api/profiles/' + selected, {});
    } catch (err) {
        // postJson 只处理 POST；删除改用 fetch
    }
    const res = await fetch('/api/profiles/' + selected, { method: 'DELETE' });
    const json = await res.json();
    if (!json.ok) { toast('删除失败：' + (json.error || ''), 'error'); return; }
    await loadProfiles('');
    toast('档案已删除', 'success');
}

async function initProfilePanel() {
    if (!$('#profile-panel')) return;
    await loadProfiles('');
    $('#profile-save').addEventListener('click', saveProfile);
    $('#profile-delete').addEventListener('click', deleteProfile);
    $('#profile-select').addEventListener('change', async event => {
        const id = event.target.value;
        if (!id) return;
        const res = await fetch('/api/profiles');
        const json = await res.json();
        const profile = (json.profiles || []).find(item => item.profile_id === id);
        if (profile) {
            fillProfileForm(profile);
            $('#profile-name').value = profile.name;
        }
    });
}
```

并在初始化链追加 `initProfilePanel();`。

在 `startMigration` 里 `data.append('data_channel', ...)` 附近加：

```js
    if ($('#profile-select') && $('#profile-select').value) {
        data.append('profile_id', $('#profile-select').value);
    }
```

同时在 Ceph conf 字段旁新增提示节点 `<div class="text-muted small" id="profile-ceph-hint"></div>`。

- [ ] **Step 5: Run tests**

Run: `python3 -m unittest tests.test_relay_ui_render -v`
Expected: PASS

---

### Task 6: 迁移清单摘要 + 自动映射 + 批量

**Files:**
- Modify: `templates/index.html`（`renderPreview` ~2384、`fillSourceNetworksForRows` ~2350、`refreshChecklistUI` ~2701、CSS ~536）
- Modify: `tests/test_relay_ui_render.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_checklist_has_summary_and_bulk_controls(self):
        html = self._html()
        self.assertIn("plan-card-body", html)
        self.assertIn("function togglePlanCard", html)
        self.assertIn("function autoMapNetworks", html)
        self.assertIn("批量应用到已选 VM", html)
        self.assertIn("inherited", html)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_ui_render.RelayPageRenderTest.test_checklist_has_summary_and_bulk_controls -v`
Expected: FAIL

- [ ] **Step 3: Implement card wrapper + collapse**

在 `renderPreview` 的卡片构建里，把 `head` 与明细包进两层容器：摘要行 `head` 常显，明细放 `plan-card-body`（默认 `hidden`），标题行加展开按钮：

```js
        const expandBtn = document.createElement('button');
        expandBtn.type = 'button';
        expandBtn.className = 'btn btn-ghost btn-sm plan-expand';
        expandBtn.textContent = '展开';
        expandBtn.addEventListener('click', () => togglePlanCard(card, expandBtn));
        head.appendChild(expandBtn);
```

新增：

```js
function togglePlanCard(card, button) {
    const body = $('.plan-card-body', card);
    if (!body) return;
    const hidden = body.classList.toggle('hidden');
    button.textContent = hidden ? '展开' : '收起';
}
```

**约束：** 卷类型逐卷选择器、`classList.contains('volume-row')` 两处判断、`channelSelect.dataset.role = 'data-channel'` 必须保留（既有测试依赖）。

- [ ] **Step 4: Implement auto-map**

在 `fillSourceNetworksForRows` 完成后调用：

```js
function autoMapNetworks() {
    const subnets = state.targetCatalog.subnets || [];
    const networks = state.targetCatalog.networks || [];
    state.previewRows.forEach(row => {
        (row.source_networks || []).forEach(src => {
            const entry = (row._networkPlan = row._networkPlan || {})[src.ip] || {};
            if (entry.target_subnet) return;
            const sourceCidr = src.cidr || '';
            const match = subnets.find(item => item.cidr && item.cidr === sourceCidr)
                || subnets.find(item => item.name === src.network)
                || networks.find(item => item.name === src.network);
            if (match) entry.target_subnet = match.id;
            entry.target_network = entry.target_network
                || (networks.find(item => item.name === src.network) || {}).id;
            row._networkPlan[src.ip] = entry;
        });
    });
    renderPreview();
}
```

在工具栏按钮组加「自动映射目标网络」按钮调用它。**说明：** 具体写入 `vmOverrides` 的路径以 `collectPreviewValues` 为准；若 `renderPreview` 重建 DOM 会丢失自动选择，则把 `autoMapNetworks` 的结果先写入 `state.previewRows[i]` 的持久字段（如 `row.target_subnet_by_ip`），并在 `renderPreview` 渲染 `nic-row` 时优先采用该字段，再叠加用户手选。

- [ ] **Step 5: Implement bulk apply**

工具栏加：

```html
<button type="button" class="btn btn-ghost btn-sm" id="bulk-apply">批量应用到已选 VM</button>
```

```js
function applyBulkToSelected() {
    const az = ($('#bulk-az') && $('#bulk-az').value) || '';
    const image = ($('#global-image') && $('#global-image').value) || '';
    state.previewRows.forEach(row => {
        if (!state.selectedServerIds.has(row.server_id) && az) row.target_az = az;
        if (image) row.target_image = image;
    });
    renderPreview();
}
```

`#bulk-az` 为工具栏新增的可选目标 AZ 下拉（空值表示不改）。绑定在初始化链。

- [ ] **Step 6: Run tests**

Run: `python3 -m unittest tests.test_relay_ui_render -v`
Expected: PASS

---

### Task 7: 中转机入口迁移到方案内

**Files:**
- Modify: `templates/index.html`（`<nav class="stepper">`、`switchStep`、relay 分组标题、`initRelayResourcePage`）
- Modify: `tests/test_relay_ui_render.py`

- [ ] **Step 1: Write the failing test**

```python
    def test_relay_resource_page_not_in_main_nav(self):
        html = self._html()
        self.assertNotIn('data-step="resources"', html)

    def test_relay_ops_entry_lives_in_relay_group(self):
        html = self._html()
        self.assertIn('id="relay-ops-open"', html)
        self.assertIn("function openRelayOps", html)

    def test_existing_relay_markup_is_preserved(self):
        html = self._html()
        for element_id in ("relay-resource-page", "relay-node-grid",
                           "relay-tenant-credentials", "relay-scale-form"):
            self.assertIn(f'id="{element_id}"', html)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_relay_ui_render.RelayPageRenderTest.test_relay_resource_page_not_in_main_nav -v`
Expected: FAIL（当前 nav 含 `data-step="resources"`）

- [ ] **Step 3: Implement**

- 从 `<nav class="stepper">` 删除「中转机资源」按钮；
- 在 `cfg-group-relay` 标题右侧加：

```html
<button type="button" class="btn btn-ghost btn-sm" id="relay-ops-open">中转机运维</button>
```

- `switchStep` 简化为只处理 1-3；新增：

```js
function openRelayOps() {
    $$('.step-btn').forEach(btn => btn.classList.remove('active'));
    for (let i = 1; i <= 3; i++) $('#step-' + i).classList.add('hidden');
    const page = $('#relay-resource-page');
    page.classList.remove('hidden');
    page.dataset.opsOpen = '1';
    refreshRelayResourcePage();
}

function closeRelayOps() {
    if ($('#relay-resource-page')) $('#relay-resource-page').classList.add('hidden');
    switchStep(1);
}
```

- `#relay-resource-page` 顶部加「返回迁移向导」按钮绑定 `closeRelayOps`；
- 临时池模式下把网络/子网/IP/端口/心跳/空洞/整卷校验等收进 `<details>`「高级」（仅结构调整，`name="relay_*"` 全保留）。

- [ ] **Step 4: Run tests**

Run: `python3 -m unittest tests.test_relay_ui_render -v`
Expected: PASS（含新增 3 项）

---

### Task 8: 文档与全量验证

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README**

在「功能」段新增：

```markdown
迁移方案在页面顶部显式选择，共 3 种：

- `RBD 直连 · 全量`（默认）：源/目标 Ceph conf，一次性导出导入；
- `RBD 直连 · 增量预拷贝`（实验）：额外配置轮次/阈值/间隔；
- `中转机 · 全量`：适用于 iSCSI/FC 商业存储，配置两侧中转机池
  （常驻池作业表单只需两侧 AZ，其余在「中转机运维」维护）。

环境配置页支持「环境档案」：把源/目标 OpenStack 凭据与 Ceph conf 加密
保存（`uploads/environment-profiles.json`，0600），提交作业时选择档案即可
复用；密码与 conf 不回显。不选档案时行为与旧版一致。
```

- [ ] **Step 2: Run full suite**

Run: `python3 -m unittest discover -s tests 2>&1 | tail -5`
Expected: `OK`

- [ ] **Step 3: Manual verification**

- `python app.py` → `http://localhost:19099/`；
- 切换 3 个方案，确认可见分组与方案矩阵一致；
- 保存档案 → 刷新 → 载入 → 密码占位为「已配置（留空沿用档案）」；
- 清空表单只选档案提交，确认 conf 落到 `uploads/<job_id>/source_ceph.conf`；
- 加载源 VM → 自动映射 → 批量应用 → 逐台覆盖 → 开始迁移；
- 确认非 relay 方案下看不到「中转机运维」，relay 方案下可达；
- 窄屏 ≤900px 摘要行不溢出。

---

## Self-Review

- **Spec coverage:** 方案选择器/分组显隐 = Task 4；环境档案存储 = Task 1；API = Task 2；提交衔接 = Task 3；清单摘要/自动映射/批量 = Task 6；中转机入口 = Task 7；README/验证 = Task 8。spec 第 2 节矩阵由 `SCHEMES` + `applyScheme` 落地；第 5.4 节交互由 Task 5 落地。
- **Placeholder scan:** 无 TBD/TODO；所有新增函数均给出完整代码；前端改动给出锚点与保留约束。
- **Type consistency:** `EnvironmentProfileStore.load/list_public/get/reveal/save/delete/flush` 在 Task 1 定义，Task 2/3 一致引用；`_auth_args(prefix, fallback)`、`_profile_auth(profile, prefix)`、`_create_job_and_files(..., profile, store)` 签名在 Task 3 内自洽。
- **风险点:** Task 6 的自动映射需与 `collectPreviewValues` 的 DOM 取值路径对齐（Step 4 已给出回退策略）；Task 4 必须保留既有 `id`/`name` 以免破坏 `test_relay_ui_render.py`。

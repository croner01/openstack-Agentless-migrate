# 布局对齐的 RBD 增量迁移 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把增量迁移做成与既有全量迁移并列的独立功能项：默认全量行为不变、增量按 VM 显式开启，两条路径共用布局对齐与 stage+rename 基础能力，迁移快照独立治理并在完成后清理。

**Architecture:** `MigrationRow`/`VmTask`/`VolumeTask` 新增 `mode`（`full`/`incremental`）；`MigrationManager._copy_volumes` 按 mode 分派到 `CephUtils.replace_rbd_data`（全量，保持现行为）或 `precopy_volume` + `finalize_incremental_volume`（增量）；布局解析/比对、建像参数、快照原语放在 `ceph_utils.py`，快照命名/保留/孤儿选择的纯逻辑放在 `migration_planner.py`，不新增 Python 模块。

**Tech Stack:** Flask、openstacksdk、Ceph `rbd` CLI、标准库 `unittest`。

---

## 说明

当前目录不是 git 仓库（`.git` 为空），所有任务跳过 commit 步骤。改动全部落在
已挂载文件，不新增 Python 模块，因此 ConfigMap `--from-file` 列表无需变化。

对应设计文档：`docs/superpowers/specs/2026-09-10-rbd-layout-aligned-incremental-migration-design.md`。

## 文件结构

| 文件 | 职责 |
| --- | --- |
| `migration_planner.py` | 新增快照命名、归属判断、保留与孤儿选择（纯函数，无 I/O） |
| `state_machine.py` | 新增 `MigrationMode`；`VolumeTask`/`VmTask` 增加 mode、布局与快照字段 |
| `ceph_utils.py` | 新增布局解析/比对、建像参数、快照原语、diff 传输、增量预拷贝与最终切换 |
| `migration_manager.py` | 按 mode 分派策略；增量阶段编排；快照清理 |
| `job_manager.py` | 活跃 job 集合与迁移快照孤儿回收（GC） |
| `app.py` | 传递 `mode` 与 `job_id`；启动 GC 线程 |
| `excel_parser.py` | `MigrationRow.mode` 解析与校验 |
| `templates/index.html` | 逐 VM 模式选择、"以全量重跑"入口与快照状态展示 |
| `tests/` | 上述行为的单元测试 |

---

### Task 1: 快照命名与保留纯逻辑

**Files:**
- Modify: `migration_planner.py`
- Test: `tests/test_migration_planner.py`

- [ ] **Step 1: Write the failing test**

追加到 `tests/test_migration_planner.py`（放在文件末尾、`if __name__` 之前）：

```python
from migration_planner import (
    orphan_snapshots,
    snapshot_job_id,
    snapshot_name,
    snapshots_to_prune,
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_migration_planner -v`
Expected: FAIL with `ImportError: cannot import name 'snapshot_name'`

- [ ] **Step 3: Write minimal implementation**

追加到 `migration_planner.py` 末尾：

```python
SNAPSHOT_PREFIX = "mig-"


def snapshot_name(job_id: str, seq: int) -> str:
    """迁移快照统一命名：mig-<job_id>-<seq>。"""
    if not job_id:
        raise ValueError("job_id 不能为空")
    if seq < 0:
        raise ValueError("seq 不能为负数")
    return f"{SNAPSHOT_PREFIX}{job_id}-{seq}"


def is_migration_snapshot(name: str) -> bool:
    return bool(name) and str(name).startswith(SNAPSHOT_PREFIX)


def snapshot_job_id(name: str) -> str | None:
    """解析迁移快照的归属 job_id；非迁移快照或格式不符返回 None。"""
    if not is_migration_snapshot(name):
        return None
    head, sep, tail = str(name).rpartition("-")
    if not sep or not tail.isdigit():
        return None
    job_id = head[len(SNAPSHOT_PREFIX):]
    return job_id or None


def _snapshot_seq(name: str) -> int:
    return int(str(name).rpartition("-")[2])


def snapshots_to_prune(names: list[str], job_id: str, keep: int) -> list[str]:
    """返回本 job 该删的迁移快照，按 seq 升序保留最新 keep 个。"""
    mine = sorted(
        (name for name in names if snapshot_job_id(name) == job_id),
        key=_snapshot_seq,
    )
    if keep <= 0:
        return mine
    return mine[:-keep]


def orphan_snapshots(names: list[str], active_job_ids: set[str]) -> list[str]:
    """返回归属 job 已结束/不存在的迁移快照；非迁移快照一律不动。"""
    return sorted(
        name
        for name in names
        if (job_id := snapshot_job_id(name)) and job_id not in active_job_ids
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_migration_planner -v`
Expected: PASS（新增 10 个用例通过，原有 flavor/IP/映射用例不受影响）

---

### Task 2: 迁移模式与快照字段

**Files:**
- Modify: `state_machine.py`
- Test: `tests/test_state_machine.py`

- [ ] **Step 1: Write the failing test**

追加到 `tests/test_state_machine.py`（放在 `if __name__` 之前）：

```python
from state_machine import MigrationMode


class MigrationModePersistenceTest(unittest.TestCase):
    def test_volume_defaults_to_full_mode(self):
        volume = VolumeTask(source_volume_id="s1", source_rbd_name="volume-s1")
        self.assertEqual(volume.mode, MigrationMode.FULL)
        self.assertEqual(volume.cleanup_status, "none")

    def test_volume_round_trip_keeps_mode_layout_and_snapshots(self):
        volume = VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            source_pool="volumes",
            target_pool="volumes",
            mode=MigrationMode.INCREMENTAL,
            layout={"order": 20, "size": 10737418240},
            snapshots=[{"name": "mig-j1-0", "state": "created"}],
        )
        restored = VolumeTask.from_dict(volume.to_dict())
        self.assertEqual(restored.mode, MigrationMode.INCREMENTAL)
        self.assertEqual(restored.layout["order"], 20)
        self.assertEqual(restored.snapshots[0]["name"], "mig-j1-0")
        self.assertEqual(restored.source_pool, "volumes")

    def test_vm_round_trip_keeps_mode(self):
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        restored = VmTask.from_dict(vm.to_dict())
        self.assertEqual(restored.mode, MigrationMode.INCREMENTAL)

    def test_unknown_mode_falls_back_to_full(self):
        restored = VmTask.from_dict({"name": "vm1", "target_az": "az1", "mode": "v2"})
        self.assertEqual(restored.mode, MigrationMode.FULL)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_state_machine -v`
Expected: FAIL with `ImportError: cannot import name 'MigrationMode'`

- [ ] **Step 3: Write minimal implementation**

在 `state_machine.py` 的 `VolumeStatus` 之后新增：

```python
class MigrationMode(str, Enum):
    """迁移功能项：全量（既有）与增量（新增），两者相互独立。"""

    FULL = "full"
    INCREMENTAL = "incremental"

    @classmethod
    def parse(cls, raw: object) -> "MigrationMode":
        try:
            return cls(str(raw or cls.FULL.value).strip().lower())
        except ValueError:
            return cls.FULL
```

`VolumeTask` 新增字段（放在 `throughput_mb_s` 之后）：

```python
    source_pool: str = ""
    target_pool: str = ""
    mode: MigrationMode = MigrationMode.FULL
    layout: dict[str, Any] | None = None
    snapshots: list[dict[str, Any]] = field(default_factory=list)
    cleanup_status: str = "none"
```

`VolumeTask.to_dict()` 追加：

```python
            "source_pool": self.source_pool,
            "target_pool": self.target_pool,
            "mode": self.mode.value,
            "layout": self.layout,
            "snapshots": self.snapshots,
            "cleanup_status": self.cleanup_status,
```

`VolumeTask.from_dict()` 追加：

```python
            source_pool=data.get("source_pool") or "",
            target_pool=data.get("target_pool") or "",
            mode=MigrationMode.parse(data.get("mode")),
            layout=data.get("layout"),
            snapshots=data.get("snapshots") or [],
            cleanup_status=data.get("cleanup_status") or "none",
```

`VmTask` 新增字段（放在 `target_az` 之后）：

```python
    mode: MigrationMode = MigrationMode.FULL
```

`VmTask.to_dict()` 追加 `"mode": self.mode.value,`；`VmTask.from_dict()` 追加
`mode=MigrationMode.parse(data.get("mode")),`。

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_state_machine -v`
Expected: PASS

---

### Task 3: 布局解析与比对

**Files:**
- Modify: `ceph_utils.py`
- Test: `tests/test_ceph_utils.py`

- [ ] **Step 1: Write the failing test**

先扩展 `tests/test_ceph_utils.py` 的 `FakeRunner`，新增构造参数与按镜像名匹配的
返回逻辑，以便同一用例里让源与暂存映像返回不同的 `rbd info`：

```python
    def __init__(
        self,
        stdout_by_index=None,
        raise_on_command=None,
        status_watchers: list | None = None,
        snap_list: list | None = None,
        info_by_spec: dict | None = None,
        missing_once: list | None = None,
        snaps_by_image: dict | None = None,
    ):
        self.stdout_by_index = list(stdout_by_index or [])
        self.raise_on_command = raise_on_command or []
        self.status_watchers = status_watchers if status_watchers is not None else []
        self.snap_list = snap_list if snap_list is not None else []
        self.info_by_spec = dict(info_by_spec or {})
        self.missing_once = list(missing_once or [])
        self.snaps_by_image = dict(snaps_by_image or {})
        self.calls = []

    @staticmethod
    def _match(text: str, table: dict):
        # 长键优先，"volume-dst-1-mig-stage" 必须优先于 "volume-dst-1"
        for key in sorted(table, key=len, reverse=True):
            if key and key in text:
                return table[key]
        return None
```

把 `FakeRunner.__call__` 改为：

```python
    def __call__(self, command, **kwargs):
        self.calls.append(command)
        command_text = command if isinstance(command, str) else " ".join(command)
        if any(tag in command_text for tag in self.raise_on_command):
            import subprocess

            raise subprocess.CalledProcessError(2, command)
        for spec in list(self.missing_once):
            if spec in command_text:
                self.missing_once.remove(spec)
                import subprocess

                raise subprocess.CalledProcessError(2, command)
        if "info --format json" in command_text:
            scripted = self._match(command_text, self.info_by_spec)
            if scripted is not None:
                return CompletedFake(scripted)
            if self.stdout_by_index:
                return CompletedFake(self.stdout_by_index.pop(0))
            return CompletedFake(json.dumps({"size": 10}))
        if "status --format json" in command_text:
            return CompletedFake(json.dumps({"watchers": self.status_watchers}))
        if "snap ls" in command_text:
            scripted = self._match(command_text, self.snaps_by_image)
            if scripted is not None:
                return CompletedFake(json.dumps(scripted))
            return CompletedFake(json.dumps(self.snap_list))
        return CompletedFake("")
```

再追加用例：

```python
from ceph_utils import layout_mismatches, parse_layout


class LayoutCompareTest(unittest.TestCase):
    def test_features_accepts_string_and_list(self):
        as_list = parse_layout({"features": ["fast-diff", "layering"]})
        as_str = parse_layout({"features": "layering, fast-diff"})
        self.assertEqual(as_list["features"], ["fast-diff", "layering"])
        self.assertEqual(as_str["features"], ["fast-diff", "layering"])

    def test_identical_layout_has_no_mismatch(self):
        info = {
            "size": 100,
            "order": 20,
            "stripe_unit": 4096,
            "stripe_count": 2,
            "features": ["layering"],
        }
        self.assertEqual(layout_mismatches(info, dict(info)), [])

    def test_order_difference_is_reported(self):
        source = {"size": 100, "order": 20}
        target = {"size": 100, "order": 22}
        self.assertEqual(
            layout_mismatches(source, target), ["order: source=20 target=22"]
        )

    def test_size_difference_is_reported(self):
        self.assertEqual(
            layout_mismatches({"size": 100}, {"size": 200}),
            ["size: source=100 target=200"],
        )

    def test_target_missing_source_feature_is_reported(self):
        source = {"size": 100, "features": ["layering", "exclusive-lock"]}
        target = {"size": 100, "features": ["layering"]}
        self.assertEqual(
            layout_mismatches(source, target),
            ["features 缺失: exclusive-lock"],
        )

    def test_target_extra_feature_is_accepted(self):
        source = {"size": 100, "features": ["layering"]}
        target = {"size": 100, "features": ["layering", "fast-diff"]}
        self.assertEqual(layout_mismatches(source, target), [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: FAIL with `ImportError: cannot import name 'layout_mismatches'`

- [ ] **Step 3: Write minimal implementation**

在 `ceph_utils.py` 的 `ValidationError` 之后新增：

```python
LAYOUT_EQ_FIELDS = ("order", "stripe_unit", "stripe_count", "data_pool", "size")


class LayoutMismatchError(ValueError):
    """Raised when a target image cannot host the source image layout."""

    def __init__(self, mismatches: list[str]):
        self.mismatches = list(mismatches)
        super().__init__("RBD 布局不一致: " + "; ".join(self.mismatches))


def _normalize_features(raw: Any) -> list[str]:
    if not raw:
        return []
    parts = raw.split(",") if isinstance(raw, str) else list(raw)
    return sorted(str(part).strip() for part in parts if str(part).strip())


def parse_layout(info: dict[str, Any]) -> dict[str, Any]:
    """提取必须与源保持一致的布局字段。"""
    layout = {field: (info or {}).get(field) for field in LAYOUT_EQ_FIELDS}
    layout["features"] = _normalize_features((info or {}).get("features"))
    return layout


def layout_mismatches(source: dict[str, Any], target: dict[str, Any]) -> list[str]:
    """返回源与目标的布局差异清单；目标 features 只需覆盖源。"""
    src = parse_layout(source)
    dst = parse_layout(target)
    diffs = [
        f"{field}: source={src[field]!r} target={dst[field]!r}"
        for field in LAYOUT_EQ_FIELDS
        if src[field] != dst[field]
    ]
    missing = [feature for feature in src["features"] if feature not in dst["features"]]
    if missing:
        diffs.append(f"features 缺失: {', '.join(missing)}")
    return diffs


def assert_layout_matches(source: dict[str, Any], target: dict[str, Any]) -> None:
    mismatches = layout_mismatches(source, target)
    if mismatches:
        raise LayoutMismatchError(mismatches)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: PASS（原有 replace 用例不受影响）

---

### Task 4: 建像参数与全量路径布局对齐（功能项 A）

本任务给既有的全量路径补上布局对齐，属于两条路径共用的基础修复：全量路径
的对外语义（一次全量拷贝、不建快照）不变，只把建像参数由默认值改为按源对齐。

**Files:**
- Modify: `ceph_utils.py`（`replace_rbd_data`）
- Test: `tests/test_ceph_utils.py`

- [ ] **Step 1: Write the failing test**

```python
from ceph_utils import build_import_args


class BuildImportArgsTest(unittest.TestCase):
    def test_carries_order_and_striping(self):
        args = build_import_args(
            {"order": 20, "stripe_unit": 4096, "stripe_count": 2, "features": []}
        )
        self.assertEqual(args[args.index("--order") + 1], "20")
        self.assertEqual(args[args.index("--stripe-unit") + 1], "4096")
        self.assertEqual(args[args.index("--stripe-count") + 1], "2")

    def test_adds_cinder_required_features_to_source_features(self):
        args = build_import_args({"features": ["object-map"]})
        features = args[args.index("--image-feature") + 1].split(",")
        self.assertIn("object-map", features)
        self.assertIn("layering", features)
        self.assertIn("exclusive-lock", features)

    def test_omits_unset_fields(self):
        self.assertEqual(build_import_args({"order": None, "features": []}), [])


class FullPathLayoutAlignmentTest(ReplaceRbdTest):
    def test_import_command_carries_source_layout(self):
        source = json.dumps({"size": 10, "order": 20, "features": ["layering"]})
        runner = FakeRunner(
            info_by_spec={"volume-src-1": source, "volume-dst-1-mig-stage": source},
            missing_once=["volume-dst-1-mig-stage"],
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertTrue(self.utils.replace_rbd_data("volume-src-1", "volume-dst-1"))
        import_cmd = runner.calls[-2]
        self.assertIn("--order", import_cmd)
        self.assertEqual(import_cmd[import_cmd.index("--order") + 1], "20")

    def test_stage_layout_mismatch_aborts_swap(self):
        runner = FakeRunner(
            info_by_spec={
                "volume-src-1": json.dumps({"size": 10, "order": 20}),
                "volume-dst-1-mig-stage": json.dumps({"size": 10, "order": 22}),
            },
            missing_once=["volume-dst-1-mig-stage"],
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertFalse(self.utils.replace_rbd_data("volume-src-1", "volume-dst-1"))
        self.assertFalse(
            any(isinstance(call, list) and "rename" in call for call in runner.calls)
        )

    def test_align_layout_false_keeps_legacy_behaviour(self):
        runner = FakeRunner(
            info_by_spec={
                "volume-src-1": json.dumps({"size": 10, "order": 20}),
                "volume-dst-1-mig-stage": json.dumps({"size": 10, "order": 22}),
            },
            missing_once=["volume-dst-1-mig-stage"],
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        self.assertTrue(
            self.utils.replace_rbd_data(
                "volume-src-1", "volume-dst-1", align_layout=False
            )
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: FAIL with `ImportError: cannot import name 'build_import_args'`

- [ ] **Step 3: Write minimal implementation**

在 `ceph_utils.py` 的 `assert_layout_matches` 之后新增：

```python
CINDER_REQUIRED_FEATURES = ("layering", "exclusive-lock")


def build_import_args(
    layout: dict[str, Any],
    extra_features: tuple[str, ...] = (),
) -> list[str]:
    """生成让目标映像复现源布局的 rbd import 参数。"""
    args: list[str] = []
    if layout.get("order") is not None:
        args += ["--order", str(layout["order"])]
    if layout.get("stripe_count"):
        args += ["--stripe-count", str(layout["stripe_count"])]
    if layout.get("stripe_unit"):
        args += ["--stripe-unit", str(layout["stripe_unit"])]
    if layout.get("data_pool"):
        args += ["--data-pool", str(layout["data_pool"])]
    features = sorted(
        set(layout.get("features") or ())
        | set(CINDER_REQUIRED_FEATURES)
        | set(extra_features)
    )
    if features:
        args += ["--image-feature", ",".join(features)]
    return args
```

`replace_rbd_data` 签名增加 `align_layout: bool = True`：

```python
    def replace_rbd_data(
        self,
        source_rbd_name: str,
        target_rbd_name: str,
        *,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
        align_layout: bool = True,
    ) -> bool:
```

import 命令改为按源布局生成：

```python
            import_cmd = (
                ["rbd", "--conf", self.target_conf, "import"]
                + (
                    build_import_args(parse_layout(source_info))
                    if align_layout
                    else []
                )
                + ["-", stage_spec]
            )
```

在暂存映像大小校验之后追加布局校验：

```python
            if align_layout:
                mismatches = layout_mismatches(source_info, stage_info)
                if mismatches:
                    logging.error(
                        "[MIGRATION] 暂存 RBD 布局与源不一致，拒绝换入: %s",
                        "; ".join(mismatches),
                    )
                    return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: PASS（含既有 replace 用例）

---

### Task 5: 快照原语与 diff 传输

**Files:**
- Modify: `ceph_utils.py`
- Test: `tests/test_ceph_utils.py`

- [ ] **Step 1: Write the failing test**

在 `tests/test_ceph_utils.py` 的 import 列表中补充：

```python
from ceph_utils import IncrementalSession, parse_layout
```

追加用例：

```python
class SnapshotPrimitiveTest(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.utils = CephUtils(
            "/src/ceph.conf", "volumes", "/dst/ceph.conf", "volumes"
        )

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _runner(self, snaps=None):
        runner = FakeRunner(snaps_by_image={"volume-src-1": snaps or []})
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        return runner

    def test_create_snapshot_uses_source_conf_and_snap_spec(self):
        runner = self._runner()
        self.utils.create_source_snapshot("volume-src-1", "mig-j1-0")
        self.assertEqual(
            runner.calls[0],
            [
                "rbd",
                "--conf",
                "/src/ceph.conf",
                "snap",
                "create",
                "volumes/volume-src-1@mig-j1-0",
            ],
        )

    def test_list_source_snapshots_returns_names(self):
        self._runner(
            [{"id": 1, "name": "mig-j1-0"}, {"id": 2, "name": "daily"}]
        )
        self.assertEqual(
            self.utils.list_source_snapshots("volume-src-1"),
            ["mig-j1-0", "daily"],
        )

    def test_remove_snapshot_uses_source_conf(self):
        runner = self._runner()
        self.utils.remove_source_snapshot("volume-src-1", "mig-j1-0")
        self.assertEqual(
            runner.calls[0],
            [
                "rbd",
                "--conf",
                "/src/ceph.conf",
                "snap",
                "rm",
                "volumes/volume-src-1@mig-j1-0",
            ],
        )

    def test_export_import_diff_uses_from_snap_and_import_diff(self):
        runner = self._runner()
        self.utils.export_import_diff(
            "volume-src-1",
            "volume-dst-1-mig-stage",
            from_snap="mig-j1-0",
            to_snap="mig-j1-1",
        )
        export_cmd, import_cmd = runner.calls[-2], runner.calls[-1]
        self.assertIn("export-diff", export_cmd)
        self.assertEqual(
            export_cmd[export_cmd.index("--from-snap") + 1], "mig-j1-0"
        )
        self.assertIn("volumes/volume-src-1@mig-j1-1", export_cmd)
        self.assertIn("import-diff", import_cmd)
        self.assertIn("volumes/volume-dst-1-mig-stage", import_cmd)

    def test_prune_snapshots_only_removes_own_job(self):
        self._runner(
            [
                {"id": 1, "name": "mig-j1-0"},
                {"id": 2, "name": "mig-j1-1"},
                {"id": 3, "name": "daily"},
            ]
        )
        removed = self.utils.prune_source_snapshots("volume-src-1", "j1", keep=1)
        self.assertEqual(removed, ["mig-j1-0"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: FAIL with `ImportError: cannot import name 'IncrementalSession'`

- [ ] **Step 3: Write minimal implementation**

`ceph_utils.py` 顶部补 `from dataclasses import dataclass`。

让 `_export_import` 返回已拷贝字节数（`replace_rbd_data` 忽略返回值，行为不变）：

```python
            return pump_stream(
                exporter.stdout,
                importer.stdin,
                total_bytes=total_bytes,
                progress_cb=wrapped_progress,
                rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            )
```

在 `CephUtils` 之前新增会话数据类：

```python
@dataclass
class IncrementalSession:
    """一次增量预拷贝的上下文，供停机后的最终切换复用。"""

    job_id: str
    source_rbd_name: str
    target_rbd_name: str
    stage_name: str
    layout: dict[str, Any]
    size: int
    last_snapshot: str | None = None
    next_seq: int = 1
```

在 `CephUtils` 内新增方法：

```python
    def _snap_spec(self, pool: str, name: str, snap: str) -> str:
        return f"{pool}/{name}@{snap}"

    def create_source_snapshot(self, rbd_name: str, snap_name: str) -> None:
        self._validate_identifier(rbd_name, "rbd_name")
        self._validate_identifier(snap_name, "snap_name")
        spec = self._snap_spec(self.source_pool, rbd_name, snap_name)
        self._run(
            ["rbd", "--conf", self.source_conf, "snap", "create", spec]
        )
        logging.info("[MIGRATION] 已创建源快照 %s", spec)

    def remove_source_snapshot(self, rbd_name: str, snap_name: str) -> None:
        self._validate_identifier(rbd_name, "rbd_name")
        self._validate_identifier(snap_name, "snap_name")
        spec = self._snap_spec(self.source_pool, rbd_name, snap_name)
        self._run(["rbd", "--conf", self.source_conf, "snap", "rm", spec])
        logging.info("[MIGRATION] 已删除迁移快照 %s", spec)

    def list_source_snapshots(self, rbd_name: str) -> list[str]:
        self._validate_identifier(rbd_name, "rbd_name")
        output = self._run(
            [
                "rbd",
                "--conf",
                self.source_conf,
                "snap",
                "ls",
                "--format",
                "json",
                self._rbd_spec(self.source_pool, rbd_name),
            ],
            capture=True,
        )
        return [entry.get("name") for entry in json.loads(output or "[]")]

    def prune_source_snapshots(
        self,
        rbd_name: str,
        job_id: str,
        keep: int,
        on_removed=None,
    ) -> list[str]:
        """删除本 job 的旧迁移快照；keep=0 表示清空本 job 全部快照。"""
        doomed = snapshots_to_prune(
            self.list_source_snapshots(rbd_name), job_id, keep
        )
        for snap_name in doomed:
            self.remove_source_snapshot(rbd_name, snap_name)
            if on_removed:
                on_removed(snap_name)
        return doomed

    def export_import_diff(
        self,
        source_rbd_name: str,
        target_rbd_name: str,
        *,
        from_snap: str,
        to_snap: str,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
        total_bytes: Optional[int] = None,
    ) -> int:
        export_cmd = [
            "rbd",
            "--conf",
            self.source_conf,
            "export-diff",
            "--from-snap",
            from_snap,
            self._snap_spec(self.source_pool, source_rbd_name, to_snap),
            "-",
        ]
        import_cmd = [
            "rbd",
            "--conf",
            self.target_conf,
            "import-diff",
            "-",
            self._rbd_spec(self.target_pool, target_rbd_name),
        ]
        logging.info(
            "[MIGRATION] 增量 diff %s..%s -> %s",
            from_snap,
            to_snap,
            target_rbd_name,
        )
        return self._export_import(
            export_cmd,
            import_cmd,
            total_bytes=total_bytes,
            progress_cb=progress_cb,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )
```

`ceph_utils.py` 顶部补 import：

```python
from migration_planner import snapshots_to_prune
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: PASS

---

### Task 6: 增量预拷贝与最终切换

把 `replace_rbd_data` 里"watcher/快照校验 -> rm -> rename"这段抽成
`_swap_stage_into_place`，两条路径共用；提取过程不改变全量路径行为。

**Files:**
- Modify: `ceph_utils.py`
- Test: `tests/test_ceph_utils.py`

- [ ] **Step 1: Write the failing test**

```python
class IncrementalCopyTest(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.utils = CephUtils(
            "/src/ceph.conf", "volumes", "/dst/ceph.conf", "volumes"
        )

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _runner(self, source_info, stage_info=None, snaps=None):
        runner = FakeRunner(
            info_by_spec={
                "volume-src-1": json.dumps(source_info),
                "volume-dst-1-mig-stage": json.dumps(stage_info or source_info),
            },
            missing_once=["volume-dst-1-mig-stage"],
            snaps_by_image={"volume-src-1": snaps or []},
        )
        self.utils._runner = runner
        self.utils._export_import = runner.fake_export_import
        return runner

    @staticmethod
    def _commands(runner):
        return " ".join(
            " ".join(call) if isinstance(call, list) else str(call)
            for call in runner.calls
        )

    def test_precopy_creates_base_snapshot_with_source_layout(self):
        runner = self._runner({"size": 10, "order": 20})
        session = self.utils.precopy_volume(
            "volume-src-1", "volume-dst-1", "j1", rounds=0
        )
        self.assertEqual(session.last_snapshot, "mig-j1-0")
        commands = self._commands(runner)
        self.assertIn("volumes/volume-src-1@mig-j1-0", commands)
        self.assertIn("--order 20", commands)
        self.assertIn("volumes/volume-dst-1-mig-stage", commands)

    def test_precopy_rejects_stage_with_wrong_layout(self):
        runner = self._runner(
            {"size": 10, "order": 20}, stage_info={"size": 10, "order": 22}
        )
        with self.assertRaises(LayoutMismatchError):
            self.utils.precopy_volume(
                "volume-src-1", "volume-dst-1", "j1", rounds=0
            )
        self.assertNotIn("rename", self._commands(runner))

    def test_precopy_runs_delta_rounds_and_prunes_older_snapshots(self):
        snaps = [
            {"id": 1, "name": "mig-j1-0"},
            {"id": 2, "name": "mig-j1-1"},
            {"id": 3, "name": "mig-j1-2"},
        ]
        runner = self._runner({"size": 10, "order": 20}, snaps=snaps)
        session = self.utils.precopy_volume(
            "volume-src-1", "volume-dst-1", "j1", rounds=2
        )
        self.assertEqual(session.last_snapshot, "mig-j1-2")
        commands = self._commands(runner)
        self.assertIn("export-diff --from-snap mig-j1-0", commands)
        self.assertIn("export-diff --from-snap mig-j1-1", commands)
        self.assertIn("snap rm volumes/volume-src-1@mig-j1-0", commands)
        self.assertNotIn("snap rm volumes/volume-src-1@mig-j1-1", commands)

    def test_finalize_swaps_and_removes_all_job_snapshots(self):
        source = {"size": 10, "order": 20}
        runner = self._runner(
            source,
            snaps=[
                {"id": 1, "name": "mig-j1-0"},
                {"id": 2, "name": "mig-j1-1"},
                {"id": 3, "name": "daily"},
            ],
        )
        session = IncrementalSession(
            job_id="j1",
            source_rbd_name="volume-src-1",
            target_rbd_name="volume-dst-1",
            stage_name="volume-dst-1-mig-stage",
            layout=parse_layout(source),
            size=10,
            last_snapshot="mig-j1-0",
        )
        self.assertTrue(self.utils.finalize_incremental_volume(session))
        commands = self._commands(runner)
        self.assertIn("export-diff --from-snap mig-j1-0", commands)
        self.assertIn(
            "rename volumes/volume-dst-1-mig-stage volumes/volume-dst-1",
            commands,
        )
        self.assertIn("snap rm volumes/volume-src-1@mig-j1-0", commands)
        self.assertIn("snap rm volumes/volume-src-1@mig-j1-1", commands)
        self.assertNotIn("snap rm volumes/volume-src-1@daily", commands)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: FAIL with `AttributeError: 'CephUtils' object has no attribute 'precopy_volume'`

- [ ] **Step 3: Write minimal implementation**

抽取共用换入逻辑：

```python
    def _swap_stage_into_place(self, target_rbd_name: str, stage_name: str) -> bool:
        """校验 watcher/快照后删除同名目标映像并 rename 换入暂存映像。"""
        stage_spec = self._rbd_spec(self.target_pool, stage_name)
        target_spec = self._rbd_spec(self.target_pool, target_rbd_name)
        status_output = self._run(
            [
                "rbd",
                "--conf",
                self.target_conf,
                "status",
                "--format",
                "json",
                target_spec,
            ],
            capture=True,
        )
        if watchers_present(parse_rbd_info_json(status_output)):
            logging.error(
                "[MIGRATION] 目标 RBD %s 仍有 watcher，拒绝换入", target_rbd_name
            )
            return False
        snap_output = self._run(
            [
                "rbd",
                "--conf",
                self.target_conf,
                "snap",
                "ls",
                "--format",
                "json",
                target_spec,
            ],
            capture=True,
        )
        if json.loads(snap_output):
            logging.error(
                "[MIGRATION] 目标 RBD %s 存在快照，拒绝换入", target_rbd_name
            )
            return False
        self._run(["rbd", "--conf", self.target_conf, "rm", target_spec])
        self._run(
            ["rbd", "--conf", self.target_conf, "rename", stage_spec, target_spec]
        )
        return True
```

`replace_rbd_data` 中原来的 status/snap/rm/rename 段替换为：

```python
            if not self._swap_stage_into_place(target_rbd_name, stage_name):
                return False
```

新增增量方法：

```python
    def _next_source_snapshot(self, session, on_snapshot=None) -> str:
        snap_name = snapshot_name(session.job_id, session.next_seq)
        session.next_seq += 1
        self.create_source_snapshot(session.source_rbd_name, snap_name)
        if on_snapshot:
            on_snapshot(snap_name)
        return snap_name

    def precopy_volume(
        self,
        source_rbd_name: str,
        target_rbd_name: str,
        job_id: str,
        *,
        rounds: int = 3,
        delta_threshold_bytes: int = 0,
        interval_seconds: float = 0.0,
        on_snapshot=None,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
    ) -> "IncrementalSession":
        """源 VM 运行期间完成基像与若干轮增量，返回可续用的会话。"""
        self._validate_identifier(source_rbd_name, "source_rbd_name")
        self._validate_identifier(target_rbd_name, "target_rbd_name")
        source_info = self._info(
            self.source_conf, self.source_pool, source_rbd_name
        )
        layout = parse_layout(source_info)
        stage_name = f"{target_rbd_name}-mig-stage"
        stage_spec = self._rbd_spec(self.target_pool, stage_name)
        if self._image_exists(self.target_conf, self.target_pool, stage_name):
            logging.info("[MIGRATION] 清理上次遗留的暂存镜像 %s", stage_spec)
            self._run(["rbd", "--conf", self.target_conf, "rm", stage_spec])

        session = IncrementalSession(
            job_id=job_id,
            source_rbd_name=source_rbd_name,
            target_rbd_name=target_rbd_name,
            stage_name=stage_name,
            layout=layout,
            size=int(source_info.get("size") or 0),
        )
        base_snap = self._next_source_snapshot(session, on_snapshot)
        export_cmd = [
            "rbd",
            "--conf",
            self.source_conf,
            "export",
            self._snap_spec(self.source_pool, source_rbd_name, base_snap),
            "-",
        ]
        import_cmd = (
            ["rbd", "--conf", self.target_conf, "import"]
            + build_import_args(layout)
            + ["-", stage_spec]
        )
        self._export_import(
            export_cmd,
            import_cmd,
            total_bytes=source_info.get("size") or None,
            progress_cb=progress_cb,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )
        stage_info = self._info(self.target_conf, self.target_pool, stage_name)
        assert_layout_matches(source_info, stage_info)
        session.last_snapshot = base_snap

        for _ in range(max(0, rounds)):
            if interval_seconds > 0:
                time.sleep(interval_seconds)
            next_snap = self._next_source_snapshot(session, on_snapshot)
            copied = self.export_import_diff(
                source_rbd_name,
                stage_name,
                from_snap=session.last_snapshot,
                to_snap=next_snap,
                progress_cb=progress_cb,
                rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            )
            session.last_snapshot = next_snap
            self.prune_source_snapshots(source_rbd_name, job_id, keep=2)
            if delta_threshold_bytes and copied < delta_threshold_bytes:
                break
        return session

    def finalize_incremental_volume(
        self,
        session: "IncrementalSession",
        *,
        on_snapshot=None,
        on_removed=None,
        progress_cb: Optional[ProgressCallback] = None,
        rate_limit_bytes_per_sec: Optional[float] = None,
    ) -> bool:
        """停机后打最终快照、补齐最后一轮增量、换入并清空本 job 快照。"""
        try:
            final_snap = self._next_source_snapshot(session, on_snapshot)
            self.export_import_diff(
                session.source_rbd_name,
                session.stage_name,
                from_snap=session.last_snapshot,
                to_snap=final_snap,
                progress_cb=progress_cb,
                rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
            )
            source_info = self._info(
                self.source_conf, self.source_pool, session.source_rbd_name
            )
            stage_info = self._info(
                self.target_conf, self.target_pool, session.stage_name
            )
            if stage_info.get("size") != source_info.get("size"):
                logging.error("[MIGRATION] 增量后大小校验失败")
                return False
            assert_layout_matches(source_info, stage_info)
            if not self._swap_stage_into_place(
                session.target_rbd_name, session.stage_name
            ):
                return False
            self.prune_source_snapshots(
                session.source_rbd_name,
                session.job_id,
                keep=0,
                on_removed=on_removed,
            )
            logging.info(
                "[MIGRATION] 增量迁移完成 %s -> %s",
                session.source_rbd_name,
                session.target_rbd_name,
            )
            return True
        except LayoutMismatchError as exc:
            logging.error("[MIGRATION] 增量后布局校验失败: %s", exc)
            return False
        except (subprocess.CalledProcessError, OSError) as exc:
            logging.error("[MIGRATION] 增量迁移失败: %s", exc)
            return False
```

`ceph_utils.py` 顶部补 import：

```python
from migration_planner import snapshot_name, snapshots_to_prune
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_ceph_utils -v`
Expected: PASS（含既有全量 replace 用例，确认抽取换入逻辑未改变行为）

---

### Task 7: 数据搬运器与唯一分叉点

迁移流水线的生命周期步骤（端口/数据卷/BFV 创建、停目标、采集卷映射、停源、
开机、验证）只有一份实现，两条路径共用。本任务把"卷数据搬运"抽成接口一致的
两个搬运器，使 `_migrate_vm_inner` 内不出现任何 mode 判断。

**Files:**
- Modify: `migration_manager.py`
- Modify: `state_machine.py`（新增 `VmStatus.PRECOPYING`）
- Test: `tests/test_migration_manager.py`

- [ ] **Step 1: Write the failing test**

```python
from state_machine import MigrationMode
from migration_manager import FullVolumeMover, IncrementalVolumeMover


class CopyModeDispatchTest(unittest.TestCase):
    def _manager(self):
        ceph = mock.Mock()
        ceph.replace_rbd_data.return_value = True
        ceph.finalize_incremental_volume.return_value = True
        ceph.prune_source_snapshots.return_value = []
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        return manager, ceph

    @staticmethod
    def _volume(mode):
        return VolumeTask(
            source_volume_id="s1",
            source_rbd_name="volume-s1",
            target_volume_id="t1",
            target_rbd_name="volume-t1",
            source_pool="volumes",
            target_pool="volumes",
            mode=mode,
        )

    @staticmethod
    def _vm(volume):
        vm = VmTask(name="vm1", target_az="az1")
        vm.volumes = [volume]
        return vm

    def test_full_mode_uses_full_replace_only(self):
        manager, ceph = self._manager()
        volume = self._volume(MigrationMode.FULL)
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.replace_rbd_data.assert_called_once()
        ceph.finalize_incremental_volume.assert_not_called()
        self.assertEqual(volume.status, VolumeStatus.SUCCESS)

    def test_incremental_mode_uses_finalize_only(self):
        manager, ceph = self._manager()
        volume = self._volume(MigrationMode.INCREMENTAL)
        manager._sessions[volume.source_volume_id] = mock.Mock()
        manager._job_id = "j1"
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.replace_rbd_data.assert_not_called()
        ceph.finalize_incremental_volume.assert_called_once()
        self.assertEqual(volume.status, VolumeStatus.SUCCESS)

    def test_incremental_failure_does_not_fall_back_by_default(self):
        manager, ceph = self._manager()
        ceph.finalize_incremental_volume.return_value = False
        volume = self._volume(MigrationMode.INCREMENTAL)
        manager._sessions[volume.source_volume_id] = mock.Mock()
        manager._job_id = "j1"
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.replace_rbd_data.assert_not_called()
        self.assertEqual(volume.status, VolumeStatus.FAILED)

    def test_failed_incremental_volume_cleans_its_snapshots(self):
        manager, ceph = self._manager()
        ceph.finalize_incremental_volume.return_value = False
        ceph.prune_source_snapshots.return_value = ["mig-j1-0"]
        volume = self._volume(MigrationMode.INCREMENTAL)
        volume.snapshots = [{"name": "mig-j1-0", "state": "created"}]
        manager._sessions[volume.source_volume_id] = mock.Mock()
        manager._job_id = "j1"
        manager._copy_volumes(self._vm(volume), max_workers=1)
        ceph.prune_source_snapshots.assert_called_once_with(
            "volume-s1", "j1", keep=0
        )
        self.assertEqual(volume.cleanup_status, "cleaned")

    def test_mover_selection_is_the_only_mode_branch(self):
        manager, _ceph = self._manager()
        self.assertIsInstance(
            manager._mover_for_mode(MigrationMode.FULL), FullVolumeMover
        )
        self.assertIsInstance(
            manager._mover_for_mode(MigrationMode.INCREMENTAL),
            IncrementalVolumeMover,
        )

    def test_full_mover_hooks_are_noops(self):
        manager, ceph = self._manager()
        mover = manager._mover_for_mode(MigrationMode.FULL)
        volume = self._volume(MigrationMode.FULL)
        self.assertIsNone(mover.prepare(self._vm(volume), {}))
        mover.on_failure(volume)
        ceph.precopy_volume.assert_not_called()
        ceph.prune_source_snapshots.assert_not_called()


class PrecopyTest(unittest.TestCase):
    def test_precopy_records_session_layout_and_snapshots(self):
        ceph = mock.Mock()
        session = mock.Mock()
        session.layout = {"order": 20}
        ceph.precopy_volume.return_value = session

        def run_precopy(*args, **kwargs):
            kwargs["on_snapshot"]("mig-j1-0")
            return session

        ceph.precopy_volume.side_effect = run_precopy
        manager = MigrationManager(source_os=None, target_os=None, ceph_utils=ceph)
        manager._job_id = "j1"
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                target_volume_id="t1",
                target_rbd_name="volume-t1",
                mode=MigrationMode.INCREMENTAL,
            )
        ]
        manager._precopy_volumes(vm, {"job_id": "j1", "delta_rounds": 1})

        self.assertIs(manager._sessions["s1"], session)
        self.assertEqual(vm.volumes[0].layout, {"order": 20})
        self.assertEqual(vm.volumes[0].snapshots[0]["name"], "mig-j1-0")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_migration_manager -v`
Expected: FAIL with `AttributeError: 'MigrationManager' object has no attribute '_sessions'`

- [ ] **Step 3: Write minimal implementation**

`state_machine.py` 的 `VmStatus` 中新增：

```python
    PRECOPYING = "precopying"
```

`migration_manager.py` 顶部补 import：

```python
from datetime import datetime, timezone
from state_machine import MigrationMode
from migration_planner import order_system_and_data  # 已存在，勿重复添加
```

`__init__` 内新增会话表：

```python
        self._sessions: dict[str, Any] = {}
        self._job_id = ""
```

`migrate_vm` 开头记录作业级上下文（放在 `vm.start()` 之前）：

```python
        self._job_id = str(options.get("job_id") or "")
```

`_migrate_vm_inner` 只调整顺序并调用搬运器，不出现 mode 判断：

1. 把原"6. map source and target volumes"整段（`COLLECTING_VOLUMES` 阶段与
   `_build_volume_tasks` 调用）上移到原"5. stop source"之前——该步骤只读
   attachment 与 `rbd info`，不依赖关机状态；
2. 映射完成后调用 `mover.prepare(vm, options)`：全量为空操作，增量为基像与
   多轮增量（此时源 VM 仍在运行）；
3. `_copy_volumes` 传入同一个 `mover`。

```python
        mover = self._mover_for(vm)

        # 5. map source and target volumes（只读，提前到停源之前）
        self._set_phase(vm, VmStatus.COLLECTING_VOLUMES, "collecting_volumes")
        target_entries = self.target_os.get_server_volumes_with_device(
            vm.target_server_id
        )
        self._build_volume_tasks(...)

        # 6. 预拷贝：全量空操作；增量做基像 + 多轮增量
        mover.prepare(vm, options)

        # 7. stop source for data consistency
        self._set_phase(vm, VmStatus.STOPPING_SOURCE, "stopping_source")
        self._stop_and_wait(vm.source_server_id, self.source_os)
```

并把卷拷贝调用改为传入搬运器：

```python
        self._copy_volumes(
            vm,
            max_workers=int(options.get("volume_concurrency") or 1),
            rate_limit_mb_s=float(options.get("rate_limit_mb_s") or 0),
            mover=mover,
        )
```

`_build_volume_tasks` 签名末尾增加池参数，并在构造 `VolumeTask` 时写入 mode/池：

```python
    def _build_volume_tasks(
        self,
        vm: VmTask,
        source_boot_entries: list[dict[str, Any]],
        source_data_entries: list[dict[str, Any]],
        target_entries: list[dict[str, Any]],
        target_data_volume_ids: list[str],
        source_pool: str = "",
        target_pool: str = "",
    ) -> None:
```

```python
                VolumeTask(
                    source_volume_id=source_id,
                    source_rbd_name=f"volume-{source_id}",
                    target_volume_id=target_id,
                    target_rbd_name=f"volume-{target_id}",
                    size=source_entry.get("size") or target_entry.get("size") or 0,
                    source_pool=source_pool,
                    target_pool=target_pool,
                    mode=vm.mode,
                )
```

调用处补池参数：

```python
        self._build_volume_tasks(
            vm,
            source_boot_entries=source_boot_entries,
            source_data_entries=source_data_entries,
            target_entries=target_entries,
            target_data_volume_ids=target_data_volume_ids,
            source_pool=str(options.get("source_ceph_pool") or ""),
            target_pool=str(options.get("target_ceph_pool") or ""),
        )
```

`_copy_volumes` 签名增加搬运器参数，内部只调搬运器；重试、内存门控、进度回调
与 `_persist` 仍是一套共用逻辑：

```python
    def _copy_volumes(
        self,
        vm: VmTask,
        max_workers: int,
        rate_limit_mb_s: float = 0.0,
        mover=None,
    ) -> None:
        mover = mover or self._mover_for(vm)
```

```python
                    ok = mover.finish(
                        volume, on_progress, rate_limit_bytes_per_sec
                    )
```

重试循环结束后、写 `volume.error` 之前统一走搬运器的失败钩子：

```python
            if not ok:
                mover.on_failure(volume)
```

**不做自动降级**：增量失败只标记该 VM 失败并保留目标资源；要改走全量由操作员
在前端把该 VM 的模式改成"全量拷贝"后重新提交。

`copy_one` 至此不再出现任何 mode 判断：失败清理由搬运器的 `on_failure` 承担，
全量实现为空操作。

新增方法：

```python
    def _record_snapshot(self, volume: VolumeTask, snap_name: str) -> None:
        volume.snapshots.append(
            {
                "name": snap_name,
                "state": "created",
                "at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._persist()

    def _mark_snapshot_removed(self, volume: VolumeTask, snap_name: str) -> None:
        for record in volume.snapshots:
            if record.get("name") == snap_name:
                record["state"] = "removed"
        self._persist()

    def _precopy_volumes(self, vm: VmTask, options: dict[str, Any]) -> None:
        """源 VM 仍在运行时完成基像与增量轮次（仅增量模式调用）。"""
        self._set_phase(vm, VmStatus.PRECOPYING, "precopying")
        rounds = int(options.get("delta_rounds") or 1)
        threshold_mb = float(options.get("delta_threshold_mb") or 0)
        for volume in vm.volumes:
            self._check_stop()
            self.copy_gate.acquire(can_proceed=self._can_proceed)
            try:
                session = self.ceph_utils.precopy_volume(
                    volume.source_rbd_name,
                    volume.target_rbd_name,
                    self._job_id,
                    rounds=rounds,
                    delta_threshold_bytes=int(threshold_mb * 1024 * 1024),
                    interval_seconds=float(
                        options.get("delta_interval_seconds") or 0
                    ),
                    on_snapshot=lambda name, vol=volume: self._record_snapshot(
                        vol, name
                    ),
                )
            finally:
                self.copy_gate.release()
            self._sessions[volume.source_volume_id] = session
            volume.layout = session.layout
            self._persist()

    def _finalize_incremental_volume(
        self,
        volume: VolumeTask,
        on_progress,
        rate_limit_bytes_per_sec: float,
    ) -> bool:
        session = self._sessions.get(volume.source_volume_id)
        if session is None:
            volume.error = "缺少增量会话，无法完成最终切换"
            return False
        ok = self.ceph_utils.finalize_incremental_volume(
            session,
            on_snapshot=lambda name: self._record_snapshot(volume, name),
            on_removed=lambda name: self._mark_snapshot_removed(volume, name),
            progress_cb=on_progress,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )
        if ok:
            self._sessions.pop(volume.source_volume_id, None)
        return ok

    def _cleanup_volume_snapshots(self, volume: VolumeTask) -> None:
        if not self._job_id or volume.mode != MigrationMode.INCREMENTAL:
            return
        try:
            removed = self.ceph_utils.prune_source_snapshots(
                volume.source_rbd_name, self._job_id, keep=0
            )
        except Exception:  # noqa: BLE001 - 清理失败不改写迁移结论
            logging.exception(
                "[MIGRATION] 迁移快照清理失败 %s", volume.source_rbd_name
            )
            volume.cleanup_status = "pending"
            self._persist()
            return
        for snap_name in removed:
            self._mark_snapshot_removed(volume, snap_name)
        volume.cleanup_status = "cleaned"
        self._persist()
```

新增搬运器选择（整个流水线唯一一处 mode 判断）：

```python
    def _mover_for_mode(self, mode: MigrationMode):
        if mode == MigrationMode.INCREMENTAL:
            return IncrementalVolumeMover(self)
        return FullVolumeMover(self)

    def _mover_for(self, vm: VmTask):
        return self._mover_for_mode(vm.mode)
```

在 `migration_manager.py` 末尾追加两个搬运器，接口一致（`prepare` /
`finish` / `on_failure`），全量实现的两个钩子都是空操作：

```python
class FullVolumeMover:
    """全量功能项：一次性 export/import，不创建任何迁移快照。"""

    mode = MigrationMode.FULL

    def __init__(self, manager: "MigrationManager"):
        self.manager = manager

    def prepare(self, vm: VmTask, options: dict[str, Any]) -> None:
        """全量路径无预拷贝。"""
        return None

    def finish(self, volume: VolumeTask, on_progress, rate_limit_bytes_per_sec):
        return self.manager.ceph_utils.replace_rbd_data(
            volume.source_rbd_name,
            volume.target_rbd_name,
            progress_cb=on_progress,
            rate_limit_bytes_per_sec=rate_limit_bytes_per_sec,
        )

    def on_failure(self, volume: VolumeTask) -> None:
        """全量路径没有迁移快照需要清理。"""
        return None


class IncrementalVolumeMover(FullVolumeMover):
    """增量功能项：预拷贝 + 末轮 diff + 换入 + 快照清理。"""

    mode = MigrationMode.INCREMENTAL

    def prepare(self, vm: VmTask, options: dict[str, Any]) -> None:
        self.manager._precopy_volumes(vm, options)

    def finish(self, volume: VolumeTask, on_progress, rate_limit_bytes_per_sec):
        return self.manager._finalize_incremental_volume(
            volume, on_progress, rate_limit_bytes_per_sec
        )

    def on_failure(self, volume: VolumeTask) -> None:
        self.manager._cleanup_volume_snapshots(volume)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_migration_manager -v`
Expected: PASS

---

### Task 8: 快照清理与孤儿回收（GC）

**Files:**
- Modify: `job_manager.py`
- Modify: `app.py`
- Test: `tests/test_job_manager.py`

- [ ] **Step 1: Write the failing test**

```python
import os
import shutil
import tempfile
from unittest import mock

from job_manager import JobManager
from state_machine import (
    JobStatus,
    MigrationJob,
    MigrationMode,
    VolumeTask,
    VmTask,
)


class OrphanSnapshotSweepTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _manager_with_job(self, job_id, status):
        manager = JobManager(state_file=os.path.join(self.root, "jobs_state.json"))
        job = MigrationJob(id=job_id, status=status)
        vm = VmTask(name="vm1", target_az="az1", mode=MigrationMode.INCREMENTAL)
        vm.volumes = [
            VolumeTask(
                source_volume_id="s1",
                source_rbd_name="volume-s1",
                source_pool="volumes",
                target_pool="volumes",
                mode=MigrationMode.INCREMENTAL,
            )
        ]
        job.vms = [vm]
        manager._jobs[job.id] = job
        job_dir = os.path.join(self.root, job_id)
        os.makedirs(job_dir, exist_ok=True)
        for name in ("source_ceph.conf", "target_ceph.conf"):
            with open(os.path.join(job_dir, name), "w", encoding="utf-8") as handle:
                handle.write("")
        return manager

    def test_sweeps_only_orphan_migration_snapshots(self):
        manager = self._manager_with_job("jold", JobStatus.COMPLETED)
        fake = mock.Mock()
        fake.list_source_snapshots.return_value = ["mig-jold-0", "daily"]

        removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, ["mig-jold-0"])
        fake.remove_source_snapshot.assert_called_once_with(
            "volume-s1", "mig-jold-0"
        )

    def test_active_job_snapshots_are_kept(self):
        manager = self._manager_with_job("jlive", JobStatus.RUNNING)
        fake = mock.Mock()
        fake.list_source_snapshots.return_value = ["mig-jlive-0"]

        removed = manager.sweep_orphan_snapshots(ceph_factory=lambda **_: fake)

        self.assertEqual(removed, [])
        fake.remove_source_snapshot.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_job_manager -v`
Expected: FAIL with `AttributeError: 'JobManager' object has no attribute 'sweep_orphan_snapshots'`

- [ ] **Step 3: Write minimal implementation**

`job_manager.py` 顶部补 import：

```python
from migration_planner import orphan_snapshots
from state_machine import MigrationMode
```

`JobManager` 新增方法：

```python
    def active_job_ids(self) -> set[str]:
        with self._lock:
            return {
                job.id
                for job in self._jobs.values()
                if job.status == JobStatus.RUNNING
            }

    def sweep_orphan_snapshots(self, ceph_factory=None) -> list[str]:
        """删除归属已结束 job 的迁移快照；非 mig- 前缀一律不动。"""
        from ceph_utils import CephUtils  # 延迟导入，避免模块级循环依赖

        factory = ceph_factory or CephUtils
        active = self.active_job_ids()
        upload_root = os.path.dirname(self._state_file) or UPLOAD_FOLDER
        with self._lock:
            jobs = list(self._jobs.values())
        removed: list[str] = []
        for job in jobs:
            job_dir = os.path.join(upload_root, job.id)
            source_conf = os.path.join(job_dir, "source_ceph.conf")
            target_conf = os.path.join(job_dir, "target_ceph.conf")
            if not (
                os.path.exists(source_conf) and os.path.exists(target_conf)
            ):
                continue
            client = None
            for vm in job.vms:
                for volume in vm.volumes:
                    if volume.mode != MigrationMode.INCREMENTAL:
                        continue
                    if client is None:
                        client = factory(
                            source_conf=source_conf,
                            source_pool=volume.source_pool or "volumes",
                            target_conf=target_conf,
                            target_pool=volume.target_pool or "volumes",
                        )
                    try:
                        names = client.list_source_snapshots(
                            volume.source_rbd_name
                        )
                    except Exception:  # noqa: BLE001 - GC 失败不影响服务
                        logging.exception(
                            "[MIGRATION] 扫描迁移快照失败 %s",
                            volume.source_rbd_name,
                        )
                        continue
                    for snap_name in orphan_snapshots(names, active):
                        try:
                            client.remove_source_snapshot(
                                volume.source_rbd_name, snap_name
                            )
                            removed.append(snap_name)
                        except Exception:  # noqa: BLE001
                            logging.exception(
                                "[MIGRATION] 删除孤儿快照失败 %s", snap_name
                            )
        return removed

    def start_snapshot_gc(self) -> None:
        """服务启动时后台清扫上次运行遗留的迁移快照。"""
        threading.Thread(
            target=self.sweep_orphan_snapshots,
            name="snapshot-gc",
            daemon=True,
        ).start()
```

`app.py` 的 `__main__` 块中，`job_manager.start_persistent_saver()` 之后加一行：

```python
    job_manager.start_snapshot_gc()
```

`app.py` 的 job worker `finally` 段改为每个 job 结束后再扫一次：

```python
            finally:
                job_manager.unregister_worker(job_id)
                job_manager.sweep_best_effort()
```

并在 `job_manager.py` 增加：

```python
    def sweep_best_effort(self) -> None:
        try:
            self.sweep_orphan_snapshots()
        except Exception:  # noqa: BLE001 - 清扫失败不影响任务结果
            logging.exception("[MIGRATION] 迁移快照清扫失败")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_job_manager -v`
Expected: PASS

---

### Task 9: mode 解析与请求参数传递

**Files:**
- Modify: `excel_parser.py`
- Modify: `job_manager.py`（`create_job`）
- Modify: `app.py`（`/api/migrate` 的 options）
- Test: `tests/test_excel_parser.py`, `tests/test_job_manager.py`

- [ ] **Step 1: Write the failing test**

追加到 `tests/test_excel_parser.py`：

```python
class MigrationModeParsingTest(unittest.TestCase):
    def test_defaults_to_full(self):
        rows = parse_selected_rows(
            [{"server_id": "s1", "vm_name": "vm1", "target_az": "az1"}]
        )
        self.assertEqual(rows[0].mode, "full")

    def test_accepts_incremental(self):
        rows = parse_selected_rows(
            [
                {
                    "server_id": "s1",
                    "vm_name": "vm1",
                    "target_az": "az1",
                    "mode": "incremental",
                }
            ]
        )
        self.assertEqual(rows[0].mode, "incremental")

    def test_rejects_unknown_mode(self):
        with self.assertRaises(ValueError):
            parse_selected_rows(
                [
                    {
                        "server_id": "s1",
                        "vm_name": "vm1",
                        "target_az": "az1",
                        "mode": "fast",
                    }
                ]
            )
```

追加到 `tests/test_job_manager.py`：

```python
class CreateJobModeTest(unittest.TestCase):
    def test_create_job_propagates_incremental_mode(self):
        root = tempfile.mkdtemp()
        try:
            manager = JobManager(state_file=os.path.join(root, "jobs_state.json"))
            rows = [
                MigrationRow(
                    vm_name="vm1", target_az="az1", mode="incremental"
                )
            ]
            job = manager.create_job(rows, job_id="j1")
            self.assertEqual(job.vms[0].mode, MigrationMode.INCREMENTAL)
        finally:
            shutil.rmtree(root, ignore_errors=True)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_excel_parser tests.test_job_manager -v`
Expected: FAIL with `AttributeError: 'MigrationRow' object has no attribute 'mode'`

- [ ] **Step 3: Write minimal implementation**

`excel_parser.py`：`MigrationRow` 增加字段与模式校验：

```python
    mode: str = "full"
```

```python
VALID_MODES = {"full", "incremental"}


def _parse_mode(raw: Any) -> str:
    text = str(raw or "full").strip().lower()
    if text not in VALID_MODES:
        raise ValueError(f"不支持的迁移模式: {raw}（可选 full/incremental）")
    return text
```

`OPTIONAL_COLUMNS` 增加 `"mode"`；`parse_rows` 与 `parse_selected_rows`
构造 `MigrationRow` 时都加 `mode=_parse_mode(record.get("mode"))`。

`job_manager.py` 的 `create_job` 中把 `mode` 传给 `VmTask`：

```python
                mode=MigrationMode.parse(row.mode),
```

`app.py` 的 `options` 字典补充：

```python
            "job_id": job_id,
            "delta_rounds": _positive_int(
                request.form.get("delta_rounds"), 0, 10
            ),
            "delta_threshold_mb": _non_negative_float(
                request.form.get("delta_threshold_mb")
            ),
            "delta_interval_seconds": _non_negative_float(
                request.form.get("delta_interval_seconds")
            ),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_excel_parser tests.test_job_manager -v`
Expected: PASS

---

### Task 10: 前端模式选择与快照状态展示

**Files:**
- Modify: `templates/index.html`

- [ ] **Step 1: 作业级模式选择与逐 VM 模式下拉**

在清单工具栏（`#global-image` 之后）新增作业级模式下拉，作为"前端选择增量还是
全量"的主入口：

```html
<div class="toolbar-group">
    <label for="global-mode">③ 迁移模式（可逐台覆盖）</label>
    <select class="form-control" id="global-mode">
        <option value="full">全量拷贝</option>
        <option value="incremental">增量预拷贝（实验）</option>
    </select>
</div>
```

在 `DOMContentLoaded` 区增加与 `#global-image` 同构的联动（模式不随目标镜像
清空而重置，这里显式同步所有行与行内下拉）：

```javascript
    $('#global-mode').addEventListener('change', () => {
        if (!state.previewRows.length) return;
        state.previewRows.forEach(row => { row.mode = $('#global-mode').value; });
        $$('#preview-wrap select').forEach(select => {
            if (select.dataset.field === 'mode') select.value = $('#global-mode').value;
        });
    });
```

`addSelectedSourceServer` 里的 `state.previewRows.push({...})` 增加
`mode: ($('#global-mode') && $('#global-mode').value) || 'full',`
（放在 `target_flavor: ''` 之后），勾选新 VM 时继承当前作业级选择。

`renderPreview` 中把 `specGrid.append(azField, imageField, flavorField);` 替换为：

```javascript
        const modeField = fieldBox('迁移模式');
        const modeSelect = document.createElement('select');
        modeSelect.className = 'form-control';
        modeSelect.appendChild(new Option('全量拷贝（默认）', 'full'));
        modeSelect.appendChild(new Option('增量预拷贝（实验）', 'incremental'));
        modeSelect.value = row.mode || 'full';
        modeSelect.addEventListener('change', () => {
            row.mode = modeSelect.value;
            refreshChecklistUI();
        });
        modeSelect.dataset.row = index;
        modeSelect.dataset.field = 'mode';
        modeField.appendChild(modeSelect);
        specGrid.append(azField, imageField, flavorField, modeField);
```

- [ ] **Step 2: 提交参数**

`selectedRows` 映射中加 `mode: row.mode || 'full'`；`rowOverrides` 里同样加
`mode: row.mode || 'full'`。

在 `data.append('rate_limit_mb_s', ...)` 之后追加：

```javascript
    ['delta_rounds', 'delta_threshold_mb', 'delta_interval_seconds'].forEach(name => {
        data.append(name, $(`[name="${name}"]`).value);
    });
```

在并发/限速输入所在行后追加增量参数控件：

```html
<div class="col-md-3">
  <label>增量轮次</label>
  <input class="form-control" type="number" name="delta_rounds" min="0" max="10" value="3">
</div>
<div class="col-md-3">
  <label>增量阈值 (MiB)</label>
  <input class="form-control" type="number" name="delta_threshold_mb" min="0" value="512">
</div>
<div class="col-md-3">
  <label>增量轮次间隔 (秒)</label>
  <input class="form-control" type="number" name="delta_interval_seconds" min="0" value="300">
</div>
```

- [ ] **Step 3: 状态与快照展示**

`statusBadge` 的阶段映射表增加：

```javascript
        precopying: ['badge-info', '增量预拷贝'],
```

卷表格表头与行改为带模式与快照清理列：

```javascript
            thead.innerHTML = '<tr><th>源 RBD</th><th>目标 RBD</th><th>模式</th><th>状态</th><th>进度</th><th>快照清理</th><th>错误</th></tr>';
```

```javascript
                const modeCell = text('td', '', volume.mode === 'incremental' ? '增量' : '全量');
                const cleanupCell = text(
                    'td',
                    '',
                    volume.cleanup_status === 'cleaned' ? '已清理'
                        : volume.cleanup_status === 'pending' ? '待清理' : '—'
                );
                tr.append(src, dst, modeCell, st, volumeProgressCell(volume), cleanupCell, text('td', '', volume.error || ''));
```

- [ ] **Step 4: 失败 VM 的"以全量重跑"提示**

在 VM 卡片尾部（`created_resources` 展示之后）追加：

```javascript
        if (vm.status === 'failed' && (vm.volumes || []).some(v => v.mode === 'incremental')) {
            const retry = document.createElement('button');
            retry.className = 'btn btn-sm btn-outline-danger mt-2';
            retry.textContent = '以全量模式重跑该 VM';
            retry.addEventListener('click', () => {
                toast('请在上方清单把该 VM 的迁移模式改为“全量拷贝”后重新提交', 'info');
            });
            card.appendChild(retry);
        }
```

- [ ] **Step 5: 手工验证**

Run: `python3 -m unittest discover -s tests`
Expected: PASS。

启动 `python app.py`，勾选一台源 VM，确认模式下拉可选"增量预拷贝"，提交后
任务卡片出现"增量预拷贝"阶段与"快照清理"列。

---

### Task 11: 全量回归、文档与上线同步

**Files:**
- `README.md`

- [ ] **Step 1: 回归**

Run:

```bash
python3 -m unittest discover -s tests
python3 -m py_compile app.py ceph_utils.py migration_manager.py job_manager.py state_machine.py excel_parser.py migration_planner.py
```

Expected: 全部 PASS，编译 exit 0。

- [ ] **Step 2: 更新 README**

在功能/API 段落补充：

```markdown
- 迁移模式为两个独立功能项：`全量拷贝`（默认，一次性 export/import）
  与 `增量预拷贝`（源 VM 运行期间做基像 + `export-diff/import-diff`
  多轮增量，停机后仅补最后一轮，需逐 VM 开启）；
- 两条路径共用布局对齐：建像参数（`order`/`stripe`/`features`）按源 RBD
  对齐，换入前逐字段校验，避免迁移后业务无法启动；
- 增量迁移会在源卷创建 `mig-<job_id>-<seq>` 临时快照，任务结束（成功或
  失败）自动清理，服务启动与每个任务结束后回收孤儿快照；
- 迁移模式在页面显式选择：配置区作业级"迁移模式"下拉（全量拷贝 /
  增量预拷贝），清单卡片可逐台覆盖；两条路径不做运行中自动切换。
```

- [ ] **Step 3: 同步 ConfigMap 并重启（集群侧执行）**

```bash
kubectl -n migrate create configmap migrate-vm-bin \
  --from-file=app.py --from-file=openstack_utils.py --from-file=config.py \
  --from-file=ceph_utils.py --from-file=migration_manager.py \
  --from-file=job_manager.py --from-file=graceful_shutdown.py \
  --from-file=state_machine.py --from-file=excel_parser.py \
  --from-file=migration_planner.py --from-file=repro_create.py \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n migrate create configmap migrate-vm-html \
  --from-file=index.html=templates/index.html \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n migrate rollout restart deployment/openstack-vm-migration-deployment
```

---

## 自检

- Spec 覆盖：布局清单与比对（Task 3/4）、全量与增量作为独立功能项
  （Task 2/7，唯一分叉点是搬运器）、增量预拷贝与最终切换（Task 5/6）、
  临时快照生命周期与清理（Task 1/5/6/7/8）、孤儿回收（Task 8）、前端入口
  （Task 10）、上线（Task 11）均已对应。
- 代码复用：生命周期步骤（端口/数据卷/BFV 创建、停目标、采集卷映射、停源、
  开机、验证）只有一份实现；`_migrate_vm_inner` 与 `_copy_volumes` 内均无
  mode 判断，分叉收敛在 `_mover_for_mode` 一处。
- 无占位符：所有新增函数都给出完整实现与测试；前端改动给出可直接替换的
  代码片段与定位说明。
- 类型一致性：`mode` 在 `MigrationRow`（str）、`VmTask`/`VolumeTask`
  （`MigrationMode`）中语义一致，解析统一走 `MigrationMode.parse`；
  快照命名只在 `migration_planner.snapshot_name` 产出，`ceph_utils` 与其
  共用；`cleanup_status` 取值 `none`/`cleaned`/`pending` 在前后端一致。
- 全量路径零回归：Task 4/6 对 `replace_rbd_data` 的改动都要求在
  `tests.test_ceph_utils` 全绿后继续；`align_layout=False` 保留旧行为。
- 当前目录不是 git 仓库，任务内不包含 commit 步骤。
